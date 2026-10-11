## 6. Operators, XComs and relationships

§4's Dag used `@task`-decorated functions. This one uses **operators**, the
pre-built task types, and shows that the two styles mix freely in one Dag. Both
styles produce **tasks**: a task is any step in a Dag, and a *TaskFlow task* is
one written as a `@task` function instead of by instantiating an operator
yourself (underneath, Airflow turns it into an operator anyway; *Which class to
subclass?*, further down, explains it). The common
operators ship in `apache-airflow-providers-standard`, which is already
installed:

```python
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
```

Beyond the standard ones there is a provider for nearly every system you would
want to talk to — S3, Snowflake, BigQuery, Postgres, Kubernetes, Databricks,
Slack, HTTP, Docker, dbt Cloud, Airbyte — each shipping operators, hooks (the
clients underneath them, §6's *Hooks, Connections and Variables*) and sensors
(tasks that wait, §11), installed as `apache-airflow-providers-<name>` and listed in the UI
under Admin → Providers. Use one before writing HTTP calls by hand: a
provider's operator already handles connections, retries and secret lookup.
When none fits, you can write your own; §6's *Writing your own operator*
subsection shows how, once this Dag is built.

### Build it: XComs first

The Dag you are about to write, `dags/02_operators_and_xcoms.py`, wires ten
tasks of four kinds — `EmptyOperator`, `PythonOperator`, `BashOperator` and
TaskFlow `@task` — and passes a number between them. The thing it teaches is how
tasks hand values to each other.

**XComs** ("cross-communications") are how tasks pass small values. They are
stored in the metadata database, but a task never accesses the metadata
database directly: it reads and writes XComs by asking the API server, through
the Task Execution API (§3). §4's `@task` functions already used them, out of
sight: every
`return` was an XCom push, every argument an XCom pull. With operators you push and pull explicitly.

Create `dags/02_operators_and_xcoms.py` with the imports and two plain Python
functions that a `PythonOperator` will call:

```python dags/02_operators_and_xcoms.py
"""Lesson 2 - operators, XComs, and relationships."""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Label, chain, cross_downstream, dag, task
from airflow.sdk.types import RuntimeTaskInstanceProtocol

DEFAULT_ARGS = {
    "owner": "mlops-course",
    "retries": 1,
    "retry_delay": pendulum.duration(seconds=30),
}


def _push_seed(ti: RuntimeTaskInstanceProtocol) -> None:
    ti.xcom_push(key="seed", value=7)


def _multiply(ti: RuntimeTaskInstanceProtocol, factor: int) -> None:
    seed = ti.xcom_pull(task_ids="push_seed", key="seed")
    ti.xcom_push(key="product", value=seed * factor)
```

`ti` — the task instance — is injected because the parameter is named `ti`,
the same name-based injection `**context` did in §4. Its type,
`RuntimeTaskInstanceProtocol` from `airflow.sdk.types`, describes that object,
`xcom_push` and `xcom_pull` among its methods. Two things to notice:

**`xcom_pull` without `task_ids` reads the current task's own XComs** and
quietly returns `None`. Almost every pull you write needs `task_ids=`; the
`_multiply` above names its source.

**XComs are for metadata, not payloads.** They live in the metadata database.
Pass a path, a run id, a row count; put the dataframe in object storage and
the model in a registry. Airflow serialises the value when it stores it and
accepts more than JSON — a `datetime`, a `set`, even a dataframe —, but that
it fits does not mean it belongs there.

Now the Dag header and the first three operators:

```python dags/02_operators_and_xcoms.py

@dag(
    dag_id="operators_and_xcoms",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "basics"],
    doc_md=__doc__,
)
def operators_and_xcoms():
    start = EmptyOperator(task_id="start")
    push_seed = PythonOperator(task_id="push_seed", python_callable=_push_seed)
    multiply = PythonOperator(
        task_id="multiply",
        python_callable=_multiply,
        op_kwargs={"factor": 6},
    )
```

An operator is instantiated, not called: `task_id` names it, and the rest are
its own arguments. `EmptyOperator` does nothing and is used as a marker or a
join point; `PythonOperator` calls the function you give it, with `op_kwargs`
as keyword arguments.

### Templates

The next operator is a `BashOperator`, and its command is not a plain string.
Some operator arguments — the ones the operator names in its `template_fields`,
`bash_command` among them — are **Jinja templates**. Jinja is a Python templating
library, the same one Flask and dbt use: a template is ordinary text with
placeholders, and *rendering* it replaces each `{{ expression }}` with that
expression's value. Inside the braces you can read an attribute
(`dag_run.run_after`), call a method (`ti.xcom_pull(...)`), or pass a value
through a **filter** with `|` — `default('x')` gives `'x'` when the value is
undefined. Airflow renders these arguments *just before the task instance
runs*, with that run's context as the variables. It is how one task, written
once, acts on a different run id, date or upstream value every time. Add it
below `multiply`:

```python dags/02_operators_and_xcoms.py
    describe = BashOperator(
        task_id="describe",
        bash_command=(
            'echo "run={{ run_id }} ds={{ ds | default(\'none\') }} '
            'run_after={{ dag_run.run_after }} '
            'product={{ ti.xcom_pull(task_ids=\'multiply\', key=\'product\') }}"'
        ),
    )
```

Three things to know about templates:

1. **Rendering happens per task instance**, in the task's own process, so a
   template can use anything in the task context — the same keys `**context`
   gave a `@task` in §4:

   - `run_id` — this run's id, e.g. `manual__2026-09-09T08:22:31+00:00`.
   - `dag_run` — the run itself: its `run_id`, `run_after` (when it was due to
     start), `conf` (the JSON passed with a trigger), `state`.
   - `ti` — this task instance, the object `_multiply` pushed and pulled XComs
     with.
   - `params` — the Dag's declared inputs (§4), with any per-run overrides.
   - `ds` — the run's logical date as `YYYY-MM-DD`. §7 explains logical dates;
     the paragraph on `| default(...)` after this list explains why `ds` needs
     a default.
   - `var` and `conn` — Airflow Variables and Connections, read by name:
     `{{ var.value.min_rows }}`, `{{ conn.warehouse.host }}`. *Hooks,
     Connections and Variables*, at the end of §6, covers what they hold.

   And the result is kept: it is the **Rendered Templates** tab on the task
   instance (§5), the place to look when a command did not say what you meant.

2. **Only `template_fields` are rendered.** Airflow does not scan every string
   in the Dag: it renders only the arguments the operator names in its
   `template_fields` (for `BashOperator`: `bash_command`, `env`, `cwd`). A
   string anywhere else — another argument, or a string written in the body
   of a `@task` function — is never rendered: Python knows nothing about Jinja and Airflow
   never looks at that string, so its braces (`{{ }}`) stay as literal text. A
   log showing `{{ … }}` unreplaced usually means exactly this.

3. **A `@task` function rarely needs this.** It receives the context as
   Python (`**context`, or a named argument like `ti`), so templating is mostly
   an operator concern. The arguments you pass when calling a `@task` are
   rendered, though: the `@task` decorator turns the function into an
   operator, stores those arguments in its `op_args` and `op_kwargs`
   attributes, and names both in its `template_fields`. If `show` is
   a `@task` function that prints its argument, `show("run={{ run_id }}")`
   prints the real run id.

The `| default(...)` on `ds` does in the template what `.get()` did in §4.
`ds` is derived from `logical_date`, and an asset-triggered run, or one
triggered from the CLI without a logical date, has none. Without the default,
the task fails while rendering the template, before it executes, with
`UndefinedError: 'ds' is undefined`. `airflow dags test` never shows this
failure, for the same reason it never shows the `KeyError` from §4: it always
gives the run a logical date. §7 explains why some runs have no logical date.

If all you need is a date for the run, to name a file for example, use
`{{ dag_run.run_after }}` instead of `ds`: every run has a `run_after`, so no
default is needed. It is not always the same date as the logical date; §7
explains when they differ.

A template field can also hold the name of a file instead of the template.
Each operator declares in `template_ext` the extensions it recognises —
`BashOperator` declares `(".sh", ".bash")` —, and when the value ends in one of
them, Airflow looks for that file next to the Dag file and renders its
contents. With `bash_command="describe.sh"`, the task runs `describe.sh` with
its `{{ run_id }}` already filled in. That way a long script or SQL query lives
in its own file, not in a string inside the Dag. A `@task` does the same with
two decorator arguments: `@task(templates_exts=[".sql"],
templates_dict={"query": "query.sql"})` passes the function, in its
`templates_dict` parameter, the rendered contents of `query.sql`.

Now a TaskFlow task in the same Dag, reading an operator's XCom:

```python dags/02_operators_and_xcoms.py
    @task
    def taskflow_reader(**context: Any) -> int:
        product = context["ti"].xcom_pull(task_ids="multiply", key="product")
        print(f"TaskFlow saw product={product}")
        return product
```

TaskFlow tasks and operators mix freely: a `@task` can read an operator's XCom
and vice versa, and the same `ti.xcom_pull` works from either side. For calling
a Python function, `@task` and `PythonOperator` are equivalent — the Airflow
docs present `@task` as `PythonOperator`'s TaskFlow equivalent, without
recommending one over the other —, and choosing is a matter of style:
`PythonOperator` takes the function and its arguments separately
(`python_callable`, `op_kwargs`, as in `push_seed` and `multiply`); with
`@task`, calling the function is creating the task.

### Relationships

Nothing above is connected yet. Finish the file with the remaining tasks and
the edges between them all:

```python dags/02_operators_and_xcoms.py
    chain(start, push_seed, Label("seed -> product"), multiply, describe)

    product = taskflow_reader()
    describe >> product

    check_parity = BashOperator(
        task_id="check_parity",
        bash_command="test $(( {{ ti.xcom_pull(task_ids='taskflow_reader') }} % 2 )) -eq 0 && echo even",
    )
    check_range = BashOperator(
        task_id="check_range",
        bash_command="test {{ ti.xcom_pull(task_ids='taskflow_reader') }} -lt 100 && echo 'under 100'",
    )
    product >> [check_parity, check_range] >> EmptyOperator(task_id="checks_done")

    publish = EmptyOperator(task_id="publish")
    notify = EmptyOperator(task_id="notify")
    cross_downstream([check_parity, check_range], [publish, notify])


operators_and_xcoms()
```

`check_parity` and `check_range` read the value `taskflow_reader` returned from
XCom and check it with `test`, which exits with an error when the condition is
false, and that fails the task.

An edge between two tasks is declared one of five ways, and this code uses the
last four:

1. **Implicitly, by passing one task's output to another.** §4's Dag was made
   of it: a `@task` called with another task's return value. An operator's
   output works too — `describe.output` passed as an argument would draw the
   same edge. Here `taskflow_reader` takes no argument, so its edge from
   `describe` has to be declared the second way.
2. **The shift operators**, when no data flows — `describe >> product` is
   "describe, then product". `>>` only sets the order: it passes nothing to the
   downstream task, which is why the checks read their value from XCom.
   `product << describe` is the same edge backwards, and so are
   `describe.set_downstream(product)` and `product.set_upstream(describe)`, the
   methods the operators call underneath.
3. **Fan-out and fan-in**: a list on one side of `>>` (one side, not both:
   see the fifth way) creates one edge per
   task in the list, so `product >> [check_parity, check_range] >> checks_done` is four
   dependencies, not two: `product` → `check_parity`, `product` → `check_range`,
   `check_parity` → `checks_done` and `check_range` → `checks_done`. Both checks
   start when `product` finishes, and `checks_done` waits for both to succeed.
4. **`chain()`** — the readable form of a long `>>` line. Its arguments are
   the tasks in order, so `chain(start, push_seed, Label("seed -> product"),
   multiply, describe)` is `start >> push_seed >> Label("seed -> product") >>
   multiply >> describe`. A `Label` is not a task: it goes between the two tasks
   whose edge it names — here `push_seed` → `multiply` — and that edge shows its
   text in the Graph view. `>>` takes a `Label` the same way.
5. **`cross_downstream()`**, for connecting every task in one list to every task
   in another — which looks like it should work with `>>` and does not:

   ```python
   [a, b] >> [c, d]              # TypeError: no operator between two lists
   chain([a, b], [c, d])         # legal, but PAIRS them: a->c and b->d only,
                                 # and raises if the lists differ in length
   cross_downstream([a, b], [c, d])   # what you actually wanted: all-to-all
   ```

`cross_downstream` is the Cartesian product — every task on the left upstream of
every task on the right. `chain_linear` does the same for more than two groups
in a row: `chain_linear([a, b], [c, d], [e, f])` connects every task in each
group to every task in the next.

### Run it and look at the graph

```bash
docker compose exec airflow-scheduler airflow dags test operators_and_xcoms
```

**Checkpoint.** In the output, look for the `describe` task's line,
`run=manual__… ds=… run_after=… product=42`, then `TaskFlow saw product=42`
from `taskflow_reader`, and `even` and `under 100` from the checks. Then open the Dag's **Graph**
view in the UI: the `seed -> product` label on one edge, a two-way fan-out into
`check_parity`/`check_range`, and those two fanning into `checks_done`,
`publish` and `notify`. If the graph does not match, compare with
[`solutions/02_operators_and_xcoms.py`](../../solutions/02_operators_and_xcoms.py).

### Writing your own operator

When no provider has an operator for a step, you can subclass `BaseOperator`
and write its `execute` method. Most of the time you do not need to: a `@task`
function does the same job with less code, and it can be shared just as well —
define it in a module, import it into every Dag that needs it, and pass
different arguments in each call. Its arguments are even rendered as
templates, as the third point under *Templates* says.

A class is worth its extra code when the step needs something a function does
not get:

- **Argument values checked when the file is parsed.** A `@task` call already
  fails at parse time when an argument is missing, but it cannot look at the
  values. An operator's `__init__` runs every time the operator is
  instantiated, and the Dag processor instantiates every operator in the Dag
  each time it parses the file. Since `__init__` can run any check, a wrong
  value shows up as an import error in the UI as soon as the Dag processor
  parses the file again (§3), not as a task failure hours later.
- **Methods Airflow calls while the task runs.** `on_kill` runs when the task
  is stopped (for example, when you mark the task instance as failed in the
  UI), so it can cancel the work the task started on another service, such as
  a BigQuery query. `self.defer(...)` suspends the task and hands the wait to
  the triggerer (§3); that is how the deferrable operators in §11 are built.
- **Buttons of its own in the UI.** `operator_extra_links`, a class
  attribute, adds an **Extra Links** section to the task instance view in the
  UI, with one button per link. Each link is a `BaseOperatorLink` subclass
  whose `get_link` method computes the URL for each task instance, usually the
  page of the job the task started on the service it talks to: the Databricks
  job, the BigQuery query. None of the classes underneath a `@task`
  (`_PythonDecoratedOperator`, `PythonOperator`, `BaseOperator`) accepts it as
  an argument — `@task(operator_extra_links=...)` fails with `TypeError` —, and
  neither does any operator from the providers installed in this stack. Buttons
  of your own take a class of your own.

Provider operators need these, which is why they are classes. The training
step below needs the first one. It takes a threshold, `min_f1`, and fails when
the model's f1 (its quality metric, from 0 to 1) falls below it. A threshold typed as `65` instead of `0.65` should
be caught as soon as the Dag processor parses the file, not after a training
run:

```python
from airflow.sdk import BaseOperator

from project_tagger import train as train_module


class ProjectTaggerTrainOperator(BaseOperator):
    template_fields = ("experiment_name",)

    def __init__(self, *, experiment_name: str, min_f1: float, **kwargs) -> None:
        super().__init__(**kwargs)          # task_id, retries, ... go to BaseOperator
        if not 0 < min_f1 <= 1:             # runs at parse time
            raise ValueError(f"min_f1 must be between 0 and 1, got {min_f1}")
        self.experiment_name = experiment_name
        self.min_f1 = min_f1

    def execute(self, context):
        print(f"experiment={self.experiment_name}")   # already rendered here
        _, metrics = train_module.train_from_files()
        if metrics["f1"] < self.min_f1:
            raise ValueError(f"f1={metrics['f1']:.4f} is below min_f1={self.min_f1}")
        return metrics["f1"]                # pushed as an XCom
```

A Dag instantiates it like any other operator:

```python
train = ProjectTaggerTrainOperator(
    task_id="train",
    experiment_name="project_tagger_{{ run_id }}",
    min_f1=0.65,
)
```

`__init__` runs when the Dag file is parsed: it checks `min_f1` and stores the
arguments. With `min_f1=65` the Dag does not load at all, and the UI shows the
import error `ValueError: min_f1 must be between 0 and 1, got 65`. `execute`
runs when the task instance runs, after Airflow has rendered every attribute
named in `template_fields` — so `self.experiment_name` holds
`project_tagger_manual__…`, not the braces. Whatever `execute` returns is pushed as an
XCom, exactly like a `@task` function's `return`.

`template_fields` is what links the argument to the rendering. It is a class
attribute that `BaseOperator` defines as an empty tuple and each operator
overrides, and it holds **attribute names**: `"experiment_name"` refers to
`self.experiment_name`, which is why `__init__` must store the argument under
exactly that name. Before calling `execute`, Airflow reads each listed
attribute from `self`, renders it, and writes the result back. It is declared
on the class rather than passed to `super().__init__()` because it describes
the operator type, the same for every instance; a subclass extends its
parent's list with `template_fields = (*Parent.template_fields, "new_field")`.
`min_f1` stays out of `template_fields` on purpose: a template is still
unrendered text at parse time, so `__init__` could not check it.

The same step as a `@task` can only run the check inside the function:

```python
@task
def train(experiment_name: str, min_f1: float) -> float:
    if not 0 < min_f1 <= 1:                       # runs only when the task runs
        raise ValueError(f"min_f1 must be between 0 and 1, got {min_f1}")
    print(f"experiment={experiment_name}")        # rendered before the call
    _, metrics = train_module.train_from_files()
    if metrics["f1"] < min_f1:
        raise ValueError(f"f1={metrics['f1']:.4f} is below min_f1={min_f1}")
    return metrics["f1"]

train(experiment_name="project_tagger_{{ run_id }}", min_f1=0.65)
```

With `min_f1=65` this Dag loads without errors, and the mistake shows up only
when a run reaches `train`. That difference is the whole reason for the class.
A step with no such need is better as a `@task`, which is what this repo uses
throughout.

> This holds for a value written in the Dag file. If the value arrives when
> the run is triggered, as a param (§4), it does not exist yet at parse time,
> and neither the class nor the `@task` can check it at parse time, only when
> the task runs. For the case where it
> needs validating, put the limit on the `Param` itself:
> `Param(0.65, type="number", exclusiveMinimum=0, maximum=1)` makes Airflow
> reject a trigger with `min_f1=65` before the run is created.

**Which class to subclass?** The one whose behaviour you want to keep. Airflow's
guide to custom operators starts from `BaseOperator`, which brings what all
tasks share (`task_id`, retries, templating) and leaves `execute` to you: it is
the starting point when your operator does something new, like
`ProjectTaggerTrainOperator`. But subclassing an existing operator is the norm when
you want what it does plus something: the same guide does it to add
`template_fields` to an existing operator, and most provider operators derive
from another operator, not from `BaseOperator` directly.

`PythonOperator`, for example, is the right parent when the job is calling a
function plus something extra: that is how `BranchPythonOperator`,
`ShortCircuitOperator`, `PythonVirtualenvOperator` and the class `@task`
creates, `_PythonDecoratedOperator`, are built — which is why the logs show
`<Task(_PythonDecoratedOperator): taskflow_reader>` even though the UI labels
it `@task`. For `ProjectTaggerTrainOperator` it is not: it writes its own `execute`,
so it would replace the one thing `PythonOperator` does, and still have to
take `python_callable`, which is required.

### Hooks, Connections and Variables

An operator is a whole task that does one fixed thing: `SQLExecuteQueryOperator`
runs a query, `SqlToS3Operator` copies a query's result into a file on S3. A
**hook** is the client underneath an operator. It takes a `conn_id` — the name
of a **Connection**, the credentials for a service that Airflow stores under
that name (explained further down) —, builds the authenticated client, and
gives you methods to call — `PostgresHook.get_df` runs a query and returns a
dataframe, `S3Hook.load_string` writes a string to S3. Every operator that
talks to a service outside Airflow (a database, S3, an API) does it through a
hook.

You call a hook yourself, inside a `@task`, when one task has to make several
calls with your own code in between — something no single operator does. None
of the tutorial's Dags needs one. The example assumes the projects live in a
Postgres database and the task exports a CSV to S3:

```python
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

@task
def export_labeled_projects(day: str) -> str:
    df = PostgresHook(postgres_conn_id="warehouse").get_df(
        "SELECT * FROM projects WHERE created_on::date = %(day)s",
        parameters={"day": day},
    )
    df = df[df["tag"].notna()]                  # your own code, between the two calls
    key = f"exports/labeled_projects_{day}.csv"
    S3Hook(aws_conn_id="aws_default").load_string(
        df.to_csv(index=False), key=key, bucket_name="project-tagger-exports"
    )
    return key
```

The task names two Connections, `warehouse` and `aws_default`, and never a
password. If the step were only "copy the query's result to S3",
`SqlToS3Operator` would do it as one task with no code; the hook version is for
when your own logic sits in the middle. A service with no hook can still use a
Connection: `BaseHook`, the class every hook derives from, has
`BaseHook.get_connection("warehouse")`, which returns the Connection itself —
host, login, password, extra — for you to pass to any client library.

A **Connection** is a named set of credentials for a service (`conn_type`,
host, login, password, extra). A **Variable** is a key-value pair global to the
whole installation: any task in any Dag reads it with
`Variable.get("min_rows", default=500)`, and it is for configuration you do not
want written into the code, such as a limit or a bucket name. Both are defined
in one of three ways:

- **In the UI** (Admin → Connections / Variables) **or with the CLI**
  (`airflow connections add`, `airflow variables set`). They are stored in the
  metadata database, in the `connection` and `variable` tables. Airflow
  encrypts the password, the extra and each Variable's value with the key in
  `[core] fernet_key`; with no key it stores them in plain text, which is what
  happens on this stack, since it sets none.
- **As an environment variable**, `AIRFLOW_CONN_<ID>` / `AIRFLOW_VAR_<KEY>`.
  These never go through the database and do not show in the UI. That is how
  the compose file defines `fs_default`, the Connection the `FileSensor` in §11
  uses.
- **In a secrets backend** (Vault, AWS Secrets Manager…), the usual choice in
  production (§15).

Templates read them as `{{ conn.warehouse.host }}` and `{{ var.value.min_rows }}`.

> A task fetching either one does not touch the metadata database. It first
> looks at the `AIRFLOW_CONN_*` / `AIRFLOW_VAR_*` environment variables — that
> is how it finds `fs_default` without asking anyone — and, if it is not there,
> asks the API server over the Task Execution API, which is what lets a remote
> worker resolve a Connection without access to the metadata database.
>
> Prefer a **param** (§4) over a global Variable when the value is a setting
> that changes what the Dag decides, such as a threshold: params are recorded
> with the run that used them, Variables are not.

---

← [5. The UI](05-the-ui.md) · [Index](README.md) · [7. Scheduling and logical dates](07-scheduling-and-logical-dates.md) →
