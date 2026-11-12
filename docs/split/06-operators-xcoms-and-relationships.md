## 6. Operators, XComs and relationships

§4's Dag used `@task` functions; this one adds **operators**, the pre-built
task types, and mixes both styles in one Dag. Either way you get **tasks**, the
steps of a Dag; one written as a `@task` function is a *TaskFlow task* (Airflow
turns it into an operator underneath, as *Which class to subclass?* explains).
The common operators ship in `apache-airflow-providers-standard`, already
installed:

```python
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
```

Beyond these, nearly every system has a provider — S3, Snowflake, BigQuery,
Postgres, Kubernetes, Databricks, Slack, HTTP, Docker, dbt Cloud, Airbyte —
installed as `apache-airflow-providers-<name>`, listed in the UI under Admin →
Providers, and shipping operators, hooks (the clients underneath them, *Hooks,
Connections and Variables*) and sensors (tasks that wait, §11). Prefer one to
hand-written HTTP calls: its operators already handle connections, retries and
secret lookup. When none fits, write your own (*Writing your own operator*).

### Build it: XComs first

The Dag, `dags/02_operators_and_xcoms.py`, wires ten tasks of four kinds —
`EmptyOperator`, `PythonOperator`, `BashOperator` and TaskFlow `@task` — and
passes a number between them to show how tasks hand values to each other.

**XComs** ("cross-communications") are how tasks pass small values. They live
in the Metadata database, but a task never touches it directly: it reads and
writes them through the API server's Task Execution API (§3). §4's `@task`
functions used them out of sight — every `return` an XCom push, every argument
a pull. With operators you push and pull explicitly.

Create the file with the imports and two plain Python functions for the
`PythonOperator`s to call:

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

`ti`, the task instance, is injected by parameter name, as `**context` was in
§4; its type, `RuntimeTaskInstanceProtocol` from `airflow.sdk.types`, has
`xcom_push` and `xcom_pull` among its methods. Two things to notice:

**`xcom_pull` without `task_ids` reads the current task's own XComs** and
quietly returns `None`. Almost every pull you write needs `task_ids=`;
`_multiply` names its source.

**XComs are for metadata, not payloads**: every value lands in the Metadata
database. Pass a path, a run id, a row count; put the dataframe in object
storage and the model in a registry. Airflow serialises more than JSON — a
`datetime`, a `set`, even a dataframe —, but that a value fits does not mean it
belongs there.

**Every task must be safe to run twice.** With `"retries": 1` in
`DEFAULT_ARGS`, a failed task runs again, so each task must be **idempotent**:
running it twice leaves the same result as running it once. One that appends
rows, increments a counter or sends an email on every try is not: a retry
duplicates the rows, the count or the email. The usual ways to get there:

- **Overwrite, do not append.** Replace the whole table or file, or delete what
  you are about to write first.
- **Name outputs after the run.** A file whose path contains the run id is the
  same file on every try.
- **Create only if missing.** `CREATE TABLE IF NOT EXISTS`, for example, or the
  compose file's `extra-databases` service (§2), which creates its database
  only if it is not there yet.

§9 and §10 point out where their tasks do each of these.

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

An operator is instantiated, not called: `task_id` names it, the rest are its
own arguments. `EmptyOperator` does nothing and serves as a marker or a join
point; `PythonOperator` calls the function you give it, with `op_kwargs` as
keyword arguments.

### Templates

The next operator, a `BashOperator`, takes a command that is not a plain
string. The arguments an operator lists in its `template_fields`,
`bash_command` among them, are **Jinja templates**. Jinja is the Python
templating library Flask and dbt use: a template is text with placeholders, and
*rendering* it replaces each `{{ expression }}` with its value. Inside the
braces you can read an attribute (`dag_run.run_after`), call a method
(`ti.xcom_pull(...)`), or pass a value through a **filter** with `|` —
`default('x')` gives `'x'` when the value is undefined. Airflow renders these
arguments *just before the task instance runs*, with the run's context as
variables, so one task, written once, acts on each run's own id, date or
upstream values. Add it below `multiply`:

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
   - `ds` — the run's logical date (§7) as `YYYY-MM-DD`; the paragraph on
     `| default(...)` after this list explains why it needs a default.
   - `var` and `conn` — Airflow Variables and Connections, read by name:
     `{{ var.value.min_rows }}`, `{{ conn.warehouse.host }}`. *Hooks,
     Connections and Variables* covers what they hold.

   The result is kept in the task instance's **Rendered Templates** tab (§5):
   look there when a command did not say what you meant.

2. **Only `template_fields` are rendered** (for `BashOperator`:
   `bash_command`, `env`, `cwd`). Any other string — another argument, or one
   in the body of a `@task` function — keeps its braces (`{{ }}`) as literal
   text: Airflow never looks at it, and Python knows nothing about Jinja. A log
   showing `{{ … }}` unreplaced usually means this.

3. **A `@task` function rarely needs this**: it gets the context as Python
   (`**context`, or a named argument like `ti`). Its call arguments are
   rendered, though: `@task` turns the function into an operator that stores
   them in its `op_args` and `op_kwargs` attributes, both named in its
   `template_fields`. If `show` is a `@task` function that prints its
   argument, `show("run={{ run_id }}")` prints the real run id.

The `| default(...)` on `ds` is the template's version of §4's `.get()`. `ds`
derives from `logical_date`, which an asset-triggered run, or one triggered
from the CLI without a logical date, does not have (§7 explains why). Without
the default, the task fails while rendering, before it executes, with
`UndefinedError: 'ds' is undefined`, which `airflow dags test` never shows
(§4).

If all you need is a date for the run, to name a file for example, use
`{{ dag_run.run_after }}`: every run has one, so no default is needed. It is
not always the logical date; §7 explains when they differ.

A template field can also hold a file name, so a long script or SQL query lives
in its own file instead of a string in the Dag. When the value ends in an
extension the operator lists in `template_ext` — `(".sh", ".bash")` for
`BashOperator` —, Airflow renders that file, found next to the Dag file:
`bash_command="describe.sh"` runs `describe.sh` with its `{{ run_id }}` filled
in. A `@task` does the same with two decorator arguments:
`@task(templates_exts=[".sql"], templates_dict={"query": "query.sql"})` passes
the rendered contents of `query.sql` to the function's `templates_dict`
parameter.

Now a TaskFlow task in the same Dag, reading an operator's XCom:

```python dags/02_operators_and_xcoms.py
    @task
    def taskflow_reader(**context: Any) -> int:
        product = context["ti"].xcom_pull(task_ids="multiply", key="product")
        print(f"TaskFlow saw product={product}")
        return product
```

The same `ti.xcom_pull` works from either side: a `@task` reads an operator's
XCom and vice versa. For calling a Python function, `@task` and
`PythonOperator` are equivalent — the Airflow docs present `@task` as
`PythonOperator`'s TaskFlow equivalent and recommend neither — so the choice is
style: `PythonOperator` takes the function and its arguments separately
(`python_callable`, `op_kwargs`, as in `push_seed` and `multiply`); with
`@task`, calling the function creates the task.

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

`check_parity` and `check_range` pull `taskflow_reader`'s return value from
XCom and check it with `test`, which exits with an error when the condition is
false, failing the task.

An edge between two tasks is declared one of five ways, and this code uses the
last four:

1. **Implicitly, by passing one task's output to another**, as §4's Dag did
   with `@task` return values. An operator's output works too: passing
   `describe.output` as an argument would draw the same edge. `taskflow_reader`
   takes no argument, so its edge from `describe` needs the second way.
2. **The shift operators**, when no data flows: `describe >> product` is
   "describe, then product". `>>` only sets the order and passes nothing
   downstream, which is why the checks read their value from XCom.
   `product << describe` is the same edge backwards, and so are
   `describe.set_downstream(product)` and `product.set_upstream(describe)`, the
   methods `>>` and `<<` call underneath.
3. **Fan-out and fan-in**: a list on one side of `>>` (only one side; see the
   fifth way) creates one edge per task in it, so
   `product >> [check_parity, check_range] >> checks_done` is four
   dependencies, not two: `product` → `check_parity`, `product` → `check_range`,
   `check_parity` → `checks_done` and `check_range` → `checks_done`. Both checks
   start when `product` finishes; `checks_done` waits for both to succeed.
4. **`chain()`**, the readable form of a long `>>` line:
   `chain(start, push_seed, Label("seed -> product"), multiply, describe)` is
   `start >> push_seed >> Label("seed -> product") >> multiply >> describe`. A
   `Label` is not a task: placed between two tasks (here `push_seed` and
   `multiply`), it shows its text on their edge in the Graph view, in `chain()`
   and `>>` alike.
5. **`cross_downstream()`**, to connect every task in one list to every task in
   another, which `>>` looks like it should do and does not:

   ```python
   [a, b] >> [c, d]              # TypeError: no operator between two lists
   chain([a, b], [c, d])         # legal, but PAIRS them: a->c and b->d only,
                                 # and raises if the lists differ in length
   cross_downstream([a, b], [c, d])   # what you actually wanted: all-to-all
   ```

`chain_linear` does the same as `cross_downstream` for more than two groups in
a row: `chain_linear([a, b], [c, d], [e, f])` connects every task in each group
to every task in the next.

### Run it and look at the graph

```bash
docker compose exec airflow-scheduler airflow dags test operators_and_xcoms
```

**Checkpoint.** In the output, look for the `describe` line,
`run=manual__… ds=… run_after=… product=42`, then `TaskFlow saw product=42`
from `taskflow_reader`, and `even` and `under 100` from the checks. Then open
the Dag's **Graph** view in the UI: the `seed -> product` label on one edge, a
two-way fan-out into `check_parity`/`check_range`, and those two fanning into
`checks_done`, `publish` and `notify`. If the graph does not match, compare
with [`solutions/02_operators_and_xcoms.py`](../../solutions/02_operators_and_xcoms.py).

### Writing your own operator

When no provider has an operator for a step, you can subclass `BaseOperator`
and write its `execute` method. Usually you do not need to: a `@task` function
does the same job with less code and shares just as well — define it in a
module, import it into every Dag that needs it, and call it with different
arguments, which are even rendered as templates (the third point under
*Templates*).

A class is worth its extra code when the step needs something a function does
not get:

- **Argument values checked when the file is parsed.** A `@task` call fails at
  parse time if an argument is missing, but cannot check the values. An
  operator's `__init__` runs every time the Dag processor parses the file
  (§3), since parsing instantiates every operator, and it can run any check: a
  wrong value shows up as an import error in the UI right away, not as a task
  failure hours later.
- **Methods Airflow calls while the task runs.** `on_kill` runs when the task
  is stopped (for example, when you mark the task instance as failed in the
  UI), so it can cancel work the task started on another service, such as a
  BigQuery query. `self.defer(...)` suspends the task and hands the wait to the
  Triggerer (§3); that is how §11's deferrable operators are built.
- **Buttons of its own in the UI.** The class attribute `operator_extra_links`
  adds an **Extra Links** section to the task instance view, one button per
  link. Each link is a `BaseOperatorLink` subclass whose `get_link` method
  computes the URL per task instance, usually the page of the job the task
  started on the service it talks to: the Databricks job, the BigQuery query.
  `@task` has no argument for it: `@task(operator_extra_links=...)` fails with
  `TypeError` (§10 shows the workaround).

Provider operators need these, which is why they are classes. The training
step below needs the first: it fails when the model's f1 (its quality metric,
from 0 to 1) falls below a threshold, `min_f1`, and a `min_f1` typed as `65`
instead of `0.65` should be caught when the Dag processor parses the file, not
after a training run:

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

`__init__` runs when the Dag file is parsed, checking `min_f1` and storing the
arguments: with `min_f1=65` the Dag does not load at all, and the UI shows the
import error `ValueError: min_f1 must be between 0 and 1, got 65`. `execute`
runs when the task instance runs, after Airflow has rendered every attribute
named in `template_fields`, so `self.experiment_name` holds
`project_tagger_manual__…`, not the braces. Whatever `execute` returns is
pushed as an XCom, like a `@task` function's `return`.

`template_fields` links the argument to the rendering. It is a class attribute,
an empty tuple on `BaseOperator` that each operator overrides, and it holds
**attribute names**: `"experiment_name"` means `self.experiment_name`, so
`__init__` must store the argument under exactly that name. Before calling
`execute`, Airflow reads each listed attribute from `self`, renders it and
writes the result back. It is declared on the class, not passed to
`super().__init__()`, because it describes the operator type, the same for
every instance; a subclass extends its parent's list with
`template_fields = (*Parent.template_fields, "new_field")`. `min_f1` stays out
on purpose: at parse time a template is still unrendered text, which
`__init__` could not check.

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
when a run reaches `train`.

> This holds for a value written in the Dag file. A param (§4) arrives when the
> run is triggered, after parsing, so neither the class nor the `@task` can
> check it before the task runs. To validate it earlier, put the limit on the
> `Param` itself: `Param(0.65, type="number", exclusiveMinimum=0, maximum=1)`
> makes Airflow reject a trigger with `min_f1=65` before the run is created.

**Which class to subclass?** The one whose behaviour you want to keep.
`BaseOperator` brings what all tasks share (`task_id`, retries, templating) and
leaves `execute` to you: it is the parent for an operator that does something
new, like `ProjectTaggerTrainOperator`. To get what an existing operator does
plus something, subclass that operator instead, which is the norm: most
provider operators derive from another operator, not from `BaseOperator`
directly.

`PythonOperator`, for example, is the parent for calling a function plus
something extra: `BranchPythonOperator`, `ShortCircuitOperator`,
`PythonVirtualenvOperator` and `_PythonDecoratedOperator`, the class `@task`
creates, are built on it — which is why the logs show
`<Task(_PythonDecoratedOperator): taskflow_reader>` though the UI labels it
`@task`. It is the wrong parent for `ProjectTaggerTrainOperator`, whose own
`execute` would replace the one thing `PythonOperator` does, while still having
to take the required `python_callable`.

### Hooks, Connections and Variables

An operator is a whole task that does one fixed thing: `SQLExecuteQueryOperator`
runs a query, `SqlToS3Operator` copies a query's result into a file on S3. A
**hook** is the client underneath, and every operator that talks to a service
outside Airflow (a database, S3, an API) goes through one. It takes a
`conn_id` — the name of a **Connection**, a service's credentials that Airflow
stores under that name (explained below) —, builds the authenticated client,
and gives you methods to call: `PostgresHook.get_df` runs a query and returns a
dataframe, `S3Hook.load_string` writes a string to S3.

You call a hook yourself, inside a `@task`, when one task has to make several
calls with your own code in between, which no single operator does. None of
the tutorial's Dags needs one; this example assumes the projects live in a
Postgres database and exports a CSV to S3:

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

The task names two Connections, `warehouse` and `aws_default`, never a
password. A service with no hook can still use a
Connection: `BaseHook.get_connection("warehouse")`, on the class every hook
derives from, returns the Connection itself — host, login, password, extra —
for you to pass to any client library.

A **Connection** is a named set of credentials for a service (`conn_type`,
host, login, password, extra). A **Variable** is a key-value pair global to the
whole installation, for configuration you do not want written into the code,
such as a limit or a bucket name; any task in any Dag reads it with
`Variable.get("min_rows", default=500)`. Both are defined in one of three ways:

- **In the UI** (Admin → Connections / Variables) **or with the CLI**
  (`airflow connections add`, `airflow variables set`). They are stored in the
  Metadata database's `connection` and `variable` tables, with the password,
  the extra and each Variable's value encrypted by the key in
  `[core] fernet_key`; with no key, as on this stack, they are stored in plain
  text.
- **As an environment variable**, `AIRFLOW_CONN_<ID>` / `AIRFLOW_VAR_<KEY>`,
  never stored in the database or shown in the UI. That is how the compose
  file defines `google_cloud_default`, the Connection §11's BigQuery sensor
  uses, and `fs_default`, the one a `FileSensor` uses by default.
- **In a secrets backend** (Vault, AWS Secrets Manager…), the usual choice in
  production (§16).

Templates read them as `{{ conn.warehouse.host }}` and `{{ var.value.min_rows }}`.

> A task fetching either one never touches the Metadata database. It first
> checks the `AIRFLOW_CONN_*` / `AIRFLOW_VAR_*` environment variables, which is
> where it finds `google_cloud_default`, and otherwise asks the API server over
> the Task Execution API, which lets a remote worker resolve a Connection
> without access to the Metadata database.
>
> Prefer a **param** (§4) over a global Variable when the value is a setting
> that changes what the Dag decides, such as a threshold: params are recorded
> with the run that used them, Variables are not.

---

← [5. The UI](05-the-ui.md) · [Index](README.md) · [7. Scheduling and logical dates](07-scheduling-and-logical-dates.md) →
