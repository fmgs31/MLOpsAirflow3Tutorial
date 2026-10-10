## 6. Operators, XComs and relationships

§4's Dag used `@task`-decorated functions. This one uses **operators**, the
pre-built task types, and shows that the two styles mix freely in one Dag. Both
styles produce **tasks**: a task is any step in a Dag, and a *TaskFlow task* is
one written as a `@task` function instead of as an operator. The common
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
the Task Execution API (§3). TaskFlow used them implicitly in §4: every
`return` was an XCom push, every argument an XCom pull. With operators you push and pull explicitly.

Create `dags/02_operators_and_xcoms.py` with the imports and two plain Python
functions that a `PythonOperator` will call:

```python dags/02_operators_and_xcoms.py
"""Lesson 2 - operators, XComs, and relationships."""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Label, chain, cross_downstream, dag, task

DEFAULT_ARGS = {
    "owner": "mlops-course",
    "retries": 1,
    "retry_delay": pendulum.duration(seconds=30),
}


def _push_seed(ti) -> None:
    ti.xcom_push(key="seed", value=7)


def _multiply(ti, factor: int) -> None:
    seed = ti.xcom_pull(task_ids="push_seed", key="seed")
    ti.xcom_push(key="product", value=seed * factor)
```

`ti` — the task instance — is injected because the parameter is named `ti`,
the same name-based injection `**context` did in §4. Two things to
notice:

**`xcom_pull` without `task_ids` reads the current task's own XComs** and
quietly returns `None`. Almost every pull you write needs `task_ids=`; the
`_multiply` above names its source.

**XComs are for metadata, not payloads.** They live in the metadata database.
Pass a path, a run id, a row count; put the dataframe in object storage and
the model in a registry. Values must be JSON-serialisable.

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
            'echo "run={{ run_id }} ds={{ ds | default(\'none (manual run)\') }} '
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
   rendered, though: TaskFlow hands them to the task through template fields,
   so `show("run={{ run_id }}")` receives the real run id.

The `| default(...)` on `ds` is the template equivalent of `.get()` in §4:
`ds` is derived from `logical_date`, and an asset-triggered run, or one
triggered from the CLI without a logical date, has none. Without the default the task fails with `UndefinedError: 'ds' is
undefined` while rendering, before it executes — and, as with `KeyError`,
`airflow dags test` never shows it. §7 is where both traps are explained;
`{{ dag_run.run_after }}` is the safe alternative, because every run has one.

Now a TaskFlow task in the same Dag, reading an operator's XCom:

```python dags/02_operators_and_xcoms.py
    @task
    def taskflow_reader(**context) -> int:
        product = context["ti"].xcom_pull(task_ids="multiply", key="product")
        print(f"TaskFlow saw product={product}")
        return product
```

TaskFlow tasks and operators mix freely: a `@task` can read an operator's XCom
and vice versa, and the same `ti.xcom_pull` works from either side.

### Relationships

Nothing above is connected yet. An edge between two tasks is declared one of
five ways, and this Dag uses the last four. Finish the file with the wiring:

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

1. **Implicitly, by passing one task's output to another.** §4's Dag was made
   of it: a `@task` called with another task's return value. An operator's
   output works too — `describe.output` passed as an argument would draw the
   same edge. Here `taskflow_reader` takes no argument, so nothing draws its
   edge for it.
2. **The shift operators**, when no data flows — `describe >> product` is
   "describe, then product". `>>` only sets the order: it passes nothing to the
   downstream task, which is why the checks read their value from XCom.
   `product << describe` is the same edge backwards, and so are
   `describe.set_downstream(product)` and `product.set_upstream(describe)`, the
   methods the operators call underneath.
3. **Fan-out and fan-in**: a list on either side of `>>` creates one edge per
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

**Checkpoint.** The `describe` task's line, `run=manual__… ds=… run_after=… product=42`,
then `TaskFlow saw product=42` from the reader, and `even` and `under 100` from
the checks. Then open the Dag's **Graph**
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

- **Arguments checked when the file is parsed.** `__init__` runs at parse
  time, so a missing or invalid argument shows up as an import error in the UI
  as soon as the file is saved, not as a task failure hours later.
- **Hooks into the task's lifecycle.** `on_kill` runs when the task is
  stopped, so it can cancel the job the task started on an external system;
  `self.defer(...)` hands the wait to the triggerer, which is how the
  deferrable operators in §11 are built.
- **Settings Airflow and the UI read from the class.** `template_ext` makes
  Airflow render a file named in an argument — a `.sql` file, say — and
  `operator_extra_links` adds a button that opens the run in the external
  system.

That is why every provider's operator is a class. The training step below
needs none of those; it shows the shape of an operator, using three things
this Dag already showed: arguments given when the operator is instantiated,
`template_fields`, and a returned value becoming an XCom.

```python
from airflow.sdk import BaseOperator

from tagifai import train as train_module


class TagifaiTrainOperator(BaseOperator):
    template_fields = ("experiment_name",)

    def __init__(self, *, experiment_name: str, **kwargs) -> None:
        super().__init__(**kwargs)          # task_id, retries, ... go to BaseOperator
        self.experiment_name = experiment_name

    def execute(self, context):
        print(f"experiment={self.experiment_name}")   # already rendered here
        _, metrics = train_module.train_from_files()
        return metrics["f1"]                # pushed as an XCom
```

A Dag instantiates it like any other operator:

```python
train = TagifaiTrainOperator(task_id="train", experiment_name="tagifai_{{ run_id }}")
```

`__init__` runs when the Dag file is parsed and only stores the arguments.
`execute` runs when the task instance runs, after Airflow has rendered every
attribute named in `template_fields` — so `self.experiment_name` holds
`tagifai_manual__…`, not the braces. Whatever `execute` returns is pushed as an
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

The same step as a `@task`:

```python
@task
def train(experiment_name: str) -> float:
    print(f"experiment={experiment_name}")        # rendered before the call
    _, metrics = train_module.train_from_files()
    return metrics["f1"]

train(experiment_name="tagifai_{{ run_id }}")
```

Both give one task with the same rendered name and the same XCom, and for this
step the `@task` is the better choice — it is what this repo uses throughout.

**Why `BaseOperator` and not `PythonOperator`?** `BaseOperator` is the class
every operator derives from: it holds what all tasks share (`task_id`,
retries, templating) and leaves `execute` for you to write. `PythonOperator`
is one of its subclasses, and its whole job is to call the function you hand
it as `python_callable`. A subclass of it would carry that machinery without
using it — and when calling a Python function is all you want, `@task`
already is that: under the hood a `@task` is a `PythonOperator` subclass,
which is why the logs call `taskflow_reader` a `_PythonDecoratedOperator`.

### Hooks, Connections and Variables

An operator is a whole task that does one fixed thing: `SQLExecuteQueryOperator`
runs a query, `SqlToS3Operator` copies a query's result into a file on S3. A
**hook** is the client underneath an operator. It takes a `conn_id`, looks up
that Connection, builds the authenticated client, and gives you methods to call
— `PostgresHook.get_df` runs a query and returns a dataframe,
`S3Hook.load_string` writes a string to S3. Every operator that talks to an
outside system does it through a hook.

You call a hook yourself, inside a `@task`, when one task has to make several
calls with your own code in between — something no single operator does. None
of the tutorial's Dags needs one, so here is what it would look like with the
warehouse on Postgres and the exports on S3:

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
        df.to_csv(index=False), key=key, bucket_name="tagifai-exports"
    )
    return key
```

The task names two Connections, `warehouse` and `aws_default`, and never a
password. If the step were only "copy the query's result to S3",
`SqlToS3Operator` would do it as one task with no code; the hook version is for
when your own logic sits in the middle. A system with no hook can still use a
Connection: `BaseHook`, the class every hook derives from, has
`BaseHook.get_connection("warehouse")`, which returns the Connection itself —
host, login, password, extra — for you to pass to any client library.

**Connections** hold credentials (`conn_type`, host, login, password, extra);
**Variables** hold configuration, and a task reads one with
`Variable.get("min_rows", default=500)`. Both are editable under Admin, settable from
the CLI, and overridable by an `AIRFLOW_CONN_*` / `AIRFLOW_VAR_*` environment
variable — which is how this repo's compose file defines `fs_default`, the
Connection the `FileSensor` in §11 uses.
Templates read them as `{{ conn.warehouse.host }}` and `{{ var.value.min_rows }}`.

> A task fetching either one does not touch the metadata database: the request
> goes over the Task Execution API, which is what lets a remote worker resolve a
> Connection without access to the metadata database.
>
> Prefer a **param** (§4) over a global Variable when the value is a setting
> that changes what the Dag decides, such as a threshold: params are recorded
> with the run that used them, Variables are not.

---

← [5. The UI](05-the-ui.md) · [Index](README.md) · [7. Scheduling and logical dates](07-scheduling-and-logical-dates.md) →
