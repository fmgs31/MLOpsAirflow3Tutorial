## 6. Operators, XComs and relationships

Lesson 1 used `@task`-decorated functions. This one uses **operators**, the
pre-built task types, and shows that the two styles mix freely in one Dag. The
common ones ship in `apache-airflow-providers-standard`, which is already
installed:

```python
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
```

Beyond the standard ones there is a provider for nearly every system you would
want to talk to — S3, Snowflake, BigQuery, Postgres, Kubernetes, Databricks,
Slack, HTTP, Docker, dbt Cloud, Airbyte — each shipping operators, hooks and
sensors, installed as `apache-airflow-providers-<name>` and listed in the UI
under Admin → Providers. Reach for one before writing HTTP calls by hand: a
provider's operator already handles connections, retries and secret lookup.

When none fits, subclass `BaseOperator` — not in this lesson, but this is the
shape:

```python
from airflow.sdk import BaseOperator

class TagifaiTrainOperator(BaseOperator):
    template_fields = ("experiment_name",)      # these get Jinja-rendered

    def __init__(self, *, experiment_name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.experiment_name = experiment_name

    def execute(self, context):                 # return value becomes the XCom
        return train_module.train_from_files()[1]["f1"]
```

That is the right move when a step is reused across Dags and needs its own
parameters, retries or template fields. For everything else `@task` is less code
and this repo uses it throughout.

### Build it: XComs first

The Dag you are about to write, `dags/02_operators_and_xcoms.py`, pushes a
number through five tasks of three different kinds. The thing it teaches is how
tasks hand values to each other.

**XComs** ("cross-communications") are how tasks pass small values. They are
stored in the metadata database — and, as everything a task does, a task reads
and writes them through the Task Execution API, never by touching the database
itself. TaskFlow used them implicitly in lesson 1: every `return` was an XCom
push, every argument an XCom pull. With operators you push and pull explicitly.

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
the same name-based injection `**context` did in lesson 1. Two things to
notice:

**`xcom_pull` without `task_ids` reads the current task's own XComs** and
quietly returns `None`. Almost every pull you write wants `task_ids=`; the
`_multiply` above names its source.

> **XComs are for metadata, not payloads.** They live in the metadata database.
> Pass a path, a run id, a row count; put the dataframe in object storage and
> the model in a registry. Values must be JSON-serialisable.

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
Some operator arguments — `bash_command`, and any field an operator names in
its `template_fields` — are **Jinja templates**: strings in which `{{ … }}` is
filled in *just before the task instance runs*, from that run's context. It is
how one task, written once, acts on a different run id, date or upstream value
every time. Add it below `multiply`:

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

Three consequences. Rendering happens per task instance, in the task's own
process, so a template can reach anything in the task context — `run_id`,
`ds`, `ti`, `params`, `var`, `conn`, `dag_run` — and the result is kept: it is
the **Rendered Templates** tab on the task instance (§5), the place to look
when a command did not say what you meant. A string that is *not* in
`template_fields` is never rendered, however many braces it contains. And a
`@task` function needs none of this — it receives the context as Python
(`**context`, or a named argument like `ti`) — so templating is mostly an
operator concern.

The `| default(...)` on `ds` is the template-side twin of `.get()` in lesson
1: `ds` is derived from `logical_date`, and a manual or asset-triggered run has
none. Without the default the task fails with `UndefinedError: 'ds' is
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
four ways, and this Dag uses all of them. Finish the file with the wiring:

```python dags/02_operators_and_xcoms.py
    chain(start, push_seed, Label("seed -> product"), multiply, describe)

    product = taskflow_reader()
    describe >> product

    check_parity = BashOperator(task_id="check_parity", bash_command='echo "parity checked"')
    check_range = BashOperator(task_id="check_range", bash_command='echo "range checked"')
    product >> [check_parity, check_range] >> EmptyOperator(task_id="checks_done")

    publish = EmptyOperator(task_id="publish")
    notify = EmptyOperator(task_id="notify")
    cross_downstream([check_parity, check_range], [publish, notify])


operators_and_xcoms()
```

1. **Implicitly, by passing one task's output to another.** TaskFlow only, and
   lesson 1 was made of it. Here `taskflow_reader` takes no argument, so
   nothing draws its edge for it.
2. **The shift operators**, when no data flows — `describe >> product` is
   "describe, then product". `product << describe` is the same edge backwards,
   and so are `describe.set_downstream(product)` and
   `product.set_upstream(describe)`, the methods the operators call underneath;
   use those when the tasks are built in a loop and `>>` reads badly.
3. **Fan-out and fan-in**: a list on either side of `>>` broadcasts the edge,
   so `product >> [check_parity, check_range] >> checks_done` is four
   dependencies, not two.
4. **`chain()`** — the readable form of a long `>>` line, and the only one that
   can carry a `Label`, which annotates the edge in the Graph view.

And a fifth, `cross_downstream`, for connecting every task in one list to every
task in another — which looks like it should work with `>>` and does not:

```python
[a, b] >> [c, d]              # TypeError: no operator between two lists
chain([a, b], [c, d])         # legal, but PAIRS them: a->c and b->d only,
                              # and raises if the lists differ in length
cross_downstream([a, b], [c, d])   # what you actually wanted: all-to-all
```

`cross_downstream` is the Cartesian product — every task on the left upstream of
every task on the right. `chain_linear` is its sibling for chaining several such
groups in sequence.

### Run it and look at the graph

```bash
docker compose exec airflow-scheduler airflow dags test operators_and_xcoms
```

**Checkpoint.** The `describe` task's line, `run=manual__… ds=… product=42`,
then `TaskFlow saw product=42` from the reader. Then open the Dag's **Graph**
view in the UI: the `seed -> product` label on one edge, a two-way fan-out into
`check_parity`/`check_range`, and those two fanning into both `publish` and
`notify`. If the graph does not match, compare with
[`solutions/02_operators_and_xcoms.py`](../../solutions/02_operators_and_xcoms.py).

### Hooks, Connections and Variables

An operator is the *task*; a **hook** is the client underneath it. Hooks take a
`conn_id`, resolve the Connection, build the authenticated client and expose
methods — `S3Hook.load_file`, `PostgresHook.get_pandas_df`. Use one directly
inside a `@task` when you want the connection handling but not a whole
operator. None of the tutorial's Dags needs one; the shape:

```python
from airflow.sdk import BaseHook, Connection, Variable
from airflow.providers.standard.hooks.filesystem import FSHook

@task
def land_file() -> str:
    base = FSHook(fs_conn_id="fs_default").get_path()       # "/" here
    threshold = Variable.get("min_rows", default=500)
    conn = BaseHook.get_connection("fs_default")            # or Connection.get(...)
    ...
```

**Connections** hold credentials (`conn_type`, host, login, password, extra);
**Variables** hold configuration. Both are editable under Admin, settable from
the CLI, and overridable by an `AIRFLOW_CONN_*` / `AIRFLOW_VAR_*` environment
variable — which is how `fs_default` is defined in this repo's compose file.
Templates reach them as `{{ conn.fs_default.host }}` and `{{ var.value.my_key }}`.

> A task fetching either one does not touch the metadata database: the request
> goes over the Task Execution API, which is what lets a remote worker resolve a
> Connection it has no database access to.
>
> Prefer a **param** (§5) over a global Variable when the value is a setting
> that changes what the Dag decides, such as a threshold: params are recorded
> with the run that used them, Variables are not.

---

← [5. The UI](05-the-ui.md) · [Index](README.md) · [7. Scheduling and logical dates](07-scheduling-and-logical-dates.md) →
