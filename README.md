# Orchestration for MLOps — with Apache Airflow 3

An updated, runnable version of the [Made With ML orchestration
lesson](https://madewithml.com/courses/mlops/orchestration/), rewritten for
**Airflow 3.3.1**. The original targets Airflow 2.3.3, and enough has changed
since that most of its code no longer imports, let alone runs: `schedule_interval`,
`days_ago`, `airflow.operators.*`, `airflow webserver`, `airflow db init`,
Datasets, SLAs and SubDAGs are all gone or moved.

The lesson structure is kept — DataOps, then MLOps, then continual learning —
but the pipeline runs end to end on your machine with no Airbyte, dbt Cloud,
BigQuery or GCP account.

Every command and Dag in this repo was executed against a real Airflow 3.3.1
stack; see [What was actually verified](#what-was-actually-verified).

---

## Contents

1. [Why orchestrate at all](#1-why-orchestrate-at-all)
2. [Setup](#2-setup)
3. [How Airflow 3 is put together](#3-how-airflow-3-is-put-together)
4. [The UI](#4-the-ui)
5. [Dags and tasks](#5-dags-and-tasks)
6. [Operators, XComs and relationships](#6-operators-xcoms-and-relationships)
7. [Scheduling, logical dates and backfills](#7-scheduling-logical-dates-and-backfills)
8. [Assets: scheduling on data, not on time](#8-assets-scheduling-on-data-not-on-time)
9. [The DataOps Dag](#9-the-dataops-dag)
10. [The MLOps Dag](#10-the-mlops-dag)
11. [Waiting: sensors, deferrable tasks, events](#11-waiting-sensors-deferrable-tasks-events)
12. [Continual learning: monitor, decide, act](#12-continual-learning-monitor-decide-act)
13. [Deadline alerts (SLAs are gone)](#13-deadline-alerts-slas-are-gone)
14. [Testing Dags](#14-testing-dags)
15. [Taking it to production](#15-taking-it-to-production)
16. [Appendix: every Dag in full](#16-appendix-every-dag-in-full)
17. [What was actually verified](#what-was-actually-verified)

Reference material: [Airflow 2 → 3 migration](docs/airflow2-to-airflow3.md) ·
[CLI cheatsheet](docs/cheatsheet.md) · [Troubleshooting](docs/troubleshooting.md)

---

## 1. Why orchestrate at all

So far the ML work is a pile of functions you run by hand in the right order and
hope nobody skips a step. Orchestration turns that into a system: the work is
declared as a graph of tasks, something else decides when to run it, retries the
parts that fail, and records what happened.

A workflow in Airflow is a **Dag** — a directed acyclic graph. Directed: edges
have a direction. Acyclic: no loops, so a finished upstream task never re-runs
because of something downstream. Each node is a **task**, and one execution of
the graph is a **Dag run**.

Two Dags are built here, and the interesting part is how they connect:

```
dataops:   extract ─► validate_raw ─► transform ─► validate_transformed ──┐
                                                                          │ emits
                                                            Asset: labeled_projects
                                                                          │ triggers
mlops:                  prepare ─► validate ─► train×N ─► select ─► gate ─┴─► promote
                                                                          │ emits
                                                            Asset: production_model

monitoring:  measure ─► decide ─► {continue | inspect | rollback | approve ─► retrain}
```

`mlops` has no schedule. It runs when `dataops` publishes good data. That is
Airflow 3's data-aware scheduling, and it is the single biggest reason to
rewrite the original lesson rather than patch it.

### Why Airflow and not one of the others

Airflow is not the only orchestrator — Prefect, Dagster, Luigi, Metaflow, Flyte,
Kubeflow Pipelines and Vertex AI Pipelines all cover this ground, and several of
them are nicer to write. Airflow is used here for the reasons the original
lesson gave, which have held up:

- **Adoption.** It is the default in most data platforms, so the concepts
  transfer to whatever job you land in and every vendor ships an integration.
- **Pure Python, no DSL.** Workflows are ordinary functions, testable with
  pytest (§14).
- **The same code runs on your laptop and on a cluster** — swap the executor,
  not the Dag.
- **Apache governance**, a large provider ecosystem, and no single vendor.

The two things it is genuinely worse at are covered in §3: sub-second/streaming
work, and anything that needs a loop.

The original lesson sits on top of four earlier ones in its course — the data
stack, testing, experiment tracking and monitoring. This repo stands alone
instead, with a small local stand-in for each: `src/tagifai/data.py` (the EL and
dbt layer), `validate.py` (the Great Expectations suites), `registry.py` (the
experiment tracker and model registry) and `monitoring.py` (drift and live
performance). Every one is a slot you can swap for the real tool — the swap
tables in §9 and §10 say which.

## 2. Setup

### Docker (recommended, and the only option on Windows)

Airflow has no native Windows support. On Windows use Docker Desktop, or run the
native install inside WSL2.

```bash
git clone <this repo> && cd Airflow3Tutorial

# Linux / macOS / WSL2 / Git Bash
./scripts/up.sh

# Windows PowerShell
.\scripts\up.ps1
```

Then open <http://localhost:8080>.

The stack is deliberately smaller than the [official
docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/3.3.1/docker-compose.yaml):
`LocalExecutor` instead of Celery + Redis, and the Simple auth manager instead
of FAB. Fewer moving parts while you are learning the concepts.

> **Auth:** `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=true` is set, so the
> UI has no login. The port is bound to `127.0.0.1` so it stays on your machine.
> For a real login form, see [the auth section of the migration
> doc](docs/airflow2-to-airflow3.md#8-auth).

Useful commands:

```bash
docker compose ps
docker compose logs -f airflow-scheduler
docker compose exec airflow-scheduler airflow dags list
docker compose run --rm --entrypoint pytest airflow-cli /opt/airflow/tests -q
./scripts/reset.sh        # nuke containers, DB volume and artifacts
```

### Native (Linux / macOS / WSL2)

```bash
./scripts/setup-native.sh
```

What that script does, and why each line matters:

```bash
AIRFLOW_VERSION=3.3.1
PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install -r requirements.txt --constraint "${CONSTRAINT_URL}"   # constraints are NOT optional

export AIRFLOW_HOME=${PWD}/airflow
export PYTHONPATH=${PWD}/src                    # so tasks can import `tagifai`
export AIRFLOW__CORE__DAGS_FOLDER=${PWD}/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False

airflow db migrate                              # `airflow db init` was REMOVED
airflow connections add fs_default --conn-type fs --conn-extra '{"path": "/"}'
```

Airflow 3.3.1 needs Python ≥ 3.10 (and not 3.15).

`airflow db migrate` populates `$AIRFLOW_HOME`:

```
airflow.cfg     every setting, with the defaults commented — see config/airflow.cfg.example
airflow.db      the SQLite metadata DB (fine for learning; Postgres for anything else)
logs/           task logs, one directory per dag_id / run_id / task_id
dags/           only if you leave dags_folder at its default
```

Two Airflow 2 files are **not** there any more: `unittests.cfg`, and
`webserver_config.py` — the latter was FAB's, and under the default Simple auth
manager its replacement is
`$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`. Any setting in
`airflow.cfg` can also be given as `AIRFLOW__SECTION__KEY`, which is what this
repo's compose file does instead of shipping a config file.

Then either the all-in-one process:

```bash
airflow standalone
```

…or the components separately, which is what a real deployment looks like:

```bash
airflow api-server --port 8080   # Airflow 2 called this `airflow webserver`
airflow scheduler
airflow dag-processor            # a separate process in Airflow 3 — required
airflow triggerer                # deferrable tasks and async callbacks
```

## 3. How Airflow 3 is put together

Airflow is a scheduler for batch workflows written in Python. You describe the
graph in code, and Airflow decides when each node runs, retries it, records the
attempt, and shows you the result. It does not move your data and it does not
train your model — it starts the thing that does, waits, and reacts.

That framing also tells you when to reach for something else. Airflow is a poor
fit for sub-second or per-event work (use a queue or a stream processor), for
anything needing a cycle or a `while` loop across runs, and for heavy compute in
the worker itself — the pattern is to trigger a Spark/Ray/Kubernetes job and
track it, not to fit the model inside the scheduler's process. If your pipeline
is three scripts run once a week, cron is still allowed.

The processes it splits that job across:

| Component | Job | Change from Airflow 2 |
|---|---|---|
| **API server** | REST API v2, the React UI, and the **Task Execution API** workers call | Replaces the webserver |
| **Scheduler** | Decides what to run; hands work to the executor | Same role, no longer the parser |
| **Dag processor** | Parses Dag files into the DB | **Now its own process**, always |
| **Triggerer** | Runs async triggers for deferred tasks | Same |
| **Executor** | Where tasks actually run | `SequentialExecutor` removed; `LocalExecutor` is the default and works with SQLite |
| **Metadata DB** | State of everything | Tasks can no longer touch it directly |

The last row is the architectural change everything else follows from. In
Airflow 2 a task ran with a live database session. In Airflow 3 task code talks
to the Task Execution API over HTTP, which is what makes remote workers, the
Edge executor and non-Python task SDKs possible — and why your task code can no
longer `session.query(DagRun)`.

Health check:

```bash
curl -s localhost:8080/api/v2/monitor/health
```

The interactive API docs are at <http://localhost:8080/docs>.

## 4. The UI

`airflow webserver` does not exist any more. The process is `airflow
api-server`, and what it serves at <http://localhost:8080> is a React
application talking to the same public REST API you can call yourself — not the
Flask-AppBuilder pages Airflow 2 rendered. Every Airflow 2 UI screenshot you
find online is out of date, which is reason enough to walk through this once.

The nav is **Home · Dags · Assets · Browse · Admin · Plugins · Security · Docs**.

**Home** — health of the scheduler, triggerer and Dag processor, import errors,
failed runs and tasks, recent asset events, deadlines and pool slots. Open it
first when something is not running: a Dag that never appears in the list is
usually an import error sitting on this page.

**Dags** — the list, as cards or a table, filtered by tag, owner or state, with
the pause toggle and a **Trigger** button on each row. A Dag scheduled on assets
shows its asset condition where a cron Dag shows its schedule.

**A Dag's page** puts the run history on the left and detail tabs on the right.
The left panel holds **Grid** and **Graph** — `g` toggles between them —
plus **Show Gantt**, a graph direction control (top-to-bottom, left-to-right, …)
and filters that matter on the Dags here:

- **Dependencies: All Dag Dependencies / External conditions / Only tasks.**
  Switch it away from "Only tasks" on `mlops` and the graph draws the
  `labeled_projects` asset that triggered the run, not just the tasks inside it.
- **Min. map index** — "shows mapped tasks expanded to at least this index". A
  mapped task like `train_candidate` is one collapsed node until you raise this;
  raise it and each hyperparameter set gets its own.
- Version indicators, and **Download graph image**.

The tabs are Overview, Runs, Tasks, Backfills, Asset Events, Audit Log,
Calendar, Code, Details and — when a task is waiting on a human — **Required
Actions**. Click a run, then a task, and the same strip becomes that task
instance's: **Logs**, **XCom**, **Rendered Templates**, and **Task Instances**
for the mapped fan-out.

**The Dag version selector** is new in Airflow 3 and the most under-advertised
thing in this list. Every Dag run records the version of the code it ran with,
so the picker (`v1`, `v2`, … or **All Versions**) lets you look at a run through
the code that actually produced it, with the bundle name, bundle version and
parse time on the **Code** tab. In Airflow 2, editing a Dag rewrote history: the
graph of last week's run was redrawn with today's tasks.

**The Trigger form is generated from your `params`.** This is why `params` are
worth declaring with types:

```python
params={"greeting": Param("hello", type="string")}          # dags/01
POLICY_PARAMS = {"max_label_tvd": Param(0.15, type="number")}  # dags/06
```

A string param becomes a text field, a number a numeric one, an enum a dropdown,
and required fields are validated before the run is created. Under **Advanced
Options** the same thing is editable as raw **Configuration JSON**, and the form
also offers a run id, a logical date, a manual data interval, and an *"Unpause
… on trigger"* checkbox — which exists because triggering a paused Dag leaves
the run **queued** forever (§7).

The same dialog has a second mode, **Backfill**: a date range, a **Reprocess
Behavior** (missing runs / missing and errored runs / all runs), max active runs,
"run backwards", and a dry run that tells you *"N runs will be triggered"*
before you commit. Asset-scheduled Dags say *"This Dag's schedule does not
support backfills"* — `mlops` cannot be backfilled, because there is no date
range to replay, only asset events.

**Assets** — the tab this tutorial was built for. It lists every asset with its
producing tasks, consuming Dags and event history, and draws the cross-Dag
graph: `dataops` → `labeled_projects` → `mlops` → `production_model` (produced
by the promotion, consumed by nobody yet — point a Dag's `schedule` at it and
the edge appears). That graph is the one picture of the whole pipeline; the Dags
list cannot show it, because the connection is not a dependency between Dags, it
is an asset between them. **Create Event** on an asset either materializes it (run
the Dag upstream) or records a manual event — which is how you trigger a
downstream Dag without pretending to produce the data.

**Browse** — Audit Log (who triggered, paused, cleared what), **Deadlines**
(pending and missed deadline alerts, §13), and XComs.

**Admin** — Connections, Variables, Pools, Providers, Plugins, Jobs and the
rendered Airflow **Config**. Connections and Variables are the two you will use:
never put a credential in a Dag file.

> **Where the FAB pages went:** with the FAB provider installed you get a
> **Legacy Views** entry, and **Security** (users, roles) only means anything
> under an auth manager that has them. This stack runs the Simple auth manager
> with `SIMPLE_AUTH_MANAGER_ALL_ADMINS=true`, so there is no login and no user
> admin at all.

## 5. Dags and tasks

Everything a Dag author needs is in `airflow.sdk` — the Task SDK. This is the
only supported authoring surface in Airflow 3.

```python
# Airflow 2                              # Airflow 3
from airflow.decorators import dag, task from airflow.sdk import dag, task
from airflow.models.dag import DAG       from airflow.sdk import DAG
```

[`dags/01_hello_airflow3.py`](dags/01_hello_airflow3.py):

```python
import pendulum
from airflow.sdk import Param, dag, task

@dag(
    dag_id="hello_airflow3",
    schedule=None,                                   # was `schedule_interval`
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),  # `days_ago()` was removed
    catchup=False,
    default_args={"owner": "mlops-course", "retries": 2,
                  "retry_delay": pendulum.duration(seconds=30)},
    tags=["tutorial", "basics"],
    params={"greeting": Param("hello", type="string")},
)
def hello_airflow3():

    @task
    def make_greeting(**context) -> str:
        return f"{context['params']['greeting']} from run {context['run_id']}"

    @task
    def shout(message: str) -> str:
        return message.upper()

    @task
    def count_words(message: str) -> int:
        return len(message.split())

    @task
    def report(loud: str, words: int) -> None:
        print(f"{loud}  ({words} words)")

    message = make_greeting()
    report(loud=shout(message), words=count_words(message))

hello_airflow3()          # the decorator defines it; calling it registers it
```

Calling a task function does **not** run it. It returns an `XComArg`, a lazy
reference to that task's output; passing it to another task is what draws the
edge. The graph is built at parse time, executed later, possibly on another
machine.

**The other way to declare a Dag** is the context manager, which is still fully
supported — the decorator is sugar on top of it. Use it when the tasks are
operators rather than functions:

```python
from airflow.sdk import DAG          # the legacy paths warn: DeprecatedImportWarning

with DAG(
    dag_id="hello_operators",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["tutorial"],
) as dag:
    BashOperator(task_id="hello", bash_command="echo hi")
```

Anything created inside the `with` block attaches to that Dag automatically.
Both forms produce the same object, and a file may hold several Dags either way
— the Dag processor picks up every `DAG` instance at module level.

Two documentation arguments are worth setting on either form: `description=` is
the one-line summary shown next to the Dag in the list, and `doc_md=` is
markdown rendered on the Dag's page — the Dags here pass `doc_md=__doc__`, so
the module docstring you are reading in each file *is* the in-UI documentation.
`@task(doc_md=...)` does the same for a single task.

Run one Dag run in-process, no scheduler involved:

```bash
airflow dags test hello_airflow3
```

This is the fastest authoring loop there is — a full run, real logs, no UI.

## 6. Operators, XComs and relationships

Operators are the pre-built task types. In Airflow 3 the common ones moved out
of core into `apache-airflow-providers-standard` (pre-installed, but the import
path changed):

```python
# Airflow 2                                    Airflow 3
airflow.operators.bash.BashOperator            airflow.providers.standard.operators.bash.BashOperator
airflow.operators.python.PythonOperator        airflow.providers.standard.operators.python.PythonOperator
airflow.operators.empty.EmptyOperator          airflow.providers.standard.operators.empty.EmptyOperator
airflow.sensors.filesystem.FileSensor          airflow.providers.standard.sensors.filesystem.FileSensor
```

[The full list is in the migration doc.](docs/airflow2-to-airflow3.md#2-imports)

Beyond the standard ones there is a provider for nearly every system you would
want to talk to — S3, Snowflake, BigQuery, Postgres, Kubernetes, Databricks,
Slack, HTTP, Docker, dbt Cloud, Airbyte — each shipping operators, hooks and
sensors, installed as `apache-airflow-providers-<name>` and listed in the UI
under Admin → Providers. Reach for one before writing HTTP calls by hand: a
provider's operator already handles connections, retries and secret lookup.

When none fits, subclass `BaseOperator`:

```python
from airflow.sdk import BaseOperator     # was airflow.models.baseoperator

class TagifaiTrainOperator(BaseOperator):
    template_fields = ("experiment_name",)      # these get Jinja-rendered

    def __init__(self, *, experiment_name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.experiment_name = experiment_name

    def execute(self, context):                 # return value becomes the XCom
        return train_module.train_from_files()[1]["f1"]
```

That is the right move when a step is reused across Dags and needs its own
parameters, retries or template fields. For everything else `@task` is less
code and this repo uses it throughout.

### Hooks, Connections and Variables

An operator is the *task*; a **hook** is the client underneath it. Hooks take a
`conn_id`, resolve the Connection, build the authenticated client and expose
methods — `S3Hook.load_file`, `PostgresHook.get_pandas_df`. Use one directly
inside a `@task` when you want the connection handling but not a whole operator:

```python
from airflow.sdk import BaseHook, Connection, Variable   # was airflow.hooks.base / airflow.models
from airflow.providers.standard.hooks.filesystem import FSHook

@task
def land_file() -> str:
    base = FSHook(fs_conn_id="fs_default").get_path()       # "/" here
    threshold = Variable.get("min_rows", default=500, deserialize_json=False)
    conn = BaseHook.get_connection("fs_default")            # or Connection.get(...)
    ...
```

**Connections** hold credentials (`conn_type`, host, login, password, extra);
**Variables** hold configuration. Both are editable under Admin, settable from
the CLI, and overridable by an `AIRFLOW_CONN_*` / `AIRFLOW_VAR_*` environment
variable — which is how `fs_default` is defined in this repo's compose file.
Templates reach them as `{{ conn.fs_default.host }}` and `{{ var.value.my_key }}`.

> **Airflow 3 detail:** these come from `airflow.sdk` now, and a task fetching
> one no longer touches the metadata database — the request goes over the Task
> Execution API, which is what lets a remote worker resolve a Connection it has
> no database access to. The old `airflow.models` / `airflow.hooks.base` paths
> still import, with a `DeprecatedImportWarning`.
>
> Prefer a Variable **per Dag run** (`params`, §4) over a global Variable when
> the value is a policy knob: params are recorded with the run that used them,
> Variables are not.

**XComs** ("cross-communications") are how tasks pass small values through the
metadata database. TaskFlow uses them implicitly for return values; operators
use them explicitly — and this is where Airflow 3 will bite you:

```python
# Airflow 2: searched the whole Dag run and returned the most recent push.
seed = ti.xcom_pull(key="seed")

# Airflow 3: pulls from the CURRENT task only, unless you name the source.
seed = ti.xcom_pull(task_ids="push_seed", key="seed")
```

Code that relied on the old behaviour now silently gets `None`. There is no
deprecation warning, because the call is still valid — it just means something
else.

> **Rule that has not changed:** XComs are for metadata, not payloads. They live
> in the metadata database. Pass a path, a run id, a row count. Put the
> dataframe in object storage and the model in a registry. Airflow 3 also
> removed `enable_xcom_pickling`, so values must be JSON-serialisable.

### Relationships

An edge between two tasks is declared one of four ways, all of them in
[`dags/02_operators_and_xcoms.py`](dags/02_operators_and_xcoms.py):

```python
from airflow.sdk import Label, chain, cross_downstream

# 1. Implicitly, by passing one task's output to another. TaskFlow only.
report(loud=shout(message))

# 2. The shift operators, when no data flows — `a >> b` is "a then b".
describe >> product          # `product << describe` is the same edge, backwards
                             # so is describe.set_downstream(product), and
                             # product.set_upstream(describe) — the methods the
                             # operators call underneath. Use them when the
                             # tasks are built in a loop and `>>` reads badly.

# 3. Fan-out and fan-in: a list on either side broadcasts the edge.
product >> [check_parity, check_range] >> checks_done   # four dependencies

# 4. `chain()` — the readable form of a long `>>` line, and the only one that
#    can carry a `Label`, which annotates the edge in the Graph view.
chain(start, push_seed, Label("seed -> product"), multiply, describe)
```

Two shapes that look like they should work, and do not:

```python
[a, b] >> [c, d]              # TypeError: no operator between two lists
chain([a, b], [c, d])         # legal, but PAIRS them: a->c and b->d only,
                              # and raises if the lists differ in length
cross_downstream([a, b], [c, d])   # what you actually wanted: all-to-all
```

`cross_downstream` is the Cartesian product — every task on the left upstream of
every task on the right. `chain_linear` is its sibling for chaining several such
groups in sequence.

> Mixing styles is fine, and the Dags here do: `dataops` passes dataframe paths
> between TaskFlow tasks (style 1) and adds `raw_report >> curated_fp` (style 2)
> for the gate that transforms nothing but must still block the transform.

## 7. Scheduling, logical dates and backfills

```python
schedule=None                       # manual / API only
schedule="0 3 * * *"                # cron
schedule=timedelta(days=1)          # interval
schedule=[some_asset]               # data-aware (§8)
schedule=CronTriggerTimetable(...)  # custom timetable
```

Three defaults changed, and all three can surprise you:

**`catchup` defaults to `False`.** Un-pausing a Dag with a 2024 start date no
longer floods the scheduler with a year of runs. If you actually want the
backfill on activation, ask for it: `catchup=True`.

**Cron means `CronTriggerTimetable` now.** `create_cron_data_intervals` defaults
to `False`, so `schedule="0 3 * * *"` fires at 03:00 with `logical_date` = 03:00.
In Airflow 2 the same string fired at 03:00 with `logical_date` = *yesterday*
03:00. Any Dag that used `{{ ds }}` to name a partition shifts by one interval
on upgrade.

**`execution_date` no longer exists.** It is `logical_date`, and it is
*nullable* — manual and asset-triggered runs have none. Also removed from the
context: `next_ds`, `prev_ds`, `tomorrow_ds`, `yesterday_ds`,
`next_execution_date`, `prev_execution_date`. Still available: `ds`, `ts`,
`data_interval_start`, `data_interval_end`, `run_id`, `dag_run`, `ti`, `params`,
`var`, `conn`, `macros`.

```bash
$ airflow dags list-runs mlops -o plain
dag_id  run_id                                      state    run_after             logical_date
mlops   asset_triggered__2026-09-09T08:23:16+00:00  success  2026-09-09T08:23:16              ← no logical date
mlops   manual__2026-09-09T08:22:31+00:00           success  2026-09-09T08:22:32   2026-09-09T08:22:31
```

Use `run_after` when you mean "when did this actually start".

### Manual runs

A schedule is not the only way to start a Dag. Two commands cover most of it:

```bash
airflow dags unpause hello_airflow3
airflow dags trigger hello_airflow3 -c '{"greeting": "hola"}' -r manual-demo-1
```

- **`-c/--conf`** is a JSON object. It lands in `dag_run.conf`, and any key that
  matches a declared `Param` overrides it for that run — which is how
  `airflow dags test monitoring --conf '{"max_label_tvd": 0.0}'` in §12 forces a
  branch.
- **`-r/--run-id`** names the run. Without it you get
  `manual__<timestamp>`.
- **`-l/--logical-date`** sets one. Without it the run has **none**, which is
  the trap below.
- In the UI the same thing is the **Trigger** button, with the form generated
  from your `params` (§4).

A new Dag arrives **paused** — `[core] dags_are_paused_at_creation` is `True`,
same as Airflow 2 — so `unpause` is not optional, and it is the step everyone
forgets:

**A paused Dag still accepts a trigger — the run is created and then sits in
`queued` until you unpause.** Nothing fails, nothing warns; the CLI help says it
in one line and that is the only notice you get:

```console
$ airflow dags pause hello_airflow3 && airflow dags trigger hello_airflow3 -r demo
$ airflow dags list-runs hello_airflow3 -o plain
dag_id          run_id  state   run_after                         logical_date
hello_airflow3  demo    queued  2026-09-09T12:19:29.483234+00:00             ← stuck
$ airflow dags unpause hello_airflow3     # now it starts
```

The UI's trigger form has an *"Unpause … on trigger"* checkbox for exactly this.

> **The trap worth knowing before you trigger anything:** a manual run has **no
> logical date**, so `logical_date` is missing from the task context and `ds`,
> `ts` and friends are *undefined* in templates. Both fail at runtime and
> neither fails under `airflow dags test`, which always supplies a logical date:
>
> ```python
> context["logical_date"]          # KeyError on a manual run
> context.get("logical_date")      # None — fine
> "{{ ds }}"                       # UndefinedError: 'ds' is undefined
> "{{ ds | default('none') }}"     # fine
> "{{ dag_run.run_after }}"        # fine — every run has one
> ```
>
> Both of those were live bugs in `dags/01` and `dags/02` until a manual trigger
> found them. Asset-triggered runs have no logical date either, so `mlops` is
> exposed to the same thing.

Two more ways in, for completeness: `airflow dags test <dag_id>` runs the whole
Dag in-process (§14), and the REST API does it over HTTP —
`POST /api/v2/dags/{dag_id}/dagRuns`, which is what the UI button calls.

**Backfills** are first-class objects in Airflow 3, with their own API, UI form
and reprocess policy — not a blocking CLI process:

```bash
airflow backfill create \
    --dag-id dataops \
    --from-date 2026-01-01 --to-date 2026-01-31 \
    --reprocess-behavior failed \
    --max-active-runs 3
```

(`airflow dags backfill` is gone. In the UI: Dag → **Trigger** → **Backfill**.)

## 8. Assets: scheduling on data, not on time

Airflow 2's Datasets are Airflow 3's **Assets**, with considerably more behind
them. An asset is a named piece of data. A task declares it produces one with
`outlets=`; a Dag declares it consumes one by putting it in `schedule=`.

[`src/tagifai/assets.py`](src/tagifai/assets.py):

```python
from airflow.sdk import Asset

LABELED_PROJECTS = Asset(
    name="labeled_projects",
    group="dataset",
    extra={"layer": "warehouse", "owner": "data-engineering"},
)
```

Producer — note that the outlet is on the **validation** task, not the transform
task:

```python
@task(outlets=[LABELED_PROJECTS])
def validate_transformed(fp: str, *, outlet_events) -> dict:
    df = data.load_labeled(fp)
    result = validate.run_suite(df, "labeled_projects")
    if not result.passed:
        raise AirflowFailException(...)
    outlet_events[LABELED_PROJECTS].extra = {"rows": len(df), "path": fp}
    return {...}
```

Consumer:

```python
@dag(schedule=[LABELED_PROJECTS], ...)
def mlops():
    @task
    def prepare_data(*, triggering_asset_events):
        for asset, events in triggering_asset_events.items():
            print(f"triggered by {asset} extra={event.extra}")
```

That is the whole idea: **downstream Dags are scheduled on "good data arrived",
never on "a task finished" and never on a cron guess.** No
`ExternalTaskSensor`, no sleeping worker, and the event carries the row count
that caused it.

More asset machinery worth knowing:

| Feature | What it does |
|---|---|
| `AssetAny(a, b)` / `AssetAll(a, b)` | OR / AND conditions in a `schedule` |
| `AssetAlias` | outlets whose URI is only known at runtime |
| `@asset` / `@asset.multi` | define asset + Dag + task in one decorator |
| `Metadata(asset, {...})` | attach metadata by yielding from the task |
| `AssetWatcher` + `BaseEventTrigger` | **event-driven** runs (§11) |
| `CronPartitionTimetable` (3.2+) | partitioned assets |

```bash
$ airflow assets list -o plain
name              uri               group    extra
labeled_projects  labeled_projects  dataset  {'layer': 'warehouse', ...}
production_model  production_model  model    {'owner': 'ml-engineering'}
raw_projects      raw_projects      dataset  {'layer': 'raw', ...}
```

The **Assets** tab in the UI draws the cross-Dag graph these produce.

## 9. The DataOps Dag

[`dags/03_dataops.py`](dags/03_dataops.py) — extract → validate → transform →
validate, exactly the shape of the original lesson. Each task is a slot you can
swap for the real vendor:

| Task here | The original lesson | Production option |
|---|---|---|
| `extract_projects` | `AirbyteTriggerSyncOperator` | Airbyte / Fivetran / an EL job |
| `validate_raw`, `validate_transformed` | `GreatExpectationsOperator` | Great Expectations / Soda / dbt tests |
| `transform` | dbt via `BashOperator` | `DbtCloudRunJobOperator` / astronomer-cosmos |

The validation gate is the part worth copying:

```python
@task
def validate_raw(fp: str) -> dict:
    result = validate.run_suite(data.load_raw(fp), "raw_projects")
    if not result.passed:
        # AirflowFailException stops retries: bad data will not fix itself
        # by running the same task again.
        raise AirflowFailException(f"Raw data failed validation: {result.failures}")
    return {"suite": result.suite, "results": result.results}
```

Two details that are easy to get wrong:

- **`AirflowFailException`, not `AirflowException`.** A data-quality failure is
  not transient; retrying it three times just delays the alert. Import it from
  `airflow.sdk.exceptions` — `airflow.exceptions` is deprecated for task code,
  because a remote worker only has the Task SDK installed.
- **Expectations are non-critical when they mean "look at this", not "stop".**
  An unexpected tag appearing is a signal to inspect the label space, not a
  reason to halt the pipeline. `validate.py` marks that one `critical=False`.

The Dag also uses `@task_group`, which is what replaced **SubDAGs** — removed
entirely in Airflow 3.

### Swapping in the real vendors

Each swap is one operator plus one **Connection** — the credential never appears
in the Dag file. Add it under Admin → Connections, or from the CLI, or as an
`AIRFLOW_CONN_*` environment variable; the Dag only ever names it:

```bash
airflow connections add airbyte \
    --conn-type http --conn-host localhost --conn-port 8000
```

```python
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

extract_projects = AirbyteTriggerSyncOperator(
    task_id="extract_projects",
    airbyte_conn_id="airbyte",            # ← the Connection, not a credential
    connection_id="XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
    asynchronous=False, timeout=3600, wait_seconds=3,
)

validate_raw = GreatExpectationsOperator(
    task_id="validate_raw",
    checkpoint_name="projects",
    data_context_root_dir=GE_ROOT_DIR,
    fail_task_on_validation_failure=True,  # the gate: a failed suite fails the task
)

transform = DbtCloudRunJobOperator(
    task_id="transform", job_id=118680,
    wait_for_termination=True, check_interval=10, timeout=300,
)
# ...or dbt Core in-process: BashOperator(bash_command=f"cd {DBT_DIR} && dbt run && dbt test")
```

Where those names come from — each tool is set up **outside** Airflow, and the
Dag only references the artefact:

```bash
great_expectations init                        # creates the context directory
great_expectations datasource new              # point it at your warehouse
great_expectations suite new                   # the expectations ("projects")
great_expectations checkpoint new projects     # ← this name is `checkpoint_name=`

dbt init dbt_transforms                        # then models/<name>/<name>.sql + schema.yml
```

That separation is the point of the swap table: Airflow schedules and gates the
tools, it does not replace them.

The Dag's shape does not change — `outlets=[LABELED_PROJECTS]` still goes on the
last validation task, and everything downstream still schedules on the asset.
Providers install as `apache-airflow-providers-airbyte`,
`apache-airflow-providers-dbt-cloud`, and so on; these three are **not**
installed here, which is the entire reason the tutorial runs offline.

```bash
docker compose exec airflow-scheduler airflow dags test dataops
```

## 10. The MLOps Dag

[`dags/04_mlops.py`](dags/04_mlops.py) — no schedule, triggered by the asset.

```
prepare_data ─► validate_training_data ─► train_candidate×N ─► select_best
             ─► evaluate_best ─► promotion_gate ─► promote_model ──► done
                                               └─► keep_current_model ─┘
```

Mapped against the original lesson's `extract_from_dwh >> validate >> optimize >> train`:

| Task here | The original lesson | Production option |
|---|---|---|
| the asset trigger + `prepare_data` | `extract_from_dwh` (a BigQuery query into a CSV) | the warehouse client of your stack — or, as here, let the asset event carry the pointer instead of re-querying |
| `validate_training_data` | `validate` (`GreatExpectationsOperator`) | Great Expectations / Pandera / Soda, plus split checks |
| `candidates` + `train_candidate.expand()` | `optimize` (Optuna, `num_trials`) | Optuna / Ray Tune / a sweep service; dynamic mapping is the orchestration half of it, and gives you one retryable task instance per trial |
| `select_best` + `evaluate_best` | `train` (fit the best params, log to MLflow) | your registry's promotion API |
| `promotion_gate` → `promote_model` | *(not in the original)* | the model registry stage transition |

**Validate before you train.** `dataops` validated `labeled_projects` on its way
out of the warehouse, and this Dag validates it again on the way in. That is not
redundant: it was a different Dag run, possibly hours ago, this run may have been
triggered by hand, and what the model is fitted on is the snapshot on disk *now*.

The second half of the check is one a table-level suite cannot do at all — it
looks at the **split**:

```python
@task
def validate_training_data(split_sizes: dict) -> dict:
    df = data.load_labeled()
    table = validate.run_suite(df, "labeled_projects")     # the same suite as dataops
    split = validate.check_split(*data.split(df))          # and this one on the split
    if not (table.passed and split.passed):
        raise AirflowFailException(...)
```

```python
# src/tagifai/validate.py — check_split()
("split_is_non_empty",          ...,  critical)
("classes_in_both_splits",      ...,  critical)   # a class only in train
("min_rows_per_class:20",       ...,  critical)   # 3 rows of a class in train
("no_id_leakage",               ...,  critical)   # the same project in both
("test_fraction_between_0.1_and_0.4", ..., non-critical)
```

Every one of those passes a column-level suite and still produces a model whose
held-out f1 means nothing — and that f1 is exactly what the promotion gate below
compares against production. `snapshot >> results` wires the gate upstream of
*every* mapped training instance, carrying no data, only permission to run.

**Dynamic task mapping** fans out one task instance per hyperparameter set at
runtime, then reduces:

```python
@task(max_active_tis_per_dag=2)
def train_candidate(hparams: dict, **context) -> dict:
    run_id = f"{context['run_id']}__{context['ti'].map_index}"
    model, metrics = train_module.train_from_files(hparams)
    registry.save_run(run_id, model, metrics)
    return {"run_id": run_id, "f1": metrics["f1"], "hparams": hparams}   # small!

@task
def select_best(results: list[dict]) -> dict:
    return max(results, key=lambda r: r["f1"])

results = train_candidate.expand(hparams=candidates())
best = select_best(results)
```

> **Gotcha:** the parameter is named `hparams`, not `params`. TaskFlow injects
> context keys by parameter name, and `params` is reserved — a task argument
> called `params` gets the Dag's params, not your data.

The **promotion gate** is a branch, not a failure:

```python
@task.branch
def promotion_gate(evaluation: dict) -> str:
    f1 = evaluation["metrics"]["f1"]
    current = registry.get_production()
    if f1 < config.MIN_F1_TO_PROMOTE:      return "keep_current_model"
    if current and f1 < current["metrics"]["f1"]:  return "keep_current_model"
    return "promote_model"

done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")
gate >> promote_model(evaluation) >> done
gate >> keep_current_model >> done
```

A candidate that misses the threshold is a normal outcome — the run succeeds and
production is left alone. The join needs
`trigger_rule="none_failed_min_one_success"`, because the default `all_success`
would skip it as soon as either branch is skipped.

**Trigger rules** are how a task states what it wants from its parents, and
branching is where you first need one. The useful ones:
`all_success` (default), `all_done` (run whatever happened upstream — cleanup
tasks), `none_failed_min_one_success` (joins after a branch),
`none_failed` (allows skips but not failures), `one_success`, `one_failed`
(alerting paths), `all_skipped`, and `always`.

Promotion emits the second asset, which is what `monitoring` reacts to:

```python
@task(outlets=[PRODUCTION_MODEL])
def promote_model(evaluation: dict, *, outlet_events) -> dict:
    pointer = registry.promote(evaluation["run_id"])
    outlet_events[PRODUCTION_MODEL].extra = {"run_id": pointer["run_id"], ...}
    return pointer
```

Watch the whole chain fire:

```bash
docker compose exec airflow-scheduler bash -c '
  airflow dags unpause dataops
  airflow dags unpause mlops
  airflow dags trigger dataops'

# ~30s later, mlops has a run nobody triggered:
docker compose exec airflow-scheduler airflow dags list-runs mlops -o plain
# asset_triggered__2026-09-09T08:23:16.740131+00:00_0X07AfKf  success
```

## 11. Waiting: sensors, deferrable tasks, events

[`dags/05_sensors_and_events.py`](dags/05_sensors_and_events.py). Three ways to
react to the outside world, cheapest last:

```python
# 1. Poking sensor — holds a worker slot the entire time. Seconds, not hours.
FileSensor(task_id="wait", filepath=..., poke_interval=10, timeout=300)

# 2. `reschedule` mode — the task instance exits between pokes and is re-queued.
FileSensor(..., mode="reschedule")

# 3. Deferrable — suspends onto the triggerer and frees the slot entirely.
TimeDeltaSensorAsync(task_id="settle", delta=timedelta(seconds=5))
```

A sensor written as a function:

```python
@task.sensor(poke_interval=10, timeout=120, mode="reschedule")
def enough_rows() -> PokeReturnValue:
    rows = ...
    return PokeReturnValue(is_done=rows >= 500, xcom_value=rows)
```

> **Looking for smart sensors?** They were Airflow 2.2's answer to "a thousand
> sensors are eating the cluster" — a batched, centralised sensor service. They
> were deprecated almost immediately and **removed**; `airflow.sensors.smart_sensor`
> does not exist in Airflow 3. Deferrable operators replaced them and solve the
> problem properly: a deferred task holds no worker slot at all, so the count
> stops mattering. Any tutorial that points you at smart sensors predates 2.4.

And the Airflow 3 option that removes the wait altogether — **event-driven
scheduling**. Attach a watcher to an asset and an external event starts the run;
nothing polls, nothing occupies a slot:

```python
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher

trigger = MessageQueueTrigger(queue="https://sqs.us-east-1.amazonaws.com/.../new-projects")
new_projects = Asset(name="new_projects",
                     watchers=[AssetWatcher(name="sqs_watcher", trigger=trigger)])

@dag(schedule=[new_projects])
def ingest_on_message(): ...
```

Only triggers deriving from `BaseEventTrigger` can be watchers — a trigger
testing a permanent condition ("file exists") would re-fire forever.

> Deferrable sensors and watchers both need `airflow triggerer` running. Without
> it they sit in `deferred` indefinitely.

## 12. Continual learning: monitor, decide, act

**Continual**, not continuous — the original lesson is precise about this and it
is worth keeping. Continuous would mean the system updates itself on new data
with no interruption and no human in it. Continual means it repeats *with
breaks*, and the breaks are deliberate: a monitor fires, something is decided —
by a person where it matters — and then the pipeline runs. The
`ApprovalOperator` gate below is that comma made executable.

What you are watching for is **drift**, in three flavours:

| Drift | What moved | Caught here by |
|---|---|---|
| **data drift** | the input distribution — the text itself | `mean_length_shift` in `monitoring.drift_report()` |
| **target drift** | the label distribution — one tag takes over, or a new tag appears | `label_tvd` (total variation distance over the tag mix) and `new_labels` |
| **concept drift** | the mapping itself — the same input now deserves a different label | only the live performance check: f1 falls while the inputs look unchanged |

Concept drift is the one no input-side statistic can see, which is why
`monitoring.production_performance()` scores recently labelled rows instead of
trusting the distribution comparison alone.

The original lesson closes on four responses to a monitoring event.
[`dags/06_monitoring_and_retraining.py`](dags/06_monitoring_and_retraining.py)
makes them branches:

| Response | When | What happens |
|---|---|---|
| **continue** | drift and performance within budget | nothing; keep serving |
| **improve** | f1 dropped or the input distribution moved | ask a human, then trigger `mlops` |
| **inspect** | an unseen label appeared | stop and page someone — the label space changed |
| **rollback** | performance collapsed | re-point production at the previous model |

The thresholds are Dag **params**, so the policy is visible in the UI,
overridable per run, and recorded with the run that used it:

```python
POLICY_PARAMS = {
    "max_label_tvd": Param(0.15, type="number"),
    "max_f1_drop": Param(0.10, type="number"),
    "critical_f1_drop": Param(0.25, type="number"),
}
```

```bash
airflow dags test monitoring --conf '{"max_label_tvd": 0.0}'
```

Note the ordering in `decide()`: **a new label routes to `inspect`, not to
`improve`.** Retraining on data whose label space you have not understood yet is
how you ship a worse model with full confidence.

Retraining goes through a **human-in-the-loop** gate — an Airflow 3.1 feature,
`ApprovalOperator`:

```python
request_retraining = ApprovalOperator(
    task_id="request_retraining",
    subject="Retrain the tag classifier?",
    body="Monitoring detected drift or a performance drop.\n\nApprove to trigger `mlops`.",
    defaults="Approve",                       # what an unanswered run resolves to
    response_timeout=timedelta(seconds=60),   # give this hours in production
)
request_retraining >> TriggerDagRunOperator(task_id="trigger_retraining",
                                            trigger_dag_id="mlops")
```

The task enters `awaiting_input` and shows a prompt in the UI. On **Reject**,
everything downstream is skipped. With `defaults` set, an unattended run still
resolves instead of hanging forever.

### What "retrain" leaves out

`trigger_retraining` fires `mlops`, which fits on whatever is in the warehouse.
That is the orchestration; it is not the whole retraining problem. The original
lesson lists what belongs between the alert and the fit, and each is a task
slot in a real version of this Dag:

| Step | Why it is not automatic |
|---|---|
| **labeling** | the new rows need real labels, not the model's own predictions fed back as truth |
| **active learning** | label the rows that would teach the model most, not a random sample — you rarely have budget for all of them |
| **QA** | audit the labels before training on them: false positives, disagreement between labellers, poorly-performing slices |
| **augmentation** | expand the new data so a small window is not drowned out by history |
| **sampling** | up/downsample to fix the imbalance the drift introduced |
| **evaluation** | build the held-out set to reflect *deployment* conditions, not the training distribution |

The other constraint is **catastrophic forgetting**: fitting only on the new
window trades yesterday's failure mode for tomorrow's. The Dag here sidesteps it
by always training on the full labelled table — fine at this size, and the first
thing to revisit when the table stops fitting in a task.

Which is also why the promotion gate in §10 compares against the current
production model instead of a fixed threshold alone: a retrained model that is
worse than what is serving should not ship just because a monitor fired.

## 13. Deadline alerts (SLAs are gone)

`sla=` and `sla_miss_callback` were **removed** in Airflow 3.0 and replaced by
Deadline Alerts in 3.1. The difference is not cosmetic: an SLA was per-task,
measured from the run's start, and fired after the fact. A deadline is
per-Dag-run, measured from a reference point you choose, and evaluated while the
run is still going.

[`dags/07_deadline_alerts.py`](dags/07_deadline_alerts.py):

```python
from airflow.sdk import DeadlineAlert, DeadlineReference, SyncCallback

@dag(
    dag_id="deadline_alerts",
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(seconds=30),
        callback=SyncCallback("tagifai.callbacks.deadline_missed",
                              kwargs={"budget": "30s"}),
    ),
)
```

References: `DAGRUN_QUEUED_AT`, `DAGRUN_LOGICAL_DATE`, `FIXED_DATETIME(dt)`,
`AVERAGE_RUNTIME(max_runs=10)`. Callbacks: `SyncCallback` (runs on the executor)
or `AsyncCallback` (runs on the triggerer — use it for Notifiers, e.g.
`AsyncCallback(SlackWebhookNotifier, kwargs={"text": "..."})`). Pass a list to
`deadline=` for several budgets.

```bash
docker compose exec airflow-scheduler airflow dags trigger deadline_alerts
docker compose logs airflow-scheduler | grep "DEADLINE MISSED"
```

> **Known issue on 3.3.1:** `airflow dags test` crashes on any Dag with a
> `deadline=` (`psycopg2.ProgrammingError: can't adapt type 'dict'` out of
> `DagRun.update_state`). The tasks run fine; only the CLI's final state update
> fails. Trigger such Dags through the scheduler instead. This is why `mlops`
> carries no deadline — it would break the `airflow dags test mlops` instruction
> above. [Details.](docs/troubleshooting.md)

## 14. Testing Dags

Three levels, cheapest first.

**Dag integrity tests** — [`tests/test_dag_integrity.py`](tests/test_dag_integrity.py).
These catch the failures that otherwise surface as a red banner in the UI hours
later, and they are the single highest-value thing you can put in CI:

```python
from airflow.dag_processing.dagbag import DagBag   # moved in Airflow 3

def test_no_import_errors(dagbag):
    assert not dagbag.import_errors

def test_mlops_is_scheduled_on_the_labeled_projects_asset(dagbag):
    asset_names = {a.name for a in dagbag.dags["mlops"].timetable.asset_condition.objects}
    assert "labeled_projects" in asset_names
```

It also greps the Dag sources for constructs that were removed in Airflow 3 —
`schedule_interval=`, `days_ago(`, `airflow.operators.`, `execution_date`,
`SubDagOperator`, `sla=` — so they cannot creep back in.

**Unit tests on the workload** — [`tests/test_tagifai.py`](tests/test_tagifai.py).
Nothing in `src/tagifai/` imports Airflow, so the ML code is testable without a
scheduler in the loop. That separation is deliberate: Airflow orchestrates the
code, it does not contain it.

**Whole-run tests** — `airflow dags test <dag_id>` executes a real Dag run
in-process, and `airflow tasks test <dag_id> <task_id>` runs one task without
writing state.

```bash
docker compose run --rm --entrypoint pytest airflow-cli /opt/airflow/tests -q
# 27 passed
```

**A test on the documentation** — [`tests/test_readme_listings.py`](tests/test_readme_listings.py).
The appendix in §16 shows every Dag in full, and those listings are injected
from the files by [`scripts/sync_readme.py`](scripts/sync_readme.py) rather than
copied by hand. The test re-runs the injection in memory and fails if the result
differs, so a Dag edited without re-syncing breaks the build instead of leaving
a tutorial that quotes code nobody runs. It also fails when a new Dag file has
no listing at all.

```bash
python scripts/sync_readme.py --check     # exit 1 when a listing is stale
python scripts/sync_readme.py             # fix it
```

None of those three levels would have caught the two bugs in §7's "trap" box:
`airflow dags test` always supplies a logical date, so a Dag that breaks on a
manual trigger passes every test above. **Trigger each Dag once for real** —
through the scheduler, the way it will actually run — before you believe it.

## 15. Taking it to production

**Executors.** `LocalExecutor` is the Airflow 3 default and is fine up to one
machine. Beyond that: `CeleryExecutor` (worker pool), `KubernetesExecutor` (a
pod per task), or **multiple executors** configured together — which replaced
the `CeleryKubernetesExecutor`/`LocalKubernetesExecutor` hybrids. Airflow 3 also
adds the **Edge executor** for workers outside your cluster, which the Task
Execution API is what made possible.

**Isolation.** Do not install your ML dependencies next to Airflow's. Use
`KubernetesPodOperator`, or `@task.virtualenv` / `@task.external_python` for
lighter isolation. The heavy compute belongs on a cluster (Ray, Spark, EMR,
Dataproc) with Airflow triggering and tracking it — Airflow is an orchestrator,
not a compute engine.

**Dag bundles.** `dags_folder` still works, but the Dag processor reads
*bundles*; the default is one `LocalDagBundle` named `dags-folder`. Switching to
`GitDagBundle` gets you versioning: each Dag run records the commit it ran with,
so a re-run uses the same code.

```ini
[dag_processor]
dag_bundle_config_list = [
    {"name": "team-ml",
     "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
     "kwargs": {"git_conn_id": "git_default", "subdir": "dags", "tracking_ref": "main"}}
  ]
```

**Secrets.** Never in Dag code, never in bundle `kwargs`, never in an asset's
`uri` or `extra` — asset extras are stored in cleartext. Use Connections,
Variables, or a secrets backend.

**Auth.** Swap the Simple auth manager for FAB or a real identity provider
before anyone else can reach the UI.

**Observability.** Airflow emits its own metrics, and this is the part people
skip until an incident. Three layers:

- **Metrics** — `[metrics] statsd_on = True` for StatsD/Datadog, or
  `[metrics] otel_on = True` to push OpenTelemetry to a collector. Watch
  `scheduler.tasks.starving`, `executor.open_slots`, `dagrun.duration.failed`
  and `dag_processing.import_errors`. Note the naming change: Airflow 3 puts
  `dag_id`/`task_id` in **tags** rather than baking them into the metric name,
  and `[metrics] legacy_names_on` (default `True`, added in 3.2) exports both
  forms so old dashboards keep working — turn it off once they are migrated.
- **Traces** — `[traces] otel_on = True` sends spans for scheduler loops and
  task runs, which is how you find *which* task in a slow Dag got slower.
- **Health** — `GET /api/v2/monitor/health` reports scheduler, triggerer and Dag
  processor status; the UI Home page renders the same thing.

Then the ordinary hygiene: `airflow db clean` on a schedule (the metadata DB
grows forever otherwise), deadline alerts on the Dags that matter (§13), and
Notifiers — `SlackWebhookNotifier`, `SmtpNotifier` — wired to
`on_failure_callback` or an `AsyncCallback`.

## 16. Appendix: every Dag in full

The sections above quote the parts of each Dag that teach something, which means
they never show a whole file. Here is each one end to end, in reading order —
including the comments, which are where the Airflow 2 → 3 deltas are explained
at the point they matter.

> These listings are **injected from the files** by
> [`scripts/sync_readme.py`](scripts/sync_readme.py) and verified by
> [`tests/test_readme_listings.py`](tests/test_readme_listings.py): edit a Dag
> without re-running the script and the test suite fails. They cannot drift out
> of sync with the code that actually runs.
>
> ```bash
> python scripts/sync_readme.py            # inject
> python scripts/sync_readme.py --check    # what CI/pytest runs
> ```

The shared Asset definitions come first, because every Dag below imports them.

<details>
<summary><b>src/tagifai/assets.py</b> — the Assets the Dags produce and consume (§8)</summary>

<!-- BEGIN FILE: src/tagifai/assets.py -->
```python
"""Airflow 3 Assets shared by the Dags.

Assets replace Airflow 2's Datasets and are how the Dags in this tutorial are
wired together: the DataOps Dag *produces* `labeled_projects`, and the MLOps Dag
is scheduled on it (`schedule=[LABELED_PROJECTS]`) instead of guessing a cron
that runs "some time after" DataOps finishes.

An Asset is identified by a name and a URI. Here the assets are name-only, which
keeps them identical whether you run in Docker or natively; in production give
them the real URI of the data (`s3://...`, `bq://...`) so lineage in the UI
points somewhere.
"""

from __future__ import annotations

from airflow.sdk import Asset

# Landing-zone copy of the source data, produced by `dataops`.
RAW_PROJECTS = Asset(
    name="raw_projects",
    group="dataset",
    extra={"layer": "raw", "owner": "data-engineering"},
)

# The curated table the ML Dag trains on. Produced by `dataops`,
# consumed (as a schedule) by `mlops`.
LABELED_PROJECTS = Asset(
    name="labeled_projects",
    group="dataset",
    extra={"layer": "warehouse", "owner": "data-engineering"},
)

# Produced by `mlops` when a model clears the promotion gate. `monitoring`
# reacts to it.
PRODUCTION_MODEL = Asset(
    name="production_model",
    group="model",
    extra={"owner": "ml-engineering"},
)
```
<!-- END FILE: src/tagifai/assets.py -->

</details>

<details>
<summary><b>dags/01_hello_airflow3.py</b> — a Dag, tasks, dependencies, params (§5)</summary>

<!-- BEGIN FILE: dags/01_hello_airflow3.py -->
```python
"""Lesson 1 - a Dag, tasks, and dependencies in Airflow 3.

Everything a Dag author needs now comes from one place: `airflow.sdk`. That is
the Task SDK, and it is the *only* supported authoring surface in Airflow 3 -
`airflow.decorators`, `airflow.models.dag`, `airflow.operators.*` are gone or
deprecated.

Run it without a scheduler:
    airflow dags test hello_airflow3
"""

from __future__ import annotations

import pendulum

# Airflow 2:  from airflow.decorators import dag, task
# Airflow 3:
from airflow.sdk import Param, dag, task

DEFAULT_ARGS = {
    "owner": "mlops-course",
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=30),
}


@dag(
    dag_id="hello_airflow3",
    # Airflow 2 called this `schedule_interval`. That argument is REMOVED.
    # `schedule` takes None, a cron string, a timedelta, a Timetable, or a
    # list of Assets (see 03_dataops / 04_mlops).
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    # Airflow 3 default is already False, but being explicit is good manners:
    # it is the flag that decides whether activating a Dag floods you with
    # historical runs.
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "basics"],
    doc_md=__doc__,
    params={
        # Params render as a form when you trigger the Dag from the UI.
        "greeting": Param("hello", type="string", description="What to say."),
    },
)
def hello_airflow3():
    """A three-task Dag: one source, two branches of work, one join."""

    @task
    def make_greeting(**context) -> str:
        # Context keys are injected by name. `logical_date` replaces Airflow 2's
        # `execution_date`, which no longer exists in the context at all.
        #
        # Read it with `.get()`, never `context["logical_date"]`: in Airflow 3 a
        # manual or asset-triggered run has no logical date, and the key is then
        # missing from the context entirely - indexing it raises KeyError at
        # runtime while `airflow dags test` (which always supplies one) stays
        # green. Use `run_id` when you need something that is always there.
        greeting = context["params"]["greeting"]
        when = context.get("logical_date") or "no logical date (manual run)"
        return f"{greeting} from run {context['run_id']} at {when}"

    @task
    def shout(message: str) -> str:
        return message.upper()

    @task
    def count_words(message: str) -> int:
        return len(message.split())

    @task
    def report(loud: str, words: int) -> None:
        # Anything you `print`/log lands in the task log, viewable in the UI.
        print(f"{loud}  ({words} words)")

    # Calling the task functions builds the dependency graph. The return value
    # of a task is an XComArg - a lazy reference to its XCom, not the value -
    # so passing it to another task is what creates the edge.
    message = make_greeting()
    report(loud=shout(message), words=count_words(message))


# The decorator only defines the Dag; calling it registers it.
hello_airflow3()
```
<!-- END FILE: dags/01_hello_airflow3.py -->

</details>

<details>
<summary><b>dags/02_operators_and_xcoms.py</b> — operators, XComs, relationships (§6)</summary>

<!-- BEGIN FILE: dags/02_operators_and_xcoms.py -->
```python
"""Lesson 2 - operators, XComs, and the import moves that break Airflow 2 Dags.

Two things bite everyone porting a Dag to Airflow 3:

1. `BashOperator`, `PythonOperator`, `EmptyOperator`, `FileSensor` and friends
   left airflow-core. They now live in `apache-airflow-providers-standard`
   (pre-installed, but the import path changed):

       Airflow 2: from airflow.operators.bash   import BashOperator
       Airflow 3: from airflow.providers.standard.operators.bash import BashOperator

2. `xcom_pull()` without `task_ids` now pulls only from the *current* task.
   In Airflow 2 it silently searched the whole Dag run and handed back the most
   recent push. Any Dag relying on that returns None here - loudly wrong instead
   of quietly wrong.

The tail of the Dag is a tour of the ways to declare **relationships**: `>>`,
fan-out and fan-in with a list, `chain()` with `Label()`, and
`cross_downstream()` for an all-to-all edge set.

Run it:
    airflow dags test operators_and_xcoms
"""

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
    """Classic operator style: push an explicit key."""
    ti.xcom_push(key="seed", value=7)


def _multiply(ti, factor: int) -> None:
    # REQUIRED in Airflow 3: name the task you are pulling from.
    seed = ti.xcom_pull(task_ids="push_seed", key="seed")
    ti.xcom_push(key="product", value=seed * factor)


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

    # Templated fields still work the same way; only the variable names changed.
    # `ds` and `ts` survive, `execution_date`/`next_ds`/`tomorrow_ds` do not.
    #
    # But `ds` is derived from `logical_date`, and a manual or asset-triggered
    # run in Airflow 3 has none - the variable is then *undefined*, and
    # `{{ ds }}` fails the task with `UndefinedError: 'ds' is undefined`. Note
    # that `airflow dags test` always supplies a logical date, so this only
    # breaks once a real trigger reaches it. Guard it with `default`, or
    # template `{{ dag_run.run_after }}`, which every run has.
    describe = BashOperator(
        task_id="describe",
        bash_command=(
            'echo "run={{ run_id }} ds={{ ds | default(\'none (manual run)\') }} '
            'run_after={{ dag_run.run_after }} '
            'product={{ ti.xcom_pull(task_ids=\'multiply\', key=\'product\') }}"'
        ),
    )

    @task
    def taskflow_reader(**context) -> int:
        """TaskFlow can read operator XComs, and vice versa - they mix freely."""
        product = context["ti"].xcom_pull(task_ids="multiply", key="product")
        print(f"TaskFlow saw product={product}")
        return product

    # --- Relationships ------------------------------------------------------
    # `chain` is the readable form of `a >> b >> c`; `Label` annotates the edge
    # in the graph view. Only `chain` can carry a Label - `>>` cannot.
    chain(start, push_seed, Label("seed -> product"), multiply, describe)

    # `>>` between single tasks. TaskFlow would have drawn this edge implicitly
    # if `describe` returned the value the reader needs; it does not, so the
    # dependency is declared by hand.
    product = taskflow_reader()
    describe >> product

    check_parity = BashOperator(task_id="check_parity", bash_command='echo "parity checked"')
    check_range = BashOperator(task_id="check_range", bash_command='echo "range checked"')

    # Fan-out and fan-in on one line: a list on either side of `>>` broadcasts
    # the edge, so this is four dependencies, not two.
    product >> [check_parity, check_range] >> EmptyOperator(task_id="checks_done")

    publish = EmptyOperator(task_id="publish")
    notify = EmptyOperator(task_id="notify")

    # All-to-all: every check upstream of every sink, four more edges.
    # `chain([a, b], [c, d])` would pair them element-wise instead (a->c, b->d,
    # and it raises if the lists differ in length), and `[a, b] >> [c, d]` is a
    # TypeError - Python has no list-to-list operator to dispatch on.
    cross_downstream([check_parity, check_range], [publish, notify])


operators_and_xcoms()
```
<!-- END FILE: dags/02_operators_and_xcoms.py -->

</details>

<details>
<summary><b>dags/03_dataops.py</b> — extract → validate → transform → validate, and the outlet (§9)</summary>

<!-- BEGIN FILE: dags/03_dataops.py -->
```python
"""Lesson 3 - the DataOps Dag: extract -> validate -> transform -> validate.

Same shape as the original lesson (Airbyte EL, Great Expectations checkpoints,
dbt transforms), with the vendor pieces swapped for local equivalents so it runs
anywhere. Every task is a drop-in slot:

    extract_projects   -> AirbyteTriggerSyncOperator / FivetranOperator / an EL job
    validate_*         -> GreatExpectationsOperator / SodaOperator / dbt test
    transform          -> DbtCloudRunJobOperator / astronomer-cosmos task group

What is genuinely Airflow 3 here:

* The Dag **produces Assets** (`outlets=[...]`). Finishing the producing task
  emits an asset event, which is what schedules the MLOps Dag. No cron guessing,
  no ExternalTaskSensor, no polling.
* Asset events carry metadata (`outlet_events[asset].extra`), so the consumer
  can see the row count that triggered it.
* Validation results move through XCom as small dicts; the dataframes never do.

Run it:
    airflow dags test dataops
"""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.sdk import dag, task, task_group

# `airflow.exceptions` still resolves but is deprecated for task code in
# Airflow 3 - the Task SDK is the supported namespace, and it is the only one
# available to remote/Edge workers that do not install airflow-core.
from airflow.sdk.exceptions import AirflowFailException

from tagifai import config, data, validate
from tagifai.assets import LABELED_PROJECTS, RAW_PROJECTS

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}


@dag(
    dag_id="dataops",
    # Airflow 3 defaults to CronTriggerTimetable: the run fires *at* 03:00 and
    # `logical_date` is 03:00 - it is no longer shifted back by one interval the
    # way CronDataIntervalTimetable did in Airflow 2.
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "dataops"],
    doc_md=__doc__,
)
def dataops():
    @task(outlets=[RAW_PROJECTS])
    def extract_projects(*, outlet_events) -> str:
        """Land the source data. Returns a path, not a dataframe."""
        config.ensure_dirs()
        fp = data.extract()
        df = data.load_raw(fp)
        outlet_events[RAW_PROJECTS].extra = {"rows": len(df), "path": str(fp)}
        return str(fp)

    @task
    def validate_raw(fp: str) -> dict[str, Any]:
        """Quality gate on the landing zone."""
        result = validate.run_suite(data.load_raw(fp), "raw_projects")
        print(result.summary())
        if not result.passed:
            # AirflowFailException stops retries: a data-quality break will not
            # fix itself by running the same task again.
            raise AirflowFailException(f"Raw data failed validation: {result.failures}")
        return {"suite": result.suite, "results": result.results}

    @task
    def transform(fp: str) -> str:
        """Raw -> curated `labeled_projects` (the dbt model of the original lesson)."""
        df = data.transform(data.load_raw(fp))
        out = data.save_labeled(df)
        print(f"Wrote {len(df)} rows to {out}")
        return str(out)

    @task(outlets=[LABELED_PROJECTS])
    def validate_transformed(fp: str, *, outlet_events) -> dict[str, Any]:
        """Final gate. Only a passing run emits the asset event.

        Placing the outlet on the *validation* task, not the transform task, is
        the whole trick: downstream Dags are then scheduled on
        "good data arrived", never on "a task finished".
        """
        df = data.load_labeled(fp)
        result = validate.run_suite(df, "labeled_projects")
        print(result.summary())
        if not result.passed:
            raise AirflowFailException(f"Labeled data failed validation: {result.failures}")

        outlet_events[LABELED_PROJECTS].extra = {
            "rows": len(df),
            "tags": sorted(df["tag"].unique().tolist()),
            "path": fp,
            "warnings": [r["expectation"] for r in result.failures],
        }
        return {"suite": result.suite, "rows": len(df), "results": result.results}

    @task_group(group_id="quality_report")
    def quality_report(raw_report: dict[str, Any], curated_report: dict[str, Any]):
        """Task groups replace SubDags, which were removed in Airflow 3."""

        @task
        def summarize(raw: dict[str, Any], curated: dict[str, Any]) -> None:
            for report in (raw, curated):
                failed = [r["expectation"] for r in report["results"] if not r["passed"]]
                print(f"{report['suite']}: {len(report['results'])} checks, failed={failed or 'none'}")

        summarize(raw_report, curated_report)

    raw_fp = extract_projects()
    raw_report = validate_raw(raw_fp)

    # `transform` waits on the raw gate even though it does not use its output.
    curated_fp = transform(raw_fp)
    raw_report >> curated_fp

    curated_report = validate_transformed(curated_fp)
    quality_report(raw_report, curated_report)


dataops()
```
<!-- END FILE: dags/03_dataops.py -->

</details>

<details>
<summary><b>dags/04_mlops.py</b> — asset-triggered training, mapping, promotion gate (§10)</summary>

<!-- BEGIN FILE: dags/04_mlops.py -->
```python
"""Lesson 4 - the MLOps Dag: asset-triggered training, tuning, and promotion.

This is the payoff of Airflow 3's data-aware scheduling. The Dag has no cron.
It runs when `dataops` emits the `labeled_projects` asset event - which only
happens when the data passed validation.

Also shown:

* **A validation gate before training** - `dataops` validated the warehouse
  table; this validates the *training snapshot*, including the split, which no
  table-level suite can see.
* **Dynamic task mapping** - one `train_candidate` task instance per
  hyperparameter set, fanned out at runtime, then reduced by `select_best`.
* **Branching as a promotion gate** - a model that misses the f1 threshold does
  not reach production, and the Dag run still succeeds (a rejected candidate is
  a normal outcome, not a failure).
* **XCom discipline** - tasks exchange run ids and metric dicts. The models and
  dataframes live in the registry and on disk.

Deadline alerts (the replacement for SLAs) live in `07_deadline_alerts.py` -
see the note there for why they are not attached to this Dag.

Run it:
    airflow dags test mlops
"""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException

from tagifai import config, data, registry, train as train_module, validate
from tagifai.assets import LABELED_PROJECTS, PRODUCTION_MODEL

DEFAULT_ARGS = {
    "owner": "ml-engineering",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=2),
}


@dag(
    dag_id="mlops",
    # The whole point: schedule on data, not on time. Pass several assets for
    # AND semantics, or AssetAny(a, b) for OR.
    schedule=[LABELED_PROJECTS],
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "mlops"],
    doc_md=__doc__,
)
def mlops():
    @task
    def prepare_data(*, triggering_asset_events) -> dict[str, int]:
        """Inspect what triggered us, then materialise the train/test split."""
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")

        df = data.load_labeled()
        train_df, test_df = data.split(df)
        return {"num_train": len(train_df), "num_test": len(test_df)}

    @task
    def validate_training_data(split_sizes: dict[str, int]) -> dict[str, Any]:
        """The gate between the warehouse and the trainer.

        `dataops` already validated `labeled_projects` - but that was a
        different Dag run, possibly hours ago, and this run may have been
        triggered by hand. What the model is fitted on is the snapshot on disk
        *now*, plus a split that no single-frame suite can inspect, so both are
        checked here.

        The dataframe is re-read rather than passed from `prepare_data`: XComs
        carry metadata, never payloads.
        """
        df = data.load_labeled()
        table = validate.run_suite(df, "labeled_projects")
        split = validate.check_split(*data.split(df))
        print(f"{table.summary()} {split.summary()}")

        if not (table.passed and split.passed):
            # AirflowFailException, not AirflowException: a bad snapshot will
            # not become good by retrying the same task.
            raise AirflowFailException(
                f"Training snapshot failed validation: {table.failures + split.failures}"
            )
        return {
            "rows": len(df),
            "split_sizes": split_sizes,
            "warnings": [r["expectation"] for r in table.failures + split.failures],
            "results": table.results + split.results,
        }

    @task
    def candidates() -> list[dict[str, Any]]:
        """The search space. Returning a list is what `.expand()` maps over."""
        return train_module.SEARCH_SPACE

    @task(max_active_tis_per_dag=2)
    def train_candidate(hparams: dict[str, Any], **context) -> dict[str, Any]:
        """One task instance per hyperparameter set.

        The argument is named `hparams`, not `params`: TaskFlow injects context
        keys by parameter name, and `params` is a reserved context key.
        """
        config.ensure_dirs()
        run_id = f"{context['run_id']}__{context['ti'].map_index}".replace(":", "-").replace("+", "-")
        model, metrics = train_module.train_from_files(hparams)
        registry.save_run(run_id, model, metrics)
        print(f"run={run_id} hparams={hparams} f1={metrics['f1']:.4f}")
        # Small, JSON-serialisable payload. The model itself stays in the registry.
        return {"run_id": run_id, "f1": metrics["f1"], "hparams": hparams}

    @task
    def select_best(results: list[dict[str, Any]]) -> dict[str, Any]:
        """The reduce step: every mapped instance's return value arrives as a list."""
        best = max(results, key=lambda r: r["f1"])
        print(f"best={best['run_id']} f1={best['f1']:.4f} of {len(results)} candidates")
        return best

    @task
    def evaluate_best(best: dict[str, Any]) -> dict[str, Any]:
        """Re-read the full metrics, including per-class and slice performance."""
        metrics = registry.load_metrics(best["run_id"])
        for name, slice_metrics in metrics.get("slices", {}).items():
            print(f"slice {name}: f1={slice_metrics['f1']:.4f} n={slice_metrics['support']}")
        return {"run_id": best["run_id"], "metrics": metrics}

    @task.branch
    def promotion_gate(evaluation: dict[str, Any]) -> str:
        """Return the task_id to run. The other branch is skipped, not failed."""
        f1 = evaluation["metrics"]["f1"]
        threshold = config.MIN_F1_TO_PROMOTE
        current = registry.get_production()
        current_f1 = current["metrics"]["f1"] if current else 0.0

        if f1 < threshold:
            print(f"f1={f1:.4f} below threshold {threshold} - not promoting")
            return "keep_current_model"
        if f1 < current_f1:
            print(f"f1={f1:.4f} below production {current_f1:.4f} - not promoting")
            return "keep_current_model"
        return "promote_model"

    @task(outlets=[PRODUCTION_MODEL])
    def promote_model(evaluation: dict[str, Any], *, outlet_events) -> dict[str, Any]:
        pointer = registry.promote(evaluation["run_id"])
        outlet_events[PRODUCTION_MODEL].extra = {
            "run_id": pointer["run_id"],
            "f1": pointer["metrics"]["f1"],
            "previous_run_id": pointer["previous_run_id"],
        }
        print(f"promoted {pointer['run_id']} (was {pointer['previous_run_id']})")
        return pointer

    keep_current_model = EmptyOperator(task_id="keep_current_model")

    # `none_failed_min_one_success` lets the join run whichever branch was taken;
    # the default `all_success` would skip it after any branch skip.
    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")

    snapshot = validate_training_data(prepare_data())
    results = train_candidate.expand(hparams=candidates())
    # No data flows from the gate to the trainer, only permission to run: every
    # mapped instance waits on it.
    snapshot >> results

    evaluation = evaluate_best(select_best(results))
    gate = promotion_gate(evaluation)

    gate >> promote_model(evaluation) >> done
    gate >> keep_current_model >> done


mlops()
```
<!-- END FILE: dags/04_mlops.py -->

</details>

<details>
<summary><b>dags/05_sensors_and_events.py</b> — sensors, deferrable tasks, watchers (§11)</summary>

<!-- BEGIN FILE: dags/05_sensors_and_events.py -->
```python
"""Lesson 5 - waiting for things: sensors, deferrable operators, and events.

Three ways to react to the outside world, cheapest last:

1. **Poking sensors** - a worker slot is held while polling. Fine for seconds.
2. **Deferrable sensors** (`*Async`, or `deferrable=True`) - the task suspends,
   the slot is released, and the triggerer resumes it. This is what you want for
   anything measured in minutes or hours.
3. **Event-driven scheduling** (`AssetWatcher`) - no task waits at all: an
   external event emits an asset update and *that* starts the Dag run. Airflow 3
   only; see the note at the bottom.

Run it:
    airflow dags test sensors_and_events
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensorAsync
from airflow.sdk import PokeReturnValue, dag, task

from tagifai import config

DEFAULT_ARGS = {"owner": "data-engineering", "retries": 0}


@dag(
    dag_id="sensors_and_events",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "sensors"],
    doc_md=__doc__,
)
def sensors_and_events():
    # Deferrable: releases its worker slot while waiting. Needs `airflow triggerer`.
    settle = TimeDeltaSensorAsync(task_id="settle", delta=timedelta(seconds=5))

    # `reschedule` mode is the non-deferrable way to free a slot between pokes:
    # the task instance exits and is re-queued each interval.
    wait_for_curated_data = FileSensor(
        task_id="wait_for_curated_data",
        filepath=str(config.LABELED_PROJECTS_FP),
        poke_interval=10,
        timeout=60 * 5,
        mode="reschedule",
    )

    @task.sensor(poke_interval=10, timeout=120, mode="reschedule")
    def enough_rows() -> PokeReturnValue:
        """A sensor written as a function.

        Return `PokeReturnValue(is_done=..., xcom_value=...)` to both stop poking
        and hand a value to downstream tasks.
        """
        if not config.LABELED_PROJECTS_FP.exists():
            return PokeReturnValue(is_done=False)
        rows = sum(1 for _ in config.LABELED_PROJECTS_FP.open(encoding="utf-8")) - 1
        print(f"curated table has {rows} rows")
        return PokeReturnValue(is_done=rows >= 500, xcom_value=rows)

    @task
    def proceed(rows: int) -> None:
        print(f"Data is ready ({rows} rows). Downstream work can start.")

    settle >> wait_for_curated_data >> proceed(enough_rows())


sensors_and_events()


# --- Event-driven scheduling (Airflow 3) -------------------------------------
#
# Instead of a task that waits, attach a watcher to an Asset. The trigger runs in
# the triggerer; when it fires, the asset updates and every Dag scheduled on that
# asset starts. Nothing occupies a worker slot in the meantime.
#
#     from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
#     from airflow.sdk import Asset, AssetWatcher, dag
#
#     trigger = MessageQueueTrigger(queue="https://sqs.us-east-1.amazonaws.com/0123456789/new-projects")
#     new_projects = Asset(
#         name="new_projects",
#         watchers=[AssetWatcher(name="sqs_watcher", trigger=trigger)],
#     )
#
#     @dag(schedule=[new_projects], ...)
#     def ingest_on_message():
#         ...
#
# Only triggers deriving from `BaseEventTrigger` can be used as watchers -
# a trigger that tests a permanent condition ("file exists") would re-fire
# forever.
```
<!-- END FILE: dags/05_sensors_and_events.py -->

</details>

<details>
<summary><b>dags/06_monitoring_and_retraining.py</b> — drift, branching, human-in-the-loop (§12)</summary>

<!-- BEGIN FILE: dags/06_monitoring_and_retraining.py -->
```python
"""Lesson 6 - continual learning: monitor, decide, and act.

The original lesson ends on four responses to a monitoring event. This Dag makes
them explicit branches:

    continue  keep serving; log the alert and move on
    improve   retrain on new data (triggers the mlops Dag)
    inspect   the label space or schema changed - a human should look
    rollback  live performance collapsed - revert to the previous model

Airflow 3 pieces on show:

* `@task.branch` picking one arm of the response.
* `ApprovalOperator` (human-in-the-loop, Airflow 3.1+): retraining waits for a
  person to click Approve in the UI. `defaults` + `response_timeout` mean an
  unattended run still resolves instead of hanging forever.
* `TriggerDagRunOperator` for cross-Dag orchestration.

Run it:
    airflow dags test monitoring
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task

from tagifai import monitoring, registry

DEFAULT_ARGS = {"owner": "ml-engineering", "retries": 1}

# Thresholds that turn a measurement into a decision. Exposed as Dag params so
# the policy is visible in the UI, overridable per run, and recorded with the
# run that used it:
#     airflow dags test monitoring --conf '{"max_label_tvd": 0.0}'
POLICY_PARAMS = {
    "max_label_tvd": Param(0.15, type="number", description="Label-drift budget."),
    "max_f1_drop": Param(0.10, type="number", description="f1 drop that triggers retraining."),
    "critical_f1_drop": Param(0.25, type="number", description="f1 drop that triggers rollback."),
}


@dag(
    dag_id="monitoring",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "mlops", "monitoring"],
    doc_md=__doc__,
    params=POLICY_PARAMS,
)
def monitoring_dag():
    @task
    def measure() -> dict[str, Any]:
        report = monitoring.monitor()
        drift = report["drift"]
        print(f"label TVD={drift['label_tvd']:.4f} new_labels={drift['new_labels']}")
        if report["performance"]:
            perf = report["performance"]
            print(f"f1 at promotion={perf['f1_at_promotion']:.4f} now={perf['f1_now']:.4f}")
        return report

    @task.branch
    def decide(report: dict[str, Any], **context) -> str:
        drift, performance = report["drift"], report["performance"]
        policy = context["params"]

        if performance is None:
            print("No production model yet - train one.")
            return "request_retraining"

        f1_drop = performance["f1_at_promotion"] - performance["f1_now"]
        print(f"f1_drop={f1_drop:.4f} label_tvd={drift['label_tvd']:.4f} policy={dict(policy)}")

        if f1_drop >= policy["critical_f1_drop"]:
            return "rollback"
        if drift["new_labels"]:
            # An unseen label means the schema/label space moved. Retraining on
            # data you do not understand yet is how you ship a worse model.
            return "inspect"
        if f1_drop >= policy["max_f1_drop"] or drift["label_tvd"] >= policy["max_label_tvd"]:
            return "request_retraining"
        return "continue_serving"

    continue_serving = EmptyOperator(task_id="continue_serving")

    @task
    def inspect(report: dict[str, Any]) -> None:
        print("MANUAL INSPECTION NEEDED")
        print(f"  new labels: {report['drift']['new_labels']}")
        print(f"  current distribution: {report['drift']['current_distribution']}")

    @task
    def rollback() -> dict[str, Any] | None:
        pointer = registry.rollback()
        print(f"rolled back to {pointer['run_id']}" if pointer else "nothing to roll back")
        return pointer

    # Human-in-the-loop gate. The task sits in `awaiting_input` and shows a
    # prompt in the UI; on Reject, everything downstream is skipped.
    request_retraining = ApprovalOperator(
        task_id="request_retraining",
        subject="Retrain the tag classifier?",
        body=(
            "Monitoring detected drift or a performance drop.\n\n"
            "Approve to trigger the `mlops` Dag on the current data."
        ),
        # No answer within the window resolves to `defaults`, so an unattended
        # run still completes. Give it hours in production; kept short here so
        # `airflow dags test monitoring` finishes.
        defaults="Approve",
        response_timeout=timedelta(seconds=60),
    )

    trigger_retraining = TriggerDagRunOperator(
        task_id="trigger_retraining",
        trigger_dag_id="mlops",
        conf={"reason": "monitoring_drift"},
        wait_for_completion=False,
        reset_dag_run=True,
    )

    report = measure()
    branch = decide(report)

    branch >> continue_serving
    branch >> inspect(report)
    branch >> rollback()
    branch >> request_retraining >> trigger_retraining


monitoring_dag()
```
<!-- END FILE: dags/06_monitoring_and_retraining.py -->

</details>

<details>
<summary><b>dags/07_deadline_alerts.py</b> — deadline alerts, the SLA replacement (§13)</summary>

<!-- BEGIN FILE: dags/07_deadline_alerts.py -->
```python
"""Lesson 7 - Deadline Alerts, the Airflow 3 replacement for SLAs.

SLAs were **removed** in Airflow 3.0 and replaced by Deadline Alerts in 3.1.
The difference is not cosmetic:

    Airflow 2 SLA          per-task, measured from the Dag run's start, fired
                           after the fact, configured with `sla=timedelta(...)`
                           plus a Dag-level `sla_miss_callback`.

    Airflow 3 deadline     per-Dag-run, measured from a reference point you
                           choose, evaluated while the run is still going, and
                           the callback is a first-class object.

Three mandatory pieces: a **reference** (where the clock starts), an
**interval** (how long after it), and a **callback**.

    DeadlineReference.DAGRUN_QUEUED_AT       when the run was queued
    DeadlineReference.DAGRUN_LOGICAL_DATE    the run's logical date
    DeadlineReference.FIXED_DATETIME(dt)     an absolute wall-clock time
    DeadlineReference.AVERAGE_RUNTIME(...)   this Dag's own recent average

    SyncCallback   runs on the executor
    AsyncCallback  runs on the triggerer - use this for Notifiers
                   (e.g. AsyncCallback(SlackWebhookNotifier, kwargs={...}))

This Dag deliberately misses its own 30-second deadline, so the callback in
`tagifai/callbacks.py` fires and you can find it in the scheduler/executor logs.

    docker compose logs -f airflow-scheduler | grep "DEADLINE MISSED"

KNOWN ISSUE (verified on Airflow 3.3.1): `airflow dags test <dag_id>` crashes on
any Dag with a `deadline=`, with
`psycopg2.ProgrammingError: can't adapt type 'dict'` raised from
`DagRun.update_state`. The tasks themselves run to completion first - only the
CLI's final state update fails. Trigger this Dag through the scheduler instead:

    airflow dags unpause deadline_alerts
    airflow dags trigger deadline_alerts
"""

from __future__ import annotations

import time
from datetime import timedelta

import pendulum
from airflow.sdk import DeadlineAlert, DeadlineReference, SyncCallback, dag, task


@dag(
    dag_id="deadline_alerts",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "ml-engineering"},
    tags=["tutorial", "monitoring"],
    doc_md=__doc__,
    # Pass a list to attach several deadlines with different budgets.
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(seconds=30),
        callback=SyncCallback(
            "tagifai.callbacks.deadline_missed",
            kwargs={"dag_id": "deadline_alerts", "budget": "30s"},
        ),
    ),
)
def deadline_alerts():
    @task
    def slow_step() -> str:
        """Takes longer than the deadline on purpose."""
        time.sleep(75)
        return "finished, but late"

    slow_step()


deadline_alerts()
```
<!-- END FILE: dags/07_deadline_alerts.py -->

</details>

The rest of `src/tagifai/` — `data.py`, `validate.py`, `train.py`,
`evaluate.py`, `registry.py`, `monitoring.py`, `predict.py`, `config.py`,
`callbacks.py` — is the ML project being orchestrated, not the orchestration, and
imports no Airflow. It is in the repo; it is not reproduced here.

---

## What was actually verified

Against a live `apache/airflow:3.3.1` stack (Postgres 16, LocalExecutor,
api-server + scheduler + dag-processor + triggerer), on 2026-09-09:

- All 7 Dags parse with **zero import errors** (`airflow dags list-import-errors`).
- `airflow dags test` runs green for `hello_airflow3`, `operators_and_xcoms`,
  `dataops`, `mlops`, `sensors_and_events`, `monitoring`.
- The DataOps → Asset → MLOps chain fires for real: triggering `dataops`
  produced an `asset_triggered__…` run of `mlops` with no manual trigger.
- `mlops` trained 4 mapped candidates, selected the best (f1 = 0.884) and
  promoted it.
- `mlops` refused nothing on the way: `validate_training_data` reported
  `suite=labeled_projects passed=6/6 suite=training_split passed=5/5` before any
  candidate was trained.
- The HITL path resolved: `awaiting_input` → default `Approve` after the
  timeout → `TriggerDagRunOperator` fired.
- The deadline callback fired at exactly +30s with the full `dag_run` context.
- Manual triggers were exercised for real: a trigger against a **paused** Dag
  produced a run stuck in `queued` until `airflow dags unpause`, and
  `-c '{"greeting": "hola"}'` reached the task as a `params` override.
- The §4 UI walkthrough was checked against this stack's api-server, not
  remembered: nav, tabs, trigger/backfill form fields and the version selector
  come from the shipped UI strings, and the endpoints behind them
  (`/api/v2/assets`, `/backfills`, `/dags/{id}/dagVersions`, `/hitlDetails`)
  from its OpenAPI spec.
- `pytest`: **27 passed**.

Five real defects were found while building this, all documented in
[troubleshooting](docs/troubleshooting.md): `fs_default` is no longer
auto-created, `airflow dags test` breaks on Dags with a `deadline=`, `DagBag`
moved and lost `include_examples`, and — found by the manual triggers above,
after every test in §14 had passed — `context["logical_date"]` raises `KeyError`
and `{{ ds }}` raises `UndefinedError` on any run without a logical date.

## Repository layout

```
dags/                       the seven lesson Dags, numbered in reading order
src/tagifai/                the ML project Airflow orchestrates (imports no Airflow)
  config.py                 paths and thresholds
  data.py                   extract / clean / transform / split
  validate.py               a tiny expectation-suite runner
  train.py  evaluate.py     TF-IDF + SGD classifier, metrics and slices
  registry.py               filesystem stand-in for a model registry
  monitoring.py             drift and live-performance checks
  assets.py                 the Airflow Assets the Dags share
  callbacks.py              deadline-alert callbacks
tests/                      Dag integrity tests + unit tests
docs/                       migration reference, CLI cheatsheet, troubleshooting
scripts/                    up / reset / native setup
docker-compose.yaml         LocalExecutor + Postgres + Simple auth
```

## Further reading

- [Airflow 3 documentation](https://airflow.apache.org/docs/apache-airflow/stable/)
- [Upgrading to Airflow 3](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html)
- [Task SDK reference](https://airflow.apache.org/docs/task-sdk/stable/api.html)
- [Assets and data-aware scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html)
- [The original Made With ML lesson](https://madewithml.com/courses/mlops/orchestration/)
