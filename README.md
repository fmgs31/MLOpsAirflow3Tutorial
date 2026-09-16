# Learn Apache Airflow 3 by building an ML pipeline

A hands-on tutorial for **Airflow 3.3.1**. You will write seven Dags, one file
at a time, that grow into one working system: data is extracted, validated and
transformed, that publishes an **Asset**, the Asset triggers model training,
the best model is promoted to production, and a monitoring Dag decides whether
to retrain.

Everything runs on your machine in Docker. No cloud account, no warehouse, no
vendor signup. Every command below was executed against a real Airflow 3.3.1
stack — see [What was actually verified](#17-what-was-actually-verified).

**Assumed:** Python, the command line, and Docker installed. No Airflow
experience — this teaches Airflow 3 as it is today, and nothing about older
versions.

**Time:** ~30 minutes to your first Dag run. A day for all seven if you type
them yourself, which is the point.

---

## Contents

1. [What you're building](#1-what-youre-building)
2. [Get it running](#2-get-it-running)
3. [The pieces of Airflow](#3-the-pieces-of-airflow)
4. [Your first Dag](#4-your-first-dag)
5. [The UI](#5-the-ui)
6. [Operators, XComs and relationships](#6-operators-xcoms-and-relationships)
7. [Scheduling and logical dates](#7-scheduling-and-logical-dates)
8. [Assets: scheduling on data, not on time](#8-assets-scheduling-on-data-not-on-time)
9. [The DataOps Dag](#9-the-dataops-dag)
10. [The MLOps Dag](#10-the-mlops-dag)
11. [Waiting: sensors, deferrable tasks, events](#11-waiting-sensors-deferrable-tasks-events)
12. [Continual learning: monitor, decide, act](#12-continual-learning-monitor-decide-act)
13. [Deadline alerts](#13-deadline-alerts)
14. [Testing Dags](#14-testing-dags)
15. [Taking it to production](#15-taking-it-to-production)
16. [Appendix: the finished Dags in full](#16-appendix-the-finished-dags-in-full)
17. [What was actually verified](#17-what-was-actually-verified)

Reference material: [CLI cheatsheet](docs/cheatsheet.md) ·
[Troubleshooting](docs/troubleshooting.md)

---

## 1. What you're building

Orchestration turns a pile of scripts you run by hand into a system: the work is
declared as a graph of tasks, something else decides when to run it, retries the
parts that fail, and records what happened.

A workflow in Airflow is a **Dag** — a directed acyclic graph. *Directed*: edges
have a direction. *Acyclic*: no loops, so a finished task never re-runs because
of something downstream. Each node is a **task**, and one execution of the whole
graph is a **Dag run**.

Those last two words are worth separating now, because every log line and error
message assumes you have. A **task** is the thing you wrote once, in the file. A
**task instance** is that task inside one particular Dag run — the thing that
actually has a state, a duration, a log and a number of attempts. You write
tasks; you debug task instances. §3 follows one through its whole life.

Three Dags do the real work here, and the interesting part is how they connect:

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

`mlops` has **no schedule**. It runs when `dataops` publishes good data. That is
data-aware scheduling, and it is the idea this whole tutorial is built around.

Four more Dags exist to teach one thing each: `hello_airflow3` (a first Dag),
`operators_and_xcoms` (task types and how tasks talk), `sensors_and_events`
(waiting for the outside world) and `deadline_alerts` (being told when a run is
too slow).

### How this tutorial works

You write every one of those seven Dags yourself. The folder Airflow reads,
`dags/`, is **empty** when you clone this repo. Each section explains a piece of
Airflow, then has you build the Dag that uses it — a few lines at a time, with
the reason for each piece next to it — and then run it and check what happened.
The code is given; the work is typing it, seeing it parse, and watching it run.

Two safety nets:

- **[`solutions/`](solutions/)** holds a finished copy of every file, with the
  teaching comments left in. Compare when something does not parse, or skip a
  lesson by copying its file in: `cp solutions/03_dataops.py dags/`. §16 prints
  every solution in full.
- **§14's tests** read *your* `dags/` folder. When all seven are written, they
  tell you whether you built what the tutorial meant.

The lessons build on each other from §9 on — `mlops` is scheduled on an asset
`dataops` produces, `sensors_and_events` waits for a file `dataops` writes — so
do them in order, or copy the earlier solutions in first.

### Why Airflow

Airflow is not the only orchestrator — Prefect, Dagster, Luigi, Metaflow, Flyte,
Kubeflow Pipelines and Vertex AI Pipelines all cover this ground. Airflow is
worth learning first because:

- **Adoption.** It is the default in most data platforms, so the concepts
  transfer and every vendor ships an integration.
- **Pure Python, no DSL.** Workflows are ordinary functions, testable with
  pytest (§14).
- **The same code runs on your laptop and on a cluster** — swap the executor,
  not the Dag.
- **Apache governance**, a large provider ecosystem, and no single vendor.

### The ML project it orchestrates

Airflow orchestrates code; it does not contain it. The code being orchestrated
lives in `src/tagifai/` and imports no Airflow at all — a small text classifier
that tags ML projects. You do not write this part; the Dags call it:

| Module | Role |
|---|---|
| `data.py` | extract, clean, transform, train/test split |
| `validate.py` | data-quality expectations, the thing the gates run |
| `train.py`, `evaluate.py` | TF-IDF + SGD classifier, metrics and slices |
| `registry.py` | filesystem stand-in for a model registry |
| `monitoring.py` | drift and live-performance checks |
| `predict.py` | tag new text with whatever is currently promoted |
| `assets.py` | the Airflow Assets the Dags share |
| `callbacks.py` | deadline-alert callbacks |

Each is a slot you can swap for a real tool — the tables in §9 and §10 say which.

## 2. Get it running

### Docker (recommended, and on Windows the only option outside WSL2)

Airflow has no native Windows support. On Windows use Docker Desktop, or run the
native install inside WSL2.

```bash
git clone <this repo>
cd Airflow3Tutorial
```

Then start the stack. On Linux, macOS, WSL2 or Git Bash on Windows:

```bash
./scripts/up.sh
```

In Windows PowerShell, where a default install refuses to run `.ps1` files,
hence the flag:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\up.ps1
```

**Checkpoint.** The script waits for the API server, then prints the service
table. Open <http://localhost:8080> — there is no login. The Dags list is
**empty**, and it should be: you have not written one yet. The Home page shows
the scheduler, triggerer and Dag processor as healthy. If the page does not
load, jump to [troubleshooting](docs/troubleshooting.md).

```bash
docker compose ps
docker compose exec airflow-scheduler airflow dags list
```

The second prints `No data found` — the same empty list. Those two lines are
the shape of every command from here on. `docker compose …` runs on your
machine; `airflow …` runs inside the scheduler container, which is what the
`docker compose exec airflow-scheduler` prefix does. Where this tutorial shows
an `airflow …` command bare — in a transcript, or as a one-liner in prose — it
means the same thing, prefix included.

The stack is deliberately smaller than the [official
docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/3.3.1/docker-compose.yaml):
`LocalExecutor` instead of Celery + Redis (§3 explains what an executor is), and
the Simple auth manager instead of FAB — Flask-AppBuilder, the provider that
gives Airflow users, roles and a login page. Fewer moving parts while you are
learning.

> **Auth:** `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=true` is set, so the
> UI has no login, and port 8080 is published on `127.0.0.1` only — the loopback
> address — so nothing outside your machine can reach it. Do not deploy this
> configuration anywhere else.

One more script, for later. Everything the stack accumulates — runs, task
logs, the data and models the Dags write — survives a restart of the
containers, because it lives in a Docker volume and in the `data/`,
`model_registry/` and `logs/` folders of this repo (§3 comes back to this). To
throw all of it away and start over from a clean slate, on Linux, macOS, WSL2
or Git Bash:

```bash
./scripts/reset.sh
```

or in Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\reset.ps1
```

It asks before deleting, and `up.sh` / `up.ps1` brings the stack back. It does
not touch `dags/` — the files you write survive a reset. No step in this
tutorial needs it; it is here for the day something is wedged and
[troubleshooting](docs/troubleshooting.md) has not helped.

> **One shell for the rest.** Every command after this point is written for
> bash: Linux, macOS, WSL2, or Git Bash on Windows. The two `.ps1` scripts
> exist so that PowerShell can start and reset the stack, but the tutorial's
> commands use `\` line continuations, `grep` and single-quoted JSON, all of
> which PowerShell reads differently. On Windows, open Git Bash for everything
> below.
>
> On **Git Bash**, a few later commands pass a `/opt/...` path into a
> container, and each carries the prefix `MSYS_NO_PATHCONV=1`. It is not
> decoration: without it Git rewrites the path into a Windows one before Docker
> sees it.

> **Your editor.** You will be creating files in `dags/` from §4 on. Any editor
> works; the container sees the folder through a bind mount, so a file saved on
> your machine is a file inside Airflow a moment later. Nothing needs copying.

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
export AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=5            # see §3: pick up a new file in seconds
export AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL=5

airflow db migrate                              # create the metadata database
airflow connections add fs_default --conn-type fs --conn-extra '{"path": "/"}'
```

Airflow 3.3.1 needs Python ≥ 3.10 (and not 3.15). Install it **with the
constraint file** — Airflow has a large dependency tree and an unconstrained
install will resolve to something that does not work.

`airflow db migrate` populates `$AIRFLOW_HOME`:

```
airflow.cfg     every setting, with the defaults commented — see config/airflow.cfg.example
airflow.db      the SQLite metadata DB
logs/           task logs, one directory per dag_id / run_id / task_id
```

> **SQLite is for development only.** The Airflow docs are blunt about it: it
> "should NEVER be used for production". It is fine for working through this
> tutorial — `LocalExecutor` runs tasks in parallel against it, and needs
> SQLite ≥ 3.15 — but move to Postgres or MySQL for anything real. The Docker
> stack in this repo already runs Postgres 16, which is why the Docker path is
> the recommended one.

Any setting in `airflow.cfg` can also be given as an `AIRFLOW__SECTION__KEY`
environment variable, which is what this repo's compose file does instead of
shipping a config file.

> **Default connections are not created for you.** `fs_default` is added by hand
> above because `FileSensor` (§11) needs it. In Docker the compose file provides
> it as `AIRFLOW_CONN_FS_DEFAULT`.

Then either the all-in-one process:

```bash
airflow standalone
```

…or the components separately, which is what a real deployment looks like:

```bash
airflow api-server --port 8080
airflow scheduler
airflow dag-processor            # a separate process — required
airflow triggerer                # deferrable tasks and async callbacks
```

## 3. The pieces of Airflow

Airflow is a scheduler for batch workflows written in Python. You describe the
graph in code, and Airflow decides when each node runs, retries it, records the
attempt, and shows you the result. It does not move your data and it does not
train your model — it starts the thing that does, waits, and reacts.

That also tells you when to reach for something else. Airflow is a poor fit for
sub-second or per-event work (use a queue or a stream processor), for anything
needing a cycle or a `while` loop across runs, and for heavy compute inside the
worker — trigger a Spark/Ray/Kubernetes job and track it instead. If your
pipeline is three scripts run once a week, cron is still allowed.

Four processes, a database, and an executor — which, as the next section
explains, is not always a process of its own. All of it is running in your stack
right now:

| Component | Job |
|---|---|
| **API server** | Serves the REST API, the React UI, and the **Task Execution API** that workers call |
| **Scheduler** | Decides what to run and hands work to the executor |
| **Dag processor** | Parses your Dag files into the database — its own process |
| **Triggerer** | Runs async triggers for deferred tasks (§11) and async callbacks |
| **Executor** | Where tasks actually run. `LocalExecutor` here; Celery or Kubernetes in production |
| **Metadata DB** | State of everything: runs, task instances, XComs, connections |

That table is the whole system, but a few of those rows decide almost everything
about how your Dags behave, and they are worth more than a row each.

### The scheduler, and why things happen "a moment later"

The scheduler is a loop. It wakes up, asks the metadata database a few questions
— which Dag runs are due, which task instances have all their upstream
dependencies satisfied, which asset events have arrived — marks what it finds as
`queued`, hands those to the executor, and goes back to sleep. Then it does it
again. `[scheduler] scheduler_idle_sleep_time` is how long it sleeps when it
found nothing to do; on this stack it is **1 second**.

This is why nothing in Airflow is instant, and why nothing is very slow either.
When `dataops` publishes its asset in §10, the `mlops` run does not appear
*because* the task finished — it appears because the next scheduler loop noticed
the event the task recorded. With a one-second sleep between loops, that gap is
a second at most, and you will not notice it. What you *will* notice is the time
the upstream tasks themselves take, and that has nothing to do with the
scheduler.

Parsing is a separate clock, and it is the one you are about to depend on. The
Dag processor re-reads a changed file at most every `[dag_processor]
min_file_process_interval` seconds and re-scans the folder for new files every
`refresh_interval`. This stack sets both to **5**, so a file you save shows up
in the UI within a few seconds. The defaults are 30 and 300: on a stack that
keeps them, a brand-new file can take five minutes to appear, which looks
exactly like a broken Dag if you do not know the number exists.

All three numbers are settings, and you can print yours:

```bash
docker compose exec airflow-scheduler airflow config get-value scheduler scheduler_idle_sleep_time
docker compose exec airflow-scheduler airflow config get-value dag_processor min_file_process_interval
docker compose exec airflow-scheduler airflow config get-value dag_processor refresh_interval
```

### The executor, and what "at the same time" means

The executor is the answer to "where does this task's Python actually run". The
scheduler never runs your code; it only decides *that* code should run and hands
the task to the executor, which starts it somewhere and reports back.

`LocalExecutor` — the one in this stack — starts each task as its **own
subprocess on the machine running the scheduler**, using Python's
`multiprocessing`. Real parallelism, one machine's worth of it.
`CeleryExecutor` instead drops the task on a queue for a pool of long-running
worker machines, and `KubernetesExecutor` asks Kubernetes for a fresh pod per
task. Swapping between them changes no Dag code at all — that is the claim in
§1, and this is the mechanism behind it.

Which is why `docker compose ps` shows you no executor container. `LocalExecutor`
is a *component of the scheduler process*, not a service: the scheduler imports
it and it forks children from there. Change `AIRFLOW__CORE__EXECUTOR` to
`CeleryExecutor` and workers become real machines you deploy and scale
separately. The executor is the seam between "one laptop" and "a cluster", and
it is a config value.

"Worker" therefore means something slightly different per executor: a forked
subprocess here, a Celery worker process there, a pod in Kubernetes. What is
constant is that the worker gets the task, runs it, and talks back over the Task
Execution API.

How many run at once is decided by the **narrowest** of four limits, all of them
defaults on this stack:

| Limit | Default here | Scope |
|---|---|---|
| `[core] parallelism` | 32 | task instances running across the whole installation |
| `[core] max_active_tasks_per_dag` | 16 | running task instances in one Dag |
| `default_pool` slots | 128 | tasks not assigned to another pool |
| `max_active_tis_per_dag` on a task | unset | that one task, across all its runs |

(`airflow config get-value core parallelism` prints the first; `airflow pools
list` the third.)

A **pool** is a named bucket of slots you assign tasks to with `pool="…"`
(Admin → Pools), and it is how you stop twenty concurrent tasks from opening
twenty connections to a database that tolerates five. Everything lands in
`default_pool` unless you say otherwise. §10 sets the fourth limit on a single
task and you will watch it bite.

### The metadata database

Every fact Airflow knows lives in one relational database: Dag definitions as
parsed, every Dag run, every task instance and its state and try count, XComs,
Connections, Variables, pools, asset events, the audit log. The UI is a view of
it. The scheduler's loop is a query against it. Restart the containers and
nothing is lost, because the state is in the database volume, not in the
containers — `scripts/reset.sh` deletes that volume, which is why it is the
nuclear option.

Two consequences follow. It is the thing to back up, and it grows forever
unless you run `airflow db clean` (§15) — a year of runs, task instances and
XComs does not remove itself.

### The lifecycle of one task

The vocabulary is exact, and mixing it up is what makes error messages
confusing. A **Dag** is the graph you wrote. A **Dag run** is one execution of
it, identified by a `run_id`, and it is `queued`, `running`, `success` or
`failed` — those four, no others. A **task instance** is one task inside one Dag
run, and it has many more states, because more can happen to it:

```
                                   ┌─► success              done
scheduled ──► queued ──► running ──┼─► failed               done — out of retries
     ▲                             │
     │                             ├─► up_for_retry         waiting out retry_delay
     │                             ├─► up_for_reschedule    a sensor between pokes
     │                             ├─► deferred             parked on the triggerer
     │                             └─► awaiting_input       parked until a person answers
     └─────────────────────────────────┘                    (or the timeout does)
        those four are all "parked, will come back"

  never started: skipped (a branch chose otherwise) · upstream_failed
```

The distinction that catches everyone first is **`queued` vs `running`**:

- **`scheduled`** — the scheduler has decided this task *may* run: its upstream
  tasks are done and its trigger rule is satisfied. Nothing has started.
- **`queued`** — the scheduler has handed it to the executor. Still nothing is
  executing. A task that sits here is waiting on a *slot*, not on your code, so
  the place to look is the four limits above, not the task log — which will be
  empty, because no process has written one yet.
- **`running`** — a worker has it and your Python is executing. Now there are
  logs.

The rest you will meet as you go: `up_for_retry` between attempts (§4),
`skipped` from a branch (§10), `up_for_reschedule` from a sensor that exits
between pokes and `deferred` from one suspended onto the triggerer (§11),
`awaiting_input` while a human decides (§12), and `upstream_failed` on
everything downstream of a failure.

### Task code never touches the database

The architectural point worth internalising: **task code never touches the
metadata database.** A task talks to the Task Execution API over HTTP, which is
what makes remote workers and the Edge executor possible — and why a task cannot
open a database session and query `DagRun` itself.

```bash
curl -s localhost:8080/api/v2/monitor/health
```

(If you are in PowerShell after all, `curl` there is an alias for
`Invoke-WebRequest` and rejects `-s`; call `curl.exe` instead.)

You should get `healthy` four times — one JSON object each for the
metadatabase, scheduler, triggerer and dag processor, the last three with the
time of their most recent heartbeat. The interactive API docs are at
<http://localhost:8080/docs>.

## 4. Your first Dag

Everything a Dag author needs is in `airflow.sdk`, the Task SDK:

```python
from airflow.sdk import dag, task, DAG, Param, Asset, chain, Label
```

You are going to write `dags/01_hello_airflow3.py`: four tasks that build a
greeting, shout it, count its words and print the result. Small enough to hold
in your head, and every idea in it comes back in every later Dag.

### Build it

Create the file `dags/01_hello_airflow3.py` and start it with a docstring, the
imports, and a dictionary of defaults:

```python dags/01_hello_airflow3.py
"""Lesson 1 - a Dag, tasks, and dependencies."""

from __future__ import annotations

import pendulum
from airflow.sdk import Param, dag, task

DEFAULT_ARGS = {
    "owner": "mlops-course",
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=30),
}
```

The docstring is not decoration: in a moment you will pass it as the Dag's
in-UI documentation. `pendulum` is the timezone-aware datetime library Airflow
uses everywhere; a naive `datetime` for a `start_date` is a mistake you would
rather not learn about at 03:00 UTC.

**`default_args` is a dictionary applied to every task in the Dag.** Anything a
task accepts can go in it, and a task that sets the same argument itself wins.
It exists so you do not repeat `owner=` and `retries=` on twenty tasks.

`owner` is a label: it shows in the Dags list, you can filter on it, and it
says who to ask when the Dag breaks. This repo's integrity tests (§14) reject
the default, `airflow`, because an owner that is nobody means nobody gets paged.

The other two are the first thing you will wish you had set:

- **`retries=2`** — if the task raises, Airflow will run it up to two more
  times. The task instance goes to `up_for_retry`, waits, and is scheduled
  again. It is the *same* task instance throughout: its `try_number` increases,
  each attempt keeps its own log, and only the last attempt's outcome decides
  whether downstream tasks run.
- **`retry_delay`** — how long it waits before the next attempt. Thirty seconds
  is generous for a demo and short for real work; a flaky API usually wants
  minutes, and `retry_exponential_backoff=True` spreads them out instead.

**The default is `retries=0`** (`[core] default_task_retries`), which means a
task with one bad network moment just fails and takes the run with it. Retrying
is opt-in, and that is why this repo's integrity tests (§14) insist every
non-demo Dag sets it.

Retries only help with failures that might not happen again — a timeout, a
throttled API, a node that died. For a failure that *will* happen again, raising
`AirflowFailException` (§9) skips the remaining attempts and fails immediately:
bad data does not become good on the third read.

Now the Dag itself. Add the decorator and the function it decorates:

```python dags/01_hello_airflow3.py

@dag(
    dag_id="hello_airflow3",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "basics"],
    doc_md=__doc__,
    params={"greeting": Param("hello", type="string", description="What to say.")},
)
def hello_airflow3():
```

Line by line:

- **`@dag` turns a function into a Dag.** This decorator style has a name,
  **TaskFlow**, and the name will keep coming up.
- **`schedule=None`** — this Dag runs only when something asks it to: you, the
  UI, or the API. §7 has the other values a schedule can take.
- **`start_date`** — no run is ever scheduled before this moment (§7). A Dag
  without one still parses, so Airflow will not remind you.
- **`catchup=False`** — the default, stated explicitly because it is the flag
  that decides whether un-pausing a Dag floods you with historical runs (§7).
- **`tags`** — free-form labels; the Dags list filters on them.
- **`doc_md=__doc__`** — the module docstring you just wrote, rendered as
  markdown on the Dag's page in the UI. `description=` is the shorter cousin,
  one line shown next to the Dag in the list.
- **`params`** — declared inputs, with types. The UI turns these into a form
  when you trigger the Dag (§5), and a run records the values it used.

Inside the function, the tasks. Indent them — they are the body of
`hello_airflow3()`:

```python dags/01_hello_airflow3.py
    @task
    def make_greeting(**context) -> str:
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
        print(f"{loud}  ({words} words)")
```

**`@task` turns a function into a task.** Calling it does *not* run it: it
returns an `XComArg`, a lazy reference to that task's future output. Whatever
the function returns becomes the task's XCom (§6), and whatever it prints lands
in the task log, viewable in the UI.

`**context` is how a task sees the run it belongs to: `run_id`, `params`, `ti`
(the task instance), `dag_run`, `ds`, `data_interval_start`/`end`, `var`,
`conn`, `macros`. Read `logical_date` with `.get()`, never
`context["logical_date"]`: a manual or asset-triggered run has no logical date,
and the key is then missing from the context entirely — indexing it raises
`KeyError` at runtime, while `airflow dags test` (which always supplies one)
stays green. §7 explains the why. `run_id` is always there; prefer it for
naming things.

Finish the file by wiring the tasks together and registering the Dag:

```python dags/01_hello_airflow3.py
    message = make_greeting()
    report(loud=shout(message), words=count_words(message))


hello_airflow3()
```

**Passing one task's output to another draws the edge.** `shout(message)` is
what makes `shout` run after `make_greeting`. The graph is built when the file
is parsed; the code runs later, possibly on another machine. Nothing makes
`shout` and `count_words` wait for each other, so a scheduler-driven run is
free to execute them at the same time (§3 says what decides that):

```
make_greeting ─┬─► shout ──────┬─► report
               └─► count_words ┘
```

And the last line: **the decorator only defines the Dag; calling the function
registers it.** A Dag file that defines a Dag and never calls it produces
nothing — imports cleanly, appears nowhere. It is the most common first mistake
and the hardest to spot, because there is no error to read.

Save the file. Compare with
[`solutions/01_hello_airflow3.py`](solutions/01_hello_airflow3.py) if you want
— it is the same code with the comments left in.

### See it parse

The Dag processor (§3) scans `dags/` every five seconds on this stack, imports
each `.py` file it finds, and writes what it parsed into the metadata database.
Within a few seconds of saving:

```bash
docker compose exec airflow-scheduler airflow dags list -o plain
```

**Checkpoint.** One row: `hello_airflow3`, owner `['mlops-course']`,
`is_paused True`, `bundle_name dags-folder`. Reload the UI and the Dag is in
the list, paused. (Every Dag starts paused — §7.)

If the row is not there, two things can be wrong, and they look identical from
here. The first is a file that raises on import — a typo, a missing package, a
`@dag` function that throws. That is not a broken Dag in the list, it is **no
Dag at all**: absent from the UI, absent from `airflow dags list`. It surfaces
on the Home page (§5) and in this command:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors -o plain
```

**Checkpoint.** `No data found` — your file imports. See what the other case
looks like by making one: save this as `dags/zz_broken_on_purpose.py`,

```python
# An airflow dag file that does not import
import a_module_that_does_not_exist
```

wait a few seconds, and run the command again:

```
bundle_name    filepath                 error
dags-folder    zz_broken_on_purpose.py  Traceback (most recent call last):
                                          File "<frozen importlib._bootstrap>", line 488, in _call_with_frames_removed
                                          File "/opt/airflow/dags/zz_broken_on_purpose.py", line 2, in <module>
                                            import a_module_that_does_not_exist
                                        ModuleNotFoundError: No module named 'a_module_that_does_not_exist'
```

The file, the line and the full traceback. Delete the file and the row clears
by itself on the next scan.

That first comment line is not filler. The Dag processor does not import every
`.py` it finds: with `[core] dag_discovery_safe_mode` on — the default — it
first reads the file as text and skips it unless the lowercased contents
contain `airflow` *and* either `dag` or `asset`. Leave the comment out of that
broken file and it is never parsed, never listed, and never reported — silence,
not an error. Every real Dag file passes this test by importing from
`airflow.sdk`, so it only bites helper modules and experiments; but it is the
second thing to check when a file is simply not there.

The other kind of nothing is Mistake 1 above — a file that defines a Dag and
never calls it. It imports cleanly, so `list-import-errors` is empty too, and
the only symptom is a Dag that is not there. Check this list first, then the
bottom of the file.

### Run it

This executes a complete Dag run in your terminal, no scheduler involved:

```bash
docker compose exec airflow-scheduler airflow dags test hello_airflow3
```

**Checkpoint.** Scroll for the `report` task's line, which looks like this —
seven words, because `make_greeting` appends the run id *and* the logical date,
and the printed date has a space in the middle of it:

```
HELLO FROM RUN MANUAL__2026-09-15T12:18:32.454459+00:00 AT 2026-09-15 12:18:33.500053+00:00  (7 words)
```

and, at the end, `DagRun Finished: ... state=success`. That is the fastest
authoring loop there is: a full run with real logs and no UI. You will use it
after every Dag you write.

> **What `airflow dags test` is, and is not.** It runs the Dag inside this one
> command — no scheduler, no executor, no triggerer — and that changes four
> things you will meet later:
>
> - Tasks run **one at a time**, in an order the graph allows. In the log
>   above, one of `shout`/`count_words` finished before the other started;
>   which goes first varies from run to run.
> - It **always supplies a logical date**. A real manual trigger does not (§7),
>   which is how a Dag can pass here and fail in production.
> - It is **not a dry run**: tasks write real files and real asset events (§9).
> - A task that defers runs its trigger inside the same process (§11), and on
>   3.3.1 the command crashes on any Dag with a `deadline=` (§13).
>
> It is the fastest way to see a Dag work; §14 says when to stop trusting it.

> **Try it.** Change the greeting without touching the file:
> `airflow dags test hello_airflow3 --conf '{"greeting": "hola"}'`. The
> `report` line now starts with `HOLA`. That `--conf` reached the task through
> `context["params"]` — the `Param` you declared, overridden for one run.

**The other way to declare a Dag** is the context manager, which is fully
supported and reads better when the tasks are operators (§6) rather than
functions. Not for this file — just so you recognise it:

```python
from airflow.sdk import DAG

with DAG(
    dag_id="hello_operators",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    BashOperator(task_id="hello", bash_command="echo hi")
```

Anything created inside the `with` block attaches to that Dag automatically.
Both forms produce the same object, and one file may hold several Dags.

## 5. The UI

The UI is a React application on top of the REST API, served by the api-server
at <http://localhost:8080>. Half of what this tutorial teaches is easier to see
than to read about, so take the tour now that there is a Dag to look at.

The nav is **Home · Dags · Assets · Browse · Admin · Plugins · Security · Docs**.

**Home** — health of the scheduler, triggerer and Dag processor, import errors,
failed runs and tasks, recent asset events, deadlines and pool slots. Open it
first when something is not running: a Dag that never appears in the list is
usually an import error sitting on this page.

**Dags** — the list, as cards or a table, filtered by tag, owner or state, with
the pause toggle and a **Trigger** button on each row. Yours shows
`hello_airflow3`, its owner, its two tags, and the one run `airflow dags test`
made. A Dag scheduled on assets shows its asset condition where a cron Dag
shows its schedule; yours, with `schedule=None`, shows neither.

**A Dag's page** puts the run history on the left and detail tabs on the right.
The left panel holds **Grid** and **Graph** — `g` toggles between them — plus
**Show Gantt**, a graph direction control, and filters worth knowing:

- **Dependencies: All Dag Dependencies / External conditions / Only tasks.**
  Switch it away from "Only tasks" on `mlops` (§10) and the graph draws the
  `labeled_projects` asset that triggered the run, not just the tasks inside it.
- **Min. map index** — "shows mapped tasks expanded to at least this index". A
  mapped task like `train_candidate` (§10) is one collapsed node until you raise
  this; raise it and each hyperparameter set gets its own.
- Version indicators, and **Download graph image**.

The tabs are Overview, Runs, Tasks, Backfills, Asset Events, Audit Log,
Calendar, Code, Details and — when a task is waiting on a human — **Required
Actions**. Click a run, then a task, and the same strip becomes that task
instance's: **Logs**, **XCom**, **Rendered Templates**, and **Task Instances**
for a mapped fan-out.

**Do it now.** Open `hello_airflow3`, look at the Graph — the fan-out and
fan-in you drew with two function calls — then click the run, click `report`,
and open **Logs**: the `(7 words)` line, in the browser this time. Click
`make_greeting` and open **XCom**: the greeting string, stored under the key
`return_value`. That is the value `shout` and `count_words` received.

**The Dag version selector.** Every Dag run records the version of the code it
ran with, so the picker (`v1`, `v2`, … or **All Versions**) lets you look at a
run through the code that actually produced it, with the bundle name, bundle
version and parse time on the **Code** tab. A *bundle* is a place the Dag
processor loads Dags from; this stack has one, the `dags/` folder, and it is
named `dags-folder` — the `bundle_name` column you saw in `airflow dags list`
(§15 has the other kinds). Every time you save a change to a Dag file its
version count goes up, and last week's run still shows last week's graph.

**The Trigger form is generated from your `params`.** This is why params are
worth declaring with types:

```python
params={"greeting": Param("hello", type="string")}              # dags/01
POLICY_PARAMS = {"max_label_tvd": Param(0.15, type="number")}   # dags/06
```

A string param becomes a text field, a number a numeric one, an enum a dropdown,
and required fields are validated before the run is created. Under **Advanced
Options** the same thing is editable as raw **Configuration JSON**, and the form
also offers a run id, a logical date, a manual data interval, and an *"Unpause …
on trigger"* checkbox — which exists because triggering a paused Dag leaves the
run **queued** forever (§7).

**Do it now.** Press **Trigger** on `hello_airflow3`. The form has one field,
`greeting`, pre-filled with `hello` — type something else, tick *Unpause
hello_airflow3 on trigger*, and submit. A second run appears in the Grid; its
`report` log has your word in capitals. That is the same `Param` reaching the
same task by a third route: a default in the file, `--conf` on the command
line, a form in the browser.

The same dialog has a second mode, **Backfill**: a date range, a **Reprocess
Behavior** (missing runs / missing and errored runs / all runs), max active runs,
"run backwards", and a dry run that tells you *"N runs will be triggered"*
before you commit. It needs a Dag with a schedule, so it waits for §9.
Asset-scheduled Dags say *"This Dag's schedule does not support backfills"* —
`mlops` (§10) cannot be backfilled, because there is no date range to replay,
only asset events.

**Assets** — the tab this tutorial was built for, and empty until §9. Once the
Dags that use them exist, it lists every asset with its producing tasks,
consuming Dags and event history, and draws the cross-Dag graph: `dataops` →
`labeled_projects` → `mlops` → `production_model`. That graph is the one picture
of the whole pipeline; the Dags list cannot show it, because the connection is
not a dependency between Dags, it is an asset between them. **Create Event** on
an asset either materializes it (run the Dag upstream) or records a manual
event — which is how you trigger a downstream Dag without pretending to produce
the data.

**Browse** — Audit Log (who triggered, paused, cleared what — your UI trigger
is in it now), **Deadlines** (pending and missed deadline alerts, §13), and
XComs.

**Admin** — Connections, Variables, Pools, Providers, Plugins, Jobs and the
rendered Airflow **Config**. Connections and Variables are the two you will use:
never put a credential in a Dag file.

> **Security and Plugins** are near-empty here. This stack runs the Simple auth
> manager with everyone an admin, so there are no users or roles to manage.

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

And the fifth thing, `cross_downstream`, for the shape that looks like it
should work with a list and does not:

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
[`solutions/02_operators_and_xcoms.py`](solutions/02_operators_and_xcoms.py).

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
> Prefer a **param** (§5) over a global Variable when the value is a policy
> knob: params are recorded with the run that used them, Variables are not.

## 7. Scheduling and logical dates

Both Dags so far have `schedule=None`. The other values:

```python
schedule=None                       # manual / API only
schedule="0 3 * * *"                # cron
schedule=timedelta(days=1)          # interval
schedule=[some_asset]               # data-aware (§8)
schedule=CronTriggerTimetable(...)  # a Timetable, named directly
```

Every one of the first four is shorthand for a **Timetable**, the object that
owns the two decisions a schedule makes; the last line names one directly, and
writing your own Timetable class is how you get a cadence cron cannot express.

The two decisions: *when* to create a run, and which slice of time that run is
*for* — its **data interval**, exposed to every task as `data_interval_start`
and `data_interval_end`, and labelled by the run's `logical_date`. None of
those is "when it ran"; `run_after` is that. How the slice is cut depends on
the form of `schedule=`, and the two common forms differ:

- **A cron string** — `"0 3 * * *"`, which is the schedule you will give
  `dataops` in §9 — fires at the moment it names, and the slice *is* that
  moment: `logical_date`, `data_interval_start` and `data_interval_end` are
  all 03:00.
- **A `timedelta`** — `timedelta(days=1)` — is a span. The run *for* 2026-09-10
  is created once that day is over, at 2026-09-11 00:00, and its `logical_date`
  is 2026-09-10 00:00: the start of the span, a day before the run happens.
  Use this form when a task must process "yesterday's data", because the
  interval tells it exactly which day that is.

`start_date` is where the slices begin: no run is ever scheduled before it. A
Dag file without one still parses without error, so this is something you
remember, not something Airflow reminds you of.

Hold that picture and the rest of this section is consequences of it. Three
things about scheduling that will save you an afternoon:

**`catchup` defaults to `False`** (`[scheduler] catchup_by_default`), and what
it does on un-pause is precise. Un-pausing a cron Dag with a 2025 `start_date`
does not create a run for every slice since then. It creates **one**, for the
most recent slice the cron has already passed — today's 03:00 if that is
behind you, yesterday's if not — and it creates it immediately, because that
slice is overdue. From there on, one per slice as they come due. You will
watch this happen in §10: un-pausing `dataops` is enough to start it. If you
*want* the history, ask for it with `catchup=True` — or backfill just the range
you need (§9).

**A cron string fires at the time it says.** `schedule="0 3 * * *"` runs at
03:00 and the run's `logical_date` is 03:00. Timetables are free to define that
differently, which is why the Dag's **Calendar** tab and the *"Next run"* column
are worth checking after you write one.

**`logical_date` is nullable.** It labels the slice a scheduled run is for —
and manual and asset-triggered runs, which are for no slice, simply have none.
This is what `mlops`'s run list will look like in §10, one run of each kind:

```bash
$ airflow dags list-runs mlops -o plain
dag_id  run_id                                      state    run_after             logical_date
mlops   asset_triggered__2026-09-09T08:23:16+00:00  success  2026-09-09T08:23:16              ← none
mlops   manual__2026-09-09T08:22:31+00:00           success  2026-09-09T08:22:32   2026-09-09T08:22:31
```

Use `run_after` when you mean "when did this actually start". Context keys
available to every task: `ds`, `ts`, `logical_date`, `data_interval_start`,
`data_interval_end`, `run_id`, `dag_run`, `ti`, `params`, `var`, `conn`,
`macros`, `inlets`/`outlets`, `triggering_asset_events`.

### Manual runs

A schedule is not the only way to start a Dag. Two commands cover most of it,
and `hello_airflow3` is the Dag to try them on:

```bash
docker compose exec airflow-scheduler airflow dags unpause hello_airflow3
docker compose exec airflow-scheduler airflow dags trigger hello_airflow3 -c '{"greeting": "hola"}' -r manual-demo-1
```

- **`-c/--conf`** is a JSON object. It lands in `dag_run.conf`, and any key that
  matches a declared `Param` overrides it for that run.
- **`-r/--run-id`** names the run. Without it you get `manual__<timestamp>`.
- **`-l/--logical-date`** sets one. Without it the run has none — see the trap
  below.
- In the UI the same thing is the **Trigger** button, with the form generated
  from your `params` (§5) — you pressed it in §5.

A new Dag arrives **paused** (`[core] dags_are_paused_at_creation`), so
`unpause` is not optional — and it is the step everyone forgets:

**A paused Dag still accepts a trigger — the run is created and then sits in
`queued` until you unpause.** Nothing fails, nothing warns:

```console
$ airflow dags pause hello_airflow3 && airflow dags trigger hello_airflow3 -r demo
$ airflow dags list-runs hello_airflow3 -o plain
dag_id          run_id  state   run_after                         logical_date
hello_airflow3  demo    queued  2026-09-09T12:19:29.483234+00:00             ← stuck
$ airflow dags unpause hello_airflow3     # now it starts
```

**Checkpoint.** Run those four commands yourself against `hello_airflow3`. While
the Dag is paused the run is `queued` and stays there however long you wait —
the scheduler sees it every loop and deliberately passes over it. Shortly after
the `unpause`, `list-runs` shows the same `run_id` as `success`, and its
`start_date` is later than its `run_after`: it started when you un-paused, not
when you triggered. Nothing was lost and nothing warned you; it was simply held.

The UI's trigger form has an *"Unpause … on trigger"* checkbox for exactly this.

> **The trap worth knowing before you trigger anything:** a manual run has **no
> logical date**, so `logical_date` is missing from the task context and `ds`,
> `ts` and friends are *undefined* in templates. Both fail at runtime, and
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
> You wrote the safe form of both in lessons 1 and 2, which is why the runs
> you just triggered succeeded. Both were live bugs in this tutorial's own
> Dags until a manual trigger found them. Asset-triggered runs have no logical
> date either, so `mlops` is exposed to the same thing.

Two more ways in: `airflow dags test <dag_id>` runs the whole Dag in-process
(§14), and the REST API does it over HTTP —
`POST /api/v2/dags/{dag_id}/dagRuns`, which is what the UI button calls.

## 8. Assets: scheduling on data, not on time

An **Asset** is a named piece of data. A task declares it produces one with
`outlets=`; a Dag declares it consumes one by putting it in `schedule=`. That is
the whole mechanism, and it replaces every "run 30 minutes after the other Dag,
and hope" cron you have ever seen.

The assets this tutorial uses are already defined, in
[`src/tagifai/assets.py`](src/tagifai/assets.py), so that both Dags import the
same objects:

```python
from airflow.sdk import Asset

LABELED_PROJECTS = Asset(
    name="labeled_projects",
    group="dataset",
    extra={"layer": "warehouse", "owner": "data-engineering"},
)
```

The producer side is what you will write in §9 — note that the outlet goes on
the **validation** task, not the transform task:

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

And the consumer side, §10:

```python
@dag(schedule=[LABELED_PROJECTS], ...)
def mlops():
    @task
    def prepare_data(*, triggering_asset_events):
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")
```

That is the idea in one line: **downstream Dags are scheduled on "good data
arrived", never on "a task finished" and never on a cron guess.** The event even
carries the row count that caused it.

More asset machinery worth knowing:

| Feature | What it does |
|---|---|
| `AssetAny(a, b)` / `AssetAll(a, b)` | OR / AND conditions in a `schedule` |
| `AssetAlias` | outlets whose URI is only known at runtime |
| `@asset` / `@asset.multi` | define asset + Dag + task in one decorator |
| `Metadata(asset, {...})` | attach metadata by yielding from the task |
| `AssetWatcher` + `BaseEventTrigger` | **event-driven** runs (§11) |
| `CronPartitionTimetable` | partitioned assets |

Assets here are **name-only**, which keeps their identity the same in Docker and
native runs. In production give them the real URI of the data (`s3://…`,
`bq://…`) so the lineage in the UI points somewhere.

An asset exists in Airflow's eyes only once a parsed Dag refers to it — as an
outlet or a schedule. Right now `airflow assets list` prints `No data found`,
and the Assets tab is empty; §9 changes that the moment you save a file.

### What an asset event actually is

It is a row. When a task with `outlets=[…]` **succeeds**, Airflow records an
asset event — which asset, which Dag run produced it, when, and whatever you put
in `outlet_events[…].extra`. That is all a "trigger" is. A task that fails
writes no event, which is the entire safety property: `mlops` cannot be started
by data that did not pass validation, because the task that would have announced
it never finished.

The scheduler then does the second half. On its next loop it reads new asset
events and queues a Dag run for **each unpaused Dag** scheduled on that asset.

Events that arrive faster than the consumer can run are neither lost nor run
one by one. `mlops` allows a single active run (`max_active_runs=1`), so if
three `dataops` runs finish while it is busy, the *next* `mlops` run takes all
three events, and `triggering_asset_events` in `prepare_data` lists every one
of them. A consumer catches up in one run, not three.

> **A paused consumer does not queue, and does not catch up.** The word
> *unpaused* above is load-bearing. If `mlops` is paused when `dataops` emits,
> the asset event is still written — you will see it in the table and on the
> Assets page — but no run is queued for `mlops`, and **un-pausing later does
> not replay it**. Airflow filters paused Dags at the moment it decides what to
> enqueue, so there is nothing left to find afterwards. The same goes for a
> consumer that did not exist yet when the event was written: the `dataops`
> run you make in §9 will emit an event that `mlops`, written in §10, never
> sees. (Checked all three ways on this stack — see §17.)
>
> This is the opposite of the paused-Dag behaviour in §7, where a *manual*
> trigger creates the run first and the pause only holds it. Asset scheduling
> has no run to hold yet.

## 9. The DataOps Dag

`dags/03_dataops.py` — extract → validate → transform → validate. The first
Dag here that does real work, and each task is a slot you can swap for a real
vendor:

| Task here | Production option |
|---|---|
| `extract_projects` | Airbyte / Fivetran / an EL job |
| `validate_raw`, `validate_transformed` | Great Expectations / Soda / dbt tests |
| `transform` | dbt Cloud / astronomer-cosmos |

It is also the first Dag here with a real cron — `schedule="0 3 * * *"`, the
example from §7 — so once you un-pause it in §10 it will run by itself: once
immediately, for the slice its cron most recently missed, and then at 03:00 UTC
every day, each run triggering `mlops` in turn. (`monitoring`, §12, is the
other scheduled one, at 06:00.) Leave the stack up overnight and the
`scheduled__` runs you find in the morning are that, not a bug.

> **About the source data.** `projects.csv` arrives already labelled — one table
> of `id, created_on, title, description, tag` — so `transform` cleans the text
> and collapses rare tags, and that is all. A real warehouse usually keeps the
> labels in a separate table, which makes `transform` a join and gives the Dag a
> second extract-and-validate arm feeding it:
>
> ```python
> [validate_projects, validate_tags] >> transform >> validate_transformed
> ```
>
> That is the fan-in from §6, and it is the only structural difference. Nothing
> else in this Dag changes with the number of sources.

### Build it

The workload this Dag calls, all in `src/tagifai/`: `data.extract()` downloads
the CSV into the landing zone and returns its path; `data.load_raw(fp)` and
`data.load_labeled(fp)` read a table; `data.transform(df)` produces the curated
table and `data.save_labeled(df)` writes it, returning the path;
`validate.run_suite(df, suite_name)` runs a named set of expectations and
returns a result with `.passed`, `.failures`, `.results`, `.suite` and a
`.summary()` line. `config.ensure_dirs()` creates the folders.

Create `dags/03_dataops.py`:

```python dags/03_dataops.py
"""Lesson 3 - the DataOps Dag: extract -> validate -> transform -> validate."""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.sdk import dag, task, task_group
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
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "dataops"],
    doc_md=__doc__,
)
def dataops():
```

Two imports are new. `AirflowFailException` comes from `airflow.sdk.exceptions`
— task code imports exceptions from the Task SDK, because that is the namespace
a remote worker has. And `tagifai` imports at all because the compose file puts
`/opt/airflow/src` on `PYTHONPATH` for every service: the Dag processor needs
it to parse the file, and the executor needs it to run the tasks.

`max_active_runs=1` caps the Dag at one run at a time — a second one, from a
trigger or a schedule, waits in `queued` until the first is done.

The first task produces an asset. Add it inside `dataops()`:

```python dags/03_dataops.py
    @task(outlets=[RAW_PROJECTS])
    def extract_projects(*, outlet_events) -> str:
        config.ensure_dirs()
        fp = data.extract()
        df = data.load_raw(fp)
        outlet_events[RAW_PROJECTS].extra = {"rows": len(df), "path": str(fp)}
        return str(fp)
```

`outlets=` is the declaration from §8; `outlet_events` is injected by name,
like `ti`, and lets the task attach metadata to the event it is about to emit.
The task returns a **path**, not the dataframe: XComs carry pointers.

Now the gate on the raw data, and the transform:

```python dags/03_dataops.py
    @task
    def validate_raw(fp: str) -> dict[str, Any]:
        result = validate.run_suite(data.load_raw(fp), "raw_projects")
        print(result.summary())
        if not result.passed:
            raise AirflowFailException(f"Raw data failed validation: {result.failures}")
        return {"suite": result.suite, "results": result.results}

    @task
    def transform(fp: str) -> str:
        df = data.transform(data.load_raw(fp))
        out = data.save_labeled(df)
        print(f"Wrote {len(df)} rows to {out}")
        return str(out)
```

The validation gate is the part worth copying, and two details in it are easy
to get wrong:

- **`AirflowFailException`, not `AirflowException`.** A data-quality failure is
  not transient; with `retries=2` in the defaults, a plain exception would run
  the same check three times and delay the alert by two minutes.
  `AirflowFailException` fails the task at once, retries or not.
- **Expectations are non-critical when they mean "look at this", not "stop".**
  An unexpected tag appearing is a signal to inspect the label space, not a
  reason to halt the pipeline. `validate.py` marks that one `critical=False`,
  so it lands in `result.failures` without flipping `result.passed`.

The final gate — the one that emits the asset `mlops` will be scheduled on:

```python dags/03_dataops.py
    @task(outlets=[LABELED_PROJECTS])
    def validate_transformed(fp: str, *, outlet_events) -> dict[str, Any]:
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
```

Placing the outlet on the *validation* task, not the transform task, is the
whole trick: the event is written only when this task succeeds, so downstream
Dags are scheduled on "good data arrived", never on "a task finished".

One more task, inside a **task group** — `@task_group` nests tasks under one
collapsible node in the Graph view, worth reaching for once a Dag passes a
dozen tasks — and then the wiring:

```python dags/03_dataops.py
    @task_group(group_id="quality_report")
    def quality_report(raw_report: dict[str, Any], curated_report: dict[str, Any]):

        @task
        def summarize(raw: dict[str, Any], curated: dict[str, Any]) -> None:
            for report in (raw, curated):
                failed = [r["expectation"] for r in report["results"] if not r["passed"]]
                print(f"{report['suite']}: {len(report['results'])} checks, failed={failed or 'none'}")

        summarize(raw_report, curated_report)

    raw_fp = extract_projects()
    raw_report = validate_raw(raw_fp)

    curated_fp = transform(raw_fp)
    raw_report >> curated_fp

    curated_report = validate_transformed(curated_fp)
    quality_report(raw_report, curated_report)


dataops()
```

Read the wiring twice. `transform` takes `raw_fp`, so data flow draws its edge
from `extract_projects` — but nothing it consumes comes from `validate_raw`, so
`raw_report >> curated_fp` adds the edge by hand: the transform must wait for
the gate even though it does not use its output. That is the §6 rule in
practice — implicit edges for data, `>>` for permission.

### See the assets appear

Save the file. Within a few seconds `airflow dags list` has a third row — and
something else changed:

```bash
docker compose exec airflow-scheduler airflow assets list -o plain
```

**Checkpoint.** Two rows, `labeled_projects` and `raw_projects`, each with its
group and extra. Nothing has run; the Dag processor parsed a file with
`outlets=` in it, and that was enough. `production_model`, defined in the same
`assets.py`, is not listed — no Dag refers to it yet. The Assets tab in the UI
now shows both, with `dataops` as the producer and no consumer.

### Run it

```bash
docker compose exec airflow-scheduler airflow dags test dataops
```

**Checkpoint.** Look for `suite=raw_projects passed=4/4`, `Wrote 764 rows to
/opt/airflow/data/warehouse/labeled_projects.csv`, `suite=labeled_projects
passed=6/6`, and from the task group, `raw_projects: 4 checks, failed=none`
and `labeled_projects: 6 checks, failed=none`. The file
`data/warehouse/labeled_projects.csv` now exists on your machine. (`extract`
downloads the source CSV over HTTPS — it is the one task in the pipeline that
needs an internet connection, and the row count follows that file.)

If a task fails, compare with [`solutions/03_dataops.py`](solutions/03_dataops.py).

> **`airflow dags test` is not a dry run.** It executed the real tasks and wrote
> the real rows: the file on disk, and a genuine `labeled_projects` asset event
> in the metadata database — open the Assets tab and it is there, with the
> `extra` you attached. Nothing is scheduled on that asset yet, so nothing else
> happens. And per §8, nothing will: when `mlops` appears in §10 this event is
> already history, and Airflow does not replay history.

### Backfills

Now that you have a Dag with a schedule, backfills make sense. A backfill
creates the runs for every slice in a date range, after the fact. They are
first-class objects, with their own API, UI form and reprocess policy.
**Dry-run it first** — a backfill of a busy Dag is the easiest way to create
thirty runs by accident:

```bash
docker compose exec airflow-scheduler airflow backfill create \
    --dag-id dataops --from-date 2026-02-01 --to-date 2026-02-04 --dry-run
```

**Checkpoint.** A table headed `Runs to be attempted:` with three rows —
2026-02-01, 02 and 03, each at 03:00 — and nothing created. `--to-date` is
exclusive, and each row is one slice of `dataops`'s daily cron.

Drop `--dry-run` and the same command creates those three runs, with run ids
beginning `backfill__`. Know what follows before you do it:

- The runs are created **queued**, and if the Dag is paused they stay that way
  — the §7 rule — until you un-pause it, when they all start,
  `--max-active-runs` at a time.
- Every `dataops` backfill run that succeeds emits a `labeled_projects` asset
  event exactly as a normal run does (§8), so an un-paused `mlops` trains after
  them. Backfilling a producer backfills its consumers.
- The CLI in 3.3.1 only *creates* backfills. To stop one, cancel it in the
  Dag's **Backfills** tab, or through the API — `PUT
  /api/v2/backfills/{id}/cancel`, no token needed on this all-admins stack —
  which marks its still-queued runs `failed`. Pausing the Dag is the emergency
  brake: a paused Dag starts nothing.

`--reprocess-behavior` decides what to do with slices that already have a run —
`none`, `failed` or `completed` — which is the difference between "fill the
gaps" and "redo the month". In the UI: Dag → **Trigger** → **Backfill**, which
dry-runs the range and tells you how many runs it will create before you
commit. This is also why an asset-scheduled Dag cannot be backfilled (§5): it
has no slices, only events.

### Swapping in the real vendors

Each swap is one operator plus one **Connection** — the credential never appears
in the Dag file. Add it under Admin → Connections, from the CLI, or as an
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

The expectations inside that suite are Great Expectations' own vocabulary, and
they map one-to-one onto the checks in `validate.py`:
`expect_column_values_to_be_unique`, `expect_column_values_to_not_be_null`,
`expect_column_values_to_be_of_type`, `expect_compound_columns_to_be_unique`,
`expect_table_row_count_to_be_between`.

The Dag's shape does not change: `outlets=[LABELED_PROJECTS]` still goes on the
last validation task, and everything downstream still schedules on the asset.
Those three providers are **not** installed here, which is why this tutorial
needs no vendor account.

## 10. The MLOps Dag

`dags/04_mlops.py` — no schedule, triggered by the asset.

```
prepare_data ─► validate_training_data ─► train_candidate×N ─► select_best
             ─► evaluate_best ─► promotion_gate ─► promote_model ──► done
                                               └─► keep_current_model ─┘
```

| Task | Production option |
|---|---|
| the asset trigger + `prepare_data` | your warehouse client — or, as here, let the asset event carry the pointer |
| `validate_training_data` | Great Expectations / Pandera / Soda, plus split checks |
| `candidates` + `train_candidate.expand()` | Optuna / Ray Tune / a sweep service |
| `select_best` + `evaluate_best` | your experiment tracker (MLflow, W&B) |
| `promotion_gate` → `promote_model` | your registry's stage transition |

### Build it

New workload functions: `data.split(df)` returns a stratified `(train, test)`
pair; `validate.check_split(train, test)` is a second suite, on the split;
`train_module.SEARCH_SPACE` is a list of hyperparameter dicts and
`train_module.train_from_files(hparams)` fits one model, returning
`(model, metrics)`; `registry.save_run`, `load_metrics`, `promote` and
`get_production` are the filesystem registry; `config.MIN_F1_TO_PROMOTE` is
the promotion threshold, read from `TAGIFAI_MIN_F1` if set.

Create `dags/04_mlops.py`:

```python dags/04_mlops.py
"""Lesson 4 - the MLOps Dag: asset-triggered training, tuning, and promotion."""

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
    schedule=[LABELED_PROJECTS],
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "mlops"],
    doc_md=__doc__,
)
def mlops():
```

`schedule=[LABELED_PROJECTS]` is the whole point: a list of assets, and the Dag
runs when they update. Several assets in the list mean *all* of them;
`AssetAny(a, b)` means either.

**Validate before you train.** `dataops` validated `labeled_projects` on its way
out of the warehouse, and this Dag validates it again on the way in. That is not
redundant: it was a different Dag run, possibly hours ago, this run may have been
triggered by hand, and what the model is fitted on is the snapshot on disk *now*.
Add the first two tasks:

```python dags/04_mlops.py
    @task
    def prepare_data(*, triggering_asset_events) -> dict[str, int]:
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")
        df = data.load_labeled()
        train_df, test_df = data.split(df)
        return {"num_train": len(train_df), "num_test": len(test_df)}

    @task
    def validate_training_data(split_sizes: dict[str, int]) -> dict[str, Any]:
        df = data.load_labeled()
        table = validate.run_suite(df, "labeled_projects")
        split = validate.check_split(*data.split(df))
        print(f"{table.summary()} {split.summary()}")
        if not (table.passed and split.passed):
            raise AirflowFailException(
                f"Training snapshot failed validation: {table.failures + split.failures}"
            )
        return {
            "rows": len(df),
            "split_sizes": split_sizes,
            "warnings": [r["expectation"] for r in table.failures + split.failures],
            "results": table.results + split.results,
        }
```

`triggering_asset_events` is injected by name — the events that caused this
run, keyed by asset, each carrying the `extra` the producer attached. On a run
you start by hand it is empty. `validate_training_data` re-reads the table
rather than receiving it from `prepare_data`: XComs carry metadata, never
payloads. The second half of its check is one a table-level suite cannot do at
all — it looks at the **split**:

```python
# src/tagifai/validate.py — check_split()
("split_is_non_empty",          ...,  critical)
("classes_in_both_splits",      ...,  critical)   # a class only in train
("min_rows_per_class:20",       ...,  critical)   # 3 rows of a class in train
("no_id_leakage",               ...,  critical)   # the same project in both
("test_fraction_between_0.1_and_0.4", ..., non-critical)
```

Every one of those passes a column-level suite and still produces a model whose
held-out f1 means nothing — and that f1 is exactly what the promotion gate
compares against production.

**Dynamic task mapping** fans out one task instance per hyperparameter set at
runtime, then reduces. Three tasks:

```python dags/04_mlops.py
    @task
    def candidates() -> list[dict[str, Any]]:
        return train_module.SEARCH_SPACE

    @task(max_active_tis_per_dag=2)
    def train_candidate(hparams: dict[str, Any], **context) -> dict[str, Any]:
        config.ensure_dirs()
        run_id = f"{context['run_id']}__{context['ti'].map_index}".replace(":", "-").replace("+", "-")
        model, metrics = train_module.train_from_files(hparams)
        registry.save_run(run_id, model, metrics)
        print(f"run={run_id} hparams={hparams} f1={metrics['f1']:.4f}")
        return {"run_id": run_id, "f1": metrics["f1"], "hparams": hparams}

    @task
    def select_best(results: list[dict[str, Any]]) -> dict[str, Any]:
        best = max(results, key=lambda r: r["f1"])
        print(f"best={best['run_id']} f1={best['f1']:.4f} of {len(results)} candidates")
        return best
```

`candidates` returns a list; in the wiring below, `train_candidate.expand(hparams=candidates())`
creates one task instance per item at runtime — four here, one per
hyperparameter set — and each is retried, logged and inspected on its own.
`context['ti'].map_index` is which one this is. The reduce step, `select_best`,
just takes a `list`. Note the return value of `train_candidate`: a run id, an
f1 and the hparams — small and JSON-serialisable. The model itself stays in the
registry.

**How many of the four train at once is a choice, and it is written above.**
Nothing about `.expand()` says "in parallel"; it says "these are four separate
task instances". Whether they overlap is the executor's business, bounded by the
narrowest of the limits from §3 — and `max_active_tis_per_dag=2` on that task is
deliberately the narrowest one here. So the fan-out runs **two at a time, in two
waves**. Raise the cap and all four overlap; remove it and `[core]
max_active_tasks_per_dag` (16) is the next limit up. A cap on a training task
is not a toy setting: four models fitting at once is four copies of the data in
memory, and a `pool` is how you say the same thing about a database that will
not take four connections.

> **Gotcha:** the parameter is named `hparams`, not `params`. TaskFlow injects
> context keys by parameter name, and `params` is reserved — a task argument
> called `params` gets the Dag's params, not your data.

The **promotion gate** is a branch, not a failure. Add the evaluation, the
gate, the promotion and the two `EmptyOperator`s the branch needs:

```python dags/04_mlops.py
    @task
    def evaluate_best(best: dict[str, Any]) -> dict[str, Any]:
        metrics = registry.load_metrics(best["run_id"])
        for name, slice_metrics in metrics.get("slices", {}).items():
            print(f"slice {name}: f1={slice_metrics['f1']:.4f} n={slice_metrics['support']}")
        return {"run_id": best["run_id"], "metrics": metrics}

    @task.branch
    def promotion_gate(evaluation: dict[str, Any]) -> str:
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
    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")
```

A `@task.branch` returns the `task_id` to run; the other branch is **skipped**,
not failed. A candidate that misses the threshold is a normal outcome — the run
succeeds and production is left alone. `promote_model` emits the second asset,
`production_model`, which nothing consumes yet; point a Dag's `schedule` at it
and the edge appears in the Assets graph.

**Trigger rules** are how a task states what it wants from its parents, and
branching is where you first need one. The join, `done`, needs
`none_failed_min_one_success`, because the default `all_success` would skip it as
soon as either branch is skipped. The useful ones: `all_success` (default),
`all_done` (run whatever happened upstream — cleanup tasks),
`none_failed_min_one_success` (joins after a branch), `none_failed` (allows skips
but not failures), `one_success`, `one_failed` (alerting paths), `all_skipped`,
and `always`.

Finish the file with the wiring:

```python dags/04_mlops.py
    snapshot = validate_training_data(prepare_data())
    results = train_candidate.expand(hparams=candidates())
    snapshot >> results

    evaluation = evaluate_best(select_best(results))
    gate = promotion_gate(evaluation)

    gate >> promote_model(evaluation) >> done
    gate >> keep_current_model >> done


mlops()
```

`snapshot >> results` wires the validation gate upstream of *every* mapped
instance, carrying no data, only permission to run. The gate's two `>>` lines
give the branch its two arms and join them at `done`.

### See the wiring

Save it. You have now written both halves of the pipeline, and before running
anything you can ask Airflow what it knows:

```bash
docker compose exec airflow-scheduler airflow assets details --name labeled_projects -o plain
```

**Checkpoint.** Among the rows:

```
scheduled_dags    [{'dag_id': 'mlops', ...}]
producing_tasks   [{'dag_id': 'dataops', 'task_id': 'validate_transformed', ...}]
```

`producing_tasks` names the exact task that emits it; `scheduled_dags` names
what will start when it does. Neither Dag mentions the other anywhere. The
Assets tab now draws the whole graph, `dataops → labeled_projects → mlops →
production_model`.

### Watch the whole chain fire

This is the payoff of §8. Un-pause the consumer first, so it is listening, then
the producer:

```bash
docker compose exec airflow-scheduler airflow dags unpause mlops
docker compose exec airflow-scheduler airflow dags unpause dataops
```

Nothing happens on the first line: `mlops` has no schedule and no event has
arrived. The second line is enough to start everything, and you did not
trigger anything — that is the §7 rule: `dataops` has a cron and a 2025
`start_date`, so the moment it is un-paused the scheduler creates the run for
the most recent 03:00 UTC it missed, and starts it. Give it about half a
minute, then:

```bash
docker compose exec airflow-scheduler airflow dags list-runs dataops -o plain
docker compose exec airflow-scheduler airflow dags list-runs mlops -o plain
```

**Checkpoint.** `dataops` has a run whose id begins `scheduled__` and ends in
`T03:00:00+00:00` — today's date, or yesterday's if it is before 03:00 UTC —
with that as its `logical_date` and a `start_date` of a moment ago. (The
`manual__` run from `airflow dags test` in §9 is there too.) And `mlops` has a
run nobody triggered: id beginning `asset_triggered__`, `running` if you were
quick, `success` once its four candidates have trained, with an **empty**
`logical_date` — asset-triggered runs have no logical date, exactly like
manual ones (§7). In the UI, open **Assets** and you will see the edge that
caused it.

Know what the wait is. Almost all of it is `dataops` itself running its four
tasks — `extract` downloads the source file, so that part depends on your
connection. The Airflow part is small: one scheduler loop to create the
`dataops` run after the un-pause, and one more to queue `mlops` once the asset
event lands (§3). If nothing has appeared after a minute, check that both Dags
are un-paused and that the `dataops` run succeeded — a failed
`validate_transformed` emits no event, by design.

Now the two waves. Open that `mlops` run in the UI, click `train_candidate`,
and read its **Task Instances** tab — four rows, map index 0 to 3, each with
its own start and end time. Two start together; the other two start only after
the first pair has finished. **Show Gantt** draws the same thing as two pairs
of bars. On the **Graph**, the four are one collapsed node until you raise the
**Min. map index** filter (§5). Or read it off the CLI:

```bash
docker compose exec airflow-scheduler airflow tasks states-for-dag-run mlops <the asset_triggered__ run id> -o plain
```

Four `train_candidate` rows with `map_index` 0–3; the start times of 2 and 3
are after the end times of 0 and 1. And `keep_current_model` is `skipped`
while `promote_model` is `success`: the gate took the promoting arm, because
there was no production model to beat.

> **Try it, once more by hand.** `airflow dags trigger dataops` — a
> `manual__` run this time. When it succeeds, one more event, one more
> `asset_triggered__` run of `mlops`. Every success of the producer is one
> run of the consumer, whichever way the producer was started.

> **Try it, the other branch.** Set the bar impossibly high and watch the gate
> take the other arm. The threshold is read from an environment variable inside
> the task's own process, so set it in the same shell that runs the command:
>
> ```bash
> docker compose exec airflow-scheduler bash -c 'export TAGIFAI_MIN_F1=0.99; airflow dags test mlops'
> ```
>
> `promotion_gate` logs `below threshold 0.99 - not promoting`, `promote_model`
> is skipped, `keep_current_model` runs, and the Dag run still succeeds.

Stuck anywhere? [`solutions/04_mlops.py`](solutions/04_mlops.py).

## 11. Waiting: sensors, deferrable tasks, events

`dags/05_sensors_and_events.py`. Three ways to wait for the outside world,
cheapest last — and, after them, the one way not to wait at all:

```python
# 1. Poking sensor — holds a worker slot the entire time. Seconds, not hours.
FileSensor(task_id="wait", filepath=..., poke_interval=10, timeout=300)

# 2. `reschedule` mode — the task instance exits between pokes and is re-queued.
FileSensor(..., mode="reschedule")

# 3. Deferrable — suspends onto the triggerer and frees the slot entirely.
TimeDeltaSensorAsync(task_id="settle", delta=timedelta(seconds=5))
```

The word **trigger** is overloaded, so pin it down before the third option can
make sense. You *trigger* a Dag (§7): a verb. The triggerer process from §3
runs **triggers**: nouns — small pieces of asynchronous code that wait for one
thing (a moment in time, a file, a message) and fire an event when it happens.
"Deferrable" means the task hands the triggerer such a trigger, exits, and is
resumed when the event arrives. `TriggerDagRunOperator` (§12) is the verb again.

### Build it

Create `dags/05_sensors_and_events.py` with the header and the two operator
sensors:

```python dags/05_sensors_and_events.py
"""Lesson 5 - waiting for things: sensors, deferrable operators, and events."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensorAsync
from airflow.sdk import PokeReturnValue, dag, task

from tagifai import config, data

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
    settle = TimeDeltaSensorAsync(task_id="settle", delta=timedelta(seconds=5))

    wait_for_curated_data = FileSensor(
        task_id="wait_for_curated_data",
        filepath=str(config.LABELED_PROJECTS_FP),
        poke_interval=10,
        timeout=60 * 5,
        mode="reschedule",
    )
```

`settle` is deferrable: it releases its worker slot while it waits, which
needs `airflow triggerer` running. `wait_for_curated_data` watches for the
file `dataops` writes, through the `fs_default` connection the compose file
provides. `mode="reschedule"` is the non-deferrable way to free a slot: the
task instance exits and is re-queued every `poke_interval` seconds, and gives
up after `timeout`.

A sensor can also be written as a function. Add it, with the task that uses
its value, and the wiring:

```python dags/05_sensors_and_events.py
    @task.sensor(poke_interval=10, timeout=120, mode="reschedule")
    def enough_rows() -> PokeReturnValue:
        if not config.LABELED_PROJECTS_FP.exists():
            return PokeReturnValue(is_done=False)
        rows = len(data.load_labeled())
        print(f"curated table has {rows} rows")
        return PokeReturnValue(is_done=rows >= 500, xcom_value=rows)

    @task
    def proceed(rows: int) -> None:
        print(f"Data is ready ({rows} rows). Downstream work can start.")

    settle >> wait_for_curated_data >> proceed(enough_rows())


sensors_and_events()
```

`PokeReturnValue(is_done=..., xcom_value=...)` both stops the poking and hands
a value downstream — `proceed` receives the row count. The table is read with
pandas rather than counted by lines, because descriptions contain newlines.

Not in the file, because it needs a queue you do not have: the option that
removes the wait altogether — **event-driven scheduling**. Attach a watcher to
an asset and an external event starts the run; nothing polls, nothing occupies
a slot:

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

### Run it

This Dag waits for the file `dataops` produced, so §9 must have run or the
sensors will poll until they time out:

```bash
docker compose exec airflow-scheduler airflow dags test sensors_and_events
```

**Checkpoint.** It finishes in a few seconds, because everything it waits for
has already happened. In the log:

```
curated table has 764 rows
Poking for file /opt/airflow/data/warehouse/labeled_projects.csv
Success criteria met. Exiting.
Data is ready (764 rows). Downstream work can start.
```

`wait_for_curated_data` pokes once, finds the file and exits; `enough_rows`
counts the rows and clears its threshold on the first poke. And `settle`, the
deferrable one, returns at once without deferring at all: its five-second target
is measured from the run's logical date, which `airflow dags test` fixed before
the task started, so by the time the task ran the moment had already passed.
Nothing in this run touched the triggerer.

**To watch a task actually defer**, send the run through the scheduler instead,
then read the task states a few times over the next ten seconds:

```bash
docker compose exec airflow-scheduler bash -c '
  airflow dags unpause sensors_and_events
  airflow dags trigger sensors_and_events -r defer-demo'   # pick a new id if you rerun

docker compose exec airflow-scheduler airflow tasks states-for-dag-run sensors_and_events defer-demo -o plain
```

**Checkpoint.** `settle` reads `deferred` for about five seconds — its process
has exited and the triggerer is holding the timer — then `success`, with an
`end_date` five-odd seconds after its `start_date`. That `deferred` is the
state from §3, and it is what "frees the worker slot" looks like from outside.
Be quick with the second command; a `docker compose exec` takes a second or
two of its own.

**Now see the interesting case** — a sensor that actually has to wait. Move the
file away and run the Dag again:

```bash
# Git Bash needs the MSYS_NO_PATHCONV prefix on any /opt/... path (§2)
MSYS_NO_PATHCONV=1 docker compose exec airflow-scheduler \
    mv /opt/airflow/data/warehouse/labeled_projects.csv /tmp/labeled_projects.csv

docker compose exec airflow-scheduler airflow dags test sensors_and_events
```

This one does not return. Both sensors are in `mode="reschedule"`, so their task
instances **exit between pokes** instead of holding a slot, and land in
`up_for_reschedule` — the state from §3 that only sensors produce. The run
announces it every second:

```
No tasks to run. unrunnable tasks: {<TaskInstance: sensors_and_events.enough_rows ... [up_for_reschedule]>,
                                    <TaskInstance: sensors_and_events.wait_for_curated_data ... [up_for_reschedule]>}
```

In a **second terminal**, put the file back:

```bash
MSYS_NO_PATHCONV=1 docker compose exec airflow-scheduler \
    mv /tmp/labeled_projects.csv /opt/airflow/data/warehouse/labeled_projects.csv
```

The next poke finds it and the run completes. Leave it moved instead and each
sensor eventually fails on its declared `timeout`: 120 seconds for
`enough_rows`, 300 for `wait_for_curated_data`, so the run is over after two
minutes.

The point is what was *not* happening during the wait: the task instance was
not running between pokes — it was in `up_for_reschedule`, which you saw — so
no worker slot was held. A poking sensor in the default mode would have occupied
a slot for the whole time doing nothing, which is the entire reason the other
two modes exist.

**Rule of thumb:** seconds → a poking sensor is fine. Minutes or hours →
deferrable, or you are paying a worker slot to sleep. An external system that can
push → an `AssetWatcher`, and no task waits at all.

Reference: [`solutions/05_sensors_and_events.py`](solutions/05_sensors_and_events.py).

## 12. Continual learning: monitor, decide, act

**Continual**, not continuous: the system repeats *with breaks*, and the breaks
are deliberate. A monitor fires, something is decided — by a person where it
matters — and then the pipeline runs. The approval gate below is that comma made
executable.

What you are watching for is **drift**, in three flavours:

| Drift | What moved | Caught here by |
|---|---|---|
| **data drift** | the input distribution — the text itself | `mean_length_shift` in `monitoring.drift_report()` |
| **target drift** | the label distribution — one tag takes over, or a new tag appears | `label_tvd` (total variation distance) and `new_labels` |
| **concept drift** | the mapping itself — the same input now deserves a different label | only the live performance check: f1 falls while inputs look unchanged |

Concept drift is the one no input-side statistic can see, which is why
`monitoring.production_performance()` scores recently labelled rows instead of
trusting a distribution comparison alone.

A monitoring event has four sensible responses, and
`dags/06_monitoring_and_retraining.py` makes them branches:

| Response | When | What happens |
|---|---|---|
| **continue** | drift and performance within budget | nothing; keep serving |
| **improve** | f1 dropped or the input distribution moved | ask a human, then trigger `mlops` |
| **inspect** | an unseen label appeared | stop and page someone — the label space changed |
| **rollback** | performance collapsed | re-point production at the previous model |

### Build it

The workload: `monitoring.monitor()` returns a report with a `drift` dict
(`label_tvd`, `new_labels`, `current_distribution`, …) and a `performance`
dict (`f1_at_promotion`, `f1_now`) — or `performance=None` when nothing is
promoted; `registry.rollback()` re-promotes the previous model.

Create `dags/06_monitoring_and_retraining.py`. The thresholds are Dag
**params**, so the policy is visible in the UI, overridable per run, and
recorded with the run that used it:

```python dags/06_monitoring_and_retraining.py
"""Lesson 6 - continual learning: monitor, decide, and act."""

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
```

(The function is `monitoring_dag` because `monitoring` is already the name of
the imported module. The `dag_id` is what Airflow sees.)

Measure, then decide:

```python dags/06_monitoring_and_retraining.py
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
            return "inspect"
        if f1_drop >= policy["max_f1_drop"] or drift["label_tvd"] >= policy["max_label_tvd"]:
            return "request_retraining"
        return "continue_serving"
```

`context["params"]` is the policy — the defaults above, or whatever this run
was given. Note the ordering: **a new label routes to `inspect`, not to
`improve`.** Retraining on data whose label space you have not understood yet is
how you ship a worse model with full confidence.

Now the four arms. Three are simple; the fourth is a **human-in-the-loop**
gate, `ApprovalOperator`, followed by the task that starts another Dag:

```python dags/06_monitoring_and_retraining.py
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

    request_retraining = ApprovalOperator(
        task_id="request_retraining",
        subject="Retrain the tag classifier?",
        body=(
            "Monitoring detected drift or a performance drop.\n\n"
            "Approve to trigger the `mlops` Dag on the current data."
        ),
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
```

`ApprovalOperator` parks the task in `awaiting_input` and shows a prompt in the
UI, under the Dag's **Required Actions** tab. On **Reject**, everything
downstream is skipped. `defaults` is what an unanswered run resolves to after
`response_timeout` — sixty seconds here so the lesson finishes; give it hours
in production. `TriggerDagRunOperator` is the cross-Dag verb: it creates a
`manual__` run of `mlops` with the `conf` given, and with
`wait_for_completion=False` it does not stay around to watch it.

Finish with the wiring:

```python dags/06_monitoring_and_retraining.py
    report = measure()
    branch = decide(report)

    branch >> continue_serving
    branch >> inspect(report)
    branch >> rollback()
    branch >> request_retraining >> trigger_retraining


monitoring_dag()
```

Four arms out of one branch; `decide` returns the `task_id` of the one to run.
There is no join at the end, because nothing needs to run after all of them.

### Run it

Forcing the label-drift budget to zero guarantees `decide()` takes the
retraining branch, so you see the interesting path rather than whichever one
your current data happens to deserve:

```bash
docker compose exec airflow-scheduler \
    airflow dags test monitoring --conf '{"max_label_tvd": 0.0}'
```

> **This command takes about 70 seconds and looks frozen for 60 of them.** That
> is not a hang. `request_retraining` is the human-in-the-loop gate: it enters
> `awaiting_input` and waits out its full 60-second `response_timeout` before
> defaulting to Approve, because nobody is going to click the button. Let it
> sit.

**Checkpoint.** In order, roughly a minute apart:

```
Branch into request_retraining
Waiting for response                         ← state becomes awaiting_input
Approved. Proceeding with downstream tasks…  ← 60s later, the default resolves
Dag Run triggered successfully.  trigger_dag_id=mlops
```

That last line is real. Check for yourself — `airflow dags list-runs mlops -o
plain` now has a fresh `manual__…` run that this Dag created. One Dag decided
another Dag should run, and a person was given the chance to say no.

While it is in `awaiting_input`, open the Dag in the UI: the prompt is waiting
for you under the **Required Actions** tab (§5), and answering it there resolves
the task without waiting for the timeout. Airflow records the answer either way
— the choice that was made, when, and *by whom*. A run that timed out into its
`defaults` stores the choice with the "who" left empty, which is how you tell "a
person approved this" from "nobody was looking".

> **Without the `--conf` override** the outcome depends on state, which is the
> point of a monitor. If §10 promoted a model and the data has not moved,
> `decide()` returns `continue_serving`, the retraining arm is skipped, and the
> run is over in seconds. If there is no production model yet, `decide()` says
> so — `No production model yet - train one.` — and takes the retraining
> branch anyway, timeout included. Both are correct. Run it to see a branch
> *not* taken.

Reference: [`solutions/06_monitoring_and_retraining.py`](solutions/06_monitoring_and_retraining.py).

### What "retrain" leaves out

`trigger_retraining` fires `mlops`, which fits on whatever is in the warehouse.
That is the orchestration; it is not the whole retraining problem. Each of these
is a task slot in a real version of this Dag:

| Step | Why it is not automatic |
|---|---|
| **labeling** | the new rows need real labels, not the model's own predictions fed back as truth |
| **active learning** | label the rows that would teach the model most — you rarely have budget for all of them |
| **QA** | audit the labels before training on them: false positives, labeller disagreement, weak slices |
| **augmentation** | expand the new data so a small window is not drowned out by history |
| **sampling** | up/downsample to fix the imbalance the drift introduced |
| **evaluation** | build the held-out set to reflect *deployment* conditions |

The other constraint is **catastrophic forgetting**: fitting only on the new
window trades yesterday's failure mode for tomorrow's. The Dag here sidesteps it
by always training on the full labelled table — fine at this size, and the first
thing to revisit when the table stops fitting in a task.

Which is also why the promotion gate in §10 compares against the current
production model instead of a fixed threshold alone: a retrained model that is
worse than what is serving should not ship just because a monitor fired.

## 13. Deadline alerts

A **deadline** answers "tell me if this run is taking too long". It is attached
to the Dag, applies to the whole run, and is evaluated *while the run is still
going* — so you hear about a stuck pipeline during the incident, not in the
morning report.

### Build it

`dags/07_deadline_alerts.py` is the shortest file of the seven, and all of it
is the decorator:

```python dags/07_deadline_alerts.py
"""Lesson 7 - deadline alerts: "tell me if this run is taking too long"."""

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
```

Three pieces: a **reference** (where the clock starts), an **interval** (how
long after it), and a **callback** — named as a dotted path, because it runs
somewhere other than the task, with `kwargs` passed through. Pass a list to
`deadline=` for several budgets.

References: `DAGRUN_QUEUED_AT`, `DAGRUN_LOGICAL_DATE`, `FIXED_DATETIME(dt)`,
`AVERAGE_RUNTIME(max_runs=10)` — that last one is "slower than this Dag's own
recent average", which is usually what you actually mean. Callbacks:
`SyncCallback` (runs on the executor) or `AsyncCallback` (runs on the triggerer —
use it for Notifiers, e.g. `AsyncCallback(SlackWebhookNotifier, kwargs={...})`).

The Dag deliberately misses its own deadline:

```python dags/07_deadline_alerts.py
    @task
    def slow_step() -> str:
        time.sleep(75)
        return "finished, but late"

    slow_step()


deadline_alerts()
```

### Run it

```bash
# unpause first: the 30-second clock starts when the run is queued, i.e. at the trigger
docker compose exec airflow-scheduler airflow dags unpause deadline_alerts
docker compose exec airflow-scheduler airflow dags trigger deadline_alerts

# give it 40 seconds, then:
docker compose logs airflow-scheduler | grep "DEADLINE MISSED"
```

**Checkpoint.** One `DEADLINE MISSED` line, about 30 seconds after the trigger,
carrying the run's context — and `slow_step` is still running when it fires,
because that task sleeps for 75 seconds. That is the point: you heard about the
slow run before it finished. Missed and pending deadlines are also listed under
Browse → **Deadlines**.

> **Known issue on 3.3.1:** `airflow dags test` crashes on any Dag with a
> `deadline=` (`psycopg2.ProgrammingError: can't adapt type 'dict'` out of
> `DagRun.update_state`). The tasks run fine; only the CLI's final state update
> fails. Trigger such Dags through the scheduler instead — which is why `mlops`
> carries no deadline, since it would break the `airflow dags test mlops`
> instruction above. [Details.](docs/troubleshooting.md)

Reference: [`solutions/07_deadline_alerts.py`](solutions/07_deadline_alerts.py).

## 14. Testing Dags

Four levels, cheapest first — and the first of them is the grader for the seven
files you wrote.

**Dag integrity tests** — [`tests/test_dag_integrity.py`](tests/test_dag_integrity.py).
These catch the failures that otherwise surface as a red banner in the UI hours
later, and they are the single highest-value thing you can put in CI:

```python
from airflow.dag_processing.dagbag import DagBag

def test_no_import_errors(dagbag):
    assert not dagbag.import_errors

def test_mlops_is_scheduled_on_the_labeled_projects_asset(dagbag):
    asset_names = {a.name for a in dagbag.dags["mlops"].timetable.asset_condition.objects}
    assert "labeled_projects" in asset_names
```

They load **your `dags/` folder**, and they encode what this tutorial meant:
seven specific `dag_id`s; every Dag with tags, a `doc_md` and an owner that is
not `airflow`; `retries` of at least one on every Dag that does real work;
`dataops` producing both assets; `mlops` scheduled on `labeled_projects` and
validating before it trains; no cycles. It also greps the sources for
constructs that do not exist in this version of Airflow — `schedule_interval=`,
`days_ago(`, `airflow.operators.`, `execution_date`, `SubDagOperator`, `sla=`.
Most Airflow code on the internet predates Airflow 3, so a copy-pasted snippet
can bring one of these in, and several of them still *import* and fail only
once the task runs.

**Unit tests on the workload** — [`tests/test_tagifai.py`](tests/test_tagifai.py).
Nothing in `src/tagifai/` imports Airflow, so the ML code is testable without a
scheduler in the loop. That separation is deliberate: Airflow orchestrates the
code, it does not contain it.

**Whole-run tests** — `airflow dags test <dag_id>` executes a real Dag run
in-process, and `airflow tasks test <dag_id> <task_id>` runs one task without
writing state.

**Tests on the documentation** — two of them, because a tutorial that quotes
code is only useful while the quotes are true.
[`tests/test_readme_listings.py`](tests/test_readme_listings.py): the appendix
in §16 shows every solution in full, injected from `solutions/` by
[`scripts/sync_readme.py`](scripts/sync_readme.py) rather than copied by hand;
the test re-runs the injection in memory and fails if the result differs.
[`tests/test_readme_build_steps.py`](tests/test_readme_build_steps.py): every
"Build it" block above is tagged with the file it belongs to; the test
concatenates them in order, loads the result with `DagBag`, and fails if what
you were told to type does not add up to a Dag that imports.

Run all of it:

```bash
docker compose run --rm --entrypoint pytest airflow-cli /opt/airflow/tests -q
```

`airflow-cli` is not one of the five services `docker compose ps` lists: it is
a one-off container the compose file defines under a profile, which `run`
creates for that command and `--rm` removes afterwards.

**Checkpoint.** A row of dots and `36 passed`, in well under a minute:

```
....................................                                     [100%]
36 passed in 7.11s
```

No scheduler was involved and no Dag ran. That is the point: every one of those
36 is fast enough to run on every commit, which is what makes them worth having.

When one fails, the message says which file and what is missing — `mlops has no
retries`, `Dags not yet written in /opt/airflow/dags: ['monitoring']`,
`dataops` missing an outlet — and the fix is in the lesson that built that
file, or in `solutions/`. To see the suite pass against the finished versions,
point it there:

```bash
docker compose run --rm -e DAGS_DIR=solutions --entrypoint pytest airflow-cli /opt/airflow/tests -q
```

> On **Git Bash**, prefix both commands with `MSYS_NO_PATHCONV=1` or the
> `/opt/airflow/tests` argument is rewritten into a Windows path and pytest
> collects nothing.

```bash
python scripts/sync_readme.py --check     # exit 1 when an appendix listing is stale
python scripts/sync_readme.py             # fix it
```

None of those levels would have caught the two bugs in §7's "trap" box:
`airflow dags test` always supplies a logical date, so a Dag that breaks on a
manual trigger passes every test above. **Trigger each Dag once for real** —
through the scheduler, the way it will actually run — before you believe it.

## 15. Taking it to production

**Executors.** `LocalExecutor` is the default and is fine up to one machine.
Beyond that: `CeleryExecutor` (worker pool), `KubernetesExecutor` (a pod per
task), or **multiple executors** configured together. There is also the **Edge
executor**, for workers outside your cluster — the thing the Task Execution API
(§3) made possible.

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

**Parsing intervals.** This stack scans for files every 5 seconds so that
what you save appears at once. Put the defaults back (`refresh_interval` 300,
`min_file_process_interval` 30) on anything with hundreds of Dag files — the
processor's job is to parse, not to poll.

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
  and `dag_processing.import_errors`. Metric names carry `dag_id`/`task_id` as
  **tags**; `[metrics] legacy_names_on` (default `True`) also exports the older
  name-suffix form.
- **Traces** — `[traces] otel_on = True` sends spans for scheduler loops and
  task runs, which is how you find *which* task in a slow Dag got slower.
- **Health** — `GET /api/v2/monitor/health` reports scheduler, triggerer and Dag
  processor status; the UI Home page renders the same thing.

Then the ordinary hygiene: `airflow db clean` on a schedule (the metadata DB
grows forever otherwise), deadline alerts on the Dags that matter (§13), and
Notifiers — `SlackWebhookNotifier`, `SmtpNotifier` — wired to
`on_failure_callback` or an `AsyncCallback`.

## 16. Appendix: the finished Dags in full

The lessons above give you each file in pieces. Here is each one end to end,
in reading order, as it sits in [`solutions/`](solutions/) — the same code you
typed, plus the comments, which explain the details at the point they matter.

> These listings are **injected from the files** by
> [`scripts/sync_readme.py`](scripts/sync_readme.py) and verified by
> [`tests/test_readme_listings.py`](tests/test_readme_listings.py): edit a
> solution without re-running the script and the test suite fails. They cannot
> drift out of sync with the code that actually runs.

The shared Asset definitions come first, because every Dag below imports them.
<details>
<summary><b>src/tagifai/assets.py</b> — the Assets the Dags produce and consume (§8)</summary>

<!-- BEGIN FILE: src/tagifai/assets.py -->
```python
"""The Assets shared by the Dags.

An Asset is a named piece of data, and it is how the Dags in this tutorial are
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

# Produced by `mlops` when a model clears the promotion gate. Nothing is
# scheduled on it yet - point a Dag's `schedule` at it and the edge appears in
# the UI's Assets graph.
PRODUCTION_MODEL = Asset(
    name="production_model",
    group="model",
    extra={"owner": "ml-engineering"},
)
```
<!-- END FILE: src/tagifai/assets.py -->

</details>

<details>
<summary><b>solutions/01_hello_airflow3.py</b> — a Dag, tasks, dependencies, params (§4)</summary>

<!-- BEGIN FILE: solutions/01_hello_airflow3.py -->
```python
"""Lesson 1 - a Dag, tasks, and dependencies.

Everything a Dag author needs comes from one place: `airflow.sdk`, the Task SDK.
`dag`, `task`, `DAG`, `Param`, `Asset`, `chain`, `Label` - all of it is there.

Four tasks, wired by passing one task's output to another. Read `make_greeting`
first, then the two lines at the bottom that build the graph.

Run it without a scheduler:
    airflow dags test hello_airflow3
"""

from __future__ import annotations

import pendulum

from airflow.sdk import Param, dag, task

DEFAULT_ARGS = {
    "owner": "mlops-course",
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=30),
}


@dag(
    dag_id="hello_airflow3",
    # `schedule` takes None (manual/API only), a cron string, a timedelta, a
    # Timetable, or a list of Assets (see 03_dataops / 04_mlops).
    schedule=None,
    # No run is ever scheduled before it. A Dag without one still parses, so
    # Airflow will not remind you - set it.
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    # Defaults to False; being explicit is good manners. This is the flag that
    # decides whether un-pausing a Dag floods you with historical runs.
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
    """A four-task Dag: one source, two branches of work, one join."""

    @task
    def make_greeting(**context) -> str:
        # `**context` gives you the run's metadata: run_id, params, ti,
        # dag_run, ds, data_interval_start/end, var, conn, macros.
        #
        # Read `logical_date` with `.get()`, never `context["logical_date"]`:
        # a manual or asset-triggered run has no logical date, and the key is
        # then missing from the context entirely - indexing it raises KeyError
        # at runtime while `airflow dags test` (which always supplies one)
        # stays green. `run_id` is always there; prefer it for naming things.
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
<!-- END FILE: solutions/01_hello_airflow3.py -->

</details>

<details>
<summary><b>solutions/02_operators_and_xcoms.py</b> — operators, XComs, relationships (§6)</summary>

<!-- BEGIN FILE: solutions/02_operators_and_xcoms.py -->
```python
"""Lesson 2 - operators, XComs, and relationships.

Lesson 1 used `@task`-decorated functions. This one uses **operators**, the
pre-built task types, and shows that the two styles mix freely in one Dag.

Two things to take away:

1. The common operators live in `apache-airflow-providers-standard`, which is
   pre-installed. That is where you import them from:

       from airflow.providers.standard.operators.bash import BashOperator

2. `xcom_pull()` pulls from the *current* task unless you name the source, so
   an XCom read almost always wants `task_ids=`:

       ti.xcom_pull(task_ids="push_seed", key="seed")

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
    """Operator style: push an XCom under an explicit key.

    `ti` (the task instance) is injected because the parameter is named `ti` -
    the same name-based injection `**context` does in lesson 1.
    """
    ti.xcom_push(key="seed", value=7)


def _multiply(ti, factor: int) -> None:
    # Name the task you are pulling from. Without `task_ids` this reads the
    # current task's own XComs and quietly returns None.
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

    # Some operator arguments are *templated*: Airflow renders them with Jinja
    # just before the task runs, so `{{ run_id }}`, `{{ ds }}` and
    # `{{ ti.xcom_pull(...) }}` are filled in with this run's values.
    #
    # Careful with `ds`: it is derived from `logical_date`, and a manual or
    # asset-triggered run has none. The variable is then *undefined* and the
    # task fails with `UndefinedError: 'ds' is undefined` while rendering.
    # `airflow dags test` always supplies a logical date, so this only shows up
    # once a real trigger reaches it. Guard it with `default`, or template
    # `{{ dag_run.run_after }}`, which every run has.
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
<!-- END FILE: solutions/02_operators_and_xcoms.py -->

</details>

<details>
<summary><b>solutions/03_dataops.py</b> — extract → validate → transform → validate, and the outlet (§9)</summary>

<!-- BEGIN FILE: solutions/03_dataops.py -->
```python
"""Lesson 3 - the DataOps Dag: extract -> validate -> transform -> validate.

The first Dag that does real work. Every task is a slot you can swap for the
vendor tool of a production stack, without changing the Dag's shape:

    extract_projects   -> AirbyteTriggerSyncOperator / FivetranOperator / an EL job
    validate_*         -> GreatExpectationsOperator / SodaOperator / dbt test
    transform          -> DbtCloudRunJobOperator / astronomer-cosmos task group

Three things to watch for:

* The Dag **produces Assets** (`outlets=[...]`). Finishing the producing task
  emits an asset event, and that event is what schedules the MLOps Dag - no
  cron guessing, no sensor, no polling.
* Asset events carry metadata (`outlet_events[asset].extra`), so the consumer
  can see the row count that triggered it.
* Validation results move through XCom as small dicts; the dataframes never do.
  Tasks exchange paths and counts, not payloads.

Run it:
    airflow dags test dataops
"""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.sdk import dag, task, task_group

# Task code imports exceptions from the Task SDK. It is the namespace a remote
# worker has, and the one to reach for in a task.
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
    # A cron string uses CronTriggerTimetable: the run fires *at* 03:00 and its
    # `logical_date` is 03:00. Straightforward, but worth stating, because a
    # timetable is free to define that differently.
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
        """Raw -> curated `labeled_projects` - the dbt model of a real stack, in pandas."""
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
        """A task group nests tasks under one collapsible node in the UI."""

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
<!-- END FILE: solutions/03_dataops.py -->

</details>

<details>
<summary><b>solutions/04_mlops.py</b> — asset-triggered training, mapping, promotion gate (§10)</summary>

<!-- BEGIN FILE: solutions/04_mlops.py -->
```python
"""Lesson 4 - the MLOps Dag: asset-triggered training, tuning, and promotion.

The payoff of data-aware scheduling. This Dag has no cron. It runs when
`dataops` emits the `labeled_projects` asset event - which only happens when
the data passed validation.

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

Deadline alerts live in `07_deadline_alerts.py` - see the note there for why
they are not attached to this Dag.

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
<!-- END FILE: solutions/04_mlops.py -->

</details>

<details>
<summary><b>solutions/05_sensors_and_events.py</b> — sensors, deferrable tasks, watchers (§11)</summary>

<!-- BEGIN FILE: solutions/05_sensors_and_events.py -->
```python
"""Lesson 5 - waiting for things: sensors, deferrable operators, and events.

Three ways to react to the outside world, cheapest last:

1. **Poking sensors** - a worker slot is held while polling. Fine for seconds.
2. **Deferrable sensors** (`*Async`, or `deferrable=True`) - the task suspends,
   the slot is released, and the triggerer resumes it. This is what you want for
   anything measured in minutes or hours.
3. **Event-driven scheduling** (`AssetWatcher`) - no task waits at all: an
   external event emits an asset update and *that* starts the Dag run. See the
   note at the bottom.

Run it:
    airflow dags test sensors_and_events       # everything it waits for is already true
    airflow dags trigger sensors_and_events    # through the scheduler (unpause first):
                                               # `settle` visibly sits in `deferred`

`dags test` never defers `settle`: the sensor's five-second target is measured
from the run's logical date, which the command fixes before the task starts, so
the moment has passed by the time the task runs and it returns at once. Only a
scheduler-driven run shows the `deferred` state.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensorAsync
from airflow.sdk import PokeReturnValue, dag, task

from tagifai import config, data

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
    # Deferrable: releases its worker slot while waiting. Needs `airflow triggerer`,
    # and a scheduler-driven run to see it - `dags test` returns at once (docstring).
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
        # Read it as a table: descriptions contain newlines, so counting lines
        # would overstate the row count.
        rows = len(data.load_labeled())
        print(f"curated table has {rows} rows")
        return PokeReturnValue(is_done=rows >= 500, xcom_value=rows)

    @task
    def proceed(rows: int) -> None:
        print(f"Data is ready ({rows} rows). Downstream work can start.")

    settle >> wait_for_curated_data >> proceed(enough_rows())


sensors_and_events()


# --- Event-driven scheduling -------------------------------------------------
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
<!-- END FILE: solutions/05_sensors_and_events.py -->

</details>

<details>
<summary><b>solutions/06_monitoring_and_retraining.py</b> — drift, branching, human-in-the-loop (§12)</summary>

<!-- BEGIN FILE: solutions/06_monitoring_and_retraining.py -->
```python
"""Lesson 6 - continual learning: monitor, decide, and act.

A monitoring event has four sensible responses, and this Dag makes them
explicit branches:

    continue  keep serving; log the alert and move on
    improve   retrain on new data (triggers the mlops Dag)
    inspect   the label space or schema changed - a human should look
    rollback  live performance collapsed - revert to the previous model

Pieces on show:

* `@task.branch` picking one arm of the response.
* `ApprovalOperator` (human-in-the-loop): retraining waits for a person to
  click Approve in the UI. `defaults` + `response_timeout` mean an unattended
  run still resolves instead of hanging forever.
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
<!-- END FILE: solutions/06_monitoring_and_retraining.py -->

</details>

<details>
<summary><b>solutions/07_deadline_alerts.py</b> — deadline alerts, and a Dag that misses one (§13)</summary>

<!-- BEGIN FILE: solutions/07_deadline_alerts.py -->
```python
"""Lesson 7 - Deadline Alerts: "tell me if this run is taking too long".

A deadline is attached to the **Dag**, applies to the whole run, and is
evaluated *while the run is still going* - so you hear about a stuck pipeline
during the incident, not in the morning report.

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

KNOWN ISSUE (verified on 3.3.1): `airflow dags test <dag_id>` crashes on
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
<!-- END FILE: solutions/07_deadline_alerts.py -->

</details>

The rest of `src/tagifai/` — `data.py`, `validate.py`, `train.py`,
`evaluate.py`, `registry.py`, `monitoring.py`, `predict.py`, `config.py`,
`callbacks.py` — is the ML project being orchestrated, not the orchestration, and
imports no Airflow. It is in the repo; it is not reproduced here.

---

## 17. What was actually verified

The body of this tutorial says what you will see. This section says how that
is known: every figure and claim above was read off a live stack, and this is
the record of when, and how it was measured. Nothing here is a step to run.
Skip it, or come back when you want to know how far to trust a number.

Against a live `apache/airflow:3.3.1` stack (Postgres 16, LocalExecutor,
api-server + scheduler + dag-processor + triggerer), on 2026-09-09:

- All 7 Dags parse with **zero import errors** (`airflow dags list-import-errors`).
- `airflow dags test` runs green for `hello_airflow3`, `operators_and_xcoms`,
  `dataops`, `mlops`, `sensors_and_events`, `monitoring`.
- The DataOps → Asset → MLOps chain fires for real: triggering `dataops`
  produced an `asset_triggered__…` run of `mlops` with no manual trigger.
- `mlops` validated the snapshot (`suite=labeled_projects passed=6/6
  suite=training_split passed=5/5`), trained 4 mapped candidates, selected the
  best (f1 = 0.884) and promoted it.
- The HITL path resolved: `awaiting_input` → default `Approve` after the
  timeout → `TriggerDagRunOperator` fired.
- The deadline callback fired at exactly +30s with the full `dag_run` context.
- Manual triggers were exercised: a trigger against a **paused** Dag produced a
  run stuck in `queued` until `airflow dags unpause`, and
  `-c '{"greeting": "hola"}'` reached the task as a `params` override.
- The §5 UI walkthrough was checked against this stack's api-server: nav, tabs,
  trigger/backfill form fields and the version selector come from the shipped UI
  strings, and the endpoints behind them (`/api/v2/assets`, `/backfills`,
  `/dags/{id}/dagVersions`, `/hitlDetails`) from its OpenAPI spec.
- `pytest`: **30 passed**.

Re-verified on 2026-09-11, against the same stack, for the numbers and claims
the body prints back to you:

- §4's checkpoint: the first Dag logs `(7 words)` — the greeting carries the
  run id *and* the date.
- §8's asset rules, tested both ways. With `mlops` **unpaused**, `airflow dags
  test dataops` triggered a real `asset_triggered__` run of `mlops` — `dags
  test` writes genuine asset events, it is not a dry run. With `mlops`
  **paused**, the asset event was written but no run was queued, and
  un-pausing afterwards did **not** replay it. Airflow filters paused Dags when
  it decides what to enqueue.
- §10's wait, decomposed from database timestamps over three triggers: the
  `dataops` run itself took 5.2–5.9s from queued to finished; the gap from its
  asset event being written to the `mlops` run being queued was 0.12–0.65s —
  one scheduler loop. The text quotes no end-to-end figure because the
  `dataops` part depends on the network.
- §11's `settle` (`TimeDeltaSensorAsync`, 5s) does **not** defer under `airflow
  dags test`: its target is measured from the run's logical date, which the
  command sets before the task starts, so the moment has already passed when
  the task runs and it returns at once — no `deferred` state, no triggerer
  (`sensors/time_delta.py`: `if utcnow() > target_dttm: return`). A deferring
  task under `dags test` would not reach the triggerer either: the command runs
  triggers inline (`sdk/definitions/dag.py`, `_run_inline_trigger`). Sent
  through the scheduler with `airflow dags trigger`, the same task sat in
  `deferred` for six seconds, went to `scheduled`, then `success` — polled every
  half-second from the `task_instance` table.
- §12's `ApprovalOperator` on 3.3 parks the task in `awaiting_input` with no
  trigger involved; the scheduler's sweep resolves it — `AWAITING_INPUT timeout
  sweep: 1 resolved (response/defaults)` in the scheduler log — and the task
  resumes in `execute_complete`. The triggerer is not needed for HITL.
- §10's fan-out is **two at a time, in two waves** (`max_active_tis_per_dag=2`),
  read off the four mapped task instances' start and end times — not four at
  once.
- §11's reschedule mode: with the curated file moved away, both sensors sat in
  `up_for_reschedule`; restoring the file let the next poke succeed and the run
  finished.
- §12's timing: **69 seconds**, of which **60** were `request_retraining` in
  `awaiting_input` before the `Approve` default resolved and
  `TriggerDagRunOperator` created a real `mlops` run. Without the `--conf`
  override the same Dag took 7 seconds and branched to `continue_serving`.
- §3's numbers are this stack's live config: `parallelism=32`,
  `max_active_tasks_per_dag=16`, `default_pool=128` slots,
  `scheduler_idle_sleep_time=1`, `min_file_process_interval=30`,
  `default_task_retries=0`.
- §7's data-interval shapes, read off the timetables themselves: a cron string
  is a `CronTriggerTimetable` here (`create_cron_data_intervals=False`) and
  gives `run_after == data_interval_start == data_interval_end`;
  `timedelta(days=1)` is a `DeltaDataIntervalTimetable` and gives `run_after`
  2026-09-11 00:00 with `logical_date == data_interval_start` 2026-09-10 00:00.
- §4/§7's `start_date`: a Dag file with none parses with **no import error**
  under `DagBag`, for `schedule=None` and `@daily` alike.
- §4's `dags test` box: `hello_airflow3` ran `make_greeting → shout →
  count_words → report`, each finishing before the next started.
- §13, unpause then trigger: `DEADLINE MISSED` at +31s, `slow_step` still
  running.
- §10's Try-it in its explicit form (`bash -c 'export TAGIFAI_MIN_F1=0.99;
  airflow dags test mlops'`): `promote_model` skipped, `keep_current_model`
  success.
- §3's `curl` note: on a Windows machine PowerShell resolves `curl` to
  `Invoke-WebRequest`; `curl.exe` is in `System32`.
- §11's `enough_rows` reads the table with pandas and reports **764** rows, the
  number `dataops` wrote.

Finally, a cold run on 2026-09-11 — `reset.sh`, `up.sh`, then every command
in §2–§14 in order:

- 28s from `up.sh` to seven Dags listed, all paused; the health endpoint
  `healthy` four times.
- §4 `(7 words)`, and `HOLA` with `--conf`. Tasks ran one at a time —
  `count_words` before `shout` this time; the order between siblings varies.
- §6 `product=42`. §7 `manual-demo-1` success; `demo` queued while paused,
  success after un-pause with `start_date` 14s after `run_after`.
- §9's backfill (then in §7), both ways. Without `--dry-run`, `2026-01-01`→`01-31` created
  30 queued `backfill__` runs against the paused `dataops`; at §10 they ran
  three at a time and re-triggered `mlops` behind them. The CLI has only
  `backfill create`; `PUT /api/v2/backfills/1/cancel` returned 200 and turned
  the six still-queued runs `failed`. With `--dry-run`, `02-01`→`02-04` listed
  three rows and created nothing — which is why §9 leads with it.
- §8's coalescing: 22 asset events became 6 `mlops` runs, consuming 2–6 each
  (`dagrun_asset_event`).
- §9 `passed=4/4`, `6/6`, `Wrote 764 rows`; with `mlops` paused, one event and
  zero runs.
- §11 `deferred → scheduled → success` through the scheduler; both sensors
  `up_for_reschedule` with the file moved; `764 rows` once restored.
- §12 76s with `--conf` — `awaiting_input`, `Approved`, `trigger_dag_id=mlops`,
  one new `manual__` run; 9s and `continue_serving` without it.
- §13 `DEADLINE MISSED` at +33s with `slow_step` still `running`. §14 30
  passed, listings in sync.

Added on 2026-09-14, same stack: §4's `list-import-errors`. With the seven
lesson files, `No data found`. With an eighth file whose second line imports a
module that does not exist, one row carrying the traceback — and `airflow dags
list` still showed seven, because a file that fails to import is no Dag, not a
broken one. Deleting the file and restarting the dag-processor cleared the row.

Also on 2026-09-14: §2's start-up, cold, with `AIRFLOW_UID=1000` in `.env`
(what `up.sh` writes on WSL2). With the previous compose file, `airflow-init`
failed inside 0.2 s — `ModuleNotFoundError: No module named 'airflow'` — and
exited 0 anyway; the four services then restarted forever on `Database
migration required`, and `up.ps1` waited forever. Replayed in a one-off
container under the same conditions, the failure is deterministic: `HOME=/`,
so the `airflow` script's interpreter never sees `/home/airflow/.local`. With
init routed through the image entrypoint, the same `.env` and the same empty
database: `Database migration done!`, health endpoint `healthy` 33 s after
`up`, seven Dags listed.

Restructured on 2026-09-15 into a write-it-yourself tutorial — `dags/`
empty, the finished files moved to `solutions/`, every lesson rebuilt as typed
pieces — and cold-run again in the new order: `reset.sh`, `up.sh`, the seven
files typed from the "Build it" blocks, every command in §2–§14 as written.

- §2 with an empty `dags/`: `airflow dags list`, `list-import-errors` and
  `assets list` all print `No data found`; the health endpoint `healthy` four
  times.
- §3's parsing intervals are this stack's live config: `refresh_interval=5`,
  `min_file_process_interval=5`, `scheduler_idle_sleep_time=1`. A file saved
  into `dags/` was listed by `airflow dags list` **2 seconds** later.
- §4's broken file: with the comment `# An airflow dag file that does not
  import`, the row was in `list-import-errors` 6 s after saving and gone 6 s
  after deleting the file, no restart. **Without** the words `airflow` and
  `dag` in the file, nothing appeared in 90 s and the Dag processor's log never
  mentioned the file. `[core] dag_discovery_safe_mode` is `True` here, and
  `might_contain_dag_via_default_heuristic` (`airflow/utils/file.py`) requires
  the lowercased contents to contain `airflow` and one of `dag` / `asset`. The
  2026-09-14 check above passed because its comment happened to contain both.
- §4 `(7 words)`, `HOLA` with `--conf`. §6 `product=42`, then `TaskFlow saw
  product=42`.
- §7's catch-up rule, read off `CronTriggerTimetable.next_dagrun_info`
  (`airflow/timetables/trigger.py`): with `catchup=False` and no previous run,
  the start is `max(align_to_prev(utcnow()), align_to_next(start_date))` — the
  most recent tick before now. On this stack, un-pausing `dataops` at 12:22
  UTC created `scheduled__2026-09-15T03:00:00+00:00` with a `start_date` of
  12:22:28, five seconds after the un-pause. The earlier text's "you get the
  next one only" was wrong and is gone.
- §9 with `dataops` saved and nothing run: `assets list` shows
  `labeled_projects` and `raw_projects` only; `assets details` has
  `producing_tasks [dataops.validate_transformed]` and `scheduled_dags []`.
  `dags test dataops`: `passed=4/4`, `Wrote 764 rows`, `passed=6/6`, both
  `checks, failed=none` lines, 5.3 s. Backfill dry-run `02-01`→`02-04`: three
  rows, nothing created.
- §8's third case, a consumer that did not exist yet. The asset event from
  that `dags test dataops` (id 2 in `asset_event`) was never consumed: after
  `mlops` was written and un-paused, `dagrun_asset_event` links its runs to
  events 4 and 6 — the `scheduled__` and a later `manual__` `dataops` run —
  and never to 2.
- §10 with `mlops` saved: `scheduled_dags [mlops]`. Un-pausing `mlops`, then
  `dataops`, no trigger: one `scheduled__…T03:00:00` `dataops` run; one
  `asset_triggered__` `mlops` run queued 1.5 s after its event, with an empty
  `logical_date`; `train_candidate` map indexes 2 and 3 started after 0 and 1
  ended; `promote_model` success, `keep_current_model` skipped. The Try-it
  with `TAGIFAI_MIN_F1=0.99`: `below threshold 0.99 - not promoting`, `Branch
  into keep_current_model`.
- §11 `dags test`: `curated table has 764 rows`, `Success criteria met`,
  `Data is ready (764 rows)`, 5.6 s. Through the scheduler, polled every half
  second: `settle` `deferred` at +2 s, `success` at +8 s.
- §12 with `--conf`: 74 s; `Branch into request_retraining`, `Waiting for
  response`, `Approved` 65 s later, `trigger_dag_id=mlops`, and one new
  `manual__` run of `mlops`.
- §13: `DEADLINE MISSED` at +31 s, `slow_step` still running.
- §14: the seven typed files pass the integrity suite (27 with the workload
  tests); the same suite with `DAGS_DIR=solutions` passes; the "Build it"
  blocks in both READMEs assemble into files that import; **36 passed**
  in total.
- §5's two "Do it now" steps and the UI tour rest on the shipped UI strings
  and the API endpoints behind them, as before — nobody clicked through them
  for this run.

Six real defects were found while building this, all documented in
[troubleshooting](docs/troubleshooting.md):

- `fs_default` is not created for you.
- `airflow dags test` breaks on Dags with a `deadline=`.
- `DagBag` needs importing from `airflow.dag_processing.dagbag`.
- `context["logical_date"]` raises `KeyError` on any run without a logical
  date — found by a manual trigger, after every test in §14 had passed.
- `{{ ds }}` raises `UndefinedError` on the same runs — found the same way.
- `airflow-init` bypassed the image entrypoint, could not import Airflow, and
  still exited 0 — found by a cold start with a WSL2 uid in `.env`.

## Repository layout

```
dags/                       your workspace - empty until you write the seven Dags
solutions/                  the finished version of each, numbered in reading order
src/tagifai/                the ML project Airflow orchestrates (imports no Airflow)
  config.py                 paths and thresholds
  data.py                   extract / clean / transform / split
  validate.py               a tiny expectation-suite runner
  train.py  evaluate.py     TF-IDF + SGD classifier, metrics and slices
  registry.py               filesystem stand-in for a model registry
  monitoring.py             drift and live-performance checks
  predict.py                inference against the promoted model
  assets.py                 the Airflow Assets the Dags share
  callbacks.py              deadline-alert callbacks
tests/                      Dag integrity (your dags/), unit tests, README checks
docs/                       CLI cheatsheet, troubleshooting
scripts/                    up / reset / native setup / README sync
docker-compose.yaml         LocalExecutor + Postgres + Simple auth
```

## Further reading

- [Airflow 3 documentation](https://airflow.apache.org/docs/apache-airflow/stable/)
- [Task SDK reference](https://airflow.apache.org/docs/task-sdk/stable/api.html)
- [Assets and data-aware scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html)
- [Provider packages index](https://airflow.apache.org/docs/apache-airflow-providers/)
- The ML pipeline this tutorial orchestrates follows the [Made With ML
  orchestration lesson](https://madewithml.com/courses/mlops/orchestration/) —
  useful for the MLOps reasoning; its Airflow code is for an older version.
