## 11. Waiting: sensors, deferrable tasks, events

`dags/05_sensors_and_events.py`. Three ways to wait for the outside world,
the one that costs fewest worker slots last — and, after them, the one way not
to wait at all.

A **sensor** is a task whose only job is to wait for something to become true:
a file to exist, a table to have rows, an API to answer. Each check is called a
**poke**. If the poke returns true, the task succeeds; if not, it waits
`poke_interval` seconds and checks again; if `timeout` passes without success,
it fails. The three ways differ in what happens *between* pokes:

```python
# 1. `poke` mode (the default) — holds a worker slot the entire time. For waits of seconds, not hours.
BigQueryTableExistenceSensor(task_id="wait", project_id=..., dataset_id=..., table_id=...,
                             poke_interval=10, timeout=300)

# 2. `reschedule` mode — the task's process exits between pokes and is re-queued.
BigQueryTableExistenceSensor(..., mode="reschedule")

# 3. Deferrable — hands the wait to the triggerer and frees the slot entirely.
BigQueryTableExistenceSensor(..., deferrable=True)
```

In the default mode, `mode="poke"`, the task's process stays alive between
pokes, asleep, holding its worker slot. With `mode="reschedule"`, the process
exits after each poke; the task instance sits in `up_for_reschedule` (§3), and
the scheduler re-queues it for the next poke.

The third option, deferrable, needs one term first: a **trigger** is a small
piece of asynchronous code that waits for one thing (a moment in time, a table,
a message) and fires an event when it happens. The triggerer process from §3
runs them. This has nothing to do with *triggering* a Dag (§7), which means
starting a run. "Deferrable" means the task hands the triggerer such a
trigger, exits, and is resumed when the event arrives.

Here is where each step of `dags/05_sensors_and_events.py` waits. Each column
is one system, as in the §9 diagram:

```
 Airflow: task process          │ Airflow: the triggerer │ BigQuery
 sensors_and_events Dag         │ (§3)                   │ mlops_course
────────────────────────────────┼────────────────────────┼──────────────────
                                │                        │
 1 settle (deferrable) ──────────► waits 5 seconds       │
   resumes ◄────────────────────── fires the event       │
                                │                        │
 2 wait_for_table (deferrable) ──► every 10 s, asks ──────► does the table
                                │                        │  exist yet?
   resumes ◄────────────────────── fires the event       │
                                │                        │
 3 enough_rows (reschedule)     │                        │
   a poke every 10 s ─────────────────────────────────────► counts its rows
   exits between pokes          │                        │
 4 proceed: 500+ rows           │                        │
```

While `settle` and `wait_for_table` wait, the waiting happens in the
triggerer, and no task process exists. `enough_rows` does its own checking: a
poke every 10 seconds, with its process gone in between. The numbers label the
tasks, not the order they run in: `enough_rows` starts at the same time as
`settle`, as the wiring in Build it shows.

### Build it

Create `dags/05_sensors_and_events.py` with the header and the two sensors
written as operators, `settle` and `wait_for_table`:

```python dags/05_sensors_and_events.py
"""Lesson 5 - waiting for things: sensors, deferrable operators, and events."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.sdk import Param, PokeReturnValue, dag, task

from project_tagger import config, data

DEFAULT_ARGS = {"owner": "data-engineering", "retries": 0}


@dag(
    dag_id="sensors_and_events",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "sensors"],
    doc_md=__doc__,
    params={"table": Param(config.LABELED_PROJECTS_TABLE, type="string", description="Table to wait for.")},
)
def sensors_and_events():
    settle = TimeDeltaSensor(task_id="settle", delta=timedelta(seconds=5), deferrable=True)

    wait_for_table = BigQueryTableExistenceSensor(
        task_id="wait_for_table",
        project_id=config.GCP_PROJECT_ID,
        dataset_id=config.BQ_DATASET,
        table_id="{{ params.table }}",
        deferrable=True,
        poke_interval=10,
        timeout=60 * 5,
    )
```

Both are deferrable (`deferrable=True`). `settle` waits five seconds. It does
no useful work: its trigger is the simplest kind, a timer, and it is the task
you will watch defer in Run it. `wait_for_table` waits for a BigQuery table to
exist: its trigger, running in the triggerer, asks
BigQuery every `poke_interval` seconds through the `google_cloud_default`
connection the compose file defines (§9).

The table to wait for comes from a **Dag param**, `table`, with
`labeled_projects` as its default, so a run can be pointed at another table
with `-c`, as you will do in Run it. `table_id` is a templated field (§6), so `{{ params.table }}` is
rendered when the task runs.

A sensor can also be written as a function, with `@task.sensor`. The third
sensor, `enough_rows`, is one, in `reschedule` mode (the second of the three
ways at the start of §11). Add it, with the task that uses its value, and the
wiring:

```python dags/05_sensors_and_events.py
    @task.sensor(poke_interval=10, timeout=120, mode="reschedule")
    def enough_rows(*, params: dict[str, Any]) -> PokeReturnValue:
        from google.api_core.exceptions import NotFound

        try:
            rows = data.count_rows(params["table"])  # SELECT COUNT(*) in BigQuery
        except NotFound:
            return PokeReturnValue(is_done=False)
        print(f"{params['table']} has {rows} rows")
        return PokeReturnValue(is_done=rows >= 500, xcom_value=rows)

    @task
    def proceed(rows: int) -> None:
        print(f"Data is ready ({rows} rows). Downstream work can start.")

    settle >> wait_for_table >> proceed(enough_rows())


sensors_and_events()
```

Each poke returns a `PokeReturnValue`. With `is_done=True` the poking stops
and `xcom_value` is handed downstream: `proceed` receives the row count. While
the table does not exist, `data.count_rows` raises BigQuery's `NotFound`, and
the poke returns `is_done=False` instead of failing.

Read the wiring: `enough_rows` has no upstream task, so it starts at once,
alongside `settle`, and `proceed` waits for both `wait_for_table` and
`enough_rows`.

> Deferrable sensors need the triggerer (§3) running. Without it, they sit in
> `deferred` indefinitely.

### Run it

This Dag waits for `labeled_projects`, the table the `dataops` Dag builds, so
you must have run `dataops` in §9, or the sensors will poll until they time
out:

```bash
docker compose exec airflow-scheduler airflow dags test sensors_and_events
```

**Checkpoint.** It finishes in a few seconds, because everything it waits for
has already happened. In the log:

```
labeled_projects has 764 rows
Data is ready (764 rows). Downstream work can start.
```

`wait_for_table` finds the table on its first check; `enough_rows` counts the
rows and clears its threshold on the first poke. And `settle` barely waits:
its five seconds are counted from when the run was created, not from when the
task starts, so some or all of them have passed by the time it runs.

**To watch a task defer**, send the run through the scheduler instead, then
run the last command a few times over the next ten seconds. Be quick: a
`docker compose exec` takes a second or two of its own.

```bash
docker compose exec airflow-scheduler airflow dags unpause sensors_and_events
docker compose exec airflow-scheduler airflow dags trigger sensors_and_events -r defer-demo   # pick a new id if you rerun
docker compose exec airflow-scheduler airflow tasks states-for-dag-run sensors_and_events defer-demo -o plain
```

**Checkpoint.** `settle` reads `deferred` for a few seconds (its process has
exited and the triggerer is holding the timer), then turns `success` about
five seconds after you triggered the run. That `deferred` is the state from §3,
and it is what "frees the worker slot" looks like from outside.

**Now the interesting case**: a sensor that has to wait. Point
the run at a table that does not exist yet:

```bash
docker compose exec airflow-scheduler airflow dags test sensors_and_events -c '{"table": "labeled_projects_copy"}'
```

This time the command sits there waiting. `enough_rows` pokes, gets
`NotFound`, and goes into `up_for_reschedule`, the state §3 gives for a sensor
between pokes; ten seconds later `airflow dags test` runs it again.
`wait_for_table` defers, and its trigger asks BigQuery every ten seconds.

`airflow dags test` has no workers and no triggerer: it runs every task, and
`wait_for_table`'s trigger, in its own process, and says so in the log with
`[DAG TEST] running trigger in line` (§4). No worker slot is involved here. In a run sent through the
scheduler, as `defer-demo` was, neither sensor would hold a worker slot while
it waits: `enough_rows` gives its slot back between pokes, and
`wait_for_table` waits in the triggerer. In the default `poke` mode they would
hold one each for the whole wait, doing nothing.

Now create the table. In the Google Cloud console, open BigQuery, open a new
query tab, and run, with your Project ID:

```sql
CREATE TABLE `<your project id>.mlops_course.labeled_projects_copy` AS
SELECT * FROM `<your project id>.mlops_course.labeled_projects`
```

Within ten seconds both sensors see the table and the run completes, ending
with `labeled_projects_copy has 764 rows`. Do nothing instead and each sensor
eventually fails on its declared `timeout`: 120 seconds for `enough_rows`, 300
for `wait_for_table`. The run fails only once both have given up, after about
five minutes.

Drop the copy when you are done, in the same query editor:

```sql
DROP TABLE `<your project id>.mlops_course.labeled_projects_copy`
```

### Not waiting at all: event-driven scheduling

The three sensors in `sensors_and_events` ask again and again whether
something has happened.
**Event-driven scheduling** turns that around: the outside service sends a
message to a queue (such as Amazon SQS) when something happens, and Airflow
reacts. It does not go in the file, because it needs a queue you do not have.
Attach a *watcher* (a trigger that watches the queue) to an asset, and each
message that arrives updates the asset, which starts a run of every Dag
scheduled on it (§8). No task waits, and no worker slot is used:

```python
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher

trigger = MessageQueueTrigger(queue="https://sqs.us-east-1.amazonaws.com/.../new-projects")
new_projects = Asset(name="new_projects",
                     watchers=[AssetWatcher(name="sqs_watcher", trigger=trigger)])

@dag(schedule=[new_projects])
def ingest_on_message(): ...
```

Only triggers deriving from `BaseEventTrigger` can be watchers. A watcher keeps
watching after each event, so a trigger that tests a lasting condition ("the
table exists") would fire again at once, and start runs forever.

Like deferrable sensors, watchers run in the triggerer: without it, a watcher
sees no messages, so it starts no runs.

**Rule of thumb:** seconds → the default `poke` mode is fine. Minutes or hours →
`mode="reschedule"` or deferrable, or you are paying a worker slot to sleep. A
service that announces changes itself, by sending a message to a queue → an
`AssetWatcher`, and no task waits at all.

Reference: [`solutions/05_sensors_and_events.py`](../../solutions/05_sensors_and_events.py).

---

← [10. The MLOps Dag](10-the-mlops-dag.md) · [Index](README.md) · [12. Continual learning: monitor, decide, act](12-continual-learning-monitor-decide-act.md) →
