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
# 1. Poking sensor — holds a worker slot the entire time. Seconds, not hours.
BigQueryTableExistenceSensor(task_id="wait", project_id=..., dataset_id=..., table_id=...,
                             poke_interval=10, timeout=300)

# 2. `reschedule` mode — the task instance exits between pokes and is re-queued.
BigQueryTableExistenceSensor(..., mode="reschedule")

# 3. Deferrable — suspends onto the triggerer and frees the slot entirely.
BigQueryTableExistenceSensor(..., deferrable=True)
```

In the default mode, `mode="poke"`, the process stays alive and asleep,
holding its worker slot. With `mode="reschedule"`, the task instance exits,
sits in `up_for_reschedule` (§3), and the scheduler re-queues it for the next
poke.

The third option, deferrable, needs the word **trigger** pinned down first,
because it has more than one meaning. You *trigger* a Dag (§7): a verb. The triggerer process from §3
runs **triggers**: nouns — small pieces of asynchronous code that wait for one
thing (a moment in time, a table, a message) and fire an event when it happens.
"Deferrable" means the task hands the triggerer such a trigger, exits, and is
resumed when the event arrives. `TriggerDagRunOperator` (§12) is the verb again.

Here is where each step of `dags/05_sensors_and_events.py` waits. Each column
is one system, as in the §9 diagram; the first two are both parts of Airflow:

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
   every 10 s, a new try ─────────────────────────────────► counts its rows
   exits between tries          │                        │
 4 proceed: 500+ rows           │                        │
```

While `settle` and `wait_for_table` wait, the waiting happens in the
triggerer, and no task process exists. `enough_rows` does its own checking: a
new try every 10 seconds, with the process gone in between.

### Build it

Create `dags/05_sensors_and_events.py` with the header and the two sensors that
are operators, `settle` and `wait_for_table`:

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

Both are the deferrable kind. `settle` waits five seconds. `wait_for_table`
waits for a BigQuery table to exist: its trigger, running in the triggerer, asks
BigQuery every `poke_interval` seconds through the `google_cloud_default`
connection the compose file defines (§9).

Which table is a **Dag param**, `table`, with `labeled_projects` as its
default, so a run can be pointed at another table with `-c`, as you will do
below. `table_id` is a templated field (§6), so `{{ params.table }}` is
rendered when the task runs.

A sensor can also be written as a function. Add it, with the task that uses
its value, and the wiring:

```python dags/05_sensors_and_events.py
    @task.sensor(poke_interval=10, timeout=120, mode="reschedule")
    def enough_rows(*, params: dict[str, Any]) -> PokeReturnValue:
        from google.api_core.exceptions import NotFound

        try:
            rows = data.count_rows(params["table"])
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

`PokeReturnValue(is_done=..., xcom_value=...)` both stops the poking and hands
a value downstream — `proceed` receives the row count. `data.count_rows` runs a
`SELECT COUNT(*)` in BigQuery; while the table does not exist, BigQuery answers
`NotFound`, and the poke reports not done instead of failing. `enough_rows` is
in `reschedule` mode, the second option in the list that opens §11.

Read the wiring: `enough_rows` has no upstream task, so it starts at once,
alongside `settle`, and `proceed` waits for both `wait_for_table` and
`enough_rows`.

One more option removes the wait altogether: **event-driven scheduling**. It
does not go in the file, because it needs a message queue (such as Amazon SQS)
you do not have. Attach a *watcher* (a trigger that watches the queue) to an
asset and each message that arrives starts a run; nothing polls,
nothing occupies a slot:

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
testing a permanent condition ("table exists") would re-fire forever.

> Deferrable sensors and watchers both need `airflow triggerer` running. Without
> it, a deferrable sensor sits in `deferred` indefinitely, and a watcher sees no
> messages, so it starts no runs.

### Run it

This Dag waits for the table `dataops` builds, so §9 must have run or the
sensors will poll until they time out:

```bash
docker compose exec airflow-scheduler airflow dags test sensors_and_events
```

**Checkpoint.** It finishes in a few seconds, because everything it waits for
has already happened. In the log:

```
labeled_projects has 764 rows
Data is ready (764 rows). Downstream work can start.
```

`wait_for_table` finds the table on its first check; `enough_rows`
counts the rows and clears its threshold on the first poke. And `settle`
barely waits: its five-second target is measured from the run's
`data_interval_end` (or its `run_after`), which `airflow dags test` sets when it
starts, so by the time the task runs some or all of that time has passed. If
any is left, there is no triggerer to hand the wait to: `airflow dags test`
runs the trigger itself, and says so in the log with `[DAG TEST] running
trigger in line` (§4).

**To watch a task actually defer**, send the run through the scheduler instead,
then read the task states a few times over the next ten seconds:

```bash
docker compose exec airflow-scheduler airflow dags unpause sensors_and_events
docker compose exec airflow-scheduler airflow dags trigger sensors_and_events -r defer-demo   # pick a new id if you rerun
docker compose exec airflow-scheduler airflow tasks states-for-dag-run sensors_and_events defer-demo -o plain
```

**Checkpoint.** `settle` reads `deferred` for a few seconds — its process has
exited and the triggerer is holding the timer — then turns `success` about
five seconds after the trigger. That `deferred` is the
state from §3, and it is what "frees the worker slot" looks like from outside.
Be quick with the last command; a `docker compose exec` takes a second or
two of its own.

**Now see the interesting case** — a sensor that actually has to wait. Point
the run at a table that does not exist yet:

```bash
docker compose exec airflow-scheduler airflow dags test sensors_and_events -c '{"table": "labeled_projects_copy"}'
```

This time the command sits there waiting. `enough_rows` pokes, gets
`NotFound`, and its task instance **exits** instead of holding a slot: it goes
into `up_for_reschedule`, the state §3 gives for a sensor between pokes, and
the run re-queues it ten seconds later. `wait_for_table` defers, and its trigger
asks BigQuery every ten seconds.

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

While they waited, neither sensor held a worker slot: `enough_rows` was out of
its slot between pokes, and `wait_for_table` was in the triggerer. In the
default mode they would have held one each for the whole wait, doing nothing.

Drop the copy when you are done, in the same query editor:

```sql
DROP TABLE `<your project id>.mlops_course.labeled_projects_copy`
```

**Rule of thumb:** seconds → a poking sensor is fine. Minutes or hours →
`mode="reschedule"` or deferrable, or you are paying a worker slot to sleep. A service that announces
changes itself, by sending a message to a queue → an `AssetWatcher`, and no
task waits at all.

Reference: [`solutions/05_sensors_and_events.py`](../../solutions/05_sensors_and_events.py).

---

← [10. The MLOps Dag](10-the-mlops-dag.md) · [Index](README.md) · [12. Continual learning: monitor, decide, act](12-continual-learning-monitor-decide-act.md) →
