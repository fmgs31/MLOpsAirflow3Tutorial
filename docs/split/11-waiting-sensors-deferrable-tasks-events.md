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

### Which way to wait

Deferrable frees the most, but it is not always on offer. What decides first is
the check itself:

- **Deferrable needs a trigger written for that check.** A trigger is
  asynchronous code, and someone has to have written it. Providers ship
  triggers for their common waits: the Google provider has one that asks
  BigQuery whether a table exists, and `BigQueryTableExistenceSensor(deferrable=True)`
  hands it to the triggerer. A check of your own (a row count against a
  threshold, a question to a system with no Airflow provider) has no trigger.
  Writing one means writing asynchronous code that never blocks: the triggerer
  runs all triggers in one process, so one blocking call stalls every wait
  it holds.
- **`poke` and `reschedule` work with any check** written as ordinary Python,
  such as a `@task.sensor` function (Build it shows one). The check runs in the
  task's own process, like any task, with every library the worker has.

When a check can be deferrable or `reschedule`, two things decide:

- **How long, and how often.** Each `reschedule` poke is a full task start: the
  scheduler queues it, and a worker starts a process and loads the Dag file. A
  poke every few minutes for an hour costs little. A poke every few seconds for
  a day adds up, and deferrable is cheaper: one triggerer process holds many
  waits at once.
- **How heavy the check is.** A check that needs pandas, an ML library or a
  query that takes seconds belongs in a task process. A trigger should only
  make quick asynchronous calls.

`poke` is for waits of seconds: it holds the slot the whole time, and in
exchange the task does not restart between pokes.

`dags/05_sensors_and_events.py` chains one of each. `wait_for_table` waits for
the table to exist: the Google provider has the trigger, and the wait may be
long, so it is deferrable. `enough_rows` then waits for the table to have 500
rows: a threshold of your own, with no trigger, so it is a `@task.sensor` in
`reschedule` mode. It starts only once there is a table to count. Here is where
each step waits. Each column is one system, as in the §9 diagram:

```
 Airflow: task process          │ Airflow: the triggerer │ BigQuery
 sensors_and_events Dag         │ (§3)                   │ mlops_course
────────────────────────────────┼────────────────────────┼──────────────────
                                │                        │
 1 wait_for_table (deferrable) ──► every 10 s, asks ──────► does the table
                                │                        │  exist yet?
   resumes ◄────────────────────── fires the event       │
                                │                        │
 2 enough_rows (reschedule)     │                        │
   a poke every 10 s ─────────────────────────────────────► counts its rows
   exits between pokes          │                        │
 3 proceed: 500+ rows           │                        │
```

While `wait_for_table` waits, the waiting happens in the triggerer, and no
task process exists. `enough_rows` does its own checking: a poke every 10
seconds, with its process gone in between.

### Build it

Create `dags/05_sensors_and_events.py` with the header and the first sensor,
`wait_for_table`, written as an operator:

```python dags/05_sensors_and_events.py
"""Lesson 5 - waiting for things: sensors, deferrable operators, and events."""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
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

`deferrable=True` makes it deferrable. Its trigger, running in the triggerer,
asks BigQuery every `poke_interval` seconds whether the table exists, through
the `google_cloud_default` connection the compose file defines (§9). After
`timeout` seconds without the table, the task fails.

The table to wait for comes from a **Dag param**, `table`, with
`labeled_projects` as its default, so a run can be pointed at another table
with `-c`, as you will do in Run it. `table_id` is a templated field (§6), so `{{ params.table }}` is
rendered when the task runs.

A sensor can also be written as a function, with `@task.sensor`: the function
is the check, and Airflow pokes it. The second sensor, `enough_rows`, is one,
in `reschedule` mode (the second of the three ways at the start of §11). Add
it, with the task that uses its value, and the wiring:

```python dags/05_sensors_and_events.py
    @task.sensor(poke_interval=10, timeout=60 * 5, mode="reschedule")
    def enough_rows(*, params: dict[str, Any]) -> PokeReturnValue:
        rows = data.count_rows(params["table"])  # SELECT COUNT(*) in BigQuery
        print(f"{params['table']} has {rows} rows")
        return PokeReturnValue(is_done=rows >= 500, xcom_value=rows)

    @task
    def proceed(rows: int) -> None:
        print(f"Data is ready ({rows} rows). Downstream work can start.")

    rows = enough_rows()
    wait_for_table >> rows
    proceed(rows)


sensors_and_events()
```

Each poke returns a `PokeReturnValue`. With `is_done=False` the task waits
`poke_interval` seconds and pokes again. With `is_done=True` the poking stops
and `xcom_value` is handed downstream: `proceed` receives the row count.

Read the wiring: `enough_rows` starts only after `wait_for_table` succeeds, so
by its first poke the table exists and the check only has to count. The
result of `enough_rows()` goes in a variable, `rows`, because it is used
twice: `wait_for_table >> rows` sets the order, and `proceed(rows)` passes the
row count (§6).

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
rows and clears its threshold on the first poke. Nothing deferred: a
deferrable sensor checks once in its own process before it hands anything to
the triggerer, and defers only if the answer is no.

When a sensor does defer, `airflow dags test`, which has no workers and no
triggerer, runs the trigger in its own process and says so in the log with
`[DAG TEST] running trigger in line` (§4). To see the two ways of waiting as
they really happen, send a run through the scheduler instead.

**Now the interesting case**: sensors that have to wait. Trigger a run that
points at a table that does not exist yet, then check its task states:

```bash
docker compose exec airflow-scheduler airflow dags unpause sensors_and_events
docker compose exec airflow-scheduler airflow dags trigger sensors_and_events -r wait-demo -c '{"table": "labeled_projects_copy"}'   # pick a new id if you rerun
docker compose exec airflow-scheduler airflow tasks states-for-dag-run sensors_and_events wait-demo -o plain
```

**Checkpoint.** After a few seconds, `wait_for_table` reads `deferred`: its
first check found no table, its process has exited, and the triggerer asks
BigQuery every ten seconds. `enough_rows` and `proceed` have
no state yet, because they have not been scheduled. That `deferred` is the
state from §3, and it is what "frees the worker slot" looks like from outside.

Now create the table, with fewer rows than `enough_rows` asks for. In the
Google Cloud console, open BigQuery, open a new query tab, and run, with your
Project ID:

```sql
CREATE TABLE `<your project id>.mlops_course.labeled_projects_copy` AS
SELECT * FROM `<your project id>.mlops_course.labeled_projects` LIMIT 300
```

Wait about twenty seconds and run the `states-for-dag-run` command again.

**Checkpoint.** `wait_for_table` reads `success`: its trigger saw the table.
`enough_rows` reads `up_for_reschedule`, the state §3 gives for a sensor
between pokes. It has poked, counted 300 rows, and exited; ten seconds after
each poke the scheduler queues it again, and it pokes again. Its log in the UI
(§5) shows `labeled_projects_copy has 300 rows` once per poke.

Now give it enough rows, by replacing the copy with the whole table:

```sql
CREATE OR REPLACE TABLE `<your project id>.mlops_course.labeled_projects_copy` AS
SELECT * FROM `<your project id>.mlops_course.labeled_projects`
```

**Checkpoint.** Within about fifteen seconds `enough_rows` counts 764 rows,
and the `states-for-dag-run` command shows all three tasks as `success`.
`proceed`'s log reads `Data is ready (764 rows)`.

Neither sensor held a worker slot while it waited: `wait_for_table` waited in
the triggerer, and `enough_rows` gave its slot back between pokes. In the
default `poke` mode each would have held one for its whole wait, doing
nothing. Stop at either step instead and the sensor waiting there fails on its
`timeout`, five minutes, and the tasks after it end `upstream_failed` (§3).

Drop the copy when you are done, in the same query editor:

```sql
DROP TABLE `<your project id>.mlops_course.labeled_projects_copy`
```

### Not waiting at all: event-driven scheduling

The two sensors in `sensors_and_events` ask again and again whether
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
deferrable if a trigger exists for the check, `mode="reschedule"` if the check
is your own Python, or you are paying a worker slot to sleep. A service that
announces changes itself, by sending a message to a queue → an `AssetWatcher`,
and no task waits at all.

Reference: [`solutions/05_sensors_and_events.py`](../../solutions/05_sensors_and_events.py).

---

← [10. The MLOps Dag](10-the-mlops-dag.md) · [Index](README.md) · [12. Continual learning: monitor, decide, act](12-continual-learning-monitor-decide-act.md) →
