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
FileSensor(task_id="wait", filepath=..., poke_interval=10, timeout=300)

# 2. `reschedule` mode — the task instance exits between pokes and is re-queued.
FileSensor(..., mode="reschedule")

# 3. Deferrable — suspends onto the triggerer and frees the slot entirely.
TimeDeltaSensorAsync(task_id="settle", delta=timedelta(seconds=5))
```

In the default mode, `mode="poke"`, the process stays alive and asleep,
holding its worker slot. With `mode="reschedule"`, the task instance exits,
sits in `up_for_reschedule` (§3), and the scheduler re-queues it for the next
poke. A deferrable sensor does not wait on the worker at all: it hands the
wait to the triggerer.

The word **trigger** has more than one meaning, so pin it down before the
third option can make sense. You *trigger* a Dag (§7): a verb. The triggerer process from §3
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

Read the wiring: `enough_rows` has no upstream task, so it starts at once,
alongside `settle`, and `proceed` waits for both `wait_for_curated_data` and
`enough_rows`.

One more option removes the wait altogether: **event-driven scheduling**. It
does not go in the file, because it needs a queue you do not have. Attach a
watcher to an asset and an external event starts the run; nothing polls,
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
is measured from the run's `data_interval_end`, which `airflow dags test` fixed
before the task started, so by the time the task ran the moment had already
passed.
Nothing in this run touched the triggerer.

**To watch a task actually defer**, send the run through the scheduler instead,
then read the task states a few times over the next ten seconds:

```bash
docker compose exec airflow-scheduler airflow dags unpause sensors_and_events
docker compose exec airflow-scheduler airflow dags trigger sensors_and_events -r defer-demo   # pick a new id if you rerun
docker compose exec airflow-scheduler airflow tasks states-for-dag-run sensors_and_events defer-demo -o plain
```

**Checkpoint.** `settle` reads `deferred` for about five seconds — its process
has exited and the triggerer is holding the timer — then `success`, with an
`end_date` just over five seconds after its `start_date`. That `deferred` is the
state from §3, and it is what "frees the worker slot" looks like from outside.
Be quick with the last command; a `docker compose exec` takes a second or
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
instances **exit between pokes** instead of holding a slot, and go into
`up_for_reschedule` — the state §3 gives for a sensor between pokes. The run
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
`enough_rows`, 300 for `wait_for_curated_data`. The run fails only once both
have given up, after about five minutes.

The point is what was *not* happening during the wait: the task instance was
not running between pokes — it was in `up_for_reschedule`, as you saw — so
no worker slot was held. A poking sensor in the default mode would have occupied
a slot for the whole time doing nothing, and that is exactly why the other two
modes exist.

**Rule of thumb:** seconds → a poking sensor is fine. Minutes or hours →
deferrable, or you are paying a worker slot to sleep. An external system that can
push → an `AssetWatcher`, and no task waits at all.

Reference: [`solutions/05_sensors_and_events.py`](../../solutions/05_sensors_and_events.py).

---

← [10. The MLOps Dag](10-the-mlops-dag.md) · [Index](README.md) · [12. Continual learning: monitor, decide, act](12-continual-learning-monitor-decide-act.md) →
