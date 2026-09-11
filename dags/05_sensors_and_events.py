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
