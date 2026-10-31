"""Lesson 5 - waiting for things: sensors, deferrable operators, and events.

Three ways to react to the outside world, cheapest last:

1. **Poking sensors** - a worker slot is held while polling. Fine for seconds.
2. **`reschedule` mode** - the task instance exits between pokes and is
   re-queued, so no slot is held while it waits.
3. **Deferrable sensors** (`deferrable=True`) - the task suspends, the slot is
   released, and the triggerer resumes it. This is what you want for anything
   measured in minutes or hours.

And one way not to wait at all: **event-driven scheduling** (`AssetWatcher`),
where an external event emits an asset update and *that* starts the Dag run.
See the note at the bottom.

Which one a wait can use depends first on the check: deferrable needs a
trigger written for it, while `poke` and `reschedule` run any Python. This Dag
chains one of each: the provider's trigger waits for the table to exist, then
a check of our own waits for it to have enough rows.

The table to wait for is a Dag param, `table`, so a run can be pointed at one
that does not exist yet:

Run it:
    airflow dags test sensors_and_events       # labeled_projects already exists
    airflow dags trigger sensors_and_events -c '{"table": "labeled_projects_copy"}'
                                               # waits until you create that table
"""

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
    # Deferrable: the Google provider ships a trigger for this check, so the
    # triggerer asks BigQuery whether the table exists, through the
    # `google_cloud_default` connection, every `poke_interval`, and no worker
    # slot is held while it waits. Needs `airflow triggerer`.
    wait_for_table = BigQueryTableExistenceSensor(
        task_id="wait_for_table",
        project_id=config.GCP_PROJECT_ID,
        dataset_id=config.BQ_DATASET,
        table_id="{{ params.table }}",
        deferrable=True,
        poke_interval=10,
        timeout=60 * 5,
    )

    @task.sensor(poke_interval=10, timeout=60 * 5, mode="reschedule")
    def enough_rows(*, params: dict[str, Any]) -> PokeReturnValue:
        """A sensor written as a function: a check of our own, with no trigger,
        so `reschedule` mode - the process exits between pokes.

        Return `PokeReturnValue(is_done=..., xcom_value=...)` to both stop poking
        and hand a value to downstream tasks.
        """
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
# a trigger that tests a permanent condition ("table exists") would re-fire
# forever.
