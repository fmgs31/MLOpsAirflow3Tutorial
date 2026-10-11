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
`project_tagger/callbacks.py` fires and you can find it in the scheduler/executor logs.

    docker compose logs -f airflow-scheduler | grep "DEADLINE MISSED"

AIRFLOW 3.3.1 BUG (the version this repo installs): `airflow dags test <dag_id>`
crashes on any Dag that sets `deadline=`, with
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
            "project_tagger.callbacks.deadline_missed",
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
