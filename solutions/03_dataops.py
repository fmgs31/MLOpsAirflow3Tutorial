"""Lesson 3 - the DataOps Dag: extract & load -> validate -> transform -> validate.

The first Dag that does real work, and none of it happens inside Airflow:
Airflow tells each tool when to run and checks the result.

    extract_and_load_*   Airbyte copies each CSV into a BigQuery table
    validate_*           Great Expectations suites gate each raw table
    transform            dbt builds `labeled_projects` (a join) and tests it
    validate_transformed a final suite; only a pass emits the asset event

Three things to watch for:

* The Dag **produces Assets** (`outlets=[...]`). Finishing the producing task
  emits an asset event, and that event is what schedules the MLOps Dag - no
  cron guessing, no sensor, no polling.
* The Airbyte connection ids are **Variables**, read through a template at run
  time, so the Dag file names no id that only exists in your Airbyte.
* Validation results move through XCom as small dicts; the tables never do.
  Tasks exchange names and counts, not payloads.

Run it:
    airflow dags test dataops
"""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task, task_group

# Task code imports exceptions from the Task SDK. It is the namespace a remote
# worker has, and the one to reach for in a task.
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk.types import OutletEventAccessorsProtocol

from project_tagger import config, data, validate
from project_tagger_orchestration.assets import LABELED_PROJECTS, RAW_PROJECTS, RAW_TAGS

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
    # Extract and load: Airbyte does the copying, Airflow only starts it and
    # waits. `deferrable=True` hands the waiting to the triggerer, so no worker
    # slot is held while the sync runs.
    extract_and_load_projects = AirbyteTriggerSyncOperator(
        task_id="extract_and_load_projects",
        airbyte_conn_id="my_airbyte_conn",
        connection_id="{{ var.value.airbyte_projects_connection_id }}",
        deferrable=True,
        timeout=3600,
        outlets=[RAW_PROJECTS],
    )
    extract_and_load_tags = AirbyteTriggerSyncOperator(
        task_id="extract_and_load_tags",
        airbyte_conn_id="my_airbyte_conn",
        connection_id="{{ var.value.airbyte_tags_connection_id }}",
        deferrable=True,
        timeout=3600,
        outlets=[RAW_TAGS],
    )

    @task
    def validate_projects() -> dict[str, Any]:
        """Quality gate on the raw `projects` table."""
        result = validate.run_suite(data.load_projects(), "projects")
        print(result.summary())
        if not result.passed:
            # AirflowFailException stops retries: a data-quality break will not
            # fix itself by running the same task again.
            raise AirflowFailException(f"projects failed validation: {result.failures}")
        return {"suite": result.suite, "results": result.results}

    @task
    def validate_tags() -> dict[str, Any]:
        """Quality gate on the raw `tags` table."""
        result = validate.run_suite(data.load_tags(), "tags")
        print(result.summary())
        if not result.passed:
            raise AirflowFailException(f"tags failed validation: {result.failures}")
        return {"suite": result.suite, "results": result.results}

    # Transform: dbt runs in its own virtualenv, so a BashOperator calls its
    # executable. `dbt build` builds every model and then runs its tests; a
    # failing test exits non-zero, and that fails the task.
    transform = BashOperator(
        task_id="transform",
        bash_command=(
            f"{config.DBT_BIN} build"
            f" --project-dir {config.DBT_PROJECT_DIR}"
            f" --profiles-dir {config.DBT_PROJECT_DIR}"
        ),
    )

    @task(outlets=[LABELED_PROJECTS])
    def validate_transformed(*, outlet_events: OutletEventAccessorsProtocol) -> dict[str, Any]:
        """Final gate. Only a passing run emits the asset event.

        Placing the outlet on the *validation* task, not the transform task, is
        the whole trick: downstream Dags are then scheduled on
        "good data arrived", never on "a task finished".
        """
        df = data.load_labeled()
        result = validate.run_suite(df, "labeled_projects")
        print(result.summary())
        if not result.passed:
            raise AirflowFailException(f"labeled_projects failed validation: {result.failures}")

        outlet_events[LABELED_PROJECTS].extra = {
            "rows": len(df),
            "tags": sorted(df["tag"].unique().tolist()),
            "warnings": [r["expectation"] for r in result.failures],
        }
        return {"suite": result.suite, "rows": len(df), "results": result.results}

    @task_group(group_id="quality_report")
    def quality_report(reports: list[dict[str, Any]]):
        """A task group nests tasks under one collapsible node in the UI."""

        @task
        def summarize(reports: list[dict[str, Any]]) -> None:
            for report in reports:
                failed = [r["expectation"] for r in report["results"] if not r["passed"]]
                print(f"{report['suite']}: {len(report['results'])} checks, failed={failed or 'none'}")

        summarize(reports)

    projects_report = validate_projects()
    tags_report = validate_tags()
    extract_and_load_projects >> projects_report
    extract_and_load_tags >> tags_report

    # Fan-in: dbt joins the two tables, so it waits for both gates.
    curated_report = validate_transformed()
    [projects_report, tags_report] >> transform >> curated_report

    quality_report([projects_report, tags_report, curated_report])


dataops()
