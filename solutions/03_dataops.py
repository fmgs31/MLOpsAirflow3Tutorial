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

from project_tagger import config, data, validate
from project_tagger.assets import LABELED_PROJECTS, RAW_PROJECTS

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
