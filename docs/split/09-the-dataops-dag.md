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
a remote worker has. And `from tagifai import …` works only because the compose file puts
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
practice — implicit edges when data flows, `>>` when only the order matters.

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

If a task fails, compare with [`solutions/03_dataops.py`](../../solutions/03_dataops.py).

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
**Dry-run it first** — a backfill over the wrong date range is the easiest way
to create thirty runs by accident:

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

---

← [8. Assets: scheduling on data, not on time](08-assets-scheduling-on-data-not-on-time.md) · [Index](README.md) · [10. The MLOps Dag](10-the-mlops-dag.md) →
