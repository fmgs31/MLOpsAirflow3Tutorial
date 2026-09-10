# Airflow 3 CLI cheatsheet

In Docker, prefix with `docker compose exec airflow-scheduler ` (or
`docker compose run --rm airflow-cli ` for a throwaway container).

## Processes

```bash
airflow standalone              # everything at once, for local dev
airflow api-server --port 8080  # REST API + UI + Task Execution API (was: webserver)
airflow scheduler
airflow dag-processor           # mandatory in Airflow 3
airflow triggerer               # deferrable tasks, async deadline callbacks
```

## Database

```bash
airflow db migrate              # create or upgrade  (was: db init / db upgrade)
airflow db check
airflow db clean --clean-before-timestamp 2026-01-01
airflow db reset -y             # destructive
```

## Dags

```bash
airflow dags list
airflow dags list-import-errors
airflow dags details DAG_ID
airflow dags show DAG_ID
airflow dags show-dependencies          # cross-Dag / asset graph
airflow dags reserialize
airflow dags pause DAG_ID
airflow dags unpause DAG_ID
airflow dags trigger DAG_ID -c '{"key": "value"}' -r MY_RUN_ID
                                        # a PAUSED Dag's triggered run stays `queued`
                                        # no -l/--logical-date => the run has none
airflow dags list-runs DAG_ID -o plain  # dag_id is POSITIONAL in Airflow 3
airflow dags list-runs DAG_ID --state queued -o plain
airflow dags state DAG_ID RUN_ID
airflow dags next-execution DAG_ID
airflow dags test DAG_ID                # run one DagRun in-process, no scheduler
airflow dags delete DAG_ID
```

## Tasks

```bash
airflow tasks list DAG_ID
airflow tasks test DAG_ID TASK_ID       # run one task, no DB state written
airflow tasks states-for-dag-run DAG_ID RUN_ID
airflow tasks clear DAG_ID --task-regex 'train.*' --start-date 2026-01-01
```

## Backfill (replaces `airflow dags backfill`)

```bash
airflow backfill create \
    --dag-id mlops \
    --from-date 2026-01-01 \
    --to-date 2026-01-31 \
    --reprocess-behavior failed \
    --max-active-runs 3 \
    --run-backwards \
    --dag-run-conf '{"reason": "reprocess"}'
```

`--reprocess-behavior`: `none` (skip existing runs), `failed`, `completed`.
`create` is the only `airflow backfill` subcommand — list and manage backfills
in the UI or through `/api/v2/backfills`.

## Assets

```bash
airflow assets list
airflow assets details --name labeled_projects
airflow assets materialize --name labeled_projects   # run the Dag that produces it
```

## Connections, variables, pools

```bash
airflow connections add fs_default --conn-type fs --conn-extra '{"path": "/"}'
airflow connections list
airflow variables set min_f1 0.7
airflow pools set training 4 "training slots"
```

Env-var form, no database needed:

```bash
export AIRFLOW_CONN_FS_DEFAULT='{"conn_type": "fs", "extra": {"path": "/"}}'
export AIRFLOW_VAR_MIN_F1=0.7
```

## Config and diagnostics

```bash
airflow config list
airflow config get-value core executor
airflow config update --fix     # migrate a 2.x airflow.cfg to 3.x
airflow info
airflow version
airflow providers list
airflow cheat-sheet
```

## Config via environment

`AIRFLOW__<SECTION>__<KEY>`, e.g.:

```bash
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__AUTH_MANAGER=airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager
AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-apiserver:8080/execution/
AIRFLOW__API__PORT=8080
AIRFLOW__API_AUTH__JWT_SECRET=...
AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT=False
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://...
```

## REST API (v2)

```bash
curl -s localhost:8080/api/v2/monitor/health | jq
```

Everything the UI does is on `/api/v2/...`; the interactive docs are at
`http://localhost:8080/docs`.

## Dag authoring quick reference

```python
from airflow.sdk import (
    DAG, dag, task, task_group, asset, setup, teardown,
    Asset, AssetAlias, AssetAll, AssetAny, AssetWatcher,
    BaseOperator, BaseSensorOperator, PokeReturnValue,
    Connection, Variable, Param, Metadata, Context, ObjectStoragePath,
    TaskGroup, Label, EdgeModifier, XComArg, TriggerRule, WeightRule,
    DeadlineAlert, DeadlineReference, AsyncCallback, SyncCallback,
    chain, chain_linear, cross_downstream, get_current_context, literal, macros,
    CronTriggerTimetable, CronDataIntervalTimetable, MultipleCronTriggerTimetable,
    DeltaTriggerTimetable, EventsTimetable, AssetOrTimeSchedule,
)
```

Task decorator flavours (from the standard provider):
`@task`, `@task.bash`, `@task.branch`, `@task.short_circuit`, `@task.sensor`,
`@task.virtualenv`, `@task.external_python`, `@task.branch_virtualenv`,
`@task.branch_external_python`.
