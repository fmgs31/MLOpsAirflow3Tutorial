# Airflow 2 → Airflow 3: what actually changes

> **Not part of the tutorial.** The tutorial in `README.md` teaches Airflow 3 on
> its own terms and never mentions Airflow 2. This file is kept for one
> situation: you inherit an Airflow 2 codebase, or you find Airflow 2 code
> online and need to know what it maps to. Nothing links here; ignore it while
> learning.

Reference for porting the [Made With ML orchestration
lesson](https://madewithml.com/courses/mlops/orchestration/) (Airflow 2.3.3) to
Airflow 3.3.1. Everything here is from the official docs, and the "verified"
notes are things this repo's stack actually exercised.

Prerequisite for an in-place upgrade: **Airflow 2.7+ and Python 3.10–3.14**
(3.3.1 requires `>=3.10, !=3.15`). Upgrade to 2.11 first, then to 3.

---

## 1. Processes and commands

| Airflow 2 | Airflow 3 | Notes |
|---|---|---|
| `airflow webserver` | `airflow api-server` | The webserver is gone. The API server serves the REST API, the React UI, **and** the Task Execution API that workers call. |
| (part of the scheduler) | `airflow dag-processor` | Now a **mandatory separate process**, even locally. Forget it and your Dags never appear. |
| `airflow db init` / `db upgrade` | `airflow db migrate` | Both old commands removed. |
| `airflow dags backfill` | `airflow backfill create` | Backfills are first-class objects with their own UI, API and reprocess policy. |
| `airflow users create` | FAB provider only | The default auth manager is now Simple auth. `airflow users` exists only when `apache-airflow-providers-fab` is installed (it ships in the official image). |
| `--subdir` / `-S` | Dag bundles | Removed from every command. |
| `airflow dags list-runs -d ID` | `airflow dags list-runs ID` | `dag_id` is positional now. *(Verified.)* |
| SequentialExecutor | LocalExecutor | Removed. LocalExecutor works with SQLite for local dev. |
| CeleryKubernetes / LocalKubernetes executors | Multiple Executor Configuration | Removed. |
| — | `airflow config update --fix` | Rewrites a 2.x `airflow.cfg` to 3.x names. |

The default executor is now `LocalExecutor`, not `SequentialExecutor`.

## 2. Imports

Dag authoring moved to the **Task SDK** (`airflow.sdk`), and the common
operators moved to `apache-airflow-providers-standard`.

| Airflow 2 | Airflow 3 |
|---|---|
| `from airflow.decorators import dag, task` | `from airflow.sdk import dag, task` |
| `from airflow.models.dag import DAG` | `from airflow.sdk import DAG` |
| `from airflow.models.baseoperator import BaseOperator` | `from airflow.sdk import BaseOperator` |
| `from airflow.sensors.base import BaseSensorOperator` | `from airflow.sdk import BaseSensorOperator` |
| `from airflow.utils.context import Context` | `from airflow.sdk import Context` |
| `from airflow.utils.trigger_rule import TriggerRule` | `from airflow.task.trigger_rule import TriggerRule` — the old path warns |
| `from airflow.models import Variable, Connection` | `from airflow.sdk import Variable, Connection` |
| `from airflow.datasets import Dataset` | `from airflow.sdk import Asset` |
| `from airflow.exceptions import AirflowFailException` | `from airflow.sdk.exceptions import AirflowFailException` |
| `from airflow.operators.bash import BashOperator` | `from airflow.providers.standard.operators.bash import BashOperator` |
| `from airflow.operators.python import PythonOperator` | `from airflow.providers.standard.operators.python import PythonOperator` |
| `from airflow.operators.empty import EmptyOperator` | `from airflow.providers.standard.operators.empty import EmptyOperator` |
| `from airflow.operators.trigger_dagrun import TriggerDagRunOperator` | `from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator` |
| `from airflow.sensors.filesystem import FileSensor` | `from airflow.providers.standard.sensors.filesystem import FileSensor` |
| `from airflow.sensors.external_task import ExternalTaskSensor` | `from airflow.providers.standard.sensors.external_task import ExternalTaskSensor` |
| `from airflow.models.dagbag import DagBag` | `from airflow.dag_processing.dagbag import DagBag` — and `include_examples` is gone *(verified)* |
| `from airflow.hooks.base import BaseHook` | `from airflow.sdk import BaseHook` — old path warns *(verified)* |
| `from airflow.sensors.smart_sensor import SmartSensorOperator` | **gone.** Smart sensors were deprecated in 2.2 and removed in 2.4; the module does not exist *(verified)*. Use deferrable operators + the triggerer |

Full contents of the standard provider (v1.18.0):

```
airflow.providers.standard.operators.bash          BashOperator
airflow.providers.standard.operators.python        PythonOperator, BranchPythonOperator,
                                                   ShortCircuitOperator, PythonVirtualenvOperator,
                                                   BranchPythonVirtualenvOperator,
                                                   ExternalPythonOperator, BranchExternalPythonOperator
airflow.providers.standard.operators.empty         EmptyOperator
airflow.providers.standard.operators.branch        BaseBranchOperator
airflow.providers.standard.operators.datetime      BranchDateTimeOperator
airflow.providers.standard.operators.weekday       BranchDayOfWeekOperator
airflow.providers.standard.operators.latest_only   LatestOnlyOperator
airflow.providers.standard.operators.trigger_dagrun TriggerDagRunOperator
airflow.providers.standard.operators.hitl          ApprovalOperator, HITLBranchOperator,
                                                   HITLEntryOperator          (human-in-the-loop, 3.1+)
airflow.providers.standard.sensors.filesystem      FileSensor
airflow.providers.standard.sensors.external_task   ExternalTaskSensor, ExternalTaskMarker
airflow.providers.standard.sensors.python          PythonSensor
airflow.providers.standard.sensors.bash            BashSensor
airflow.providers.standard.sensors.date_time       DateTimeSensor, DateTimeSensorAsync
airflow.providers.standard.sensors.time            TimeSensor, TimeSensorAsync
airflow.providers.standard.sensors.time_delta      TimeDeltaSensor, TimeDeltaSensorAsync, WaitSensor
airflow.providers.standard.hooks.filesystem        FSHook
```

## 3. Dag parameters

| Airflow 2 | Airflow 3 |
|---|---|
| `schedule_interval=timedelta(days=1)` | `schedule=timedelta(days=1)` — `schedule_interval` **removed** |
| `start_date=days_ago(2)` | `start_date=pendulum.datetime(2025, 1, 1, tz="UTC")` — `airflow.utils.dates.days_ago` removed |
| `catchup=True` by default | `catchup=False` by default (`[scheduler] catchup_by_default`) |
| `sla=timedelta(hours=1)` + `sla_miss_callback` | **removed**; use `deadline=DeadlineAlert(...)` on the Dag (3.1+) |
| `SubDagOperator` | **removed**; use `@task_group` / Assets |
| `default_view`, `orientation` | moved to UI preferences |

Timetable default also changed: `create_cron_data_intervals` now defaults to
`False`, so a cron `schedule` builds a **`CronTriggerTimetable`**, not a
`CronDataIntervalTimetable`. Practical effect: `schedule="0 3 * * *"` fires at
03:00 with `logical_date` = 03:00. In Airflow 2 the same string fired at 03:00
with `logical_date` = *yesterday* 03:00. Any Dag that used `{{ ds }}` to name a
partition will silently shift by one interval.

## 4. Task context

Removed keys: `execution_date`, `next_execution_date`, `prev_execution_date`,
`next_ds`, `next_ds_nodash`, `prev_ds`, `prev_ds_nodash`, `tomorrow_ds`,
`yesterday_ds` (and their `_nodash` forms), `conf`, `execution_date_fn`.

Still there: `ds`, `ds_nodash`, `ts`, `ts_nodash`, `ts_nodash_with_tz`,
`logical_date`, `data_interval_start`, `data_interval_end`, `dag`, `dag_run`,
`run_id`, `task`, `task_instance` / `ti`, `params`, `var.value`, `var.json`,
`conn`, `macros`, `inlets`, `outlets`, `inlet_events`, `outlet_events`,
`triggering_asset_events`, `test_mode`, `exception`. Added in 3.3:
`partition_key`, `partition_date`.

`execution_date` → `logical_date`, and note that `logical_date` is now
**nullable**: manual and asset-triggered runs have none. *(Verified: an
asset-triggered `mlops` run shows an empty `logical_date` in `dags list-runs`.)*
Use `run_after` or `dag_run.run_after` when you need "when did this actually
start".

"Nullable" understates it, and this is the sharpest edge in the whole migration:
on a run with no logical date the key is **missing from the context** and the
date-derived template variables are **undefined**. Both blow up at runtime, and
neither shows up under `airflow dags test`, which always supplies a logical date.

```python
context["logical_date"]        # KeyError on a manual/asset-triggered run
context.get("logical_date")    # None
```

```jinja
{{ ds }}                       {# UndefinedError: 'ds' is undefined #}
{{ ds | default('none') }}     {# safe #}
{{ dag_run.run_after }}        {# safe: every run has one #}
```

*(Verified: both raised on real triggers of this repo's `solutions/01` and `solutions/02`,
and both were fixed there.)* Audit every `{{ ds }}` in a Dag you are porting —
in Airflow 2 that variable always existed.

## 5. XCom

```python
# Airflow 2: searched the whole Dag run, returned the most recent push.
value = ti.xcom_pull(key="x")

# Airflow 3: pulls only from the CURRENT task unless you say otherwise.
value = ti.xcom_pull(task_ids="upstream_task", key="x")
```

Also: `enable_xcom_pickling` is gone — XCom values must be JSON-serialisable.
The original lesson enabled pickling for Great Expectations; on Airflow 3 you
return a plain dict instead (see `solutions/03_dataops.py`).

## 6. No direct metadata-DB access from tasks

Task code can no longer import Airflow models or open a DB session. This is the
change that makes remote/Edge execution possible: workers talk to the Task
Execution API over HTTP, not to Postgres.

```python
# Airflow 2 - no longer possible inside a task
from airflow.models import DagRun
from airflow.utils.session import create_session
with create_session() as session:
    runs = session.query(DagRun).filter(...).all()
```

Use the REST API (`/api/v2/...`) or the official Python client instead. Anything
you used to reach into the DB for — Variables, Connections, XComs, task states —
has an SDK or API equivalent.

## 7. Datasets → Assets

```python
# Airflow 2
from airflow.datasets import Dataset
projects = Dataset("s3://bucket/projects.csv")

@dag(schedule=[projects])
def consumer(): ...

# Airflow 3
from airflow.sdk import Asset
projects = Asset(name="labeled_projects", uri="s3://bucket/projects.csv")

@dag(schedule=[projects])
def consumer(): ...
```

New in Airflow 3 on top of the rename:

- `@asset` / `@asset.multi` decorators — define asset, Dag and task in one go.
- `AssetAlias` — outlets whose URI is only known at runtime.
- `AssetAny` / `AssetAll` — OR / AND conditions in a `schedule`.
- `AssetWatcher` + a `BaseEventTrigger` — **event-driven scheduling**: an
  external event (message queue, etc.) updates the asset and starts the run,
  with no task poking anything.
- `Metadata` / `outlet_events[asset].extra` — attach a payload (row counts,
  paths) to the asset event, readable by the consumer via
  `triggering_asset_events`.
- Asset partitions (3.2+): `CronPartitionTimetable`, `PartitionedAssetTimetable`.

## 8. Auth

Default `auth_manager` is `SimpleAuthManager`. Users are declared in config, not
in the database:

```ini
[core]
simple_auth_manager_users = "admin:admin,alice:op,bob:viewer"
```

Roles: `viewer` < `user` < `op` < `admin`. Passwords are generated into
`$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated` (editable).
`simple_auth_manager_all_admins = True` disables auth entirely — local dev only.

For the Airflow 2 experience (`airflow users create`, roles in the DB), install
`apache-airflow-providers-fab` and set
`auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager`.

## 9. Dag bundles

`[core] dags_folder` still exists, but the Dag processor now reads **bundles**.
By default Airflow registers one `LocalDagBundle` named `dags-folder` pointing at
`dags_folder`, so nothing changes until you want more:

```ini
[dag_processor]
dag_bundle_config_list = [
    {
      "name": "team-ml",
      "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
      "kwargs": {"git_conn_id": "git_default", "subdir": "dags", "tracking_ref": "main"}
    }
  ]
```

`GitDagBundle` supports versioning: each Dag run records the commit it ran with,
so a re-run uses the same code. Never put credentials in `kwargs` — use a
Connection id.

## 10. Doing the upgrade

1. Get to Airflow 2.7+ (ideally 2.11) and Python 3.10+.
2. Static-check the Dags with Ruff's Airflow rules:
   ```bash
   ruff check dags/ --select AIR301,AIR302          # breaking - must fix
   ruff check dags/ --select AIR301 --fix           # auto-fix what it can
   ruff check dags/ --select AIR311,AIR312          # recommended, non-breaking
   ```
3. `airflow config update --fix` to migrate `airflow.cfg`.
4. Back up the metadata DB, then `airflow db migrate`.
5. Replace `webserver` with `api-server` and **add a `dag-processor`** to your
   deployment.
6. Add a Dag-integrity test (see `tests/test_dag_integrity.py`) so the removed
   constructs cannot come back.

## Sources

- [Upgrading to Airflow 3](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html)
- [Task SDK API reference](https://airflow.apache.org/docs/task-sdk/stable/api.html)
- [Assets and data-aware scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html)
- [Event-driven scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/event-scheduling.html)
- [Dag runs and backfill](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html)
- [Deadline alerts](https://airflow.apache.org/docs/apache-airflow/stable/howto/deadline-alerts.html)
- [Migrating from SLA to deadline alerts](https://airflow.apache.org/docs/apache-airflow/stable/howto/sla-to-deadlines.html)
- [Dag bundles](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html)
- [Simple auth manager](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/auth-manager/simple/index.html)
- [Template variables](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html)
- [CLI reference](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html)
- [Configuration reference](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html)
