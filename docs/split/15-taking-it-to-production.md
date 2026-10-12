## 15. Taking it to production

**Executors.** `LocalExecutor` is the default and is fine as long as one machine
is enough.
Beyond that: `CeleryExecutor` (several worker machines taking tasks from a queue), `KubernetesExecutor` (a pod per
task), or **multiple executors** configured together. There is also the **Edge
executor**, for workers outside your cluster, which the Task Execution API (§3)
makes possible.

**Isolation.** Do not install your ML dependencies next to Airflow's. Use
`KubernetesPodOperator`, or `@task.virtualenv` / `@task.external_python` for
lighter isolation. The heavy compute belongs on a cluster (Ray, Spark, EMR,
Dataproc) with Airflow triggering and tracking it — Airflow is an orchestrator,
not a compute engine.

**Dag bundles.** The Dag processor reads *bundles*; the default is one
`LocalDagBundle` named `dags-folder`, which reads the `dags_folder` setting. Switching to
`GitDagBundle` gets you versioning: each Dag run records the commit it ran with,
so a re-run uses the same code.

```ini
[dag_processor]
dag_bundle_config_list = [
    {"name": "team-ml",
     "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
     "kwargs": {"git_conn_id": "git_default", "subdir": "dags", "tracking_ref": "main"}}
  ]
```

**More asset machinery.** §8 covered one asset, one producer and one
consumer. Four more pieces answer needs that appear as a pipeline grows.

*A task that decides while it runs which asset it produces* — one export per
day, say. Its outlet is an `AssetAlias`, a placeholder name, and the task adds
the actual asset to it through `outlet_events`:

```python
from airflow.sdk import Asset, AssetAlias

DAILY_EXPORTS = AssetAlias(name="daily_exports")

@task(outlets=[DAILY_EXPORTS])
def export(*, outlet_events: OutletEventAccessorsProtocol, ds: str) -> None:
    outlet_events[DAILY_EXPORTS].add(Asset(name=f"export_{ds}"), extra={"rows": 764})
```

*A Dag that exists only to produce one asset.* `@asset` builds the asset, the
Dag and its single task from one function, and all three take the function's
name. The result is an asset like any other, so another Dag can put
`raw_projects` in its `schedule=`. `@asset.multi(outlets=[...])` does the same
for a function that produces several.

```python
from airflow.sdk import asset

@asset(schedule="@daily")
def raw_projects() -> str:
    return str(data.extract())
```

*Setting an event's `extra` without `outlet_events`.* The task `yield`s one
`Metadata` per asset instead of assigning to `outlet_events[…].extra`:

```python
from airflow.sdk import Metadata

@task(outlets=[LABELED_PROJECTS])
def validate_transformed(fp: str):
    df = data.load_labeled(fp)
    ...
    yield Metadata(LABELED_PROJECTS, {"rows": len(df)})
```

*Runs per partition.* `CronPartitionTimetable` fires like a cron, but also
gives each run a **partition key**: a string naming the slice of data the run
is for, by default the run's date (`run_offset=-1` shifts it back one cron
interval, so today's run processes yesterday's slice). The task reads it from
`dag_run.partition_key`:

```python
from airflow.sdk import CronPartitionTimetable
from airflow.sdk.types import DagRunProtocol

@dag(schedule=CronPartitionTimetable("0 3 * * *", timezone="UTC"), ...)
def daily_partitions():
    @task
    def process(*, dag_run: DagRunProtocol) -> None:
        print(f"processing partition {dag_run.partition_key}")
```

**Parsing intervals.** This stack scans for files every 5 seconds so that
what you save appears at once. Put the defaults back (`refresh_interval` 300,
`min_file_process_interval` 30) on anything with hundreds of Dag files: with
5-second intervals the Dag processor keeps re-parsing files that did not change
and burns CPU for nothing.

**Secrets.** Never in Dag code, never in bundle `kwargs`, never in an asset's
`uri` or `extra` — asset extras are stored in cleartext. Use Connections,
Variables, or a secrets backend.

**Auth.** Swap the Simple auth manager for FAB (§2) or a real identity provider
before anyone else can reach the UI.

**Observability.** Airflow emits its own metrics, and this is the part people
skip until an incident. Three layers:

- **Metrics** — `[metrics] statsd_on = True` for StatsD/Datadog, or
  `[metrics] otel_on = True` to push OpenTelemetry to a collector. Watch
  `scheduler.tasks.starving`, `executor.open_slots`, `dagrun.duration.failed`
  and `dag_processing.import_errors`. Metric names carry `dag_id`/`task_id` as
  **tags**; `[metrics] legacy_names_on` (default `True`) also exports the older
  name-suffix form.
- **Traces** — `[traces] otel_on = True` sends spans for scheduler loops and
  task runs, which is how you find *which* task in a slow Dag got slower.
- **Health** — `GET /api/v2/monitor/health` reports metadata database,
  scheduler, triggerer and Dag processor status; the UI Home page renders the same thing.

Then the ordinary hygiene: `airflow db clean` on a schedule (the metadata
database grows forever otherwise), deadline alerts on the Dags that matter (§13), and
Notifiers — `SlackWebhookNotifier`, `SmtpNotifier` — wired to
`on_failure_callback` or an `AsyncCallback`.

---

← [14. Testing Dags](14-testing-dags.md) · [Index](README.md) · [16. Appendix and provenance](16-appendix-and-provenance.md) →
