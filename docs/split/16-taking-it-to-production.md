## 16. Taking it to production

**Executors.** `LocalExecutor`, the default, is fine while one machine is
enough. Beyond that: `CeleryExecutor` (several worker machines taking tasks
from a queue), `KubernetesExecutor` (a pod per task), or **multiple Executors**
configured together. The **Edge Executor**, which the Task Execution API (§3)
makes possible, is for workers outside your cluster.

Changing Executor changes where files can live. `mlops` and `monitoring` write
their snapshots to `data/snapshots/` (§10, §12), a folder on the one machine
that runs every task in this stack. With Celery or Kubernetes the next task
can run on a machine without that folder, so the snapshots go to object
storage, a GCS or S3 bucket every worker can reach, with a lifecycle rule that
deletes them after a set time (retention); nothing in this stack deletes them.
Nothing that has to last depends on them: what each production model was
trained and tested on is in MLflow, with its run (`split_ids.json`, §10).

**Write, audit, publish.** In `dataops`, `dbt build` writes
`labeled_projects` before the checks run on it, so a table that fails them is
already published to anything that reads it without waiting for the asset
(§14, day 1). **Write-audit-publish** closes the gap: dbt builds into a
staging dataset, the suites validate the table there, and only if they pass
does a task copy it to the dataset everyone reads and emit the asset. A
BigQuery copy job replaces the table in one step, so no reader sees half of
it. The raw tables can skip this step: they are the landing zone, read only by
dbt after the suites have checked them.

**Isolation.** Keep your ML dependencies out of Airflow's environment. dbt
(§9) and alibi-detect (§12) pin versions Airflow's environment cannot accept,
so each has its own virtualenv in the image, one called from a
`BashOperator`, the other through `@task.external_python`. That works for a
few tools and one team. With many teams, give each task its own container
image with `KubernetesPodOperator` (or `DockerOperator`): Airflow's image then
holds only Airflow, and each team upgrades its libraries without touching
anyone else's. Heavy compute belongs on a cluster (Ray, Spark, EMR, Dataproc)
that Airflow triggers and tracks: Airflow is an orchestrator, not a compute
engine.

**Dag bundles.** The Dag processor reads *bundles*; the default is one
`LocalDagBundle` named `dags-folder`, which reads the `dags_folder` setting.
`GitDagBundle` adds versioning: each Dag run records the commit it ran with,
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
consumer. Four more pieces cover needs that appear as a pipeline grows.

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
Dag and its single task from one function, all three named after it. The
result is an ordinary asset: another Dag can put `project_counts` in its
`schedule=`. `@asset.multi(outlets=[...])` does the same for a function that
produces several.

```python
from airflow.sdk import asset

@asset(schedule="@daily")
def project_counts() -> dict:
    return {"projects": data.count_rows("projects")}
```

*Setting an event's `extra` without `outlet_events`.* The task `yield`s one
`Metadata` per asset instead of assigning to `outlet_events[…].extra`:

```python
from airflow.sdk import Metadata

@task(outlets=[LABELED_PROJECTS])
def validate_transformed():
    df = data.load_labeled()
    ...
    yield Metadata(LABELED_PROJECTS, {"rows": len(df)})
```

*Runs per partition.* `CronPartitionTimetable` fires like a cron and also
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

**Parsing intervals.** This stack scans for files every 5 seconds so what you
save appears at once. With hundreds of Dag files, put the defaults back
(`refresh_interval` 300, `min_file_process_interval` 30): at 5 seconds the Dag
processor keeps re-parsing unchanged files and burns CPU for nothing.

**Secrets.** Never in Dag code, bundle `kwargs`, or an asset's `uri` or
`extra` (asset extras are stored in cleartext). Use Connections, Variables, or
a secrets backend (HashiCorp Vault, AWS Secrets Manager, Google Secret
Manager) that Airflow reads Connections and Variables from, so they never sit
in the Metadata database. Set `AIRFLOW_FERNET_KEY` in `.env` (`env.example`
shows how to generate one) to encrypt whatever is in the Metadata database.
Two things this stack does that a production one should not: the GCP
service-account key is a file mounted into every container, where a platform
such as GKE would give the workers their own identity (Workload Identity),
with no key to leak; and the Airbyte credentials are typed into a CLI command,
where a secrets backend would hold them.

**Auth.** Swap the Simple auth manager for FAB (§2) or a real identity provider
before anyone else can reach the UI.

**Observability.** The part people skip until an incident. Three layers:

- **Metrics** — `[metrics] statsd_on = True` for StatsD/Datadog, or
  `[metrics] otel_on = True` to push OpenTelemetry to a collector. Watch
  `scheduler.tasks.starving`, `executor.open_slots`, `dagrun.duration.failed`
  and `dag_processing.import_errors`. Metric names carry `dag_id`/`task_id` as
  **tags**; `[metrics] legacy_names_on` (default `True`) also exports the older
  name-suffix form.
- **Traces** — `[traces] otel_on = True` sends spans for Scheduler loops and
  task runs, so you can find *which* task in a slow Dag got slower.
- **Health** — `GET /api/v2/monitor/health` reports Metadata database,
  Scheduler, Triggerer and Dag processor status; the UI Home page shows the
  same.

Then the ordinary hygiene: `airflow db clean` on a schedule (otherwise the
Metadata database grows forever), deadline alerts on the Dags that matter
(§13), and Notifiers — `SlackWebhookNotifier`, `SmtpNotifier` — wired to
`on_failure_callback` or an `AsyncCallback`.

**When you are done with the tutorial.** Three things outlive it: the Airflow
stack (`scripts/reset.sh`, §2), Airbyte (`abctl local uninstall`), and the
Google Cloud project. To remove the project with everything in it, service
account and key included, open **IAM & Admin → Settings** in the console and
choose **Shut down**; Google deletes it after 30 days.

---

← [15. Testing Dags](15-testing-dags.md) · [Index](README.md) · [17. Appendix and provenance](17-appendix-and-provenance.md) →
