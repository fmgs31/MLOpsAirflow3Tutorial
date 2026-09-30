## 15. Taking it to production

**Executors.** `LocalExecutor` is the default and is fine up to one machine.
Beyond that: `CeleryExecutor` (worker pool), `KubernetesExecutor` (a pod per
task), or **multiple executors** configured together. There is also the **Edge
executor**, for workers outside your cluster — the thing the Task Execution API
(§3) made possible.

**Isolation.** Do not install your ML dependencies next to Airflow's. Use
`KubernetesPodOperator`, or `@task.virtualenv` / `@task.external_python` for
lighter isolation. The heavy compute belongs on a cluster (Ray, Spark, EMR,
Dataproc) with Airflow triggering and tracking it — Airflow is an orchestrator,
not a compute engine.

**Dag bundles.** `dags_folder` still works, but the Dag processor reads
*bundles*; the default is one `LocalDagBundle` named `dags-folder`. Switching to
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

**Parsing intervals.** This stack scans for files every 5 seconds so that
what you save appears at once. Put the defaults back (`refresh_interval` 300,
`min_file_process_interval` 30) on anything with hundreds of Dag files — the
processor's job is to parse, not to poll.

**Secrets.** Never in Dag code, never in bundle `kwargs`, never in an asset's
`uri` or `extra` — asset extras are stored in cleartext. Use Connections,
Variables, or a secrets backend.

**Auth.** Swap the Simple auth manager for FAB or a real identity provider
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
- **Health** — `GET /api/v2/monitor/health` reports scheduler, triggerer and Dag
  processor status; the UI Home page renders the same thing.

Then the ordinary hygiene: `airflow db clean` on a schedule (the metadata DB
grows forever otherwise), deadline alerts on the Dags that matter (§13), and
Notifiers — `SlackWebhookNotifier`, `SmtpNotifier` — wired to
`on_failure_callback` or an `AsyncCallback`.

---

← [14. Testing Dags](14-testing-dags.md) · [Index](README.md) · [16. Appendix and provenance](16-appendix-and-provenance.md) →
