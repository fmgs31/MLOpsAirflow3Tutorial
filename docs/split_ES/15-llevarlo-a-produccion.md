## 15. Llevarlo a producción

**Executors.** `LocalExecutor` es el de por defecto y sirve mientras baste una máquina. Más
allá: `CeleryExecutor` (varias máquinas worker que toman tasks de una cola),
`KubernetesExecutor` (un pod por task),
o **varios executors** configurados a la vez. También existe el **Edge
executor**, para workers fuera de tu clúster, algo que es posible gracias a la
Task Execution API (§3).

**Aislamiento.** No instales tus dependencias de ML junto a las de Airflow. Usa
`KubernetesPodOperator`, o `@task.virtualenv` / `@task.external_python` para un
aislamiento más ligero. El cómputo pesado pertenece a un clúster (Ray, Spark,
EMR, Dataproc) con Airflow disparándolo y siguiéndolo — Airflow es un
orquestador, no un motor de cómputo.

**Dag bundles.** El Dag processor lee *bundles*; el de por defecto es un
`LocalDagBundle` llamado `dags-folder`, que lee el ajuste `dags_folder`.
Cambiar a `GitDagBundle` te da versionado: cada Dag run registra el commit con el
que corrió, así que un re-run usa el mismo código.

```ini
[dag_processor]
dag_bundle_config_list = [
    {"name": "team-ml",
     "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
     "kwargs": {"git_conn_id": "git_default", "subdir": "dags", "tracking_ref": "main"}}
  ]
```

**Más maquinaria de assets.** La §8 cubrió un asset, un productor y un
consumidor. Otras cuatro piezas responden a necesidades que aparecen cuando un
pipeline crece.

*Una task que decide mientras corre qué asset produce* — por ejemplo, un export
por día. Su outlet es un `AssetAlias`, un nombre provisional, y la task le
añade el asset concreto a través de `outlet_events`:

```python
from airflow.sdk import Asset, AssetAlias

DAILY_EXPORTS = AssetAlias(name="daily_exports")

@task(outlets=[DAILY_EXPORTS])
def export(*, outlet_events: OutletEventAccessorsProtocol, ds: str) -> None:
    outlet_events[DAILY_EXPORTS].add(Asset(name=f"export_{ds}"), extra={"rows": 764})
```

*Un Dag que solo existe para producir un asset.* `@asset` construye el asset,
el Dag y su única task a partir de una función, y los tres llevan el nombre de
la función. El resultado es un asset como cualquier otro, así que otro Dag
puede poner `raw_projects` en su `schedule=`. `@asset.multi(outlets=[...])` hace
lo mismo para una función que produce varios.

```python
from airflow.sdk import asset

@asset(schedule="@daily")
def raw_projects() -> str:
    return str(data.extract())
```

*Rellenar el `extra` de un evento sin `outlet_events`.* La task hace `yield` de
un `Metadata` por asset en vez de asignar `outlet_events[…].extra`:

```python
from airflow.sdk import Metadata

@task(outlets=[LABELED_PROJECTS])
def validate_transformed(fp: str):
    df = data.load_labeled(fp)
    ...
    yield Metadata(LABELED_PROJECTS, {"rows": len(df)})
```

*Runs por partición.* `CronPartitionTimetable` se dispara como un cron, pero
además le da a cada run una **partition key**: un string que nombra la parte de
los datos que le toca al run, por defecto la fecha del run (`run_offset=-1` la
retrasa un intervalo del cron, así que el run de hoy procesa la parte de ayer).
La task la lee de `dag_run.partition_key`:

```python
from airflow.sdk import CronPartitionTimetable
from airflow.sdk.types import DagRunProtocol

@dag(schedule=CronPartitionTimetable("0 3 * * *", timezone="UTC"), ...)
def daily_partitions():
    @task
    def process(*, dag_run: DagRunProtocol) -> None:
        print(f"processing partition {dag_run.partition_key}")
```

**Intervalos de parseo.** Este stack escanea archivos cada 5 segundos para que
lo que guardas aparezca al momento. Vuelve a los valores por defecto
(`refresh_interval` 300, `min_file_process_interval` 30) en cualquier cosa con
cientos de archivos de Dag: con intervalos de 5 segundos, el Dag processor
vuelve a parsear archivos que no cambiaron y gasta CPU para nada.

**Secretos.** Nunca en el código del Dag, nunca en los `kwargs` de un bundle,
nunca en la `uri` o el `extra` de un asset — los extras de asset se guardan en
texto plano. Usa Connections, Variables o un secrets backend.

**Auth.** Cambia el Simple auth manager por FAB (§2) o por un proveedor de
identidad real antes de que nadie más pueda llegar a la UI.

**Observabilidad.** Airflow emite sus propias métricas, y esta es la parte que la
gente se salta hasta que hay un incidente. Tres capas:

- **Metrics** — `[metrics] statsd_on = True` para StatsD/Datadog, o
  `[metrics] otel_on = True` para enviar OpenTelemetry a un collector. Vigila
  `scheduler.tasks.starving`, `executor.open_slots`, `dagrun.duration.failed` y
  `dag_processing.import_errors`. Los nombres de métrica llevan `dag_id`/`task_id`
  como **tags**; `[metrics] legacy_names_on` (por defecto `True`) también exporta
  la forma antigua con el nombre como sufijo.
- **Traces** — `[traces] otel_on = True` envía spans de los bucles del scheduler
  y de los runs de task, y así encuentras *qué* task de un Dag lento se
  volvió más lenta.
- **Health** — `GET /api/v2/monitor/health` informa del estado de la metadata
  database, el scheduler, el triggerer y el Dag processor; la página Home de la UI muestra lo mismo.

Y luego el mantenimiento de siempre: `airflow db clean` programado (si no, la metadata
database crece indefinidamente), deadline alerts en los Dags que importan (§13), y
Notifiers — `SlackWebhookNotifier`, `SmtpNotifier` — conectados a
`on_failure_callback` o a un `AsyncCallback`.

---

← [14. Testear Dags](14-testear-dags.md) · [Índice](README.md) · [16. Apéndice y procedencia](16-apendice-y-procedencia.md) →
