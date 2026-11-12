## 16. Llevarlo a producción

**Executors.** `LocalExecutor`, el de por defecto, sirve mientras baste una
máquina. Más allá: `CeleryExecutor` (varias máquinas worker que toman tasks de
una cola), `KubernetesExecutor` (un pod por task) o **varios Executors**
configurados a la vez. El **Edge Executor**, posible gracias a la Task
Execution API (§3), es para workers fuera de tu clúster.

Cambiar de Executor cambia dónde pueden vivir los archivos. `mlops` y
`monitoring` escriben sus snapshots en `data/snapshots/` (§10, §12), una
carpeta de la única máquina que corre todas las tasks de este stack. Con
Celery o Kubernetes la siguiente task puede correr en otra máquina, sin esa
carpeta, así que los snapshots van a un bucket de GCS o S3 al que llegan todos
los workers, con una regla de ciclo de vida que borre los antiguos (aquí nada
los borra). Borrarlos es seguro: con qué se entrenó y evaluó cada modelo en
producción queda en MLflow, en su run (`split_ids.json`, §10).

**Write, audit, publish.** En `dataops`, `dbt build` escribe
`labeled_projects` antes de que corran las comprobaciones, así que una tabla
que no las pasa ya está publicada para todo lo que la lee sin esperar al asset
(§14, día 1). **Write-audit-publish** (escribir, auditar, publicar) cierra ese
hueco: dbt construye en un dataset de staging, las suites validan la tabla
allí, y solo si pasan, una task la copia al dataset que lee todo el mundo y
emite el asset. Un copy job de BigQuery sustituye la tabla en un solo paso,
así que ningún lector ve media tabla. Las tablas sin procesar pueden saltarse
ese paso: son la zona de aterrizaje, y solo las lee dbt, después de que las
suites las hayan comprobado.

**Aislamiento.** Mantén tus dependencias de ML fuera del entorno de Airflow.
dbt (§9) y alibi-detect (§12) fijan versiones que el entorno de Airflow no
puede aceptar, así que cada uno tiene su propio virtualenv en la imagen, uno
ejecutado como un programa aparte (§14) y el otro a través de
`@task.external_python`. Eso funciona para unas pocas herramientas y un
equipo. Con muchos equipos, dale a cada task su propia imagen de contenedor
con `KubernetesPodOperator` (o `DockerOperator`): la imagen de Airflow solo
contiene Airflow, y cada equipo actualiza sus librerías sin tocar las de nadie
más. El cómputo pesado pertenece a un clúster (Ray, Spark, EMR, Dataproc) que
Airflow dispara y sigue (§3).

**Servir el modelo.** Aquí nada fuera de MLflow carga el modelo. En producción
algo lo hace, como un Dag que vuelve a etiquetar todos los proyectos o uno que
avisa al servicio de inferencia para que lo recargue. Ese Dag se programa sobre
`production_model` y corre tras cada promoción o rollback, con la nueva versión
en el `extra` del evento:

```python
@dag(schedule=PRODUCTION_MODEL)
def score_projects():
    @task
    def score(*, triggering_asset_events: Mapping[Asset, Sequence[Any]]) -> None:
        version = triggering_asset_events[PRODUCTION_MODEL][-1].extra["version"]
        model = registry.load_model(version=version)
        ...
```

**Dag bundles.** El Dag processor lee *bundles*; el de por defecto es un
`LocalDagBundle` llamado `dags-folder`, que lee el ajuste `dags_folder`.
`GitDagBundle` añade versionado: cada Dag run registra el commit con el que
corrió, así que un re-run usa el mismo código.

```ini
[dag_processor]
dag_bundle_config_list = [
    {"name": "team-ml",
     "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
     "kwargs": {"git_conn_id": "git_default", "subdir": "dags", "tracking_ref": "main"}}
  ]
```

**Más maquinaria de assets.** La §8 cubrió un asset, un productor y un
consumidor. Otras tres piezas cubren necesidades que aparecen cuando un
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
el Dag y su única task a partir de una función, y los tres llevan su nombre.
El resultado es un asset normal: otro Dag puede poner `project_counts` en su
`schedule=`. `@asset.multi(outlets=[...])` hace lo mismo para una función que
produce varios.

```python
from airflow.sdk import asset

@asset(schedule="@daily")
def project_counts() -> dict:
    return {"projects": data.count_rows("projects")}
```

*Runs por partición.* `CronPartitionTimetable` se dispara como un cron y
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
lo que guardas aparezca al momento. Con cientos de archivos de Dag, vuelve a
los valores por defecto (`refresh_interval` 300, `min_file_process_interval`
30): a 5 segundos, el Dag processor vuelve a parsear archivos que no cambiaron
y gasta CPU para nada.

**Secretos.** Nunca en el código del Dag, en los `kwargs` de un bundle ni en
la `uri` o el `extra` de un asset (los extras de asset se guardan en texto
plano). Usa Connections, Variables o un secrets backend (HashiCorp Vault, AWS
Secrets Manager, Google Secret Manager), del que Airflow las lee sin guardarlas
en la Metadata database. Define `AIRFLOW_FERNET_KEY` en `.env` (`env.example`
explica cómo generar una) para cifrar lo que sí se guarde allí. Dos cosas que
hace este stack y uno de producción no debería: la clave de la service account
de GCP es un archivo montado en todos los contenedores, cuando una plataforma
como GKE les daría a los workers una identidad propia (Workload Identity), sin
clave que se pueda filtrar; y las credenciales de Airbyte se teclean en un
comando de la CLI, cuando las guardaría un secrets backend.

**Auth.** Cambia el Simple auth manager por FAB (§2) o por un proveedor de
identidad real antes de que nadie más pueda llegar a la UI.

**Observabilidad.** Tres capas:

- **Metrics** — `[metrics] statsd_on = True` para StatsD/Datadog, o
  `[metrics] otel_on = True` para enviar OpenTelemetry a un collector. Vigila
  `scheduler.tasks.starving`, `executor.open_slots`, `dagrun.duration.failed` y
  `dag_processing.import_errors`.
- **Traces** — `[traces] otel_on = True` envía spans de los bucles del Scheduler
  y de los runs de task, para encontrar *qué* task de un Dag lento se volvió
  más lenta.
- **Health** — `GET /api/v2/monitor/health` informa del estado de la Metadata
  database, el Scheduler, el Triggerer y el Dag processor; la página Home de la
  UI muestra lo mismo.

Y luego el mantenimiento de siempre: `airflow db clean` programado (§3), deadline alerts en los Dags que
importan (§13), y Notifiers — `SlackWebhookNotifier`, `SmtpNotifier` — conectados a
`on_failure_callback` o a un `AsyncCallback`.

**Cuando termines el tutorial.** Tres cosas siguen existiendo después: el
stack de Airflow (`scripts/reset.sh`, §2), Airbyte (`abctl local uninstall`) y
el proyecto de Google Cloud. Para eliminar el proyecto con todo lo que
contiene, incluidas la service account y su clave, abre **IAM & Admin →
Settings** en la consola y elige **Shut down**; Google lo borra a los 30 días.

---

← [15. Testear Dags](15-testear-dags.md) · [Índice](README.md) · [17. Apéndice y procedencia](17-apendice-y-procedencia.md) →
