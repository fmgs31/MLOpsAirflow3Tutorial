# Qué se verificó de verdad

> Parte del [tutorial de Airflow 3](../README_ES.md), sacado de él para que el
> tutorial siga siendo legible. `§N` se refiere a una sección numerada de ese
> README.

El cuerpo del tutorial dice lo que vas a ver. Este documento dice cómo se
sabe: cada cifra y afirmación de allí se leyó de un stack vivo, y esto es el
registro de cuándo, y de cómo se midió — salvo lo que la primera sección de
abajo lista como pendiente. Nada de esto es un paso que ejecutar.
Sáltatelo, o vuelve cuando quieras saber hasta qué punto fiarte de un número.

## 2026-10-21: la aprobación pasa a la promoción

`ApprovalOperator` salió de `monitoring` (`request_retraining`) y pasó a
`mlops` (`approve_promotion`, entre `promotion_gate` y `promote_model`); el
`decide` de la §12 ahora va directo a `trigger_retraining`. No se recorrió en
vivo. Los dos Dags de las soluciones se construyeron en el contenedor del
Scheduler: `mlops` tiene 14 tasks, `promotion_gate → approve_promotion,
keep_current_model`, `approve_promotion → promote_model`; `monitoring` tiene
8, `decide → continue_serving, inspect, rollback, trigger_retraining`. El
comportamiento de Reject de `ApprovalOperator` (salta la task directamente
aguas abajo salvo con `ignore_downstream_trigger_rules=True`) se leyó de
`apache-airflow-providers-standard` 1.17.0 en la imagen.

## 2026-10-21: fuera el guardarraíl de las filas viejas

`promotion_gate` ya no tiene el guardarraíl por bootstrap sobre las filas de
test viejas: deciden las filas nuevas, o el split entero si hay menos de 30.
`compare_with_champion` sigue escribiendo `all`, `new` y `old`, sin intervalo.
No se volvió a recorrer en vivo. `pytest` sobre
`tests/test_project_tagger.py`: `19 passed, 1 skipped`; la cifra de la suite
completa en la §15 (`45 passed, 1 skipped`) es la del 2026-10-17 menos el test
de bootstrap eliminado. Con las cifras del 2026-10-17 de abajo, todo run
promociona en la §12: el rango de f1 del modelo nuevo en las filas nuevas
(0.915–0.957) queda por encima del del champion (0.828–0.847), y el único
rechazo offline vino del guardarraíl.

## 2026-10-17, más tarde: la entrega de arXiv y el promotion gate nuevo

La entrega de la §12 son ahora 800 papers de arXiv (`datasets/batch_1/`), la
§9 carga los 764 proyectos de Made With ML, el lote de la §14 va encima de
los dos, y `promotion_gate` decide con las filas de test que el champion nunca
vio, con un guardarraíl por bootstrap sobre las viejas. Recorrido desde un
stack reiniciado (`docker compose down --volumes`, `apache/airflow:3.3.1`,
BigQuery, Airbyte con sus sources reapuntadas por la API pública), con los
Dags de solución en `dags/`:

- `pytest`: `46 passed, 1 skipped`.
- §9: `projects`, `tags`, `labeled_projects` con 764 filas, suites `6/6`,
  `5/5`, `6/6`.
- §10: la versión 1 se promocionó con `no champion yet` (f1 0,8788). El run a
  mano: `all test rows: 157` y `old test rows: 157`, ninguna fila nueva, `…
  clears threshold 0.65 and the champion's 0.8788 on the same 157 test rows -
  promoting` (0,8911).
- §12: `labeled_projects` con 1564 filas, ningún run de `mlops` en pausa.
  `num_current': 800`; `f1 at promotion=0.8911 now=0.8224`; `label drift
  p=1.082e-22 length drift p=0`; referencia `computer-vision` 38 %, `mlops`
  8 %; actual `computer-vision` 57,5 %, sin `mlops`; `request_retraining`,
  84 s. Con `drift_p_val` 0: `continue_serving` en 18 s. El reentrenamiento:
  split de test de 319 filas; nuevas 162: 0,9566 frente a 0,8190 (+0,0787 a
  +0,1999); viejas 157: 0,8782 frente a 0,8911 (−0,0575 a +0,0241);
  promocionado. Después, `only 0 new rows since version 3 was trained -
  nothing to measure`.
- Fuera del stack, la misma §10 → §12 con 20 búsquedas sin semilla:
  promocionó 19 de 20 (el rechazo, del guardarraíl de las filas viejas,
  −0,095 a −0,012); caída del f1 sobre los papers 0,049–0,099; f1 en las filas
  nuevas 0,915–0,957 frente a 0,828–0,847.
- §14 día 1: `FAIL 191 not_null_labeled_projects_tag`, tres intentos; las 191
  filas sin tag con fechas del 2026-09-11 al 2026-10-15; `tags` 1564. Día 2:
  `validate_projects` falla en `title,description`, los cuatro pares con ids
  3800–3803 a `2026-10-16 09:00:00`; la primera fila de ejemplo cambia entre
  runs (`HuggingTweets` aquí). Día 3: `Invalid timestamp: '09/11/2026
  17:19:07'`, `ERROR=1 SKIP=5`; tras el arreglo, `PASS=7`, 1755 filas. La
  alarma de drift: `num_current': 191`, label drift p=5.817e-41, length drift
  p=5.022e-19, `new_labels=['other']`, `inspect`; la query por tag tal como
  aparece. Tras el arreglo del vocabulario: label drift p=0.005921, length
  igual, `f1 at promotion=0.9171 now=0.9252`, `request_retraining`, 91 s. Con
  `other` en `KNOWN_RAW_TAGS`: `tags 5/5`, `labeled_projects 6/6`, sin aviso.
  El run de `mlops` en cola: 368 filas de test, 49 nuevas; `below the
  champion's 0.9345 on the 49 new test rows - not promoting`.

## 2026-10-17: snapshots en Parquet, el split por hash, las ventanas de monitorización, el rollback

Desde un stack reiniciado (`docker compose down --volumes`,
`apache/airflow:3.3.1`, BigQuery, Airbyte), con los Dags de solución copiados
en `dags/`:

- `pytest`: `44 passed, 1 skipped`. El skip es el test de drift; pyarrow
  25.0.0 lee y escribe Parquet en el virtualenv de alibi-detect (numpy 1.26.4,
  pandas 2.3.3).
- §9 con `datasets/initial/`: `projects`, `tags` y `labeled_projects` con 573
  filas cada una, suites `6/6`, `5/5`, `6/6`. §11: `Data is ready (573 rows)`.
- §10: `training_split passed=5/5`; splits de 373 / 87 / 113 filas (train /
  val / test). La versión 1 se promocionó con `no champion yet`. El
  `test.parquet` del segundo run tenía los mismos 113 ids, y `score_champion`
  puntuó la versión 1 sobre él exactamente con el f1 que registró al
  entrenarse; la versión 2 puntuó menos y no se promocionó. Airbyte carga `id`
  como NUMERIC, que llega como `Decimal`, y por eso `data.load_labeled` lo
  convierte en entero.
- Un asset event emitido con `mlops` en pausa no arrancó ningún run; el
  siguiente run disparado por asset lo listó entre sus eventos.
- §12: tras la entrega completa, `num_current=191`, referencia de 457 filas;
  label drift p=0.0001645, length drift p=0.8746; `mlops` 5,7 % → 15,7 %,
  `natural-language-processing` 44 % → 30 %; caída del f1 de 0,05; run de
  84 s. Con `drift_p_val` 0.00001: `continue_serving` en 15 s. El run
  disparado quedó en `queued` hasta quitarle la pausa a `mlops`, y entonces
  promocionó la versión 4 (0,8911 frente al 0,8892 del champion sobre las
  mismas 157 filas de test). Veinte repeticiones de la búsqueda fuera del
  stack sobre ese snapshot promocionaron 18 veces. La monitorización después:
  `only 0 new rows since version 4 was trained - nothing to measure`.
- §14, después del día 3: `num_current=191`, label drift p=3,6e-21, length
  0,918, `new_labels=['other']`, `inspect`; tras el arreglo del vocabulario,
  label drift p=0,996, `continue_serving`. Los días 1–3 fallaron donde dice
  la §14.
- El rollback (`registry.rollback`, el borrado del alias, los tags de versión,
  un segundo rollback que devuelve `None`) lo cubre el test unitario; no se
  forzó en el stack vivo.

## 2026-10-15: cuatro clases, y los controles de la §14

Contra el stack real (`apache/airflow:3.3.1`, BigQuery), después de cambiar
`data.preprocess` para que agrupe en `other` todo tag fuera de
`config.ACCEPTED_TAGS` (hasta entonces un umbral de frecuencia de 30 dejaba
`reinforcement-learning` y `graph-learning` como clases propias):

- §10: `airflow dags test mlops` con los Dags de solución: 20 trials, mejor
  trial f1 0.9000, modelo final f1 0.8951, promovido, porque no había modelo en
  producción.
- §12: label drift p = 7.718e-05, length drift p = 0.9169, la rama de
  reentrenamiento, aprobada por el timeout, `mlops` disparado. Con
  `drift_p_val=0.00001`: `continue_serving`.
- §9: `dbt test` sobre la tabla de 764 filas, con el test de `description`:
  pasan 5 tests.
- §14: cada entrega cargada directamente en un dataset temporal, con los tipos
  de columna que Airbyte les da a las tablas (`id` NUMERIC, el resto STRING), y
  después las suites reales de GX y `dbt build`. Día 1:
  `FAIL 191 not_null_labeled_projects_tag`. Día 2: la suite de `projects` falla
  en `title,description`, la de `tags` avisa por `other`. Día 3:
  `Invalid timestamp: '06/30/2021 03:58:46'`, `ERROR=1 SKIP=5`; con el model
  arreglado, `PASS=7`. Todas las consultas de la §14 devuelven lo que dice la
  sección. El Dag de monitoring, ejecutado con `BQ_DATASET` apuntando al
  dataset temporal: label drift p = 1.013e-17, `new_labels=['other']`,
  `Branch into inspect`; después del mapeo de tags, p = 0.4574,
  `continue_serving`; `f1 at promotion=0.8951 now=0.9330`. Con `other` en
  `KNOWN_RAW_TAGS`, las suites de `tags` y `labeled_projects` pasan sin avisos.
- Encontrado en el camino: con más de una página de resultados,
  `to_dataframe()` pasa a la BigQuery Storage API, que necesita un rol que la
  service account no tiene (403, `bigquery.readsessions.create`).
  `data.load_table` ahora lee con la API normal.
- `pytest tests` con `DAGS_DIR=solutions`: 41 passed, 1 skipped.

Pendiente de un recorrido de la propia §14: el lote pasando por Airbyte (los
archivos tienen que estar en sus URLs públicas), la edición de la URL de una
source de Airbyte, los runs del Dag y sus tiempos, y los clics en el diálogo
Clear, cuyas etiquetas se leyeron del bundle de la UI de 3.3.1.

## Aún no verificado en un stack real: la reescritura del stack de datos (2026-10-12)

Las secciones 9 a 12 se reescribieron para usar las herramientas reales —
Airbyte, BigQuery, dbt, Great Expectations, Optuna, MLflow, alibi-detect — en
lugar de sustitutos locales. El registro del 2026-09-09 de más abajo describe la
versión anterior de esas secciones.

Comprobado sin un stack real, el 2026-10-12:

- La imagen se construye: `apache/airflow:3.3.1` (Python 3.13) más los
  providers de Airbyte y Google, GX Core 1.24, Optuna 5.0, mlflow-skinny 3.17 y
  skops, resueltos contra los constraints de 3.3.1; dbt-core 1.12.5 con
  dbt-bigquery 1.12.1 en `/opt/venvs/dbt`; alibi-detect 0.13.0 sobre Python
  3.12 en `/opt/venvs/alibi`.
- En esa imagen, `pytest tests` con `DAGS_DIR=solutions`: 41 passed, 1 skipped
  (el test de drift, que necesita alibi-detect). Eso incluye los tests de
  integridad de Dags, el montaje de todos los bloques "Constrúyelo" en los dos
  idiomas, y la comprobación de que los archivos de dbt que se teclean en la
  §9 coinciden con `solutions/dbt_transforms/`.
- Los pasos de dbt de la §9, ejecutados en esa imagen: `dbt init
  --skip-profile-setup`, el cambio del bloque `models:` y los cuatro archivos
  dan un proyecto que `dbt parse` acepta, con 1 model, 4 tests y 2 sources.
- Los tests unitarios ejecutan Optuna y MLflow de verdad, sobre SQLite: crear
  un study, añadir trials desde dos "workers", entrenar, registrar dos
  versiones, promocionar, hacer rollback, cargar el champion.
- El test de drift pasa dentro de un entorno con alibi-detect 0.13.
- Las cifras de la §12 — label drift p ≈ 7.7e-05, length drift p ≈ 0.92,
  `mlops` de ~6 % a ~16 %, NLP del 44 % al 30 % — salen de ejecutar
  `drift.detect_drift` sobre `datasets/projects.csv` unido a `tags.csv`.
- `datasets/tags.csv` coincide con el `dataset.csv` original en las 764 filas
  una vez agrupados los tres tags más raros.

Pendiente del primer recorrido real:

- Los pasos de Google Cloud y Airbyte de la §9, incluidas todas las etiquetas
  de la consola y de la UI que se citan, los comandos de `abctl`, y que la
  Connection de Airbyte llegue a `host.docker.internal:8000` con el par
  `client-id` / `client-secret`.
- La afirmación de que el sandbox de BigQuery no admite las escrituras de
  Airbyte, que se apoya en la descripción del sandbox de Google y en informes
  de la comunidad de Airbyte.
- Las URLs de origen, que necesitan que este repo sea público.
- Todas las comprobaciones de la §9 a la §12: recuentos de las suites, el
  `PASS=6` de dbt, las tasks deferrable de Airbyte bajo `airflow dags test`,
  los runs de MLflow (el del study con sus veinte hijos, más el final), la demo del sensor con `labeled_projects_copy`,
  `@task.external_python` en el Dag de monitorización.
- `abctl local uninstall` (§16).

## 2026-09-09

Contra un stack vivo de `apache/airflow:3.3.1` (Postgres 16, LocalExecutor,
api-server + scheduler + dag-processor + triggerer), el 2026-09-09:

- Los 7 Dags parsean con **cero errores de import** (`airflow dags list-import-errors`).
- `airflow dags test` sale en verde para `hello_airflow3`, `operators_and_xcoms`,
  `dataops`, `mlops`, `sensors_and_events`, `monitoring`.
- La cadena DataOps → Asset → MLOps dispara de verdad: disparar `dataops` produjo
  un run `asset_triggered__…` de `mlops` sin ningún trigger manual.
- `mlops` validó el snapshot (`suite=labeled_projects passed=6/6
  suite=training_split passed=5/5`), entrenó 4 candidatos mapeados, seleccionó el
  mejor (f1 = 0.884) y lo promocionó.
- El camino HITL se resolvió: `awaiting_input` → `Approve` por defecto tras el
  timeout → disparó `TriggerDagRunOperator`.
- El callback del deadline disparó exactamente a +30s con el contexto completo de
  `dag_run`.
- Se ejercitaron los triggers manuales: un trigger contra un Dag **en pausa**
  produjo un run atascado en `queued` hasta `airflow dags unpause`, y
  `-c '{"greeting": "hola"}'` llegó a la task como override de `params`.
- El recorrido por la UI de la §5 se comprobó contra el api-server de este stack:
  navegación, pestañas, campos del formulario de trigger/backfill y el selector
  de versión salen de los strings de la UI publicada, y los endpoints que hay
  detrás (`/api/v2/assets`, `/backfills`, `/dags/{id}/dagVersions`,
  `/hitlDetails`) de su especificación OpenAPI.
- `pytest`: **30 passed**.

Re-verificado el 2026-09-11, contra el mismo stack, para los números y
afirmaciones que el cuerpo te devuelve impresos:

- La comprobación de la §4: el primer Dag loguea `(7 words)` — el saludo lleva el
  run id *y* la fecha.
- Las reglas de assets de la §8, probadas en ambos sentidos. Con `mlops` **sin
  pausa**, `airflow dags test dataops` disparó un run `asset_triggered__` real de
  `mlops` — `dags test` escribe asset events genuinos, no es un dry run. Con
  `mlops` **en pausa**, el asset event se escribió pero no se encoló ningún run,
  y quitar la pausa después **no** lo reprodujo. Airflow filtra los Dags en pausa
  cuando decide qué encolar.
- La espera de la §10, descompuesta a partir de timestamps de la base de datos en
  tres triggers: el propio run de `dataops` tardó 5.2–5.9s desde queued hasta
  terminado; el intervalo desde que se escribió su asset event hasta que se encoló el
  run de `mlops` fue de 0.12–0.65s — un bucle del Scheduler. El texto no cita
  ninguna cifra de extremo a extremo porque la parte de `dataops` depende de la
  red.
- El `wait_for_table` de la §11 comprueba una vez en el worker antes de
  quedar en `deferred` (`BigQueryTableExistenceSensor.execute`: `if not
  self.poke(context=context): self.defer(...)`), así que con `labeled_projects`
  presente `airflow dags test` no registra `running trigger in line` y nada
  queda en `deferred`. El 2026-10-31, por el Scheduler contra
  `labeled_projects_copy`: `wait_for_table` `queued` → `running` → `deferred`,
  mientras `enough_rows` y `proceed` salían con el estado vacío en
  `states-for-dag-run`. `CREATE TABLE … LIMIT 300`: `wait_for_table` `success`
  en menos de 10 s; el primer poke de `enough_rows` 4 s después, luego
  `up_for_reschedule`, registrando `labeled_projects_copy has 300 rows` cada
  ~14 s. `INSERT INTO` falló en este proyecto con `403 Billing has not been
  enabled … DML queries are not allowed in the free tier`, de ahí el
  `CREATE OR REPLACE TABLE` de la §11; tras él, `764 rows` y las tres en
  `success` en menos de 10 s.
- El `ApprovalOperator` de la §12 en 3.3 aparca la task en `awaiting_input` sin
  ningún trigger de por medio; la resuelve el barrido del Scheduler —
  `AWAITING_INPUT timeout sweep: 1 resolved (response/defaults)` en el log del
  Scheduler — y la task se reanuda en `execute_complete`. El Triggerer no hace
  falta para HITL.
- El fan-out de la §10 es **de dos en dos, en dos oleadas**
  (`max_active_tis_per_dag=2`), leído de las horas de inicio y fin de las cuatro
  task instances mapeadas — no cuatro a la vez.
- El modo reschedule de la §11: con el archivo curado apartado, ambos sensors se
  quedaron en `up_for_reschedule`; restaurar el archivo hizo que el siguiente
  poke tuviera éxito y el run terminó.
- Los tiempos de la §12: **69 segundos**, de los cuales **60** fueron
  `request_retraining` en `awaiting_input` antes de que se resolviera el default
  `Approve` y `TriggerDagRunOperator` creara un run real de `mlops`. Sin el
  override de `--conf` el mismo Dag tardó 7 segundos y ramificó a
  `continue_serving`.
- Los números de la §3 son la configuración viva de este stack: `parallelism=32`,
  `max_active_tasks_per_dag=16`, `default_pool=128` slots,
  `scheduler_idle_sleep_time=1`, `min_file_process_interval=30`,
  `default_task_retries=0`.
- Las formas de data interval de la §7, leídas de los propios timetables: un
  string cron es aquí un `CronTriggerTimetable`
  (`create_cron_data_intervals=False`) y da `run_after == data_interval_start ==
  data_interval_end`; `timedelta(days=1)` es aquí un `DeltaTriggerTimetable`
  (`create_delta_data_intervals=False`) y da la misma forma de longitud cero;
  `CronDataIntervalTimetable("0 0 * * *", timezone="UTC")` con un `start_date`
  de 2026-09-10 da `run_after` 2026-09-11 00:00 con `logical_date ==
  data_interval_start` 2026-09-10 00:00 y `data_interval_end` 2026-09-11
  00:00.
- El `start_date` de la §4/§7: un archivo de Dag sin él parsea **sin error de
  import** bajo `DagBag`, tanto con `schedule=None` como con `@daily`.
- El recuadro de `dags test` de la §4: `hello_airflow3` ejecutó `make_greeting →
  shout → count_words → report`, cada una terminando antes de que empezara la
  siguiente.
- La §13, unpause y luego trigger: `DEADLINE MISSED` a +31s, con `slow_step`
  todavía corriendo.
- El "pruébalo" de la §10 con la variable exportada en el contenedor (`bash -c
  'export PROJECT_TAGGER_MIN_F1=0.99; airflow dags test mlops'`): `promote_model`
  skipped, `keep_current_model` success. La forma que usa ahora la §10,
  `docker compose exec -e PROJECT_TAGGER_MIN_F1=0.99 …`, fija la misma variable:
  `config.MIN_F1_TO_PROMOTE` lee `0.99` en ese proceso.
- La nota de `curl` de la §3: en una máquina Windows, PowerShell resuelve `curl` a
  `Invoke-WebRequest`; `curl.exe` está en `System32`.
- El `enough_rows` de la §11 lee la tabla con pandas e informa de **764** filas,
  el número que escribió `dataops`.

Por último, una ejecución en frío el 2026-09-11 — `reset.sh`, `up.sh`, y luego
todos los comandos de la §2 a la §13 y de la §15 en orden:

- 28s desde `up.sh` hasta siete Dags listados, todos en pausa; el endpoint de
  salud `healthy` cuatro veces.
- §4 `(7 words)`, y `HOLA` con `--conf`. Las tasks corrieron de una en una —
  `count_words` antes que `shout` esta vez; el orden entre hermanas varía.
- §6 `product=42`. §7 `manual-demo-1` success; `demo` queued mientras estaba en
  pausa, success tras quitar la pausa con `start_date` 14s después de
  `run_after`.
- El backfill de la §9 (entonces en la §7), en ambos sentidos. Sin `--dry-run`, `2026-01-01`→`01-31`
  creó 30 runs `backfill__` en cola contra el `dataops` en pausa; en la §10
  corrieron de tres en tres y volvieron a disparar `mlops` detrás de ellos. La
  CLI solo tiene `backfill create`; `PUT /api/v2/backfills/1/cancel` devolvió
  200 y puso `failed` los seis runs que seguían en cola. Con `--dry-run`,
  `02-01`→`02-04` listó tres filas y no creó nada — y por eso la §9 empieza
  con él.
- La coalescencia de la §8: 22 asset events se convirtieron en 6 runs de `mlops`,
  consumiendo de 2 a 6 cada uno (`dagrun_asset_event`).
- §9 `passed=4/4`, `6/6`, `Wrote 764 rows`; con `mlops` en pausa, un evento y
  cero runs.
- §11 `deferred → scheduled → success` a través del Scheduler; ambos sensors
  `up_for_reschedule` con el archivo apartado; `764 rows` una vez restaurado.
- §12 76s con `--conf` — `awaiting_input`, `Approved`, `trigger_dag_id=mlops`,
  un run `manual__` nuevo; 9s y `continue_serving` sin él.
- §13 `DEADLINE MISSED` a +33s con `slow_step` todavía `running`. §15 30 passed,
  listados en sync.

Añadido el 2026-09-14, mismo stack: el `list-import-errors` de la §4. Con los
siete archivos de la lección, `No data found`. Con un octavo archivo cuya
segunda línea importa un módulo que no existe, una fila con el traceback — y
`airflow dags list` seguía mostrando siete, porque un archivo que falla al
importarse no es ningún Dag, no un Dag roto. Borrar el archivo y reiniciar el
dag-processor quitó la fila.

También el 2026-09-14: el arranque de la §2, en frío, con `AIRFLOW_UID=1000` en
`.env` (lo que `up.sh` escribe en WSL2). Con el archivo de compose anterior,
`airflow-init` fallaba en 0,2 s — `ModuleNotFoundError: No module named
'airflow'` — y aun así salía con 0; los cuatro servicios reiniciaban sin fin
con `Database migration required`, y `up.ps1` esperaba para siempre. Repetido
en un contenedor de usar y tirar en las mismas condiciones, el fallo es
determinista: `HOME=/`, así que el intérprete del script `airflow` nunca ve
`/home/airflow/.local`. Con init pasando por el entrypoint de la imagen, el
mismo `.env` y la misma base de datos vacía: `Database migration done!`,
endpoint de salud `healthy` 33 s después de `up`, siete Dags listados.

Reestructurado el 2026-09-15 como un tutorial de escribirlo-tú-mismo —
`dags/` vacía, los archivos terminados movidos a `solutions/`, cada lección
reconstruida como trozos que se teclean — y vuelto a ejecutar en frío en el
nuevo orden: `reset.sh`, `up.sh`, los siete archivos tecleados a partir de los
bloques de "Constrúyelo", todos los comandos de la §2 a la §13 y de la §15 tal cual están
escritos.

- §2 con `dags/` vacía: `airflow dags list`, `list-import-errors` y `assets
  list` imprimen los tres `No data found`; el endpoint de salud `healthy`
  cuatro veces.
- Los intervalos de parseo de la §3 son la configuración viva de este stack:
  `refresh_interval=5`, `min_file_process_interval=5`,
  `scheduler_idle_sleep_time=1`. Un archivo guardado en `dags/` apareció en
  `airflow dags list` **2 segundos** después.
- El archivo roto de la §4: con el comentario `# An airflow dag file that does
  not import`, la fila estaba en `list-import-errors` 6 s después de guardar y
  desapareció 6 s después de borrar el archivo, sin reiniciar nada. **Sin** las
  palabras `airflow` y `dag` en el archivo, no apareció nada en 90 s y el log
  del Dag processor nunca mencionó el archivo. `[core] dag_discovery_safe_mode`
  es `True` aquí, y `might_contain_dag_via_default_heuristic`
  (`airflow/utils/file.py`) exige que el contenido en minúsculas contenga
  `airflow` y uno de `dag` / `asset`. La comprobación del 2026-09-14 de arriba
  pasó porque su comentario contenía ambas por casualidad.
- §4 `(7 words)`, `HOLA` con `--conf`. §6 `product=42`, y después `TaskFlow saw
  product=42`.
- La regla de catch-up de la §7, leída de `CronTriggerTimetable.next_dagrun_info`
  (`airflow/timetables/trigger.py`): con `catchup=False` y sin run previo, el
  inicio es `max(align_to_prev(utcnow()), align_to_next(start_date))` — el tick
  más reciente antes de ahora. En este stack, quitar la pausa a `dataops` a las
  12:22 UTC creó `scheduled__2026-09-15T03:00:00+00:00` con un `start_date` de
  12:22:28, cinco segundos después de quitar la pausa. El "te da solo el
  siguiente" del texto anterior era falso y ha desaparecido.
- §9 con `dataops` guardado y nada ejecutado: `assets list` muestra solo
  `labeled_projects` y `raw_projects`; `assets details` tiene
  `producing_tasks [dataops.validate_transformed]` y `scheduled_dags []`.
  `dags test dataops`: `passed=4/4`, `Wrote 764 rows`, `passed=6/6`, las dos
  líneas `checks, failed=none`, 5,3 s. Dry-run del backfill `02-01`→`02-04`:
  tres filas, nada creado.
- El tercer caso de la §8, un consumidor que todavía no existía. El asset event de
  ese `dags test dataops` (id 2 en `asset_event`) nunca se consumió: después de
  escribir `mlops` y quitarle la pausa, `dagrun_asset_event` enlaza sus runs con
  los eventos 4 y 6 — el run `scheduled__` y un `manual__` posterior de
  `dataops` — y nunca con el 2.
- §10 con `mlops` guardado: `scheduled_dags [mlops]`. Quitar la pausa a `mlops`
  y luego a `dataops`, sin trigger: un run `scheduled__…T03:00:00` de
  `dataops`; un run `asset_triggered__` de `mlops` encolado 1,5 s después de su
  evento, con `logical_date` vacía; los map index 2 y 3 de `train_candidate`
  arrancaron después de que terminaran el 0 y el 1; `promote_model` success,
  `keep_current_model` skipped. El "pruébalo" con `PROJECT_TAGGER_MIN_F1=0.99`: `below
  threshold 0.99 - not promoting`, `Branch into keep_current_model`.
- §11 `dags test`: `curated table has 764 rows`, `Success criteria met`, `Data
  is ready (764 rows)`, 5,6 s. Por el Scheduler, consultado cada medio segundo:
  `settle` `deferred` a +2 s, `success` a +8 s.
- §12 con `--conf`: 74 s; `Branch into request_retraining`, `Waiting for
  response`, `Approved` 65 s después, `trigger_dag_id=mlops`, y un run
  `manual__` nuevo de `mlops`.
- §13: `DEADLINE MISSED` a +31 s, `slow_step` todavía corriendo.
- §15: los siete archivos tecleados pasan la suite de integridad (27 con los
  tests de la carga de trabajo); la misma suite con `DAGS_DIR=solutions` pasa;
  los bloques de "Constrúyelo" de ambos READMEs se ensamblan en archivos que
  importan; **36 passed** en total.
- Los dos pasos "Hazlo ahora" de la §5 y el recorrido por la UI se apoyan en las
  strings de la UI publicada y en los endpoints que hay detrás, como antes —
  nadie hizo clic en ellos en esta ejecución.

Comprobado el 2026-10-10 contra el código de 3.3.1 de la imagen de este stack,
después de que un lector detectara una afirmación errónea sobre `run_after`:

- `run_after` es "the date before which dag run won't start"
  (`airflow/models/dagrun.py`): el arranque más temprano posible, no el real.
- El schedule sin `start_date` de la §7: `CronTriggerTimetable` usa por defecto
  `run_immediately=False`, y `next_dagrun_info` sin `start_date` dio mañana a
  las 03:00 para `"0 3 * * *"` a las 08:04 UTC, mientras que
  `DeltaTriggerTimetable` dio el momento actual. Un Dag de prueba en pausa sin
  `start_date` con `*/2 * * * *` recalculaba su próximo run en cada parseo
  (08:00, luego 08:02), así que el primer run es la siguiente hora después de
  quitar la pausa, no después del primer parseo.
- La cola de la §8: `AssetManager.register_asset_change` escribe la entrada de
  cola de cada consumidor sin pausa en el mismo paso que el evento; el
  Scheduler solo convierte esas entradas en runs.
- El rango del backfill de la §9: `iter_dagrun_infos_between` incluye los dos
  extremos. Para `dataops`, `2026-02-01` → `2026-02-04` da tres runs y
  `2026-02-01` → `2026-02-04T03:00:00` da cuatro.
- El backoff de la §4: `retry_exponential_backoff` es un multiplicador
  (`float`, por defecto 0); `True` se sigue deserializando como `2.0`.

Se encontraron seis defectos reales mientras se construía esto, todos
documentados en [troubleshooting](troubleshooting.md):

- `fs_default` no se crea solo.
- `airflow dags test` se rompe con Dags que definen `deadline=` (un bug de
  Airflow 3.3.1).
- `DagBag` hay que importarlo de `airflow.dag_processing.dagbag`.
- `context["logical_date"]` lanza `KeyError` en cualquier run sin logical date
  — encontrado por un trigger manual, después de que todos los tests de la §15
  pasaran.
- `{{ ds }}` lanza `UndefinedError` en esos mismos runs — encontrado de la misma
  forma.
- `airflow-init` se saltaba el entrypoint de la imagen, no podía importar
  Airflow, y aun así salía con 0 — encontrado por un arranque en frío con un uid
  de WSL2 en `.env`.
