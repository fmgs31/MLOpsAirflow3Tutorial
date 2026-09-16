# Qué se verificó de verdad

> Parte del [tutorial de Airflow 3](../README_ES.md), sacado de él para que el
> tutorial siga siendo legible. `§N` se refiere a una sección numerada de ese
> README.

El cuerpo del tutorial dice lo que vas a ver. Este documento dice cómo se
sabe: cada cifra y afirmación de allí se leyó de un stack vivo, y esto es el
registro de cuándo, y de cómo se midió. Nada de esto es un paso que ejecutar.
Sáltatelo, o vuelve cuando quieras saber hasta qué punto fiarte de un número.

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
  de versión salen de las cadenas de la UI publicada, y los endpoints que hay
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
  terminado; el hueco desde que se escribió su asset event hasta que se encoló el
  run de `mlops` fue de 0.12–0.65s — un bucle del scheduler. El texto no cita
  ninguna cifra de extremo a extremo porque la parte de `dataops` depende de la
  red.
- El `settle` de la §11 (`TimeDeltaSensorAsync`, 5s) **no** se defiere bajo
  `airflow dags test`: su objetivo se mide desde la logical date del run, que el
  comando fija antes de que arranque la task, así que el momento ya ha pasado
  cuando la task corre y vuelve de inmediato — sin estado `deferred`, sin
  triggerer (`sensors/time_delta.py`: `if utcnow() > target_dttm: return`). Una
  task que se difiriera bajo `dags test` tampoco llegaría al triggerer: el
  comando ejecuta los triggers inline (`sdk/definitions/dag.py`,
  `_run_inline_trigger`). Enviada por el scheduler con `airflow dags trigger`, la
  misma task estuvo `deferred` seis segundos, pasó a `scheduled` y luego a
  `success` — consultado cada medio segundo desde la tabla `task_instance`.
- El `ApprovalOperator` de la §12 en 3.3 aparca la task en `awaiting_input` sin
  ningún trigger de por medio; la resuelve el barrido del scheduler —
  `AWAITING_INPUT timeout sweep: 1 resolved (response/defaults)` en el log del
  scheduler — y la task se reanuda en `execute_complete`. El triggerer no hace
  falta para HITL.
- El fan-out de la §10 es **de dos en dos, en dos oleadas**
  (`max_active_tis_per_dag=2`), leído de las horas de inicio y fin de las cuatro
  task instances mapeadas — no cuatro a la vez.
- El modo reschedule de la §11: con el fichero curado apartado, ambos sensors se
  quedaron en `up_for_reschedule`; restaurar el fichero hizo que el siguiente
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
- Las formas de data interval de la §7, leídas de los propios timetables: una
  cadena cron es aquí un `CronTriggerTimetable`
  (`create_cron_data_intervals=False`) y da `run_after == data_interval_start ==
  data_interval_end`; `timedelta(days=1)` es un `DeltaDataIntervalTimetable` y da
  `run_after` 2026-09-11 00:00 con `logical_date == data_interval_start`
  2026-09-10 00:00.
- El `start_date` de la §4/§7: un fichero de Dag sin él parsea **sin error de
  import** bajo `DagBag`, tanto con `schedule=None` como con `@daily`.
- El recuadro de `dags test` de la §4: `hello_airflow3` ejecutó `make_greeting →
  shout → count_words → report`, cada una terminando antes de que empezara la
  siguiente.
- La §13, unpause y luego trigger: `DEADLINE MISSED` a +31s, con `slow_step`
  todavía corriendo.
- El "pruébalo" de la §10 en su forma explícita (`bash -c 'export
  TAGIFAI_MIN_F1=0.99; airflow dags test mlops'`): `promote_model` skipped,
  `keep_current_model` success.
- La nota de `curl` de la §3: en una máquina Windows, PowerShell resuelve `curl` a
  `Invoke-WebRequest`; `curl.exe` está en `System32`.
- El `enough_rows` de la §11 lee la tabla con pandas e informa de **764** filas,
  el número que escribió `dataops`.

Por último, una ejecución en frío el 2026-09-11 — `reset.sh`, `up.sh`, y luego
todos los comandos de la §2 al §14 en orden:

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
- §11 `deferred → scheduled → success` a través del scheduler; ambos sensors
  `up_for_reschedule` con el fichero apartado; `764 rows` una vez restaurado.
- §12 76s con `--conf` — `awaiting_input`, `Approved`, `trigger_dag_id=mlops`,
  un run `manual__` nuevo; 9s y `continue_serving` sin él.
- §13 `DEADLINE MISSED` a +33s con `slow_step` todavía `running`. §14 30 passed,
  listados en sync.

Añadido el 2026-09-14, mismo stack: el `list-import-errors` de la §4. Con los
siete ficheros de la lección, `No data found`. Con un octavo fichero cuya
segunda línea importa un módulo que no existe, una fila con el traceback — y
`airflow dags list` seguía mostrando siete, porque un fichero que falla al
importarse no es ningún Dag, no un Dag roto. Borrar el fichero y reiniciar el
dag-processor quitó la fila.

También el 2026-09-14: el arranque de la §2, en frío, con `AIRFLOW_UID=1000` en
`.env` (lo que `up.sh` escribe en WSL2). Con el fichero de compose anterior,
`airflow-init` fallaba en 0,2 s — `ModuleNotFoundError: No module named
'airflow'` — y aun así salía con 0; los cuatro servicios reiniciaban sin fin
con `Database migration required`, y `up.ps1` esperaba para siempre. Repetido
en un contenedor de usar y tirar en las mismas condiciones, el fallo es
determinista: `HOME=/`, así que el intérprete del script `airflow` nunca ve
`/home/airflow/.local`. Con init pasando por el entrypoint de la imagen, el
mismo `.env` y la misma base de datos vacía: `Database migration done!`,
endpoint de salud `healthy` 33 s después de `up`, siete Dags listados.

Reestructurado el 2026-09-15 como un tutorial de escribirlo-tú-mismo —
`dags/` vacía, los ficheros terminados movidos a `solutions/`, cada lección
reconstruida como trozos que se teclean — y vuelto a ejecutar en frío en el
nuevo orden: `reset.sh`, `up.sh`, los siete ficheros tecleados a partir de los
bloques de "Constrúyelo", todos los comandos de la §2 al §14 tal cual están
escritos.

- §2 con `dags/` vacía: `airflow dags list`, `list-import-errors` y `assets
  list` imprimen los tres `No data found`; el endpoint de salud `healthy`
  cuatro veces.
- Los intervalos de parseo de la §3 son la configuración viva de este stack:
  `refresh_interval=5`, `min_file_process_interval=5`,
  `scheduler_idle_sleep_time=1`. Un fichero guardado en `dags/` apareció en
  `airflow dags list` **2 segundos** después.
- El fichero roto de la §4: con el comentario `# An airflow dag file that does
  not import`, la fila estaba en `list-import-errors` 6 s después de guardar y
  desapareció 6 s después de borrar el fichero, sin reiniciar nada. **Sin** las
  palabras `airflow` y `dag` en el fichero, no apareció nada en 90 s y el log
  del Dag processor nunca mencionó el fichero. `[core] dag_discovery_safe_mode`
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
  `keep_current_model` skipped. El "pruébalo" con `TAGIFAI_MIN_F1=0.99`: `below
  threshold 0.99 - not promoting`, `Branch into keep_current_model`.
- §11 `dags test`: `curated table has 764 rows`, `Success criteria met`, `Data
  is ready (764 rows)`, 5,6 s. Por el scheduler, consultado cada medio segundo:
  `settle` `deferred` a +2 s, `success` a +8 s.
- §12 con `--conf`: 74 s; `Branch into request_retraining`, `Waiting for
  response`, `Approved` 65 s después, `trigger_dag_id=mlops`, y un run
  `manual__` nuevo de `mlops`.
- §13: `DEADLINE MISSED` a +31 s, `slow_step` todavía corriendo.
- §14: los siete ficheros tecleados pasan la suite de integridad (27 con los
  tests de la carga de trabajo); la misma suite con `DAGS_DIR=solutions` pasa;
  los bloques de "Constrúyelo" de ambos READMEs se ensamblan en ficheros que
  importan; **36 passed** en total.
- Los dos pasos "Hazlo ahora" de la §5 y el recorrido por la UI se apoyan en las
  cadenas de la UI publicada y en los endpoints que hay detrás, como antes —
  nadie hizo clic en ellos en esta ejecución.

Se encontraron seis defectos reales mientras se construía esto, todos
documentados en [troubleshooting](troubleshooting.md):

- `fs_default` no se crea solo.
- `airflow dags test` se rompe con Dags que llevan un `deadline=`.
- `DagBag` hay que importarlo de `airflow.dag_processing.dagbag`.
- `context["logical_date"]` lanza `KeyError` en cualquier run sin logical date
  — encontrado por un trigger manual, después de que todos los tests de la §14
  pasaran.
- `{{ ds }}` lanza `UndefinedError` en esos mismos runs — encontrado de la misma
  forma.
- `airflow-init` se saltaba el entrypoint de la imagen, no podía importar
  Airflow, y aun así salía con 0 — encontrado por un arranque en frío con un uid
  de WSL2 en `.env`.
