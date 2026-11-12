## 11. Esperar: sensors, deferrable tasks, eventos

`dags/05_sensors_and_events.py`. Tres formas de esperar al mundo exterior, de
más a menos worker slots consumidos — y, después de ellas, la única forma de
no esperar en absoluto.

Un **sensor** es una task cuyo único trabajo es esperar a que algo sea verdadero:
que exista un archivo, que una tabla tenga filas, que responda una API. Cada
comprobación se llama **poke**. Si el poke da verdadero, la task termina con
éxito; si no, espera `poke_interval` segundos y vuelve a comprobar; si pasa
`timeout` sin éxito, falla. Las tres formas se distinguen por lo que ocurre
*entre* pokes:

```python
# 1. `poke` mode (the default) — holds a worker slot the entire time. For waits of seconds, not hours.
BigQueryTableExistenceSensor(task_id="wait", project_id=..., dataset_id=..., table_id=...,
                             poke_interval=10, timeout=300, mode="poke")

# 2. `reschedule` mode — the task's process exits between pokes and is re-queued.
BigQueryTableExistenceSensor(..., poke_interval=10, timeout=300, mode="reschedule")

# 3. Deferrable — hands the wait to the triggerer and frees the slot entirely.
BigQueryTableExistenceSensor(..., poke_interval=10, timeout=300, deferrable=True)
```

`poke_interval` y `timeout` se aplican a las tres. `mode` elige entre las dos
primeras; con `deferrable=True`, `mode` se ignora. Entre pokes de
`reschedule`, la task instance queda en `up_for_reschedule` (§3) hasta que el
Scheduler la vuelve a encolar.

Deferrable necesita antes un término: un **trigger** es un pequeño fragmento de
código asíncrono que espera a algo (un momento concreto, una tabla, un mensaje)
y lanza un evento cuando ocurre. Los ejecuta el Triggerer de la §3. No tiene
nada que ver con *disparar* (trigger) un Dag (§7), que es lanzar un run. Una
task deferrable le entrega al Triggerer un trigger, termina, y se reanuda
cuando llega el evento.

### Qué forma de esperar usar

Deferrable es la que más libera, pero no siempre está disponible. Lo primero
que decide es la propia comprobación:

- **Deferrable necesita un trigger escrito para esa comprobación.** Los
  providers incluyen triggers para sus esperas habituales: el provider de
  Google tiene uno que pregunta a BigQuery si existe una tabla, y
  `BigQueryTableExistenceSensor(deferrable=True)` se lo entrega al Triggerer.
  Una comprobación propia (un recuento de filas contra un umbral, un sistema
  sin provider de Airflow) no tiene. Escribir uno significa escribir código
  asíncrono que nunca bloquee: el Triggerer ejecuta todos los triggers en un
  solo proceso, así que una llamada que bloquea detiene todas las esperas que
  lleva.
- **`poke` y `reschedule` sirven para cualquier comprobación** escrita en
  Python normal, como una función `@task.sensor` (Constrúyelo muestra una).
  Corre en el proceso de la propia task, con todas las librerías que tiene el
  worker.

Cuando una comprobación puede ser deferrable o `reschedule`, deciden dos cosas:

- **Cuánto dura la espera, y cada cuánto se comprueba.** Cada poke de
  `reschedule` es un arranque completo de la task: el Scheduler la encola, y un
  worker arranca un proceso y carga el archivo del Dag. Un poke cada pocos
  minutos durante una hora cuesta poco; un poke cada pocos segundos durante un
  día se acumula, y deferrable sale más barato: un solo proceso Triggerer lleva
  muchas esperas a la vez.
- **Cuánto pesa la comprobación.** Una comprobación que necesita pandas, una
  librería de ML o una consulta de varios segundos va en un proceso de task. Un
  trigger solo debería hacer llamadas asíncronas rápidas.

`dags/05_sensors_and_events.py` encadena dos sensors. Las dos esperas pueden
ser largas, así que ninguno usa `poke`; los separa el trigger. `wait_for_table`
espera a que exista la tabla, algo para lo que el provider de Google tiene
trigger, así que es deferrable. Después, `enough_rows` espera a 500 filas: un
umbral propio, sin trigger, así que es un `@task.sensor` en modo `reschedule`.
Dónde espera cada paso, una columna por sistema como en el diagrama de la §9:

```
 Airflow: proceso de la task    │ Airflow: el Triggerer  │ BigQuery
 Dag sensors_and_events         │ (§3)                   │ mlops_course
────────────────────────────────┼────────────────────────┼──────────────────
                                │                        │
 1 wait_for_table (deferrable) ──► cada 10 s pregunta ────► ¿existe ya la
                                │                        │  tabla?
   se reanuda ◄─────────────────── dispara el evento     │
                                │                        │
 2 enough_rows (reschedule)     │                        │
   un poke cada 10 s ─────────────────────────────────────► cuenta sus filas
   sale entre pokes             │                        │
 3 proceed: 500+ filas          │                        │
```

### Constrúyelo

Crea `dags/05_sensors_and_events.py` con la cabecera y el primer sensor,
`wait_for_table`, escrito como operator:

```python dags/05_sensors_and_events.py
"""Lesson 5 - waiting for things: sensors, deferrable operators, and events."""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.sdk import Param, PokeReturnValue, dag, task

from project_tagger import config, data

DEFAULT_ARGS = {"owner": "data-engineering", "retries": 0}


@dag(
    dag_id="sensors_and_events",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "sensors"],
    doc_md=__doc__,
    params={"table": Param(config.LABELED_PROJECTS_TABLE, type="string", description="Table to wait for.")},
)
def sensors_and_events():
    wait_for_table = BigQueryTableExistenceSensor(
        task_id="wait_for_table",
        project_id=config.GCP_PROJECT_ID,
        dataset_id=config.BQ_DATASET,
        table_id="{{ params.table }}",
        deferrable=True,
        poke_interval=10,
        timeout=60 * 5,
    )
```

Con `deferrable=True`, su trigger corre en el Triggerer y pregunta a BigQuery
cada `poke_interval` segundos si existe la tabla, a través de la connection
`google_cloud_default` que define el archivo de compose (§9). Si pasan
`timeout` segundos sin la tabla, la task falla.

La tabla la indica un **param del Dag**, `table`, con valor por defecto
`config.LABELED_PROJECTS_TABLE` (`labeled_projects` en `config.py`); un run
puede apuntarlo a otra tabla con `-c`, como hace Ejecútalo. `table_id` es un
campo con template (§6), así que `{{ params.table }}` se renderiza cuando corre
la task.

Un sensor también puede ser una función, con `@task.sensor`: la función es la
comprobación, y Airflow le hace los pokes. El segundo sensor, `enough_rows`, es
así, en modo `reschedule`. Añádelo, con la task que usa su valor y el cableado
entre tasks:

```python dags/05_sensors_and_events.py
    @task.sensor(poke_interval=10, timeout=60 * 5, mode="reschedule")
    def enough_rows(*, params: dict[str, Any]) -> PokeReturnValue:
        rows = data.count_rows(params["table"])  # SELECT COUNT(*) in BigQuery
        print(f"{params['table']} has {rows} rows")
        return PokeReturnValue(is_done=rows >= 500, xcom_value=rows)

    @task
    def proceed(rows: int) -> None:
        print(f"Data is ready ({rows} rows). Downstream work can start.")

    rows = enough_rows()
    wait_for_table >> rows
    proceed(rows)


sensors_and_events()
```

Cada poke devuelve un `PokeReturnValue`. Con `is_done=False` la task vuelve a
hacer un poke tras `poke_interval` segundos; con `is_done=True` los pokes se
detienen y `xcom_value` va aguas abajo: `proceed` recibe el recuento de filas.

`enough_rows` solo arranca cuando `wait_for_table` termina con éxito, así que
en su primer poke la tabla ya existe y la comprobación solo tiene que contar.
`enough_rows()` se guarda en una variable, `rows`, porque se usa dos veces:
`wait_for_table >> rows` fija el orden, y `proceed(rows)` pasa el recuento de
filas (§6).

Los dos sensors comprueban cada 10 segundos y se rinden a los cinco minutos,
para que cada estado de Ejecútalo aparezca en segundos. Una espera real de una
tabla diaria comprobaría cada pocos minutos y se rendiría tras horas:
`poke_interval=60 * 5, timeout=60 * 60 * 6`, por ejemplo.

> Los sensors deferrable necesitan el Triggerer (§3) corriendo. Sin él, se
> quedan en `deferred` indefinidamente.

### Ejecútalo

Este Dag espera a `labeled_projects`, que construye el Dag `dataops`; si no has
ejecutado `dataops` (§9), los sensors harán polling hasta agotar su timeout:

```bash
docker compose exec airflow-scheduler airflow dags test sensors_and_events
```

**Comprobación.** Termina en unos segundos, porque todo lo que espera ya ha
ocurrido. En el log:

```
labeled_projects has 764 rows
Data is ready (764 rows). Downstream work can start.
```

`wait_for_table` encuentra la tabla en su primera comprobación, y `enough_rows`
supera su umbral en el primer poke. Nada quedó en `deferred`: un sensor
deferrable comprueba primero una vez en su propio proceso, y solo queda en
`deferred` si la respuesta es no.

Cuando un sensor sí queda en `deferred`, `airflow dags test`, que no tiene
workers ni Triggerer, ejecuta el trigger en su propio proceso y lo anuncia en
el log con `[DAG TEST] running trigger in line` (§4). Para ver las dos formas
de esperar tal como ocurren de verdad, lanza un run a través del Scheduler.

**Ahora el caso interesante**: sensors que sí tienen que esperar. Lanza un run
cuyo param `table` apunte a `labeled_projects_copy`, una tabla que todavía no
existe, y luego consulta los estados de sus tasks:

```bash
docker compose exec airflow-scheduler airflow dags unpause sensors_and_events
docker compose exec airflow-scheduler airflow dags trigger sensors_and_events -r wait-demo -c '{"table": "labeled_projects_copy"}'   # pick a new id if you rerun
docker compose exec airflow-scheduler airflow tasks states-for-dag-run sensors_and_events wait-demo -o plain
```

`-r wait-demo` le pone nombre al run (§7) para que `states-for-dag-run` pueda
encontrarlo; un run id solo se puede usar una vez por Dag, de ahí el
comentario. `-c` fija el param `table`.

**Comprobación.** A los pocos segundos, `wait_for_table` aparece como
`deferred`: su primera comprobación no encontró la tabla, su proceso ha
terminado, y el Triggerer pregunta a BigQuery cada diez segundos.
`enough_rows` y `proceed` todavía no tienen estado, porque no se han
programado. Ese `deferred` (§3) es lo que se ve desde fuera cuando una task
"libera el worker slot".

Ahora crea la tabla, con menos filas de las que pide `enough_rows`. En la
consola de Google Cloud, abre BigQuery, abre una pestaña de consulta nueva y
ejecuta, con tu Project ID:

```sql
CREATE TABLE `<your project id>.mlops_course.labeled_projects_copy` AS
SELECT * FROM `<your project id>.mlops_course.labeled_projects` LIMIT 300
```

Espera unos veinte segundos y vuelve a ejecutar el comando
`states-for-dag-run`.

**Comprobación.** `wait_for_table` aparece como `success`: su trigger vio la
tabla. `enough_rows` aparece como `up_for_reschedule`, el estado de la §3 para
un sensor entre pokes: ha contado 300 filas y ha terminado su proceso, y diez
segundos después de cada poke el Scheduler la vuelve a encolar. Su log en la UI
(§5) muestra `labeled_projects_copy has 300 rows` una vez por poke.

Ahora dale filas suficientes, reemplazando la copia por la tabla entera:

```sql
CREATE OR REPLACE TABLE `<your project id>.mlops_course.labeled_projects_copy` AS
SELECT * FROM `<your project id>.mlops_course.labeled_projects`
```

**Comprobación.** En unos quince segundos `enough_rows` cuenta 764 filas,
`states-for-dag-run` muestra las tres tasks en `success`, y el log de `proceed`
dice `Data is ready (764 rows)`.

Si te detienes en cualquiera de los dos pasos, el sensor que espera falla por su
`timeout` de cinco minutos, y las tasks que van detrás terminan en
`upstream_failed` (§3).

Borra la copia cuando acabes, en el mismo editor de consultas:

```sql
DROP TABLE `<your project id>.mlops_course.labeled_projects_copy`
```

### No esperar en absoluto: event-driven scheduling

Los dos sensors de `sensors_and_events` preguntan una y otra vez si algo ha
ocurrido. El **event-driven scheduling** le da la vuelta: el servicio externo
envía un mensaje a una cola (como Amazon SQS) cuando algo ocurre, y Airflow
reacciona. Asocia a un asset un *watcher* (un trigger que vigila la cola), y
cada mensaje actualiza el asset, lo que arranca un run de cada Dag programado
sobre él (§8). Ninguna task espera, y no se usa ningún worker slot. El ejemplo
necesita una cola de mensajes real, que el stack del tutorial no tiene, así que
no se añade a `dags/05_sensors_and_events.py`:

```python
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher

trigger = MessageQueueTrigger(queue="https://sqs.us-east-1.amazonaws.com/.../new-projects")
new_projects = Asset(name="new_projects",
                     watchers=[AssetWatcher(name="sqs_watcher", trigger=trigger)])

@dag(schedule=[new_projects])
def ingest_on_message(): ...
```

Solo los triggers que derivan de `BaseEventTrigger` pueden ser watchers. Un
watcher sigue vigilando después de cada evento, así que un trigger que
comprueba una condición duradera ("la tabla existe") se dispararía otra vez
enseguida y arrancaría runs para siempre. Igual que los sensors deferrable, los
watchers corren en el Triggerer: sin él, no ven ningún mensaje ni arrancan
ningún run.

**Regla práctica:** segundos → el modo `poke` por defecto. Minutos u horas →
deferrable si existe un trigger para la comprobación, `mode="reschedule"` si
la comprobación es tu propio Python; `poke` ahí ocupa un worker slot solo para
dormir. Un servicio que avisa enviando un mensaje a una cola → un
`AssetWatcher`, y ninguna task espera.

Referencia: [`solutions/05_sensors_and_events.py`](../../solutions/05_sensors_and_events.py).

---

← [10. El Dag de MLOps](10-el-dag-de-mlops.md) · [Índice](README.md) · [12. Continual learning: monitorizar, decidir, actuar](12-continual-learning-monitorizar-decidir-actuar.md) →
