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
                             poke_interval=10, timeout=300)

# 2. `reschedule` mode — the task's process exits between pokes and is re-queued.
BigQueryTableExistenceSensor(..., mode="reschedule")

# 3. Deferrable — hands the wait to the triggerer and frees the slot entirely.
BigQueryTableExistenceSensor(..., deferrable=True)
```

Con el modo por defecto, `mode="poke"`, el proceso de la task sigue vivo entre
pokes, dormido, ocupando su worker slot. Con `mode="reschedule"`, el proceso
termina después de cada poke; la task instance queda en `up_for_reschedule`
(§3) y el Scheduler la vuelve a encolar para el siguiente poke.

Para entender la tercera opción, deferrable, hace falta saber qué es un
**trigger**: un pequeño fragmento de código asíncrono que espera a algo (un
momento concreto, una tabla, un mensaje) y lanza un evento cuando ocurre. Los
ejecuta el proceso Triggerer de la §3. No tiene nada que ver con *disparar*
(trigger) un Dag (§7), que es lanzar un run. "Deferrable" significa que la task
le entrega al Triggerer uno de esos triggers, termina su proceso, y se reanuda
cuando llega el evento.

### Qué forma de esperar usar

Deferrable es la que más libera, pero no siempre está disponible. Lo primero
que decide es la propia comprobación:

- **Deferrable necesita un trigger escrito para esa comprobación.** Un trigger
  es código asíncrono, y alguien tiene que haberlo escrito. Los providers
  incluyen triggers para sus esperas habituales: el provider de Google tiene
  uno que pregunta a BigQuery si existe una tabla, y
  `BigQueryTableExistenceSensor(deferrable=True)` se lo entrega al Triggerer.
  Una comprobación propia (un recuento de filas contra un umbral, una consulta
  a un sistema sin provider de Airflow) no tiene trigger. Escribir uno
  significa escribir código asíncrono que nunca bloquee: el Triggerer ejecuta
  todos los triggers en un solo proceso, así que una llamada que bloquea
  detiene todas las esperas que lleva.
- **`poke` y `reschedule` sirven para cualquier comprobación** escrita en
  Python normal, como una función `@task.sensor` (Constrúyelo muestra una). La
  comprobación corre en el proceso de la propia task, como cualquier task, con
  todas las librerías que tiene el worker.

Cuando una comprobación puede ser deferrable o `reschedule`, deciden dos cosas:

- **Cuánto dura la espera, y cada cuánto se comprueba.** Cada poke de
  `reschedule` es un arranque completo de la task: el Scheduler la encola, y un
  worker arranca un proceso y carga el archivo del Dag. Un poke cada pocos
  minutos durante una hora cuesta poco. Un poke cada pocos segundos durante un
  día se acumula, y deferrable sale más barato: un solo proceso Triggerer lleva
  muchas esperas a la vez.
- **Cuánto pesa la comprobación.** Una comprobación que necesita pandas, una
  librería de ML o una consulta que tarda segundos va en un proceso de task. Un
  trigger solo debería hacer llamadas asíncronas rápidas.

`poke` es para esperas de segundos: ocupa el slot todo el tiempo y, a cambio,
la task no vuelve a arrancar entre pokes.

`dags/05_sensors_and_events.py` encadena uno de cada. `wait_for_table` espera
a que exista la tabla: el provider de Google tiene el trigger, y la espera
puede ser larga, así que es deferrable. Después, `enough_rows` espera a que la
tabla tenga 500 filas: un umbral propio, sin trigger, así que es un
`@task.sensor` en modo `reschedule`. Solo arranca cuando ya hay una tabla que
contar. Aquí se ve dónde espera cada paso. Cada columna es un sistema, como en
el diagrama de la §9:

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

Mientras `wait_for_table` espera, la espera ocurre en el Triggerer, y no existe
ningún proceso de la task. `enough_rows` hace sus propias comprobaciones: un
poke cada 10 segundos, sin proceso entre uno y otro.

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

`deferrable=True` lo hace deferrable. Su trigger, que corre en el Triggerer,
pregunta a BigQuery cada `poke_interval` segundos si existe la tabla, a través
de la connection `google_cloud_default` que define el archivo de compose
(§9). Si pasan `timeout` segundos sin la tabla, la task falla.

La tabla que hay que esperar la indica un **param del Dag**, `table`, con
`labeled_projects` por defecto, así que se puede apuntar un run a otra tabla
con `-c`, como harás en Ejecútalo. `table_id` es un campo con template (§6), así
que `{{ params.table }}` se renderiza cuando corre la task.

Un sensor también se puede escribir como función, con `@task.sensor`: la
función es la comprobación, y Airflow le hace los pokes. El segundo sensor,
`enough_rows`, es así, en modo `reschedule` (la segunda de las tres formas del
principio de la §11). Añádelo, con la task que usa su valor, y el cableado
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

Cada poke devuelve un `PokeReturnValue`. Con `is_done=False` la task espera
`poke_interval` segundos y vuelve a hacer un poke. Con `is_done=True` los
pokes se detienen y `xcom_value` se entrega aguas abajo: `proceed` recibe el
recuento de filas.

Fíjate en el cableado: `enough_rows` solo arranca cuando `wait_for_table`
termina con éxito, así que en su primer poke la tabla ya existe y la
comprobación solo tiene que contar. El resultado de `enough_rows()` se guarda
en una variable, `rows`, porque se usa dos veces: `wait_for_table >> rows`
fija el orden, y `proceed(rows)` pasa el recuento de filas (§6).

> Los sensors deferrable necesitan el Triggerer (§3) corriendo. Sin él, se
> quedan en `deferred` indefinidamente.

### Ejecútalo

Este Dag espera a `labeled_projects`, la tabla que construye el Dag `dataops`,
así que tienes que haber ejecutado `dataops` en la §9, o los sensors harán
polling hasta agotar su timeout:

```bash
docker compose exec airflow-scheduler airflow dags test sensors_and_events
```

**Comprobación.** Termina en unos segundos, porque todo lo que espera ya ha
ocurrido. En el log:

```
labeled_projects has 764 rows
Data is ready (764 rows). Downstream work can start.
```

`wait_for_table` encuentra la tabla en su primera comprobación; `enough_rows`
cuenta las filas y supera su umbral en el primer poke. Nada quedó en
`deferred`: un sensor deferrable comprueba una vez en su propio proceso antes
de entregarle nada al Triggerer, y solo queda en `deferred` si la respuesta es
no.

Cuando un sensor sí queda en `deferred`, `airflow dags test`, que no tiene
workers ni Triggerer, ejecuta el trigger en su propio proceso y lo anuncia en
el log con `[DAG TEST] running trigger in line` (§4). Para ver las dos formas
de esperar tal como ocurren de verdad, lanza un run a través del Scheduler.

**Ahora el caso interesante**: sensors que sí tienen que esperar. Lanza un run
que apunte a una tabla que todavía no existe, y luego consulta los estados de
sus tasks:

```bash
docker compose exec airflow-scheduler airflow dags unpause sensors_and_events
docker compose exec airflow-scheduler airflow dags trigger sensors_and_events -r wait-demo -c '{"table": "labeled_projects_copy"}'   # pick a new id if you rerun
docker compose exec airflow-scheduler airflow tasks states-for-dag-run sensors_and_events wait-demo -o plain
```

**Comprobación.** A los pocos segundos, `wait_for_table` aparece como
`deferred`: su primera comprobación no encontró la tabla, su proceso ha
terminado, y el Triggerer pregunta a BigQuery cada diez segundos.
`enough_rows` y `proceed` todavía no tienen estado, porque no se han
programado. Ese `deferred` es el estado de la §3, y es lo que se ve desde
fuera cuando una task "libera el worker slot".

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
tabla. `enough_rows` aparece como `up_for_reschedule`, el estado que la §3
describe para un sensor entre pokes. Ha hecho un poke, ha contado 300 filas y
ha terminado su proceso; diez segundos después de cada poke el Scheduler la
vuelve a encolar, y hace otro poke. Su log en la UI (§5) muestra
`labeled_projects_copy has 300 rows` una vez por poke.

Ahora dale filas suficientes, reemplazando la copia por la tabla entera:

```sql
CREATE OR REPLACE TABLE `<your project id>.mlops_course.labeled_projects_copy` AS
SELECT * FROM `<your project id>.mlops_course.labeled_projects`
```

**Comprobación.** En unos quince segundos `enough_rows` cuenta 764 filas, y el
comando `states-for-dag-run` muestra las tres tasks en `success`. El log de
`proceed` dice `Data is ready (764 rows)`.

Ningún sensor ocupó un worker slot mientras esperaba: `wait_for_table` esperó
en el Triggerer, y `enough_rows` devolvió su slot entre pokes. En el modo
`poke` por defecto cada uno habría ocupado uno durante toda su espera, sin
hacer nada. Si te detienes en cualquiera de los dos pasos, el sensor que está
esperando falla por su `timeout`, cinco minutos, y las tasks que van detrás terminan en
`upstream_failed` (§3).

Borra la copia cuando acabes, en el mismo editor de consultas:

```sql
DROP TABLE `<your project id>.mlops_course.labeled_projects_copy`
```

### No esperar en absoluto: event-driven scheduling

Los dos sensors de `sensors_and_events` preguntan una y otra vez si algo ha
ocurrido. El **event-driven scheduling** le da la vuelta: el servicio externo
envía un mensaje a una cola (como Amazon SQS) cuando algo ocurre, y Airflow
reacciona. No va en el archivo, porque necesita una cola que no tienes. Asocia
a un asset un *watcher* (un trigger que vigila la cola), y cada mensaje que
llega actualiza el asset, lo que arranca un run de cada Dag programado sobre
él (§8). Ninguna task espera, y no se usa ningún worker slot:

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
enseguida, y arrancaría runs para siempre.

Igual que los sensors deferrable, los watchers corren en el Triggerer: sin él,
un watcher no ve ningún mensaje, así que no arranca ningún run.

**Regla práctica:** segundos → el modo `poke` por defecto vale. Minutos u horas →
deferrable si existe un trigger para la comprobación, `mode="reschedule"` si
la comprobación es tu propio Python, o estarás ocupando un worker slot solo
para dormir. Un servicio que avisa por sí mismo, enviando un mensaje a una cola → un
`AssetWatcher`, y ninguna task espera.

Referencia: [`solutions/05_sensors_and_events.py`](../../solutions/05_sensors_and_events.py).

---

← [10. El Dag de MLOps](10-el-dag-de-mlops.md) · [Índice](README.md) · [12. Continual learning: monitorizar, decidir, actuar](12-continual-learning-monitorizar-decidir-actuar.md) →
