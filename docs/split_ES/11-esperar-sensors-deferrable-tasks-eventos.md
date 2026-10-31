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
(§3) y el scheduler la vuelve a encolar para el siguiente poke.

Para entender la tercera opción, deferrable, hace falta saber qué es un
**trigger**: un pequeño fragmento de código asíncrono que espera a algo (un
momento concreto, una tabla, un mensaje) y lanza un evento cuando ocurre. Los
ejecuta el proceso triggerer de la §3. No tiene nada que ver con *disparar*
(trigger) un Dag (§7), que es lanzar un run. "Deferrable" significa que la task
le entrega al triggerer uno de esos triggers, termina su proceso, y se reanuda
cuando llega el evento.

Aquí se ve dónde espera cada paso de `dags/05_sensors_and_events.py`. Cada
columna es un sistema, como en el diagrama de la §9:

```
 Airflow: proceso de la task    │ Airflow: el triggerer  │ BigQuery
 Dag sensors_and_events         │ (§3)                   │ mlops_course
────────────────────────────────┼────────────────────────┼──────────────────
                                │                        │
 1 settle (deferrable) ──────────► espera 5 segundos     │
   se reanuda ◄─────────────────── dispara el evento     │
                                │                        │
 2 wait_for_table (deferrable) ──► cada 10 s pregunta ────► ¿existe ya la
                                │                        │  tabla?
   se reanuda ◄─────────────────── dispara el evento     │
                                │                        │
 3 enough_rows (reschedule)     │                        │
   un poke cada 10 s ─────────────────────────────────────► cuenta sus filas
   sale entre pokes             │                        │
 4 proceed: 500+ filas          │                        │
```

Mientras `settle` y `wait_for_table` esperan, la espera ocurre en el
triggerer, y no existe ningún proceso de la task. `enough_rows` hace sus
propias comprobaciones: un poke cada 10 segundos, sin proceso entre uno y
otro. Los números identifican las tasks, no el orden en que corren:
`enough_rows` arranca a la vez que `settle`, como muestra el cableado en
Constrúyelo.

### Constrúyelo

Crea `dags/05_sensors_and_events.py` con la cabecera y los dos sensors
escritos como operators, `settle` y `wait_for_table`:

```python dags/05_sensors_and_events.py
"""Lesson 5 - waiting for things: sensors, deferrable operators, and events."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
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
    settle = TimeDeltaSensor(task_id="settle", delta=timedelta(seconds=5), deferrable=True)

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

Los dos son deferrable (`deferrable=True`). `settle` espera cinco segundos. No
hace ningún trabajo útil: su trigger es el más simple, un temporizador, y es la
task que verás en `deferred` en Ejecútalo. `wait_for_table` espera a que exista una tabla de BigQuery: su trigger, que
corre en el triggerer, consulta a BigQuery cada `poke_interval` segundos a
través de la connection `google_cloud_default` que define el archivo de
compose (§9).

La tabla que hay que esperar la indica un **param del Dag**, `table`, con
`labeled_projects` por defecto, así que se puede apuntar un run a otra tabla
con `-c`, como harás en Ejecútalo. `table_id` es un campo con template (§6), así
que `{{ params.table }}` se renderiza cuando corre la task.

Un sensor también se puede escribir como función, con `@task.sensor`. El
tercer sensor, `enough_rows`, es así, en modo `reschedule` (la segunda de las
tres formas del principio de la §11). Añádelo, con la task que usa su valor, y
el cableado entre tasks:

```python dags/05_sensors_and_events.py
    @task.sensor(poke_interval=10, timeout=120, mode="reschedule")
    def enough_rows(*, params: dict[str, Any]) -> PokeReturnValue:
        from google.api_core.exceptions import NotFound

        try:
            rows = data.count_rows(params["table"])  # SELECT COUNT(*) in BigQuery
        except NotFound:
            return PokeReturnValue(is_done=False)
        print(f"{params['table']} has {rows} rows")
        return PokeReturnValue(is_done=rows >= 500, xcom_value=rows)

    @task
    def proceed(rows: int) -> None:
        print(f"Data is ready ({rows} rows). Downstream work can start.")

    settle >> wait_for_table >> proceed(enough_rows())


sensors_and_events()
```

Cada poke devuelve un `PokeReturnValue`. Con `is_done=True` los pokes se
detienen y `xcom_value` se entrega aguas abajo: `proceed` recibe el recuento
de filas. Mientras la tabla no existe, `data.count_rows` lanza el `NotFound` de
BigQuery, y el poke devuelve `is_done=False` en lugar de fallar.

Fíjate en el cableado: `enough_rows` no tiene ninguna task aguas arriba, así
que arranca enseguida, a la vez que `settle`, y `proceed` espera tanto a
`wait_for_table` como a `enough_rows`.

> Los sensors deferrable necesitan el triggerer (§3) corriendo. Sin él, se
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
cuenta las filas y supera su umbral en el primer poke. Y `settle` apenas
espera: sus cinco segundos se cuentan desde que se creó el run, no desde que
arranca la task, así que cuando corre ya ha pasado parte de ese tiempo, o todo.

**Para ver una task en estado `deferred`**, lanza el run a través del
scheduler, y luego ejecuta el último comando unas cuantas veces a lo largo de
los siguientes diez segundos. Sé rápido: un `docker compose exec` ya tarda uno
o dos segundos por sí solo.

```bash
docker compose exec airflow-scheduler airflow dags unpause sensors_and_events
docker compose exec airflow-scheduler airflow dags trigger sensors_and_events -r defer-demo   # pick a new id if you rerun
docker compose exec airflow-scheduler airflow tasks states-for-dag-run sensors_and_events defer-demo -o plain
```

**Comprobación.** `settle` aparece como `deferred` unos segundos (su proceso
ha terminado y el triggerer lleva la cuenta del temporizador) y pasa a
`success` unos cinco segundos después de lanzar el run. Ese `deferred` es el
estado de la §3, y es lo que se ve desde fuera cuando una task "libera el
worker slot".

**Ahora el caso interesante**: un sensor que sí tiene que esperar. Apunta
el run a una tabla que todavía no existe:

```bash
docker compose exec airflow-scheduler airflow dags test sensors_and_events -c '{"table": "labeled_projects_copy"}'
```

Esta vez el comando se queda esperando. `enough_rows` hace un poke, recibe
`NotFound` y pasa a `up_for_reschedule`, el estado que la §3 describe para un
sensor entre pokes; diez segundos después `airflow dags test` la vuelve a
ejecutar. `wait_for_table` queda en `deferred`, y su trigger consulta a
BigQuery cada diez segundos.

`airflow dags test` no tiene workers ni triggerer: ejecuta todas las tasks, y
el trigger de `wait_for_table`, en su propio proceso, y lo anuncia en el log
con `[DAG TEST] running trigger in line` (§4). Aquí no hay ningún worker slot
en juego. En un run
enviado a través del scheduler, como `defer-demo`, ningún sensor retendría un
worker slot mientras espera: `enough_rows` devuelve su slot entre pokes, y
`wait_for_table` espera en el triggerer. En el modo `poke` por defecto
ocuparían uno cada uno durante toda la espera, sin hacer nada.

Ahora crea la tabla. En la consola de Google Cloud, abre BigQuery, abre una
pestaña de consulta nueva y ejecuta, con tu Project ID:

```sql
CREATE TABLE `<your project id>.mlops_course.labeled_projects_copy` AS
SELECT * FROM `<your project id>.mlops_course.labeled_projects`
```

En menos de diez segundos los dos sensors ven la tabla y el run se completa,
terminando con `labeled_projects_copy has 764 rows`. Si no haces nada, cada
sensor acabará fallando por su `timeout` declarado: 120 segundos para
`enough_rows`, 300 para `wait_for_table`. El run solo falla cuando los dos se
han rendido, a los cinco minutos más o menos.

Borra la copia cuando acabes, en el mismo editor de consultas:

```sql
DROP TABLE `<your project id>.mlops_course.labeled_projects_copy`
```

### No esperar en absoluto: event-driven scheduling

Los tres sensors de `sensors_and_events` preguntan una y otra vez si algo ha
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

Igual que los sensors deferrable, los watchers corren en el triggerer: sin él,
un watcher no ve ningún mensaje, así que no arranca ningún run.

**Regla práctica:** segundos → el modo `poke` por defecto vale. Minutos u horas →
`mode="reschedule"` o deferrable, o estarás ocupando un worker slot solo para
dormir. Un servicio que avisa por sí mismo, enviando un mensaje a una cola → un
`AssetWatcher`, y ninguna task espera.

Referencia: [`solutions/05_sensors_and_events.py`](../../solutions/05_sensors_and_events.py).

---

← [10. El Dag de MLOps](10-el-dag-de-mlops.md) · [Índice](README.md) · [12. Continual learning: monitorizar, decidir, actuar](12-continual-learning-monitorizar-decidir-actuar.md) →
