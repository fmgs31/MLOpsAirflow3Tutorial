## 11. Esperar: sensors, deferrable tasks, eventos

`dags/05_sensors_and_events.py`. Tres formas de esperar al mundo exterior, de
más a menos worker slots consumidos — y, después de ellas, la única forma de
no esperar en absoluto.

Un **sensor** es una task cuyo único trabajo es esperar a que algo sea verdad:
que exista un archivo, que una tabla tenga filas, que responda una API. Cada
comprobación se llama **poke**. Si el poke da verdadero, la task termina con
éxito; si no, espera `poke_interval` segundos y vuelve a comprobar; si pasa
`timeout` sin éxito, falla. Las tres formas se distinguen por lo que ocurre *entre*
pokes:

```python
# 1. Poking sensor — holds a worker slot the entire time. Seconds, not hours.
BigQueryTableExistenceSensor(task_id="wait", project_id=..., dataset_id=..., table_id=...,
                             poke_interval=10, timeout=300)

# 2. `reschedule` mode — the task instance exits between pokes and is re-queued.
BigQueryTableExistenceSensor(..., mode="reschedule")

# 3. Deferrable — suspends onto the triggerer and frees the slot entirely.
BigQueryTableExistenceSensor(..., deferrable=True)
```

Con el modo por defecto, `mode="poke"`, el proceso sigue
vivo y dormido, ocupando su worker slot. Con `mode="reschedule"`, la task
instance termina su proceso, queda en `up_for_reschedule` (§3) y el scheduler la vuelve a
encolar para el siguiente poke.

La tercera opción, deferrable, necesita aclarar antes la palabra **trigger**,
porque tiene más de un significado. Tú *disparas* (trigger) un Dag (§7): un verbo. El
proceso triggerer de la §3 ejecuta **triggers**: sustantivos — pequeños fragmentos de
código asíncrono que esperan a algo (un momento concreto, una tabla, un
mensaje) y lanzan un evento cuando ocurre. "Deferrable" significa que la task le
entrega al triggerer uno de esos triggers, termina su proceso, y se reanuda
cuando llega el evento. `TriggerDagRunOperator` (§12) vuelve a ser el verbo.

Aquí se ve dónde espera cada paso de `dags/05_sensors_and_events.py`. Cada
columna es un sistema, como en el diagrama de la §9; las dos primeras son
partes de Airflow:

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
   cada 10 s, un intento ─────────────────────────────────► cuenta sus filas
   sale entre intentos          │                        │
 4 proceed: 500+ filas          │                        │
```

Mientras `settle` y `wait_for_table` esperan, la espera ocurre en el
triggerer, y no existe ningún proceso de la task. `enough_rows` hace sus
propias comprobaciones: un intento nuevo cada 10 segundos, sin proceso entre
uno y otro.

### Constrúyelo

Crea `dags/05_sensors_and_events.py` con la cabecera y los dos sensors que son
operators, `settle` y `wait_for_table`:

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

Los dos son de tipo deferrable. `settle` espera cinco segundos.
`wait_for_table` espera a que exista una tabla de BigQuery: su trigger, que
corre en el triggerer, consulta a BigQuery cada `poke_interval` segundos a
través de la connection `google_cloud_default` que define el archivo de
compose (§9).

La tabla que hay que esperar la indica un **param del Dag**, `table`, con
`labeled_projects` por defecto, así que se puede apuntar un run a otra tabla
con `-c`, como harás más abajo. `table_id` es un campo con template (§6), así que
`{{ params.table }}` se renderiza cuando corre la task.

Un sensor también se puede escribir como función. Añádelo, con la task que usa su
valor, y las conexiones entre tasks:

```python dags/05_sensors_and_events.py
    @task.sensor(poke_interval=10, timeout=120, mode="reschedule")
    def enough_rows(*, params: dict[str, Any]) -> PokeReturnValue:
        from google.api_core.exceptions import NotFound

        try:
            rows = data.count_rows(params["table"])
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

`PokeReturnValue(is_done=..., xcom_value=...)` detiene los pokes y a la vez entrega
un valor aguas abajo — `proceed` recibe el recuento de filas. `data.count_rows`
ejecuta un `SELECT COUNT(*)` en BigQuery; mientras la tabla no existe, BigQuery
responde `NotFound`, y el poke indica que aún no ha terminado, en lugar de
fallar. `enough_rows` está en modo `reschedule`, la segunda opción de la lista con
que empieza la §11.

Fíjate en las conexiones: `enough_rows` no tiene ninguna task aguas arriba, así que
arranca enseguida, a la vez que `settle`, y `proceed` espera tanto a
`wait_for_table` como a `enough_rows`.

Una opción más elimina la espera del todo: el **event-driven scheduling**. No va
en el archivo, porque necesita una cola de mensajes (como Amazon SQS) que no
tienes. Asocia a un asset un *watcher* (un trigger que vigila la cola) y cada
mensaje que llega arranca un run; nada hace polling, nada ocupa un slot:

```python
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher

trigger = MessageQueueTrigger(queue="https://sqs.us-east-1.amazonaws.com/.../new-projects")
new_projects = Asset(name="new_projects",
                     watchers=[AssetWatcher(name="sqs_watcher", trigger=trigger)])

@dag(schedule=[new_projects])
def ingest_on_message(): ...
```

Solo los triggers que derivan de `BaseEventTrigger` pueden ser watchers — un
trigger que comprueba una condición permanente ("la tabla existe") se
dispararía una y otra vez para siempre.

> Tanto los sensors deferrable como los watchers necesitan `airflow triggerer`
> corriendo. Sin él, un sensor deferrable se queda en `deferred`
> indefinidamente, y un watcher no ve ningún mensaje, así que no arranca ningún
> run.

### Ejecútalo

Este Dag espera la tabla que construye `dataops`, así que la §9 tiene que haber
corrido o los sensors harán polling hasta agotar su timeout:

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
espera: su objetivo de cinco segundos se mide desde el `data_interval_end`
del run (o su `run_after`), que `airflow dags test` fija al arrancar, así que
cuando la task corre ya ha pasado parte de ese tiempo, o todo. Si queda algo, no
hay triggerer al que pasarle la espera: `airflow dags test` ejecuta el trigger él
mismo, y lo anuncia en el log con `[DAG TEST] running trigger in line` (§4).

**Para ver de verdad una task en estado `deferred`**, lanza el run a través del scheduler, y luego
lee los estados de las tasks unas cuantas veces a lo largo de los siguientes diez
segundos:

```bash
docker compose exec airflow-scheduler airflow dags unpause sensors_and_events
docker compose exec airflow-scheduler airflow dags trigger sensors_and_events -r defer-demo   # pick a new id if you rerun
docker compose exec airflow-scheduler airflow tasks states-for-dag-run sensors_and_events defer-demo -o plain
```

**Comprobación.** `settle` aparece como `deferred` unos segundos — su proceso ha
terminado y el triggerer lleva la cuenta del temporizador — y pasa a `success`
unos cinco segundos después del trigger. Ese
`deferred` es el estado de la §3, y es lo que se ve desde fuera cuando una task
"libera el worker slot". Sé rápido con el último comando; un `docker compose exec`
ya tarda uno o dos segundos por sí solo.

**Ahora mira el caso interesante** — un sensor que sí tiene que esperar. Apunta
el run a una tabla que todavía no existe:

```bash
docker compose exec airflow-scheduler airflow dags test sensors_and_events -c '{"table": "labeled_projects_copy"}'
```

Esta vez el comando se queda esperando. `enough_rows` hace un poke, recibe
`NotFound`, y su task instance **termina su proceso** en vez de retener un
slot: pasa a `up_for_reschedule`, el estado que la §3 describe para un sensor
entre pokes, y el run la vuelve a encolar diez segundos después.
`wait_for_table` queda en `deferred`, y su trigger consulta a BigQuery cada
diez segundos.

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

Mientras esperaban, ningún sensor retuvo un worker slot: `enough_rows` no
ocupaba ninguno entre pokes, y `wait_for_table` estaba en el triggerer. En el
modo por defecto habrían ocupado uno cada uno durante toda la espera, sin
hacer nada.

Borra la copia cuando acabes, en el mismo editor de consultas:

```sql
DROP TABLE `<your project id>.mlops_course.labeled_projects_copy`
```

**Regla práctica:** segundos → un poking sensor vale. Minutos u horas →
`mode="reschedule"` o deferrable, o estarás ocupando un worker slot solo para dormir. Un servicio que avisa
por sí mismo, enviando un mensaje a una cola → un `AssetWatcher`, y ninguna
task espera.

Referencia: [`solutions/05_sensors_and_events.py`](../../solutions/05_sensors_and_events.py).

---

← [10. El Dag de MLOps](10-el-dag-de-mlops.md) · [Índice](README.md) · [12. Continual learning: monitorizar, decidir, actuar](12-continual-learning-monitorizar-decidir-actuar.md) →
