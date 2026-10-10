## 11. Esperar: sensors, deferrable tasks, eventos

`dags/05_sensors_and_events.py`. Tres formas de esperar al mundo exterior, la
que menos worker slots gasta la última — y, después de ellas, la única forma de
no esperar en absoluto.

Un **sensor** es una task cuyo único trabajo es esperar a que algo sea verdad:
que exista un archivo, que una tabla tenga filas, que responda una API. Cada
comprobación se llama **poke**. Si el poke da verdadero, la task termina con
éxito; si no, espera `poke_interval` segundos y vuelve a comprobar; si pasa
`timeout` sin éxito, falla. Las tres formas se distinguen en qué ocurre *entre*
pokes:

```python
# 1. Poking sensor — holds a worker slot the entire time. Seconds, not hours.
FileSensor(task_id="wait", filepath=..., poke_interval=10, timeout=300)

# 2. `reschedule` mode — the task instance exits between pokes and is re-queued.
FileSensor(..., mode="reschedule")

# 3. Deferrable — suspends onto the triggerer and frees the slot entirely.
TimeDeltaSensorAsync(task_id="settle", delta=timedelta(seconds=5))
```

Con el modo por defecto, `mode="poke"`, el proceso sigue
vivo y dormido, ocupando su worker slot. Con `mode="reschedule"`, la task
instance sale, queda en `up_for_reschedule` (§3) y el scheduler la vuelve a
encolar para el siguiente poke. Un sensor deferrable no espera en el worker: le
pasa la espera al triggerer.

La palabra **trigger** tiene más de un significado, así que conviene aclararla
antes de que la tercera opción pueda tener sentido. Tú *disparas* (trigger) un Dag (§7): un verbo. El
proceso triggerer de la §3 ejecuta **triggers**: sustantivos — pequeños trozos de
código asíncrono que esperan a una cosa (un momento en el tiempo, un archivo, un
mensaje) y lanzan un evento cuando ocurre. "Deferrable" significa que la task le
entrega al triggerer uno de esos triggers, sale, y se reanuda cuando llega el
evento. `TriggerDagRunOperator` (§12) vuelve a ser el verbo.

### Constrúyelo

Crea `dags/05_sensors_and_events.py` con la cabecera y los dos sensors de tipo
operator:

```python dags/05_sensors_and_events.py
"""Lesson 5 - waiting for things: sensors, deferrable operators, and events."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensorAsync
from airflow.sdk import PokeReturnValue, dag, task

from tagifai import config, data

DEFAULT_ARGS = {"owner": "data-engineering", "retries": 0}


@dag(
    dag_id="sensors_and_events",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "sensors"],
    doc_md=__doc__,
)
def sensors_and_events():
    settle = TimeDeltaSensorAsync(task_id="settle", delta=timedelta(seconds=5))

    wait_for_curated_data = FileSensor(
        task_id="wait_for_curated_data",
        filepath=str(config.LABELED_PROJECTS_FP),
        poke_interval=10,
        timeout=60 * 5,
        mode="reschedule",
    )
```

`settle` es deferrable: libera su worker slot mientras espera, lo que necesita
`airflow triggerer` corriendo. `wait_for_curated_data` vigila el archivo que
escribe `dataops`, a través de la connection `fs_default` que aporta el archivo
de compose. `mode="reschedule"` es la forma no-deferrable de liberar un slot: la
task instance sale y se vuelve a encolar cada `poke_interval` segundos, y se
rinde pasado `timeout`.

Un sensor también se puede escribir como función. Añádelo, con la task que usa su
valor, y el cableado:

```python dags/05_sensors_and_events.py
    @task.sensor(poke_interval=10, timeout=120, mode="reschedule")
    def enough_rows() -> PokeReturnValue:
        if not config.LABELED_PROJECTS_FP.exists():
            return PokeReturnValue(is_done=False)
        rows = len(data.load_labeled())
        print(f"curated table has {rows} rows")
        return PokeReturnValue(is_done=rows >= 500, xcom_value=rows)

    @task
    def proceed(rows: int) -> None:
        print(f"Data is ready ({rows} rows). Downstream work can start.")

    settle >> wait_for_curated_data >> proceed(enough_rows())


sensors_and_events()
```

`PokeReturnValue(is_done=..., xcom_value=...)` detiene los pokes y a la vez entrega
un valor aguas abajo — `proceed` recibe el recuento de filas. La tabla se lee con
pandas en vez de contar líneas, porque las descripciones contienen saltos de
línea.

Lee el cableado: `enough_rows` no tiene ninguna task aguas arriba, así que
arranca enseguida, a la vez que `settle`, y `proceed` espera tanto a
`wait_for_curated_data` como a `enough_rows`.

Una opción más elimina la espera del todo: el **event-driven scheduling**. No va
en el archivo, porque necesita una cola de mensajes (como Amazon SQS) que no
tienes. Engancha a un asset un *watcher* (un trigger que vigila la cola) y cada
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
trigger que comprueba una condición permanente ("el archivo existe") se
dispararía una y otra vez para siempre.

> Tanto los sensors deferrable como los watchers necesitan `airflow triggerer`
> corriendo. Sin él se quedan en `deferred` indefinidamente.

### Ejecútalo

Este Dag espera el archivo que produjo `dataops`, así que la §9 tiene que haber
corrido o los sensors harán polling hasta agotar su timeout:

```bash
docker compose exec airflow-scheduler airflow dags test sensors_and_events
```

**Comprobación.** Termina en unos segundos, porque todo lo que espera ya ha
ocurrido. En el log:

```
curated table has 764 rows
Poking for file /opt/airflow/data/warehouse/labeled_projects.csv
Success criteria met. Exiting.
Data is ready (764 rows). Downstream work can start.
```

`wait_for_curated_data` hace un poke, encuentra el archivo y sale; `enough_rows`
cuenta las filas y supera su umbral en el primer poke. Y `settle`, el deferrable,
vuelve de inmediato sin diferirse en absoluto: su objetivo de cinco segundos se
mide desde el `data_interval_end` del run, que `airflow dags test` fijó antes de
que la task arrancara, así que para cuando la task corrió el momento ya había pasado.
Nada de este run tocó el triggerer.

**Para ver una task diferirse de verdad**, manda el run por el scheduler, y luego
lee los estados de las tasks unas cuantas veces a lo largo de los siguientes diez
segundos:

```bash
docker compose exec airflow-scheduler airflow dags unpause sensors_and_events
docker compose exec airflow-scheduler airflow dags trigger sensors_and_events -r defer-demo   # pick a new id if you rerun
docker compose exec airflow-scheduler airflow tasks states-for-dag-run sensors_and_events defer-demo -o plain
```

**Comprobación.** `settle` marca `deferred` durante unos cinco segundos — su
proceso ha salido y el triggerer sostiene el temporizador — y luego `success`,
con un `end_date` algo más de cinco segundos después de su `start_date`. Ese
`deferred` es el estado de la §3, y es el aspecto que tiene, visto desde fuera,
"liberar el worker slot". Sé rápido con el último comando; un `docker compose exec`
se lleva uno o dos segundos por su cuenta.

**Ahora mira el caso interesante** — un sensor que sí tiene que esperar. Aparta
el archivo y ejecuta el Dag otra vez:

```bash
# Git Bash needs the MSYS_NO_PATHCONV prefix on any /opt/... path (§2)
MSYS_NO_PATHCONV=1 docker compose exec airflow-scheduler \
    mv /opt/airflow/data/warehouse/labeled_projects.csv /tmp/labeled_projects.csv

docker compose exec airflow-scheduler airflow dags test sensors_and_events
```

Esta vez el comando no termina. Ambos sensors están en `mode="reschedule"`, así que sus task
instances **salen entre pokes** en vez de retener un slot, y pasan a
`up_for_reschedule` — el estado que la §3 da para un sensor entre pokes. El run lo
anuncia cada segundo:

```
No tasks to run. unrunnable tasks: {<TaskInstance: sensors_and_events.enough_rows ... [up_for_reschedule]>,
                                    <TaskInstance: sensors_and_events.wait_for_curated_data ... [up_for_reschedule]>}
```

En una **segunda terminal**, devuelve el archivo a su sitio:

```bash
MSYS_NO_PATHCONV=1 docker compose exec airflow-scheduler \
    mv /tmp/labeled_projects.csv /opt/airflow/data/warehouse/labeled_projects.csv
```

El siguiente poke lo encuentra y el run se completa. Déjalo movido y cada sensor
acabará fallando por su `timeout` declarado: 120 segundos para `enough_rows`,
300 para `wait_for_curated_data`. El run solo falla cuando los dos se han
rendido, a los cinco minutos más o menos.

Lo importante es lo que *no* estaba pasando durante la espera: la task instance
no estaba corriendo entre pokes — estaba en `up_for_reschedule`, como has
visto — así que no se retuvo ningún worker slot. Un poking sensor en el modo por
defecto habría ocupado un slot todo ese tiempo sin hacer nada, y esa es
precisamente la razón por la que existen los otros dos modos.

**Regla práctica:** segundos → un poking sensor vale. Minutos u horas →
deferrable, o estás pagando un worker slot para dormir. Un servicio que avisa
por sí mismo, enviando un mensaje a una cola → un `AssetWatcher`, y ninguna
task espera.

Referencia: [`solutions/05_sensors_and_events.py`](../../solutions/05_sensors_and_events.py).

---

← [10. El Dag de MLOps](10-el-dag-de-mlops.md) · [Índice](README.md) · [12. Continual learning: monitorizar, decidir, actuar](12-continual-learning-monitorizar-decidir-actuar.md) →
