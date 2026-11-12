## 13. Deadline alerts

Un **deadline** responde a "avísame si este run está tardando demasiado". Se
define en el Dag, cubre el run entero y se evalúa *mientras el run sigue en
marcha*, así que te enteras de un pipeline atascado durante el incidente, no a
la mañana siguiente.

### Constrúyelo

`dags/07_deadline_alerts.py` es el más corto de los siete; todo lo interesante
está en el decorador:

```python dags/07_deadline_alerts.py
"""Lesson 7 - deadline alerts: "tell me if this run is taking too long"."""

from __future__ import annotations

import time
from datetime import timedelta

import pendulum
from airflow.sdk import DeadlineAlert, DeadlineReference, SyncCallback, dag, task


@dag(
    dag_id="deadline_alerts",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "ml-engineering"},
    tags=["tutorial", "monitoring"],
    doc_md=__doc__,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(seconds=30),
        callback=SyncCallback(
            "project_tagger.callbacks.deadline_missed",
            kwargs={"dag_id": "deadline_alerts", "budget": "30s"},
        ),
    ),
)
def deadline_alerts():
```

Tres piezas: una **reference** (dónde arranca el reloj), un **interval** (cuánto
después) y un **callback**, la función que se llama con `kwargs` cuando vence
el plazo. El callback se da como ruta de import (`módulo.función`) porque corre
fuera de cualquier task, en otro proceso que lo importa. Pasa una lista a
`deadline=` para varios plazos.

References: `DAGRUN_QUEUED_AT`, `DAGRUN_LOGICAL_DATE`, `FIXED_DATETIME(dt)`,
`AVERAGE_RUNTIME(max_runs=10)`; la última significa "más lento que la media
reciente de este Dag", normalmente lo que quieres. Callbacks, cuya clase decide
qué proceso los ejecuta: `SyncCallback` (el Executor) o `AsyncCallback` (el
Triggerer; úsalo para Notifiers, los emisores de alertas ya hechos que los
providers traen para Slack, email y similares, p. ej.
`AsyncCallback(SlackWebhookNotifier, kwargs={...})`).

El Dag incumple su propio deadline a propósito:

```python dags/07_deadline_alerts.py
    @task
    def slow_step() -> str:
        time.sleep(75)
        return "finished, but late"

    slow_step()


deadline_alerts()
```

### Ejecútalo

```bash
# unpause first: the 30-second clock starts when the run is queued, i.e. at the trigger
docker compose exec airflow-scheduler airflow dags unpause deadline_alerts
docker compose exec airflow-scheduler airflow dags trigger deadline_alerts

# give it 40 seconds, then:
docker compose logs airflow-scheduler | grep "DEADLINE MISSED"
```

**Comprobación.** Una línea `DEADLINE MISSED` con el contexto del run, unos 30
segundos después del trigger, mientras `slow_step` (un sleep de 75 segundos)
sigue corriendo. Los
deadlines perdidos y pendientes también se listan bajo Browse → **Deadlines**.

> **Bug de Airflow 3.3.1 (la versión que instala este tutorial):** con un Dag
> que define `deadline=`, `airflow dags test` revienta al final: todas las
> tasks terminan bien, y en el momento en que marca el run como exitoso falla
> con `psycopg2.ProgrammingError: can't adapt type 'dict'` (lanzado desde
> `DagRun.update_state`). Los runs que dispara el Scheduler no tienen el
> problema, así que prueba esos Dags a través del Scheduler.
> [Detalles.](../troubleshooting.md)

Referencia: [`solutions/07_deadline_alerts.py`](../../solutions/07_deadline_alerts.py).

---

← [12. Continual learning: monitorizar, decidir, actuar](12-continual-learning-monitorizar-decidir-actuar.md) · [Índice](README.md) · [14. Cuando los datos fallan](14-cuando-los-datos-fallan.md) →
