## 13. Deadline alerts

Un **deadline** responde a "avísame si este run está tardando demasiado". Se
adjunta al Dag, se aplica al run entero, y se evalúa *mientras el run sigue en
marcha* — así que te enteras de un pipeline atascado durante el incidente, no en
el informe de la mañana.

### Constrúyelo

`dags/07_deadline_alerts.py` es el archivo más corto de los siete, y todo él es
el decorador:

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
            "tagifai.callbacks.deadline_missed",
            kwargs={"dag_id": "deadline_alerts", "budget": "30s"},
        ),
    ),
)
def deadline_alerts():
```

Tres piezas: una **reference** (dónde arranca el reloj), un **interval** (cuánto
después) y un **callback** — nombrado como ruta con puntos, porque corre en
algún sitio que no es la task, con `kwargs` que se le pasan. Pasa una lista a
`deadline=` para varios presupuestos.

References: `DAGRUN_QUEUED_AT`, `DAGRUN_LOGICAL_DATE`, `FIXED_DATETIME(dt)`,
`AVERAGE_RUNTIME(max_runs=10)` — esta última es "más lento que la propia media
reciente de este Dag", que suele ser lo que de verdad quieres decir. Callbacks:
`SyncCallback` (corre en el executor) o `AsyncCallback` (corre en el triggerer —
úsalo para Notifiers, p. ej. `AsyncCallback(SlackWebhookNotifier, kwargs={...})`).

El Dag se pasa su propio deadline a propósito:

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

**Comprobación.** Una línea `DEADLINE MISSED`, unos 30 segundos después del
trigger, con el contexto del run — y `slow_step` sigue corriendo cuando dispara,
porque esa task duerme 75 segundos. Ese es el objetivo: te enteraste del run
lento antes de que terminara. Los deadlines perdidos y pendientes también se
listan bajo Browse → **Deadlines**.

> **Problema conocido en 3.3.1:** `airflow dags test` revienta con cualquier Dag
> que tenga un `deadline=` (`psycopg2.ProgrammingError: can't adapt type 'dict'`
> saliendo de `DagRun.update_state`). Las tasks corren bien; lo único que falla
> es la actualización final de estado de la CLI. Dispara esos Dags por el
> scheduler en su lugar — que es por lo que `mlops` no lleva deadline, ya que
> rompería la instrucción `airflow dags test mlops` de más arriba.
> [Detalles.](../troubleshooting.md)

Referencia: [`solutions/07_deadline_alerts.py`](../../solutions/07_deadline_alerts.py).

---

← [12. Continual learning: monitorizar, decidir, actuar](12-continual-learning-monitorizar-decidir-actuar.md) · [Índice](README.md) · [14. Testear Dags](14-testear-dags.md) →
