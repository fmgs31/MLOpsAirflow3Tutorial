## 13. Deadline alerts

A **deadline** answers "tell me if this run is taking too long". It is attached
to the Dag, applies to the whole run, and is evaluated *while the run is still
going* — so you hear about a stuck pipeline during the incident, not in the
morning report.

### Build it

`dags/07_deadline_alerts.py` is the shortest file of the seven, and the
interesting part is all in the decorator:

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

Three pieces: a **reference** (where the clock starts), an **interval** (how
long after it), and a **callback**, the function called when the deadline
passes, with the `kwargs` passed to it. The callback is named by its import
path (`module.function`) because it does not run inside any task: another
process imports and runs it, the one its type says (below). Pass a list to
`deadline=` for several deadlines.

References: `DAGRUN_QUEUED_AT`, `DAGRUN_LOGICAL_DATE`, `FIXED_DATETIME(dt)`,
`AVERAGE_RUNTIME(max_runs=10)` — that last one is "slower than this Dag's own
recent average", which is usually what you actually mean. Callbacks:
`SyncCallback` (runs on the executor) or `AsyncCallback` (runs on the triggerer —
use it for Notifiers, the ready-made alert senders that providers ship for
Slack, email and the like, e.g. `AsyncCallback(SlackWebhookNotifier, kwargs={...})`).

The Dag deliberately misses its own deadline:

```python dags/07_deadline_alerts.py
    @task
    def slow_step() -> str:
        time.sleep(75)
        return "finished, but late"

    slow_step()


deadline_alerts()
```

### Run it

```bash
# unpause first: the 30-second clock starts when the run is queued, i.e. at the trigger
docker compose exec airflow-scheduler airflow dags unpause deadline_alerts
docker compose exec airflow-scheduler airflow dags trigger deadline_alerts

# give it 40 seconds, then:
docker compose logs airflow-scheduler | grep "DEADLINE MISSED"
```

**Checkpoint.** One `DEADLINE MISSED` line, about 30 seconds after the trigger,
carrying the run's context — and `slow_step` is still running when it fires,
because that task sleeps for 75 seconds. That is the point: you heard about the
slow run before it finished. Missed and pending deadlines are also listed under
Browse → **Deadlines**.

> **Airflow 3.3.1 bug (the version this tutorial installs):** `airflow dags test`
> crashes on any Dag that sets `deadline=`
> (`psycopg2.ProgrammingError: can't adapt type 'dict'` out of
> `DagRun.update_state`). The tasks run fine; only the CLI's final state update
> fails. Trigger such Dags through the scheduler instead — which is why `mlops`
> carries no deadline, since it would break the `airflow dags test mlops`
> step in §10. [Details.](../troubleshooting.md)

Reference: [`solutions/07_deadline_alerts.py`](../../solutions/07_deadline_alerts.py).

---

← [12. Continual learning: monitor, decide, act](12-continual-learning-monitor-decide-act.md) · [Index](README.md) · [14. Testing Dags](14-testing-dags.md) →
