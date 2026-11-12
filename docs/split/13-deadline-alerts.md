## 13. Deadline alerts

A **deadline** answers "tell me if this run is taking too long". It is set on
the Dag, covers the whole run, and is checked *while the run is still going*,
so you hear about a stuck pipeline during the incident, not the next morning.

### Build it

`dags/07_deadline_alerts.py` is the shortest of the seven; everything
interesting is in the decorator:

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

Three pieces: a **reference** (where the clock starts), an **interval** (how
long after it), and a **callback**, the function called with `kwargs` when the
deadline passes. The callback is given as an import path (`module.function`)
because it runs outside any task, in another process that imports it. Pass a
list to `deadline=` for several deadlines.

References: `DAGRUN_QUEUED_AT`, `DAGRUN_LOGICAL_DATE`, `FIXED_DATETIME(dt)`,
`AVERAGE_RUNTIME(max_runs=10)`; the last means "slower than this Dag's recent
average", usually what you want. Callbacks, whose class decides which process
runs them: `SyncCallback` (the Executor) or `AsyncCallback` (the Triggerer;
use it for Notifiers, the ready-made alert senders providers ship for Slack,
email and the like, e.g. `AsyncCallback(SlackWebhookNotifier, kwargs={...})`).

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

**Checkpoint.** One `DEADLINE MISSED` line with the run's context, about 30
seconds after the trigger, while `slow_step` (a 75-second sleep) is still
running. Missed and pending
deadlines are also listed under Browse → **Deadlines**.

> **Airflow 3.3.1 bug (the version this tutorial installs):** on a Dag that sets
> `deadline=`, `airflow dags test` crashes at the very end: every task finishes
> fine, and the moment it marks the run successful it fails with
> `psycopg2.ProgrammingError: can't adapt type 'dict'` (out of
> `DagRun.update_state`). Runs the Scheduler starts are not affected, so test
> such Dags through the Scheduler.
> [Details.](../troubleshooting.md)

Reference: [`solutions/07_deadline_alerts.py`](../../solutions/07_deadline_alerts.py).

---

← [12. Continual learning: monitor, decide, act](12-continual-learning-monitor-decide-act.md) · [Index](README.md) · [14. When the data goes wrong](14-when-the-data-goes-wrong.md) →
