## 12. Continual learning: monitor, decide, act

**Continual**, not continuous: the system repeats *with breaks*, and the breaks
are deliberate. A monitor fires, something is decided — by a person where it
matters — and then the pipeline runs. The approval gate this section builds,
`request_retraining`, is that decision, built into the Dag as a task.

What you are watching for is **drift**, in three flavours:

| Drift | What moved | Caught here by |
|---|---|---|
| **data drift** | the input distribution — the text itself | `mean_length_shift` in `monitoring.drift_report()` |
| **target drift** | the label distribution — one tag takes over, or a new tag appears | `label_tvd` (total variation distance) and `new_labels` |
| **concept drift** | the mapping itself — the same input now deserves a different label | only the live performance check: f1 falls while inputs look unchanged |

Concept drift is the one no input-side statistic can see, which is why
`monitoring.production_performance()` scores recently labelled rows instead of
trusting a distribution comparison alone.

A monitoring event has four sensible responses, and
`dags/06_monitoring_and_retraining.py` makes them branches:

| Response | When | What happens |
|---|---|---|
| **continue** | drift and performance within limits | nothing; keep serving |
| **improve** | f1 dropped or the input distribution moved | ask a human, then trigger `mlops` |
| **inspect** | an unseen label appeared | stop and page someone — the label space changed |
| **rollback** | performance collapsed | re-point production at the previous model |

### Build it

The workload: `monitoring.monitor()` returns a report with a `drift` dict
(`label_tvd`, `new_labels`, `current_distribution`, …) and a `performance`
dict (`f1_at_promotion`, `f1_now`) — or `performance=None` when nothing is
promoted; `registry.rollback()` re-promotes the previous model.

Create `dags/06_monitoring_and_retraining.py`. The thresholds are Dag
**params**, so the policy is visible in the UI, overridable per run, and
recorded with the run that used it:

```python dags/06_monitoring_and_retraining.py
"""Lesson 6 - continual learning: monitor, decide, and act."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task

from tagifai import monitoring, registry

DEFAULT_ARGS = {"owner": "ml-engineering", "retries": 1}

POLICY_PARAMS = {
    "max_label_tvd": Param(0.15, type="number", description="Label-drift budget."),
    "max_f1_drop": Param(0.10, type="number", description="f1 drop that triggers retraining."),
    "critical_f1_drop": Param(0.25, type="number", description="f1 drop that triggers rollback."),
}


@dag(
    dag_id="monitoring",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "mlops", "monitoring"],
    doc_md=__doc__,
    params=POLICY_PARAMS,
)
def monitoring_dag():
```

(The function is `monitoring_dag` because `monitoring` is already the name of
the imported module. The `dag_id` is what Airflow sees.)

Measure, then decide:

```python dags/06_monitoring_and_retraining.py
    @task
    def measure() -> dict[str, Any]:
        report = monitoring.monitor()
        drift = report["drift"]
        print(f"label TVD={drift['label_tvd']:.4f} new_labels={drift['new_labels']}")
        if report["performance"]:
            perf = report["performance"]
            print(f"f1 at promotion={perf['f1_at_promotion']:.4f} now={perf['f1_now']:.4f}")
        return report

    @task.branch
    def decide(report: dict[str, Any], **context) -> str:
        drift, performance = report["drift"], report["performance"]
        policy = context["params"]

        if performance is None:
            print("No production model yet - train one.")
            return "request_retraining"

        f1_drop = performance["f1_at_promotion"] - performance["f1_now"]
        print(f"f1_drop={f1_drop:.4f} label_tvd={drift['label_tvd']:.4f} policy={dict(policy)}")

        if f1_drop >= policy["critical_f1_drop"]:
            return "rollback"
        if drift["new_labels"]:
            return "inspect"
        if f1_drop >= policy["max_f1_drop"] or drift["label_tvd"] >= policy["max_label_tvd"]:
            return "request_retraining"
        return "continue_serving"
```

`context["params"]` is the policy — the defaults above, or whatever this run
was given. Note the ordering: **a new label routes to `inspect`, not to
`request_retraining`.** Retraining on data whose label space you have not understood yet is
how you ship a worse model with full confidence.

Now the four arms. Three are simple; the fourth is a **human-in-the-loop**
gate, `ApprovalOperator`, followed by the task that starts another Dag:

```python dags/06_monitoring_and_retraining.py
    continue_serving = EmptyOperator(task_id="continue_serving")

    @task
    def inspect(report: dict[str, Any]) -> None:
        print("MANUAL INSPECTION NEEDED")
        print(f"  new labels: {report['drift']['new_labels']}")
        print(f"  current distribution: {report['drift']['current_distribution']}")

    @task
    def rollback() -> dict[str, Any] | None:
        pointer = registry.rollback()
        print(f"rolled back to {pointer['run_id']}" if pointer else "nothing to roll back")
        return pointer

    request_retraining = ApprovalOperator(
        task_id="request_retraining",
        subject="Retrain the tag classifier?",
        body=(
            "Monitoring detected drift or a performance drop.\n\n"
            "Approve to trigger the `mlops` Dag on the current data."
        ),
        defaults="Approve",
        response_timeout=timedelta(seconds=60),
    )

    trigger_retraining = TriggerDagRunOperator(
        task_id="trigger_retraining",
        trigger_dag_id="mlops",
        conf={"reason": "monitoring_drift"},
        wait_for_completion=False,
        reset_dag_run=True,
    )
```

`ApprovalOperator` parks the task in `awaiting_input` and shows a prompt in the
UI, on the task instance's **Required Actions** tab and under Browse →
**Required Actions**. On **Reject**, everything
downstream is skipped. `defaults` is the answer applied if nobody responds within
`response_timeout` — sixty seconds here so the lesson finishes; give it hours
in production. `TriggerDagRunOperator` starts another Dag: it creates a
`manual__` run of `mlops` with the `conf` given, and with
`wait_for_completion=False` it does not stay around to watch it.

Finish with the wiring:

```python dags/06_monitoring_and_retraining.py
    report = measure()
    branch = decide(report)

    branch >> continue_serving
    branch >> inspect(report)
    branch >> rollback()
    branch >> request_retraining >> trigger_retraining


monitoring_dag()
```

Four arms out of one branch; `decide` returns the `task_id` of the one to run.
There is no join at the end, because nothing needs to run after all of them.

### Run it

Forcing the label-drift limit to zero guarantees `decide()` takes the
retraining branch, so you see the interesting path rather than whichever one
your current data happens to deserve:

```bash
docker compose exec airflow-scheduler \
    airflow dags test monitoring --conf '{"max_label_tvd": 0.0}'
```

> **This command takes about 70 seconds and looks frozen for 60 of them.** That
> is not a hang. `request_retraining` is the human-in-the-loop gate: it enters
> `awaiting_input` and waits out its full 60-second `response_timeout` before
> defaulting to Approve, because nobody is going to click the button. Let it
> sit.

**Checkpoint.** In order, with the minute-long wait between the second and
third lines:

```
Branch into request_retraining
Waiting for response                         ← state becomes awaiting_input
Approved. Proceeding with downstream tasks…  ← 60s later, the default resolves
Dag Run triggered successfully.  trigger_dag_id=mlops
```

That last line is real. Check for yourself:

```bash
docker compose exec airflow-scheduler airflow dags list-runs mlops -o plain
```

It now has a fresh `manual__…` run that this Dag created. One Dag decided
another Dag should run, and a person was given the chance to say no.

While it is in `awaiting_input`, open the run in the UI and click
`request_retraining`: the prompt is waiting for you on its **Required Actions**
tab (§5), and answering it there resolves
the task without waiting for the timeout. Airflow records the answer either way
— the choice that was made, when, and *by whom*. A run that timed out and fell
back to its `defaults` stores the choice with the "who" left empty, which is how
you tell "a person approved this" from "nobody was looking".

> **Without the `--conf` override** the outcome depends on state, which is the
> point of a monitor. If §10 promoted a model and the data has not moved,
> `decide()` returns `continue_serving`, the retraining arm is skipped, and the
> run is over in seconds. If there is no production model yet, `decide()` says
> so — `No production model yet - train one.` — and takes the retraining
> branch anyway, timeout included. Both are correct. Run it without `--conf`
> to see which branch your current state picks.

Reference: [`solutions/06_monitoring_and_retraining.py`](../../solutions/06_monitoring_and_retraining.py).

### What "retrain" leaves out

`trigger_retraining` fires `mlops`, which fits on whatever is in the warehouse.
That is the orchestration; it is not the whole retraining problem. Each of these
would be one more task in a real version of this Dag:

| Step | Why it is not automatic |
|---|---|
| **labeling** | the new rows need real labels, not the model's own predictions fed back as truth |
| **active learning** | label the rows that would teach the model most — you rarely have budget for all of them |
| **QA** | audit the labels before training on them: false positives, labeller disagreement, weak slices |
| **augmentation** | expand the new data so a small window is not drowned out by history |
| **sampling** | up/downsample to fix the imbalance the drift introduced |
| **evaluation** | build the held-out set to reflect *deployment* conditions |

The other constraint is **catastrophic forgetting**: fitting only on the new
window makes the model forget patterns it used to get right, so it fixes
today's errors and brings back old ones. The Dag here sidesteps it by always
training on the full labelled table — fine at this size, and the first thing to
revisit when the table stops fitting in a task.

Which is also why the promotion gate in §10 compares against the current
production model instead of a fixed threshold alone: a retrained model that is
worse than what is serving should not ship just because a monitor fired.

---

← [11. Waiting: sensors, deferrable tasks, events](11-waiting-sensors-deferrable-tasks-events.md) · [Index](README.md) · [13. Deadline alerts](13-deadline-alerts.md) →
