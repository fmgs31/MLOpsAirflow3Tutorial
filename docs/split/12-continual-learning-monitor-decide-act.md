## 12. Continual learning: monitor, decide, act

**Continual**, not continuous: the system repeats *with breaks*, and the breaks
are deliberate. A monitor fires, something is decided — by a person where it
matters — and then the pipeline runs. The approval gate this section builds,
`request_retraining`, is that decision, built into the Dag as a task.

What you are watching for is **drift**, in three flavours:

| Drift | What moved | Caught here by |
|---|---|---|
| **data drift** | the input distribution — the text itself | `length_drift`: a Kolmogorov–Smirnov test on the number of words per text |
| **target drift** | the label distribution — one tag takes over, or a new tag appears | `label_drift`: a chi-squared test on the tag counts, and `new_labels` |
| **concept drift** | the relation between input and label — the same input now deserves a different label | only the live performance check, `monitoring.production_performance()`, which scores the production model on recently labelled rows: f1 falls while inputs look unchanged |

The two tests come from **alibi-detect**, a drift-detection library. Each takes
a **reference** window (the older projects, what the model was built on) and a
**current** window (the newest quarter, by date added), and answers with a
**p-value**: the probability of seeing a difference at least this large if
nothing had really changed. A small p-value means the difference is unlikely
to be chance. Below a threshold you choose, conventionally 0.05, the test
reports drift. The Kolmogorov–Smirnov (KS) test compares two distributions of a
number; the chi-squared test compares two sets of category counts.

A monitoring event has four sensible responses, and
`dags/06_monitoring_and_retraining.py` makes them branches:

| Response | When | What happens |
|---|---|---|
| **continue** | drift and performance within limits | nothing; keep serving |
| **improve** | f1 dropped, or the input or label distribution moved | ask a human, then trigger `mlops` |
| **inspect** | an unseen label appeared | stop and page someone — the label space changed |
| **rollback** | performance collapsed | re-point production at the previous model |

How the pieces talk to each other when the Dag runs. Each column is one
system, as in the §9 diagram:

```
 Airflow (Docker)               │ BigQuery          │ MLflow (Docker)
 monitoring Dag                 │ mlops_course      │ localhost:5000
────────────────────────────────┼───────────────────┼───────────────────────
                                │                   │
 1 prepare_windows ◄────────────── labeled_projects │
   writes reference.csv and     │                   │
   current.csv in data/         │                   │
 2 detect_drift (alibi-detect,  │                   │
   in its own virtualenv)       │                   │
   reads the two files          │                   │
 3 check_performance ◄──────────────────────────────── the champion model
   scores it on current.csv     │                   │
 4 decide: one branch of        │                   │
   continue_serving             │                   │
   inspect                      │                   │
   rollback ─────────────────────────────────────────► moves champion back
   request_retraining:          │                   │  to previous_champion
     a person approves in       │                   │
     the UI, then               │                   │
     trigger_retraining         │                   │
       │ starts a run of        │                   │
       ▼                        │                   │
     mlops Dag (§10)            │                   │
```

### Build it

The workload: `monitoring.prepare_windows(key)` reads `labeled_projects`,
splits it into the two windows and writes each to a file, returning their
paths; `drift.detect_drift(reference_fp, current_fp, p_val)` runs both tests
and returns `label_drift` and `length_drift` (each with `is_drift`, `p_val` and
`distance`), `new_labels` and the two tag distributions;
`monitoring.production_performance(current_fp)` scores the champion on the
current window and returns `f1_at_promotion` and `f1_now`, or `None` when
nothing is promoted; `registry.rollback()` moves `champion` back to
`previous_champion` (§10).

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

from project_tagger import config, monitoring, registry

DEFAULT_ARGS = {"owner": "ml-engineering", "retries": 1}

POLICY_PARAMS = {
    "drift_p_val": Param(
        0.05, type="number", description="A drift test with a p-value below this reports drift."
    ),
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

The drift task, `detect_drift`, runs in **another Python**. alibi-detect needs `numpy<2`, which
Airflow's own environment cannot have, so the image has a second virtualenv
just for it, `/opt/venvs/alibi`, and `config.ALIBI_PYTHON` is its interpreter.
`@task.external_python` sends the function there: Airflow writes the
function's source and its arguments to files, runs that interpreter on them in
a subprocess, and reads the return value back. That has consequences you will
see in the code below:

- Everything the function uses is **imported inside it**. The module-level
  imports of this file do not exist in that other Python.
- Arguments and return value must be **plain data**: they are pickled to cross
  the process boundary. Hence paths in, a dict of numbers out; never a
  dataframe.
- `expect_airflow=False`, because that virtualenv has no Airflow. The function
  gets no context either. The threshold arrives as a template,
  `"{{ params.drift_p_val }}"`, rendered before the subprocess starts, so it
  arrives as a string, and `float()` converts it.

This is the light version of what §15 recommends for teams with clashing
dependencies: one container image per task, with `KubernetesPodOperator`.

Measure first. The windows are written once; the drift tests and the
performance check both read them:

```python dags/06_monitoring_and_retraining.py
    @task
    def prepare_windows(*, run_id: str) -> dict[str, str]:
        key = run_id.replace(":", "-").replace("+", "-")
        windows = monitoring.prepare_windows(f"monitoring-{key}")
        print(f"windows: {windows}")
        return windows

    @task.external_python(python=config.ALIBI_PYTHON, expect_airflow=False)
    def detect_drift(windows: dict, p_val: str) -> dict:
        from project_tagger import drift

        return drift.detect_drift(windows["reference"], windows["current"], float(p_val))

    @task
    def check_performance(windows: dict[str, str]) -> dict[str, Any] | None:
        performance = monitoring.production_performance(windows["current"])
        if performance:
            print(f"f1 at promotion={performance['f1_at_promotion']:.4f} now={performance['f1_now']:.4f}")
        return performance
```

Then decide:

```python dags/06_monitoring_and_retraining.py
    @task.branch
    def decide(
        drift: dict[str, Any], performance: dict[str, Any] | None, *, params: dict[str, Any]
    ) -> str:
        label, length = drift["label_drift"], drift["length_drift"]
        print(
            f"label drift p={label['p_val']:.4g} length drift p={length['p_val']:.4g} "
            f"new_labels={drift['new_labels']} policy={dict(params)}"
        )

        if performance is None:
            print("No production model yet - train one.")
            return "request_retraining"

        f1_drop = performance["f1_at_promotion"] - performance["f1_now"]
        print(f"f1_drop={f1_drop:.4f}")

        if f1_drop >= params["critical_f1_drop"]:
            return "rollback"
        if drift["new_labels"]:
            return "inspect"
        if (
            f1_drop >= params["max_f1_drop"]                    # concept drift
            or label["p_val"] < params["drift_p_val"]           # target drift
            or length["p_val"] < params["drift_p_val"]          # data drift
        ):
            return "request_retraining"
        return "continue_serving"
```

`params` is the policy — the defaults in `POLICY_PARAMS`, or whatever this run
was given. Each retraining condition watches one of the three kinds of drift
in the table. Note the ordering: **a new label routes to `inspect`, not to
`request_retraining`.** Retraining on data whose label space you have not understood yet is
how you ship a worse model with full confidence.

Now the four branches `decide` can pick. Three are simple; the fourth is a **human-in-the-loop**
gate (a task that waits for a person to answer), `ApprovalOperator`, followed
by the task that starts another Dag:

```python dags/06_monitoring_and_retraining.py
    continue_serving = EmptyOperator(task_id="continue_serving")

    @task
    def inspect(drift: dict[str, Any]) -> None:
        print("MANUAL INSPECTION NEEDED")
        print(f"  new labels: {drift['new_labels']}")
        print(f"  current distribution: {drift['current_distribution']}")

    @task
    def rollback() -> dict[str, Any] | None:
        champion = registry.rollback()
        print(f"champion -> version {champion['version']}" if champion else "nothing to roll back to")
        return champion

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

`rollback` moves the registry's `champion` alias back to `previous_champion`.
Nothing is redeployed and no file is copied: whatever loads
`models:/project_tagger@champion` gets the previous version from its next
load on. That is what makes a rollback cheap enough to automate.

Finish with the wiring:

```python dags/06_monitoring_and_retraining.py
    windows = prepare_windows()
    drift = detect_drift(windows, "{{ params.drift_p_val }}")
    branch = decide(drift, check_performance(windows))

    branch >> continue_serving
    branch >> inspect(drift)
    branch >> rollback()
    branch >> request_retraining >> trigger_retraining


monitoring_dag()
```

Four branches out of `decide`, which returns the `task_id` of the one to run.
There is no join at the end, because nothing needs to run after all of them.

### Run it

```bash
docker compose exec airflow-scheduler airflow dags test monitoring
```

> **This command takes about 70 seconds and looks frozen for 60 of them.** That
> is not a hang. `request_retraining` is the human-in-the-loop gate: it enters
> `awaiting_input` and waits out its full 60-second `response_timeout` before
> defaulting to Approve, because nobody is going to click the button. Let it
> sit.

**Checkpoint.** `decide` logs the two p-values: label drift around `7.7e-05`,
length drift around `0.92`. The tag mix of the newest quarter really did move.
`mlops` grows from about 6% of the older projects to about 16% of the newest,
and `natural-language-processing` falls from 44% to 30%; the text length did
not change. The label test is far below 0.05, so `decide` takes the retraining
branch. Then, in order, with the minute-long wait between the second and third
lines:

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

> **Change the policy, change the decision.** The threshold is a param, so a
> run can be given a stricter one:
>
> ```bash
> docker compose exec airflow-scheduler \
>     airflow dags test monitoring --conf '{"drift_p_val": 0.00001}'
> ```
>
> The label test's p-value is now above the threshold, so no test reports
> drift. If §10 promoted a model, `decide()` returns `continue_serving`, the
> retraining branch is skipped, and the run is over in seconds. If there is no
> production model yet, `decide()` says so — `No production model yet - train
> one.` — and takes the retraining branch anyway, timeout included. Both are
> correct: the outcome depends on state and policy, which is the point of a
> monitor.

One caveat about `f1_now`. The current window is the newest quarter of the
same table `mlops` trained on, so the champion has seen most of those rows,
and its f1 there is flattering. In production the current window is rows that
arrived *after* training, labelled since, which is what makes `f1_now` an
honest measure of concept drift.

Reference: [`solutions/06_monitoring_and_retraining.py`](../../solutions/06_monitoring_and_retraining.py).

### What "retrain" leaves out

`trigger_retraining` fires `mlops`, which fits on whatever is in the warehouse.
That is the orchestration; it is not the whole retraining problem. Each of these
would be one more task in a real version of this Dag:

| Step | Why it is not automatic |
|---|---|
| **labeling** | the new rows need real labels, not the model's own predictions fed back as truth |
| **active learning** | label the rows that would teach the model most — you rarely have budget for all of them |
| **QA** | audit the labels before training on them: false positives, labeller disagreement, subgroups of the data where the model fails |
| **augmentation** | expand the new data so a small window is not drowned out by history |
| **sampling** | up/downsample to fix the imbalance the drift introduced |
| **evaluation** | build the test set to resemble the data the model will see in production |

The other constraint is **catastrophic forgetting**: fitting only on the new
window makes the model forget patterns it used to get right, so it fixes
today's errors and brings back old ones. The Dag here sidesteps it by always
training on the full labelled table — fine at this size, and the first thing to
revisit when the table stops fitting in a task.

Which is also why `promotion_gate` in §10 compares against the current
production model instead of a fixed threshold alone: a retrained model that is
worse than what is serving should not ship just because a monitor fired.

### Rolling back less often

A rollback means a bad model reached production first. **Deployment
strategies** shrink how much damage it can do before anyone notices:

| Strategy | How the new model meets traffic |
|---|---|
| **canary** | a small share of requests (say 5%) goes to the new model; its metrics are compared with the champion's before the share grows |
| **shadow** (or *dark canary*) | the new model receives a copy of real requests and its predictions are logged, never returned; it is judged on live data with no user affected |
| **blue-green** | the new version runs alongside the old one, and traffic switches over all at once, with the old one kept ready to switch back |

They belong to the serving layer, the service that answers prediction
requests, which this project does not have: here `champion` switches all at
once. The registry is where they start, though: a
`challenger` alias next to `champion` is how a serving system knows which
version gets the canary share or the shadow copy, and the promotion gate
becomes "the challenger's live metrics beat the champion's" instead of a test
set score.

---

← [11. Waiting: sensors, deferrable tasks, events](11-waiting-sensors-deferrable-tasks-events.md) · [Index](README.md) · [13. Deadline alerts](13-deadline-alerts.md) →
