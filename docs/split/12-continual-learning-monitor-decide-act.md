## 12. Continual learning: monitor, decide, act

**Continual**, not continuous: the system repeats *with breaks*, and the breaks
are deliberate. A monitor fires, something is decided, and then the pipeline
runs. Where it matters, a person decides: what reaches production waits for
`approve_promotion`, the approval task of §10.

What you are watching for is **drift**, in three flavours:

| Drift | What moved | Caught here by |
|---|---|---|
| **data drift** | the input distribution — the text itself | `length_drift`: a Kolmogorov–Smirnov test on the number of words per text |
| **target drift** | the label distribution — one tag takes over, or a new tag appears | `label_drift`: a chi-squared test on the tag counts, and `new_labels` |
| **concept drift** | the relation between input and label — the same input now deserves a different label | only the live performance check, `monitoring.production_performance()`, which scores the production model on labelled rows it has never seen: f1 falls while inputs look unchanged |

The two tests come from **alibi-detect**, a drift-detection library. Each takes
a **reference** window (the rows the production model learned from) and a
**current** window (the rows that reached the warehouse after it was trained,
which it has never seen), and answers with a
**p-value**: the probability of seeing a difference at least this large if
nothing had really changed. A small p-value means the difference is unlikely
to be chance. Below a threshold you choose, conventionally 0.05, the test
reports drift. The Kolmogorov–Smirnov (KS) test compares two distributions of a
number; the chi-squared test compares two sets of category counts.

Both windows come from the production model's own training run. In §10,
`train_best` logged `split_ids.json` with the run: the project ids in each of
its splits. **Reference** is every row of `labeled_projects` whose id was in
its training or validation split. **Current** is every row whose id is in
none of its splits. Its test rows are in neither window: it never learned
from them, but it was chosen on them. With fewer than 50 current rows
(`config.MIN_MONITOR_ROWS`) there is nothing to measure yet, and the Dag keeps
serving and looks again on its next run.

A monitoring event has four sensible responses, and
`dags/06_monitoring_and_retraining.py` makes them branches:

| Response | When | What happens |
|---|---|---|
| **continue** | drift and performance within limits | nothing; keep serving |
| **improve** | f1 dropped, or the input or label distribution moved | trigger `mlops`, whose gate and approval decide what ships (§10) |
| **inspect** | an unseen label appeared | stop and page someone — the label space changed |
| **rollback** | performance collapsed | re-point production at the previous model |

How the pieces talk when the Dag runs. Each column is one system, as in the
§9 diagram:

```
 Airflow (Docker)               │ BigQuery          │ MLflow (Docker)
 monitoring Dag                 │ mlops_course      │ localhost:5000
────────────────────────────────┼───────────────────┼───────────────────────
                                │                   │
 1 prepare_windows ◄────────────── labeled_projects │
   and ◄────────────────────────────────────────────── the champion's
                                │                   │  split_ids.json
   writes reference.parquet     │                   │
   and current.parquet in data/ │                   │
 2 detect_drift (alibi-detect,  │                   │
   in its own virtualenv)       │                   │
   reads the two files          │                   │
 3 check_performance ◄──────────────────────────────── the champion model
   scores it on current.parquet │                   │
 4 decide: one branch of        │                   │
   continue_serving             │                   │
   inspect                      │                   │
   rollback ─────────────────────────────────────────► moves champion back
   trigger_retraining           │                   │  to previous_champion
     │ starts a run of          │                   │
     ▼                          │                   │
   mlops Dag (§10)              │                   │
```

### New data arrives

The production model from §10 was trained on the 764 Made With ML projects,
so every row in the warehouse is in one of its splits and there is nothing to
monitor. Now 800 more arrive: real computer-science papers from arXiv,
submitted between January 2025 and September 2026, each written as a project
(its title, and the first sentence of its abstract as the description) and
tagged from its arXiv category. [`datasets/README.md`](../../datasets/README.md)
says how they were chosen and tagged.

Nobody altered them to make drift appear. They differ from the Made With ML
projects in two ways: more computer vision and no `mlops` at all, and longer
descriptions, because a paper's first sentence says more than a one-line
blurb. The second is a change in how the source writes, not in what the
projects are about; a test on text length cannot tell the two apart.
The papers are also newer, but date and source changed together, so this
data cannot say how much of the drift is down to the date.

Pause `mlops` first:

```bash
docker compose exec airflow-scheduler airflow dags pause mlops
```

A `dataops` run that passes emits the asset event that starts `mlops` (§8).
Here that would train a model on the new projects before `monitoring` has
compared them with what the production model knows, and that comparison is
what this section is about. `dataops` stays un-paused, as §10 left it.

A delivery is a change of URL in an Airbyte source. In Airbyte, open
**Sources**, choose `projects`, replace its **URL** with:

```
https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/batch_1/projects.csv
```

and save. Do the same for `tags`, with:

```
https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/batch_1/tags.csv
```

Then load it:

```bash
docker compose exec airflow-scheduler airflow dags trigger dataops
```

**Checkpoint.** The run succeeds after a few minutes, and in the BigQuery
console (§9) `labeled_projects` has 1564 rows. `mlops` has no new run: a paused
Dag starts no run for an asset event.

### Build it

Create `dags/06_monitoring_and_retraining.py`. The thresholds are Dag
**params**, so the policy is visible in the UI, overridable per run, and
recorded with the run that used it:

```python dags/06_monitoring_and_retraining.py
"""Lesson 6 - continual learning: monitor, decide, and act."""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task
from airflow.sdk.exceptions import AirflowSkipException
from airflow.sdk.types import OutletEventAccessorsProtocol

from project_tagger import config, monitoring, registry
from project_tagger_orchestration.assets import PRODUCTION_MODEL

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

The drift task, `detect_drift`, runs in **another Python**. alibi-detect needs
`numpy<2`, which Airflow's own environment cannot have, so the image has a second virtualenv
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

This is the light version of what §16 recommends for teams with clashing
dependencies: one container image per task, with `KubernetesPodOperator`.

Measure first. The windows are written once; the drift tests and the
performance check both read them:

```python dags/06_monitoring_and_retraining.py
    @task
    def prepare_windows(*, run_id: str) -> dict[str, Any]:
        key = run_id.replace(":", "-").replace("+", "-")
        # Writes reference.parquet and current.parquet; returns their paths, num_current
        # and version (the champion's). With no champion, or fewer than
        # config.MIN_MONITOR_ROWS current rows, it writes nothing and the paths are None.
        windows = monitoring.prepare_windows(f"monitoring-{key}")
        print(f"windows: {windows}")
        return windows

    @task.external_python(python=config.ALIBI_PYTHON, expect_airflow=False)
    def detect_drift(windows: dict, p_val: str) -> dict | None:
        from project_tagger import drift

        # label_drift and length_drift (each with is_drift, p_val, distance), new_labels
        # and both tag distributions; None when there are no files.
        return drift.detect_drift(windows["reference"], windows["current"], float(p_val))

    @task
    def check_performance(windows: dict[str, Any]) -> dict[str, Any] | None:
        # The champion's f1_at_promotion, and its f1_now on the current window;
        # None when there is no window.
        performance = monitoring.production_performance(windows["current"])
        if performance:
            print(f"f1 at promotion={performance['f1_at_promotion']:.4f} now={performance['f1_now']:.4f}")
        return performance
```

Then decide:

```python dags/06_monitoring_and_retraining.py
    @task.branch
    def decide(
        windows: dict[str, Any],
        drift: dict[str, Any] | None,
        performance: dict[str, Any] | None,
        *,
        params: dict[str, Any],
    ) -> str:
        if windows["version"] is None:
            print("No production model yet - train one.")
            return "trigger_retraining"
        if drift is None or performance is None:
            print(
                f"only {windows['num_current']} new rows since version {windows['version']} "
                "was trained - nothing to measure"
            )
            return "continue_serving"

        label, length = drift["label_drift"], drift["length_drift"]
        print(
            f"label drift p={label['p_val']:.4g} length drift p={length['p_val']:.4g} "
            f"new_labels={drift['new_labels']} policy={dict(params)}"
        )
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
            return "trigger_retraining"
        return "continue_serving"
```

`params` is the policy — the defaults in `POLICY_PARAMS`, or whatever this run
was given. The first two checks come before any policy: no production model
means there is one to train, and no current window means there is nothing to
judge yet. After them, each retraining condition watches one of the three
kinds of drift in the table. Note the ordering: **a new label routes to
`inspect`, not to `trigger_retraining`.** Retraining on data whose label space
you have not understood yet is how you ship a worse model with full
confidence.

Now the four branches `decide` can pick. Three are simple; the fourth starts
another Dag:

```python dags/06_monitoring_and_retraining.py
    continue_serving = EmptyOperator(task_id="continue_serving")

    @task
    def inspect(drift: dict[str, Any]) -> None:
        print("MANUAL INSPECTION NEEDED")
        print(f"  new labels: {drift['new_labels']}")
        print(f"  current distribution: {drift['current_distribution']}")

    @task(outlets=[PRODUCTION_MODEL])
    def rollback(
        performance: dict[str, Any], *, outlet_events: OutletEventAccessorsProtocol
    ) -> dict[str, Any]:
        reason = f"f1 fell from {performance['f1_at_promotion']:.4f} to {performance['f1_now']:.4f}"
        champion = registry.rollback(reason)
        if champion is None:
            raise AirflowSkipException("no previous_champion - nothing to roll back to")
        outlet_events[PRODUCTION_MODEL].extra = {
            "version": champion["version"],
            "rolled_back_version": champion["rolled_back_version"],
            "reason": "rollback",
        }
        print(f"champion -> version {champion['version']} (retired {champion['rolled_back_version']}: {reason})")
        return champion

    trigger_retraining = TriggerDagRunOperator(
        task_id="trigger_retraining",
        trigger_dag_id="mlops",
        conf={"reason": "monitoring_drift"},
        wait_for_completion=False,
        reset_dag_run=True,
    )
```

`TriggerDagRunOperator` starts another Dag: it creates a `manual__` run of
`mlops` with the `conf` given, and with `wait_for_completion=False` it does
not stay around to watch it. No approval stands before it: a retrain changes
nothing in production, and the model it produces still has to pass
`promotion_gate` and `approve_promotion` (§10).

`rollback` moves the registry's `champion` alias back to `previous_champion`.
Nothing is redeployed and no file is copied: whatever loads
`models:/project_tagger@champion` gets the previous version from its next
load on. That is what makes a rollback cheap enough to automate.
`registry.rollback` also deletes the `previous_champion` alias, so a second
rollback cannot put the bad model back, and tags the retired version with
`rolled_back_reason`, which the MLflow UI shows. The task emits
`production_model`, like `promote_model` in §10: a rollback changes the model
in production as much as a promotion does. With nothing to roll back to, it
raises `AirflowSkipException`, and a skipped task emits no asset event.

Finish with the wiring:

```python dags/06_monitoring_and_retraining.py
    windows = prepare_windows()
    drift = detect_drift(windows, "{{ params.drift_p_val }}")
    performance = check_performance(windows)
    branch = decide(windows, drift, performance)

    branch >> continue_serving
    branch >> inspect(drift)
    branch >> rollback(performance)
    branch >> trigger_retraining


monitoring_dag()
```

Four branches out of `decide`, which returns the `task_id` of the one to run.
There is no join at the end, because nothing needs to run after all of them.

### Run it

```bash
docker compose exec airflow-scheduler airflow dags test monitoring
```

**Checkpoint.** `prepare_windows` logs `'num_current': 800`, with `version`
the champion's: the 800 papers are the rows it has never seen.
`check_performance` logs its f1 at promotion and now, around 0.89 and 0.82: a
drop of 0.05 to 0.10, depending on the search, under `max_f1_drop`. `decide` logs the two
p-values: label drift around `1e-22`, length drift `0`. Both are far below
0.05, and they say different things:

- **Target drift.** `computer-vision` is 38% of the rows the model learned
  from and 57% of the new ones; `mlops`, 8% of what it learned from, does not
  appear at all.
- **Data drift.** A text has 16 words at the median in the rows the model
  learned from, and 27 in the new ones: a p-value too small to print as
  anything but `0`.
- **Performance.** The f1 fell, but by less than `max_f1_drop`, so the
  concept-drift check alone would not have fired.

Two tests report drift, so `decide` takes the retraining branch. Then:

```
Branch into trigger_retraining
Dag Run triggered successfully.  trigger_dag_id=mlops
```

Check that last line:

```bash
docker compose exec airflow-scheduler airflow dags list-runs mlops -o plain
```

It now has a fresh `manual__…` run that this Dag created, `queued`: `mlops` is
paused, so the run waits. One Dag decided another Dag should run.

> **Change the policy, change the decision.** The threshold is a param, so a
> run can be given another one:
>
> ```bash
> docker compose exec airflow-scheduler \
>     airflow dags test monitoring --conf '{"drift_p_val": 0}'
> ```
>
> No p-value is below 0, so no test reports drift, and the policy watches only
> the f1, whose drop is under `max_f1_drop`: `decide()` returns
> `continue_serving`, the retraining branch is skipped, and the run is over in
> seconds. The same data and the same production model, and another decision:
> the outcome depends on the policy, which is the point of making it a param.
> Here it is also the worse policy: the f1 did drop, and the retrain below
> wins it back.

**Let the retraining run.** The run `trigger_retraining` created is waiting
for `mlops`, which is still paused. Un-pause it:

```bash
docker compose exec airflow-scheduler airflow dags unpause mlops
```

**Checkpoint.** The queued run starts within seconds and succeeds a couple of
minutes later. Its snapshot has all 1564 rows, and its test split 319 of them:
the 157 the champion was tested on, plus 162 of the papers, by the same hash.
`compare_with_champion` logs three lines, `all`, `new` and `old`. On the 162
new test rows the new model scores 0.92 to 0.96 and the champion 0.82 to
0.85; on the 157 old ones the two are close, the new model usually a point or
two lower. `promotion_gate` logs `f1=… clears the champion's … on the 162 new
test rows - promoting`, and `approve_promotion` waits for you, as in §10. This
is the decision the approval is for: the numbers are in the prompt, and the
model in MLflow. Approve it on its
**Required Actions** tab, or let the default approve it after 60 seconds, and
`promote_model` moves `champion` to the new version.

That is the drift treated: the new model learned from 638 of the papers, and
on the ones neither model has seen it is 8 to 14 points better.

Ask the monitor once more:

```bash
docker compose exec airflow-scheduler airflow dags test monitoring
```

**Checkpoint.** `decide` logs `only 0 new rows since version … was trained -
nothing to measure`, with the version `promote_model` just promoted, and takes
`continue_serving`, in seconds. The new champion was built on all 1564 rows,
so every row is in one of its splits. The next delivery, in §14, gives the
monitor something to measure again.

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

Which is also why `promotion_gate` in §10 compares against the production
model, on the same test rows, instead of a fixed threshold alone: a retrained
model that is worse than what is serving should not ship just because a
monitor fired.

The `old` line of `compare_with_champion` is where forgetting shows: the new
model's f1 on the champion's own test rows. In this section's retraining run
it cost a point or two. A team that wants a limit on it adds a **guardrail**
to the gate: no promotion if the f1 on the old rows falls by more than a set
margin.

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
once. The registry is where they start, though: a `challenger` alias next to
`champion` is how a serving system knows which version gets the canary share
or the shadow copy, and the promotion gate
becomes "the challenger's live metrics beat the champion's" instead of a test
set score.

---

← [11. Waiting: sensors, deferrable tasks, events](11-waiting-sensors-deferrable-tasks-events.md) · [Index](README.md) · [13. Deadline alerts](13-deadline-alerts.md) →
