## 12. Continual learning: monitor, decide, act

**Continual**, not continuous: the system repeats *with deliberate breaks*. A
monitor fires, something is decided, then the pipeline runs. Where it matters,
a person decides: what reaches production waits for `approve_promotion`, the
approval task of §10.

The monitor watches for **drift**, of three kinds:

| Drift | What moved | Caught here by |
|---|---|---|
| **data drift** | the input distribution — the text itself | `length_drift`: a Kolmogorov–Smirnov test on the number of words per text |
| **target drift** | the label distribution — one tag takes over, or a new tag appears | `target_drift`: a chi-squared test on the tag counts, and `new_labels` |
| **concept drift** | the relation between input and label — the same input now deserves a different label | only the performance check, `monitoring.production_performance()`, which scores the production model on labelled rows it has never seen: f1 falls while inputs look unchanged |

The two tests come from **alibi-detect**, a drift-detection library. Each
compares a **reference** window (the rows the production model learned from)
with a **current** window (the rows that reached the warehouse after it was
trained, which it has never seen) and returns a **p-value**: the probability
of a difference at least this large if nothing had really changed. Below a
threshold you choose, conventionally 0.05, the difference is unlikely to be
chance and the test reports drift. The Kolmogorov–Smirnov (KS) test compares
how a numeric value, here words per text, is spread in the two windows; the
chi-squared test compares two sets of category counts.

A p-value says how sure the test is that something moved, not how much. With
enough rows, a change too small to matter is still significant, and retraining
on every such change starts runs nobody needs. So each test also reports a
**effect size**, from 0 to 1: for the tags, the share of rows whose tag would
have to change for the two mixes to match (the *total variation distance*);
for the length, the largest gap between the two cumulative distributions (the
*KS distance*). The policy counts drift only when the p-value is small *and*
the effect size is at least `min_effect_size`.

Both windows come from the production model's training run. In §10,
`train_best` logged `split_ids.json` with the run: the project ids in each
split. **Reference** is every row of `labeled_projects` whose id was in its
training or validation split; **current**, every row whose id is in none. Its
test rows are in neither window: it never learned from them, but it was chosen
on them. With fewer than 50 current rows (`config.MIN_MONITOR_ROWS`) there is
nothing to measure yet; the Dag keeps serving and looks again on its next run.

The champion is scored on the current window in the two standard ways: a
**cumulative** f1, on all of it, and a **sliding-window** f1, on its rows
created in the 90 days (`config.SLIDING_WINDOW_DAYS`) up to the newest one,
when they are at least 50. The sliding window ends where the data ends, not
today: each run ends it at the newest row, so it moves forward as data
arrives. A cumulative f1 can hide a recent fall: a good year averages out a bad
month. The policy takes the larger of the two drops from the **baseline**,
the champion's f1 on its own test split. With the previous champion's,
`check_performance` logs four f1s:

| f1 | Model | Rows scored |
|---|---|---|
| `f1_baseline` | champion | its test split, when §10 trained it |
| `f1_cumulative` | champion | the whole current window |
| `f1_sliding_window` | champion | the current rows created in the 90 days up to the newest one; `None` if fewer than 50 |
| `f1_previous` | previous champion | the whole current window; `None` if there is no previous champion |

Every monitoring run ends in one of four actions. In
`dags/06_monitoring_and_retraining.py`, the `decide` task, a `@task.branch`
(§10), picks one; each action is its own task:

| Action | When | What happens |
|---|---|---|
| `continue_serving` | drift and performance within limits, or a retrain on this data is already queued, running or turned down | nothing; keep serving |
| `trigger_retraining` | f1 dropped, or the input or label distribution moved | trigger `mlops`, whose gate and approval decide what ships (§10) |
| `inspect` | an unseen label appeared, or performance collapsed and the previous model is no better | fail the run, so a person looks; nothing is retrained or rolled back |
| `rollback` | performance collapsed, and the previous model does better on the same rows | re-point production at the previous model |

How the pieces talk when the Dag runs, one column per system as in the §9
diagram:

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
 3 check_performance ◄──────────────────────────────── the champion and
   scores both models on        │                   │  previous_champion
   current.parquet              │                   │  models
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
(its title, and the abstract's first sentence as the description) and tagged
from its arXiv category. [`datasets/README.md`](../../datasets/README.md) says
how they were chosen and tagged.

Nobody altered them to make drift appear. They differ from the Made With ML
projects in two ways: more computer vision and no `mlops` at all; and longer
descriptions, because a paper's first sentence says more than a one-line
blurb. The second is a change in how the source writes, not in what the
projects are about, and a test on text length cannot tell the two apart. The
papers are also newer, but date and source changed together, so this data
cannot say how much of the drift is down to the date.

Pause `mlops` first:

```bash
docker compose exec airflow-scheduler airflow dags pause mlops
```

A `dataops` run that passes emits the asset event that starts `mlops` (§8).
Here that would train a model on the new projects before `monitoring` compares
them with what the production model knows: the comparison this section is
about. `dataops` stays un-paused, as §10 left it.

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
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk.types import OutletEventAccessorsProtocol

from project_tagger import config, monitoring, registry
from project_tagger_orchestration.assets import PRODUCTION_MODEL
from project_tagger_orchestration.links import MlflowModelVersionLink

DEFAULT_ARGS = {"owner": "ml-engineering", "retries": 1}

POLICY_PARAMS = {
    "drift_p_val": Param(
        0.05, type="number", description="A drift test with a p-value below this reports drift."
    ),
    "min_effect_size": Param(
        0.05, type="number", description="...and only if at least this much of the distribution moved (0-1)."
    ),
    "max_f1_drop": Param(0.15, type="number", description="f1 drop that triggers retraining."),
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
the imported module; the `dag_id` is what Airflow sees.)

The drift task, `detect_drift`, runs in **another Python**. alibi-detect needs
`numpy<2`, which Airflow's environment cannot have, so the image has a second
virtualenv for it, `/opt/venvs/alibi`, whose interpreter is
`config.ALIBI_PYTHON`. `@task.external_python` sends the function there:
Airflow writes its source and arguments to files, runs that interpreter on
them in a subprocess, and reads the return value back. Consequences, visible
in the code below:

- Everything the function uses is **imported inside it**; this file's
  module-level imports do not exist in that Python.
- Arguments and return value must be **plain data**, pickled to cross the
  process boundary: paths in, a dict of numbers out, never a dataframe.
- `expect_airflow=False`, because that virtualenv has no Airflow; the function
  gets no context either. The threshold arrives as a template,
  `"{{ params.drift_p_val }}"`, rendered before the subprocess starts, so as a
  string, which `float()` converts.

This is the light version of what §16 recommends for teams with clashing
dependencies: one container image per task, with `KubernetesPodOperator`.

Measure first. The windows are written once; the drift tests and the
performance check both read them:

```python dags/06_monitoring_and_retraining.py
    @task
    def prepare_windows(*, run_id: str) -> dict[str, Any]:
        key = run_id.replace(":", "-").replace("+", "-")
        # Writes reference.parquet and current.parquet; returns their paths, num_current,
        # version (the champion's) and already_trained_on. With no champion, or fewer than
        # config.MIN_MONITOR_ROWS current rows, it writes nothing and the paths are None.
        windows = monitoring.prepare_windows(f"monitoring-{key}")
        print(f"windows: {windows}")
        return windows

    @task.external_python(python=config.ALIBI_PYTHON, expect_airflow=False)
    def detect_drift(windows: dict, p_val: str) -> dict | None:
        from project_tagger import drift

        # target_drift and length_drift (each with is_drift, p_val, distance, effect_size),
        # new_labels and both tag distributions; None when there are no files.
        return drift.detect_drift(windows["reference"], windows["current"], float(p_val))

    @task
    def check_performance(windows: dict[str, Any]) -> dict[str, Any] | None:
        # The champion's f1_baseline; its f1_cumulative on the whole current
        # window and f1_sliding_window on its newest rows; f1_previous, the
        # previous champion's on the window; per_class and slices. None when
        # there is no window.
        performance = monitoring.production_performance(windows["current"])
        if performance:
            names = ("f1_baseline", "f1_cumulative", "f1_sliding_window", "f1_previous")
            print({k: None if performance[k] is None else round(performance[k], 4) for k in names})
            print(
                f"sliding window: the {config.SLIDING_WINDOW_DAYS} days up to the newest row, "
                f"{performance['sliding_window_rows']} rows"
            )
        return performance
```

Then decide:

```python dags/06_monitoring_and_retraining.py
    def retrain_or_wait(windows: dict[str, Any], ti) -> str:
        if ti.get_dr_count(dag_id="mlops", states=["queued", "running"]):
            print("an mlops run is already queued or running - not starting another")
            return "continue_serving"
        if windows["already_trained_on"]:
            print("a model was already trained on every current row and not promoted - waiting for new data")
            return "continue_serving"
        return "trigger_retraining"

    @task.branch
    def decide(
        windows: dict[str, Any],
        drift: dict[str, Any] | None,
        performance: dict[str, Any] | None,
        *,
        params: dict[str, Any],
        ti,
    ) -> str:
        if windows["version"] is None:
            print("No production model yet - train one.")
            return retrain_or_wait(windows, ti)
        if drift is None or performance is None:
            print(
                f"only {windows['num_current']} new rows since version {windows['version']} "
                "was trained - nothing to measure"
            )
            return "continue_serving"

        target, length = drift["target_drift"], drift["length_drift"]
        print(
            f"target drift p={target['p_val']:.4g} effect_size={target['effect_size']:.3f} "
            f"length drift p={length['p_val']:.4g} effect_size={length['effect_size']:.3f} "
            f"new_labels={drift['new_labels']} policy={dict(params)}"
        )
        f1s = (performance["f1_cumulative"], performance["f1_sliding_window"])
        f1_worst = min(f1 for f1 in f1s if f1 is not None)
        f1_drop = performance["f1_baseline"] - f1_worst
        print(f"f1_drop={f1_drop:.4f}")
        for key, value in {
            "target_drift_p": target["p_val"],
            "target_drift_effect_size": target["effect_size"],
            "length_drift_p": length["p_val"],
            "length_drift_effect_size": length["effect_size"],
            "f1_drop": f1_drop,
        }.items():
            ti.xcom_push(key=key, value=value)

        def drifted(test: dict[str, Any]) -> bool:
            return test["p_val"] < params["drift_p_val"] and test["effect_size"] >= params["min_effect_size"]

        if f1_drop >= params["critical_f1_drop"]:
            f1_previous = performance["f1_previous"]
            if f1_previous is not None and f1_previous > performance["f1_cumulative"]:
                return "rollback"
            return "inspect"
        if drift["new_labels"]:
            return "inspect"
        if (
            f1_drop >= params["max_f1_drop"]    # concept drift
            or drifted(target)                   # target drift
            or drifted(length)                  # data drift
        ):
            return retrain_or_wait(windows, ti)
        return "continue_serving"
```

Airflow injects `params` and `ti` into `decide` by parameter name, as with
`ti` in §6. `retrain_or_wait` is not a task but a plain function, so nothing
is injected into it: it gets `decide`'s `ti` as an argument.

`decide` returns only the `task_id` of the branch, so the numbers it decides
on would stay in the log. `ti.xcom_push` (§6) saves each one as an XCom of its
own: the two p-values, the two effect sizes and `f1_drop`, one row each in the
UI.

`params` is the policy: the defaults in `POLICY_PARAMS`, or whatever this run
was given. The first two checks come before any policy: no production model
means one must be trained; no current window means nothing to judge yet.
Then `f1_drop` is the larger of the two f1 drops, `drifted` requires both a
small p-value and a large enough effect size, and each retraining condition watches
one of the three kinds of drift in the table. Note the ordering: **a new label
routes to `inspect`, not to `trigger_retraining`.** Retraining on data whose
label space you have not understood yet is how you ship a worse model with
full confidence.

A collapse does not roll back by itself either. **A rollback helps only if
the model in production is the problem**, so `decide` checks first: the
previous champion has not seen the current rows either, and if it scores
better on them, the newer model is worse and `rollback` puts the previous one
back. If not, the data moved under both models, rolling back would fix
nothing, and the run goes to `inspect`, as does a collapse with no previous
champion to go back to.

Both paths into `trigger_retraining` go through `retrain_or_wait`. The monitor
runs every day; new data does not arrive every day, and until it does, the
drift found stays. Without this check, every morning would start another
`mlops` run on the same rows. It keeps serving in two cases:

- **A run of `mlops` is already queued or running.** `ti.get_dr_count` asks
  Airflow how many runs of another Dag are in the given states.
- **A model was already trained on these rows and not promoted.**
  `prepare_windows` looks up the newest training run in MLflow; if it is not
  the champion's and its `split_ids.json` has every current row, the gate or
  the approval turned it down. Retraining on the same rows only reruns the
  Optuna search on the same data, so a better score would be luck. Only new
  rows change that, and new rows already start `mlops` on their own through
  its asset (§8).

Now the four branches `decide` can pick. Three are simple; the fourth starts
another Dag:

```python dags/06_monitoring_and_retraining.py
    continue_serving = EmptyOperator(task_id="continue_serving")

    @task
    def inspect(drift: dict[str, Any], performance: dict[str, Any]) -> None:
        print("MANUAL INSPECTION NEEDED")
        print(f"  new labels: {drift['new_labels']}")
        print(f"  current distribution: {drift['current_distribution']}")
        print(f"  f1 baseline={performance['f1_baseline']:.4f} cumulative={performance['f1_cumulative']:.4f}")
        for name, m in {**performance["per_class"], **performance["slices"]}.items():
            print(f"  {name}: {m['support']} rows, f1 {m['f1']:.4f}")
        raise AirflowFailException("manual inspection needed - nothing was retrained or rolled back")

    @task(outlets=[PRODUCTION_MODEL])
    def rollback(
        performance: dict[str, Any], *, outlet_events: OutletEventAccessorsProtocol
    ) -> dict[str, Any]:
        reason = f"f1 fell from {performance['f1_baseline']:.4f} to {performance['f1_cumulative']:.4f}"
        champion = registry.rollback(reason)
        if champion is None:
            raise AirflowFailException("previous_champion is gone - nothing to roll back to")
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

`inspect` prints what a person needs to start looking: what moved, and where
the model fails, per tag and on each evaluation slice of §10. Then it fails on
purpose, with `AirflowFailException` (§9): a run that needs a person must not
end green, because a success reads as "all fine" and a failure is what
alerting is wired to (§16).

`rollback` moves the registry's `champion` alias back to `previous_champion`.
Nothing is redeployed or copied: whatever loads
`models:/project_tagger@champion` gets the previous version on its next load,
which makes a rollback cheap enough to automate. `registry.rollback` also
deletes the `previous_champion` alias, so a second rollback cannot put the bad
model back, and tags the retired version with `rolled_back_reason`, shown in
the MLflow UI. The task emits `production_model`, like `promote_model` in §10:
a rollback changes the model in production as much as a promotion does.
`decide` picks `rollback` only after seeing a previous champion; if the alias
is gone by the time the task runs, it fails rather than skips, because the
collapsed model is still serving and someone must know. A failed task emits no
asset event. The wiring gives `rollback` an extra link (§10), **MLflow model
version**, to the version it put back in production.

`TriggerDagRunOperator` starts another Dag: it creates a `manual__` run of
`mlops` with the given `conf`, and with `wait_for_completion=False` does not
wait for it. It comes with an extra link of its own, **Triggered DAG**, to that
run. No approval stands before it: a retrain changes nothing in
production, and its model still has to pass `promotion_gate` and
`approve_promotion` (§10).

Finish with the wiring:

```python dags/06_monitoring_and_retraining.py
    windows = prepare_windows()
    drift = detect_drift(windows, "{{ params.drift_p_val }}")
    performance = check_performance(windows)
    branch = decide(windows, drift, performance)
    restored = rollback(performance)
    restored.operator.operator_extra_links = [MlflowModelVersionLink()]

    branch >> continue_serving
    branch >> inspect(drift, performance)
    branch >> restored
    branch >> trigger_retraining


monitoring_dag()
```

`decide` returns the `task_id` of the branch to run. There is no join at the
end, because nothing needs to run after all of them.

### Run it

```bash
docker compose exec airflow-scheduler airflow dags test monitoring
```

The output runs to about a hundred lines. To find the lines the Checkpoint
reads, search your terminal for `windows:`, `f1_baseline`, `target drift` and
`Branch into`. The numbers are also in the UI: open the run, click
`prepare_windows` or `decide`, then **Storage** and **XCom** (§5).
`num_current`, the p-values, the effect sizes and `f1_drop` each have a row of
their own.

> The output also shows a traceback ending in
> `ModuleNotFoundError: No module named 'pendulum'`. Nothing failed. Before
> running `detect_drift`, Airflow runs `python -c "import pendulum"` in the
> alibi virtualenv, to see whether it can pass the run's dates there:
> `logical_date` and the other dates in the context are pendulum objects. The
> import fails, so Airflow leaves those dates out, and `detect_drift` does not
> use them.

**Checkpoint.** `prepare_windows` logs `'num_current': 800`, with `version`
the champion's: the 800 papers are the rows it has never seen.
`check_performance` logs:

| Metric | Value | Meaning |
|---|---|---|
| `f1_baseline` | around 0.89 | |
| `f1_cumulative` | around 0.80 | a drop of 0.05 to 0.11 depending on the search, under `max_f1_drop` (0.15) |
| `f1_sliding_window` | above `f1_cumulative` | the 122 newest papers are not the ones it gets wrong, so the cumulative f1 gives the larger drop |
| `f1_previous` | `None` | this is the first model ever promoted: nothing to compare it with |

The f1 fell, but by less than `max_f1_drop`, so the concept-drift check alone
would not have fired. `decide` logs the two drift tests, both with a p-value
far below 0.05 and an effect size well above 0.05:

| Test | p-value | Effect size | Rows the model learned from | New rows |
|---|---|---|---|---|
| Target drift | around `1e-22` | 0.19 | `computer-vision` 38%, `mlops` 8% | `computer-vision` 57%, no `mlops` |
| Data drift | `0` (too small to print) | 0.63 | median text of 16 words | median text of 27 words |

Two tests report drift, so `decide` takes the retraining branch:

```
Branch into trigger_retraining
Dag Run triggered successfully.  trigger_dag_id=mlops
```

Check that last line:

```bash
docker compose exec airflow-scheduler airflow dags list-runs mlops -o plain
```

`mlops` now has a fresh `manual__…` run created by this Dag, `queued` because
`mlops` is paused. One Dag decided another Dag should run.

> **Change the policy, change the decision.** The threshold is a param, so a
> run can be given another one:
>
> ```bash
> docker compose exec airflow-scheduler \
>     airflow dags test monitoring --conf '{"drift_p_val": 0}'
> ```
>
> No p-value is below 0, so no test reports drift and only the f1 counts,
> whose drop is under `max_f1_drop`: `decide()` returns `continue_serving`,
> the retraining branch is skipped, and the run is over in seconds. Same data,
> same production model, another decision: the outcome depends on the policy,
> which is the point of making it a param. Here it is also the worse policy:
> the f1 did drop, and the retrain below wins it back.

**Let the retraining run.** The run `trigger_retraining` created waits because
`mlops` is still paused. Un-pause it:

```bash
docker compose exec airflow-scheduler airflow dags unpause mlops
```

**Checkpoint.** The queued run starts within seconds and succeeds a couple of
minutes later. Its snapshot has all 1564 rows, and its test split 319: the 157
the champion was tested on, plus 162 of the papers, by the same hash.
`compare_with_champion` logs three lines, `all`, `new` and `old`:

| Test rows | New model | Champion |
|---|---|---|
| `new`: the 162 papers | 0.92 to 0.96 | 0.82 to 0.85 |
| `old`: the 157 the champion was tested on | usually a point or two lower | around 0.89, its `f1_baseline` |

`promotion_gate` logs `f1=… clears the champion's … on the 162 new
test rows - promoting`, and `approve_promotion` waits for you, as in §10. This
is the decision the approval is for: the numbers are in the prompt, the model
in MLflow. Approve it on its **Required Actions** tab, or let the default
approve it after 60 seconds, and `promote_model` moves `champion` to the new
version.

Drift treated: the new model learned from 638 of the papers, and on the other
162, which neither model has seen, it scores 8 to 14 points above the champion.

Ask the monitor once more:

```bash
docker compose exec airflow-scheduler airflow dags test monitoring
```

**Checkpoint.** `decide` logs `only 0 new rows since version … was trained -
nothing to measure`, with the version just promoted, and takes
`continue_serving` in seconds. The new champion was built on all 1564 rows, so
every row is in one of its splits. The next delivery, in §14, gives the
monitor something to measure again.

Reference: [`solutions/06_monitoring_and_retraining.py`](../../solutions/06_monitoring_and_retraining.py).

### What the monitor does not see

The performance check needs the true tag of every current row; here each row
arrives with its tag, chosen by whoever submitted the project. In most systems
the truth arrives late, or never: a loan defaults months after the decision,
and most recommendations get no feedback at all. Until it arrives there are
three ways round it: a **proxy** for the label, a signal that comes sooner;
**labelling a small sample** of the new rows by hand; or **importance
weighting**, which estimates the production f1 from labelled rows, each slice
weighted by how common it now is in production. The drift tests need no
labels, which is why they are worth running even where the f1 cannot be
measured yet.

Nor does it watch the system itself: latency, throughput, error rates, CPU
and memory. Those belong to the serving layer, the service that answers
prediction requests, which this project does not have.

### What "retrain" leaves out

`trigger_retraining` fires `mlops`, which fits on whatever is in the warehouse.
That is the orchestration, not the whole retraining problem. Each of these
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
today's errors and brings back old ones. This Dag sidesteps it by always
training on the full labelled table. That works at this size, but once the
table is too big to load and train on in a single task, you will have to train
on part of it, and forgetting becomes a risk again.

That is also why `promotion_gate` in §10 compares against the production
model on the same test rows, not just a fixed threshold: a retrained model
worse than what is serving should not ship just because a monitor fired.

The `old` line of `compare_with_champion` is where forgetting shows: the new
model's f1 on the champion's own test rows. In this section's retraining run
it cost a point or two. A team that wants to limit it adds a **guardrail** to
the gate: no promotion if the f1 on the old rows falls by more than a set
margin.

### Rolling back less often

A rollback means a bad model reached production first. **Deployment
strategies** limit the damage it can do before anyone notices:

| Strategy | How the new model meets traffic |
|---|---|
| **canary** | a small share of requests (say 5%) goes to the new model; its metrics are compared with the champion's before the share grows |
| **shadow** (or *dark canary*) | the new model receives a copy of real requests and its predictions are logged, never returned; it is judged on live data with no user affected |
| **blue-green** | the new version runs alongside the old one, and traffic switches over all at once, with the old one kept ready to switch back |

They belong to the serving layer, which this project does not have: here
`champion` switches all at once. They start in the registry, though: a
`challenger` alias next to `champion` tells a serving system which version
gets the canary share or the shadow copy, and the promotion gate becomes "the
challenger's live metrics beat the champion's" instead of a test-set score.

---

← [11. Waiting: sensors, deferrable tasks, events](11-waiting-sensors-deferrable-tasks-events.md) · [Index](README.md) · [13. Deadline alerts](13-deadline-alerts.md) →
