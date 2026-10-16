## 10. The MLOps Dag

`dags/04_mlops.py` has no time schedule: each `labeled_projects` asset event
triggers it.

```
prepare_data ─► validate_training_data ─┐
create_study ───────────────────────────┤
trial_budget ───────────────────────────┴─► run_trials×N ─► best_params ─► train_best
─► evaluate_best ─► promotion_gate ─► promote_model ──────► done
                                  └─► keep_current_model ─┘
```

| Task | Tool | What it does |
|---|---|---|
| `prepare_data` | BigQuery | copies `labeled_projects` to a file this run will use throughout |
| `validate_training_data` | Great Expectations | the §9 suite again, plus checks on the train/test split |
| `create_study`, `trial_budget`, `run_trials`×N, `best_params` | **Optuna**, tracked in **MLflow** | searches the hyperparameters, spread over several tasks, and records every trial |
| `train_best`, `evaluate_best` | **MLflow** tracking | fits the final model and records it: parameters, metrics, the model file |
| `promotion_gate` → `promote_model` | **MLflow** Model Registry | decides whether the new model goes to production |

### The ML stack

Two more tools, both running in the compose stack, need no account.

**Optuna** searches hyperparameters. A **study** is one search, with one goal
(here: maximise f1). A **trial** is one attempt: Optuna picks a value for each
hyperparameter, the code trains and scores a model with them, and the score
goes back to the study. Optuna does not try every combination: it uses the
scores so far to choose the next values. The study is stored in a database,
here the `optuna` database on the stack's Postgres, so several processes can
add trials to the same study at the same time, which is what lets Airflow
spread the search over several tasks.

**MLflow** records experiments. Each training is a **run**, with its
**parameters** (the hyperparameters used), its **metrics** (f1, precision...)
and its **artifacts** (files: here a `metrics.json` and the model itself). Runs
belong to an **experiment**, `project_tagger`. The **Model Registry**, also in
MLflow, gives a model a name and numbered **versions**. An **alias** is a label
you move from one version to another. This project uses two:

```
champion             the version in production: what predict.py loads
previous_champion    the version champion pointed at before its last move
```

Promoting a model means moving `champion`; rolling back means moving it back.
No file is copied: consumers ask for `models:/project_tagger@champion` and get
whatever version the alias points at.

The MLflow server is the `mlflow` service in the compose file. It stores runs
and the registry in a SQLite file and the model files in a folder of its own,
on a Docker volume, and the tasks reach it at `http://mlflow:5000`
(`MLFLOW_TRACKING_URI`). Open its UI:

**Checkpoint.** <http://localhost:5000> shows MLflow with one experiment,
`Default`, and no runs. `project_tagger` appears with the first run.

### How the pieces talk to each other

Each column is one system, as in the §9 diagram:

```
 Airflow (Docker)            │ Postgres          │ MLflow (Docker)     │ BigQuery
 mlops Dag                   │ optuna database   │ localhost:5000      │ mlops_course
─────────────────────────────┼───────────────────┼─────────────────────┼───────────────────
                             │                   │                     │
 1 prepare_data ◄──────────────────────────────────────────────────────── labeled_projects
   saves a snapshot file     │                   │                     │
   in data/snapshots/        │                   │                     │
 2 validate_training_data    │                   │                     │
   (GX, on the snapshot)     │                   │                     │
 3 create_study ──────────────► a new study      │                     │
   and its run ───────────────────────────────────► the study's run    │
 4 run_trials ×N ─────────────► asks for params, │                     │
   each task trains a few    │  stores scores    │                     │
   models and logs each ──────────────────────────► a child run of the │
                             │                   │  study's, per trial │
 5 best_params ◄─────────────── the best trial   │                     │
   and records it ────────────────────────────────► on the study's run │
 6 train_best ────────────────────────────────────► final run, with    │
                             │                   │  the model file;    │
                             │                   │  registered as a    │
                             │                   │  new version        │
 7 evaluate_best ◄───────────────────────────────── its metrics.json   │
 8 promotion_gate ◄──────────────────────────────── champion's f1      │
 9 promote_model ─────────────────────────────────► moves champion     │
   (or keep_current_model)   │                   │  to the new version │
```

BigQuery is read once, in step 1; every later task reads the snapshot file, so
the whole run works on the same data. MLflow ends up with one run for the
study, with the trials as child runs under it, plus the final run; the model
file is uploaded only with the final run.

### Build it

New workload functions: `data.save_snapshot(df, key)` writes a dataframe to
`data/snapshots/<key>/` and `data.load_snapshot(fp)` reads it back;
`data.preprocess(df)` cleans the text and collapses every tag outside the
four classes the model learns (`config.ACCEPTED_TAGS`) into `other`;
`data.split(df)` returns a `(train, test)` pair with the same share of each tag
in both; `validate.check_split(train, test)` is a second suite, on the
split. In `train_module` (`train.py`): `create_study`, `run_trials`,
`best_trial` and `train_model`. In `registry`: `register`, `load_metrics`,
`get_production` and `promote`. `config.MIN_F1_TO_PROMOTE` is the minimum f1
for moving a model to production (0.65, or the value of `PROJECT_TAGGER_MIN_F1`
if set).

Create `dags/04_mlops.py`:

```python dags/04_mlops.py
"""Lesson 4 - the MLOps Dag: asset-triggered tuning, training, and promotion."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, Param, dag, task
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk.types import OutletEventAccessorsProtocol

from project_tagger import config, data, registry, train as train_module, validate
from project_tagger_orchestration.assets import LABELED_PROJECTS, PRODUCTION_MODEL

DEFAULT_ARGS = {
    "owner": "ml-engineering",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=2),
}


def run_key(run_id: str) -> str:
    return run_id.replace(":", "-").replace("+", "-")


@dag(
    dag_id="mlops",
    schedule=[LABELED_PROJECTS],
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "mlops"],
    doc_md=__doc__,
    params={
        "n_trials": Param(20, type="integer", minimum=1, description="Optuna trials in total."),
        "n_workers": Param(4, type="integer", minimum=1, description="Tasks the trials are spread over."),
    },
)
def mlops():
```

`schedule=[LABELED_PROJECTS]` is the key line: a list of assets, and the Dag
runs when they update. Several assets in the list mean *all* of them;
`AssetAny(a, b)` means either.

`params` declares the search budget as two Dag params (§4): 20 trials in
total, spread over 4 tasks. `run_key` turns a run id such as
`manual__2026-02-01T03:00:00+00:00` into something safe in a file name and a
study name.

**Validate before you train.** `dataops` validated `labeled_projects` when it
built it, and this Dag validates it again before training on it. That is not
redundant: it was a different Dag run, possibly hours ago, this run may have
been triggered by hand, and the model is fitted on the data as it is *now*.
Add the first two tasks:

```python dags/04_mlops.py
    @task
    def prepare_data(
        *, triggering_asset_events: Mapping[Asset, Sequence[Any]], run_id: str
    ) -> str:
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")
        config.ensure_dirs()
        fp = data.save_snapshot(data.load_labeled(), run_key(run_id))
        print(f"snapshot: {fp}")
        return str(fp)

    @task
    def validate_training_data(snapshot_fp: str) -> dict[str, Any]:
        df = data.load_snapshot(snapshot_fp)
        table = validate.run_suite(df, "labeled_projects")
        split = validate.check_split(*data.split(data.preprocess(df)))
        print(f"{table.summary()} {split.summary()}")
        if not (table.passed and split.passed):
            raise AirflowFailException(
                f"Training snapshot failed validation: {table.failures + split.failures}"
            )
        return {
            "rows": len(df),
            "warnings": [r["expectation"] for r in table.failures + split.failures],
        }
```

`triggering_asset_events` works as in §8 (hence the `Mapping`, `Sequence` and
`Asset` imports); on a run you start by hand it is empty. `run_id` is another
context key (§4), received by name.

`prepare_data` reads the warehouse table once and writes it to a **snapshot**,
a file named after the run, and every later task reads that file. The table can
change while a run is in flight (a `dataops` run can finish in the middle), and
the snapshot keeps every task of this run on the same rows, including a task
that is retried an hour later. The task returns the file's path: XComs carry a
pointer to the data, not the data itself (§6).

The second half of `validate_training_data`'s check is one a table-level suite
cannot do. It looks at the **split**:

```python
# src/project_tagger/validate.py — check_split()
("split_is_non_empty",          ...,  critical)
("classes_in_both_splits",      ...,  critical)   # a class only in train
("min_rows_per_class:20",       ...,  critical)   # 3 rows of a class in train
("no_id_leakage",               ...,  critical)   # the same project in both
("test_fraction_between_0.1_and_0.4", ..., non-critical)
```

Each problem in those comments — a class only in train, three rows of a class,
the same project in both splits — passes a column-level suite without a
warning, and still produces a model whose f1 on the test set means nothing.
That f1 is what `promotion_gate`, later in this section, compares against the
production model. These checks span two dataframes, which a GX suite (one
batch of data at a time) does not express, so they are plain Python returning
the same result shape.

**Distributed tuning.** One Optuna study for the run, then **dynamic task
mapping**: while the Dag runs, Airflow creates one task instance per item of a
list, and each adds trials to the same study. Four tasks:

```python dags/04_mlops.py
    @task
    def create_study(*, run_id: str) -> str:
        return train_module.create_study(f"{config.REGISTERED_MODEL}-{run_key(run_id)}")

    @task
    def trial_budget(*, params: Mapping[str, Any]) -> list[int]:
        base, extra = divmod(params["n_trials"], params["n_workers"])
        return [base + (1 if i < extra else 0) for i in range(params["n_workers"]) if base or i < extra]

    @task(max_active_tis_per_dag=2)
    def run_trials(n_trials: int, study_name: str, snapshot_fp: str) -> list[dict[str, Any]]:
        train_df, _ = data.split(data.preprocess(data.load_snapshot(snapshot_fp)))
        results = train_module.run_trials(study_name, n_trials, train_df)
        print(f"{len(results)} trials, best here f1={max(r['f1'] for r in results):.4f}")
        return results

    @task
    def best_params(study_name: str, results: list[list[dict[str, Any]]]) -> dict[str, Any]:
        best = train_module.best_trial(study_name)
        print(f"best trial #{best['number']} f1={best['f1']:.4f} of {best['num_trials']}: {best['params']}")
        return best
```

`create_study` creates the study with `load_if_exists=True` (in `train.py`),
so a retried task loads the study instead of failing on it. It also opens the
study's run in MLflow, the **parent run** of every trial, and keeps that run's
id on the study itself, as a *user attribute* (a value Optuna stores with the
study). A retry finds the id there and opens no second run.

`trial_budget` reads the Dag params through the `params` context key and splits
the trials over `n_workers` tasks: 20 over 4 gives `[5, 5, 5, 5]`. In the wiring
below, `run_trials.partial(...).expand(n_trials=trial_budget())` creates one
`run_trials` instance per item of that list, when the Dag runs. Each instance
is retried, logged and inspected on its own. `.partial()` takes the arguments
every instance shares; `.expand()` the one that differs.

`run_trials` scores each trial on a validation split carved out of the
training data, and logs every trial to MLflow as a **child run**: a run with a
parent, which the MLflow UI folds under it. The test split is never seen during
the search, so the final model's test score is still an honest estimate.

A child run is usually opened with `mlflow.start_run(nested=True)` inside the
parent's `with mlflow.start_run():` block. That only works in the process that
opened the parent, and here the parent was opened by `create_study` and each
trial runs in one of the `run_trials` instances. So `train.py` names the parent
explicitly, `mlflow.start_run(parent_run_id=...)`, with the id read from the
study.

`best_params` asks the study for its best trial, and records it on the study's
run: its score as the metric `best_f1`, its run id as the tag
`best_trial_run_id`. It takes the mapped results anyway: that argument is what
makes it wait for every `run_trials` instance, and it arrives as a list, one
item per instance.

**How many `run_trials` instances run at once is a choice, written in
`run_trials`' decorator.** Nothing about `.expand()` says "in parallel"; it
says "these are four separate task instances". Whether they overlap is the
executor's business, bounded by the narrowest of the limits from §3, and
`max_active_tis_per_dag=2` on that task is deliberately the narrowest one here.
So the fan-out runs **two at a time, in two waves**. Raise the cap and all four overlap; remove it and `[core]
max_active_tasks_per_dag` (16) is the next limit up. A cap on a training task
is not a toy setting: four instances at once is four copies of the data in
memory. When the limited resource is shared — say, a database that accepts only
a few connections — a `pool` (§3) sets the same kind of cap across every task
and Dag that uses it.

> **`params` is reserved.** A `@task` receives context keys by parameter name
> (§4), and `params` is one of them: `trial_budget` gets the Dag's params
> because its argument is called `params`. Name a data argument `params` by
> mistake and it receives the Dag's params instead of your data.

Now the final model, and the **promotion gate**, which decides whether it goes
to production. Unlike the gates in §9, the gate does not fail the task when the
answer is no: it is a *branch task*, a task that picks which of the Dag's two
**branches** continues. Add `train_best`, `evaluate_best`, `promotion_gate`,
`promote_model` and the two `EmptyOperator`s the branches need:

```python dags/04_mlops.py
    @task
    def train_best(best: dict[str, Any], snapshot_fp: str, *, run_id: str) -> dict[str, Any]:
        train_df, test_df = data.split(data.preprocess(data.load_snapshot(snapshot_fp)))
        trained = train_module.train_model(
            best["params"], train_df, test_df,
            run_name=f"final-{run_key(run_id)}",
            tags={"airflow_run_id": run_id, "best_trial_run_id": best["mlflow_run_id"]},
        )
        version = registry.register(trained["model_uri"])
        print(f"run={trained['run_id']} version={version} f1={trained['metrics']['f1']:.4f}")
        return {"run_id": trained["run_id"], "version": version, "f1": trained["metrics"]["f1"]}

    @task
    def evaluate_best(trained: dict[str, Any]) -> dict[str, Any]:
        metrics = registry.load_metrics(trained["run_id"])
        for name, slice_metrics in metrics.get("slices", {}).items():
            print(f"slice {name}: f1={slice_metrics['f1']:.4f} n={slice_metrics['support']}")
        return {**trained, "metrics": metrics}

    @task.branch
    def promotion_gate(evaluation: dict[str, Any]) -> str:
        f1 = evaluation["metrics"]["f1"]
        threshold = config.MIN_F1_TO_PROMOTE
        current = registry.get_production()
        current_f1 = current["metrics"]["f1"] if current else 0.0

        if f1 < threshold:
            print(f"f1={f1:.4f} below threshold {threshold} - not promoting")
            return "keep_current_model"
        if f1 < current_f1:
            print(f"f1={f1:.4f} below production {current_f1:.4f} - not promoting")
            return "keep_current_model"
        return "promote_model"

    @task(outlets=[PRODUCTION_MODEL])
    def promote_model(
        evaluation: dict[str, Any], *, outlet_events: OutletEventAccessorsProtocol
    ) -> dict[str, Any]:
        champion = registry.promote(evaluation["version"])
        outlet_events[PRODUCTION_MODEL].extra = {
            "version": champion["version"],
            "f1": champion["metrics"]["f1"],
            "previous_version": champion["previous_version"],
        }
        print(f"champion -> version {champion['version']} (was {champion['previous_version']})")
        return champion

    keep_current_model = EmptyOperator(task_id="keep_current_model")
    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")
```

`train_best` fits the final model on the whole training split with the best
trial's hyperparameters, scores it on the test split, and logs it to MLflow,
model file included, as a run of its own, outside the study: it is not one more
attempt of the search. Its tags link it to the Airflow run that made it and to
the trial its hyperparameters came from. Then it **registers** it: every final model becomes a new
version of `project_tagger`, promoted or not, so a rejected candidate is still
there to inspect. The task returns a run id, a version number and an f1, small
and JSON-serialisable. The model itself stays in MLflow.

`evaluate_best` reads back the `metrics.json` the run logged and prints the f1
on each **evaluation slice**: a subgroup of the test set picked out by a rule
in `evaluate.py` (short texts, and texts that mention transformers, BERT or
LLMs). These are not the time slices of §7; they catch a model whose overall f1
looks fine while one kind of input fails.

A `@task.branch` returns the `task_id` to run; the other branch is **skipped**,
not failed. A candidate that misses the threshold, or scores below the current
champion, is a normal outcome: the run succeeds and production is left alone.
`promote_model` moves the `champion` alias and emits the second asset,
`production_model`, which nothing consumes yet; point a Dag's `schedule` at it
and the edge appears in the Assets graph.

**Trigger rules** are how a task states what its upstream tasks must have done
before it runs, and branching is where you first need one. The join, `done`,
needs `none_failed_min_one_success`, because the default `all_success` would
skip it as soon as either branch is skipped. The useful ones: `all_success` (default),
`all_done` (run whatever happened upstream — cleanup tasks),
`none_failed_min_one_success` (joins after a branch), `none_failed` (allows skips
but not failures), `one_success`, `one_failed` (alerting paths), `all_skipped`,
and `always`.

Finish the file with the wiring:

```python dags/04_mlops.py
    snapshot = prepare_data()
    gate = validate_training_data(snapshot)
    study = create_study()
    results = run_trials.partial(study_name=study, snapshot_fp=snapshot).expand(
        n_trials=trial_budget()
    )
    gate >> results

    evaluation = evaluate_best(train_best(best_params(study, results), snapshot))
    decision = promotion_gate(evaluation)

    decision >> promote_model(evaluation) >> done
    decision >> keep_current_model >> done


mlops()
```

`gate >> results` wires `validate_training_data` upstream of *every* mapped
instance of `run_trials`, carrying no data, only the order: no trial starts on
data that failed its checks. The two `decision >>` lines give `promotion_gate`
its two branches and join them at `done`.

### See the wiring

Save it. You have now written both Dags of the pipeline, `dataops` and
`mlops`, and before running anything you can ask Airflow what it knows:

```bash
docker compose exec airflow-scheduler airflow assets details --name labeled_projects -o plain
```

**Checkpoint.** Among the rows:

```
scheduled_dags    [{'dag_id': 'mlops', ...}]
producing_tasks   [{'dag_id': 'dataops', 'task_id': 'validate_transformed', ...}]
```

`producing_tasks` names the exact task that emits it; `scheduled_dags` names
what will start when it does. Neither Dag mentions the other anywhere. The
Assets page now draws the whole graph, `dataops → labeled_projects → mlops →
production_model`.

`mlops`'s own page can show the same connections. Open it, switch to **Graph**,
and change **Options → Dependencies** (§5):

- **Only tasks** — the twelve tasks you just wrote, nothing else.
- **External conditions** — the same tasks, plus `labeled_projects` flowing
  into the Dag and `production_model` coming out of `promote_model`.
- **All Dag Dependencies** — the chain the Assets page draws: `dataops` with
  the assets it produces, then `mlops` with its tasks inside it, then
  `production_model`.

### Watch the whole chain fire

This is the payoff of §8. Un-pause the consumer, `mlops`, first, so it is
listening, then the producer's Dag, `dataops`:

```bash
docker compose exec airflow-scheduler airflow dags unpause mlops
docker compose exec airflow-scheduler airflow dags unpause dataops
```

Nothing happens on the first line: `mlops`'s schedule is an asset, and no
event has arrived. The second line starts everything without a trigger, by the
§7 rule: `dataops` has a cron and a 2025 `start_date`, so the moment it is
un-paused the scheduler creates the run for the most recent 03:00 UTC it
missed, and starts it. Give it a few minutes, then:

```bash
docker compose exec airflow-scheduler airflow dags list-runs dataops -o plain
docker compose exec airflow-scheduler airflow dags list-runs mlops -o plain
```

**Checkpoint.** `dataops` has a run whose id begins `scheduled__` and ends in
`T03:00:00+00:00` — today's date, or yesterday's if it is before 03:00 UTC —
with that as its `logical_date` and a `start_date` of a few minutes ago. (The
`manual__` run from `airflow dags test` in §9 is there too.) And `mlops` has a
run nobody triggered: id beginning `asset_triggered__`, `running` if you were
quick, `success` once its trials and the final model are done, with an **empty**
`logical_date`: asset-triggered runs have no logical date, like a run
triggered from the CLI without one (§7). In the UI, **Assets** shows the edge
that caused it.

Know where the wait comes from. Almost all of it is the tools: the two Airbyte
syncs in `dataops`, then twenty trials and a final fit in `mlops`. The Airflow
part is small: one scheduler loop to create the `dataops` run after the
un-pause, and one more to create the `mlops` run once the asset event is
recorded (§3, §8). If nothing has appeared after a few minutes, check that both
Dags are un-paused and that the `dataops` run succeeded — a failed
`validate_transformed` emits no event, by design.

Now the two waves of `run_trials`. Open that `mlops` run in the UI, click
`run_trials`, and read its **Task Instances** tab: four rows, map index 0 to 3,
each with its own start and end time. Two start together; each of the other
two starts only when one of the first pair has finished. The *Show Gantt* view
(§5), available now that a run is selected, draws the same thing as two waves
of bars. On the **Graph**, the four are one collapsed node until you raise the
**Min. map index** filter (§5). Or read it off the CLI:

```bash
docker compose exec airflow-scheduler airflow tasks states-for-dag-run mlops <the asset_triggered__ run id> -o plain
```

Four `run_trials` rows with `map_index` 0–3; 2 and 3 each start after one of
0 and 1 has ended. And `keep_current_model` is `skipped` while `promote_model`
is `success`: `promotion_gate` took the `promote_model` branch, because the
final model's f1 cleared the threshold and there was no production model to
beat.

Then look at what the run left in MLflow, at <http://localhost:5000>:

**Checkpoint.** The `project_tagger` experiment shows two runs:
`project_tagger-asset_triggered__…`, the study, and `final-asset_triggered__…`.
Expand the study's run to see its twenty children, `…-trial-0` to `-trial-19`,
and sort by the `f1` column to see the search at work. Open the study's run:
`best_f1` is the best trial's score, and the tag `best_trial_run_id` names it.
Open the final run: its parameters are the best trial's, its metrics include
the per-tag and per-slice f1, and under **Artifacts** are `metrics.json` and
the model. Under **Models**, `project_tagger` has version 1, with the alias
`champion`.

> **Try it, once more by hand:**
>
> ```bash
> docker compose exec airflow-scheduler airflow dags trigger dataops
> ```
>
> A `manual__` run this time. When it succeeds, one more event, one more
> `asset_triggered__` run of `mlops`, and one more model version in MLflow.
> Every success of the producer emits an event, whichever way the producer
> was started — and the consumer runs on it (events that arrive while `mlops`
> is busy share its next run, §8). Whether version 2 becomes `champion`
> depends on its f1 against version 1's: same data, a different search, so it
> can go either way, and `promotion_gate`'s log says which.

> **Try it, the other branch.** Set the bar impossibly high and watch
> `promotion_gate` take the `keep_current_model` branch. The threshold is read
> from an environment variable inside the task's own process, and
> `airflow dags test` runs the tasks in its own process, so pass the variable
> to that command with `-e`:
>
> ```bash
> docker compose exec -e PROJECT_TAGGER_MIN_F1=0.99 airflow-scheduler airflow dags test mlops
> ```
>
> `promotion_gate` logs `below threshold 0.99 - not promoting`, `promote_model`
> is skipped, `keep_current_model` runs, and the Dag run still succeeds. In
> MLflow the new version is registered, and `champion` has not moved.

Stuck anywhere? [`solutions/04_mlops.py`](../../solutions/04_mlops.py).

---

← [9. The DataOps Dag](09-the-dataops-dag.md) · [Index](README.md) · [11. Waiting: sensors, deferrable tasks, events](11-waiting-sensors-deferrable-tasks-events.md) →
