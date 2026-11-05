## 10. The MLOps Dag

`dags/04_mlops.py` has no time schedule: each `labeled_projects` asset event
triggers it.

```
prepare_data ─► validate_training_data ─┐
create_study ───────────────────────────┤
trial_budget ───────────────────────────┴─► run_trials×N ─► best_params ─► train_best
─► compare_with_champion ─┬─► summarize_candidate ─┐
                          └─► promotion_gate ─┬────┴─► approve_promotion ─► promote_model ──► done
                                              └─► keep_current_model ──────────────────────┘
```

| Task | Tool | What it does |
|---|---|---|
| `prepare_data` | BigQuery | copies `labeled_projects` to files this run will use throughout, split once into train, validation and test |
| `validate_training_data` | Great Expectations | the §9 suite again, plus checks on the split |
| `create_study`, `trial_budget`, `run_trials`×N, `best_params` | **Optuna**, tracked in **MLflow** | searches the hyperparameters, spread over several tasks, and records every trial |
| `train_best` | **MLflow** tracking | fits the final model and records it: parameters, metrics, the model file |
| `compare_with_champion` | **MLflow** Model Registry | scores the new model and the production model on this run's test split |
| `summarize_candidate` | Airflow | writes up the new model's numbers for the person who approves it |
| `promotion_gate` → `approve_promotion` → `promote_model` | **MLflow** Model Registry, and a person | the gate decides whether the new model may go to production, a person approves it in the Airflow UI, and the registry moves it there |

### The ML stack

Two more tools, neither needing an account: Optuna is a library the tasks
import, and MLflow is a service in the compose stack.

**Optuna** searches hyperparameters. A **study** is one search, with one goal
(here: maximise f1). A **trial** is one attempt: Optuna picks a value for each
hyperparameter, the code trains and scores a model with them, and the score
goes back to the study. Optuna does not try every combination: it uses the
scores so far to choose the next values. The study lives in the `optuna`
database on the stack's Postgres, so several processes can add trials to it at
once: that is what lets Airflow spread the search over several tasks.

**MLflow** records experiments. Each training is a **run**, with its
**parameters** (the hyperparameters used), its **metrics** (f1, precision...)
and its **artifacts** (files: here `metrics.json`, `split_ids.json` and the
model itself). Runs belong to an **experiment**, `project_tagger`. The
**Model Registry**, also in MLflow, gives a model a name and numbered
**versions**. An **alias** is a label you move from one version to another.
This project uses two:

```
champion             the version in production: what predict.py loads
previous_champion    the version champion pointed at before the last promotion
```

Promoting a model means moving `champion`; rolling back means moving it back.
No file is copied: consumers ask for `models:/project_tagger@champion` and get
whatever version the alias points at.

The MLflow server is the `mlflow` compose service. It keeps runs and the
registry in a SQLite file and the model files in a folder, both on a Docker
volume; tasks reach it at `http://mlflow:5000` (`MLFLOW_TRACKING_URI`). Open
its UI:

**Checkpoint.** <http://localhost:5000> shows MLflow with one experiment,
`Default`, and no runs. `project_tagger` doesn't exist yet:
`mlflow.set_experiment` in `src/project_tagger/train.py` creates it the first
time the `mlops` Dag logs a run.

### How the pieces talk to each other

Each column is one system, as in the §9 diagram:

```
 Airflow (Docker)            │ Postgres          │ MLflow (Docker)     │ BigQuery
 mlops Dag                   │ optuna database   │ localhost:5000      │ mlops_course
─────────────────────────────┼───────────────────┼─────────────────────┼───────────────────
                             │                   │                     │
 1 prepare_data ◄──────────────────────────────────────────────────────── labeled_projects
   saves a snapshot and its  │                   │                     │
   three splits in           │                   │                     │
   data/snapshots/           │                   │                     │
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
 7 compare_with_champion ◄───────────────────────── the champion model │
   scores both on the test   │                   │  and the new one    │
   split, new rows and old   │                   │                     │
 8 summarize_candidate       │                   │                     │
   the numbers for the       │                   │                     │
   approval prompt           │                   │                     │
 9 promotion_gate            │                   │                     │
   threshold, then champion  │                   │                     │
10 promote_model ─────────────────────────────────► moves champion     │
   (or keep_current_model)   │                   │  to the new version │
```

BigQuery is read once, in step 1; every later task reads the snapshot files,
so the whole run works on the same data. MLflow ends up with the study's run,
the trials as its child runs, and the final run, the only one with the model
file.

### Links to MLflow

A run of this Dag leaves three things in MLflow worth opening: the study's
run, the final run and the new version. An **extra link** is a button on a
task instance's page that opens an address outside Airflow. The links go
in the shared package of §8, not in the Dag file, so `monitoring` (§12) can
use them too. Two tasks of the Dag, `best_params` and `train_best`, get them.
Create `dags/project_tagger_orchestration/links.py`:

```python dags/project_tagger_orchestration/links.py
"""Extra links: buttons on a task instance that open, in the MLflow UI, what the task made."""

from __future__ import annotations

from airflow.sdk import BaseOperatorLink
from airflow.sdk.execution_time.xcom import XCom

from project_tagger import registry


class MlflowRunLink(BaseOperatorLink):
    name = "MLflow run"

    def __init__(self, field: str = "run_id") -> None:
        self.field = field

    def get_link(self, operator, *, ti_key) -> str:
        result = XCom.get_value(ti_key=ti_key, key="return_value")
        return registry.run_url(result[self.field]) if result else ""


class MlflowModelVersionLink(BaseOperatorLink):
    name = "MLflow model version"

    def get_link(self, operator, *, ti_key) -> str:
        result = XCom.get_value(ti_key=ti_key, key="return_value")
        return registry.version_url(result["version"]) if result else ""
```

A link is a class with a `name`, the button's label, and a `get_link` method
that returns the address. Airflow calls `get_link` on the worker once the task
has finished and keeps the address in XCom, under a key of its own
(`_link_MlflowRunLink`). The UI reads it from there, so the button appears
when the task has finished. `XCom.get_value` reads the task's return value,
which holds the run id or the version this task instance made.
`MlflowRunLink` reads the field it is given: `run_id` by default, as
`train_best` returns it. `best_params` returns the study's run under
`study_run_id`. `registry.run_url` and `registry.version_url` turn the id or
version into an address in the MLflow UI, starting from
`config.MLFLOW_UI_URL`. That is `http://localhost:5000`, the address your
browser uses; `http://mlflow:5000` works only inside the Docker network.

### Build it

Create `dags/04_mlops.py`:

```python dags/04_mlops.py
"""Lesson 4 - the MLOps Dag: asset-triggered tuning, training, and promotion."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.sdk import Asset, Param, dag, task
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk.types import OutletEventAccessorsProtocol

from project_tagger import config, data, evaluate, registry, train as train_module, validate
from project_tagger_orchestration.assets import LABELED_PROJECTS, PRODUCTION_MODEL
from project_tagger_orchestration.links import MlflowModelVersionLink, MlflowRunLink

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

`schedule=[LABELED_PROJECTS]` is the key line: the Dag runs when the listed
assets update. Several assets in the list mean *all* of them, like
`AssetAll(a, b)`; `AssetAny(a, b)` means either.

`params` declares the search budget as two Dag params (§4): 20 trials in
total, spread over 4 tasks. They set how much is searched, not what: the
hyperparameters Optuna tunes, and their ranges, are fixed in `objective()` in
`src/project_tagger/train.py`:

| Hyperparameter | Range | What it controls |
|---|---|---|
| `ngram_max` | 1 to 3 | longest word sequence TF-IDF turns into a feature (1 = single words only) |
| `min_df` | 1 to 3 | terms found in fewer projects than this are dropped |
| `alpha` | 1e-6 to 1e-2, on a log scale | strength of the SGD classifier's regularisation: higher gives a simpler model |

`run_key` makes a run id such as `manual__2026-02-01T03:00:00+00:00` safe to
use in file and study names.

**Snapshot the data.** Add the first task:

```python dags/04_mlops.py
    @task
    def prepare_data(
        *, triggering_asset_events: Mapping[Asset, Sequence[Any]], run_id: str
    ) -> dict[str, str]:
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")
        config.ensure_dirs()
        # Writes data/snapshots/<key>/; returns the paths "snapshot", "train", "val", "test".
        paths = data.save_snapshot(data.load_labeled(), run_key(run_id))
        print(f"snapshot: {paths}")
        return paths
```

`triggering_asset_events` works as in §8 (hence the `Mapping`, `Sequence` and
`Asset` imports); on a run you start by hand it is empty. `run_id` is another
context key (§4), received by name.

`prepare_data` reads the warehouse table once and writes a **snapshot**: files
in a folder named after the run, which every later task reads. The table can
change mid-run (a `dataops` run can finish in the middle); the snapshot keeps
every task of this run on the same rows, even one retried an hour later. The
files are Parquet, not CSV: Parquet stores each column's type, so `created_on`
comes back as a timestamp, not a string; a CSV is only text, and the types are
guessed again on every read. The task returns the paths, a small dict: XComs
carry a pointer to the data, not the data itself (§6).

`data.save_snapshot` writes four files: the table as read
(`labeled_projects.parquet`), which the §9 suite checks, and the model's input
split three ways (`train.parquet`, `val.parquet`, `test.parquet`). That input
comes from `data.preprocess`, which drops rows with no title or no tag, joins
title and description into one cleaned `text` column, and turns every tag
outside the model's four classes (`config.ACCEPTED_TAGS`) into `other`. The
search fits on train and scores each trial on validation; the final model is
fitted on both and scored on test, which nothing has seen before.

The split is **written once**. Recomputed in each task, it would depend on the
code, the libraries and the seed being identical wherever each task runs: a
task retried after a deployment could silently train on some rows and test on
others. Written once, every task reads the same rows, and the rows each model
was built on are kept.

Each row's split is **decided by its id**. `data.assign_split` hashes each
project's id into a number from 0 to 99: below 20 is test, 20 to 35
validation, the rest train (`config.TEST_PCT` and `config.VAL_PCT`). So a
project lands in the same split in every run, and no model trains on another
model's test rows. That lets `compare_with_champion`, later in this section,
score the new model and the production model on the same test rows, and lets
monitoring (§12) tell which rows the production model has never seen. The
cost: the split is no longer stratified (the same share of each tag in every
split). With hundreds of rows the shares come out close, and the split check
catches a class that ends up missing or scarce.

**Validate before you train.** `dataops` validated `labeled_projects` when it
built it; this Dag validates it again before training. Not redundant: that was
another Dag run, possibly hours ago, this run may have been triggered by hand,
and the model is fitted on the data as it is *now*. Add the second task:

```python dags/04_mlops.py
    @task
    def validate_training_data(paths: dict[str, str]) -> dict[str, Any]:
        # The table as read from the warehouse, against the labeled_projects suite.
        df = data.load_snapshot(paths["snapshot"])
        table = validate.run_suite(df, "labeled_projects")
        # The three preprocessed splits (train, val, test), checked against each other.
        split = validate.check_split(*(data.load_snapshot(paths[name]) for name in data.SPLITS))
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

A table-level suite cannot do the split check:

```python
# src/project_tagger/validate.py — check_split()
("split_is_non_empty",          ...,  critical)
("classes_in_every_split",      ...,  critical)   # a class missing from test
("min_rows_per_class:20",       ...,  critical)   # 3 rows of a class in train + val
("no_id_leakage",               ...,  critical)   # the same project in two splits
("fractions_near_target",       ...,  non-critical)
```

Each problem in those comments passes a column-level suite without a warning,
yet makes the test f1 meaningless, and that f1 is what `promotion_gate`
compares against the production model. The minimum rows per class counts train
and validation together, because the final model learns from both. These
checks span three dataframes, and a GX suite validates one batch at a time, so
they are plain Python returning the same result shape as the suite.

**Distributed tuning.** One Optuna study for the run, then **dynamic task
mapping**: while the Dag runs, Airflow creates one task instance per item of a
list, and each adds trials to the same study. First the study, and the list:

```python dags/04_mlops.py
    @task
    def create_study(*, run_id: str) -> str:
        return train_module.create_study(f"{config.REGISTERED_MODEL}-{run_key(run_id)}")

    @task
    def trial_budget(*, params: Mapping[str, Any]) -> list[int]:
        base, extra = divmod(params["n_trials"], params["n_workers"])
        return [base + (1 if i < extra else 0) for i in range(params["n_workers"]) if base or i < extra]
```

`create_study` uses `load_if_exists=True` (in `train.py`), so a retry loads the
study instead of failing on it. It also opens the study's run in MLflow, the
**parent run** of every trial, and stores that run's id on the study as a
*user attribute* (a value Optuna keeps with the study), so a retry finds the id
there and opens no second run.

`trial_budget` reads the Dag params through the `params` context key and splits
the trials over `n_workers` tasks: 20 over 4 gives `[5, 5, 5, 5]`, the list the
next task is mapped over.

> **`params` is reserved.** A `@task` receives context keys by parameter name
> (§4), and `params` is one of them: `trial_budget` gets the Dag's params
> because its argument is called `params`. A data argument named `params` gets
> them too, instead of your data.

Now the mapped task:

```python dags/04_mlops.py
    @task(max_active_tis_per_dag=2)
    def run_trials(n_trials: int, study_name: str, paths: dict[str, str]) -> list[dict[str, Any]]:
        train_df, val_df = data.load_snapshot(paths["train"]), data.load_snapshot(paths["val"])
        results = train_module.run_trials(study_name, n_trials, train_df, val_df)
        print(f"{len(results)} trials, best here f1={max(r['f1'] for r in results):.4f}")
        return results
```

Nothing in it mentions mapping: `run_trials` is written for one instance, and
its `n_trials` is one number, not the list. The mapping is in the wiring at the
end of the file:

```python
run_trials.partial(study_name=study, paths=paths).expand(n_trials=trial_budget())
```

- `.partial()` takes the arguments every instance gets the same: the study's
  name and the snapshot's paths.
- `.expand()` takes the argument that differs, as a list, and makes one
  instance per item.

With `trial_budget` returning `[5, 5, 5, 5]`, that line becomes four task
instances, told apart by their **map index**:

| Map index | `n_trials` | `study_name`, `paths` |
|---|---|---|
| 0 | `5` (item 0) | the same for all four |
| 1 | `5` (item 1) | the same for all four |
| 2 | `5` (item 2) | the same for all four |
| 3 | `5` (item 3) | the same for all four |

The list only exists once `trial_budget` has run, so the number of instances
is decided when the Dag runs, not when it is parsed: set `n_workers` to 2 in a
run's params and that run has two. Each instance is retried, logged and
inspected on its own; a retried instance reruns only its own trials.

Each instance fits its trials on train, scores them on validation, and logs
each trial to MLflow as a **child run**: a run with a parent, which the MLflow
UI folds under it. The search never sees test, so the final model's test score
stays an honest estimate.

A child run is usually opened with `mlflow.start_run(nested=True)` inside the
parent's `with mlflow.start_run():` block, which only works in the process
that opened the parent. Here `create_study` opened it and the trials run in
the `run_trials` instances, so `train.py` names the parent explicitly:
`mlflow.start_run(parent_run_id=...)`, with the id read from the study.

**How many `run_trials` instances run at once is set in its decorator.**
`.expand()` does not say "in parallel", only "four separate task instances".
Whether they overlap is up to the Executor, bounded by the narrowest of the
§3 limits, and here that is deliberately `max_active_tis_per_dag=2`. So the
fan-out runs **two at a time, in two waves**. Raise the cap and all four
overlap; remove it and `[core] max_active_tasks_per_dag` (16) is the next
limit up. Capping a training task is not a toy setting: four instances at once
means four copies of the data in memory. When the limited resource is shared,
say a database that accepts only a few connections, a `pool` (§3) sets the
same kind of cap across every task and Dag that uses it.

Last, the task that reads the search's result:

```python dags/04_mlops.py
    @task
    def best_params(study_name: str, results: list[list[dict[str, Any]]]) -> dict[str, Any]:
        # Also records it on the study's run: metric best_f1, tag best_trial_run_id.
        best = train_module.best_trial(study_name)
        print(f"best trial #{best['number']} f1={best['f1']:.4f} of {best['num_trials']}: {best['params']}")
        return best
```

`best_params` asks the study for its best trial. It takes the mapped results
anyway: that argument is what makes it wait for every `run_trials` instance,
and it arrives as a list, one item per instance, in map-index order.

Now the final model. Add `train_best`:

```python dags/04_mlops.py
    @task
    def train_best(best: dict[str, Any], paths: dict[str, str], *, run_id: str) -> dict[str, Any]:
        splits = {name: data.load_snapshot(paths[name]) for name in data.SPLITS}
        trained = train_module.train_model(
            best["params"], splits,
            run_name=f"final-{run_key(run_id)}",
            tags={
                "airflow_run_id": run_id,
                "best_trial_run_id": best["mlflow_run_id"],
                "snapshot_path": paths["snapshot"],
            },
        )
        version = registry.register(trained["model_uri"])
        print(f"run={trained['run_id']} version={version} f1={trained['metrics']['f1']:.4f}")
        return {
            "run_id": trained["run_id"],
            "version": version,
            "f1": trained["metrics"]["f1"],
            "slices": trained["metrics"]["slices"],
        }
```

`train_best` fits the final model with the best trial's hyperparameters on
train and validation together (validation has done its job, choosing those
hyperparameters, and more rows make a better model), scores it on test, and
logs it to MLflow, model file included, as a run of its own outside the study:
it is not one more attempt of the search. Its tags link it to the Airflow run
that made it, the trial its hyperparameters came from, and its snapshot
(`snapshot_path`). The run also gets `split_ids.json`, the project ids in each
split, so what the model was built on stays in MLflow after the snapshot
folder is gone; §12 reads it. Then the task **registers** the model: every
final model becomes a new version of `project_tagger`, promoted or not, so a
rejected candidate is still there to inspect. The task returns only small,
JSON-serialisable values (run id, version number, f1, slice metrics); the
model itself stays in MLflow.

The slice metrics are the f1 on each **evaluation slice**: a subgroup of the
test set picked by a rule in `evaluate.py` (short texts; texts that mention
transformers, BERT or LLMs). These are not the time slices of §7: they catch a
model whose overall f1 looks fine while one kind of input fails. They are
small: of this section's 157 test rows, 4 are short texts and 23 mention
transformers, so one prediction moves a slice's f1 a lot. Too noisy for a
fixed rule; a person can weigh them, which is what `approve_promotion` is for.

Then the comparison with the production model. Add `compare_with_champion`:

```python dags/04_mlops.py
    @task
    def compare_with_champion(trained: dict[str, Any], paths: dict[str, str]) -> dict[str, Any] | None:
        champion = registry.get_production()
        if champion is None:
            print("no champion yet")
            return None
        seen = [i for ids in registry.load_split_ids(champion["run_id"]).values() for i in ids]
        comparison = evaluate.compare(
            registry.load_model(version=trained["version"]),
            registry.load_model(),
            data.load_snapshot(paths["test"]),
            seen,
        )
        for name, part in comparison.items():
            if part["rows"]:
                print(
                    f"{name} test rows: {part['rows']}, f1={part['f1']:.4f}, "
                    f"champion version {champion['version']} f1={part['champion_f1']:.4f}"
                )
        return comparison
```

`compare_with_champion` scores the production model and the version
`train_best` just registered on this run's test split. The f1 the champion
logged when it was trained was measured on another run's test rows; comparing
it with the new model's would compare two exams, not two models. On the same
rows the comparison is fair, and the hash split makes those rows honest for both: no test row has
ever been in any model's training data, the champion's included.

`evaluate.compare` also splits the test rows by the champion's
`split_ids.json`, and scores both models on each part:

| Part | Rows | What it says |
|---|---|---|
| `new` | test rows whose id is in none of the champion's splits: they reached the warehouse after it was trained | how each model does on data like what production gets next |
| `old` | the champion's own test rows | whether the new model kept what the champion could do |
| `all` | both | the whole split |

A retrain exists to do better on new data, so the `new` rows decide: the new
model's f1 on them must not be below the champion's. With fewer than 30 new
rows (`config.MIN_NEW_TEST_ROWS`), too few to decide on, the whole split
decides instead. That is the case in this section, where every run sees the
same 764 rows; new rows arrive in §12. The `old` part is only logged; §12
comes back to it.

`compare_with_champion` is a task of its own so that the Graph shows it and
its log names the version it compared with, part by part. It takes
`train_best`'s output, which carries the new version, so it runs after it.

Next, the **promotion gate**, which decides whether the new model goes to
production. Unlike the §9 gates, it does not fail when the answer is no: it is
a *branch task*, which picks which of the Dag's two **branches** continues.
Add `summarize_candidate` and `promotion_gate`:

```python dags/04_mlops.py
    @task
    def summarize_candidate(trained: dict[str, Any], comparison: dict[str, Any] | None) -> str:
        summary = evaluate.summarize(trained["version"], trained["f1"], trained["slices"], comparison)
        print(summary)
        return summary

    @task.branch
    def promotion_gate(trained: dict[str, Any], comparison: dict[str, Any] | None) -> str:
        promote, reason = evaluate.should_promote(
            trained["f1"],
            comparison,
            config.MIN_F1_TO_PROMOTE,
            config.MIN_NEW_TEST_ROWS,
        )
        print(reason)
        return "approve_promotion" if promote else "keep_current_model"
```

`summarize_candidate` writes up, with `evaluate.summarize`, what a person needs
to judge the new model: its f1, both models' f1 on each part of the test split,
and the f1 on each slice next to its row count. The task returns that Markdown
text, so it goes to XCom for `approve_promotion` to show.

`promotion_gate` applies the rule in `evaluate.should_promote`: the new
model's f1 must clear the threshold, `config.MIN_F1_TO_PROMOTE` (0.65, or the
environment variable `PROJECT_TAGGER_MIN_F1` if set), and, if there is a
champion, must not fall below it on the new rows, or on the whole split when
there are too few. A `@task.branch` returns the `task_id` to run; the other
branch is **skipped**, not failed. A candidate that misses the threshold or
loses to the champion is a normal outcome: the run succeeds and production is
left alone.

Then the gate's two branches and the task that joins them. Add
`approve_promotion`, `promote_model` and two `EmptyOperator`s,
`keep_current_model` and `done`:

```python dags/04_mlops.py
    approve_promotion = ApprovalOperator(
        task_id="approve_promotion",
        subject="Promote the new tag classifier?",
        body=(
            "The new version passed promotion_gate.\n\n"
            "{{ ti.xcom_pull(task_ids='summarize_candidate') }}\n\n"
            "Approve to move `champion` to it."
        ),
        defaults="Approve",
        response_timeout=timedelta(seconds=60),
    )

    @task(outlets=[PRODUCTION_MODEL])
    def promote_model(
        trained: dict[str, Any], *, outlet_events: OutletEventAccessorsProtocol
    ) -> dict[str, Any]:
        champion = registry.promote(trained["version"])
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

A model the gate lets through still waits for a person: `approve_promotion` is
a **human-in-the-loop** task. `ApprovalOperator` parks it in `awaiting_input`
and shows a prompt on the task instance's **Required Actions** tab and under
Browse → **Required Actions** (§5). On **Approve**, `promote_model` runs; on
**Reject**, it is skipped and production is left alone. `defaults` is the
answer applied if nobody responds within `response_timeout`: sixty seconds
here so this section's runs finish; give it hours in production. `body`, the
prompt's Markdown text, is a template field (§6): `ti.xcom_pull` puts
`summarize_candidate`'s text in it when the task runs. The gate checks the
numbers it has a rule for; the person checks what the rules miss, such as a
slice that fell, with that summary and the new version in MLflow in front of
them. Airflow records the choice, when, and *by whom*. A run that timed out
and fell back to its `defaults` leaves the "who" empty: that is how you tell
"a person approved this" from "nobody was looking".

`promote_model` moves the `champion` alias and emits `mlops`'s own asset,
`production_model`. No Dag in this tutorial consumes it; §16 shows what
would.

A **trigger rule** states what a task's upstream tasks must have done before
it runs; branching is where you first need one. The join, `done`, needs
`none_failed_min_one_success`, because the default `all_success` would skip it
as soon as either branch is skipped. The useful ones: `all_success` (default),
`all_done` (run whatever happened upstream — cleanup tasks),
`none_failed_min_one_success` (joins after a branch), `none_failed` (allows
skips but not failures), `one_success`, `one_failed` (alerting paths),
`all_skipped`, and `always`.

Finish the file with the wiring:

```python dags/04_mlops.py
    paths = prepare_data()
    approved_data_gate = validate_training_data(paths)
    study = create_study()
    results = run_trials.partial(study_name=study, paths=paths).expand(
        n_trials=trial_budget()
    )
    approved_data_gate >> results

    best = best_params(study, results)
    trained = train_best(best, paths)
    best.operator.operator_extra_links = [MlflowRunLink("study_run_id")]
    trained.operator.operator_extra_links = [MlflowRunLink(), MlflowModelVersionLink()]
    comparison = compare_with_champion(trained, paths)
    summary = summarize_candidate(trained, comparison)
    decision = promotion_gate(trained, comparison)

    [summary, decision] >> approve_promotion >> promote_model(trained) >> done
    decision >> keep_current_model >> done


mlops()
```

`approved_data_gate >> results` puts `validate_training_data` upstream of
*every* mapped instance of `run_trials`, carrying only the order, no data: no
trial starts on data that failed its checks. `promotion_gate` takes the results
of `train_best` and `compare_with_champion` (which itself waits for
`train_best`), so the three run in that order; `summarize_candidate` takes the
same two, so it runs alongside `promotion_gate`.
`[summary, decision] >> approve_promotion` is a fan-in (§9):
`approve_promotion` waits for both. The edge from `summary` is order-only, like
`approved_data_gate >> results`: `approve_promotion` reads the summary through
a template, not an argument, so Airflow sees no dependency on its own, and
without the edge the prompt could be built before the summary exists. The edge
from `decision`, on to `promote_model` and `done`, is `promotion_gate`'s first
branch; the last line is its second, and `done` joins them.

`@task` has no argument for extra links, so the wiring sets them on the
operator behind each task call: `best.operator` is `best_params`'s.
`best_params` gets one button, **MLflow run**, to the study's run;
`train_best` gets two, **MLflow run** and **MLflow model version**, to the
final run and the version it registered.

### See the wiring

Save it. Both Dags of the pipeline, `dataops` and `mlops`, are now written.
Airflow links them as soon as it parses the two files, without running
either: it records which task produces `labeled_projects` and which Dag waits
for it. Ask for that asset's details:

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

- **Only tasks** — the fourteen tasks you just wrote, nothing else.
- **External conditions** — the same tasks, plus `labeled_projects` flowing
  into the Dag and `production_model` coming out of `promote_model`. The
  `labeled_projects` arrow reaches only the first task, but it starts the whole
  Dag.
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
event has arrived. The second starts everything without a trigger, by the §7
rule: `dataops` has a cron and a 2025 `start_date`, so once it is un-paused
the Scheduler creates and starts the run for the most recent 03:00 UTC it
missed. Give it a few minutes. Near the end, the `mlops` run stops at
`approve_promotion`: open the run in the UI, click `approve_promotion`, and
answer the prompt on its **Required Actions** tab, under
`summarize_candidate`'s numbers, or let the default approve it after 60
seconds. Then:

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

Almost all the wait is the tools: the two Airbyte syncs in `dataops`, twenty
trials and a final fit in `mlops`, and up to a minute at `approve_promotion`.
Airflow adds little: one Scheduler loop to create the `dataops` run after the
un-pause, and one more to create the `mlops` run once the asset event is
recorded (§3, §8). If nothing has appeared after a few minutes, check that both
Dags are un-paused and that the `dataops` run succeeded — a failed
`validate_transformed` emits no event, by design.

Now the two waves of `run_trials`. Open that `mlops` run in the UI, click
`run_trials`, and read its **Task Instances** tab: four rows, map index 0 to 3,
each with its own start and end time. Two start together; each of the other
two starts when one of the first pair finishes. *Show Gantt* (§5), available
now that a run is selected, draws them as two waves of bars. On the **Graph**,
try the **Min. map index** filter (§5): at 3, every node dims except
`run_trials`, the only mapped task, which has an instance at map index 3; at 4
it dims too. Or read it off the CLI:

```bash
docker compose exec airflow-scheduler airflow tasks states-for-dag-run mlops <the asset_triggered__ run id> -o plain
```

Four `run_trials` rows with `map_index` 0–3; 2 and 3 each start after one of
0 and 1 has ended. And `keep_current_model` is `skipped` while
`approve_promotion` and `promote_model` are `success`: `promotion_gate` took
the `approve_promotion` branch, because the final model's f1 cleared the
threshold and there was no production model to beat (`compare_with_champion`
logged `no champion yet`), and the approval came from you or the default.

Then look at what the run left in MLflow. Start from Airflow: in that `mlops`
run, click `train_best` and open its **Details** tab. Under **Extra Links**,
**MLflow run** opens the final run and **MLflow model version** opens
version 1; `best_params`'s **MLflow run** opens the study's run. Or browse
from <http://localhost:5000>:

**Checkpoint.** The `project_tagger` experiment shows two runs:
`project_tagger-asset_triggered__…`, the study's run, and
`final-asset_triggered__…`, the final run.
Expand the study's run for its twenty children, `…-trial-0` to `-trial-19`,
and sort by `f1` to see the search at work. Open the study's run:
`best_f1` is the best trial's score, and the tag `best_trial_run_id` names it.
Open the final run: its parameters are the best trial's, its metrics include
the per-tag and per-slice f1, its tags include `snapshot_path`, and under
**Artifacts** are `metrics.json` and `split_ids.json`. The model is not among
them: MLflow stores it as a logged model of its own, listed on the run's
**Overview** under **Logged models** as `model`. Under
**Models**, `project_tagger` has version 1, with the alias `champion`.

> **Try it, once more by hand:**
>
> ```bash
> docker compose exec airflow-scheduler airflow dags trigger dataops
> ```
>
> A `manual__` run this time. When it succeeds: one more event, one more
> `asset_triggered__` run of `mlops`, one more model version in MLflow. Every
> success of the producer emits an event, however the producer was started,
> and the consumer runs on it (events that arrive while `mlops` is busy share
> its next run, §8). Whether version 2 becomes `champion` depends on its f1
> against version 1's on this run's test split. Same data means the same split
> (both runs' `test.parquet` hold the same ids) and no new rows:
> `compare_with_champion` logs `all test rows` and `old test rows`, the same
> rows, and `promotion_gate` decides on the whole split,
> `… on the same … test rows`. The search differs, so it can go either way. If
> the gate lets it through, `approve_promotion` waits for you again.

> **Try it, the other branch.** Set the bar impossibly high and watch
> `promotion_gate` take the `keep_current_model` branch. The threshold is read
> from an environment variable in the process that runs the task, and
> `airflow dags test` runs tasks in its own process, so pass the variable to
> that command with `-e`:
>
> ```bash
> docker compose exec -e PROJECT_TAGGER_MIN_F1=0.99 airflow-scheduler airflow dags test mlops
> ```
>
> `promotion_gate` logs `below threshold 0.99 - not promoting`;
> `approve_promotion` and `promote_model` are skipped, so nobody is asked;
> `keep_current_model` runs, and the Dag run still succeeds. In MLflow the new
> version is registered and `champion` has not moved. Nothing to revert: `-e`
> sets the variable for that one command only, so the next run is back on 0.65.

Stuck anywhere? [`solutions/04_mlops.py`](../../solutions/04_mlops.py).

---

← [9. The DataOps Dag](09-the-dataops-dag.md) · [Index](README.md) · [11. Waiting: sensors, deferrable tasks, events](11-waiting-sensors-deferrable-tasks-events.md) →
