## 10. The MLOps Dag

`dags/04_mlops.py` has no time schedule: each `labeled_projects` asset event
triggers it.

```
prepare_data ─► validate_training_data ─┐
create_study ───────────────────────────┤
trial_budget ───────────────────────────┴─► run_trials×N ─► best_params ─► train_best
─► compare_with_champion ─► summarize_candidate ─► promotion_gate ─► approve_promotion ─► promote_model ──► done
                                                                └─► keep_current_model ──────────────────┘
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
scores so far to choose the next values. The study is stored in a database,
here the `optuna` database on the stack's Postgres, so several processes can
add trials to the same study at the same time, which is what lets Airflow
spread the search over several tasks.

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

The MLflow server is the `mlflow` service in the compose file. It stores runs
and the registry in a SQLite file and the model files in a folder of its own,
on a Docker volume, and the tasks reach it at `http://mlflow:5000`
(`MLFLOW_TRACKING_URI`). Open its UI:

**Checkpoint.** <http://localhost:5000> shows MLflow with one experiment,
`Default`, and no runs. The `project_tagger` experiment doesn't exist yet: the
`mlops` Dag's code creates it (`mlflow.set_experiment` in
`src/project_tagger/train.py`) the first time it logs a run.

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
so the whole run works on the same data. MLflow ends up with one run for the
study, with the trials as child runs under it, plus the final run; the model
file is uploaded only with the final run.

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
runs when they update. Several assets in the list mean *all* of them,
as `AssetAll(a, b)` does; `AssetAny(a, b)` means either.

`params` declares the search budget as two Dag params (§4): 20 trials in
total, spread over 4 tasks. They set how much searching happens, not what is
searched. The hyperparameters Optuna tunes, and their ranges, are fixed in
`objective()` in `src/project_tagger/train.py`:

| Hyperparameter | Range | What it controls |
|---|---|---|
| `ngram_max` | 1 to 3 | longest word sequence TF-IDF turns into a feature (1 = single words only) |
| `min_df` | 1 to 3 | terms found in fewer projects than this are dropped |
| `alpha` | 1e-6 to 1e-2, on a log scale | strength of the SGD classifier's regularisation: higher gives a simpler model |

`run_key` turns a run id such as
`manual__2026-02-01T03:00:00+00:00` into something safe in a file name and a
study name.

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

`prepare_data` reads the warehouse table once and writes it to a **snapshot**:
files in a folder named after the run, which every later task reads. The table
can change while a run is in flight (a `dataops` run can finish in the
middle), and the snapshot keeps every task of this run on the same rows,
including a task that is retried an hour later. The files are Parquet, not
CSV: a Parquet file stores each column's type, so every column comes back as
it was written, `created_on` as a timestamp and not a string. A CSV is only
text, and the types are guessed again on every read. The task returns the
files' paths, a small dict: XComs carry a pointer to the data, not the data
itself (§6).

`data.save_snapshot` writes four files: the table as read
(`labeled_projects.parquet`), which the §9 suite checks, and the model's input
split three ways: `train.parquet`, `val.parquet` and `test.parquet`. The
model's input is what `data.preprocess` makes of the table: it drops rows with
no title or no tag, joins title and description into one cleaned `text`
column, and turns every tag outside the four classes the model learns
(`config.ACCEPTED_TAGS`) into `other`. The search fits on train and scores each trial on validation; the final model is
fitted on both and scored on test, which nothing has seen before.

The split is **written once**. If each task recomputed it, every task would
depend on the code, the libraries and the seed being identical wherever it
runs: a task retried after a deployment could train on some rows and test on
others, with no warning. Written once, every task reads the same rows, and the
rows each model was built on are kept.

Each row's split is **decided by its id**. `data.assign_split` hashes each
project's id into a number from 0 to 99: below 20 is test, 20 to 35
validation, the rest train (`config.TEST_PCT` and `config.VAL_PCT`). So a
project lands in the same split in every run, and no model trains on a row
that is test data for another. That lets `promotion_gate`, later in this
section, score the new model and the production model on the same test rows,
and lets monitoring (§12) tell which rows the production model has never seen.
The cost: the split is no longer stratified (the same share of each tag in
every split). With hundreds of rows the shares come out close, and the split
check catches a class that ends up missing or scarce.

**Validate before you train.** `dataops` validated `labeled_projects` when it
built it, and this Dag validates it again before training on it. That is not
redundant: it was a different Dag run, possibly hours ago, this run may have
been triggered by hand, and the model is fitted on the data as it is *now*.
Add the second task:

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

Each problem in those comments — a class missing from test, three rows of a
class, the same project in two splits — passes a column-level suite without a
warning, yet makes the test f1 meaningless, and that f1 is what
`promotion_gate` compares against the production model. The minimum rows per
class counts train and validation together, because the final model learns
from both. These checks span three dataframes and a GX suite validates one
batch at a time, so they are plain Python returning the same result shape as
the suite.

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

`create_study` creates the study with `load_if_exists=True` (in `train.py`),
so a retried task loads the study instead of failing on it. It also opens the
study's run in MLflow, the **parent run** of every trial, and keeps that run's
id on the study itself, as a *user attribute* (a value Optuna stores with the
study). A retry finds the id there and opens no second run.

`trial_budget` reads the Dag params through the `params` context key and splits
the trials over `n_workers` tasks: 20 over 4 gives `[5, 5, 5, 5]`. That list
is what the next task is mapped over.

> **`params` is reserved.** A `@task` receives context keys by parameter name
> (§4), and `params` is one of them: `trial_budget` gets the Dag's params
> because its argument is called `params`. Name a data argument `params` by
> mistake and it receives the Dag's params instead of your data.

Now the task that is mapped:

```python dags/04_mlops.py
    @task(max_active_tis_per_dag=2)
    def run_trials(n_trials: int, study_name: str, paths: dict[str, str]) -> list[dict[str, Any]]:
        train_df, val_df = data.load_snapshot(paths["train"]), data.load_snapshot(paths["val"])
        results = train_module.run_trials(study_name, n_trials, train_df, val_df)
        print(f"{len(results)} trials, best here f1={max(r['f1'] for r in results):.4f}")
        return results
```

Nothing in it mentions mapping. `run_trials` is written for one instance: its
`n_trials` is one number, not the list. The mapping is in how the wiring at
the end of the file calls it:

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

Each instance fits its trials on the training split, scores them on the
validation split, and logs every trial to MLflow as a **child run**: a run with
a parent, which the MLflow UI folds under it. The test split is never seen
during the search, so the final model's test score is still an honest
estimate.

A child run is usually opened with `mlflow.start_run(nested=True)` inside the
parent's `with mlflow.start_run():` block. That only works in the process that
opened the parent, and here the parent was opened by `create_study` and each
trial runs in one of the `run_trials` instances. So `train.py` names the parent
explicitly, `mlflow.start_run(parent_run_id=...)`, with the id read from the
study.

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

`train_best` fits the final model with the best trial's hyperparameters on the
training and validation splits together (the validation split has done its
job, choosing those hyperparameters, and more rows make a better model),
scores it on the test split, and logs it to MLflow, model file included, as a
run of its own, outside the study: it is not one more attempt of the search.
Its tags link it to the Airflow run that made it, the trial its
hyperparameters came from, and the snapshot it was trained on
(`snapshot_path`). The run also gets `split_ids.json`, the project ids in each
split: what the model was built on stays with it in MLflow after the snapshot
folder is gone, and §12 reads it. Then the task **registers** the model: every
final model becomes a new version of `project_tagger`, promoted or not, so a
rejected candidate is still there to inspect. The task returns a run id, a
version number, the f1 and the slice metrics, small and JSON-serialisable. The
model itself stays in MLflow.

The slice metrics are the f1 on each **evaluation slice**: a subgroup of the
test set picked out by a rule in `evaluate.py` (short texts, and texts that
mention transformers, BERT or LLMs). These are not the time slices of §7; they
show a model whose overall f1 looks fine while one kind of input fails. They
are small: of the 157 test rows in this section, 4 are short texts and 23
mention transformers, so a single prediction moves a slice's f1 a lot. Too
noisy for a fixed rule to decide on; a person can weigh them, which is what
`approve_promotion` is for.

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

`compare_with_champion` loads the production model and the version
`train_best` just registered, and scores both on this run's test split. The
champion also has the f1 it logged when it was trained, but that was measured
on another run's test split, on other rows; comparing it with the new model's
f1 would compare two exams, not two models. On the same rows the comparison
is fair, and the hash split makes those rows honest for both: no test row has
ever been in any model's training data, the champion's included.

`evaluate.compare` also splits the test rows by the champion's
`split_ids.json`, and scores both models on each part:

| Part | Rows | What it says |
|---|---|---|
| `new` | test rows whose id is in none of the champion's splits: they reached the warehouse after it was trained | how each model does on data like what production gets next |
| `old` | the champion's own test rows | whether the new model kept what the champion could do |
| `all` | both | the whole split |

A retrain exists to do better on the new data, so the `new` rows decide: the
new model's f1 on them must not be below the champion's. With fewer than 30
new rows (`config.MIN_NEW_TEST_ROWS`) there is too little new data to decide
on, and the whole split decides instead. That is the case in this section,
where every run sees the same 764 rows; in §12 new rows arrive. The `old`
part is in the log for you to read; §12 comes back to it.

`compare_with_champion` is a task of its own so that the Graph shows it and
its log names the version it compared with, part by part. It takes
`train_best`'s output, which carries the new version, so it runs after it.

Next, the **promotion gate**, which decides whether the new model goes
to production. Unlike the gates in §9, the gate does not fail the task when the
answer is no: it is a *branch task*, a task that picks which of the Dag's two
**branches** continues. Add `summarize_candidate` and `promotion_gate`:

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
and the f1 on each slice next to its row count. The text is Markdown, and the
task returns it, so it goes to XCom for `approve_promotion` to show.

`promotion_gate` applies the rule in `evaluate.should_promote`: the new
model's f1 must clear the threshold, `config.MIN_F1_TO_PROMOTE` (0.65, or the
value of the environment variable `PROJECT_TAGGER_MIN_F1` if set), and, against
a champion, must not be
below it on the new rows, or on the whole split when there are too few. A
`@task.branch` returns the `task_id` to run; the other branch is **skipped**,
not failed. A candidate that misses the threshold, or loses to the champion,
is a normal outcome: the run succeeds and production is left alone.

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

A model the gate lets through still waits for a person. `approve_promotion`
is a **human-in-the-loop** task, one that waits for a person to answer.
`ApprovalOperator` parks it in `awaiting_input` and shows a prompt in the UI,
on the task instance's **Required Actions** tab and under Browse → **Required
Actions** (§5). On **Approve**, `promote_model` runs; on **Reject**, it is
skipped and production is left alone. `defaults` is the answer applied if
nobody responds within `response_timeout`: sixty seconds here so the lesson's
runs finish; give it hours in production. `body` is the text of the prompt,
in Markdown, and it is a template field (§6): `ti.xcom_pull` puts
`summarize_candidate`'s text in it when the task runs. The gate checks the
numbers it has a rule for; the person checks what the rules miss, such as a
slice that fell, with that summary and the new version in MLflow in front of
them. Airflow records the answer: the
choice, when, and *by whom*. A run that timed out and fell back to its
`defaults` stores the choice with the "who" left empty, which is how you tell
"a person approved this" from "nobody was looking".

`promote_model` moves the `champion` alias and emits `mlops`'s own asset,
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
    paths = prepare_data()
    gate = validate_training_data(paths)
    study = create_study()
    results = run_trials.partial(study_name=study, paths=paths).expand(
        n_trials=trial_budget()
    )
    gate >> results

    trained = train_best(best_params(study, results), paths)
    comparison = compare_with_champion(trained, paths)
    decision = promotion_gate(trained, comparison)
    summarize_candidate(trained, comparison) >> decision

    decision >> approve_promotion >> promote_model(trained) >> done
    decision >> keep_current_model >> done


mlops()
```

`gate >> results` wires `validate_training_data` upstream of *every* mapped
instance of `run_trials`, carrying no data, only the order: no trial starts on
data that failed its checks. `promotion_gate` takes the results of
`train_best` and `compare_with_champion`, which itself waits for `train_best`,
so the three run in that order. `summarize_candidate >> decision` is the same
kind of edge as `gate >> results`: `approve_promotion` reads the summary
through a template, not through an argument, so Airflow sees no dependency on
its own, and this line puts the summary before the gate that opens
`approve_promotion`'s branch. The two `decision >>` lines give the gate its
two branches, the first through `approve_promotion`, and join them at `done`.

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

- **Only tasks** — the fourteen tasks you just wrote, nothing else.
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
missed, and starts it. Give it a few minutes. Near the end, the `mlops` run
stops at `approve_promotion`: open the run in the UI, click
`approve_promotion`, and answer the prompt on its **Required Actions** tab,
under `summarize_candidate`'s numbers, or leave it and the default approves it
after 60 seconds. Then:

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
syncs in `dataops`, then twenty trials and a final fit in `mlops`, and up to a
minute at `approve_promotion`. The Airflow
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
0 and 1 has ended. And `keep_current_model` is `skipped` while
`approve_promotion` and `promote_model` are `success`: `promotion_gate` took
the `approve_promotion` branch, because the final model's f1 cleared the
threshold and there was no production model to beat (`compare_with_champion`
logged `no champion yet`), and the approval came from you or from the
default.

Then look at what the run left in MLflow, at <http://localhost:5000>:

**Checkpoint.** The `project_tagger` experiment shows two runs:
`project_tagger-asset_triggered__…`, the study, and `final-asset_triggered__…`.
Expand the study's run to see its twenty children, `…-trial-0` to `-trial-19`,
and sort by the `f1` column to see the search at work. Open the study's run:
`best_f1` is the best trial's score, and the tag `best_trial_run_id` names it.
Open the final run: its parameters are the best trial's, its metrics include
the per-tag and per-slice f1, its tags include `snapshot_path`, and under
**Artifacts** are `metrics.json`, `split_ids.json` and the model. Under
**Models**, `project_tagger` has version 1, with the alias `champion`.

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
> depends on its f1 against version 1's, both on this run's test split. Same
> data, so the same split (the `test.parquet` files of the two runs hold the
> same ids), and no new rows: `compare_with_champion` logs `all test rows` and
> `old test rows`, the same rows, and `promotion_gate` decides on the whole
> split, `… on the same … test rows`. A different search, so it can go either
> way. If the gate lets it through, `approve_promotion` waits for you again.

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
> `promotion_gate` logs `below threshold 0.99 - not promoting`,
> `approve_promotion` and `promote_model` are skipped, so nobody is asked,
> `keep_current_model` runs, and the Dag run still succeeds. In
> MLflow the new version is registered, and `champion` has not moved.

Stuck anywhere? [`solutions/04_mlops.py`](../../solutions/04_mlops.py).

---

← [9. The DataOps Dag](09-the-dataops-dag.md) · [Index](README.md) · [11. Waiting: sensors, deferrable tasks, events](11-waiting-sensors-deferrable-tasks-events.md) →
