"""Lesson 4 - the MLOps Dag: asset-triggered tuning, training, and promotion.

The payoff of data-aware scheduling. This Dag has no cron. It runs when
`dataops` emits the `labeled_projects` asset event - which only happens when
the data passed validation.

Also shown:

* **A snapshot and a validation gate before training** - the warehouse table
  is copied once per run and split once, by a hash of each project's id, and
  those files are what gets validated and trained on, including the split,
  which no table-level suite can see.
* **Distributed tuning with dynamic task mapping** - one Optuna study, stored
  in a database, and one `run_trials` task instance per worker, fanned out at
  runtime. Every worker adds trials to the same study.
* **Experiment tracking** - the study is an MLflow run with one child run per
  trial, and the final model is a run of its own, registered as a new version.
* **Branching as a promotion gate** - a model that misses the f1 threshold, or
  scores below the production model on the test rows that model has never
  seen, does not reach production, and the Dag run still succeeds (a rejected
  candidate is a normal outcome, not a failure).
* **Human-in-the-loop** - `ApprovalOperator`: a model the gate lets through
  reaches production only when a person clicks Approve in the UI. `defaults` +
  `response_timeout` mean an unattended run still resolves instead of hanging.
* **XCom discipline** - tasks exchange paths, run ids and metric dicts. The
  models live in MLflow and the data in the warehouse and on disk.

Deadline alerts live in `07_deadline_alerts.py` - see the note there for why
they are not attached to this Dag.

Run it:
    airflow dags test mlops
"""

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
    """A Dag run id made safe for file names and study names."""
    return run_id.replace(":", "-").replace("+", "-")


@dag(
    dag_id="mlops",
    # The whole point: schedule on data, not on time. Pass several assets for
    # AND semantics, or AssetAny(a, b) for OR.
    schedule=[LABELED_PROJECTS],
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "mlops"],
    doc_md=__doc__,
    # The tuning budget, overridable per run from the Trigger form.
    params={
        "n_trials": Param(20, type="integer", minimum=1, description="Optuna trials in total."),
        "n_workers": Param(4, type="integer", minimum=1, description="Tasks the trials are spread over."),
    },
)
def mlops():
    @task
    def prepare_data(
        *, triggering_asset_events: Mapping[Asset, Sequence[Any]], run_id: str
    ) -> dict[str, str]:
        """Inspect what triggered us, then pin and split the rows this run will use."""
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")

        config.ensure_dirs()
        # Writes data/snapshots/<key>/; returns the paths "snapshot", "train", "val", "test".
        paths = data.save_snapshot(data.load_labeled(), run_key(run_id))
        print(f"snapshot: {paths}")
        return paths

    @task
    def validate_training_data(paths: dict[str, str]) -> dict[str, Any]:
        """The gate between the warehouse and the trainer.

        `dataops` already validated `labeled_projects` - but that was a
        different Dag run, possibly hours ago, and this run may have been
        triggered by hand. What the model is fitted on is the snapshot,
        plus a split that no single-table suite can inspect, so both are
        checked here.
        """
        # The table as read from the warehouse, against the labeled_projects suite.
        df = data.load_snapshot(paths["snapshot"])
        table = validate.run_suite(df, "labeled_projects")
        # The three preprocessed splits (train, val, test), checked against each other.
        split = validate.check_split(*(data.load_snapshot(paths[name]) for name in data.SPLITS))
        print(f"{table.summary()} {split.summary()}")

        if not (table.passed and split.passed):
            # AirflowFailException, not AirflowException: a bad snapshot will
            # not become good by retrying the same task.
            raise AirflowFailException(
                f"Training snapshot failed validation: {table.failures + split.failures}"
            )
        return {
            "rows": len(df),
            "warnings": [r["expectation"] for r in table.failures + split.failures],
        }

    @task
    def create_study(*, run_id: str) -> str:
        """One Optuna study per Dag run. Safe to retry: an existing study is loaded."""
        return train_module.create_study(f"{config.REGISTERED_MODEL}-{run_key(run_id)}")

    @task
    def trial_budget(*, params: Mapping[str, Any]) -> list[int]:
        """Split the trials over the workers. Returning a list is what `.expand()` maps over."""
        base, extra = divmod(params["n_trials"], params["n_workers"])
        return [base + (1 if i < extra else 0) for i in range(params["n_workers"]) if base or i < extra]

    @task(max_active_tis_per_dag=2)
    def run_trials(n_trials: int, study_name: str, paths: dict[str, str]) -> list[dict[str, Any]]:
        """One task instance per worker; each adds `n_trials` trials to the shared study."""
        train_df, val_df = data.load_snapshot(paths["train"]), data.load_snapshot(paths["val"])
        results = train_module.run_trials(study_name, n_trials, train_df, val_df)
        print(f"{len(results)} trials, best here f1={max(r['f1'] for r in results):.4f}")
        return results

    @task
    def best_params(study_name: str, results: list[list[dict[str, Any]]]) -> dict[str, Any]:
        """The reduce step: every mapped instance's return value arrives as a list.

        The study already knows its best trial; `results` is taken so this task
        waits for every worker.
        """
        # Also records it on the study's run: metric best_f1, tag best_trial_run_id.
        best = train_module.best_trial(study_name)
        print(f"best trial #{best['number']} f1={best['f1']:.4f} of {best['num_trials']}: {best['params']}")
        return best

    @task
    def train_best(best: dict[str, Any], paths: dict[str, str], *, run_id: str) -> dict[str, Any]:
        """Fit on train + val, score on test, log and register in MLflow."""
        splits = {name: data.load_snapshot(paths[name]) for name in data.SPLITS}
        trained = train_module.train_model(
            best["params"], splits,
            run_name=f"final-{run_key(run_id)}",
            # Links the final run back to the Airflow run, the trial it took
            # its params from, and the files it was trained on.
            tags={
                "airflow_run_id": run_id,
                "best_trial_run_id": best["mlflow_run_id"],
                "snapshot_path": paths["snapshot"],
            },
        )
        version = registry.register(trained["model_uri"])
        print(f"run={trained['run_id']} version={version} f1={trained['metrics']['f1']:.4f}")
        # Small, JSON-serialisable payload. The model itself stays in MLflow.
        return {"run_id": trained["run_id"], "version": version, "f1": trained["metrics"]["f1"]}

    @task
    def evaluate_best(trained: dict[str, Any]) -> dict[str, Any]:
        """Re-read the full metrics, including per-class and slice performance."""
        metrics = registry.load_metrics(trained["run_id"])
        for name, slice_metrics in metrics.get("slices", {}).items():
            print(f"slice {name}: f1={slice_metrics['f1']:.4f} n={slice_metrics['support']}")
        return {**trained, "metrics": metrics}

    @task
    def compare_with_champion(evaluation: dict[str, Any], paths: dict[str, str]) -> dict[str, Any] | None:
        """Both models on this run's test split, new rows and old. None if nothing is promoted."""
        champion = registry.get_production()
        if champion is None:
            print("no champion yet")
            return None
        # Every id the champion's training run had, in any split.
        seen = [i for ids in registry.load_split_ids(champion["run_id"]).values() for i in ids]
        comparison = evaluate.compare(
            registry.load_model(version=evaluation["version"]),
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

    @task.branch
    def promotion_gate(evaluation: dict[str, Any], comparison: dict[str, Any] | None) -> str:
        """Return the task_id to run. The other branch is skipped, not failed."""
        promote, reason = train_module.should_promote(
            evaluation["metrics"]["f1"],
            comparison,
            config.MIN_F1_TO_PROMOTE,
            config.MIN_NEW_TEST_ROWS,
        )
        print(reason)
        return "approve_promotion" if promote else "keep_current_model"

    # Human-in-the-loop gate. The task sits in `awaiting_input` and shows a
    # prompt in the UI; on Reject, promote_model is skipped.
    approve_promotion = ApprovalOperator(
        task_id="approve_promotion",
        subject="Promote the new tag classifier?",
        body=(
            "The new version passed promotion_gate.\n\n"
            "Read compare_with_champion's log, then approve to move `champion` to it."
        ),
        # No answer within the window resolves to `defaults`, so an unattended
        # run still completes. Give it hours in production; kept short here so
        # the lesson's runs finish.
        defaults="Approve",
        response_timeout=timedelta(seconds=60),
    )

    @task(outlets=[PRODUCTION_MODEL])
    def promote_model(
        evaluation: dict[str, Any], *, outlet_events: OutletEventAccessorsProtocol
    ) -> dict[str, Any]:
        """Move the registry's `champion` alias to the new version."""
        champion = registry.promote(evaluation["version"])
        outlet_events[PRODUCTION_MODEL].extra = {
            "version": champion["version"],
            "f1": champion["metrics"]["f1"],
            "previous_version": champion["previous_version"],
        }
        print(f"champion -> version {champion['version']} (was {champion['previous_version']})")
        return champion

    keep_current_model = EmptyOperator(task_id="keep_current_model")

    # `none_failed_min_one_success` lets the join run whichever branch was taken;
    # the default `all_success` would skip it after any branch skip.
    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")

    paths = prepare_data()
    gate = validate_training_data(paths)
    study = create_study()
    results = run_trials.partial(study_name=study, paths=paths).expand(
        n_trials=trial_budget()
    )
    # No data flows from the gate to the trainers, only permission to run: every
    # mapped instance waits on it.
    gate >> results

    evaluation = evaluate_best(train_best(best_params(study, results), paths))
    decision = promotion_gate(evaluation, compare_with_champion(evaluation, paths))

    decision >> approve_promotion >> promote_model(evaluation) >> done
    decision >> keep_current_model >> done


mlops()
