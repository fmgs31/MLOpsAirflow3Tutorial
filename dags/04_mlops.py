"""Lesson 4 - the MLOps Dag: asset-triggered training, tuning, and promotion.

The payoff of data-aware scheduling. This Dag has no cron. It runs when
`dataops` emits the `labeled_projects` asset event - which only happens when
the data passed validation.

Also shown:

* **A validation gate before training** - `dataops` validated the warehouse
  table; this validates the *training snapshot*, including the split, which no
  table-level suite can see.
* **Dynamic task mapping** - one `train_candidate` task instance per
  hyperparameter set, fanned out at runtime, then reduced by `select_best`.
* **Branching as a promotion gate** - a model that misses the f1 threshold does
  not reach production, and the Dag run still succeeds (a rejected candidate is
  a normal outcome, not a failure).
* **XCom discipline** - tasks exchange run ids and metric dicts. The models and
  dataframes live in the registry and on disk.

Deadline alerts live in `07_deadline_alerts.py` - see the note there for why
they are not attached to this Dag.

Run it:
    airflow dags test mlops
"""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException

from tagifai import config, data, registry, train as train_module, validate
from tagifai.assets import LABELED_PROJECTS, PRODUCTION_MODEL

DEFAULT_ARGS = {
    "owner": "ml-engineering",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=2),
}


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
)
def mlops():
    @task
    def prepare_data(*, triggering_asset_events) -> dict[str, int]:
        """Inspect what triggered us, then materialise the train/test split."""
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")

        df = data.load_labeled()
        train_df, test_df = data.split(df)
        return {"num_train": len(train_df), "num_test": len(test_df)}

    @task
    def validate_training_data(split_sizes: dict[str, int]) -> dict[str, Any]:
        """The gate between the warehouse and the trainer.

        `dataops` already validated `labeled_projects` - but that was a
        different Dag run, possibly hours ago, and this run may have been
        triggered by hand. What the model is fitted on is the snapshot on disk
        *now*, plus a split that no single-frame suite can inspect, so both are
        checked here.

        The dataframe is re-read rather than passed from `prepare_data`: XComs
        carry metadata, never payloads.
        """
        df = data.load_labeled()
        table = validate.run_suite(df, "labeled_projects")
        split = validate.check_split(*data.split(df))
        print(f"{table.summary()} {split.summary()}")

        if not (table.passed and split.passed):
            # AirflowFailException, not AirflowException: a bad snapshot will
            # not become good by retrying the same task.
            raise AirflowFailException(
                f"Training snapshot failed validation: {table.failures + split.failures}"
            )
        return {
            "rows": len(df),
            "split_sizes": split_sizes,
            "warnings": [r["expectation"] for r in table.failures + split.failures],
            "results": table.results + split.results,
        }

    @task
    def candidates() -> list[dict[str, Any]]:
        """The search space. Returning a list is what `.expand()` maps over."""
        return train_module.SEARCH_SPACE

    @task(max_active_tis_per_dag=2)
    def train_candidate(hparams: dict[str, Any], **context) -> dict[str, Any]:
        """One task instance per hyperparameter set.

        The argument is named `hparams`, not `params`: TaskFlow injects context
        keys by parameter name, and `params` is a reserved context key.
        """
        config.ensure_dirs()
        run_id = f"{context['run_id']}__{context['ti'].map_index}".replace(":", "-").replace("+", "-")
        model, metrics = train_module.train_from_files(hparams)
        registry.save_run(run_id, model, metrics)
        print(f"run={run_id} hparams={hparams} f1={metrics['f1']:.4f}")
        # Small, JSON-serialisable payload. The model itself stays in the registry.
        return {"run_id": run_id, "f1": metrics["f1"], "hparams": hparams}

    @task
    def select_best(results: list[dict[str, Any]]) -> dict[str, Any]:
        """The reduce step: every mapped instance's return value arrives as a list."""
        best = max(results, key=lambda r: r["f1"])
        print(f"best={best['run_id']} f1={best['f1']:.4f} of {len(results)} candidates")
        return best

    @task
    def evaluate_best(best: dict[str, Any]) -> dict[str, Any]:
        """Re-read the full metrics, including per-class and slice performance."""
        metrics = registry.load_metrics(best["run_id"])
        for name, slice_metrics in metrics.get("slices", {}).items():
            print(f"slice {name}: f1={slice_metrics['f1']:.4f} n={slice_metrics['support']}")
        return {"run_id": best["run_id"], "metrics": metrics}

    @task.branch
    def promotion_gate(evaluation: dict[str, Any]) -> str:
        """Return the task_id to run. The other branch is skipped, not failed."""
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
    def promote_model(evaluation: dict[str, Any], *, outlet_events) -> dict[str, Any]:
        pointer = registry.promote(evaluation["run_id"])
        outlet_events[PRODUCTION_MODEL].extra = {
            "run_id": pointer["run_id"],
            "f1": pointer["metrics"]["f1"],
            "previous_run_id": pointer["previous_run_id"],
        }
        print(f"promoted {pointer['run_id']} (was {pointer['previous_run_id']})")
        return pointer

    keep_current_model = EmptyOperator(task_id="keep_current_model")

    # `none_failed_min_one_success` lets the join run whichever branch was taken;
    # the default `all_success` would skip it after any branch skip.
    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")

    snapshot = validate_training_data(prepare_data())
    results = train_candidate.expand(hparams=candidates())
    # No data flows from the gate to the trainer, only permission to run: every
    # mapped instance waits on it.
    snapshot >> results

    evaluation = evaluate_best(select_best(results))
    gate = promotion_gate(evaluation)

    gate >> promote_model(evaluation) >> done
    gate >> keep_current_model >> done


mlops()
