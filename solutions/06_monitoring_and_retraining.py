"""Lesson 6 - continual learning: monitor, decide, and act.

Every monitoring run ends in one of four actions, each its own branch:

    continue  keep serving; log the alert and move on
    improve   retrain on new data (triggers the mlops Dag)
    inspect   fail the run so a human looks: the label space changed, or
              performance collapsed and the previous model is no better
    rollback  live performance collapsed and the previous model does better
              on the same rows - revert to it

Every measurement is of the production model on rows it has never seen: the
rows that reached the warehouse after it was trained.

Pieces on show:

* `@task.external_python` - the drift checks run alibi-detect, which needs
  libraries Airflow's own environment cannot have, so they run in a separate
  virtualenv baked into the image.
* `@task.branch` picking one arm of the response.
* `TriggerDagRunOperator` for cross-Dag orchestration. Retraining needs no
  approval here: what reaches production is approved in `mlops`.

Run it:
    airflow dags test monitoring
"""

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

DEFAULT_ARGS = {"owner": "ml-engineering", "retries": 1}

# Thresholds that turn a measurement into a decision. Exposed as Dag params so
# the policy is visible in the UI, overridable per run, and recorded with the
# run that used it:
#     airflow dags test monitoring --conf '{"drift_p_val": 1.0}'
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
    @task
    def prepare_windows(*, run_id: str) -> dict[str, Any]:
        """Read the warehouse once and write the two windows drift is measured on."""
        key = run_id.replace(":", "-").replace("+", "-")
        # Writes reference.parquet and current.parquet; returns their paths, num_current,
        # version (the champion's) and already_trained_on. With no champion, or fewer than
        # config.MIN_MONITOR_ROWS current rows, it writes nothing and the paths are None.
        windows = monitoring.prepare_windows(f"monitoring-{key}")
        print(f"windows: {windows}")
        return windows

    # Runs with alibi-detect's interpreter, not Airflow's. The function body is
    # shipped to that interpreter, so everything it uses is imported inside it,
    # and its arguments and return value must be plain data. `expect_airflow=False`:
    # that virtualenv has no Airflow, and the function needs none.
    @task.external_python(python=config.ALIBI_PYTHON, expect_airflow=False)
    def detect_drift(windows: dict, p_val: str) -> dict | None:
        from project_tagger import drift

        # label_drift and length_drift (each with is_drift, p_val, distance), new_labels
        # and both tag distributions; None when there are no files.
        return drift.detect_drift(windows["reference"], windows["current"], float(p_val))

    @task
    def check_performance(windows: dict[str, Any]) -> dict[str, Any] | None:
        # The champion's f1_at_promotion, its f1_now on the current window, and
        # f1_previous, the previous champion's on the same rows (None if there
        # is no previous champion); None when there is no window.
        performance = monitoring.production_performance(windows["current"])
        if performance:
            print(
                f"f1 at promotion={performance['f1_at_promotion']:.4f} now={performance['f1_now']:.4f} "
                f"previous champion={performance['f1_previous']}"
            )
        return performance

    def retrain_or_wait(windows: dict[str, Any], ti) -> str:
        """`trigger_retraining`, unless a retrain on this data is already queued, running or done."""
        if ti.get_dr_count(dag_id="mlops", states=["queued", "running"]):
            print("an mlops run is already queued or running - not starting another")
            return "continue_serving"
        if windows["already_trained_on"]:
            # A model built on these same rows was already turned down; a new
            # search on them is a retry, not new evidence. The next delivery
            # starts mlops through its asset (§8).
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

        label, length = drift["label_drift"], drift["length_drift"]
        print(
            f"label drift p={label['p_val']:.4g} length drift p={length['p_val']:.4g} "
            f"new_labels={drift['new_labels']} policy={dict(params)}"
        )
        f1_drop = performance["f1_at_promotion"] - performance["f1_now"]
        print(f"f1_drop={f1_drop:.4f}")

        if f1_drop >= params["critical_f1_drop"]:
            # Roll back only to a model that does better on the same rows. If
            # the previous one is no better, the data moved, not the model.
            f1_previous = performance["f1_previous"]
            if f1_previous is not None and f1_previous > performance["f1_now"]:
                return "rollback"
            return "inspect"
        if drift["new_labels"]:
            # An unseen label means the schema/label space moved. Retraining on
            # data you do not understand yet is how you ship a worse model.
            return "inspect"
        if (
            f1_drop >= params["max_f1_drop"]                    # concept drift
            or label["p_val"] < params["drift_p_val"]           # target drift
            or length["p_val"] < params["drift_p_val"]          # data drift
        ):
            return retrain_or_wait(windows, ti)
        return "continue_serving"

    continue_serving = EmptyOperator(task_id="continue_serving")

    # Fails on purpose: a run that needs a person must not end green, and a
    # failed task is what alerting (§16) is wired to.
    @task
    def inspect(drift: dict[str, Any], performance: dict[str, Any]) -> None:
        print("MANUAL INSPECTION NEEDED")
        print(f"  new labels: {drift['new_labels']}")
        print(f"  current distribution: {drift['current_distribution']}")
        print(f"  f1 at promotion={performance['f1_at_promotion']:.4f} now={performance['f1_now']:.4f}")
        raise AirflowFailException("manual inspection needed - nothing was retrained or rolled back")

    # Moving `champion` changes the production model as much as a promotion
    # does, so this task emits the same asset as `promote_model` in mlops.
    @task(outlets=[PRODUCTION_MODEL])
    def rollback(
        performance: dict[str, Any], *, outlet_events: OutletEventAccessorsProtocol
    ) -> dict[str, Any]:
        reason = f"f1 fell from {performance['f1_at_promotion']:.4f} to {performance['f1_now']:.4f}"
        champion = registry.rollback(reason)
        if champion is None:
            # `decide` saw a previous champion; it is gone now. Fail rather than
            # skip: the collapsed model is still serving, and someone must know.
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

    windows = prepare_windows()
    drift = detect_drift(windows, "{{ params.drift_p_val }}")
    performance = check_performance(windows)
    branch = decide(windows, drift, performance)

    branch >> continue_serving
    branch >> inspect(drift, performance)
    branch >> rollback(performance)
    branch >> trigger_retraining


monitoring_dag()
