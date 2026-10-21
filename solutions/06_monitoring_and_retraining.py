"""Lesson 6 - continual learning: monitor, decide, and act.

A monitoring event has four sensible responses, and this Dag makes them
explicit branches:

    continue  keep serving; log the alert and move on
    improve   retrain on new data (triggers the mlops Dag)
    inspect   the label space or schema changed - a human should look
    rollback  live performance collapsed - revert to the previous model

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
from airflow.sdk.exceptions import AirflowSkipException
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

        return drift.detect_drift(windows["reference"], windows["current"], float(p_val))

    @task
    def check_performance(windows: dict[str, Any]) -> dict[str, Any] | None:
        performance = monitoring.production_performance(windows["current"])
        if performance:
            print(f"f1 at promotion={performance['f1_at_promotion']:.4f} now={performance['f1_now']:.4f}")
        return performance

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
            # An unseen label means the schema/label space moved. Retraining on
            # data you do not understand yet is how you ship a worse model.
            return "inspect"
        if (
            f1_drop >= params["max_f1_drop"]                    # concept drift
            or label["p_val"] < params["drift_p_val"]           # target drift
            or length["p_val"] < params["drift_p_val"]          # data drift
        ):
            return "trigger_retraining"
        return "continue_serving"

    continue_serving = EmptyOperator(task_id="continue_serving")

    @task
    def inspect(drift: dict[str, Any]) -> None:
        print("MANUAL INSPECTION NEEDED")
        print(f"  new labels: {drift['new_labels']}")
        print(f"  current distribution: {drift['current_distribution']}")

    # Moving `champion` changes the production model as much as a promotion
    # does, so this task emits the same asset as `promote_model` in mlops.
    @task(outlets=[PRODUCTION_MODEL])
    def rollback(
        performance: dict[str, Any], *, outlet_events: OutletEventAccessorsProtocol
    ) -> dict[str, Any]:
        reason = f"f1 fell from {performance['f1_at_promotion']:.4f} to {performance['f1_now']:.4f}"
        champion = registry.rollback(reason)
        if champion is None:
            # A skipped task emits no asset event: production did not change.
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

    windows = prepare_windows()
    drift = detect_drift(windows, "{{ params.drift_p_val }}")
    performance = check_performance(windows)
    branch = decide(windows, drift, performance)

    branch >> continue_serving
    branch >> inspect(drift)
    branch >> rollback(performance)
    branch >> trigger_retraining


monitoring_dag()
