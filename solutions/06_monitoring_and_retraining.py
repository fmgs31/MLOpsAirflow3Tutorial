"""Lesson 6 - continual learning: monitor, decide, and act.

A monitoring event has four sensible responses, and this Dag makes them
explicit branches:

    continue  keep serving; log the alert and move on
    improve   retrain on new data (triggers the mlops Dag)
    inspect   the label space or schema changed - a human should look
    rollback  live performance collapsed - revert to the previous model

Pieces on show:

* `@task.branch` picking one arm of the response.
* `ApprovalOperator` (human-in-the-loop): retraining waits for a person to
  click Approve in the UI. `defaults` + `response_timeout` mean an unattended
  run still resolves instead of hanging forever.
* `TriggerDagRunOperator` for cross-Dag orchestration.

Run it:
    airflow dags test monitoring
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task

from tagifai import monitoring, registry

DEFAULT_ARGS = {"owner": "ml-engineering", "retries": 1}

# Thresholds that turn a measurement into a decision. Exposed as Dag params so
# the policy is visible in the UI, overridable per run, and recorded with the
# run that used it:
#     airflow dags test monitoring --conf '{"max_label_tvd": 0.0}'
POLICY_PARAMS = {
    "max_label_tvd": Param(0.15, type="number", description="Label-drift budget."),
    "max_length_shift": Param(
        0.25, type="number", description="Input-drift budget: relative change in mean text length."
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
    def measure() -> dict[str, Any]:
        report = monitoring.monitor()
        drift = report["drift"]
        print(
            f"label TVD={drift['label_tvd']:.4f} new_labels={drift['new_labels']} "
            f"length shift={drift['mean_length_shift']:+.4f}"
        )
        if report["performance"]:
            perf = report["performance"]
            print(f"f1 at promotion={perf['f1_at_promotion']:.4f} now={perf['f1_now']:.4f}")
        return report

    @task.branch
    def decide(report: dict[str, Any], **context) -> str:
        drift, performance = report["drift"], report["performance"]
        policy = context["params"]

        if performance is None:
            print("No production model yet - train one.")
            return "request_retraining"

        f1_drop = performance["f1_at_promotion"] - performance["f1_now"]
        print(f"f1_drop={f1_drop:.4f} label_tvd={drift['label_tvd']:.4f} policy={dict(policy)}")

        if f1_drop >= policy["critical_f1_drop"]:
            return "rollback"
        if drift["new_labels"]:
            # An unseen label means the schema/label space moved. Retraining on
            # data you do not understand yet is how you ship a worse model.
            return "inspect"
        if (
            f1_drop >= policy["max_f1_drop"]                                 # concept drift
            or drift["label_tvd"] >= policy["max_label_tvd"]                 # target drift
            or abs(drift["mean_length_shift"]) >= policy["max_length_shift"] # data drift
        ):
            return "request_retraining"
        return "continue_serving"

    continue_serving = EmptyOperator(task_id="continue_serving")

    @task
    def inspect(report: dict[str, Any]) -> None:
        print("MANUAL INSPECTION NEEDED")
        print(f"  new labels: {report['drift']['new_labels']}")
        print(f"  current distribution: {report['drift']['current_distribution']}")

    @task
    def rollback() -> dict[str, Any] | None:
        pointer = registry.rollback()
        print(f"rolled back to {pointer['run_id']}" if pointer else "nothing to roll back")
        return pointer

    # Human-in-the-loop gate. The task sits in `awaiting_input` and shows a
    # prompt in the UI; on Reject, everything downstream is skipped.
    request_retraining = ApprovalOperator(
        task_id="request_retraining",
        subject="Retrain the tag classifier?",
        body=(
            "Monitoring detected drift or a performance drop.\n\n"
            "Approve to trigger the `mlops` Dag on the current data."
        ),
        # No answer within the window resolves to `defaults`, so an unattended
        # run still completes. Give it hours in production; kept short here so
        # `airflow dags test monitoring` finishes.
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

    report = measure()
    branch = decide(report)

    branch >> continue_serving
    branch >> inspect(report)
    branch >> rollback()
    branch >> request_retraining >> trigger_retraining


monitoring_dag()
