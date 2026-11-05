"""Extra links: buttons on a task instance that open, in the MLflow UI, what the task made.

Airflow calls `get_link` on the worker once the task has finished and keeps the
URL in XCom; the UI reads it from there. Each link reads the task's return
value, so the button points at the run or version that task instance made.
"""

from __future__ import annotations

from airflow.sdk import BaseOperatorLink
from airflow.sdk.execution_time.xcom import XCom

from project_tagger import registry


class MlflowRunLink(BaseOperatorLink):
    """The MLflow run whose id the task returned under `field`."""

    name = "MLflow run"

    def __init__(self, field: str = "run_id") -> None:
        self.field = field

    def get_link(self, operator, *, ti_key) -> str:
        result = XCom.get_value(ti_key=ti_key, key="return_value")
        return registry.run_url(result[self.field]) if result else ""


class MlflowModelVersionLink(BaseOperatorLink):
    """The registered version the task returned under `version`."""

    name = "MLflow model version"

    def get_link(self, operator, *, ti_key) -> str:
        result = XCom.get_value(ti_key=ti_key, key="return_value")
        return registry.version_url(result["version"]) if result else ""
