"""The model registry: MLflow's, used through two aliases.

Registering a model gives it a numbered *version* under one name
(`project_tagger`). An *alias* is a movable label on one version:

    champion            the version in production - what predict.py loads
    previous_champion   the version champion pointed at before the last move

Promoting moves `champion` to a new version; rolling back moves it back. No
model file is copied: consumers ask for `models:/project_tagger@champion` and
get whatever the alias points at.

The rule this module exists to demonstrate: **big artifacts never travel
through XCom.** Tasks pass a version number or a run id - short strings -
and the model itself stays in MLflow.
"""

from __future__ import annotations

from typing import Any

from project_tagger import config


def _client():
    import mlflow
    from mlflow import MlflowClient

    mlflow.set_tracking_uri(config.MLFLOW_TRACKING_URI)
    return MlflowClient()


def register(model_uri: str) -> str:
    """Add the logged model as a new version. Returns the version number."""
    import mlflow

    mlflow.set_tracking_uri(config.MLFLOW_TRACKING_URI)
    version = mlflow.register_model(model_uri, config.REGISTERED_MODEL)
    return str(version.version)


def _version_by_alias(alias: str):
    from mlflow.exceptions import MlflowException

    try:
        return _client().get_model_version_by_alias(config.REGISTERED_MODEL, alias)
    except MlflowException:  # no such alias, or no such model yet
        return None


def _describe(version) -> dict[str, Any]:
    run = _client().get_run(version.run_id)
    return {"version": str(version.version), "run_id": version.run_id, "metrics": dict(run.data.metrics)}


def get_production() -> dict[str, Any] | None:
    """The champion: its version, its run and the metrics it was promoted with."""
    version = _version_by_alias(config.CHAMPION_ALIAS)
    return _describe(version) if version else None


def promote(version: str) -> dict[str, Any]:
    """Point `champion` at `version`, keeping the old champion as `previous_champion`."""
    client = _client()
    current = _version_by_alias(config.CHAMPION_ALIAS)
    if current is not None and str(current.version) != str(version):
        client.set_registered_model_alias(config.REGISTERED_MODEL, config.PREVIOUS_ALIAS, current.version)
    client.set_registered_model_alias(config.REGISTERED_MODEL, config.CHAMPION_ALIAS, version)
    return {
        **_describe(client.get_model_version(config.REGISTERED_MODEL, version)),
        "previous_version": str(current.version) if current else None,
    }


def rollback() -> dict[str, Any] | None:
    """Move `champion` back to `previous_champion`. None if there is nothing to go back to."""
    previous = _version_by_alias(config.PREVIOUS_ALIAS)
    if previous is None:
        return None
    return promote(str(previous.version))


def load_model(alias: str = config.CHAMPION_ALIAS):
    import mlflow.sklearn

    from project_tagger.train import quiet_skops

    _client()
    with quiet_skops():
        return mlflow.sklearn.load_model(f"models:/{config.REGISTERED_MODEL}@{alias}")


def load_metrics(run_id: str) -> dict[str, Any]:
    """The full nested metrics a training run logged as metrics.json."""
    import mlflow

    mlflow.set_tracking_uri(config.MLFLOW_TRACKING_URI)
    return mlflow.artifacts.load_dict(f"runs:/{run_id}/metrics.json")
