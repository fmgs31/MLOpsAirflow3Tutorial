"""The model registry: MLflow's, used through two aliases.

Registering a model gives it a numbered *version* under one name
(`project_tagger`). An *alias* is a movable label on one version:

    champion            the version in production - what predict.py loads
    previous_champion   the version champion pointed at before the last promotion

Promoting moves `champion` to a new version; rolling back moves it back to
`previous_champion` and removes that alias, so a second rollback has nothing
to go back to instead of bringing the bad version back. No model file is
copied: consumers ask for `models:/project_tagger@champion` and get whatever
the alias points at.

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


def rollback(reason: str) -> dict[str, Any] | None:
    """Move `champion` back to `previous_champion`. None if there is nothing to go back to.

    Not `promote()`: that would keep the version being retired as the new
    `previous_champion`, and the next rollback would put it back in
    production. The alias is removed instead - there is no "previous" until
    the next promotion - and the retired version is tagged with why it left.
    """
    client = _client()
    previous = _version_by_alias(config.PREVIOUS_ALIAS)
    if previous is None:
        return None
    retired = _version_by_alias(config.CHAMPION_ALIAS)
    client.set_registered_model_alias(config.REGISTERED_MODEL, config.CHAMPION_ALIAS, previous.version)
    client.delete_registered_model_alias(config.REGISTERED_MODEL, config.PREVIOUS_ALIAS)
    if retired is not None:
        client.set_model_version_tag(config.REGISTERED_MODEL, retired.version, "rolled_back", "true")
        client.set_model_version_tag(config.REGISTERED_MODEL, retired.version, "rolled_back_reason", reason)
    return {
        **_describe(previous),
        "rolled_back_version": str(retired.version) if retired else None,
    }


def load_model(alias: str = config.CHAMPION_ALIAS, version: str | None = None):
    """The version an alias points at, or the given version."""
    import mlflow.sklearn

    from project_tagger.train import quiet_skops

    _client()
    ref = f"/{version}" if version else f"@{alias}"
    with quiet_skops():
        return mlflow.sklearn.load_model(f"models:/{config.REGISTERED_MODEL}{ref}")


def load_split_ids(run_id: str) -> dict[str, list]:
    """The project ids in each split of a training run, as it logged them in split_ids.json."""
    import mlflow

    mlflow.set_tracking_uri(config.MLFLOW_TRACKING_URI)
    return mlflow.artifacts.load_dict(f"runs:/{run_id}/split_ids.json")
