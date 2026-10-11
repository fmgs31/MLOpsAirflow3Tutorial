"""A filesystem stand-in for a model registry (MLflow, W&B, Vertex, SageMaker).

The rule it exists to demonstrate: **big artifacts never travel through XCom.**
Tasks write the model here and pass a run id - a short string - between them.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib

from project_tagger import config

PRODUCTION_POINTER = "production.json"


def run_dir(run_id: str) -> Path:
    return config.MODEL_REGISTRY / run_id


def save_run(run_id: str, model, metrics: dict[str, Any]) -> str:
    """Persist model + metrics under `run_id`. Returns the run id (the XCom payload)."""
    directory = run_dir(run_id)
    directory.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, directory / "model.joblib")
    (directory / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    (directory / "meta.json").write_text(
        json.dumps(
            {"run_id": run_id, "created_at": datetime.now(timezone.utc).isoformat()}, indent=2
        ),
        encoding="utf-8",
    )
    return run_id


def load_metrics(run_id: str) -> dict[str, Any]:
    return json.loads((run_dir(run_id) / "metrics.json").read_text(encoding="utf-8"))


def load_model(run_id: str):
    return joblib.load(run_dir(run_id) / "model.joblib")


def list_runs() -> list[str]:
    if not config.MODEL_REGISTRY.exists():
        return []
    return sorted(p.name for p in config.MODEL_REGISTRY.iterdir() if (p / "model.joblib").exists())


def promote(run_id: str) -> dict[str, Any]:
    """Point production at `run_id`, keeping the previous pointer for rollback."""
    current = get_production()
    pointer = {
        "run_id": run_id,
        "promoted_at": datetime.now(timezone.utc).isoformat(),
        "previous_run_id": current.get("run_id") if current else None,
        "metrics": load_metrics(run_id),
    }
    config.MODEL_REGISTRY.mkdir(parents=True, exist_ok=True)
    (config.MODEL_REGISTRY / PRODUCTION_POINTER).write_text(
        json.dumps(pointer, indent=2), encoding="utf-8"
    )
    return pointer


def get_production() -> dict[str, Any] | None:
    fp = config.MODEL_REGISTRY / PRODUCTION_POINTER
    if not fp.exists():
        return None
    return json.loads(fp.read_text(encoding="utf-8"))


def rollback() -> dict[str, Any] | None:
    """Re-promote whatever production pointed at before the last promotion."""
    current = get_production()
    if not current or not current.get("previous_run_id"):
        return None
    return promote(current["previous_run_id"])
