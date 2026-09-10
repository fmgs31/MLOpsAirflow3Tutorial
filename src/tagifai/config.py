"""Paths and constants shared by the tutorial Dags.

Nothing in this module imports Airflow: the ML code stays runnable on its own
(`python -m tagifai.train`), which is exactly what makes it testable outside a
Dag run. Airflow only orchestrates it.
"""

from __future__ import annotations

import os
from pathlib import Path

# /opt/airflow/src/tagifai/config.py -> /opt/airflow
PROJECT_ROOT = Path(os.getenv("TAGIFAI_PROJECT_ROOT", Path(__file__).resolve().parents[2]))

DATA_DIR = Path(os.getenv("TAGIFAI_DATA_DIR", PROJECT_ROOT / "data"))
MODEL_REGISTRY = Path(os.getenv("TAGIFAI_MODEL_REGISTRY", PROJECT_ROOT / "model_registry"))

# Landing zone (raw, as-extracted) and the curated table the ML Dag consumes.
RAW_DIR = DATA_DIR / "raw"
WAREHOUSE_DIR = DATA_DIR / "warehouse"

PROJECTS_URL = "https://raw.githubusercontent.com/GokuMohandas/Made-With-ML/main/datasets/dataset.csv"
RAW_PROJECTS_FP = RAW_DIR / "projects.csv"
LABELED_PROJECTS_FP = WAREHOUSE_DIR / "labeled_projects.csv"

# Anything rarer than this becomes the catch-all class.
ACCEPTED_TAGS = ["natural-language-processing", "computer-vision", "mlops", "other"]
MIN_TAG_FREQ = 30
OTHER_TAG = "other"

# The gate the mlops Dag enforces before a model can be promoted.
MIN_F1_TO_PROMOTE = float(os.getenv("TAGIFAI_MIN_F1", "0.65"))

# Fewer rows than this for a class in the training split and its metrics are
# noise: the mlops Dag refuses to train on such a snapshot.
MIN_ROWS_PER_CLASS = int(os.getenv("TAGIFAI_MIN_ROWS_PER_CLASS", "20"))


def ensure_dirs() -> None:
    """Create the directories the pipeline writes to. Safe to call repeatedly."""
    for directory in (DATA_DIR, RAW_DIR, WAREHOUSE_DIR, MODEL_REGISTRY):
        directory.mkdir(parents=True, exist_ok=True)
