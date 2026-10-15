"""Names, paths and constants shared by the tutorial Dags.

Nothing in this module imports Airflow: the ML code stays runnable on its own
(`python -m project_tagger.train`), which is exactly what makes it testable outside a
Dag run. Airflow only orchestrates it.

Every value that differs between your machine and someone else's - the GCP
project, where MLflow and Optuna keep their state - comes from an environment
variable, set in docker-compose.yaml.
"""

from __future__ import annotations

import os
from pathlib import Path

# /opt/airflow/src/project_tagger/config.py -> /opt/airflow
PROJECT_ROOT = Path(os.getenv("PROJECT_TAGGER_PROJECT_ROOT", Path(__file__).resolve().parents[2]))

# Local scratch space: each mlops run pins the exact rows it trained on here.
DATA_DIR = Path(os.getenv("PROJECT_TAGGER_DATA_DIR", PROJECT_ROOT / "data"))
SNAPSHOTS_DIR = DATA_DIR / "snapshots"

# --- The warehouse (§9) --------------------------------------------------------
# Airbyte loads `projects` and `tags`; dbt builds `labeled_projects` from them.
# The placeholder keeps the Dags parseable before you have a GCP project; any
# task that touches BigQuery fails until GCP_PROJECT_ID is set.
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID") or "your-gcp-project-id"
BQ_DATASET = os.getenv("BQ_DATASET", "mlops_course")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

PROJECTS_TABLE = "projects"
TAGS_TABLE = "tags"
LABELED_PROJECTS_TABLE = "labeled_projects"

# The columns that belong to the data. Airbyte adds its own bookkeeping columns
# (`_airbyte_raw_id`, `_airbyte_extracted_at`, ...) to every table it loads;
# reading only these keeps them out of the expectations and the model.
PROJECTS_COLUMNS = ["id", "created_on", "title", "description"]
TAGS_COLUMNS = ["id", "tag"]
LABELED_PROJECTS_COLUMNS = ["id", "created_on", "title", "description", "tag"]

# The dbt project you create in §9, and the dbt executable (its own
# virtualenv - see Dockerfile).
DBT_PROJECT_DIR = Path(
    os.getenv("PROJECT_TAGGER_DBT_PROJECT_DIR", PROJECT_ROOT / "dbt" / "dbt_transforms")
)
DBT_BIN = os.getenv("PROJECT_TAGGER_DBT_BIN", "/opt/venvs/dbt/bin/dbt")

# --- The ML side (§10) ---------------------------------------------------------
# The four classes the model learns. Every other raw tag becomes `other`.
OTHER_TAG = "other"
ACCEPTED_TAGS = ["natural-language-processing", "computer-vision", "mlops", "other"]

# Every tag the source system is known to emit. A tag outside this set is the
# label space drifting - worth a human look before anything is retrained.
KNOWN_RAW_TAGS = [
    "natural-language-processing",
    "computer-vision",
    "mlops",
    "reinforcement-learning",
    "graph-learning",
    "time-series",
]

# MLflow: where runs are tracked and models registered. Without the env var
# MLflow writes to a SQLite file in the working directory.
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "sqlite:///mlflow.db")
MLFLOW_EXPERIMENT = "project_tagger"
REGISTERED_MODEL = "project_tagger"
CHAMPION_ALIAS = "champion"
PREVIOUS_ALIAS = "previous_champion"

# Optuna: one study per mlops run, stored where every parallel task can reach it.
OPTUNA_STORAGE = os.getenv("OPTUNA_STORAGE", "sqlite:///optuna.db")

# The gate the mlops Dag enforces before a model can be promoted.
MIN_F1_TO_PROMOTE = float(os.getenv("PROJECT_TAGGER_MIN_F1", "0.65"))

# Fewer rows than this for a class in the training split and its metrics are
# noise: the mlops Dag refuses to train on such a snapshot.
MIN_ROWS_PER_CLASS = int(os.getenv("PROJECT_TAGGER_MIN_ROWS_PER_CLASS", "20"))

# --- Monitoring (§12) ----------------------------------------------------------
# alibi-detect lives in its own virtualenv (see Dockerfile); the monitoring Dag
# runs its drift checks with this interpreter.
ALIBI_PYTHON = os.getenv("PROJECT_TAGGER_ALIBI_PYTHON", "/opt/venvs/alibi/bin/python")


def table_id(table: str) -> str:
    """Fully qualified BigQuery table id: `project.dataset.table`."""
    return f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table}"


def ensure_dirs() -> None:
    """Create the directories the pipeline writes to. Safe to call repeatedly."""
    for directory in (DATA_DIR, SNAPSHOTS_DIR):
        directory.mkdir(parents=True, exist_ok=True)
