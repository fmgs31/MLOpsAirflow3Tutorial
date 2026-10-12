"""Monitoring: the windows drift is measured on, and how production is performing.

The drift statistics themselves are in drift.py, which runs in alibi-detect's
own virtualenv. This module runs in Airflow's environment: it reads the
warehouse and the registry, which the drift virtualenv cannot.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from project_tagger import config
from project_tagger import data as data_module
from project_tagger import evaluate as evaluate_module
from project_tagger import registry


def split_reference_current(
    df: pd.DataFrame, current_frac: float = 0.25
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Oldest rows are the reference window, newest are the current window."""
    df = df.sort_values("created_on")
    cut = int(len(df) * (1 - current_frac))
    return df.iloc[:cut], df.iloc[cut:]


def prepare_windows(run_key: str, df: pd.DataFrame | None = None) -> dict[str, str]:
    """Write the reference and current windows to files; return their paths.

    Files, not dataframes, cross the boundary: the drift task runs in another
    Python, and XCom carries paths, never payloads. The raw tags are kept (no
    collapsing into `other`), so a tag the model has never seen stays visible.
    """
    if df is None:
        df = data_module.load_labeled()
    df = data_module.preprocess(df, min_tag_freq=0)
    reference, current = split_reference_current(df)

    folder = Path(config.SNAPSHOTS_DIR) / run_key
    folder.mkdir(parents=True, exist_ok=True)
    paths = {"reference": folder / "reference.csv", "current": folder / "current.csv"}
    reference.to_csv(paths["reference"], index=False)
    current.to_csv(paths["current"], index=False)
    return {name: str(fp) for name, fp in paths.items()}


def production_performance(current_fp: str) -> dict[str, Any] | None:
    """Score the champion on the current window. None if nothing is promoted."""
    champion = registry.get_production()
    if champion is None:
        return None
    # Score on the label space the model was trained on: a tag it never
    # learned counts as `other`, as it did at training time.
    current = pd.read_csv(current_fp)
    current["tag"] = current["tag"].where(current["tag"].isin(config.ACCEPTED_TAGS), config.OTHER_TAG)
    metrics = evaluate_module.evaluate(registry.load_model(), current)
    return {
        "version": champion["version"],
        "f1_at_promotion": champion["metrics"]["f1"],
        "f1_now": metrics["f1"],
        "slices": metrics["slices"],
    }
