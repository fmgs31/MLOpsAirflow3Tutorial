"""Monitoring: the windows drift is measured on, and how production is performing.

Both windows are defined by the champion, from the ids its training run logged
(split_ids.json):

    reference   rows the champion learned from (its train and validation splits)
    current     rows the champion has never seen: they arrived after it was trained

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


def prepare_windows(run_key: str, df: pd.DataFrame | None = None) -> dict[str, Any]:
    """Write the reference and current windows to files; return their paths.

    Returns `version` (the champion's, or None if nothing is promoted),
    `num_current`, and the two paths - None when there is no champion, or
    fewer than `config.MIN_MONITOR_ROWS` new rows: nothing to measure yet.

    Files, not dataframes, cross the boundary: the drift task runs in another
    Python, and XCom carries paths, never payloads. The raw tags are kept (no
    collapsing into `other`), so a tag the model has never seen stays visible.
    """
    champion = registry.get_production()
    if champion is None:
        return {"version": None, "num_current": 0, "reference": None, "current": None}

    if df is None:
        df = data_module.load_labeled()
    df = data_module.preprocess(df, collapse_tags=False)
    split_ids = registry.load_split_ids(champion["run_id"])
    learned = set(split_ids["train"]) | set(split_ids["val"])
    seen = learned | set(split_ids["test"])
    reference = df[df["id"].isin(learned)]
    current = df[~df["id"].isin(seen)]

    windows: dict[str, Any] = {
        "version": champion["version"],
        "num_current": len(current),
        "reference": None,
        "current": None,
    }
    if len(current) < config.MIN_MONITOR_ROWS:
        return windows

    folder = Path(config.SNAPSHOTS_DIR) / run_key
    folder.mkdir(parents=True, exist_ok=True)
    for name, window in (("reference", reference), ("current", current)):
        fp = folder / f"{name}.parquet"
        window.to_parquet(fp, index=False)
        windows[name] = str(fp)
    return windows


def production_performance(current_fp: str | None) -> dict[str, Any] | None:
    """Score the champion on the current window. None if there is no window to score on."""
    champion = registry.get_production()
    if champion is None or current_fp is None:
        return None
    # Score on the label space the model was trained on: a tag it never
    # learned counts as `other`, as it did at training time.
    current = pd.read_parquet(current_fp)
    current["tag"] = current["tag"].where(current["tag"].isin(config.ACCEPTED_TAGS), config.OTHER_TAG)
    metrics = evaluate_module.evaluate(registry.load_model(), current)
    return {
        "version": champion["version"],
        "f1_at_promotion": champion["metrics"]["f1"],
        "f1_now": metrics["f1"],
        "slices": metrics["slices"],
    }
