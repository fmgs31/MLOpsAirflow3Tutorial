"""Drift and performance checks that decide whether to retrain.

Deliberately simple statistics - total variation distance on the label
distribution, a mean-length shift on the inputs, and live performance of the
production model - so the monitoring Dag has something real to branch on
without adding dependencies.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from tagifai import data as data_module
from tagifai import evaluate as evaluate_module
from tagifai import registry


def label_distribution(df: pd.DataFrame) -> dict[str, float]:
    counts = df["tag"].value_counts(normalize=True)
    return {str(tag): float(share) for tag, share in counts.items()}


def total_variation_distance(a: dict[str, float], b: dict[str, float]) -> float:
    """0.0 = identical distributions, 1.0 = disjoint."""
    keys = set(a) | set(b)
    return 0.5 * sum(abs(a.get(k, 0.0) - b.get(k, 0.0)) for k in keys)


def split_reference_current(df: pd.DataFrame, current_frac: float = 0.25) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Oldest rows are the reference window, newest are the current window."""
    df = df.sort_values("created_on")
    cut = int(len(df) * (1 - current_frac))
    return df.iloc[:cut], df.iloc[cut:]


def drift_report(reference: pd.DataFrame, current: pd.DataFrame) -> dict[str, Any]:
    ref_dist, cur_dist = label_distribution(reference), label_distribution(current)
    new_labels = sorted(set(cur_dist) - set(ref_dist))
    ref_len = float(reference["text"].str.split().str.len().mean())
    cur_len = float(current["text"].str.split().str.len().mean())
    return {
        "label_tvd": total_variation_distance(ref_dist, cur_dist),
        "new_labels": new_labels,
        "reference_distribution": ref_dist,
        "current_distribution": cur_dist,
        "mean_length_shift": (cur_len - ref_len) / ref_len if ref_len else 0.0,
        "num_reference": len(reference),
        "num_current": len(current),
    }


def production_performance(current: pd.DataFrame) -> dict[str, Any] | None:
    """Score the live model on the current window. None if nothing is promoted."""
    pointer = registry.get_production()
    if pointer is None:
        return None
    model = registry.load_model(pointer["run_id"])
    metrics = evaluate_module.evaluate(model, current)
    return {
        "run_id": pointer["run_id"],
        "f1_at_promotion": pointer["metrics"]["f1"],
        "f1_now": metrics["f1"],
        "slices": metrics["slices"],
    }


def monitor() -> dict[str, Any]:
    df = data_module.load_labeled()
    reference, current = split_reference_current(df)
    return {
        "drift": drift_report(reference, current),
        "performance": production_performance(current),
    }
