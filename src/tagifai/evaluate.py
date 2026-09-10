"""Evaluation: overall metrics, per-class metrics, and slice metrics.

Slices matter for the monitoring Dag - an overall f1 that holds steady while one
slice collapses is exactly the failure a retraining trigger should catch.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from sklearn.metrics import precision_recall_fscore_support


def _prf(y_true, y_pred, average: str | None) -> dict[str, float]:
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_true, y_pred, average=average, zero_division=0
    )
    return {"precision": float(precision), "recall": float(recall), "f1": float(f1)}


def evaluate(model, df: pd.DataFrame) -> dict[str, Any]:
    y_true = df["tag"]
    y_pred = model.predict(df["text"])

    metrics: dict[str, Any] = {
        "accuracy": float((y_true.to_numpy() == y_pred).mean()),
        **_prf(y_true, y_pred, average="weighted"),
    }

    metrics["per_class"] = {}
    for tag in sorted(y_true.unique()):
        mask = y_true == tag
        metrics["per_class"][str(tag)] = {
            "support": int(mask.sum()),
            **_prf(y_true[mask], y_pred[mask.to_numpy()], average="micro"),
        }

    metrics["slices"] = slice_metrics(df, y_pred)
    return metrics


def slice_metrics(df: pd.DataFrame, y_pred) -> dict[str, Any]:
    """Behavioural slices: short texts and NLP-jargon texts."""
    slices = {
        "short_text": df["text"].str.split().str.len() < 8,
        "mentions_transformer": df["text"].str.contains("transformer|bert|llm", regex=True),
    }
    out: dict[str, Any] = {}
    for name, mask in slices.items():
        mask = mask.fillna(False)
        if mask.sum() == 0:
            continue
        out[name] = {
            "support": int(mask.sum()),
            **_prf(df.loc[mask, "tag"], y_pred[mask.to_numpy()], average="micro"),
        }
    return out
