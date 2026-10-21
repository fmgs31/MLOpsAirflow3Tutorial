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


def compare(candidate, champion, test_df: pd.DataFrame, champion_ids) -> dict[str, Any]:
    """Both models on the same test rows: all of them, and in two parts.

    `new` is the rows the champion has never seen, in any of its splits; `old`
    is the rest, which are its own test rows (the hash split keeps a test row
    out of every model's training data). Each part has both f1s; a part with
    no rows has None for both.
    """
    seen = test_df["id"].isin(set(champion_ids))
    out: dict[str, Any] = {}
    for name, part in {"all": test_df, "new": test_df[~seen], "old": test_df[seen]}.items():
        if part.empty:
            out[name] = {"rows": 0, "f1": None, "champion_f1": None}
            continue
        out[name] = {
            "rows": len(part),
            "f1": _prf(part["tag"], candidate.predict(part["text"]), "weighted")["f1"],
            "champion_f1": _prf(part["tag"], champion.predict(part["text"]), "weighted")["f1"],
        }
    return out


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
