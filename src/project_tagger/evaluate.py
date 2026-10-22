"""Evaluation: overall metrics, per-class metrics, slice metrics, and the promotion rule.

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


def should_promote(
    f1: float, comparison: dict[str, Any] | None, threshold: float, min_new_rows: int
) -> tuple[bool, str]:
    """The promotion rule. Returns the decision and why.

    `f1`, the new model's on the whole test split, must clear the threshold.
    Against a champion, `comparison` (compare) holds both models' f1 on the
    same test rows. The `new` rows, which the champion has never seen, are the
    closest thing to what production gets next, so they decide: the new model
    must not score below the champion on them. With fewer than `min_new_rows`
    new rows there is too little to decide on, and the whole split decides.
    """
    if f1 < threshold:
        return False, f"f1={f1:.4f} below threshold {threshold} - not promoting"
    if comparison is None:
        return True, f"f1={f1:.4f} clears threshold {threshold} and no champion - promoting"

    new = comparison["new"]
    if new["rows"] < min_new_rows:
        rows = comparison["all"]
        if rows["f1"] < rows["champion_f1"]:
            return False, (
                f"f1={rows['f1']:.4f} below the champion's {rows['champion_f1']:.4f} "
                f"on the same {rows['rows']} test rows - not promoting"
            )
        return True, (
            f"f1={rows['f1']:.4f} clears threshold {threshold} and the champion's "
            f"{rows['champion_f1']:.4f} on the same {rows['rows']} test rows - promoting"
        )

    if new["f1"] < new["champion_f1"]:
        return False, (
            f"f1={new['f1']:.4f} below the champion's {new['champion_f1']:.4f} "
            f"on the {new['rows']} new test rows - not promoting"
        )
    return True, (
        f"f1={new['f1']:.4f} clears the champion's {new['champion_f1']:.4f} "
        f"on the {new['rows']} new test rows - promoting"
    )


def summarize(version: int, f1: float, slices: dict[str, Any], comparison: dict[str, Any] | None) -> str:
    """A candidate's numbers as Markdown, for the person who approves its promotion.

    Every slice is listed with its row count: on a few rows one prediction
    moves the f1 a lot, and the reader has to see that to weigh it.
    """
    lines = [f"Version {version}: f1 {f1:.4f} on the test split."]
    if comparison is None:
        lines += ["", "No champion yet."]
    else:
        lines += ["", "Against the champion, on the same test rows:", ""]
        lines += [
            f"- {name}: {part['rows']} rows, f1 {part['f1']:.4f}, champion {part['champion_f1']:.4f}"
            for name, part in comparison.items()
            if part["rows"]
        ]
    lines += ["", "Evaluation slices:", ""]
    lines += [f"- {name}: {m['support']} rows, f1 {m['f1']:.4f}" for name, m in slices.items()]
    return "\n".join(lines)


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
