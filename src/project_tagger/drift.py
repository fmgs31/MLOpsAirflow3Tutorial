"""Drift detection with alibi-detect.

*Drift* is the data changing under a trained model. Each detector here takes a
**reference** window (what the model was built on) and a **current** window
(what it sees now), runs a statistical test, and reports a p-value: the
probability of seeing a difference this large if nothing had changed. A
p-value below the threshold means drift.

    ChiSquareDrift  on the tag            target drift: has the label mix moved?
    KSDrift         on the text length    data drift: do the inputs look different?

alibi-detect pins numpy<2, which Airflow 3.3.1 cannot accept, so this module
runs in its own virtualenv (/opt/venvs/alibi, see Dockerfile) through
`@task.external_python`. That is why it imports only numpy, pandas (with
pyarrow, to read Parquet) and alibi-detect - nothing else from this package,
and nothing from Airflow.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def _summary(prediction: dict[str, Any]) -> dict[str, Any]:
    data = prediction["data"]
    return {
        "is_drift": bool(data["is_drift"]),
        "p_val": float(np.ravel(data["p_val"])[0]),
        "distance": float(np.ravel(data["distance"])[0]),
        "threshold": float(data["threshold"]),
    }


def label_drift(reference: pd.Series, current: pd.Series, p_val: float) -> dict[str, Any]:
    from alibi_detect.cd import ChiSquareDrift

    # The test needs integer categories. Encode over both windows, so a tag
    # that only appears in the current window still gets a code of its own.
    categories = sorted(set(reference) | set(current))
    codes = {tag: i for i, tag in enumerate(categories)}
    x_ref = reference.map(codes).to_numpy().reshape(-1, 1)
    x_cur = current.map(codes).to_numpy().reshape(-1, 1)
    detector = ChiSquareDrift(x_ref, p_val=p_val, categories_per_feature={0: len(categories)})
    return _summary(detector.predict(x_cur))


def length_drift(reference: pd.Series, current: pd.Series, p_val: float) -> dict[str, Any]:
    from alibi_detect.cd import KSDrift

    x_ref = reference.str.split().str.len().to_numpy(dtype="float32").reshape(-1, 1)
    x_cur = current.str.split().str.len().to_numpy(dtype="float32").reshape(-1, 1)
    detector = KSDrift(x_ref, p_val=p_val)
    return _summary(detector.predict(x_cur))


def distribution(tags: pd.Series) -> dict[str, float]:
    return {str(tag): float(share) for tag, share in tags.value_counts(normalize=True).items()}


def detect_drift(
    reference_fp: str | None, current_fp: str | None, p_val: float = 0.05
) -> dict[str, Any] | None:
    """Compare the two windows written by `monitoring.prepare_windows`.

    None when it wrote none: no champion, or too few new rows to measure.
    """
    if reference_fp is None or current_fp is None:
        return None
    reference = pd.read_parquet(reference_fp)
    current = pd.read_parquet(current_fp)
    return {
        "label_drift": label_drift(reference["tag"], current["tag"], p_val),
        "length_drift": length_drift(reference["text"], current["text"], p_val),
        "new_labels": sorted(set(current["tag"]) - set(reference["tag"])),
        "reference_distribution": distribution(reference["tag"]),
        "current_distribution": distribution(current["tag"]),
        "num_reference": len(reference),
        "num_current": len(current),
    }
