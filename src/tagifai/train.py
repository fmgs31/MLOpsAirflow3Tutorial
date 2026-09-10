"""Model training - a TF-IDF + SGD text classifier over project tags.

Small on purpose: the tutorial is about orchestration, and a run that takes
seconds lets you iterate on the Dag instead of waiting on the model.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import SGDClassifier
from sklearn.pipeline import Pipeline

from tagifai import data as data_module
from tagifai.evaluate import evaluate

DEFAULT_PARAMS: dict[str, Any] = {
    "ngram_max": 2,
    "min_df": 2,
    "alpha": 1e-4,
    "max_iter": 20,
    "seed": 42,
}

# The grid the mlops Dag fans out over with dynamic task mapping.
SEARCH_SPACE: list[dict[str, Any]] = [
    {"ngram_max": 1, "alpha": 1e-3},
    {"ngram_max": 2, "alpha": 1e-4},
    {"ngram_max": 2, "alpha": 1e-5},
    {"ngram_max": 3, "alpha": 1e-4},
]


def build_pipeline(params: dict[str, Any]) -> Pipeline:
    params = {**DEFAULT_PARAMS, **params}
    return Pipeline(
        [
            (
                "tfidf",
                TfidfVectorizer(
                    ngram_range=(1, int(params["ngram_max"])),
                    min_df=int(params["min_df"]),
                    sublinear_tf=True,
                ),
            ),
            (
                "clf",
                SGDClassifier(
                    loss="log_loss",
                    alpha=float(params["alpha"]),
                    max_iter=int(params["max_iter"]),
                    random_state=int(params["seed"]),
                    class_weight="balanced",
                ),
            ),
        ]
    )


def train(
    train_df: pd.DataFrame, test_df: pd.DataFrame, params: dict[str, Any] | None = None
) -> tuple[Pipeline, dict[str, Any]]:
    """Fit on `train_df`, score on `test_df`. Returns (model, metrics)."""
    params = {**DEFAULT_PARAMS, **(params or {})}
    pipeline = build_pipeline(params)
    pipeline.fit(train_df["text"], train_df["tag"])
    metrics = evaluate(pipeline, test_df)
    metrics["params"] = params
    metrics["num_train"] = len(train_df)
    metrics["num_test"] = len(test_df)
    return pipeline, metrics


def train_from_files(params: dict[str, Any] | None = None) -> tuple[Pipeline, dict[str, Any]]:
    """Convenience entry point used by the Dags and by `python -m tagifai.train`."""
    df = data_module.load_labeled()
    train_df, test_df = data_module.split(df)
    return train(train_df, test_df, params)


if __name__ == "__main__":
    _, metrics = train_from_files()
    print(f"f1={metrics['f1']:.4f} accuracy={metrics['accuracy']:.4f}")
