"""Tests for the ML code itself - no Airflow imported.

Keeping the workload testable outside a Dag run is what lets you debug it
without a scheduler in the loop.
"""

from __future__ import annotations

import pandas as pd
import pytest

from tagifai import data, monitoring, validate

RAW = pd.DataFrame(
    {
        "id": range(1, 121),
        "created_on": pd.date_range("2020-01-01", periods=120, freq="D").astype(str),
        "title": ["Fine-tuning BERT for classification"] * 60
        + ["Object detection with YOLO"] * 40
        + ["Deploying models with Kubernetes"] * 20,
        "description": ["a description of the project"] * 120,
        "tag": ["natural-language-processing"] * 60 + ["computer-vision"] * 40 + ["mlops"] * 20,
    }
)


def test_clean_text_lowercases_and_strips_punctuation() -> None:
    assert data.clean_text("Hello, World! 42") == "hello world 42"


def test_transform_produces_text_and_collapses_rare_tags() -> None:
    out = data.transform(RAW, min_tag_freq=30)
    assert "text" in out.columns
    assert out["text"].str.len().gt(0).all()
    # `mlops` has 20 rows, below the threshold, so it becomes `other`.
    assert "mlops" not in set(out["tag"])
    assert "other" in set(out["tag"])


def test_split_is_stratified_and_disjoint() -> None:
    df = data.transform(RAW, min_tag_freq=10)
    train, test = data.split(df, test_size=0.25)
    assert len(train) + len(test) == len(df)
    assert set(train["id"]).isdisjoint(set(test["id"]))
    assert set(train["tag"]) == set(test["tag"])


def test_suite_passes_on_good_data() -> None:
    df = data.transform(RAW, min_tag_freq=10)
    # min_rows expects a real-sized table; check the per-expectation results.
    result = validate.run_suite(df, "labeled_projects")
    failed = {r["expectation"] for r in result.failures}
    assert failed <= {"min_rows:500"}


def test_suite_catches_a_null_label() -> None:
    df = data.transform(RAW, min_tag_freq=10)
    df.loc[0, "tag"] = None
    result = validate.run_suite(df, "labeled_projects")
    assert not result.passed
    assert "not_null:tag" in {r["expectation"] for r in result.failures}


def test_check_split_passes_on_a_stratified_split() -> None:
    df = data.transform(RAW, min_tag_freq=10)
    result = validate.check_split(*data.split(df), min_rows_per_class=5)
    assert result.passed, result.failures


def test_check_split_catches_a_class_missing_from_test() -> None:
    df = data.transform(RAW, min_tag_freq=10)
    train, test = data.split(df)
    test = test[test["tag"] != "mlops"]
    result = validate.check_split(train, test, min_rows_per_class=5)
    assert not result.passed
    assert "classes_in_both_splits" in {r["expectation"] for r in result.failures}


def test_check_split_catches_an_under_represented_class() -> None:
    df = data.transform(RAW, min_tag_freq=10)
    train, test = data.split(df)
    result = validate.check_split(train, test, min_rows_per_class=1000)
    assert not result.passed
    assert "min_rows_per_class:1000" in {r["expectation"] for r in result.failures}


def test_check_split_catches_leakage_between_splits() -> None:
    df = data.transform(RAW, min_tag_freq=10)
    train, test = data.split(df)
    leaky = pd.concat([test, train.head(1)])
    result = validate.check_split(train, leaky, min_rows_per_class=5)
    assert not result.passed
    assert "no_id_leakage" in {r["expectation"] for r in result.failures}


def test_total_variation_distance_bounds() -> None:
    assert monitoring.total_variation_distance({"a": 1.0}, {"a": 1.0}) == pytest.approx(0.0)
    assert monitoring.total_variation_distance({"a": 1.0}, {"b": 1.0}) == pytest.approx(1.0)


def test_drift_report_flags_a_new_label() -> None:
    df = data.transform(RAW, min_tag_freq=10)
    reference, current = monitoring.split_reference_current(df, current_frac=0.25)
    current = current.copy()
    current.loc[current.index[0], "tag"] = "reinforcement-learning"
    report = monitoring.drift_report(reference, current)
    assert "reinforcement-learning" in report["new_labels"]


@pytest.mark.slow
def test_training_beats_a_trivial_baseline() -> None:
    from tagifai import train as train_module

    df = data.transform(RAW, min_tag_freq=10)
    train_df, test_df = data.split(df, test_size=0.25)
    _, metrics = train_module.train(train_df, test_df)
    majority = test_df["tag"].value_counts(normalize=True).max()
    assert metrics["accuracy"] >= majority
