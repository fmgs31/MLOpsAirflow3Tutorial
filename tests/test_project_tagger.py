"""Tests for the ML code itself - no Airflow imported, no warehouse needed.

Keeping the workload testable outside a Dag run is what lets you debug it
without a scheduler in the loop. Nothing here talks to BigQuery: each test
builds the dataframe a warehouse query would have returned. The slow tests
run Optuna and MLflow for real, against throwaway SQLite files.
"""

from __future__ import annotations

import pandas as pd
import pytest

from project_tagger import config, data, monitoring, validate

# What `labeled_projects` looks like when read from the warehouse: raw tags,
# one of them (`graph-learning`, 20 rows) outside the four classes the model
# learns, so collapsed into `other`.
LABELED = pd.DataFrame(
    {
        "id": range(1, 121),
        "created_on": pd.date_range("2020-01-01", periods=120, freq="D").astype(str),
        "title": [f"Fine-tuning BERT for classification {i}" for i in range(60)]
        + [f"Object detection with YOLO {i}" for i in range(40)]
        + [f"Graph neural networks for molecules {i}" for i in range(20)],
        "description": ["a description of the project"] * 120,
        "tag": ["natural-language-processing"] * 60 + ["computer-vision"] * 40 + ["graph-learning"] * 20,
    }
)


def test_clean_text_lowercases_and_strips_punctuation() -> None:
    assert data.clean_text("Hello, World! 42") == "hello world 42"


def test_preprocess_produces_text_and_collapses_unlearned_tags() -> None:
    out = data.preprocess(LABELED)
    assert "text" in out.columns
    assert out["text"].str.len().gt(0).all()
    assert "graph-learning" not in set(out["tag"])
    assert "other" in set(out["tag"])


def test_split_is_stratified_and_disjoint() -> None:
    df = data.preprocess(LABELED)
    train, test = data.split(df, test_size=0.25)
    assert len(train) + len(test) == len(df)
    assert set(train["id"]).isdisjoint(set(test["id"]))
    assert set(train["tag"]) == set(test["tag"])


def test_snapshot_round_trip(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(config, "SNAPSHOTS_DIR", tmp_path)
    fp = data.save_snapshot(LABELED, "run-1")
    assert fp.parent.name == "run-1"
    pd.testing.assert_frame_equal(data.load_snapshot(fp), LABELED)


# --- Great Expectations suites -------------------------------------------------


def test_labeled_suite_passes_on_good_data() -> None:
    result = validate.run_suite(LABELED, "labeled_projects")
    failed = {r["expectation"] for r in result.failures}
    # The row-count expectation wants a real-sized table; everything else passes.
    assert failed == {"expect_table_row_count_to_be_between:table"}


def test_labeled_suite_catches_a_null_label() -> None:
    df = pd.concat([LABELED] * 5, ignore_index=True)
    df["id"] = range(len(df))
    df.loc[0, "tag"] = None
    result = validate.run_suite(df, "labeled_projects")
    assert not result.passed
    assert "expect_column_values_to_not_be_null:tag" in {r["expectation"] for r in result.failures}


def test_an_unknown_tag_warns_but_does_not_fail() -> None:
    tags = pd.DataFrame({"id": [1, 2, 3], "tag": ["mlops", "computer-vision", "quantum-ml"]})
    result = validate.run_suite(tags, "tags")
    assert result.passed
    warnings = [r for r in result.failures if not r["critical"]]
    assert [r["expectation"] for r in warnings] == ["expect_column_values_to_be_in_set:tag"]


def test_projects_suite_catches_duplicates() -> None:
    projects = LABELED[config.PROJECTS_COLUMNS].copy()
    projects.loc[1, ["title", "description"]] = projects.loc[0, ["title", "description"]].to_numpy()
    result = validate.run_suite(projects, "projects")
    assert "expect_compound_columns_to_be_unique:title,description" in {
        r["expectation"] for r in result.failures
    }


# --- The split gate ------------------------------------------------------------


def test_check_split_passes_on_a_stratified_split() -> None:
    df = data.preprocess(LABELED)
    result = validate.check_split(*data.split(df), min_rows_per_class=5)
    assert result.passed, result.failures


def test_check_split_catches_a_class_missing_from_test() -> None:
    df = data.preprocess(LABELED)
    train, test = data.split(df)
    test = test[test["tag"] != "other"]
    result = validate.check_split(train, test, min_rows_per_class=5)
    assert not result.passed
    assert "classes_in_both_splits" in {r["expectation"] for r in result.failures}


def test_check_split_catches_an_under_represented_class() -> None:
    df = data.preprocess(LABELED)
    train, test = data.split(df)
    result = validate.check_split(train, test, min_rows_per_class=1000)
    assert not result.passed
    assert "min_rows_per_class:1000" in {r["expectation"] for r in result.failures}


def test_check_split_catches_leakage_between_splits() -> None:
    df = data.preprocess(LABELED)
    train, test = data.split(df)
    leaky = pd.concat([test, train.head(1)])
    result = validate.check_split(train, leaky, min_rows_per_class=5)
    assert not result.passed
    assert "no_id_leakage" in {r["expectation"] for r in result.failures}


# --- Monitoring ----------------------------------------------------------------


def test_prepare_windows_splits_by_date(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(config, "SNAPSHOTS_DIR", tmp_path)
    windows = monitoring.prepare_windows("monitoring-1", df=LABELED)
    reference = pd.read_csv(windows["reference"])
    current = pd.read_csv(windows["current"])
    assert len(reference) + len(current) == len(LABELED)
    assert reference["created_on"].max() < current["created_on"].min()
    # Raw tags are kept, so a tag the model never saw stays visible to drift checks.
    assert "graph-learning" in set(current["tag"])


def test_drift_flags_a_new_label(tmp_path, monkeypatch) -> None:
    """alibi-detect lives in its own virtualenv; this runs only where it is installed."""
    pytest.importorskip("alibi_detect")
    from project_tagger import drift

    monkeypatch.setattr(config, "SNAPSHOTS_DIR", tmp_path)
    windows = monitoring.prepare_windows("monitoring-2", df=LABELED)
    current = pd.read_csv(windows["current"])
    current.loc[current.index[:10], "tag"] = "reinforcement-learning"
    current.to_csv(windows["current"], index=False)

    report = drift.detect_drift(windows["reference"], windows["current"], p_val=0.05)
    assert "reinforcement-learning" in report["new_labels"]
    assert report["label_drift"]["is_drift"]


# --- Tuning, tracking, registry --------------------------------------------------


@pytest.fixture
def local_tracking(tmp_path, monkeypatch):
    """Optuna and MLflow on SQLite files that vanish with the test."""
    pytest.importorskip("optuna")
    pytest.importorskip("mlflow")
    monkeypatch.setattr(config, "OPTUNA_STORAGE", f"sqlite:///{(tmp_path / 'optuna.db').as_posix()}")
    monkeypatch.setattr(config, "MLFLOW_TRACKING_URI", f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}")
    monkeypatch.chdir(tmp_path)  # MLflow's local artifact store goes here


@pytest.mark.slow
def test_training_beats_a_trivial_baseline() -> None:
    from project_tagger import train as train_module

    df = data.preprocess(LABELED)
    train_df, test_df = data.split(df, test_size=0.25)
    _, metrics = train_module.fit(train_df, test_df)
    majority = test_df["tag"].value_counts(normalize=True).max()
    assert metrics["accuracy"] >= majority


@pytest.mark.slow
def test_tune_train_register_promote_rollback(local_tracking) -> None:
    """The mlops Dag's ML calls, in order, without Airflow."""
    from project_tagger import registry, train as train_module

    df = data.preprocess(LABELED)
    train_df, test_df = data.split(df)

    study = train_module.create_study("test-study")
    assert train_module.create_study("test-study") == study  # a retry loads, not fails
    trials = train_module.run_trials(study, 2, train_df) + train_module.run_trials(study, 2, train_df)
    assert len(trials) == 4  # two "workers" added to one study
    best = train_module.best_trial(study)
    assert best["num_trials"] == 4

    import mlflow

    trial_runs = mlflow.search_runs(filter_string="tags.kind = 'trial'")
    study_runs = mlflow.search_runs(filter_string="tags.kind = 'study'")
    assert len(study_runs) == 1  # the retried create_study reused it
    assert set(trial_runs["tags.mlflow.parentRunId"]) == {study_runs["run_id"][0]}
    assert study_runs["tags.best_trial_run_id"][0] == best["mlflow_run_id"]

    versions = []
    for i in range(2):
        trained = train_module.train_model(best["params"], train_df, test_df, run_name=f"final-{i}")
        assert registry.load_metrics(trained["run_id"])["f1"] == pytest.approx(trained["metrics"]["f1"])
        versions.append(registry.register(trained["model_uri"]))

    assert registry.get_production() is None
    assert registry.rollback() is None  # nothing to go back to

    registry.promote(versions[0])
    champion = registry.promote(versions[1])
    assert champion["version"] == versions[1]
    assert champion["previous_version"] == versions[0]

    assert registry.rollback()["version"] == versions[0]
    assert registry.get_production()["version"] == versions[0]
    assert len(registry.load_model().predict(["object detection with yolo"])) == 1
