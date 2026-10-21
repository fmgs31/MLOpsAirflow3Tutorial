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
        "created_on": pd.date_range("2020-01-01", periods=120, freq="D", tz="UTC"),
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


def test_assign_split_is_stable_and_disjoint() -> None:
    ids = pd.Series(range(1, 2001))
    parts = data.assign_split(ids)
    # The same id gets the same part in another call, in any order.
    shuffled = ids.sample(frac=1.0, random_state=0)
    assert data.assign_split(shuffled).sort_index().equals(parts)
    shares = parts.value_counts(normalize=True)
    assert shares["test"] == pytest.approx(config.TEST_PCT / 100, abs=0.03)
    assert shares["val"] == pytest.approx(config.VAL_PCT / 100, abs=0.03)
    assert set(parts) == {"train", "val", "test"}


def test_snapshot_round_trip(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(config, "SNAPSHOTS_DIR", tmp_path)
    paths = data.save_snapshot(LABELED, "run-1")
    assert set(paths) == {"snapshot", "train", "val", "test"}
    snapshot = data.load_snapshot(paths["snapshot"])
    pd.testing.assert_frame_equal(snapshot, LABELED)
    # Parquet keeps the column types a CSV would turn into text.
    assert pd.api.types.is_datetime64_any_dtype(snapshot["created_on"])

    splits = [data.load_snapshot(paths[name]) for name in data.SPLITS]
    assert sum(len(s) for s in splits) == len(data.preprocess(LABELED))
    assert pd.api.types.is_datetime64_any_dtype(splits[0]["created_on"])


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


def _splits() -> list[pd.DataFrame]:
    df = data.preprocess(LABELED)
    parts = data.assign_split(df["id"])
    return [df[parts == name] for name in data.SPLITS]


def test_check_split_passes_on_a_hash_split() -> None:
    result = validate.check_split(*_splits(), min_rows_per_class=5)
    assert result.passed, result.failures


def test_check_split_catches_a_class_missing_from_test() -> None:
    train, val, test = _splits()
    test = test[test["tag"] != "other"]
    result = validate.check_split(train, val, test, min_rows_per_class=5)
    assert not result.passed
    assert "classes_in_every_split" in {r["expectation"] for r in result.failures}


def test_check_split_catches_an_under_represented_class() -> None:
    result = validate.check_split(*_splits(), min_rows_per_class=1000)
    assert not result.passed
    assert "min_rows_per_class:1000" in {r["expectation"] for r in result.failures}


def test_check_split_catches_leakage_between_splits() -> None:
    train, val, test = _splits()
    leaky = pd.concat([test, train.head(1)])
    result = validate.check_split(train, val, leaky, min_rows_per_class=5)
    assert not result.passed
    assert "no_id_leakage" in {r["expectation"] for r in result.failures}


# --- Monitoring ----------------------------------------------------------------


@pytest.fixture
def fake_champion(monkeypatch):
    """A champion whose training run saw ids 1-90: 1-60 in train/val, 61-90 in test."""
    champion = {"version": "3", "run_id": "run-3", "metrics": {"f1": 0.9}}
    split_ids = {"train": list(range(1, 46)), "val": list(range(46, 61)), "test": list(range(61, 91))}
    monkeypatch.setattr(monitoring.registry, "get_production", lambda: champion)
    monkeypatch.setattr(monitoring.registry, "load_split_ids", lambda run_id: split_ids)
    return champion


def test_prepare_windows_uses_the_champions_split_ids(tmp_path, monkeypatch, fake_champion) -> None:
    monkeypatch.setattr(config, "SNAPSHOTS_DIR", tmp_path)
    monkeypatch.setattr(config, "MIN_MONITOR_ROWS", 10)
    windows = monitoring.prepare_windows("monitoring-1", df=LABELED)
    reference = pd.read_parquet(windows["reference"])
    current = pd.read_parquet(windows["current"])
    # What the champion learned from, and what it has never seen; its test rows are in neither.
    assert set(reference["id"]) == set(range(1, 61))
    assert set(current["id"]) == set(range(91, 121))
    assert windows["num_current"] == 30
    assert windows["version"] == "3"
    # Raw tags are kept, so a tag the model never saw stays visible to drift checks.
    assert "graph-learning" in set(current["tag"])


def test_prepare_windows_has_nothing_to_measure_below_the_minimum(
    tmp_path, monkeypatch, fake_champion
) -> None:
    monkeypatch.setattr(config, "SNAPSHOTS_DIR", tmp_path)
    monkeypatch.setattr(config, "MIN_MONITOR_ROWS", 50)
    windows = monitoring.prepare_windows("monitoring-2", df=LABELED)
    assert windows == {"version": "3", "num_current": 30, "reference": None, "current": None}
    assert monitoring.production_performance(windows["current"]) is None


def test_prepare_windows_without_a_champion(monkeypatch) -> None:
    monkeypatch.setattr(monitoring.registry, "get_production", lambda: None)
    windows = monitoring.prepare_windows("monitoring-3", df=LABELED)
    assert windows["version"] is None and windows["current"] is None


def test_drift_flags_a_new_label(tmp_path, monkeypatch, fake_champion) -> None:
    """alibi-detect lives in its own virtualenv; this runs only where it is installed."""
    pytest.importorskip("alibi_detect")
    from project_tagger import drift

    monkeypatch.setattr(config, "SNAPSHOTS_DIR", tmp_path)
    monkeypatch.setattr(config, "MIN_MONITOR_ROWS", 10)
    windows = monitoring.prepare_windows("monitoring-4", df=LABELED)
    current = pd.read_parquet(windows["current"])
    current.loc[current.index[:10], "tag"] = "reinforcement-learning"
    current.to_parquet(windows["current"], index=False)

    report = drift.detect_drift(windows["reference"], windows["current"], p_val=0.05)
    assert "reinforcement-learning" in report["new_labels"]
    assert report["label_drift"]["is_drift"]
    assert drift.detect_drift(None, None) is None


# --- Tuning, tracking, registry --------------------------------------------------


@pytest.fixture
def local_tracking(tmp_path, monkeypatch):
    """Optuna and MLflow on SQLite files that vanish with the test."""
    pytest.importorskip("optuna")
    pytest.importorskip("mlflow")
    monkeypatch.setattr(config, "OPTUNA_STORAGE", f"sqlite:///{(tmp_path / 'optuna.db').as_posix()}")
    monkeypatch.setattr(config, "MLFLOW_TRACKING_URI", f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}")
    monkeypatch.chdir(tmp_path)  # MLflow's local artifact store goes here


def _comparison(new: tuple, old: tuple, all_: tuple) -> dict:
    """evaluate.compare's shape: each part is (rows, new model's f1, champion's f1, interval)."""
    keys = ("rows", "f1", "champion_f1", "difference")
    return {name: dict(zip(keys, part)) for name, part in (("new", new), ("old", old), ("all", all_))}


def test_should_promote() -> None:
    from project_tagger import train as train_module

    def rule(f1, comparison):
        return train_module.should_promote(f1, comparison, 0.65, min_new_rows=30)

    assert rule(0.80, None)[0]  # no champion yet
    assert not rule(0.60, None)[0]  # below the threshold

    # Enough new rows: they decide, and the old rows are a guardrail.
    noise = (-0.04, 0.02)  # a difference chance can explain
    clear_loss = (-0.09, -0.01)
    assert rule(0.80, _comparison((40, 0.85, 0.75, noise), (100, 0.89, 0.90, noise), (140, 0.88, 0.86, noise)))[0]
    promote, reason = rule(
        0.80, _comparison((40, 0.74, 0.75, noise), (100, 0.95, 0.90, noise), (140, 0.90, 0.86, noise))
    )
    assert not promote and "new test rows" in reason  # loses on the new rows, whatever the total says
    promote, reason = rule(
        0.80, _comparison((40, 0.85, 0.75, noise), (100, 0.85, 0.90, clear_loss), (140, 0.85, 0.86, noise))
    )
    assert not promote and "more than chance" in reason  # forgets the old data

    # Too few new rows: the whole test split decides.
    nothing = (0, None, None, None)
    assert rule(0.80, _comparison(nothing, (100, 0.86, 0.85, noise), (100, 0.86, 0.85, noise)))[0]
    promote, reason = rule(
        0.80, _comparison((5, 1.0, 0.6, noise), (100, 0.84, 0.85, noise), (105, 0.85, 0.86, noise))
    )
    assert not promote and "same 105 test rows" in reason


def test_compare_splits_the_test_rows_by_what_the_champion_saw() -> None:
    from project_tagger import evaluate

    class Always:
        def __init__(self, tag):
            self.tag = tag

        def predict(self, texts):
            return pd.Series([self.tag] * len(texts)).to_numpy()

    test_df = data.preprocess(LABELED)
    comparison = evaluate.compare(
        Always("natural-language-processing"), Always("computer-vision"), test_df, champion_ids=range(1, 61)
    )
    assert {name: part["rows"] for name, part in comparison.items()} == {"all": 120, "new": 60, "old": 60}
    # Ids 1-60 are the NLP rows: the old part, where only the new model is right.
    assert comparison["old"]["f1"] == 1.0 and comparison["old"]["champion_f1"] == 0.0
    assert comparison["new"]["f1"] == 0.0
    # Right on every old row against wrong on every one: no draw of rows changes that.
    assert comparison["old"]["difference"] == (1.0, 1.0)


def test_difference_interval_is_seeded_and_brackets_noise() -> None:
    from project_tagger import evaluate

    y = ["a", "b"] * 50
    one_off = list(y)
    one_off[0] = "b"  # one mistake in 100 rows
    interval = evaluate.difference_interval(y, one_off, y)
    assert interval == evaluate.difference_interval(y, one_off, y)  # same rows, same interval
    assert interval[0] < 0 <= interval[1]  # one row is within chance


@pytest.mark.slow
def test_training_beats_a_trivial_baseline() -> None:
    from project_tagger import train as train_module

    train_df, val_df, test_df = _splits()
    _, metrics = train_module.fit(pd.concat([train_df, val_df]), test_df)
    majority = test_df["tag"].value_counts(normalize=True).max()
    assert metrics["accuracy"] >= majority


@pytest.mark.slow
def test_tune_train_register_promote_rollback(local_tracking) -> None:
    """The mlops Dag's ML calls, in order, without Airflow."""
    from project_tagger import registry, train as train_module

    train_df, val_df, test_df = _splits()
    splits = {"train": train_df, "val": val_df, "test": test_df}

    study = train_module.create_study("test-study")
    assert train_module.create_study("test-study") == study  # a retry loads, not fails
    trials = train_module.run_trials(study, 2, train_df, val_df) + train_module.run_trials(
        study, 2, train_df, val_df
    )
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
        trained = train_module.train_model(best["params"], splits, run_name=f"final-{i}")
        assert registry.load_metrics(trained["run_id"])["f1"] == pytest.approx(trained["metrics"]["f1"])
        split_ids = registry.load_split_ids(trained["run_id"])
        assert split_ids == {name: df["id"].tolist() for name, df in splits.items()}
        versions.append(registry.register(trained["model_uri"]))

    assert registry.get_production() is None
    assert registry.rollback("test") is None  # nothing to go back to

    registry.promote(versions[0])
    champion = registry.promote(versions[1])
    assert champion["version"] == versions[1]
    assert champion["previous_version"] == versions[0]

    rolled_back = registry.rollback("f1 fell")
    assert rolled_back["version"] == versions[0]
    assert rolled_back["rolled_back_version"] == versions[1]
    assert registry.get_production()["version"] == versions[0]
    # The alias is gone, and the retired version says why it left.
    assert registry._version_by_alias(config.PREVIOUS_ALIAS) is None
    retired = registry._client().get_model_version(config.REGISTERED_MODEL, versions[1])
    assert retired.tags == {"rolled_back": "true", "rolled_back_reason": "f1 fell"}
    # A second rollback does not bring the retired version back.
    assert registry.rollback("again") is None
    assert registry.get_production()["version"] == versions[0]
    assert len(registry.load_model().predict(["object detection with yolo"])) == 1
    assert len(registry.load_model(version=versions[1]).predict(["object detection with yolo"])) == 1
