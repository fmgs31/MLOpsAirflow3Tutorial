"""Data-quality gates, written as Great Expectations suites.

An *expectation* is one assertion about the data ("`id` is unique"); a *suite*
is a named list of them; *validating* a batch of data against a suite returns
a pass/fail result per expectation. The Dags run a suite and fail the task
when a critical expectation breaks.

GX's own Airflow operators (`airflow-provider-great-expectations`) are not
used: the 1.x provider pins an older `structlog` than Airflow 3.3.1 ships
with, so it cannot be installed next to it. Calling GX Core from a `@task`
does the same job with no extra dependency.

Each expectation carries `meta={"critical": ...}`. A non-critical failure is
reported but does not fail the gate - a new tag appearing is a signal to
inspect, not a reason to stop the line.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from project_tagger import config


@dataclass
class SuiteResult:
    suite: str
    passed: bool
    results: list[dict[str, Any]] = field(default_factory=list)

    @property
    def failures(self) -> list[dict[str, Any]]:
        return [r for r in self.results if not r["passed"]]

    def summary(self) -> str:
        ok = sum(r["passed"] for r in self.results)
        return f"suite={self.suite} passed={ok}/{len(self.results)}"


# --- Suites -----------------------------------------------------------------


def build_suite(name: str):
    """The expectation suite for one warehouse table."""
    import great_expectations as gx

    e = gx.expectations
    warn = {"critical": False}
    suites = {
        "projects": [
            e.ExpectTableColumnsToMatchSet(column_set=config.PROJECTS_COLUMNS),
            e.ExpectColumnValuesToBeUnique(column="id"),
            e.ExpectColumnValuesToNotBeNull(column="id"),
            e.ExpectColumnValuesToNotBeNull(column="title"),
            # The same project submitted twice under two ids.
            e.ExpectCompoundColumnsToBeUnique(column_list=["title", "description"]),
            e.ExpectTableRowCountToBeBetween(min_value=500),
        ],
        "tags": [
            e.ExpectTableColumnsToMatchSet(column_set=config.TAGS_COLUMNS),
            e.ExpectColumnValuesToBeUnique(column="id"),
            e.ExpectColumnValuesToNotBeNull(column="id"),
            e.ExpectColumnValuesToNotBeNull(column="tag"),
            e.ExpectColumnValuesToBeInSet(column="tag", value_set=config.KNOWN_RAW_TAGS, meta=warn),
        ],
        "labeled_projects": [
            e.ExpectTableColumnsToMatchSet(column_set=config.LABELED_PROJECTS_COLUMNS),
            e.ExpectColumnValuesToBeUnique(column="id"),
            e.ExpectColumnValuesToNotBeNull(column="title"),
            e.ExpectColumnValuesToNotBeNull(column="tag"),
            e.ExpectTableRowCountToBeBetween(min_value=500),
            e.ExpectColumnValuesToBeInSet(column="tag", value_set=config.KNOWN_RAW_TAGS, meta=warn),
        ],
    }
    return gx.ExpectationSuite(name=name, expectations=suites[name])


def _ephemeral_context():
    """A GX context that lives in memory for one validation and writes nothing."""
    import great_expectations as gx
    from great_expectations.data_context.types.base import ProgressBarsConfig

    context = gx.get_context(mode="ephemeral")
    # Progress bars are for a terminal; in a task log they are noise.
    context.variables.progress_bars = ProgressBarsConfig(globally=False)
    return context


def run_suite(df: pd.DataFrame, suite_name: str) -> SuiteResult:
    """Validate a dataframe against a suite, collecting every result."""
    context = _ephemeral_context()
    batch = (
        context.data_sources.add_pandas("pandas")
        .add_dataframe_asset(name=suite_name)
        .add_batch_definition_whole_dataframe("whole_table")
        .get_batch(batch_parameters={"dataframe": df})
    )
    validation = batch.validate(build_suite(suite_name))

    result = SuiteResult(suite=suite_name, passed=True)
    for r in validation.results:
        cfg = r.expectation_config
        kwargs = dict(cfg.kwargs)
        target = kwargs.get("column") or ",".join(kwargs.get("column_list", [])) or "table"
        critical = (cfg.meta or {}).get("critical", True)
        passed = bool(r.success)
        result.results.append(
            {
                "expectation": f"{cfg.type}:{target}",
                "passed": passed,
                "critical": critical,
                # A few failing values, so the log says *what* broke.
                "details": str((r.result or {}).get("partial_unexpected_list", ""))[:200],
            }
        )
        if not passed and critical:
            result.passed = False
    return result


def check_split(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    test_df: pd.DataFrame,
    min_rows_per_class: int = config.MIN_ROWS_PER_CLASS,
) -> SuiteResult:
    """Sanity-check a train/validation/test split before anything is fitted on it.

    A table-level suite cannot see any of this. `labeled_projects` can pass every
    column expectation and still split into a training set missing a class, a
    test set with three rows of one, or two sets sharing ids. Each produces a
    model whose held-out metric means nothing - and the promotion gate
    downstream compares exactly that metric against production.

    These checks span several dataframes, which a GX suite (one batch at a
    time) does not express, so they are plain Python returning the same
    result shape.
    """
    splits = {"train": train_df, "val": val_df, "test": test_df}
    tags = {name: set(df["tag"]) for name, df in splits.items()}
    all_tags = set().union(*tags.values())
    missing = {name: sorted(all_tags - found) for name, found in tags.items() if all_tags - found}
    # The final model learns from train + val, so that is where a class needs rows.
    learned = pd.concat([train_df, val_df])
    scarce = {
        str(tag): int(n)
        for tag, n in learned["tag"].value_counts().items()
        if n < min_rows_per_class
    }
    shared = [
        f"{a}/{b}"
        for a, b in (("train", "val"), ("train", "test"), ("val", "test"))
        if not set(splits[a]["id"]).isdisjoint(set(splits[b]["id"]))
    ]
    total = sum(len(df) for df in splits.values())
    fraction = {name: len(df) / total if total else 0.0 for name, df in splits.items()}

    checks: list[tuple[str, bool, bool, str]] = [
        ("split_is_non_empty", all(len(df) for df in splits.values()), True, ""),
        ("classes_in_every_split", not missing, True, f"missing: {missing}" if missing else ""),
        (
            f"min_rows_per_class:{min_rows_per_class}",
            not scarce,
            True,
            f"under-represented in train + val: {scarce}" if scarce else "",
        ),
        (
            "no_id_leakage",
            not shared,
            True,
            f"ids shared by {shared}: the same project on both sides inflates the held-out metric"
            if shared
            else "",
        ),
        (
            "fractions_near_target",
            abs(fraction["test"] - config.TEST_PCT / 100) <= 0.05
            and abs(fraction["val"] - config.VAL_PCT / 100) <= 0.05,
            # Non-critical: an odd ratio is worth seeing in the logs, but it is a
            # judgement call, not a broken snapshot.
            False,
            f"val={fraction['val']:.3f} test={fraction['test']:.3f}",
        ),
    ]

    result = SuiteResult(suite="training_split", passed=True)
    for name, passed, critical, details in checks:
        result.results.append(
            {"expectation": name, "passed": bool(passed), "critical": critical, "details": details}
        )
        if not passed and critical:
            result.passed = False
    return result
