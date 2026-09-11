"""A tiny expectation-suite runner.

A real project would use Great Expectations, Pandera or Soda here. This
tutorial is about the *gate*, not the library, so it keeps the same shape - a
named suite of expectations, a pass/fail result per expectation, and a task
that fails the Dag run when a critical expectation breaks - with zero extra
dependencies.

Swap `run_suite` for a `GreatExpectationsOperator` (or Pandera / Soda / dbt
tests) and the Dag structure is unchanged. That substitutability is the point.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from tagifai import config


@dataclass
class Expectation:
    name: str
    check: Callable[[pd.DataFrame], bool]
    critical: bool = True
    details: str = ""


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


def expect_columns(columns: Sequence[str]) -> Expectation:
    return Expectation(
        name=f"columns_to_exist:{','.join(columns)}",
        check=lambda df: set(columns).issubset(df.columns),
    )


def expect_not_null(column: str) -> Expectation:
    return Expectation(
        name=f"not_null:{column}",
        check=lambda df: bool(df[column].notna().all()),
    )


def expect_unique(column: str) -> Expectation:
    return Expectation(
        name=f"unique:{column}",
        check=lambda df: bool(df[column].is_unique),
    )


def expect_min_rows(n: int) -> Expectation:
    return Expectation(name=f"min_rows:{n}", check=lambda df: len(df) >= n)


def expect_values_in_set(column: str, allowed: Sequence[str]) -> Expectation:
    return Expectation(
        name=f"values_in_set:{column}",
        check=lambda df: set(df[column].dropna().unique()).issubset(set(allowed)),
    )


# --- Suites -----------------------------------------------------------------

RAW_PROJECTS_SUITE: list[Expectation] = [
    expect_columns(["id", "created_on", "title", "description", "tag"]),
    expect_unique("id"),
    expect_not_null("title"),
    expect_min_rows(500),
]

LABELED_PROJECTS_SUITE: list[Expectation] = [
    expect_columns(["id", "text", "tag"]),
    expect_unique("id"),
    expect_not_null("text"),
    expect_not_null("tag"),
    expect_min_rows(500),
    # Non-critical: a new tag appearing is a signal to inspect, not to stop the line.
    Expectation(
        name="values_in_set:tag",
        check=lambda df: set(df["tag"].unique()).issubset(set(config.ACCEPTED_TAGS)),
        critical=False,
        details="An unexpected tag means the label space drifted - inspect before retraining.",
    ),
]

SUITES: dict[str, list[Expectation]] = {
    "raw_projects": RAW_PROJECTS_SUITE,
    "labeled_projects": LABELED_PROJECTS_SUITE,
}


def run_suite(df: pd.DataFrame, suite_name: str) -> SuiteResult:
    """Run every expectation, collecting results instead of raising on the first."""
    suite = SUITES[suite_name]
    result = SuiteResult(suite=suite_name, passed=True)
    for expectation in suite:
        try:
            passed = bool(expectation.check(df))
            error = ""
        except Exception as exc:  # a broken expectation is a failed expectation
            passed, error = False, f"{type(exc).__name__}: {exc}"
        result.results.append(
            {
                "expectation": expectation.name,
                "passed": passed,
                "critical": expectation.critical,
                "details": expectation.details,
                "error": error,
            }
        )
        if not passed and expectation.critical:
            result.passed = False
    return result


def check_split(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    min_rows_per_class: int = config.MIN_ROWS_PER_CLASS,
) -> SuiteResult:
    """Sanity-check a train/test split before anything is fitted on it.

    A table-level suite cannot see any of this. `labeled_projects` can pass every
    column expectation and still split into a train set missing a class, a test
    set with three rows of one, or two sets sharing ids. Each produces a model
    whose held-out metric means nothing - and the promotion gate downstream
    compares exactly that metric against production.
    """
    train_tags, test_tags = set(train_df["tag"]), set(test_df["tag"])
    scarce = {
        str(tag): int(n)
        for tag, n in train_df["tag"].value_counts().items()
        if n < min_rows_per_class
    }
    total = len(train_df) + len(test_df)
    test_fraction = len(test_df) / total if total else 0.0

    checks: list[tuple[str, bool, bool, str]] = [
        ("split_is_non_empty", bool(len(train_df) and len(test_df)), True, ""),
        (
            "classes_in_both_splits",
            train_tags == test_tags,
            True,
            f"train-only={sorted(train_tags - test_tags)} "
            f"test-only={sorted(test_tags - train_tags)}",
        ),
        (
            f"min_rows_per_class:{min_rows_per_class}",
            not scarce,
            True,
            f"under-represented in train: {scarce}" if scarce else "",
        ),
        (
            "no_id_leakage",
            set(train_df["id"]).isdisjoint(set(test_df["id"])),
            True,
            "the same project in both splits inflates the held-out metric",
        ),
        (
            "test_fraction_between_0.1_and_0.4",
            0.1 <= test_fraction <= 0.4,
            # Non-critical: an odd ratio is worth seeing in the logs, but it is a
            # judgement call, not a broken snapshot.
            False,
            f"test_fraction={test_fraction:.3f}",
        ),
    ]

    result = SuiteResult(suite="training_split", passed=True)
    for name, passed, critical, details in checks:
        result.results.append(
            {
                "expectation": name,
                "passed": bool(passed),
                "critical": critical,
                "details": details,
                "error": "",
            }
        )
        if not passed and critical:
            result.passed = False
    return result
