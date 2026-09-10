"""Dag integrity tests - the cheapest CI you can put on an Airflow repo.

They catch the failures that otherwise show up as a red "Import Error" banner in
the UI hours later: a typo, a missing dependency, a cycle, a Dag that lost its
owner. Run them in the same image your scheduler uses:

    docker compose run --rm airflow-cli pytest /opt/airflow/tests -q
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

# Airflow 3 moved DagBag out of `airflow.models` (importing it from there still
# works but warns) and dropped the `include_examples` argument.
from airflow.dag_processing.dagbag import DagBag

DAGS_FOLDER = Path(__file__).resolve().parents[1] / "dags"

# Constructs that were removed or renamed in Airflow 3. A hit here means the Dag
# would break on upgrade even if it happens to import today.
AIRFLOW_2_PATTERNS = {
    r"\bschedule_interval\s*=": "use `schedule=`",
    r"from airflow\.operators\.": "use `airflow.providers.standard.operators.*`",
    r"from airflow\.sensors\.": "use `airflow.providers.standard.sensors.*`",
    r"from airflow\.decorators import": "use `airflow.sdk`",
    r"from airflow\.utils\.dates import": "`days_ago` was removed; use a fixed start_date",
    r"\bdays_ago\s*\(": "removed; use pendulum.datetime(...)",
    r"\bexecution_date\b": "removed from context; use `logical_date`",
    r"\bSubDagOperator\b": "removed; use TaskGroups",
    r"\bsla\s*=": "SLAs were removed; use DeadlineAlert",
}


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_FOLDER))


def test_no_import_errors(dagbag: DagBag) -> None:
    assert not dagbag.import_errors, f"Dag import errors: {dagbag.import_errors}"


def test_dags_were_found(dagbag: DagBag) -> None:
    assert len(dagbag.dags) >= 7, f"only found {sorted(dagbag.dag_ids)}"


@pytest.mark.parametrize("dag_file", sorted(DAGS_FOLDER.glob("*.py")), ids=lambda p: p.name)
def test_no_airflow_2_constructs(dag_file: Path) -> None:
    source = dag_file.read_text(encoding="utf-8")
    # Strip comments so the migration notes in the docstrings do not trip this.
    code = "\n".join(
        line for line in source.splitlines() if not line.lstrip().startswith("#")
    )
    code = re.sub(r'""".*?"""', "", code, flags=re.DOTALL)

    offenders = [
        f"{pattern} ({advice})"
        for pattern, advice in AIRFLOW_2_PATTERNS.items()
        if re.search(pattern, code)
    ]
    assert not offenders, f"{dag_file.name} uses Airflow 2 constructs: {offenders}"


def test_every_dag_has_owner_tags_and_docs(dagbag: DagBag) -> None:
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"{dag_id} has no tags"
        assert dag.doc_md, f"{dag_id} has no doc_md"
        owner = dag.default_args.get("owner", dag.owner)
        assert owner and owner != "airflow", f"{dag_id} has no explicit owner"


def test_no_cycles(dagbag: DagBag) -> None:
    from airflow.utils.dag_cycle_tester import check_cycle

    for dag in dagbag.dags.values():
        check_cycle(dag)


def test_mlops_is_scheduled_on_the_labeled_projects_asset(dagbag: DagBag) -> None:
    """The wiring this whole tutorial is built on."""
    mlops = dagbag.dags["mlops"]
    asset_names = {asset.name for asset in mlops.timetable.asset_condition.objects}
    assert "labeled_projects" in asset_names


def test_dataops_produces_the_labeled_projects_asset(dagbag: DagBag) -> None:
    dataops = dagbag.dags["dataops"]
    produced = {
        outlet.name
        for task in dataops.tasks
        for outlet in getattr(task, "outlets", [])
        if hasattr(outlet, "name")
    }
    assert {"raw_projects", "labeled_projects"} <= produced


def test_mlops_validates_before_it_trains(dagbag: DagBag) -> None:
    """No model is fitted on a snapshot nobody checked."""
    mlops = dagbag.dags["mlops"]
    trainer = mlops.get_task("train_candidate")
    assert "validate_training_data" in trainer.upstream_task_ids


def test_retries_are_configured(dagbag: DagBag) -> None:
    """A task with no retry policy is a pager at 3am."""
    for dag_id, dag in dagbag.dags.items():
        if dag_id in {"sensors_and_events", "deadline_alerts", "hello_airflow3"}:
            continue  # demo Dags, intentionally retry-free
        assert dag.default_args.get("retries", 0) >= 1, f"{dag_id} has no retries"
