"""The code you are told to type must add up to a Dag that imports.

Each lesson in docs/split/ and docs/split_ES/ builds a Dag file in pieces - a fenced
block for the imports, one for the `@dag` header, one per group of tasks, one for
the wiring. A block that belongs to a file is fenced as

    ```python dags/03_dataops.py
    ...
    ```

and the pieces are meant to be typed into that file in the order they appear.
This test does exactly that: it reads each language's section files in order
(01-…, 02-…, …), concatenates every block tagged with the same path, writes the result to a temporary Dag folder, and loads
it with `DagBag`. A lesson whose pieces no longer form a working file fails
here, in the same way it would fail the reader.

Blocks with a bare ```python fence are illustrations and are left alone.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from airflow.dag_processing.dagbag import DagBag

REPO_ROOT = Path(__file__).resolve().parents[1]
TUTORIALS = [REPO_ROOT / "docs" / "split", REPO_ROOT / "docs" / "split_ES"]

BUILD_BLOCK = re.compile(r"^```python (?P<path>dags/[\w.]+\.py)\n(?P<body>.*?)^```", re.DOTALL | re.MULTILINE)

EXPECTED_FILES = {
    "dags/01_hello_airflow3.py": "hello_airflow3",
    "dags/02_operators_and_xcoms.py": "operators_and_xcoms",
    "dags/03_dataops.py": "dataops",
    "dags/04_mlops.py": "mlops",
    "dags/05_sensors_and_events.py": "sensors_and_events",
    "dags/06_monitoring_and_retraining.py": "monitoring",
    "dags/07_deadline_alerts.py": "deadline_alerts",
}


def sections(tutorial: Path) -> list[Path]:
    """The numbered section files, in reading order (the index README is skipped)."""
    return sorted(tutorial.glob("[0-9][0-9]-*.md"))


def assemble(tutorial: Path) -> dict[str, str]:
    """Path -> file contents, each the concatenation of its tagged blocks in order."""
    files: dict[str, list[str]] = {}
    for section in sections(tutorial):
        for match in BUILD_BLOCK.finditer(section.read_text(encoding="utf-8")):
            files.setdefault(match["path"], []).append(match["body"].rstrip("\n") + "\n")
    return {path: "\n".join(parts) for path, parts in files.items()}


@pytest.fixture(scope="module", params=TUTORIALS, ids=lambda p: p.name)
def assembled(request, tmp_path_factory) -> tuple[Path, dict[str, str]]:
    tutorial: Path = request.param
    if not sections(tutorial):
        pytest.skip(f"{tutorial.name} not available here")
    files = assemble(tutorial)
    folder = tmp_path_factory.mktemp(tutorial.name)
    for path, source in files.items():
        (folder / Path(path).name).write_text(source, encoding="utf-8")
    return folder, files


def test_every_lesson_builds_a_file(assembled) -> None:
    _, files = assembled
    missing = set(EXPECTED_FILES) - set(files)
    assert not missing, f"no build blocks found for {sorted(missing)}"


def test_assembled_files_import_without_errors(assembled) -> None:
    folder, _ = assembled
    dagbag = DagBag(dag_folder=str(folder))
    assert not dagbag.import_errors, f"assembled lesson files do not import: {dagbag.import_errors}"
    assert set(dagbag.dag_ids) == set(EXPECTED_FILES.values())


def test_each_file_defines_its_dag(assembled) -> None:
    folder, files = assembled
    for path, dag_id in EXPECTED_FILES.items():
        bag = DagBag(dag_folder=str(folder / Path(path).name))
        assert dag_id in bag.dag_ids, f"{path} assembled from the tutorial defines {sorted(bag.dag_ids)}, not {dag_id!r}"
