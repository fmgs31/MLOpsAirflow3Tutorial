"""The code you are told to type must add up to a Dag that imports.

Each lesson in README.md and README_ES.md builds a Dag file in pieces - a fenced
block for the imports, one for the `@dag` header, one per group of tasks, one for
the wiring. A block that belongs to a file is fenced as

    ```python dags/03_dataops.py
    ...
    ```

and the pieces are meant to be typed into that file in the order they appear.
This test does exactly that: it concatenates every block tagged with the same
path, in document order, writes the result to a temporary Dag folder, and loads
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
READMES = [REPO_ROOT / "README.md", REPO_ROOT / "README_ES.md"]

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


def assemble(readme: Path) -> dict[str, str]:
    """Path -> file contents, each the concatenation of its tagged blocks in order."""
    files: dict[str, list[str]] = {}
    for match in BUILD_BLOCK.finditer(readme.read_text(encoding="utf-8")):
        files.setdefault(match["path"], []).append(match["body"].rstrip("\n") + "\n")
    return {path: "\n".join(parts) for path, parts in files.items()}


@pytest.fixture(scope="module", params=READMES, ids=lambda p: p.name)
def assembled(request, tmp_path_factory) -> tuple[Path, dict[str, str]]:
    readme: Path = request.param
    if not readme.is_file():
        pytest.skip(f"{readme.name} not available here")
    files = assemble(readme)
    folder = tmp_path_factory.mktemp(readme.stem)
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
        assert dag_id in bag.dag_ids, f"{path} assembled from the README defines {sorted(bag.dag_ids)}, not {dag_id!r}"
