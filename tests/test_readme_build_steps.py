"""The code you are told to type must add up to a Dag that imports.

Each lesson in docs/split/ and docs/split_ES/ builds a Dag file in pieces - a fenced
block for the imports, one for the `@dag` header, one per group of tasks, one for
the wiring. A block that belongs to a file is fenced as

    ```python dags/03_dataops.py
    ...
    ```

and the pieces are meant to be typed into that file in the order they appear.
The same goes for the files in the Dag folder that are not Dags: the
`project_tagger_orchestration` package of §8 and its `.airflowignore`. This
test does exactly that: it reads each language's section files in order (01-…, 02-…, …),
concatenates every block tagged with the same path, writes the result to a
temporary Dag folder, and loads it with `DagBag`. A lesson whose pieces no longer form a working file fails
here, in the same way it would fail the reader.

Blocks with a bare ```python fence are illustrations and are left alone.
"""

from __future__ import annotations

import re
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from airflow.dag_processing.dagbag import DagBag

REPO_ROOT = Path(__file__).resolve().parents[1]
TUTORIALS = [REPO_ROOT / "docs" / "split", REPO_ROOT / "docs" / "split_ES"]

BUILD_BLOCK = re.compile(r"^```\w+ (?P<path>dags/[\w./]+)\n(?P<body>.*?)^```", re.DOTALL | re.MULTILINE)

EXPECTED_FILES = {
    "dags/01_hello_airflow3.py": "hello_airflow3",
    "dags/02_operators_and_xcoms.py": "operators_and_xcoms",
    "dags/03_dataops.py": "dataops",
    "dags/04_mlops.py": "mlops",
    "dags/05_sensors_and_events.py": "sensors_and_events",
    "dags/06_monitoring_and_retraining.py": "monitoring",
    "dags/07_deadline_alerts.py": "deadline_alerts",
}

# Not Dags: the package the Dags share (§8), and the file that keeps the Dag
# processor out of it.
SHARED_FILES = {
    "dags/.airflowignore",
    "dags/project_tagger_orchestration/__init__.py",
    "dags/project_tagger_orchestration/assets.py",
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
def assembled(request, tmp_path_factory) -> Iterator[tuple[Path, dict[str, str]]]:
    tutorial: Path = request.param
    if not sections(tutorial):
        pytest.skip(f"{tutorial.name} not available here")
    files = assemble(tutorial)
    folder = tmp_path_factory.mktemp(tutorial.name)
    for path, source in files.items():
        target = folder / path.removeprefix("dags/")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(source, encoding="utf-8")
    # Airflow puts the Dag folder on sys.path when it parses, which is how the
    # Dags import `project_tagger_orchestration`. Do the same, and forget the
    # package the other language imported, so each language is checked with
    # its own.
    _forget_shared_package()
    sys.path.insert(0, str(folder))
    yield folder, files
    sys.path.remove(str(folder))
    _forget_shared_package()


def _forget_shared_package() -> None:
    shared = [m for m in sys.modules if m.split(".")[0] == "project_tagger_orchestration"]
    for name in shared:
        del sys.modules[name]


def test_every_lesson_builds_a_file(assembled) -> None:
    _, files = assembled
    missing = (set(EXPECTED_FILES) | SHARED_FILES) - set(files)
    assert not missing, f"no build blocks found for {sorted(missing)}"


def test_assembled_files_import_without_errors(assembled) -> None:
    folder, _ = assembled
    dagbag = DagBag(dag_folder=str(folder))
    assert not dagbag.import_errors, f"assembled lesson files do not import: {dagbag.import_errors}"
    assert set(dagbag.dag_ids) == set(EXPECTED_FILES.values())


DBT_BLOCK = re.compile(
    r"^```\w+ (?P<path>dbt/dbt_transforms/[\w./-]+)\n(?P<body>.*?)^```", re.DOTALL | re.MULTILINE
)
DBT_SOLUTION = REPO_ROOT / "solutions" / "dbt_transforms"


@pytest.mark.parametrize("tutorial", TUTORIALS, ids=lambda p: p.name)
def test_dbt_files_typed_in_the_tutorial_match_the_solution(tutorial: Path) -> None:
    """§9 has the reader create the dbt project's files, one fenced block each.

    A block fenced as ```sql dbt/dbt_transforms/models/... is a whole file;
    the files must be exactly the finished ones in solutions/dbt_transforms/,
    which the dataops Dag's `transform` task was tested against.
    """
    if not sections(tutorial) or not DBT_SOLUTION.is_dir():
        pytest.skip(f"{tutorial.name} or solutions/dbt_transforms not available here")
    files: dict[str, str] = {}
    for section in sections(tutorial):
        for match in DBT_BLOCK.finditer(section.read_text(encoding="utf-8")):
            files[match["path"]] = files.get(match["path"], "") + match["body"]
    assert files, "no dbt file blocks found"
    for path, body in files.items():
        solution = DBT_SOLUTION / path.removeprefix("dbt/dbt_transforms/")
        assert solution.is_file(), f"{path} has no counterpart in solutions/dbt_transforms/"
        assert body == solution.read_text(encoding="utf-8"), f"{path} differs from {solution}"


def test_each_file_defines_its_dag(assembled) -> None:
    folder, files = assembled
    for path, dag_id in EXPECTED_FILES.items():
        bag = DagBag(dag_folder=str(folder / Path(path).name))
        assert dag_id in bag.dag_ids, f"{path} assembled from the tutorial defines {sorted(bag.dag_ids)}, not {dag_id!r}"
