"""The README's full-file listings must match the files they claim to show.

The appendix in README.md §16 does not contain hand-copied code; it contains
markers that `scripts/sync_readme.py` fills in from the files on disk. This test
runs that script's `--check` logic, so a Dag edited without re-syncing fails
here instead of leaving a plausible-looking lie in the tutorial.

    python scripts/sync_readme.py     # the fix when this test fails
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
README = REPO_ROOT / "README.md"
SYNC_SCRIPT = REPO_ROOT / "scripts" / "sync_readme.py"


def _load_sync_module():
    """Import scripts/sync_readme.py by path — `scripts/` is not a package."""
    spec = importlib.util.spec_from_file_location("sync_readme", SYNC_SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def sync():
    # Both are mounted into the container by docker-compose.yaml, but skip
    # rather than fail if this suite is run somewhere they are not.
    if not README.is_file() or not SYNC_SCRIPT.is_file():
        pytest.skip("README.md / scripts/sync_readme.py not available here")
    return _load_sync_module()


def test_listings_are_in_sync(sync) -> None:
    _, stale = sync.render(README.read_text(encoding="utf-8"))
    assert not stale, (
        f"README listings are stale for {stale}. Run: python scripts/sync_readme.py"
    )


def test_every_dag_is_listed(sync) -> None:
    """A new Dag file should not be able to skip the appendix unnoticed."""
    listed = {path for path, _ in sync.MARKER.findall(README.read_text(encoding="utf-8"))}
    dags = {f"dags/{p.name}" for p in (REPO_ROOT / "dags").glob("*.py")}
    assert dags <= listed, f"Dags missing from the README appendix: {sorted(dags - listed)}"


def test_a_changed_file_is_detected(sync) -> None:
    """The check has to actually fail on drift - otherwise it proves nothing.

    The tamper has to happen *inside* a marker block: the same code appears in
    the teaching sections above, and edits there are not what this guards.
    """
    text = README.read_text(encoding="utf-8")
    block = next(
        m for m in sync.MARKER.finditer(text) if m["path"] == "dags/01_hello_airflow3.py"
    )
    tampered = (
        text[: block.start("body")]
        + "```python\n# not what the file says\n```\n"
        + text[block.end("body") :]
    )

    _, stale = sync.render(tampered)
    assert stale == ["dags/01_hello_airflow3.py"]
