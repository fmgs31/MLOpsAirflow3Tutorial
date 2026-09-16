"""The READMEs' full-file listings must match the files they claim to show.

The appendix in §16 of README.md and README_ES.md does not contain hand-copied
code; it contains markers that `scripts/sync_readme.py` fills in from the files
in `solutions/`. This test runs that script's `--check` logic on both files, so
a solution edited without re-syncing fails here instead of leaving a
plausible-looking lie in the tutorial.

    python scripts/sync_readme.py     # the fix when this test fails
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
README = REPO_ROOT / "README.md"
READMES = [README, REPO_ROOT / "README_ES.md"]
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
    # All are mounted into the container by docker-compose.yaml, but skip
    # rather than fail if this suite is run somewhere they are not.
    if not all(p.is_file() for p in READMES) or not SYNC_SCRIPT.is_file():
        pytest.skip("README.md / README_ES.md / scripts/sync_readme.py not available here")
    return _load_sync_module()


def test_listings_are_in_sync(sync) -> None:
    for readme in READMES:
        _, stale = sync.render(readme.read_text(encoding="utf-8"))
        assert not stale, (
            f"{readme.name} listings are stale for {stale}. Run: python scripts/sync_readme.py"
        )


def test_every_solution_is_listed(sync) -> None:
    """A new solution file should not be able to skip either appendix unnoticed."""
    solutions = {f"solutions/{p.name}" for p in (REPO_ROOT / "solutions").glob("*.py")}
    for readme in READMES:
        listed = {path for path, _ in sync.MARKER.findall(readme.read_text(encoding="utf-8"))}
        assert solutions <= listed, (
            f"Solutions missing from the {readme.name} appendix: {sorted(solutions - listed)}"
        )


def test_a_changed_file_is_detected(sync) -> None:
    """The check has to actually fail on drift - otherwise it proves nothing.

    The tamper has to happen *inside* a marker block: the same code appears in
    the teaching sections above, and edits there are not what this guards.
    """
    text = README.read_text(encoding="utf-8")
    block = next(
        m for m in sync.MARKER.finditer(text) if m["path"] == "solutions/01_hello_airflow3.py"
    )
    tampered = (
        text[: block.start("body")]
        + "```python\n# not what the file says\n```\n"
        + text[block.end("body") :]
    )

    _, stale = sync.render(tampered)
    assert stale == ["solutions/01_hello_airflow3.py"]
