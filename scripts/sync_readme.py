#!/usr/bin/env python3
"""Keep the full-file listings in README.md and README_ES.md byte-identical to the files.

A tutorial that quotes its own code is only useful while the quotes are true, and
hand-maintained listings rot the first time a Dag is edited. So the READMEs do
not contain those listings in the ordinary sense: they contain markers, and this
script fills them in from the files on disk.

    <!-- BEGIN FILE: dags/01_hello_airflow3.py -->
    ```python
    ...the entire file, injected...
    ```
    <!-- END FILE: dags/01_hello_airflow3.py -->

Two modes:

    python scripts/sync_readme.py            # inject current file contents
    python scripts/sync_readme.py --check    # exit 1 if any listing is stale

`--check` is what `tests/test_readme_listings.py` runs, so a Dag edited without
re-syncing fails the test suite instead of shipping a lie. Add a listing by
pasting an empty marker pair into both READMEs and running the script.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
READMES = [ROOT / "README.md", ROOT / "README_ES.md"]

MARKER = re.compile(
    r"<!-- BEGIN FILE: (?P<path>[^\s>]+) -->\n(?P<body>.*?)<!-- END FILE: (?P=path) -->",
    re.DOTALL,
)

LANGUAGES = {".py": "python", ".yaml": "yaml", ".yml": "yaml", ".sh": "bash", ".cfg": "ini"}


def listing_for(rel_path: str) -> str:
    """The fenced block a marker should contain, trailing newline included."""
    target = ROOT / rel_path
    if not target.is_file():
        raise SystemExit(f"README references a file that does not exist: {rel_path}")
    source = target.read_text(encoding="utf-8").rstrip("\n")
    if "```" in source:
        # A fence inside the file would close the block early. No file in this
        # repo does it, but a future one might, and silently breaking the
        # README's markdown is worse than stopping here.
        raise SystemExit(f"{rel_path} contains a ``` fence and cannot be inlined")
    return f"```{LANGUAGES.get(target.suffix, '')}\n{source}\n```\n"


def render(text: str) -> tuple[str, list[str]]:
    """Return the README with every marker filled in, plus the stale paths."""
    stale: list[str] = []

    def replace(match: re.Match[str]) -> str:
        rel_path, body = match["path"], match["body"]
        wanted = listing_for(rel_path)
        if body != wanted:
            stale.append(rel_path)
        return f"<!-- BEGIN FILE: {rel_path} -->\n{wanted}<!-- END FILE: {rel_path} -->"

    return MARKER.sub(replace, text), stale


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="do not write; exit 1 if any listing differs from its file",
    )
    args = parser.parse_args()

    exit_code = 0
    for readme in READMES:
        original = readme.read_text(encoding="utf-8")
        updated, stale = render(original)
        count = len(MARKER.findall(original))

        if not count:
            print(f"No <!-- BEGIN FILE: ... --> markers found in {readme.name}")
            continue

        if args.check:
            if stale:
                print(f"{readme.name} is out of date for {len(stale)} of {count} listings:")
                for rel_path in stale:
                    print(f"  - {rel_path}")
                print("\nRun: python scripts/sync_readme.py")
                exit_code = 1
            else:
                print(f"{readme.name} listings are in sync ({count} files)")
            continue

        if stale:
            readme.write_text(updated, encoding="utf-8")
            print(f"Updated {len(stale)} of {count} listings in {readme.name}:")
            for rel_path in stale:
                print(f"  - {rel_path}")
        else:
            print(f"{readme.name} listings already in sync ({count} files)")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
