#!/usr/bin/env python3
"""Build datasets/batch_1/: the delivery §12 loads.

The new projects are real: 800 arXiv papers in computer science, submitted
between January 2025 and September 2026, in `datasets/arxiv.csv` (how they
were chosen and tagged is in datasets/README.md). Each becomes a project the
way Made With ML's projects are written:

* `id` from 3000 up, in the order of `arxiv.csv` (by submission date);
* `created_on` the date of the paper's first version, at midnight: arXiv
  gives the date, not the time;
* `title` the paper's title, and `description` the first sentence of its
  abstract: the first with four words or more, so a structured abstract's
  `Background.` is skipped.

Each file is a whole table, as a Full Refresh sync expects: the original rows
byte for byte, then the new ones.

    python scripts/make_batch_1.py

Uses only the standard library. Re-running it rewrites the same files.
"""

from __future__ import annotations

import csv
import io
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATASETS = ROOT / "datasets"
OUT = DATASETS / "batch_1"

FIRST_NEW_ID = 3000

# A sentence ends at . ! or ? followed by a space and a capital letter.
SENTENCE_END = re.compile(r"(?<=[.!?])\s+(?=[A-Z])")


def first_sentence(abstract: str) -> str:
    return next(s for s in SENTENCE_END.split(abstract) if len(s.split()) >= 4)


def to_csv(header: list[str], rows: list[dict[str, str]]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=header, lineterminator="\n")
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue()


def main() -> None:
    papers = list(csv.DictReader((DATASETS / "arxiv.csv").open(encoding="utf-8", newline="")))
    projects, tags = [], []
    for i, paper in enumerate(papers):
        project_id = str(FIRST_NEW_ID + i)
        projects.append({
            "id": project_id,
            "created_on": f"{paper['created']} 00:00:00",
            "title": paper["title"],
            "description": first_sentence(paper["abstract"]),
        })
        tags.append({"id": project_id, "tag": paper["tag"]})

    OUT.mkdir(exist_ok=True)
    for name, header, new in (
        ("projects.csv", ["id", "created_on", "title", "description"], projects),
        ("tags.csv", ["id", "tag"], tags),
    ):
        original = (DATASETS / name).read_bytes().decode("utf-8")
        (OUT / name).write_text(original + to_csv(header, new), encoding="utf-8", newline="")
        print(f"{name}: {len(new)} new rows")


if __name__ == "__main__":
    main()
