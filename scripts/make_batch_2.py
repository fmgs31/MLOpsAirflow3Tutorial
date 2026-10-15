#!/usr/bin/env python3
"""Build datasets/batch_2/: the later deliveries §14 loads, problems included.

The new projects are real: Made With ML's `holdout.csv`, 191 projects that are
not in `datasets/projects.csv`. Two things are changed so they behave as a
batch that arrives after the original data, and the problems are added on top:

* every `created_on` moves forward one year (2020 -> 2021), so the batch is
  the newest data in the warehouse;
* day 1 has the new projects but not their tags;
* day 2 has the tags, and four old projects re-sent under new ids;
* day 3 drops the re-sent rows, and writes the new projects' dates as
  `MM/DD/YYYY HH:MM:SS`.

The tags are left as upstream wrote them. Upstream had already collapsed the
three rarest tags into `other`, which the original `tags.csv` never uses: that
change is real, not added here.

    python scripts/make_batch_2.py

Uses only the standard library. Re-running it rewrites the same files.
"""

from __future__ import annotations

import csv
import io
import urllib.request
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATASETS = ROOT / "datasets"
OUT = DATASETS / "batch_2"

# Pinned to the commit that last changed the file, so the batch never moves.
HOLDOUT_URL = (
    "https://raw.githubusercontent.com/GokuMohandas/Made-With-ML/"
    "030573f1a1dad734a3a7b1bef3a4e0addcd9d92b/datasets/holdout.csv"
)

# Old projects the day-2 export sends again, each under a new id.
RESENT_IDS = ["65", "847", "1668", "2268"]
FIRST_NEW_ID = 2438  # one above the largest id in projects.csv
RESENT_ON = "2021-10-06 09:00:00"

STANDARD = "%Y-%m-%d %H:%M:%S"
DAY3_FORMAT = "%m/%d/%Y %H:%M:%S"


def read_rows(text: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(text)))


def to_csv(header: list[str], rows: list[dict[str, str]]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=header, lineterminator="\n", extrasaction="ignore")
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue()


def write(name: str, original: str, header: list[str], new_rows: list[dict[str, str]]) -> None:
    # The original rows are kept byte for byte; the new rows are appended.
    (OUT / name).write_text(original + to_csv(header, new_rows), encoding="utf-8", newline="")
    print(f"{name}: {len(read_rows(original)) + len(new_rows)} rows")


def main() -> None:
    with urllib.request.urlopen(HOLDOUT_URL) as response:
        holdout = read_rows(response.read().decode("utf-8"))

    projects_text = (DATASETS / "projects.csv").read_bytes().decode("utf-8")
    tags_text = (DATASETS / "tags.csv").read_bytes().decode("utf-8")
    originals = {row["id"]: row for row in read_rows(projects_text)}

    new = []
    for row in holdout:
        created = datetime.strptime(row["created_on"], STANDARD)
        new.append({**row, "created_on": created.replace(year=created.year + 1).strftime(STANDARD)})

    resent = [
        {**originals[old_id], "id": str(FIRST_NEW_ID + i), "created_on": RESENT_ON}
        for i, old_id in enumerate(RESENT_IDS)
    ]
    day3 = [
        {**row, "created_on": datetime.strptime(row["created_on"], STANDARD).strftime(DAY3_FORMAT)}
        for row in new
    ]

    projects_header = ["id", "created_on", "title", "description"]
    OUT.mkdir(exist_ok=True)
    write("day1_projects.csv", projects_text, projects_header, new)
    write("day2_projects.csv", projects_text, projects_header, new + resent)
    write("day2_tags.csv", tags_text, ["id", "tag"], new)
    write("day3_projects.csv", projects_text, projects_header, day3)


if __name__ == "__main__":
    main()
