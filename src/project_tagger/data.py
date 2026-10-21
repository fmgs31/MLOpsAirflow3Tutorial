"""Reading the warehouse, and the preprocessing the model needs.

Extract-and-load is Airbyte's job and the join is dbt's (§9). What is left
for Python is reading the tables they produce, and the ML-specific
preprocessing - text cleaning and label collapsing - that belongs with the
model rather than in the warehouse.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import numpy as np
import pandas as pd

from project_tagger import config

STOPWORD_PATTERN = re.compile(r"\b(a|an|the|and|or|of|to|in|for|on|with|is|are|this|that)\b")
NON_ALNUM = re.compile(r"[^A-Za-z0-9 ]")


def bigquery_client():
    """A BigQuery client authenticated with the service-account key.

    `google.cloud.bigquery` finds the key through GOOGLE_APPLICATION_CREDENTIALS,
    which docker-compose.yaml points at secrets/gcp-key.json. Imported here, not
    at module level, so the rest of this module works without the library.
    """
    from google.cloud import bigquery

    return bigquery.Client(project=config.GCP_PROJECT_ID, location=config.BQ_LOCATION)


def load_table(table: str, columns: list[str]) -> pd.DataFrame:
    """Read the named columns of a warehouse table into a dataframe."""
    query = f"SELECT {', '.join(columns)} FROM `{config.table_id(table)}`"
    # Once a result is larger than one page, the client switches to the
    # BigQuery Storage API, which needs a role the service account does not
    # have (§9). These tables are small; the plain API reads them fine.
    return bigquery_client().query(query).to_dataframe(create_bqstorage_client=False)


def count_rows(table: str) -> int:
    query = f"SELECT COUNT(*) AS n FROM `{config.table_id(table)}`"
    return int(next(iter(bigquery_client().query(query).result()))["n"])


def load_projects() -> pd.DataFrame:
    return load_table(config.PROJECTS_TABLE, config.PROJECTS_COLUMNS)


def load_tags() -> pd.DataFrame:
    return load_table(config.TAGS_TABLE, config.TAGS_COLUMNS)


def load_labeled() -> pd.DataFrame:
    df = load_table(config.LABELED_PROJECTS_TABLE, config.LABELED_PROJECTS_COLUMNS)
    # Airbyte loads the CSV's `id` as a NUMERIC column, which arrives as Python
    # Decimals (`Decimal('6.000000000')`). A project id is an integer: convert
    # it once, here, so the split hash and the ids MLflow keeps see `6`.
    df["id"] = df["id"].astype("int64")
    return df


# --- Snapshots ------------------------------------------------------------------
# The warehouse table can change while a Dag run is in flight. Each mlops run
# copies it once to a folder named after the run, splits it there, and every
# task of that run reads those files: all of them see the same rows, and a
# retried task sees the rows the first attempt saw.
#
# Parquet, not CSV: a Parquet file stores each column's type, so every column
# comes back as it was written - `created_on` as a timestamp, not a string. A
# CSV is only text, and the types are guessed again on every read.

SPLITS = ("train", "val", "test")


def save_snapshot(df: pd.DataFrame, run_key: str) -> dict[str, str]:
    """Write the table as read, and its preprocessed splits, under `SNAPSHOTS_DIR/<run_key>/`.

    Returns the paths: `snapshot` (the table as read, for the table-level
    suite), and `train`, `val` and `test` (the model input, one file each).
    """
    folder = config.SNAPSHOTS_DIR / run_key
    folder.mkdir(parents=True, exist_ok=True)
    paths = {"snapshot": folder / "labeled_projects.parquet"}
    df.to_parquet(paths["snapshot"], index=False)

    processed = preprocess(df)
    parts = assign_split(processed["id"])
    for name in SPLITS:
        paths[name] = folder / f"{name}.parquet"
        processed[parts == name].reset_index(drop=True).to_parquet(paths[name], index=False)
    return {name: str(fp) for name, fp in paths.items()}


def load_snapshot(fp: str | Path) -> pd.DataFrame:
    return pd.read_parquet(fp)


# --- Preprocessing ---------------------------------------------------------------


def clean_text(text: str) -> str:
    text = str(text).lower().strip()
    text = NON_ALNUM.sub(" ", text)
    text = STOPWORD_PATTERN.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def preprocess(df: pd.DataFrame, collapse_tags: bool = True) -> pd.DataFrame:
    """`labeled_projects` -> model input: a cleaned `text` column and a learnable label space."""
    df = df.dropna(subset=["title", "tag"]).copy()
    df["description"] = df["description"].fillna("")
    df["text"] = (df["title"] + " " + df["description"]).apply(clean_text)

    # The model learns four classes: every tag outside them becomes `other`,
    # as in Made With ML's dataset.csv.
    if collapse_tags:
        df["tag"] = df["tag"].where(df["tag"].isin(config.ACCEPTED_TAGS), config.OTHER_TAG)

    df = df[df["text"].str.len() > 0]
    return df[["id", "created_on", "title", "description", "text", "tag"]].reset_index(drop=True)


def assign_split(ids: pd.Series) -> pd.Series:
    """'train', 'val' or 'test' for each id - the same for an id in every run.

    The part comes from a hash of the id, not from a shuffle: a project that
    was in the test split once is in it every time, so no model is ever
    trained on a row another model was tested on. `hashlib`, not Python's
    `hash()`, which gives a different number in every process.
    """
    bucket = ids.astype(str).map(lambda i: int(hashlib.sha256(i.encode()).hexdigest(), 16) % 100)
    parts = np.select(
        [bucket < config.TEST_PCT, bucket < config.TEST_PCT + config.VAL_PCT], ["test", "val"], "train"
    )
    return pd.Series(parts, index=ids.index)
