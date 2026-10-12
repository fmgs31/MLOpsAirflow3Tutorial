"""Reading the warehouse, and the preprocessing the model needs.

Extract-and-load is Airbyte's job and the join is dbt's (§9). What is left
for Python is reading the tables they produce, and the ML-specific
preprocessing - text cleaning and label collapsing - that belongs with the
model rather than in the warehouse.
"""

from __future__ import annotations

import re
from pathlib import Path

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
    return bigquery_client().query(query).to_dataframe()


def count_rows(table: str) -> int:
    query = f"SELECT COUNT(*) AS n FROM `{config.table_id(table)}`"
    return int(next(iter(bigquery_client().query(query).result()))["n"])


def load_projects() -> pd.DataFrame:
    return load_table(config.PROJECTS_TABLE, config.PROJECTS_COLUMNS)


def load_tags() -> pd.DataFrame:
    return load_table(config.TAGS_TABLE, config.TAGS_COLUMNS)


def load_labeled() -> pd.DataFrame:
    return load_table(config.LABELED_PROJECTS_TABLE, config.LABELED_PROJECTS_COLUMNS)


# --- Snapshots ------------------------------------------------------------------
# The warehouse table can change while a Dag run is in flight. Each mlops run
# copies it once to a file named after the run, and every task of that run
# reads the file: all of them see the same rows, and a retried task sees the
# rows the first attempt saw.


def snapshot_path(run_key: str) -> Path:
    return config.SNAPSHOTS_DIR / run_key / "labeled_projects.csv"


def save_snapshot(df: pd.DataFrame, run_key: str) -> Path:
    fp = snapshot_path(run_key)
    fp.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(fp, index=False)
    return fp


def load_snapshot(fp: str | Path) -> pd.DataFrame:
    return pd.read_csv(fp)


# --- Preprocessing ---------------------------------------------------------------


def clean_text(text: str) -> str:
    text = str(text).lower().strip()
    text = NON_ALNUM.sub(" ", text)
    text = STOPWORD_PATTERN.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def preprocess(df: pd.DataFrame, min_tag_freq: int = config.MIN_TAG_FREQ) -> pd.DataFrame:
    """`labeled_projects` -> model input: a cleaned `text` column and a learnable label space."""
    df = df.dropna(subset=["title", "tag"]).copy()
    df["description"] = df["description"].fillna("")
    df["text"] = (df["title"] + " " + df["description"]).apply(clean_text)

    # Collapse the long tail into `other` so every class has enough examples.
    counts = df["tag"].value_counts()
    rare = counts[counts < min_tag_freq].index
    df["tag"] = df["tag"].apply(lambda tag: config.OTHER_TAG if tag in rare else tag)

    df = df[df["text"].str.len() > 0]
    return df[["id", "created_on", "title", "description", "text", "tag"]].reset_index(drop=True)


def split(
    df: pd.DataFrame, test_size: float = 0.2, seed: int = 42
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Stratified train/test split without pulling in sklearn's splitter."""
    test_parts, train_parts = [], []
    for _, group in df.groupby("tag"):
        group = group.sample(frac=1.0, random_state=seed)
        n_test = max(1, int(len(group) * test_size))
        test_parts.append(group.iloc[:n_test])
        train_parts.append(group.iloc[n_test:])
    train = pd.concat(train_parts).sample(frac=1.0, random_state=seed).reset_index(drop=True)
    test = pd.concat(test_parts).sample(frac=1.0, random_state=seed).reset_index(drop=True)
    return train, test
