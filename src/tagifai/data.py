"""Extract / transform steps for the DataOps Dag."""

from __future__ import annotations

import re
import urllib.request
from pathlib import Path

import pandas as pd

from tagifai import config

STOPWORD_PATTERN = re.compile(r"\b(a|an|the|and|or|of|to|in|for|on|with|is|are|this|that)\b")
NON_ALNUM = re.compile(r"[^A-Za-z0-9 ]")


def extract(url: str = config.PROJECTS_URL, dest: Path = config.RAW_PROJECTS_FP) -> Path:
    """Pull the source data into the landing zone.

    Stands in for the Airbyte / Fivetran EL step of a real stack: the point is
    that the Dag *triggers* the extraction, it does not implement it.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=60) as response:
        dest.write_bytes(response.read())
    return dest


def load_raw(fp: Path = config.RAW_PROJECTS_FP) -> pd.DataFrame:
    return pd.read_csv(fp)


def clean_text(text: str) -> str:
    text = str(text).lower().strip()
    text = NON_ALNUM.sub(" ", text)
    text = STOPWORD_PATTERN.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def transform(df: pd.DataFrame, min_tag_freq: int = config.MIN_TAG_FREQ) -> pd.DataFrame:
    """Raw projects -> the curated `labeled_projects` table.

    This is the dbt model of the original tutorial, expressed in pandas so the
    tutorial runs with no warehouse attached.
    """
    df = df.dropna(subset=["title", "tag"]).copy()
    df["description"] = df["description"].fillna("")
    df["text"] = (df["title"] + " " + df["description"]).apply(clean_text)

    # Collapse the long tail into `other` so the label space is learnable.
    counts = df["tag"].value_counts()
    rare = counts[counts < min_tag_freq].index
    df["tag"] = df["tag"].apply(lambda tag: config.OTHER_TAG if tag in rare else tag)

    df = df[df["text"].str.len() > 0]
    return df[["id", "created_on", "title", "description", "text", "tag"]].reset_index(drop=True)


def save_labeled(df: pd.DataFrame, fp: Path = config.LABELED_PROJECTS_FP) -> Path:
    fp.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(fp, index=False)
    return fp


def load_labeled(fp: Path = config.LABELED_PROJECTS_FP) -> pd.DataFrame:
    return pd.read_csv(fp)


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
