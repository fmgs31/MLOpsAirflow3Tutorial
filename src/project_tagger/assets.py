"""The Assets shared by the Dags.

An Asset is a named piece of data, and it is how the Dags in this tutorial are
wired together: the DataOps Dag *produces* `labeled_projects`, and the MLOps Dag
is scheduled on it (`schedule=[LABELED_PROJECTS]`) instead of guessing a cron
that runs "some time after" DataOps finishes.

An Asset is identified by a name and a URI. The tables get their real
BigQuery URI, `bigquery://<project>/<dataset>/<table>` - a scheme the Google
provider registers - so the Assets view in the UI says exactly which table
each one is.
"""

from __future__ import annotations

from airflow.sdk import Asset

from project_tagger import config


def _bigquery_uri(table: str) -> str:
    return f"bigquery://{config.GCP_PROJECT_ID}/{config.BQ_DATASET}/{table}"


# The two raw tables Airbyte loads, produced by `dataops`.
RAW_PROJECTS = Asset(
    name="raw_projects",
    uri=_bigquery_uri(config.PROJECTS_TABLE),
    group="dataset",
    extra={"layer": "raw", "owner": "data-engineering"},
)
RAW_TAGS = Asset(
    name="raw_tags",
    uri=_bigquery_uri(config.TAGS_TABLE),
    group="dataset",
    extra={"layer": "raw", "owner": "data-engineering"},
)

# The table dbt builds and the ML Dag trains on. Produced by `dataops`,
# consumed (as a schedule) by `mlops`.
LABELED_PROJECTS = Asset(
    name="labeled_projects",
    uri=_bigquery_uri(config.LABELED_PROJECTS_TABLE),
    group="dataset",
    extra={"layer": "warehouse", "owner": "data-engineering"},
)

# Produced by `mlops` when a model clears the promotion gate - that is, when
# the registry's `champion` alias moves. Nothing is scheduled on it yet - point
# a Dag's `schedule` at it and the edge appears in the UI's Assets graph.
PRODUCTION_MODEL = Asset(
    name="production_model",
    group="model",
    extra={"owner": "ml-engineering"},
)
