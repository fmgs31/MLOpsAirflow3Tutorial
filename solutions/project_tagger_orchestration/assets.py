"""The Assets the tutorial's Dags produce and consume.

An Asset is a named piece of data, and it is how the Dags in this tutorial are
wired together: the DataOps Dag *produces* `labeled_projects`, and the MLOps Dag
is scheduled on it (`schedule=[LABELED_PROJECTS]`) instead of guessing a cron
that runs "some time after" DataOps finishes.

An Asset is identified by a name and a URI. The tables get their real
BigQuery URI, `bigquery://<project>/<dataset>/<table>` - a scheme the Google
provider registers - so the Assets view in the UI says exactly which table
each one is.

This module is orchestration, so it lives with the Dags, not in
`src/project_tagger/`: the ML project imports no Airflow. It takes the table
names from `project_tagger.config`, so both sides agree on them.
"""

from __future__ import annotations

from airflow.sdk import Asset

from project_tagger import config

# The table dbt builds and the ML Dag trains on. Produced by `dataops`,
# consumed (as a schedule) by `mlops`.
LABELED_PROJECTS = Asset(
    name="labeled_projects",
    uri=config.bigquery_uri(config.LABELED_PROJECTS_TABLE),
    group="dataset",
    extra={"layer": "warehouse", "owner": "data-engineering"},
)

# The two raw tables Airbyte loads, produced by `dataops`.
RAW_PROJECTS = Asset(
    name="raw_projects",
    uri=config.bigquery_uri(config.PROJECTS_TABLE),
    group="dataset",
    extra={"layer": "raw", "owner": "data-engineering"},
)
RAW_TAGS = Asset(
    name="raw_tags",
    uri=config.bigquery_uri(config.TAGS_TABLE),
    group="dataset",
    extra={"layer": "raw", "owner": "data-engineering"},
)

# Produced whenever the registry's `champion` alias moves: by `mlops` when a
# model clears the promotion gate, and by `monitoring` when it rolls back.
# Nothing is scheduled on it yet - point a Dag's `schedule` at it and the edge
# appears in the UI's Assets graph.
PRODUCTION_MODEL = Asset(
    name="production_model",
    group="model",
    extra={"owner": "ml-engineering"},
)
