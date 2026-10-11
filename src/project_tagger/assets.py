"""The Assets shared by the Dags.

An Asset is a named piece of data, and it is how the Dags in this tutorial are
wired together: the DataOps Dag *produces* `labeled_projects`, and the MLOps Dag
is scheduled on it (`schedule=[LABELED_PROJECTS]`) instead of guessing a cron
that runs "some time after" DataOps finishes.

An Asset is identified by a name and a URI. Here the assets are name-only, which
keeps them identical whether you run in Docker or natively; in production give
them the real URI of the data (`s3://...`, `bq://...`) so lineage in the UI
points somewhere.
"""

from __future__ import annotations

from airflow.sdk import Asset

# Landing-zone copy of the source data, produced by `dataops`.
RAW_PROJECTS = Asset(
    name="raw_projects",
    group="dataset",
    extra={"layer": "raw", "owner": "data-engineering"},
)

# The curated table the ML Dag trains on. Produced by `dataops`,
# consumed (as a schedule) by `mlops`.
LABELED_PROJECTS = Asset(
    name="labeled_projects",
    group="dataset",
    extra={"layer": "warehouse", "owner": "data-engineering"},
)

# Produced by `mlops` when a model clears the promotion gate. Nothing is
# scheduled on it yet - point a Dag's `schedule` at it and the edge appears in
# the UI's Assets graph.
PRODUCTION_MODEL = Asset(
    name="production_model",
    group="model",
    extra={"owner": "ml-engineering"},
)
