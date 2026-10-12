# MLOps orchestration with Apache Airflow 3

A hands-on MLOps tutorial: a machine-learning system built from the tools a
real team uses, and orchestrated with **Airflow 3.3.1**. Airbyte loads the
data into BigQuery, Great Expectations checks it, dbt transforms it, Optuna
tunes the model, MLflow tracks it and decides what is in production, and
alibi-detect watches for drift. You write the seven Dags that run all of it,
one file at a time: the data pipeline publishes an **Asset**, the Asset
triggers training, the best model is promoted, and a monitoring Dag decides
whether to retrain.

It follows Made With ML's [orchestration lesson](https://madewithml.com/courses/mlops/orchestration/),
rebuilt for Airflow 3 and made to run, step by step, on your machine.
Airflow, MLflow and Airbyte run locally in Docker; the warehouse is BigQuery,
on a Google Cloud project you create in §9. Commands are checked against a
real stack — see [What was actually verified](../verified.md) for which,
and when.

**Assumed:** Python, the command line, Docker installed, and a Google account
for Google Cloud's Free Trial (it asks for a card, and charges nothing unless
you upgrade; §9 explains). No Airflow experience — this teaches Airflow 3 as it
is today, and nothing about older versions.

**Time:** ~30 minutes to your first Dag run. About an hour to set up the data
stack in §9. A day or two for all seven Dags if you type them yourself, which
is the point.

## Based on Made With ML

This tutorial is based on Made With ML's
[orchestration lesson](https://madewithml.com/courses/mlops/orchestration/),
by Goku Mohandas: the same project (tagging ML projects from their title and
description), the same data, and the same pipeline of DataOps, MLOps and
continual learning.

I wanted to learn using the latest Airflow version (v3), and I wanted a hands-on
tutorial rather than one to read, so I asked Claude to port the Made With ML
lesson to Airflow 3. But it turns out it was not at all that straightforward:
getting it right took extensive corrections, polishing and a fair amount of work
(it's still a work in progress). Now it is open to anyone who wants to learn.

---

## Contents

1. [What you're building](01-what-you-re-building.md)
2. [Get it running](02-get-it-running.md)
3. [The pieces of Airflow](03-the-pieces-of-airflow.md)
4. [Your first Dag](04-your-first-dag.md)
5. [The UI](05-the-ui.md)
6. [Operators, XComs and relationships](06-operators-xcoms-and-relationships.md)
7. [Scheduling and logical dates](07-scheduling-and-logical-dates.md)
8. [Assets: scheduling on data, not on time](08-assets-scheduling-on-data-not-on-time.md)
9. [The DataOps Dag](09-the-dataops-dag.md)
10. [The MLOps Dag](10-the-mlops-dag.md)
11. [Waiting: sensors, deferrable tasks, events](11-waiting-sensors-deferrable-tasks-events.md)
12. [Continual learning: monitor, decide, act](12-continual-learning-monitor-decide-act.md)
13. [Deadline alerts](13-deadline-alerts.md)
14. [Testing Dags](14-testing-dags.md)
15. [Taking it to production](15-taking-it-to-production.md)
16. [Appendix and provenance](16-appendix-and-provenance.md)

Reference material: [CLI cheatsheet](../cheatsheet.md) ·
[Troubleshooting](../troubleshooting.md) ·
[The finished Dags in full](../appendix-dags.md) ·
[What was actually verified](../verified.md)

---

## Repository layout

```
dags/                       your workspace - empty until you write the seven Dags
dbt/                        your dbt workspace - empty until you create the dbt project (§9)
solutions/                  the finished version of each Dag, and of the dbt project
src/project_tagger/         the ML project Airflow orchestrates (imports no Airflow)
  config.py                 names and settings: tables, MLflow, Optuna, thresholds
  data.py                   read the warehouse, snapshots, preprocessing, split
  validate.py               Great Expectations suites and the split checks
  train.py  evaluate.py     the classifier, the Optuna search, MLflow logging
  registry.py               MLflow model registry: register, promote, roll back
  monitoring.py  drift.py   drift windows, alibi-detect tests, live performance
  predict.py                inference against the promoted model
  assets.py                 the Airflow Assets the Dags share
  callbacks.py              deadline-alert callbacks
datasets/                   the two source CSVs Airbyte loads
secrets/                    where your GCP key goes (ignored by git)
tests/                      Dag integrity (your dags/), unit tests, README checks
docs/                       cheatsheet, troubleshooting, the Dag appendix (§16)
scripts/                    up / reset / native setup / README sync
docker-compose.yaml         Airflow (LocalExecutor) + Postgres + MLflow
```

## Further reading

- [Airflow 3 documentation](https://airflow.apache.org/docs/apache-airflow/stable/)
- [Task SDK reference](https://airflow.apache.org/docs/task-sdk/stable/api.html)
- [Assets and data-aware scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html)
- [Provider packages index](https://airflow.apache.org/docs/apache-airflow-providers/)
- [Made With ML's MLOps course](https://madewithml.com/courses/mlops/), whose
  orchestration lesson this tutorial follows, and whose earlier lessons (the
  data stack, testing, experiment tracking, monitoring) go deeper into each
  tool. Its Airflow code is for an older version.
- Each tool's own documentation: [Airbyte](https://docs.airbyte.com/),
  [BigQuery](https://cloud.google.com/bigquery/docs), [dbt](https://docs.getdbt.com/),
  [Great Expectations](https://docs.greatexpectations.io/),
  [Optuna](https://optuna.readthedocs.io/), [MLflow](https://mlflow.org/docs/latest/),
  [alibi-detect](https://docs.seldon.io/projects/alibi-detect/).
