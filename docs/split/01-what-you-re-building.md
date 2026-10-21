## 1. What you're building

Orchestration turns a pile of scripts you run by hand into a system: the work is
declared as a graph of tasks, something else decides when to run it, retries the
parts that fail, and records what happened.

A workflow in Airflow is a **Dag** — a directed acyclic graph. *Directed*: edges
have a direction. *Acyclic*: no loops, so no path of edges leads back to a
task it already passed through. Each node is a **task**, and one execution of the whole
graph is a **Dag run**.

Every log line and error message assumes you tell apart two words. A
**task** is what you wrote once, in the file. A **task instance** is that task
in one Dag run: the thing that has a state, a duration, a log and a number of
attempts. You write tasks; you debug task instances. §3 follows one through its whole life.

This is an MLOps tutorial: you build a small machine-learning system, from
raw data to a model in production and back. Each step is done by the tool a
real team would use for it; Airflow decides when each one runs:

| Tool | Its job here | Section |
|---|---|---|
| **Airbyte** | copies the source data into the warehouse, the database where data is kept for analysis | §9 |
| **BigQuery** | the warehouse, on Google Cloud | §9 |
| **dbt** | turns the raw tables into the training table, in SQL | §9 |
| **Great Expectations** (GX) | checks the data before each step uses it | §9, §10 |
| **Optuna** | searches for the best hyperparameters: the model settings fixed before training | §10 |
| **MLflow** | records every training run, and keeps the model registry: the numbered versions of the trained model, and which one is in production | §10, §12 |
| **alibi-detect** | detects drift: new data that no longer looks like the data the model learned from | §12 |

All of them except BigQuery are open source and run on your machine. BigQuery
needs a Google Cloud account with billing enabled; this tutorial's data stays
far inside the free tier (§9 explains).

Three Dags do the real work, and the interesting part is how they connect:

```
dataops:  extract & load ─► validate ─► transform ─► validate_transformed ─┐
          (Airbyte)          (GX)       (dbt)         (GX)                  │ emits
                                                              Asset: labeled_projects
                                                                            │ triggers
mlops:    prepare ─► validate ─► tune×N ─► train ─► evaluate ─► gate ───────┴─► approve ─► promote
                     (GX)        (Optuna)  (MLflow)                             (a person)  (MLflow registry)
                                                                            │ emits
                                                              Asset: production_model

monitoring:  measure ─► decide ─► {continue | inspect | rollback | retrain}
             (alibi-detect)
```

An **asset** is a named piece of data: `labeled_projects` is the labeled data
`dataops` produces, and `production_model` is the model `mlops` promotes. Each
time the task that produces an asset succeeds, Airflow records an event, and
any Dag that has that asset as its schedule starts a run (§8). So `mlops`
has **no time schedule**: it runs when `dataops` publishes good data. This
data-aware scheduling is what ties the three Dags together.

Four more Dags exist to teach one thing each: `hello_airflow3` (a first Dag),
`operators_and_xcoms` (task types and how tasks talk), `sensors_and_events`
(waiting for the outside world) and `deadline_alerts` (being told when a run is
too slow).

### How this tutorial works

You write all seven Dags yourself, plus the package they share (§8) and the
dbt project (§9). The folder Airflow reads, `dags/`, is **empty** when you
clone this repo, and so is `dbt/`. Each section explains a piece of Airflow,
has you build the Dag that uses it, a few lines at a time with the reason for
each, then run it and check the result. The code is given; the work is typing
it, seeing it parse, and watching it run.

Two safety nets:

- **[`solutions/`](../../solutions/)** holds a finished copy of every file, with the
  teaching comments left in. Compare when something does not parse, or skip a
  lesson by copying its file in: `cp solutions/03_dataops.py dags/`. The
  [appendix](../appendix-dags.md) prints every solution in full.
- **§15's tests** read *your* `dags/` folder. When all seven are written, they
  tell you whether you built what the tutorial meant.

The lessons build on each other from §9 on — `mlops` is scheduled on an asset
`dataops` produces, `sensors_and_events` waits for a table `dataops` builds — so
do them in order, or copy the earlier solutions in first. §9 also sets up the
data stack the rest depends on: a Google Cloud project, Airbyte, and the
connections between them.

### Why Airflow

Airflow is not the only orchestrator — Prefect, Dagster, Luigi, Metaflow, Flyte,
Kubeflow Pipelines and Vertex AI Pipelines all cover this ground. Airflow is
worth learning first because:

- **Adoption.** It is the default in most data platforms, so the concepts
  transfer and every vendor ships an integration.
- **Apache governance**, a large provider ecosystem, and no single vendor.

And, like Prefect or Dagster, it has what you expect of a modern
orchestrator: workflows written in ordinary Python, testable with pytest (§15), and the
same code running on your laptop and on a cluster — swap the executor, not the
Dag.

### The ML project it orchestrates

Airflow orchestrates code; it does not contain it. Here that code is a small
ML project in `src/project_tagger/`, which you do not write: the Dags call it.

**The data** is 764 ML projects collected by Made With ML between February and
October 2020, in two tables, as a source system would keep them
([`datasets/`](../../datasets/)): `projects.csv`, with each project's title,
description and the date it was added, and `tags.csv`, with each project's
**tag** — the label the model learns to predict. Joined on `id`, one row:

| title | description | tag |
|---|---|---|
| Rethinking Batch Normalization in Transformers | We found that NLP batch statistics exhibit large variance throughout training, which leads to poor BN performance. | `natural-language-processing` |

There are six tags. The three rarest (`reinforcement-learning`,
`graph-learning`, `time-series`) have too few projects to learn from, so the
model sees them as one class, `other`, next to `natural-language-processing`,
`computer-vision` and `mlops`.

Two later deliveries add to it. In §12, 800 arXiv papers from 2025–2026,
extracted for this tutorial: newer data from another source is what makes
drift cost the production model f1, and retraining win it back.
In §14, 191 more Made With ML projects, delivered with problems added for
this tutorial.

**The model** reads a project's title and description and predicts its tag. It
is a scikit-learn text classifier (TF-IDF features and a linear SGD
classifier) that trains in about a second, so a search over dozens of
hyperparameter sets fits in a Dag run. `dataops` (§9) loads, checks and joins
the data in the warehouse; `mlops` (§10) searches the hyperparameters, trains
the best model and decides whether it replaces the one in production.

**The production model** is the trained model currently used to tag new
text. Nothing serves it over the network here: MLflow's model registry marks
one version as `champion`, and `predict.py` loads that version and tags
whatever text it is given:

```
"Transfer learning with BERT for text classification"  →  natural-language-processing
"Real-time object detection on a webcam with YOLO"     →  computer-vision
```

**Drift** is the data changing under the production model. The `monitoring`
Dag (§12) compares the projects that arrived after the production model was
trained with the ones it learned from: has the share of each tag moved, has a
tag appeared that the model never saw, does the production model still score
as well.

The modules, none of which imports Airflow:

| Module | Role |
|---|---|
| `config.py` | names and settings: tables, MLflow, Optuna, thresholds |
| `data.py` | read the warehouse, snapshots, text preprocessing, the train/validation/test split |
| `validate.py` | the Great Expectations suites the gates run, and the split checks |
| `train.py`, `evaluate.py` | the classifier, the Optuna search, MLflow logging; metrics overall and on evaluation slices (§10) |
| `registry.py` | MLflow's model registry: register, promote, roll back |
| `monitoring.py`, `drift.py` | the windows drift is measured on, the alibi-detect tests, live performance |
| `predict.py` | tag new text with whatever is currently promoted |
| `callbacks.py` | deadline-alert callbacks |

The SQL is not in that package: it is the dbt project you create in §9, in
`dbt/`. Nor are the Airflow Assets the Dags share: they are orchestration, and
you write them next to the Dags, in §8.

### What changes from Made With ML's lesson

This tutorial starts from Made With ML's
[orchestration lesson](https://madewithml.com/courses/mlops/orchestration/):
the same project, the same starting data and the same tools. What makes it
different:

- **Airflow 3.** All the code is written for Airflow 3.3.1; the original uses
  an earlier version.
- **Made to run, not just to read.** You write the seven Dags yourself, step
  by step, on a Docker stack you start with one command, with solutions and
  tests to check what you wrote. Every command has been run against a real
  stack.
- **A pipeline that starts itself.** New data starts training through an
  asset, with no manual triggers (§8).
- **The full MLOps cycle.** The model is evaluated, compared with the one in
  production, approved by a person and promoted in MLflow (§10).
- **Continual learning, implemented.** The original only describes it; here a
  Dag measures drift and decides whether to continue, retrain, inspect or
  roll back (§12).
- **Data that changes and data that breaks.** arXiv papers that cause real
  drift (§12), and deliveries with the failures of a real load (§14).
- **New sections:** Airflow's components and UI (§3, §5), scheduling and
  assets (§7, §8), sensors and events (§11), deadline alerts (§13), what to do
  when the data goes wrong (§14), testing Dags (§15) and taking it to
  production (§16).

---

[Index](README.md) · [2. Get it running](02-get-it-running.md) →
