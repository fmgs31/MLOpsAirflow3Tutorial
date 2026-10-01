## 1. What you're building

Orchestration turns a pile of scripts you run by hand into a system: the work is
declared as a graph of tasks, something else decides when to run it, retries the
parts that fail, and records what happened.

A workflow in Airflow is a **Dag** — a directed acyclic graph. *Directed*: edges
have a direction. *Acyclic*: no loops, so a finished task never re-runs because
of something downstream. Each node is a **task**, and one execution of the whole
graph is a **Dag run**.

Two words are worth separating now, *task* and *task instance*, because every
log line and error message assumes you already do. A **task** is the thing you wrote once, in the file. A
**task instance** is that task inside one particular Dag run — the thing that
actually has a state, a duration, a log and a number of attempts. You write
tasks; you debug task instances. §3 follows one through its whole life.

Three Dags do the real work here, and the interesting part is how they connect:

```
dataops:   extract ─► validate_raw ─► transform ─► validate_transformed ──┐
                                                                          │ emits
                                                            Asset: labeled_projects
                                                                          │ triggers
mlops:                  prepare ─► validate ─► train×N ─► select ─► gate ─┴─► promote
                                                                          │ emits
                                                            Asset: production_model

monitoring:  measure ─► decide ─► {continue | inspect | rollback | approve ─► retrain}
```

`mlops` has **no schedule**. It runs when `dataops` publishes good data. That is
data-aware scheduling, and it is the idea this whole tutorial is built around.

Four more Dags exist to teach one thing each: `hello_airflow3` (a first Dag),
`operators_and_xcoms` (task types and how tasks talk), `sensors_and_events`
(waiting for the outside world) and `deadline_alerts` (being told when a run is
too slow).

### How this tutorial works

You write every one of those seven Dags yourself. The folder Airflow reads,
`dags/`, is **empty** when you clone this repo. Each section explains a piece of
Airflow, then has you build the Dag that uses it — a few lines at a time, with
the reason for each piece next to it — and then run it and check what happened.
The code is given; the work is typing it, seeing it parse, and watching it run.

Two safety nets:

- **[`solutions/`](../../solutions/)** holds a finished copy of every file, with the
  teaching comments left in. Compare when something does not parse, or skip a
  lesson by copying its file in: `cp solutions/03_dataops.py dags/`. The
  [appendix](../appendix-dags.md) prints every solution in full.
- **§14's tests** read *your* `dags/` folder. When all seven are written, they
  tell you whether you built what the tutorial meant.

The lessons build on each other from §9 on — `mlops` is scheduled on an asset
`dataops` produces, `sensors_and_events` waits for a file `dataops` writes — so
do them in order, or copy the earlier solutions in first.

### Why Airflow

Airflow is not the only orchestrator — Prefect, Dagster, Luigi, Metaflow, Flyte,
Kubeflow Pipelines and Vertex AI Pipelines all cover this ground. Airflow is
worth learning first because:

- **Adoption.** It is the default in most data platforms, so the concepts
  transfer and every vendor ships an integration.
- **Pure Python, no DSL.** Workflows are ordinary functions, testable with
  pytest (§14).
- **The same code runs on your laptop and on a cluster** — swap the executor,
  not the Dag.
- **Apache governance**, a large provider ecosystem, and no single vendor.

### The ML project it orchestrates

Airflow orchestrates code; it does not contain it. The code being orchestrated
lives in `src/tagifai/` and imports no Airflow at all — a small text classifier
that tags ML projects. You do not write this part; the Dags call it:

| Module | Role |
|---|---|
| `data.py` | extract, clean, transform, train/test split |
| `validate.py` | data-quality expectations, the thing the gates run |
| `train.py`, `evaluate.py` | TF-IDF + SGD classifier, metrics and slices |
| `registry.py` | filesystem stand-in for a model registry |
| `monitoring.py` | drift and live-performance checks |
| `predict.py` | tag new text with whatever is currently promoted |
| `assets.py` | the Airflow Assets the Dags share |
| `callbacks.py` | deadline-alert callbacks |

Each is a slot you can swap for a real tool — the tables in §9 and §10 say which.

---

[Index](README.md) · [2. Get it running](02-get-it-running.md) →
