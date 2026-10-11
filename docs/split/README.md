# Learn Apache Airflow 3 by building an ML pipeline

A hands-on tutorial for **Airflow 3.3.1**. You will write seven Dags, one file
at a time, that grow into one working system: data is extracted, validated and
transformed, that publishes an **Asset**, the Asset triggers model training,
the best model is promoted to production, and a monitoring Dag decides whether
to retrain.

Everything runs on your machine in Docker. No cloud account, no warehouse, no
vendor signup. Every command in this tutorial was executed against a real Airflow 3.3.1
stack — see [What was actually verified](../verified.md).

**Assumed:** Python, the command line, and Docker installed. No Airflow
experience — this teaches Airflow 3 as it is today, and nothing about older
versions.

**Time:** ~30 minutes to your first Dag run. A day for all seven if you type
them yourself, which is the point.

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
solutions/                  the finished version of each, numbered in reading order
src/project_tagger/         the ML project Airflow orchestrates (imports no Airflow)
  config.py                 paths and thresholds
  data.py                   extract / clean / transform / split
  validate.py               a tiny expectation-suite runner
  train.py  evaluate.py     TF-IDF + SGD classifier, metrics and slices
  registry.py               filesystem stand-in for a model registry
  monitoring.py             drift and live-performance checks
  predict.py                inference against the promoted model
  assets.py                 the Airflow Assets the Dags share
  callbacks.py              deadline-alert callbacks
tests/                      Dag integrity (your dags/), unit tests, README checks
docs/                       cheatsheet, troubleshooting, the Dag appendix (§16)
scripts/                    up / reset / native setup / README sync
docker-compose.yaml         LocalExecutor + Postgres + Simple auth
```

## Further reading

- [Airflow 3 documentation](https://airflow.apache.org/docs/apache-airflow/stable/)
- [Task SDK reference](https://airflow.apache.org/docs/task-sdk/stable/api.html)
- [Assets and data-aware scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html)
- [Provider packages index](https://airflow.apache.org/docs/apache-airflow-providers/)
- The ML pipeline this tutorial orchestrates follows the [Made With ML
  orchestration lesson](https://madewithml.com/courses/mlops/orchestration/) —
  useful for the MLOps reasoning; its Airflow code is for an older version.
