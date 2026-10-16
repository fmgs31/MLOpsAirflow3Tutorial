## 15. Testing Dags

Four levels of tests on the code — the first of them checks the seven files
you wrote — and one on the data.

**Dag integrity tests** — [`tests/test_dag_integrity.py`](../../tests/test_dag_integrity.py).
These catch, before the file reaches Airflow, the failures that would otherwise
surface as an import error in the UI, and they are the single highest-value
thing you can put in CI.

They rest on `DagBag`, the class Airflow uses to parse a Dag folder:
`DagBag(dag_folder=...)` imports every file in it and keeps the Dags it found
in `.dags` and the files that failed in `.import_errors`. The `dagbag` argument
in each test is a pytest fixture, defined in the same file, that
builds one from your `dags/` folder, first putting it on `sys.path` as
Airflow does (§8), so your Dags can import `project_tagger_orchestration`:

```python
from airflow.dag_processing.dagbag import DagBag

def test_no_import_errors(dagbag):
    assert not dagbag.import_errors

def test_mlops_is_scheduled_on_the_labeled_projects_asset(dagbag):
    asset_names = {a.name for a in dagbag.dags["mlops"].timetable.asset_condition.objects}
    assert "labeled_projects" in asset_names
```

They test **your `dags/` folder**, and they encode what this tutorial expects:
seven specific `dag_id`s; every Dag with tags, a `doc_md` and an owner that is
not `airflow`; `retries` of at least one on every Dag that does real work;
`dataops` producing its three assets; `mlops` scheduled on `labeled_projects` and
validating before it trains; no cycles. The suite also greps the sources for
constructs from older Airflow versions — `schedule_interval=`, `days_ago(`,
`airflow.operators.`, `execution_date`, `SubDagOperator`, `sla=`. Most Airflow
code on the internet predates Airflow 3, so a copy-pasted snippet can bring one
of these in. Some stop the file from importing (`schedule_interval=`,
`days_ago(`, `SubDagOperator`); the rest get through silently:
`airflow.operators.` still imports through a deprecation shim, `sla=` is
accepted and does nothing, and `execution_date` fails only once the task runs.

**Unit tests on the workload** — [`tests/test_project_tagger.py`](../../tests/test_project_tagger.py).
Nothing in `src/project_tagger/` imports Airflow, so the ML code is testable
without a scheduler in the loop. That separation is deliberate: Airflow orchestrates the
code, it does not contain it.

They do not need the warehouse or the MLflow server either, and that is
deliberate too. A test that queries BigQuery is slow, needs credentials and
fails when the network does. So each test builds the dataframe a query would
have returned and passes it in: the GX suites, the split checks and the drift
windows all take a dataframe, not a table name. Optuna and MLflow run for
real, but against SQLite files in a temporary folder: the test points
`config.OPTUNA_STORAGE` and `config.MLFLOW_TRACKING_URI` there with pytest's
`monkeypatch`, which replaces a value for the length of one test, then tunes,
trains, registers, promotes and rolls back. The drift test needs alibi-detect, which lives only in its own virtualenv (§12),
so in Airflow's environment it is **skipped**, not failed.

**Data tests** — the GX suites and dbt's tests (§9). They are tests too, but on
the data instead of the code, so they run where the data is: in every pipeline
run, as gates, not in CI. A code test proves the pipeline does what you meant;
a data test proves today's data is what the pipeline expects. You need both.

**Whole-run tests** — `airflow dags test <dag_id>` executes a real Dag run
inside the command itself (§4), and `airflow tasks test <dag_id> <task_id>`
runs a single task without recording its state in the metadata database.

**Tests on the documentation** — two of them, because a tutorial that quotes
code is only useful while the quotes are true.
[`tests/test_readme_listings.py`](../../tests/test_readme_listings.py): the
[appendix](../appendix-dags.md) shows every solution in full, injected from
`solutions/` by [`scripts/sync_readme.py`](../../scripts/sync_readme.py)
rather than copied by hand; the test re-runs the injection in memory and fails if the
result differs.
[`tests/test_readme_build_steps.py`](../../tests/test_readme_build_steps.py): every
"Build it" block in this tutorial is tagged with the file it belongs to; the test
concatenates them in order, loads the result with `DagBag`, and fails if what
you were told to type does not produce a Dag that imports cleanly.

The integrity, unit and documentation tests are one `pytest` run:

```bash
docker compose run --rm --entrypoint pytest airflow-cli /opt/airflow/tests -q
```

`airflow-cli` is not one of the services `docker compose ps` lists: it is
a one-off container the compose file defines under a profile, which `run`
creates for that command and `--rm` removes afterwards.

**Checkpoint.** A row of dots, one `s`, and `41 passed, 1 skipped`, in
well under a minute:

```
............................s.............                               [100%]
41 passed, 1 skipped in 20.75s
```

The `s` is the drift test, skipped for the reason given under "Unit tests on
the workload". No scheduler was involved, no Dag ran, and nothing reached
BigQuery or the MLflow server, so every one of those tests is fast enough to
run on every commit.

When one fails, the message says which file and what is missing (`mlops has no
retries`, `Dags not yet written in /opt/airflow/dags: ['monitoring']`,
`dataops` missing an outlet), and the fix is in the section that built that
file, or in `solutions/`. To see the suite pass against the finished versions,
point it there:

```bash
docker compose run --rm -e DAGS_DIR=solutions --entrypoint pytest airflow-cli /opt/airflow/tests -q
```

> On **Git Bash**, prefix both commands with `MSYS_NO_PATHCONV=1` or the
> `/opt/airflow/tests` argument is rewritten into a Windows path and pytest
> collects nothing.

When `test_readme_listings.py` fails, an appendix listing no longer matches its
file in `solutions/`. The script that injects the listings checks and fixes
that:

```bash
python scripts/sync_readme.py --check     # exit 1 when an appendix listing is stale
python scripts/sync_readme.py             # fix it
```

None of those levels would have caught the failures in §7's "trap" box (the
`KeyError` and the `UndefinedError` of a run without a logical date):
`airflow dags test` always supplies a logical date, so a Dag that breaks on a
run without one passes every test in this section. **Trigger each Dag once
for real** before you trust it: through the scheduler, with
`airflow dags trigger` and no `-l`, so the run has no logical date, like an
asset-triggered one.

---

← [14. When the data goes wrong](14-when-the-data-goes-wrong.md) · [Index](README.md) · [16. Taking it to production](16-taking-it-to-production.md) →
