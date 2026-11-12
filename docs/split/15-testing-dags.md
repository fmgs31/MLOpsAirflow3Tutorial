## 15. Testing Dags

Three levels of tests on the code, and one on the data.

**Dag integrity tests** — [`tests/test_dag_integrity.py`](../../tests/test_dag_integrity.py).
They catch, before the file reaches Airflow, the failures that would otherwise
show up as an import error in the UI: the single highest-value thing to put in
CI.

They rest on `DagBag`, the class Airflow uses to parse a Dag folder: it imports
every file, keeping the Dags in `.dags` and the files that failed in
`.import_errors`. The `dagbag` fixture builds one from your `dags/` folder,
after putting it on `sys.path` as Airflow does (§8) so your Dags can import
`project_tagger_orchestration`:

```python
import sys
from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag

DAGS_FOLDER = Path(__file__).resolve().parents[1] / "dags"

@pytest.fixture(scope="session")
def dagbag():
    sys.path.insert(0, str(DAGS_FOLDER))
    return DagBag(dag_folder=str(DAGS_FOLDER))

def test_no_import_errors(dagbag):
    assert not dagbag.import_errors

def test_mlops_is_scheduled_on_the_labeled_projects_asset(dagbag):
    asset_names = {a.name for a in dagbag.dags["mlops"].timetable.asset_condition.objects}
    assert "labeled_projects" in asset_names
```

The rest check what this tutorial expects of your Dags: the seven `dag_id`s,
tags, `doc_md`, owners, `retries`, `dataops`'s assets, `mlops`'s schedule, and
no cycles. The suite also greps the sources for constructs from older Airflow
versions, because most Airflow code on the internet predates Airflow 3 and a
copied snippet can bring them in. The grep is needed because some of them
import without error: `test_no_import_errors` misses them, and they fail later
or do nothing.

**Unit tests on the workload** — [`tests/test_project_tagger.py`](../../tests/test_project_tagger.py).
Nothing in `src/project_tagger/` imports Airflow, so the ML code is tested
without a Scheduler: Airflow orchestrates the code, it does not contain it. Nor
do the tests touch BigQuery or the MLflow server, which would make them slow
and dependent on credentials and the network. Each test hands the code the
dataframe a query would have returned, and Optuna and MLflow run against SQLite
files in a temporary folder, where pytest's `monkeypatch` (which replaces a
value for the length of one test) points them. The drift test needs
alibi-detect, which lives only in its own virtualenv (§12), so in Airflow's
environment it is **skipped**, not failed.

**Data tests** — the GX suites and dbt's tests (§9). They run where the data
is: as gates in every pipeline run, not in CI. A code test proves the pipeline
does what you meant; a data test, that today's data is what the pipeline
expects. You need both.

**Whole-run tests** — `airflow dags test <dag_id>` executes a real Dag run
inside the command itself (§4), and `airflow tasks test <dag_id> <task_id>`
runs a single task without recording its state in the Metadata database.

The integrity and unit tests are one `pytest` run:

```bash
docker compose run --rm --entrypoint pytest airflow-cli /opt/airflow/tests -q
```

`airflow-cli` does not show up in `docker compose ps`: it is a one-off
container, defined under a compose profile, that `run` creates and `--rm`
removes when it ends.

**Checkpoint.** A row of dots, one `s`, and `45 passed, 1 skipped`, in
well under a minute:

```
..............................s...............                           [100%]
45 passed, 1 skipped in 23.78s
```

The `s` is the drift test. None of them needs a Scheduler, BigQuery or MLflow,
so they are fast enough to run on every commit.

When one fails, the message names the file and what is missing (`mlops has no
retries`, `Dags not yet written in /opt/airflow/dags: ['monitoring']`); the fix
is in the section that built that file, or in `solutions/`. To see the suite
pass against the finished versions, point it there:

```bash
docker compose run --rm -e DAGS_DIR=solutions --entrypoint pytest airflow-cli /opt/airflow/tests -q
```

> On **Git Bash**, prefix both commands with `MSYS_NO_PATHCONV=1` or the
> `/opt/airflow/tests` argument is rewritten into a Windows path and pytest
> collects nothing.

No test in this section catches the failures in §7's "trap" box (the
`KeyError` and the `UndefinedError` of a run without a logical date), because
`airflow dags test` always supplies one. **Trigger each Dag once for real**
before you trust it: with `airflow dags trigger` and no `-l`, so the run has no
logical date, like an asset-triggered one.

---

← [14. When the data goes wrong](14-when-the-data-goes-wrong.md) · [Index](README.md) · [16. Taking it to production](16-taking-it-to-production.md) →
