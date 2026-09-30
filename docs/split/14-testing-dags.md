## 14. Testing Dags

Four levels, cheapest first — and the first of them is the grader for the seven
files you wrote.

**Dag integrity tests** — [`tests/test_dag_integrity.py`](../../tests/test_dag_integrity.py).
These catch the failures that otherwise surface as a red banner in the UI hours
later, and they are the single highest-value thing you can put in CI:

```python
from airflow.dag_processing.dagbag import DagBag

def test_no_import_errors(dagbag):
    assert not dagbag.import_errors

def test_mlops_is_scheduled_on_the_labeled_projects_asset(dagbag):
    asset_names = {a.name for a in dagbag.dags["mlops"].timetable.asset_condition.objects}
    assert "labeled_projects" in asset_names
```

They load **your `dags/` folder**, and they encode what this tutorial meant:
seven specific `dag_id`s; every Dag with tags, a `doc_md` and an owner that is
not `airflow`; `retries` of at least one on every Dag that does real work;
`dataops` producing both assets; `mlops` scheduled on `labeled_projects` and
validating before it trains; no cycles. It also greps the sources for
constructs that do not exist in this version of Airflow — `schedule_interval=`,
`days_ago(`, `airflow.operators.`, `execution_date`, `SubDagOperator`, `sla=`.
Most Airflow code on the internet predates Airflow 3, so a copy-pasted snippet
can bring one of these in, and several of them still *import* and fail only
once the task runs.

**Unit tests on the workload** — [`tests/test_tagifai.py`](../../tests/test_tagifai.py).
Nothing in `src/tagifai/` imports Airflow, so the ML code is testable without a
scheduler in the loop. That separation is deliberate: Airflow orchestrates the
code, it does not contain it.

**Whole-run tests** — `airflow dags test <dag_id>` executes a real Dag run
in-process, and `airflow tasks test <dag_id> <task_id>` runs one task without
writing state.

**Tests on the documentation** — two of them, because a tutorial that quotes
code is only useful while the quotes are true.
[`tests/test_readme_listings.py`](../../tests/test_readme_listings.py): the
[appendix](../appendix-dags.md) shows every solution in full, injected from
`solutions/` by [`scripts/sync_readme.py`](../../scripts/sync_readme.py) rather than
copied by hand; the test re-runs the injection in memory and fails if the
result differs.
[`tests/test_readme_build_steps.py`](../../tests/test_readme_build_steps.py): every
"Build it" block above is tagged with the file it belongs to; the test
concatenates them in order, loads the result with `DagBag`, and fails if what
you were told to type does not add up to a Dag that imports.

Run all of it:

```bash
docker compose run --rm --entrypoint pytest airflow-cli /opt/airflow/tests -q
```

`airflow-cli` is not one of the five services `docker compose ps` lists: it is
a one-off container the compose file defines under a profile, which `run`
creates for that command and `--rm` removes afterwards.

**Checkpoint.** A row of dots and `36 passed`, in well under a minute:

```
....................................                                     [100%]
36 passed in 7.11s
```

No scheduler was involved and no Dag ran. That is the point: every one of those
36 is fast enough to run on every commit, which is what makes them worth having.

When one fails, the message says which file and what is missing — `mlops has no
retries`, `Dags not yet written in /opt/airflow/dags: ['monitoring']`,
`dataops` missing an outlet — and the fix is in the lesson that built that
file, or in `solutions/`. To see the suite pass against the finished versions,
point it there:

```bash
docker compose run --rm -e DAGS_DIR=solutions --entrypoint pytest airflow-cli /opt/airflow/tests -q
```

> On **Git Bash**, prefix both commands with `MSYS_NO_PATHCONV=1` or the
> `/opt/airflow/tests` argument is rewritten into a Windows path and pytest
> collects nothing.

```bash
python scripts/sync_readme.py --check     # exit 1 when an appendix listing is stale
python scripts/sync_readme.py             # fix it
```

None of those levels would have caught the two bugs in §7's "trap" box:
`airflow dags test` always supplies a logical date, so a Dag that breaks on a
manual trigger passes every test above. **Trigger each Dag once for real** —
through the scheduler, the way it will actually run — before you believe it.

---

← [13. Deadline alerts](13-deadline-alerts.md) · [Index](README.md) · [15. Taking it to production](15-taking-it-to-production.md) →
