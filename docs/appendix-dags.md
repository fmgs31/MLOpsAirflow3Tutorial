# Appendix: the finished Dags in full

> Part of the [Airflow 3 tutorial](../README.md), split out of it to keep the
> tutorial itself readable. `§N` below refers to a numbered section of that
> README.

The lessons in the tutorial give you each file in pieces. Here is each one end
to end, in reading order, as it sits in [`solutions/`](../solutions/) — the same
code you typed, plus the comments, which explain the details at the point they
matter.

> These listings are **injected from the files** by
> [`scripts/sync_readme.py`](../scripts/sync_readme.py) and verified by
> [`tests/test_readme_listings.py`](../tests/test_readme_listings.py): edit a
> solution without re-running the script and the test suite fails. They cannot
> drift out of sync with the code that actually runs.

First, the package `dataops` and `mlops` import (§8): `.airflowignore`, then
`project_tagger_orchestration/`.
<details>
<summary><b>solutions/project_tagger_orchestration/</b> — the Assets the Dags produce and consume (§8)</summary>

<!-- BEGIN FILE: solutions/.airflowignore -->
```
project_tagger_orchestration/
```
<!-- END FILE: solutions/.airflowignore -->

<!-- BEGIN FILE: solutions/project_tagger_orchestration/__init__.py -->
```python
"""Code shared by this tutorial's Dags. Not a Dag: `.airflowignore` keeps the Dag processor out."""
```
<!-- END FILE: solutions/project_tagger_orchestration/__init__.py -->

<!-- BEGIN FILE: solutions/project_tagger_orchestration/assets.py -->
```python
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
```
<!-- END FILE: solutions/project_tagger_orchestration/assets.py -->

</details>

<details>
<summary><b>solutions/01_hello_airflow3.py</b> — a Dag, tasks, dependencies, params (§4)</summary>

<!-- BEGIN FILE: solutions/01_hello_airflow3.py -->
```python
"""Lesson 1 - a Dag, tasks, and dependencies.

Everything a Dag author needs comes from one place: `airflow.sdk`, the Task SDK.
`dag`, `task`, `DAG`, `Param`, `Asset`, `chain`, `Label` - all of it is there.

Four tasks, wired by passing one task's output to another. Read `make_greeting`
first, then the two lines at the bottom that build the graph.

Run it without a scheduler:
    airflow dags test hello_airflow3
"""

from __future__ import annotations

from typing import Any

import pendulum

from airflow.sdk import Param, dag, task

DEFAULT_ARGS = {
    "owner": "mlops-course",
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=30),
}


@dag(
    dag_id="hello_airflow3",
    # `schedule` takes None (manual/API only), a cron string, a timedelta, a
    # Timetable, or a list of Assets (see 03_dataops / 04_mlops).
    schedule=None,
    # Optional. The scheduler never creates a run before it; manual triggers
    # ignore it. Set so the Dag is ready if it ever gets a schedule.
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    # Defaults to False; being explicit is good manners. This is the flag that
    # decides whether un-pausing a Dag floods you with historical runs.
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "basics"],
    doc_md=__doc__,
    params={
        # Params render as a form when you trigger the Dag from the UI.
        "greeting": Param("hello", type="string", description="What to say."),
    },
)
def hello_airflow3():
    """A four-task Dag: one source, two branches of work, one join."""

    @task
    def make_greeting(**context: Any) -> str:
        # `**context` gives you the run's metadata: run_id, params, ti,
        # dag_run, ds, data_interval_start/end, var, conn, macros.
        #
        # Read `logical_date` with `.get()`, never `context["logical_date"]`:
        # a run triggered by an asset, or from the CLI without `-l`, has no
        # logical date, and the key is then missing from the context entirely -
        # indexing it raises KeyError at runtime while `airflow dags test`
        # (which always supplies one) stays green. `run_id` is always there;
        # prefer it for naming things.
        greeting = context["params"]["greeting"]
        when = context.get("logical_date") or "no logical date"
        return f"{greeting} from run {context['run_id']} at {when}"

    @task
    def shout(message: str) -> str:
        return message.upper()

    @task
    def count_words(message: str) -> int:
        return len(message.split())

    @task
    def report(loud: str, words: int) -> None:
        # Anything you `print`/log lands in the task log, viewable in the UI.
        print(f"{loud}  ({words} words)")

    # Calling the task functions builds the dependency graph. The return value
    # of a task is an XComArg - a lazy reference to its XCom, not the value -
    # so passing it to another task is what creates the edge.
    message = make_greeting()
    report(loud=shout(message), words=count_words(message))


# The decorator only defines the Dag; calling it registers it.
hello_airflow3()
```
<!-- END FILE: solutions/01_hello_airflow3.py -->

</details>

<details>
<summary><b>solutions/02_operators_and_xcoms.py</b> — operators, XComs, relationships (§6)</summary>

<!-- BEGIN FILE: solutions/02_operators_and_xcoms.py -->
```python
"""Lesson 2 - operators, XComs, and relationships.

Lesson 1 used `@task`-decorated functions. This one uses **operators**, the
pre-built task types, and shows that the two styles mix freely in one Dag.

Two things to take away:

1. The common operators live in `apache-airflow-providers-standard`, which is
   pre-installed. That is where you import them from:

       from airflow.providers.standard.operators.bash import BashOperator

2. `xcom_pull()` pulls from the *current* task unless you name the source, so
   an XCom read almost always wants `task_ids=`:

       ti.xcom_pull(task_ids="push_seed", key="seed")

The tail of the Dag is a tour of the ways to declare **relationships**: `>>`,
fan-out and fan-in with a list, `chain()` with `Label()`, and
`cross_downstream()` for an all-to-all edge set.

Run it:
    airflow dags test operators_and_xcoms
"""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Label, chain, cross_downstream, dag, task
from airflow.sdk.types import RuntimeTaskInstanceProtocol

DEFAULT_ARGS = {
    "owner": "mlops-course",
    "retries": 1,
    "retry_delay": pendulum.duration(seconds=30),
}


def _push_seed(ti: RuntimeTaskInstanceProtocol) -> None:
    """Operator style: push an XCom under an explicit key.

    `ti` (the task instance) is injected because the parameter is named `ti` -
    the same name-based injection `**context` does in lesson 1.
    """
    ti.xcom_push(key="seed", value=7)


def _multiply(ti: RuntimeTaskInstanceProtocol, factor: int) -> None:
    # Name the task you are pulling from. Without `task_ids` this reads the
    # current task's own XComs and quietly returns None.
    seed = ti.xcom_pull(task_ids="push_seed", key="seed")
    ti.xcom_push(key="product", value=seed * factor)


@dag(
    dag_id="operators_and_xcoms",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "basics"],
    doc_md=__doc__,
)
def operators_and_xcoms():
    start = EmptyOperator(task_id="start")

    push_seed = PythonOperator(task_id="push_seed", python_callable=_push_seed)

    multiply = PythonOperator(
        task_id="multiply",
        python_callable=_multiply,
        op_kwargs={"factor": 6},
    )

    # Some operator arguments are *templated*: Airflow renders them with Jinja
    # just before the task runs, so `{{ run_id }}`, `{{ ds }}` and
    # `{{ ti.xcom_pull(...) }}` are filled in with this run's values.
    #
    # Careful with `ds`: it is derived from `logical_date`, and a run triggered
    # by an asset, or from the CLI without `-l`, has none. The variable is then
    # *undefined* and the
    # task fails with `UndefinedError: 'ds' is undefined` while rendering.
    # `airflow dags test` always supplies a logical date, so this only shows up
    # once a real trigger reaches it. Guard it with `default`, or template
    # `{{ dag_run.run_after }}`, which every run has.
    describe = BashOperator(
        task_id="describe",
        bash_command=(
            'echo "run={{ run_id }} ds={{ ds | default(\'none\') }} '
            'run_after={{ dag_run.run_after }} '
            'product={{ ti.xcom_pull(task_ids=\'multiply\', key=\'product\') }}"'
        ),
    )

    @task
    def taskflow_reader(**context: Any) -> int:
        """TaskFlow can read operator XComs, and vice versa - they mix freely."""
        product = context["ti"].xcom_pull(task_ids="multiply", key="product")
        print(f"TaskFlow saw product={product}")
        return product

    # --- Relationships ------------------------------------------------------
    # `chain` is the readable form of `a >> b >> c`; `Label` annotates the edge
    # between its neighbours in the graph view (`a >> Label("x") >> b` works too).
    chain(start, push_seed, Label("seed -> product"), multiply, describe)

    # `>>` between single tasks. TaskFlow would have drawn this edge implicitly
    # if `describe` returned the value the reader needs; it does not, so the
    # dependency is declared by hand.
    product = taskflow_reader()
    describe >> product

    # `>>` passes no data: the checks pull the reader's return value from XCom.
    # `test` exits non-zero when the condition is false, failing the task.
    check_parity = BashOperator(
        task_id="check_parity",
        bash_command="test $(( {{ ti.xcom_pull(task_ids='taskflow_reader') }} % 2 )) -eq 0 && echo even",
    )
    check_range = BashOperator(
        task_id="check_range",
        bash_command="test {{ ti.xcom_pull(task_ids='taskflow_reader') }} -lt 100 && echo 'under 100'",
    )

    # Fan-out and fan-in on one line: a list on either side of `>>` broadcasts
    # the edge, so this is four dependencies, not two - product->check_parity,
    # product->check_range, check_parity->checks_done, check_range->checks_done.
    product >> [check_parity, check_range] >> EmptyOperator(task_id="checks_done")

    publish = EmptyOperator(task_id="publish")
    notify = EmptyOperator(task_id="notify")

    # All-to-all: every check upstream of every sink, four more edges.
    # `chain([a, b], [c, d])` would pair them element-wise instead (a->c, b->d,
    # and it raises if the lists differ in length), and `[a, b] >> [c, d]` is a
    # TypeError - Python has no list-to-list operator to dispatch on.
    cross_downstream([check_parity, check_range], [publish, notify])


operators_and_xcoms()
```
<!-- END FILE: solutions/02_operators_and_xcoms.py -->

</details>

<details>
<summary><b>solutions/03_dataops.py</b> — extract → validate → transform → validate, and the outlet (§9)</summary>

<!-- BEGIN FILE: solutions/03_dataops.py -->
```python
"""Lesson 3 - the DataOps Dag: extract & load -> validate -> transform -> validate.

The first Dag that does real work, and none of it happens inside Airflow:
Airflow tells each tool when to run and checks the result.

    extract_and_load_*   Airbyte copies each CSV into a BigQuery table
    validate_*           Great Expectations suites gate each raw table
    transform            dbt builds `labeled_projects` (a join) and tests it
    validate_transformed a final suite; only a pass emits the asset event

Three things to watch for:

* The Dag **produces Assets** (`outlets=[...]`). Finishing the producing task
  emits an asset event, and that event is what schedules the MLOps Dag - no
  cron guessing, no sensor, no polling.
* The Airbyte connection ids are **Variables**, read through a template at run
  time, so the Dag file names no id that only exists in your Airbyte.
* Validation results move through XCom as small dicts; the tables never do.
  Tasks exchange names and counts, not payloads.

Run it:
    airflow dags test dataops
"""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task, task_group

# Task code imports exceptions from the Task SDK. It is the namespace a remote
# worker has, and the one to reach for in a task.
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk.types import OutletEventAccessorsProtocol

from project_tagger import config, data, validate
from project_tagger_orchestration.assets import LABELED_PROJECTS, RAW_PROJECTS, RAW_TAGS

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}


@dag(
    dag_id="dataops",
    # A cron string uses CronTriggerTimetable: the run fires *at* 03:00 and its
    # `logical_date` is 03:00. Straightforward, but worth stating, because a
    # timetable is free to define that differently.
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "dataops"],
    doc_md=__doc__,
)
def dataops():
    # Extract and load: Airbyte does the copying, Airflow only starts it and
    # waits. `deferrable=True` hands the waiting to the triggerer, so no worker
    # slot is held while the sync runs.
    extract_and_load_projects = AirbyteTriggerSyncOperator(
        task_id="extract_and_load_projects",
        airbyte_conn_id="my_airbyte_conn",
        connection_id="{{ var.value.airbyte_projects_connection_id }}",
        deferrable=True,
        timeout=3600,
        outlets=[RAW_PROJECTS],
    )
    extract_and_load_tags = AirbyteTriggerSyncOperator(
        task_id="extract_and_load_tags",
        airbyte_conn_id="my_airbyte_conn",
        connection_id="{{ var.value.airbyte_tags_connection_id }}",
        deferrable=True,
        timeout=3600,
        outlets=[RAW_TAGS],
    )

    @task
    def validate_projects() -> dict[str, Any]:
        """Quality gate on the raw `projects` table."""
        result = validate.run_suite(data.load_projects(), "projects")
        print(result.summary())  # suite=projects passed=6/6
        if not result.passed:
            # AirflowFailException stops retries: a data-quality break will not
            # fix itself by running the same task again.
            raise AirflowFailException(f"projects failed validation: {result.failures}")
        # The suite's name, and one dict per expectation with its outcome.
        return {"suite": result.suite, "results": result.results}

    @task
    def validate_tags() -> dict[str, Any]:
        """Quality gate on the raw `tags` table."""
        result = validate.run_suite(data.load_tags(), "tags")
        print(result.summary())
        if not result.passed:
            raise AirflowFailException(f"tags failed validation: {result.failures}")
        return {"suite": result.suite, "results": result.results}

    # Transform: dbt runs in its own virtualenv, so a BashOperator calls its
    # executable. `dbt build` builds every model and then runs its tests; a
    # failing test exits non-zero, and that fails the task.
    # config.DBT_BIN is /opt/venvs/dbt/bin/dbt; config.DBT_PROJECT_DIR, dbt/dbt_transforms.
    transform = BashOperator(
        task_id="transform",
        bash_command=(
            f"{config.DBT_BIN} build"
            f" --project-dir {config.DBT_PROJECT_DIR}"
            f" --profiles-dir {config.DBT_PROJECT_DIR}"
        ),
    )

    @task(outlets=[LABELED_PROJECTS])
    def validate_transformed(*, outlet_events: OutletEventAccessorsProtocol) -> dict[str, Any]:
        """Final gate. Only a passing run emits the asset event.

        Placing the outlet on the *validation* task, not the transform task, is
        the whole trick: downstream Dags are then scheduled on
        "good data arrived", never on "a task finished".
        """
        df = data.load_labeled()
        result = validate.run_suite(df, "labeled_projects")
        print(result.summary())
        if not result.passed:
            raise AirflowFailException(f"labeled_projects failed validation: {result.failures}")

        outlet_events[LABELED_PROJECTS].extra = {
            "rows": len(df),
            "tags": sorted(df["tag"].unique().tolist()),
            "warnings": [r["expectation"] for r in result.failures],
        }
        return {"suite": result.suite, "rows": len(df), "results": result.results}

    @task_group(group_id="quality_report")
    def quality_report(reports: list[dict[str, Any]]):
        """A task group nests tasks under one collapsible node in the UI."""

        @task
        def summarize(reports: list[dict[str, Any]]) -> None:
            for report in reports:
                failed = [r["expectation"] for r in report["results"] if not r["passed"]]
                print(f"{report['suite']}: {len(report['results'])} checks, failed={failed or 'none'}")

        summarize(reports)

    projects_report = validate_projects()
    tags_report = validate_tags()
    extract_and_load_projects >> projects_report
    extract_and_load_tags >> tags_report

    # Fan-in: dbt joins the two tables, so it waits for both gates.
    curated_report = validate_transformed()
    [projects_report, tags_report] >> transform >> curated_report

    quality_report([projects_report, tags_report, curated_report])


dataops()
```
<!-- END FILE: solutions/03_dataops.py -->

</details>

<details>
<summary><b>solutions/04_mlops.py</b> — asset-triggered training, mapping, promotion gate (§10)</summary>

<!-- BEGIN FILE: solutions/04_mlops.py -->
```python
"""Lesson 4 - the MLOps Dag: asset-triggered tuning, training, and promotion.

The payoff of data-aware scheduling. This Dag has no cron. It runs when
`dataops` emits the `labeled_projects` asset event - which only happens when
the data passed validation.

Also shown:

* **A snapshot and a validation gate before training** - the warehouse table
  is copied once per run and split once, by a hash of each project's id, and
  those files are what gets validated and trained on, including the split,
  which no table-level suite can see.
* **Distributed tuning with dynamic task mapping** - one Optuna study, stored
  in a database, and one `run_trials` task instance per worker, fanned out at
  runtime. Every worker adds trials to the same study.
* **Experiment tracking** - the study is an MLflow run with one child run per
  trial, and the final model is a run of its own, registered as a new version.
* **Branching as a promotion gate** - a model that misses the f1 threshold, or
  scores below the production model on the test rows that model has never
  seen, does not reach production, and the Dag run still succeeds (a rejected
  candidate is a normal outcome, not a failure).
* **Human-in-the-loop** - `ApprovalOperator`: a model the gate lets through
  reaches production only when a person clicks Approve in the UI. `defaults` +
  `response_timeout` mean an unattended run still resolves instead of hanging.
* **XCom discipline** - tasks exchange paths, run ids and metric dicts. The
  models live in MLflow and the data in the warehouse and on disk.

Deadline alerts live in `07_deadline_alerts.py` - see the note there for why
they are not attached to this Dag.

Run it:
    airflow dags test mlops
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.sdk import Asset, Param, dag, task
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk.types import OutletEventAccessorsProtocol

from project_tagger import config, data, evaluate, registry, train as train_module, validate
from project_tagger_orchestration.assets import LABELED_PROJECTS, PRODUCTION_MODEL

DEFAULT_ARGS = {
    "owner": "ml-engineering",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=2),
}


def run_key(run_id: str) -> str:
    """A Dag run id made safe for file names and study names."""
    return run_id.replace(":", "-").replace("+", "-")


@dag(
    dag_id="mlops",
    # The whole point: schedule on data, not on time. Pass several assets for
    # AND semantics, or AssetAny(a, b) for OR.
    schedule=[LABELED_PROJECTS],
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "mlops"],
    doc_md=__doc__,
    # The tuning budget, overridable per run from the Trigger form.
    params={
        "n_trials": Param(20, type="integer", minimum=1, description="Optuna trials in total."),
        "n_workers": Param(4, type="integer", minimum=1, description="Tasks the trials are spread over."),
    },
)
def mlops():
    @task
    def prepare_data(
        *, triggering_asset_events: Mapping[Asset, Sequence[Any]], run_id: str
    ) -> dict[str, str]:
        """Inspect what triggered us, then pin and split the rows this run will use."""
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")

        config.ensure_dirs()
        # Writes data/snapshots/<key>/; returns the paths "snapshot", "train", "val", "test".
        paths = data.save_snapshot(data.load_labeled(), run_key(run_id))
        print(f"snapshot: {paths}")
        return paths

    @task
    def validate_training_data(paths: dict[str, str]) -> dict[str, Any]:
        """The gate between the warehouse and the trainer.

        `dataops` already validated `labeled_projects` - but that was a
        different Dag run, possibly hours ago, and this run may have been
        triggered by hand. What the model is fitted on is the snapshot,
        plus a split that no single-table suite can inspect, so both are
        checked here.
        """
        # The table as read from the warehouse, against the labeled_projects suite.
        df = data.load_snapshot(paths["snapshot"])
        table = validate.run_suite(df, "labeled_projects")
        # The three preprocessed splits (train, val, test), checked against each other.
        split = validate.check_split(*(data.load_snapshot(paths[name]) for name in data.SPLITS))
        print(f"{table.summary()} {split.summary()}")

        if not (table.passed and split.passed):
            # AirflowFailException, not AirflowException: a bad snapshot will
            # not become good by retrying the same task.
            raise AirflowFailException(
                f"Training snapshot failed validation: {table.failures + split.failures}"
            )
        return {
            "rows": len(df),
            "warnings": [r["expectation"] for r in table.failures + split.failures],
        }

    @task
    def create_study(*, run_id: str) -> str:
        """One Optuna study per Dag run. Safe to retry: an existing study is loaded."""
        return train_module.create_study(f"{config.REGISTERED_MODEL}-{run_key(run_id)}")

    @task
    def trial_budget(*, params: Mapping[str, Any]) -> list[int]:
        """Split the trials over the workers. Returning a list is what `.expand()` maps over."""
        base, extra = divmod(params["n_trials"], params["n_workers"])
        return [base + (1 if i < extra else 0) for i in range(params["n_workers"]) if base or i < extra]

    @task(max_active_tis_per_dag=2)
    def run_trials(n_trials: int, study_name: str, paths: dict[str, str]) -> list[dict[str, Any]]:
        """One task instance per worker; each adds `n_trials` trials to the shared study."""
        train_df, val_df = data.load_snapshot(paths["train"]), data.load_snapshot(paths["val"])
        results = train_module.run_trials(study_name, n_trials, train_df, val_df)
        print(f"{len(results)} trials, best here f1={max(r['f1'] for r in results):.4f}")
        return results

    @task
    def best_params(study_name: str, results: list[list[dict[str, Any]]]) -> dict[str, Any]:
        """The reduce step: every mapped instance's return value arrives as a list.

        The study already knows its best trial; `results` is taken so this task
        waits for every worker.
        """
        # Also records it on the study's run: metric best_f1, tag best_trial_run_id.
        best = train_module.best_trial(study_name)
        print(f"best trial #{best['number']} f1={best['f1']:.4f} of {best['num_trials']}: {best['params']}")
        return best

    @task
    def train_best(best: dict[str, Any], paths: dict[str, str], *, run_id: str) -> dict[str, Any]:
        """Fit on train + val, score on test, log and register in MLflow."""
        splits = {name: data.load_snapshot(paths[name]) for name in data.SPLITS}
        trained = train_module.train_model(
            best["params"], splits,
            run_name=f"final-{run_key(run_id)}",
            # Links the final run back to the Airflow run, the trial it took
            # its params from, and the files it was trained on.
            tags={
                "airflow_run_id": run_id,
                "best_trial_run_id": best["mlflow_run_id"],
                "snapshot_path": paths["snapshot"],
            },
        )
        version = registry.register(trained["model_uri"])
        print(f"run={trained['run_id']} version={version} f1={trained['metrics']['f1']:.4f}")
        # Small, JSON-serialisable payload. The model itself stays in MLflow.
        return {
            "run_id": trained["run_id"],
            "version": version,
            "f1": trained["metrics"]["f1"],
            "slices": trained["metrics"]["slices"],
        }

    @task
    def compare_with_champion(trained: dict[str, Any], paths: dict[str, str]) -> dict[str, Any] | None:
        """Both models on this run's test split, new rows and old. None if nothing is promoted."""
        champion = registry.get_production()
        if champion is None:
            print("no champion yet")
            return None
        # Every id the champion's training run had, in any split.
        seen = [i for ids in registry.load_split_ids(champion["run_id"]).values() for i in ids]
        comparison = evaluate.compare(
            registry.load_model(version=trained["version"]),
            registry.load_model(),
            data.load_snapshot(paths["test"]),
            seen,
        )
        for name, part in comparison.items():
            if part["rows"]:
                print(
                    f"{name} test rows: {part['rows']}, f1={part['f1']:.4f}, "
                    f"champion version {champion['version']} f1={part['champion_f1']:.4f}"
                )
        return comparison

    @task
    def summarize_candidate(trained: dict[str, Any], comparison: dict[str, Any] | None) -> str:
        """The numbers approve_promotion's prompt shows, as Markdown."""
        summary = evaluate.summarize(trained["version"], trained["f1"], trained["slices"], comparison)
        print(summary)
        return summary

    @task.branch
    def promotion_gate(trained: dict[str, Any], comparison: dict[str, Any] | None) -> str:
        """Return the task_id to run. The other branch is skipped, not failed."""
        promote, reason = evaluate.should_promote(
            trained["f1"],
            comparison,
            config.MIN_F1_TO_PROMOTE,
            config.MIN_NEW_TEST_ROWS,
        )
        print(reason)
        return "approve_promotion" if promote else "keep_current_model"

    # Human-in-the-loop gate. The task sits in `awaiting_input` and shows a
    # prompt in the UI; on Reject, promote_model is skipped.
    approve_promotion = ApprovalOperator(
        task_id="approve_promotion",
        subject="Promote the new tag classifier?",
        # `body` is a template field: the summary is pulled from XCom when the
        # task runs.
        body=(
            "The new version passed promotion_gate.\n\n"
            "{{ ti.xcom_pull(task_ids='summarize_candidate') }}\n\n"
            "Approve to move `champion` to it."
        ),
        # No answer within the window resolves to `defaults`, so an unattended
        # run still completes. Give it hours in production; kept short here so
        # the lesson's runs finish.
        defaults="Approve",
        response_timeout=timedelta(seconds=60),
    )

    @task(outlets=[PRODUCTION_MODEL])
    def promote_model(
        trained: dict[str, Any], *, outlet_events: OutletEventAccessorsProtocol
    ) -> dict[str, Any]:
        """Move the registry's `champion` alias to the new version."""
        champion = registry.promote(trained["version"])
        outlet_events[PRODUCTION_MODEL].extra = {
            "version": champion["version"],
            "f1": champion["metrics"]["f1"],
            "previous_version": champion["previous_version"],
        }
        print(f"champion -> version {champion['version']} (was {champion['previous_version']})")
        return champion

    keep_current_model = EmptyOperator(task_id="keep_current_model")

    # `none_failed_min_one_success` lets the join run whichever branch was taken;
    # the default `all_success` would skip it after any branch skip.
    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")

    paths = prepare_data()
    gate = validate_training_data(paths)
    study = create_study()
    results = run_trials.partial(study_name=study, paths=paths).expand(
        n_trials=trial_budget()
    )
    # No data flows from the gate to the trainers, only permission to run: every
    # mapped instance waits on it.
    gate >> results

    trained = train_best(best_params(study, results), paths)
    comparison = compare_with_champion(trained, paths)
    decision = promotion_gate(trained, comparison)
    # No data flows from the summary to the gate, only the order: the summary
    # exists before the gate can open the branch whose prompt shows it.
    summarize_candidate(trained, comparison) >> decision

    decision >> approve_promotion >> promote_model(trained) >> done
    decision >> keep_current_model >> done


mlops()
```
<!-- END FILE: solutions/04_mlops.py -->

</details>

<details>
<summary><b>solutions/05_sensors_and_events.py</b> — sensors, deferrable tasks, watchers (§11)</summary>

<!-- BEGIN FILE: solutions/05_sensors_and_events.py -->
```python
"""Lesson 5 - waiting for things: sensors, deferrable operators, and events.

Three ways to react to the outside world, cheapest last:

1. **Poking sensors** - a worker slot is held while polling. Fine for seconds.
2. **`reschedule` mode** - the task instance exits between pokes and is
   re-queued, so no slot is held while it waits.
3. **Deferrable sensors** (`deferrable=True`) - the task suspends, the slot is
   released, and the triggerer resumes it. This is what you want for anything
   measured in minutes or hours.

And one way not to wait at all: **event-driven scheduling** (`AssetWatcher`),
where an external event emits an asset update and *that* starts the Dag run.
See the note at the bottom.

The table to wait for is a Dag param, `table`, so a run can be pointed at one
that does not exist yet:

Run it:
    airflow dags test sensors_and_events       # labeled_projects already exists
    airflow dags test sensors_and_events -c '{"table": "labeled_projects_copy"}'
                                               # waits until you create that table
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.sdk import Param, PokeReturnValue, dag, task

from project_tagger import config, data

DEFAULT_ARGS = {"owner": "data-engineering", "retries": 0}


@dag(
    dag_id="sensors_and_events",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "sensors"],
    doc_md=__doc__,
    params={"table": Param(config.LABELED_PROJECTS_TABLE, type="string", description="Table to wait for.")},
)
def sensors_and_events():
    # Deferrable: releases its worker slot while waiting. Needs `airflow triggerer`.
    settle = TimeDeltaSensor(task_id="settle", delta=timedelta(seconds=5), deferrable=True)

    # Deferrable too: the triggerer asks BigQuery whether the table exists,
    # through the `google_cloud_default` connection, every `poke_interval`.
    wait_for_table = BigQueryTableExistenceSensor(
        task_id="wait_for_table",
        project_id=config.GCP_PROJECT_ID,
        dataset_id=config.BQ_DATASET,
        table_id="{{ params.table }}",
        deferrable=True,
        poke_interval=10,
        timeout=60 * 5,
    )

    @task.sensor(poke_interval=10, timeout=120, mode="reschedule")
    def enough_rows(*, params: dict[str, Any]) -> PokeReturnValue:
        """A sensor written as a function.

        Return `PokeReturnValue(is_done=..., xcom_value=...)` to both stop poking
        and hand a value to downstream tasks.
        """
        from google.api_core.exceptions import NotFound

        try:
            rows = data.count_rows(params["table"])  # SELECT COUNT(*) in BigQuery
        except NotFound:
            return PokeReturnValue(is_done=False)
        print(f"{params['table']} has {rows} rows")
        return PokeReturnValue(is_done=rows >= 500, xcom_value=rows)

    @task
    def proceed(rows: int) -> None:
        print(f"Data is ready ({rows} rows). Downstream work can start.")

    settle >> wait_for_table >> proceed(enough_rows())


sensors_and_events()


# --- Event-driven scheduling -------------------------------------------------
#
# Instead of a task that waits, attach a watcher to an Asset. The trigger runs in
# the triggerer; when it fires, the asset updates and every Dag scheduled on that
# asset starts. Nothing occupies a worker slot in the meantime.
#
#     from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
#     from airflow.sdk import Asset, AssetWatcher, dag
#
#     trigger = MessageQueueTrigger(queue="https://sqs.us-east-1.amazonaws.com/0123456789/new-projects")
#     new_projects = Asset(
#         name="new_projects",
#         watchers=[AssetWatcher(name="sqs_watcher", trigger=trigger)],
#     )
#
#     @dag(schedule=[new_projects], ...)
#     def ingest_on_message():
#         ...
#
# Only triggers deriving from `BaseEventTrigger` can be used as watchers -
# a trigger that tests a permanent condition ("table exists") would re-fire
# forever.
```
<!-- END FILE: solutions/05_sensors_and_events.py -->

</details>

<details>
<summary><b>solutions/06_monitoring_and_retraining.py</b> — drift, branching, human-in-the-loop (§12)</summary>

<!-- BEGIN FILE: solutions/06_monitoring_and_retraining.py -->
```python
"""Lesson 6 - continual learning: monitor, decide, and act.

A monitoring event has four sensible responses, and this Dag makes them
explicit branches:

    continue  keep serving; log the alert and move on
    improve   retrain on new data (triggers the mlops Dag)
    inspect   the label space or schema changed - a human should look
    rollback  live performance collapsed - revert to the previous model

Every measurement is of the production model on rows it has never seen: the
rows that reached the warehouse after it was trained.

Pieces on show:

* `@task.external_python` - the drift checks run alibi-detect, which needs
  libraries Airflow's own environment cannot have, so they run in a separate
  virtualenv baked into the image.
* `@task.branch` picking one arm of the response.
* `TriggerDagRunOperator` for cross-Dag orchestration. Retraining needs no
  approval here: what reaches production is approved in `mlops`.

Run it:
    airflow dags test monitoring
"""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task
from airflow.sdk.exceptions import AirflowSkipException
from airflow.sdk.types import OutletEventAccessorsProtocol

from project_tagger import config, monitoring, registry
from project_tagger_orchestration.assets import PRODUCTION_MODEL

DEFAULT_ARGS = {"owner": "ml-engineering", "retries": 1}

# Thresholds that turn a measurement into a decision. Exposed as Dag params so
# the policy is visible in the UI, overridable per run, and recorded with the
# run that used it:
#     airflow dags test monitoring --conf '{"drift_p_val": 1.0}'
POLICY_PARAMS = {
    "drift_p_val": Param(
        0.05, type="number", description="A drift test with a p-value below this reports drift."
    ),
    "max_f1_drop": Param(0.10, type="number", description="f1 drop that triggers retraining."),
    "critical_f1_drop": Param(0.25, type="number", description="f1 drop that triggers rollback."),
}


@dag(
    dag_id="monitoring",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "mlops", "monitoring"],
    doc_md=__doc__,
    params=POLICY_PARAMS,
)
def monitoring_dag():
    @task
    def prepare_windows(*, run_id: str) -> dict[str, Any]:
        """Read the warehouse once and write the two windows drift is measured on."""
        key = run_id.replace(":", "-").replace("+", "-")
        # Writes reference.parquet and current.parquet; returns their paths, num_current
        # and version (the champion's). With no champion, or fewer than
        # config.MIN_MONITOR_ROWS current rows, it writes nothing and the paths are None.
        windows = monitoring.prepare_windows(f"monitoring-{key}")
        print(f"windows: {windows}")
        return windows

    # Runs with alibi-detect's interpreter, not Airflow's. The function body is
    # shipped to that interpreter, so everything it uses is imported inside it,
    # and its arguments and return value must be plain data. `expect_airflow=False`:
    # that virtualenv has no Airflow, and the function needs none.
    @task.external_python(python=config.ALIBI_PYTHON, expect_airflow=False)
    def detect_drift(windows: dict, p_val: str) -> dict | None:
        from project_tagger import drift

        # label_drift and length_drift (each with is_drift, p_val, distance), new_labels
        # and both tag distributions; None when there are no files.
        return drift.detect_drift(windows["reference"], windows["current"], float(p_val))

    @task
    def check_performance(windows: dict[str, Any]) -> dict[str, Any] | None:
        # The champion's f1_at_promotion, and its f1_now on the current window;
        # None when there is no window.
        performance = monitoring.production_performance(windows["current"])
        if performance:
            print(f"f1 at promotion={performance['f1_at_promotion']:.4f} now={performance['f1_now']:.4f}")
        return performance

    @task.branch
    def decide(
        windows: dict[str, Any],
        drift: dict[str, Any] | None,
        performance: dict[str, Any] | None,
        *,
        params: dict[str, Any],
    ) -> str:
        if windows["version"] is None:
            print("No production model yet - train one.")
            return "trigger_retraining"
        if drift is None or performance is None:
            print(
                f"only {windows['num_current']} new rows since version {windows['version']} "
                "was trained - nothing to measure"
            )
            return "continue_serving"

        label, length = drift["label_drift"], drift["length_drift"]
        print(
            f"label drift p={label['p_val']:.4g} length drift p={length['p_val']:.4g} "
            f"new_labels={drift['new_labels']} policy={dict(params)}"
        )
        f1_drop = performance["f1_at_promotion"] - performance["f1_now"]
        print(f"f1_drop={f1_drop:.4f}")

        if f1_drop >= params["critical_f1_drop"]:
            return "rollback"
        if drift["new_labels"]:
            # An unseen label means the schema/label space moved. Retraining on
            # data you do not understand yet is how you ship a worse model.
            return "inspect"
        if (
            f1_drop >= params["max_f1_drop"]                    # concept drift
            or label["p_val"] < params["drift_p_val"]           # target drift
            or length["p_val"] < params["drift_p_val"]          # data drift
        ):
            return "trigger_retraining"
        return "continue_serving"

    continue_serving = EmptyOperator(task_id="continue_serving")

    @task
    def inspect(drift: dict[str, Any]) -> None:
        print("MANUAL INSPECTION NEEDED")
        print(f"  new labels: {drift['new_labels']}")
        print(f"  current distribution: {drift['current_distribution']}")

    # Moving `champion` changes the production model as much as a promotion
    # does, so this task emits the same asset as `promote_model` in mlops.
    @task(outlets=[PRODUCTION_MODEL])
    def rollback(
        performance: dict[str, Any], *, outlet_events: OutletEventAccessorsProtocol
    ) -> dict[str, Any]:
        reason = f"f1 fell from {performance['f1_at_promotion']:.4f} to {performance['f1_now']:.4f}"
        champion = registry.rollback(reason)
        if champion is None:
            # A skipped task emits no asset event: production did not change.
            raise AirflowSkipException("no previous_champion - nothing to roll back to")
        outlet_events[PRODUCTION_MODEL].extra = {
            "version": champion["version"],
            "rolled_back_version": champion["rolled_back_version"],
            "reason": "rollback",
        }
        print(f"champion -> version {champion['version']} (retired {champion['rolled_back_version']}: {reason})")
        return champion

    trigger_retraining = TriggerDagRunOperator(
        task_id="trigger_retraining",
        trigger_dag_id="mlops",
        conf={"reason": "monitoring_drift"},
        wait_for_completion=False,
        reset_dag_run=True,
    )

    windows = prepare_windows()
    drift = detect_drift(windows, "{{ params.drift_p_val }}")
    performance = check_performance(windows)
    branch = decide(windows, drift, performance)

    branch >> continue_serving
    branch >> inspect(drift)
    branch >> rollback(performance)
    branch >> trigger_retraining


monitoring_dag()
```
<!-- END FILE: solutions/06_monitoring_and_retraining.py -->

</details>

<details>
<summary><b>solutions/07_deadline_alerts.py</b> — deadline alerts, and a Dag that misses one (§13)</summary>

<!-- BEGIN FILE: solutions/07_deadline_alerts.py -->
```python
"""Lesson 7 - Deadline Alerts: "tell me if this run is taking too long".

A deadline is attached to the **Dag**, applies to the whole run, and is
evaluated *while the run is still going* - so you hear about a stuck pipeline
during the incident, not in the morning report.

Three mandatory pieces: a **reference** (where the clock starts), an
**interval** (how long after it), and a **callback**.

    DeadlineReference.DAGRUN_QUEUED_AT       when the run was queued
    DeadlineReference.DAGRUN_LOGICAL_DATE    the run's logical date
    DeadlineReference.FIXED_DATETIME(dt)     an absolute wall-clock time
    DeadlineReference.AVERAGE_RUNTIME(...)   this Dag's own recent average

    SyncCallback   runs on the executor
    AsyncCallback  runs on the triggerer - use this for Notifiers
                   (e.g. AsyncCallback(SlackWebhookNotifier, kwargs={...}))

This Dag deliberately misses its own 30-second deadline, so the callback in
`project_tagger/callbacks.py` fires and you can find it in the scheduler/executor logs.

    docker compose logs -f airflow-scheduler | grep "DEADLINE MISSED"

AIRFLOW 3.3.1 BUG (the version this repo installs): `airflow dags test <dag_id>`
crashes on any Dag that sets `deadline=`, with
`psycopg2.ProgrammingError: can't adapt type 'dict'` raised from
`DagRun.update_state`. The tasks themselves run to completion first - only the
CLI's final state update fails. Trigger this Dag through the scheduler instead:

    airflow dags unpause deadline_alerts
    airflow dags trigger deadline_alerts
"""

from __future__ import annotations

import time
from datetime import timedelta

import pendulum
from airflow.sdk import DeadlineAlert, DeadlineReference, SyncCallback, dag, task


@dag(
    dag_id="deadline_alerts",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "ml-engineering"},
    tags=["tutorial", "monitoring"],
    doc_md=__doc__,
    # Pass a list to attach several deadlines with different budgets.
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(seconds=30),
        callback=SyncCallback(
            "project_tagger.callbacks.deadline_missed",
            kwargs={"dag_id": "deadline_alerts", "budget": "30s"},
        ),
    ),
)
def deadline_alerts():
    @task
    def slow_step() -> str:
        """Takes longer than the deadline on purpose."""
        time.sleep(75)
        return "finished, but late"

    slow_step()


deadline_alerts()
```
<!-- END FILE: solutions/07_deadline_alerts.py -->

</details>

`src/project_tagger/` — `data.py`, `validate.py`, `train.py`,
`evaluate.py`, `registry.py`, `monitoring.py`, `predict.py`, `config.py`,
`callbacks.py` — is the ML project being orchestrated, not the orchestration, and
imports no Airflow. It is in the repo; it is not reproduced here.
