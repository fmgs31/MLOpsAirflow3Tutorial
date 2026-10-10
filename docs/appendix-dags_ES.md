# Apéndice: los Dags terminados, completos

> Parte del [tutorial de Airflow 3](../README_ES.md), sacado de él para que el
> tutorial siga siendo legible. `§N` se refiere a una sección numerada de ese
> README.

Las lecciones del tutorial te dan cada archivo por trozos. Aquí está cada uno de
principio a fin, en orden de lectura, tal y como está en
[`solutions/`](../solutions/) — el mismo código que tecleaste, más los
comentarios, que explican los detalles en el punto en que importan. El código y sus
comentarios se dejan en inglés: son los archivos que de verdad se ejecutan.

> Estos listados se **inyectan desde los archivos** con
> [`scripts/sync_readme.py`](../scripts/sync_readme.py) y se verifican con
> [`tests/test_readme_listings.py`](../tests/test_readme_listings.py): edita una
> solución sin volver a ejecutar el script y la suite de tests falla. No pueden
> desincronizarse del código que realmente corre.

Las definiciones compartidas de Assets van primero, porque todos los Dags de
abajo las importan.
<details>
<summary><b>src/tagifai/assets.py</b> — los Assets que los Dags producen y consumen (§8)</summary>

<!-- BEGIN FILE: src/tagifai/assets.py -->
```python
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
```
<!-- END FILE: src/tagifai/assets.py -->

</details>

<details>
<summary><b>solutions/01_hello_airflow3.py</b> — un Dag, tasks, dependencias, params (§4)</summary>

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
    def make_greeting(**context) -> str:
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
<summary><b>solutions/02_operators_and_xcoms.py</b> — operators, XComs, relaciones (§6)</summary>

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

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Label, chain, cross_downstream, dag, task

DEFAULT_ARGS = {
    "owner": "mlops-course",
    "retries": 1,
    "retry_delay": pendulum.duration(seconds=30),
}


def _push_seed(ti) -> None:
    """Operator style: push an XCom under an explicit key.

    `ti` (the task instance) is injected because the parameter is named `ti` -
    the same name-based injection `**context` does in lesson 1.
    """
    ti.xcom_push(key="seed", value=7)


def _multiply(ti, factor: int) -> None:
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
    def taskflow_reader(**context) -> int:
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
<summary><b>solutions/03_dataops.py</b> — extract → validate → transform → validate, y el outlet (§9)</summary>

<!-- BEGIN FILE: solutions/03_dataops.py -->
```python
"""Lesson 3 - the DataOps Dag: extract -> validate -> transform -> validate.

The first Dag that does real work. Every task is a slot you can swap for the
vendor tool of a production stack, without changing the Dag's shape:

    extract_projects   -> AirbyteTriggerSyncOperator / FivetranOperator / an EL job
    validate_*         -> GreatExpectationsOperator / SodaOperator / dbt test
    transform          -> DbtCloudRunJobOperator / astronomer-cosmos task group

Three things to watch for:

* The Dag **produces Assets** (`outlets=[...]`). Finishing the producing task
  emits an asset event, and that event is what schedules the MLOps Dag - no
  cron guessing, no sensor, no polling.
* Asset events carry metadata (`outlet_events[asset].extra`), so the consumer
  can see the row count that triggered it.
* Validation results move through XCom as small dicts; the dataframes never do.
  Tasks exchange paths and counts, not payloads.

Run it:
    airflow dags test dataops
"""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.sdk import dag, task, task_group

# Task code imports exceptions from the Task SDK. It is the namespace a remote
# worker has, and the one to reach for in a task.
from airflow.sdk.exceptions import AirflowFailException

from tagifai import config, data, validate
from tagifai.assets import LABELED_PROJECTS, RAW_PROJECTS

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
    @task(outlets=[RAW_PROJECTS])
    def extract_projects(*, outlet_events) -> str:
        """Land the source data. Returns a path, not a dataframe."""
        config.ensure_dirs()
        fp = data.extract()
        df = data.load_raw(fp)
        outlet_events[RAW_PROJECTS].extra = {"rows": len(df), "path": str(fp)}
        return str(fp)

    @task
    def validate_raw(fp: str) -> dict[str, Any]:
        """Quality gate on the landing zone."""
        result = validate.run_suite(data.load_raw(fp), "raw_projects")
        print(result.summary())
        if not result.passed:
            # AirflowFailException stops retries: a data-quality break will not
            # fix itself by running the same task again.
            raise AirflowFailException(f"Raw data failed validation: {result.failures}")
        return {"suite": result.suite, "results": result.results}

    @task
    def transform(fp: str) -> str:
        """Raw -> curated `labeled_projects` - the dbt model of a real stack, in pandas."""
        df = data.transform(data.load_raw(fp))
        out = data.save_labeled(df)
        print(f"Wrote {len(df)} rows to {out}")
        return str(out)

    @task(outlets=[LABELED_PROJECTS])
    def validate_transformed(fp: str, *, outlet_events) -> dict[str, Any]:
        """Final gate. Only a passing run emits the asset event.

        Placing the outlet on the *validation* task, not the transform task, is
        the whole trick: downstream Dags are then scheduled on
        "good data arrived", never on "a task finished".
        """
        df = data.load_labeled(fp)
        result = validate.run_suite(df, "labeled_projects")
        print(result.summary())
        if not result.passed:
            raise AirflowFailException(f"Labeled data failed validation: {result.failures}")

        outlet_events[LABELED_PROJECTS].extra = {
            "rows": len(df),
            "tags": sorted(df["tag"].unique().tolist()),
            "path": fp,
            "warnings": [r["expectation"] for r in result.failures],
        }
        return {"suite": result.suite, "rows": len(df), "results": result.results}

    @task_group(group_id="quality_report")
    def quality_report(raw_report: dict[str, Any], curated_report: dict[str, Any]):
        """A task group nests tasks under one collapsible node in the UI."""

        @task
        def summarize(raw: dict[str, Any], curated: dict[str, Any]) -> None:
            for report in (raw, curated):
                failed = [r["expectation"] for r in report["results"] if not r["passed"]]
                print(f"{report['suite']}: {len(report['results'])} checks, failed={failed or 'none'}")

        summarize(raw_report, curated_report)

    raw_fp = extract_projects()
    raw_report = validate_raw(raw_fp)

    # `transform` waits on the raw gate even though it does not use its output.
    curated_fp = transform(raw_fp)
    raw_report >> curated_fp

    curated_report = validate_transformed(curated_fp)
    quality_report(raw_report, curated_report)


dataops()
```
<!-- END FILE: solutions/03_dataops.py -->

</details>

<details>
<summary><b>solutions/04_mlops.py</b> — entrenamiento disparado por asset, mapping, promotion gate (§10)</summary>

<!-- BEGIN FILE: solutions/04_mlops.py -->
```python
"""Lesson 4 - the MLOps Dag: asset-triggered training, tuning, and promotion.

The payoff of data-aware scheduling. This Dag has no cron. It runs when
`dataops` emits the `labeled_projects` asset event - which only happens when
the data passed validation.

Also shown:

* **A validation gate before training** - `dataops` validated the warehouse
  table; this validates the *training snapshot*, including the split, which no
  table-level suite can see.
* **Dynamic task mapping** - one `train_candidate` task instance per
  hyperparameter set, fanned out at runtime, then reduced by `select_best`.
* **Branching as a promotion gate** - a model that misses the f1 threshold does
  not reach production, and the Dag run still succeeds (a rejected candidate is
  a normal outcome, not a failure).
* **XCom discipline** - tasks exchange run ids and metric dicts. The models and
  dataframes live in the registry and on disk.

Deadline alerts live in `07_deadline_alerts.py` - see the note there for why
they are not attached to this Dag.

Run it:
    airflow dags test mlops
"""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException

from tagifai import config, data, registry, train as train_module, validate
from tagifai.assets import LABELED_PROJECTS, PRODUCTION_MODEL

DEFAULT_ARGS = {
    "owner": "ml-engineering",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=2),
}


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
)
def mlops():
    @task
    def prepare_data(*, triggering_asset_events) -> dict[str, int]:
        """Inspect what triggered us, then materialise the train/test split."""
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")

        df = data.load_labeled()
        train_df, test_df = data.split(df)
        return {"num_train": len(train_df), "num_test": len(test_df)}

    @task
    def validate_training_data(split_sizes: dict[str, int]) -> dict[str, Any]:
        """The gate between the warehouse and the trainer.

        `dataops` already validated `labeled_projects` - but that was a
        different Dag run, possibly hours ago, and this run may have been
        triggered by hand. What the model is fitted on is the snapshot on disk
        *now*, plus a split that no single-frame suite can inspect, so both are
        checked here.

        The dataframe is re-read rather than passed from `prepare_data`: XComs
        carry metadata, never payloads.
        """
        df = data.load_labeled()
        table = validate.run_suite(df, "labeled_projects")
        split = validate.check_split(*data.split(df))
        print(f"{table.summary()} {split.summary()}")

        if not (table.passed and split.passed):
            # AirflowFailException, not AirflowException: a bad snapshot will
            # not become good by retrying the same task.
            raise AirflowFailException(
                f"Training snapshot failed validation: {table.failures + split.failures}"
            )
        return {
            "rows": len(df),
            "split_sizes": split_sizes,
            "warnings": [r["expectation"] for r in table.failures + split.failures],
            "results": table.results + split.results,
        }

    @task
    def candidates() -> list[dict[str, Any]]:
        """The search space. Returning a list is what `.expand()` maps over."""
        return train_module.SEARCH_SPACE

    @task(max_active_tis_per_dag=2)
    def train_candidate(hparams: dict[str, Any], **context) -> dict[str, Any]:
        """One task instance per hyperparameter set.

        The argument is named `hparams`, not `params`: TaskFlow injects context
        keys by parameter name, and `params` is a reserved context key.
        """
        config.ensure_dirs()
        run_id = f"{context['run_id']}__{context['ti'].map_index}".replace(":", "-").replace("+", "-")
        model, metrics = train_module.train_from_files(hparams)
        registry.save_run(run_id, model, metrics)
        print(f"run={run_id} hparams={hparams} f1={metrics['f1']:.4f}")
        # Small, JSON-serialisable payload. The model itself stays in the registry.
        return {"run_id": run_id, "f1": metrics["f1"], "hparams": hparams}

    @task
    def select_best(results: list[dict[str, Any]]) -> dict[str, Any]:
        """The reduce step: every mapped instance's return value arrives as a list."""
        best = max(results, key=lambda r: r["f1"])
        print(f"best={best['run_id']} f1={best['f1']:.4f} of {len(results)} candidates")
        return best

    @task
    def evaluate_best(best: dict[str, Any]) -> dict[str, Any]:
        """Re-read the full metrics, including per-class and slice performance."""
        metrics = registry.load_metrics(best["run_id"])
        for name, slice_metrics in metrics.get("slices", {}).items():
            print(f"slice {name}: f1={slice_metrics['f1']:.4f} n={slice_metrics['support']}")
        return {"run_id": best["run_id"], "metrics": metrics}

    @task.branch
    def promotion_gate(evaluation: dict[str, Any]) -> str:
        """Return the task_id to run. The other branch is skipped, not failed."""
        f1 = evaluation["metrics"]["f1"]
        threshold = config.MIN_F1_TO_PROMOTE
        current = registry.get_production()
        current_f1 = current["metrics"]["f1"] if current else 0.0

        if f1 < threshold:
            print(f"f1={f1:.4f} below threshold {threshold} - not promoting")
            return "keep_current_model"
        if f1 < current_f1:
            print(f"f1={f1:.4f} below production {current_f1:.4f} - not promoting")
            return "keep_current_model"
        return "promote_model"

    @task(outlets=[PRODUCTION_MODEL])
    def promote_model(evaluation: dict[str, Any], *, outlet_events) -> dict[str, Any]:
        pointer = registry.promote(evaluation["run_id"])
        outlet_events[PRODUCTION_MODEL].extra = {
            "run_id": pointer["run_id"],
            "f1": pointer["metrics"]["f1"],
            "previous_run_id": pointer["previous_run_id"],
        }
        print(f"promoted {pointer['run_id']} (was {pointer['previous_run_id']})")
        return pointer

    keep_current_model = EmptyOperator(task_id="keep_current_model")

    # `none_failed_min_one_success` lets the join run whichever branch was taken;
    # the default `all_success` would skip it after any branch skip.
    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")

    snapshot = validate_training_data(prepare_data())
    results = train_candidate.expand(hparams=candidates())
    # No data flows from the gate to the trainer, only permission to run: every
    # mapped instance waits on it.
    snapshot >> results

    evaluation = evaluate_best(select_best(results))
    gate = promotion_gate(evaluation)

    gate >> promote_model(evaluation) >> done
    gate >> keep_current_model >> done


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
2. **Deferrable sensors** (`*Async`, or `deferrable=True`) - the task suspends,
   the slot is released, and the triggerer resumes it. This is what you want for
   anything measured in minutes or hours.
3. **Event-driven scheduling** (`AssetWatcher`) - no task waits at all: an
   external event emits an asset update and *that* starts the Dag run. See the
   note at the bottom.

Run it:
    airflow dags test sensors_and_events       # everything it waits for is already true
    airflow dags trigger sensors_and_events    # through the scheduler (unpause first):
                                               # `settle` visibly sits in `deferred`

`dags test` never defers `settle`: the sensor's five-second target is measured
from the run's logical date, which the command fixes before the task starts, so
the moment has passed by the time the task runs and it returns at once. Only a
scheduler-driven run shows the `deferred` state.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensorAsync
from airflow.sdk import PokeReturnValue, dag, task

from tagifai import config, data

DEFAULT_ARGS = {"owner": "data-engineering", "retries": 0}


@dag(
    dag_id="sensors_and_events",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "sensors"],
    doc_md=__doc__,
)
def sensors_and_events():
    # Deferrable: releases its worker slot while waiting. Needs `airflow triggerer`,
    # and a scheduler-driven run to see it - `dags test` returns at once (docstring).
    settle = TimeDeltaSensorAsync(task_id="settle", delta=timedelta(seconds=5))

    # `reschedule` mode is the non-deferrable way to free a slot between pokes:
    # the task instance exits and is re-queued each interval.
    wait_for_curated_data = FileSensor(
        task_id="wait_for_curated_data",
        filepath=str(config.LABELED_PROJECTS_FP),
        poke_interval=10,
        timeout=60 * 5,
        mode="reschedule",
    )

    @task.sensor(poke_interval=10, timeout=120, mode="reschedule")
    def enough_rows() -> PokeReturnValue:
        """A sensor written as a function.

        Return `PokeReturnValue(is_done=..., xcom_value=...)` to both stop poking
        and hand a value to downstream tasks.
        """
        if not config.LABELED_PROJECTS_FP.exists():
            return PokeReturnValue(is_done=False)
        # Read it as a table: descriptions contain newlines, so counting lines
        # would overstate the row count.
        rows = len(data.load_labeled())
        print(f"curated table has {rows} rows")
        return PokeReturnValue(is_done=rows >= 500, xcom_value=rows)

    @task
    def proceed(rows: int) -> None:
        print(f"Data is ready ({rows} rows). Downstream work can start.")

    settle >> wait_for_curated_data >> proceed(enough_rows())


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
# a trigger that tests a permanent condition ("file exists") would re-fire
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

Pieces on show:

* `@task.branch` picking one arm of the response.
* `ApprovalOperator` (human-in-the-loop): retraining waits for a person to
  click Approve in the UI. `defaults` + `response_timeout` mean an unattended
  run still resolves instead of hanging forever.
* `TriggerDagRunOperator` for cross-Dag orchestration.

Run it:
    airflow dags test monitoring
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task

from tagifai import monitoring, registry

DEFAULT_ARGS = {"owner": "ml-engineering", "retries": 1}

# Thresholds that turn a measurement into a decision. Exposed as Dag params so
# the policy is visible in the UI, overridable per run, and recorded with the
# run that used it:
#     airflow dags test monitoring --conf '{"max_label_tvd": 0.0}'
POLICY_PARAMS = {
    "max_label_tvd": Param(0.15, type="number", description="Label-drift budget."),
    "max_length_shift": Param(
        0.25, type="number", description="Input-drift budget: relative change in mean text length."
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
    def measure() -> dict[str, Any]:
        report = monitoring.monitor()
        drift = report["drift"]
        print(
            f"label TVD={drift['label_tvd']:.4f} new_labels={drift['new_labels']} "
            f"length shift={drift['mean_length_shift']:+.4f}"
        )
        if report["performance"]:
            perf = report["performance"]
            print(f"f1 at promotion={perf['f1_at_promotion']:.4f} now={perf['f1_now']:.4f}")
        return report

    @task.branch
    def decide(report: dict[str, Any], **context) -> str:
        drift, performance = report["drift"], report["performance"]
        policy = context["params"]

        if performance is None:
            print("No production model yet - train one.")
            return "request_retraining"

        f1_drop = performance["f1_at_promotion"] - performance["f1_now"]
        print(f"f1_drop={f1_drop:.4f} label_tvd={drift['label_tvd']:.4f} policy={dict(policy)}")

        if f1_drop >= policy["critical_f1_drop"]:
            return "rollback"
        if drift["new_labels"]:
            # An unseen label means the schema/label space moved. Retraining on
            # data you do not understand yet is how you ship a worse model.
            return "inspect"
        if (
            f1_drop >= policy["max_f1_drop"]                                 # concept drift
            or drift["label_tvd"] >= policy["max_label_tvd"]                 # target drift
            or abs(drift["mean_length_shift"]) >= policy["max_length_shift"] # data drift
        ):
            return "request_retraining"
        return "continue_serving"

    continue_serving = EmptyOperator(task_id="continue_serving")

    @task
    def inspect(report: dict[str, Any]) -> None:
        print("MANUAL INSPECTION NEEDED")
        print(f"  new labels: {report['drift']['new_labels']}")
        print(f"  current distribution: {report['drift']['current_distribution']}")

    @task
    def rollback() -> dict[str, Any] | None:
        pointer = registry.rollback()
        print(f"rolled back to {pointer['run_id']}" if pointer else "nothing to roll back")
        return pointer

    # Human-in-the-loop gate. The task sits in `awaiting_input` and shows a
    # prompt in the UI; on Reject, everything downstream is skipped.
    request_retraining = ApprovalOperator(
        task_id="request_retraining",
        subject="Retrain the tag classifier?",
        body=(
            "Monitoring detected drift or a performance drop.\n\n"
            "Approve to trigger the `mlops` Dag on the current data."
        ),
        # No answer within the window resolves to `defaults`, so an unattended
        # run still completes. Give it hours in production; kept short here so
        # `airflow dags test monitoring` finishes.
        defaults="Approve",
        response_timeout=timedelta(seconds=60),
    )

    trigger_retraining = TriggerDagRunOperator(
        task_id="trigger_retraining",
        trigger_dag_id="mlops",
        conf={"reason": "monitoring_drift"},
        wait_for_completion=False,
        reset_dag_run=True,
    )

    report = measure()
    branch = decide(report)

    branch >> continue_serving
    branch >> inspect(report)
    branch >> rollback()
    branch >> request_retraining >> trigger_retraining


monitoring_dag()
```
<!-- END FILE: solutions/06_monitoring_and_retraining.py -->

</details>

<details>
<summary><b>solutions/07_deadline_alerts.py</b> — deadline alerts, y un Dag que se pasa uno (§13)</summary>

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
`tagifai/callbacks.py` fires and you can find it in the scheduler/executor logs.

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
            "tagifai.callbacks.deadline_missed",
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

El resto de `src/tagifai/` — `data.py`, `validate.py`, `train.py`,
`evaluate.py`, `registry.py`, `monitoring.py`, `predict.py`, `config.py`,
`callbacks.py` — es el proyecto de ML que se orquesta, no la orquestación, y no
importa Airflow. Está en el repo; aquí no se reproduce.
