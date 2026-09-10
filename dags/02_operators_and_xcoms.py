"""Lesson 2 - operators, XComs, and the import moves that break Airflow 2 Dags.

Two things bite everyone porting a Dag to Airflow 3:

1. `BashOperator`, `PythonOperator`, `EmptyOperator`, `FileSensor` and friends
   left airflow-core. They now live in `apache-airflow-providers-standard`
   (pre-installed, but the import path changed):

       Airflow 2: from airflow.operators.bash   import BashOperator
       Airflow 3: from airflow.providers.standard.operators.bash import BashOperator

2. `xcom_pull()` without `task_ids` now pulls only from the *current* task.
   In Airflow 2 it silently searched the whole Dag run and handed back the most
   recent push. Any Dag relying on that returns None here - loudly wrong instead
   of quietly wrong.

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
    """Classic operator style: push an explicit key."""
    ti.xcom_push(key="seed", value=7)


def _multiply(ti, factor: int) -> None:
    # REQUIRED in Airflow 3: name the task you are pulling from.
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

    # Templated fields still work the same way; only the variable names changed.
    # `ds` and `ts` survive, `execution_date`/`next_ds`/`tomorrow_ds` do not.
    #
    # But `ds` is derived from `logical_date`, and a manual or asset-triggered
    # run in Airflow 3 has none - the variable is then *undefined*, and
    # `{{ ds }}` fails the task with `UndefinedError: 'ds' is undefined`. Note
    # that `airflow dags test` always supplies a logical date, so this only
    # breaks once a real trigger reaches it. Guard it with `default`, or
    # template `{{ dag_run.run_after }}`, which every run has.
    describe = BashOperator(
        task_id="describe",
        bash_command=(
            'echo "run={{ run_id }} ds={{ ds | default(\'none (manual run)\') }} '
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
    # in the graph view. Only `chain` can carry a Label - `>>` cannot.
    chain(start, push_seed, Label("seed -> product"), multiply, describe)

    # `>>` between single tasks. TaskFlow would have drawn this edge implicitly
    # if `describe` returned the value the reader needs; it does not, so the
    # dependency is declared by hand.
    product = taskflow_reader()
    describe >> product

    check_parity = BashOperator(task_id="check_parity", bash_command='echo "parity checked"')
    check_range = BashOperator(task_id="check_range", bash_command='echo "range checked"')

    # Fan-out and fan-in on one line: a list on either side of `>>` broadcasts
    # the edge, so this is four dependencies, not two.
    product >> [check_parity, check_range] >> EmptyOperator(task_id="checks_done")

    publish = EmptyOperator(task_id="publish")
    notify = EmptyOperator(task_id="notify")

    # All-to-all: every check upstream of every sink, four more edges.
    # `chain([a, b], [c, d])` would pair them element-wise instead (a->c, b->d,
    # and it raises if the lists differ in length), and `[a, b] >> [c, d]` is a
    # TypeError - Python has no list-to-list operator to dispatch on.
    cross_downstream([check_parity, check_range], [publish, notify])


operators_and_xcoms()
