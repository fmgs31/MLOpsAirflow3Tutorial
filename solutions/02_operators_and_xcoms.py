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
    # Careful with `ds`: it is derived from `logical_date`, and a manual or
    # asset-triggered run has none. The variable is then *undefined* and the
    # task fails with `UndefinedError: 'ds' is undefined` while rendering.
    # `airflow dags test` always supplies a logical date, so this only shows up
    # once a real trigger reaches it. Guard it with `default`, or template
    # `{{ dag_run.run_after }}`, which every run has.
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
