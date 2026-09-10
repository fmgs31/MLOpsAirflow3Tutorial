"""Lesson 1 - a Dag, tasks, and dependencies in Airflow 3.

Everything a Dag author needs now comes from one place: `airflow.sdk`. That is
the Task SDK, and it is the *only* supported authoring surface in Airflow 3 -
`airflow.decorators`, `airflow.models.dag`, `airflow.operators.*` are gone or
deprecated.

Run it without a scheduler:
    airflow dags test hello_airflow3
"""

from __future__ import annotations

import pendulum

# Airflow 2:  from airflow.decorators import dag, task
# Airflow 3:
from airflow.sdk import Param, dag, task

DEFAULT_ARGS = {
    "owner": "mlops-course",
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=30),
}


@dag(
    dag_id="hello_airflow3",
    # Airflow 2 called this `schedule_interval`. That argument is REMOVED.
    # `schedule` takes None, a cron string, a timedelta, a Timetable, or a
    # list of Assets (see 03_dataops / 04_mlops).
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    # Airflow 3 default is already False, but being explicit is good manners:
    # it is the flag that decides whether activating a Dag floods you with
    # historical runs.
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
    """A three-task Dag: one source, two branches of work, one join."""

    @task
    def make_greeting(**context) -> str:
        # Context keys are injected by name. `logical_date` replaces Airflow 2's
        # `execution_date`, which no longer exists in the context at all.
        #
        # Read it with `.get()`, never `context["logical_date"]`: in Airflow 3 a
        # manual or asset-triggered run has no logical date, and the key is then
        # missing from the context entirely - indexing it raises KeyError at
        # runtime while `airflow dags test` (which always supplies one) stays
        # green. Use `run_id` when you need something that is always there.
        greeting = context["params"]["greeting"]
        when = context.get("logical_date") or "no logical date (manual run)"
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
