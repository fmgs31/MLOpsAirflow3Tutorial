"""Lesson 1 - a Dag, tasks, and dependencies.

Everything a Dag author needs comes from one place: `airflow.sdk`, the Task SDK.
`dag`, `task`, `DAG`, `Param`, `Asset`, `chain`, `Label` - all of it is there.

Four tasks, wired by passing one task's output to another. Read `make_greeting`
first, then the four lines at the bottom that build the graph.

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
    # No run is ever scheduled before it. A Dag without one still parses, so
    # Airflow will not remind you - set it.
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
    """A three-task Dag: one source, two branches of work, one join."""

    @task
    def make_greeting(**context) -> str:
        # `**context` gives you the run's metadata: run_id, params, ti,
        # dag_run, ds, data_interval_start/end, var, conn, macros.
        #
        # Read `logical_date` with `.get()`, never `context["logical_date"]`:
        # a manual or asset-triggered run has no logical date, and the key is
        # then missing from the context entirely - indexing it raises KeyError
        # at runtime while `airflow dags test` (which always supplies one)
        # stays green. `run_id` is always there; prefer it for naming things.
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
