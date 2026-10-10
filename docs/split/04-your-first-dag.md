## 4. Your first Dag

Everything a Dag author needs is in `airflow.sdk`, the Task SDK:

```python
from airflow.sdk import dag, task, DAG, Param, Asset, chain, Label
```

You are going to write `dags/01_hello_airflow3.py`: four tasks that build a
greeting, shout it, count its words and print the result. Small enough to hold
in your head, and every idea in it comes back in every later Dag.

### Build it

Create the file `dags/01_hello_airflow3.py` and start it with a docstring, the
imports, and a dictionary of defaults:

```python dags/01_hello_airflow3.py
"""Lesson 1 - a Dag, tasks, and dependencies."""

from __future__ import annotations

import pendulum
from airflow.sdk import Param, dag, task

DEFAULT_ARGS = {
    "owner": "mlops-course",
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=30),
}
```

The docstring is not decoration: in a moment you will pass it as the Dag's
in-UI documentation. `pendulum` is the timezone-aware datetime library Airflow
uses everywhere. A plain `datetime` with no timezone attached (a "naive" one) is
read in Airflow's default timezone, which may not be the one you meant — and
then your runs start at hours you did not expect.

**`default_args` is a dictionary applied to every task in the Dag.** Anything a
task accepts can go in it. The values are only defaults: if a task passes the
same argument itself, the task's value replaces the default for that task
alone. With `"retries": 2` here, a task declared with `@task(retries=5)` retries
five times, and every other task still retries twice. It exists so you do not
repeat `owner=` and `retries=` on twenty tasks.

`owner` is a label: it shows in the Dags list, you can filter on it, and it
says who to ask when the Dag breaks. This repo's integrity tests (§14) reject
the default, `airflow`, because an owner that is nobody means nobody gets paged.

The other two decide what happens when a task fails:

- **`retries=2`** — if the task raises, Airflow will run it up to two more
  times. The task instance goes to `up_for_retry`, waits, and is scheduled
  again. It is the *same* task instance throughout: its `try_number` increases,
  each attempt keeps its own log, and only the last attempt's outcome decides
  whether downstream tasks run.
- **`retry_delay`** — how long it waits before the next attempt. Thirty seconds
  is generous for a demo and short for real work; a flaky API usually wants
  minutes, and `retry_exponential_backoff=True` spreads them out instead.

**The default is `retries=0`** (`[core] default_task_retries`), which means a
task hit by a brief network glitch just fails and takes the run with it. Retrying
is opt-in, and that is why this repo's integrity tests (§14) insist every
non-demo Dag sets it.

Retries only help with failures that might not happen again — a timeout, a
throttled API, a node that died. For a failure that *will* happen again, raising
`AirflowFailException` (§9) skips the remaining attempts and fails immediately:
bad data does not become good on the third read.

Now the Dag itself. Add the decorator and the function it decorates:

```python dags/01_hello_airflow3.py

@dag(
    dag_id="hello_airflow3",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "basics"],
    doc_md=__doc__,
    params={"greeting": Param("hello", type="string", description="What to say.")},
)
def hello_airflow3():
```

Line by line:

- **`@dag` turns a function into a Dag.** This decorator style has a name,
  **TaskFlow**, and the name will keep coming up.
- **`schedule=None`** — this Dag runs only when someone triggers it: you,
  through the UI or the CLI, or a program through the API. §7 has the other
  values a schedule can take.
- **`start_date`** — the earliest moment the scheduler creates runs for on its
  own; it never schedules one before it. It does not stop you starting a run
  yourself — a manual trigger works even with a `start_date` in the future — so
  on this `schedule=None` Dag it changes nothing yet; it is set so the Dag is
  ready the day it gets a schedule. It is optional: a scheduled Dag without one
  starts from the moment you un-pause it.
- **`catchup=False`** — decides what happens when you un-pause a scheduled Dag
  whose `start_date` is in the past. With `False`, the default, the scheduler
  ignores the missed past and creates a single run, for the most recent time
  the schedule should have fired. With `True`, it creates one run for *every*
  time the schedule would have fired since `start_date` — months of runs, all
  at once. It is written out here, default or not, so you never have to guess
  which one is in effect (§7).
- **`tags`** — free-form labels; the Dags list filters on them.
- **`doc_md=__doc__`** — the module docstring you just wrote, rendered as
  markdown on the Dag's page in the UI. `description=` is the short version,
  one line shown next to the Dag in the Dags list.
- **`params`** — declared inputs, with types. The UI turns these into a form
  when you trigger the Dag (§5), and a run records the values it used.

Inside the function, the tasks. Indent them — they are the body of
`hello_airflow3()`:

```python dags/01_hello_airflow3.py
    @task
    def make_greeting(**context) -> str:
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
        print(f"{loud}  ({words} words)")
```

**`@task` turns a function into a task.** Calling it does *not* run it: it
returns an `XComArg`, a lazy reference to that task's future output. Whatever
the function returns becomes the task's XCom (§6), and whatever it prints goes
to the task log, viewable in the UI.

`**context` is how a task sees the run it belongs to: `run_id`, `params`, `ti`
(the task instance), `dag_run`, `ds`, `data_interval_start`/`end`, `var`,
`conn`, `macros`. Read `logical_date` with `.get()`, never
`context["logical_date"]`: an asset-triggered run, or one triggered from the
CLI without a logical date, has none, and the key is then missing from the context entirely — indexing it raises
`KeyError` at runtime, while `airflow dags test` (which always supplies one)
stays green. §7 explains the why. `run_id` is always there; prefer it for
naming things.

Finish the file by wiring the tasks together and registering the Dag:

```python dags/01_hello_airflow3.py
    message = make_greeting()
    report(loud=shout(message), words=count_words(message))


hello_airflow3()
```

**Passing one task's output to another draws the edge.** `shout(message)` is
what makes `shout` run after `make_greeting`. The graph is built when the file
is parsed; the code runs later, possibly on another machine. Nothing makes
`shout` and `count_words` wait for each other, so a scheduler-driven run is
free to execute them at the same time (§3 says what decides that):

```
make_greeting ─┬─► shout ──────┬─► report
               └─► count_words ┘
```

And the last line: **the decorator only defines the Dag; calling the function
registers it.** A Dag file that defines a Dag and never calls it produces
nothing — imports cleanly, appears nowhere. It is the most common first mistake
and the hardest to spot, because there is no error to read.

Save the file. Compare with
[`solutions/01_hello_airflow3.py`](../../solutions/01_hello_airflow3.py) if you want
— it is the same code with the comments left in.

### See it parse

The Dag processor (§3) scans `dags/` every five seconds on this stack, imports
each `.py` file it finds, and writes what it parsed into the metadata database.
Within a few seconds of saving:

```bash
docker compose exec airflow-scheduler airflow dags list -o plain
```

**Checkpoint.** One row: `hello_airflow3`, owner `['mlops-course']`,
`is_paused True`, `bundle_name dags-folder`. Reload the UI and the Dag is on
the **Dags** page, paused. (Every Dag starts paused — §7.)

If the row is not there, three things can be wrong, and in `airflow dags list`
they look identical. The first is a file that raises on import — a typo, a
missing package, a `@dag` function that throws. That is not a broken Dag in the
list, it is **no Dag at all**: absent from the UI, absent from `airflow dags
list`. It surfaces as an import error on the Home and Dags pages (§5) and in
this command:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors -o plain
```

**Checkpoint.** `No data found` — your file imports. See what the other case
looks like by making one: save this as `dags/zz_broken_on_purpose.py`,

```python
# An airflow dag file that does not import
import a_module_that_does_not_exist
```

wait a few seconds, and run the command again:

```
bundle_name    filepath                 error
dags-folder    zz_broken_on_purpose.py  Traceback (most recent call last):
                                          File "<frozen importlib._bootstrap>", line 488, in _call_with_frames_removed
                                          File "/opt/airflow/dags/zz_broken_on_purpose.py", line 2, in <module>
                                            import a_module_that_does_not_exist
                                        ModuleNotFoundError: No module named 'a_module_that_does_not_exist'
```

The file, the line and the full traceback. Delete the file and the row clears
by itself on the next scan.

That first comment line is not filler. The Dag processor does not import every
`.py` it finds: with `[core] dag_discovery_safe_mode` on — the default — it
first reads the file as text and skips it unless the lowercased contents
contain `airflow` *and* either `dag` or `asset`. Leave the comment out of that
broken file and it is never parsed, never listed, and never reported — silence,
not an error. A real Dag file always passes this test:
`from airflow.sdk import dag` alone contains both words, so you never have to
think about it. The files that get skipped are the ones that never mention
them — a helper module such as
a `utils.py` with no Dag in it, which is exactly right, or a throwaway
experiment like the broken file above. So when a file is simply not there, this
is the second thing to check.

The third way to get nothing is the beginner mistake from "Build it" — a file
that defines a Dag and never calls it. It imports cleanly, so
`list-import-errors` is empty too, and the only symptom is a Dag that is not
there. Check `list-import-errors` first, then the bottom of the file.

### Run it

This executes a complete Dag run in your terminal, no scheduler involved:

```bash
docker compose exec airflow-scheduler airflow dags test hello_airflow3
```

**Checkpoint.** Scroll for the `report` task's line, which looks like this —
seven words, because `make_greeting` appends the run id *and* the logical date,
and the printed date has a space in the middle of it:

```
HELLO FROM RUN MANUAL__2026-09-15T12:18:32.454459+00:00 AT 2026-09-15 12:18:33.500053+00:00  (7 words)
```

and, at the end, `DagRun Finished: ... state=success`. That is the fastest
authoring loop there is: a full run with real logs and no UI. You will use it
after every Dag you write.

> **What `airflow dags test` is, and is not.** It runs the whole Dag inside this
> one command, with no scheduler, executor or triggerer. Three things behave
> differently from a real run, and you will meet each of them later:
>
> - Tasks run **one at a time**, in an order the graph allows. In the output
>   of that run, one of `shout`/`count_words` finished before the other started;
>   which goes first varies from run to run.
> - It **always supplies a logical date**. A trigger from the CLI does not (§7),
>   which is how a Dag can pass here and fail in production.
> - A **deferrable** task (§11), when it has to wait, normally suspends itself
>   and leaves the waiting to another service, the triggerer. Here there is no
>   triggerer, so `airflow dags test` does the waiting itself, and the command
>   does not return until the wait is over.
>
> It is **not a dry run**, though: tasks write real files and real asset events
> (§9). It is the fastest way to see a Dag work; §14 says when to stop trusting
> it.

> **Try it.** Change the greeting without touching the file:
>
> ```bash
> docker compose exec airflow-scheduler airflow dags test hello_airflow3 --conf '{"greeting": "hola"}'
> ```
>
> The `report` line now starts with `HOLA`. That `--conf` reached the task through
> `context["params"]` — the `Param` you declared, overridden for one run.

**The other way to declare a Dag** is the context manager, which is fully
supported and reads better when the tasks are operators (§6) rather than
functions. Not for this file — just so you recognise it:

```python
from airflow.sdk import DAG

with DAG(
    dag_id="hello_operators",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    BashOperator(task_id="hello", bash_command="echo hi")
```

Anything created inside the `with` block attaches to that Dag automatically.
Both forms produce the same object, and one file may hold several Dags.

---

← [3. The pieces of Airflow](03-the-pieces-of-airflow.md) · [Index](README.md) · [5. The UI](05-the-ui.md) →
