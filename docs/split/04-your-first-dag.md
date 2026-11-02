## 4. Your first Dag

Everything you need to define Dags, tasks and assets is in `airflow.sdk`, the
Task SDK (operators come from the providers, §6):

```python
from airflow.sdk import dag, task, DAG, Param, Asset, chain, Label
```

You are going to write `dags/01_hello_airflow3.py`: four tasks that build a
greeting, shout it, count its words and print the result. It is small, and
every idea in it comes back in every later Dag.

### Build it

Create `dags/01_hello_airflow3.py` with a docstring, the imports, and a
dictionary of defaults:

```python dags/01_hello_airflow3.py
"""Lesson 1 - a Dag, tasks, and dependencies."""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.sdk import Param, dag, task

DEFAULT_ARGS = {
    "owner": "mlops-course",
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=30),
}
```

The docstring will become the Dag's documentation in the UI (`doc_md`).
`pendulum` is the timezone-aware datetime library Airflow uses everywhere. A
`datetime` with no timezone (a "naive" one) is read in Airflow's default
timezone, which may not be yours, and your runs start at hours you did not
expect.

**`default_args` is a dictionary applied to every task in the Dag.** Anything a
task accepts can go in it. Its values are only defaults: a task that passes the
same argument overrides it for itself alone. With `"retries": 2` here, a task
declared with `@task(retries=5)` retries five times, and every other task
twice. It saves repeating `owner=` and `retries=` on twenty tasks.

`owner` is a label: it shows in the Dags list, you can filter on it, and it
says who to ask when the Dag breaks. This repo's integrity tests (§15) reject
the default, `airflow`: an owner that is nobody means nobody gets paged.

The other two decide what happens when a task fails:

- **`retries=2`** — if the task raises, Airflow runs it up to two more times.
  The task instance goes to `up_for_retry`, waits, and is scheduled again. It
  stays the *same* task instance: its `try_number` increases, each attempt
  keeps its own log, and only the last attempt's outcome decides whether
  downstream tasks run.
- **`retry_delay`** — the wait before the next attempt. Thirty seconds is
  generous for a demo and short for real work; a flaky API usually wants
  minutes, and `retry_exponential_backoff=2.0` doubles the wait after each
  attempt.

**The default is `retries=0`** (`[core] default_task_retries`), so a brief
network glitch fails the task and the run with it. Retrying is opt-in, which is
why this repo's integrity tests (§15) require every non-demo Dag to set it.

Retries only help with failures that might not recur — a timeout, a throttled
API, a dead node. For one that *will* recur, raise `AirflowFailException` (§9):
it skips the remaining attempts and fails immediately. Bad data does not become
good on the third read.

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

- **`@dag` turns a function into a Dag.** This decorator style is called
  **TaskFlow**.
- **`schedule=None`** — the Dag runs only when triggered: by you, from the UI
  or the CLI, or by a program through the API. §7 covers the other values.
- **`start_date`** — the earliest moment the Scheduler creates runs for on its
  own. Manual triggers ignore it — they work even with a future `start_date` —
  so on this `schedule=None` Dag it changes nothing yet; it is set so the Dag
  is ready once it gets a schedule. It is optional; §7 explains when a
  scheduled Dag without one starts.
- **`catchup=False`** — what happens when you un-pause a scheduled Dag whose
  `start_date` is in the past. With `False`, the default, the Scheduler ignores
  the missed past and creates one run, for the most recent time the schedule
  should have fired. With `True`, it creates one run for *every* time the
  schedule would have fired since `start_date` — months of runs at once. It is
  written out despite being the default, so you never guess which one is in
  effect (§7).
- **`tags`** — free-form labels; the Dags list filters on them.
- **`doc_md=__doc__`** — the module docstring, rendered as markdown on the
  Dag's page. `description=` is the short version: one line next to the Dag in
  the Dags list.
- **`params`** — declared inputs, with types. The UI turns these into a form
  when you trigger the Dag (§5), and a run records the values it used.

Inside the function go the tasks, indented as the body of `hello_airflow3()`:

```python dags/01_hello_airflow3.py
    @task
    def make_greeting(**context: Any) -> str:
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
returns an `XComArg`, a lazy reference to the task's future output. The return
value becomes the task's XCom (§6); anything printed goes to the task log,
viewable in the UI.

`**context` is how a task sees its run: `run_id`, `params`, `ti` (the task
instance), `dag_run`, `ds`, `data_interval_start`/`end`, `var`, `conn`,
`macros`. They have different types, hence `**context: Any`; Airflow fills the
context in by name and ignores the annotation.

Read `logical_date`, the date that names a run ("the run of 10 September", §7),
with `.get()`, never `context["logical_date"]`. Asset-triggered runs, and CLI
triggers without a logical date, have none, and the key is missing: indexing it
raises `KeyError` at runtime, while `airflow dags test`, which always supplies
one, stays green. §7 explains why. `run_id` is always there; prefer it for
naming things.

Finish the file by wiring the tasks together and registering the Dag:

```python dags/01_hello_airflow3.py
    message = make_greeting()
    report(loud=shout(message), words=count_words(message))


hello_airflow3()
```

**Passing one task's output to another draws the edge.** `shout(message)` makes
`shout` run after `make_greeting`. The graph is built when the file is parsed;
the code runs later, possibly on another machine. Nothing orders `shout` and
`count_words` relative to each other, so a scheduler-driven run may execute them
at the same time (§3 says what decides that):

```
make_greeting ─┬─► shout ──────┬─► report
               └─► count_words ┘
```

The last line matters: **the decorator only defines the Dag; calling the
function registers it.** A file that defines a Dag without calling it imports
cleanly and appears nowhere — the most common first mistake, and the hardest to
spot, because there is no error to read.

Save the file. If you want, compare it with
[`solutions/01_hello_airflow3.py`](../../solutions/01_hello_airflow3.py): the
same code, with comments.

### See it parse

The Dag processor (§3) scans `dags/` every five seconds on this stack, imports
each `.py` file that looks like a Dag file (what that means comes further down),
and writes the result to the Metadata database. Within a few seconds of saving:

```bash
docker compose exec airflow-scheduler airflow dags list -o plain
```

**Checkpoint.** One row: `hello_airflow3`, owner `['mlops-course']`,
`is_paused True`, `bundle_name dags-folder`. Reload the UI and the Dag is on
the **Dags** page, paused. (Every Dag starts paused — §7.)

If the row is still missing, there are three causes, and in `airflow dags list`
they look identical. The first is a file that raises on import: a typo, a
missing package, a `@dag` function that throws. The result is not a broken Dag
in the list but **no Dag at all**, absent from the UI and from the list. It
surfaces as an import error on the Home and Dags pages (§5) and in this command:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors -o plain
```

**Checkpoint.** `No data found` — your file imports. To see the other case,
make one: save this as `dags/zz_broken_on_purpose.py`,

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

The comment line matters. The Dag processor does not import every `.py` it
finds: with `[core] dag_discovery_safe_mode` on (the default), it first reads
the file as text and skips it unless the lowercased contents contain `airflow`
*and* either `dag` or `asset`. Without the comment, `zz_broken_on_purpose.py`
is never parsed, listed or reported: silence, not an error. A real Dag file
always passes, since `from airflow.sdk import dag` alone contains both words.
The skipped files are the ones missing a word: a helper module with no Dag,
which is right, or a throwaway experiment like `zz_broken_on_purpose.py`. So
when a file is missing, this is the second thing to check.

The third cause is the beginner mistake from "Build it": a Dag defined but
never called. It imports cleanly, so `list-import-errors` is empty too; the only
symptom is a missing Dag. Check `list-import-errors` first, then the bottom of
the file.

### Run it

This executes a complete Dag run in your terminal, with no Scheduler:

```bash
docker compose exec airflow-scheduler airflow dags test hello_airflow3
```

**Checkpoint.** Find the `report` task's line. It has seven words, because
`make_greeting` appends the run id *and* the logical date, and the printed date
contains a space:

```
HELLO FROM RUN MANUAL__2026-09-15T12:18:32.454459+00:00 AT 2026-09-15 12:18:33.500053+00:00  (7 words)
```

and, at the end, `DagRun Finished: ... state=success`: a full run with real
logs and no UI, the fastest way to try a Dag while you write it. You will use
it after every Dag.

> **What `airflow dags test` is, and is not.** It runs the whole Dag inside the
> command, with no Scheduler, Executor or Triggerer. Three things differ from a
> real run, and you will meet each of them later:
>
> - Tasks run **one at a time**, in an order the graph allows. In your run, one
>   of `shout`/`count_words` finished before the other started; the order varies
>   between runs.
> - It **always supplies a logical date**. A CLI trigger does not (§7), so a Dag
>   can pass here and fail in production.
> - A **deferrable** task (§11) that has to wait normally suspends itself and
>   hands the waiting to another service, the Triggerer. With no Triggerer
>   here, `airflow dags test` waits itself and does not return until the wait is
>   over.
>
> It is **not a dry run** (a rehearsal that reports what a command would do
> without doing it), though: tasks write real files and real asset events
> (§9). §15 says when to stop trusting it.

> **Try it.** Change the greeting without touching the file:
>
> ```bash
> docker compose exec airflow-scheduler airflow dags test hello_airflow3 --conf '{"greeting": "hola"}'
> ```
>
> The `report` line now starts with `HOLA`: `--conf` reached the task through
> `context["params"]`, overriding the `Param` you declared for one run.

**Another way to declare a Dag** is the context manager, fully supported and
more readable when the tasks are operators (§6) rather than functions. It is
not for this file; it is here so you recognise it:

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
