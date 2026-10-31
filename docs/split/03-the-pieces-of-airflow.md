## 3. The pieces of Airflow

Airflow is a scheduler for batch workflows written in Python. You describe the
graph in code, and Airflow decides when each node runs, retries it, records the
attempt, and shows you the result. It does not move your data and it does not
train your model — it starts the thing that does, waits, and reacts.

That also tells you when to use something else. Every Dag run and every task
is scheduled, written to the Metadata database and started as a process, which
costs a few seconds per task. That is nothing for work measured in minutes, and
far too slow for work that must finish in milliseconds. Airflow can *start* a
run when an event arrives (§11), but one run per message, click or request, at
thousands per second, would swamp the Scheduler and the Metadata database; a queue
consumer or a stream processor (Kafka, Flink…) is built for that. Airflow is
also a poor fit for anything needing a cycle or a `while` loop across runs, and
for heavy compute inside the worker — trigger a Spark/Ray/Kubernetes job and
track it instead. If your pipeline is three scripts run once a week, cron is
still allowed.

Four processes, the Metadata database and an Executor, which is not always a
process of its own ("The Executor", below). All of them are running in your
stack now:

| Component | Job |
|---|---|
| **API server** | Serves the REST API, the React UI, and the **Task Execution API** that workers call |
| **Scheduler** | Decides what to run and hands work to the Executor |
| **Dag processor** | Parses your Dag files into the Metadata database — its own process |
| **Triggerer** | Runs async triggers for deferred tasks (§11) and async callbacks |
| **Executor** | Where tasks actually run. `LocalExecutor` here; Celery or Kubernetes in production |
| **Metadata DB** | State of everything: runs, task instances, XComs, connections |

The Scheduler, the Executor and the Metadata database decide most of how your
Dags behave, so each gets a section of its own.

### The Scheduler, and why things happen "a moment later"

The Scheduler is a loop. It wakes up, asks the Metadata database a few questions
— which Dag runs are due, which task instances have all their upstream
dependencies satisfied, which asset events have arrived — marks what it finds as
`queued`, hands those to the Executor, and goes back to sleep. Then it does it
again. `[scheduler] scheduler_idle_sleep_time` is how long it sleeps when it
found nothing to do; on this stack it is **1 second**.

This is why nothing in Airflow is instant, and why nothing is very slow either.
When `dataops` publishes its asset in §10, the `mlops` run does not appear
*because* the task finished — it appears because the next Scheduler loop noticed
the event the task recorded. With a one-second sleep between loops, that gap is
a second at most. What you *will* notice is how long the upstream tasks take,
which has nothing to do with the Scheduler.

Parsing is a separate clock, and it is the one you are about to depend on. The
Dag processor re-parses every file, changed or not, every `[dag_processor]
min_file_process_interval` seconds and re-scans the folder for new files every
`refresh_interval`. This stack sets both to **5**, so a file you save shows up
in the UI within a few seconds. The defaults are 30 and 300: on a stack that
keeps them, a brand-new file can take five minutes to appear, which looks like
a broken Dag if you do not know the setting exists.

All three numbers are settings, and you can print yours:

```bash
docker compose exec airflow-scheduler airflow config get-value scheduler scheduler_idle_sleep_time
docker compose exec airflow-scheduler airflow config get-value dag_processor min_file_process_interval
docker compose exec airflow-scheduler airflow config get-value dag_processor refresh_interval
```

### The Executor, and what "at the same time" means

The Executor answers "where does this task's Python run?". The Scheduler loop
does not run your code; it only decides *that* code should run and hands the
task to the Executor, which starts it somewhere and reports back.

`LocalExecutor` — the one in this stack — starts each task as its **own
subprocess on the machine running the Scheduler**, using Python's
`multiprocessing`. Real parallelism, one machine's worth of it.
`CeleryExecutor` instead drops the task on a queue for a set of long-running
worker machines, and `KubernetesExecutor` asks Kubernetes for a fresh pod per
task. Swapping between them changes no Dag code: this is the mechanism behind
§1's "swap the Executor, not the Dag".

So `docker compose ps` shows no Executor container. `LocalExecutor`
is a *component of the Scheduler process*, not a service: the Scheduler imports
it and it forks children from there. Change `AIRFLOW__CORE__EXECUTOR` to
`CeleryExecutor` and workers become real machines you deploy and scale
separately. The Executor is the seam between "one laptop" and "a cluster", and
it is a config value.

"Worker" therefore means something slightly different per Executor: a forked
subprocess here, a Celery worker process there, a pod in Kubernetes. What is
constant is that the worker gets the task, runs it, and talks back over the Task
Execution API.

How many run at once is decided by the **narrowest** of several limits. These
four are the task limits this tutorial uses, all at their defaults on this
stack (there are more: `max_active_tis_per_dagrun` on a task, `max_active_tasks`
on a Dag, and `max_active_runs`, which limits runs rather than tasks and is used
in §9 and §10):

| Limit | Default here | Scope |
|---|---|---|
| `[core] parallelism` | 32 | task instances running across the whole installation |
| `[core] max_active_tasks_per_dag` | 16 | running task instances in one Dag |
| `default_pool` slots | 128 | tasks not assigned to another pool |
| `max_active_tis_per_dag` on a task | unset | that one task, across all its runs |

To see the first and the third on this stack:

```bash
docker compose exec airflow-scheduler airflow config get-value core parallelism
docker compose exec airflow-scheduler airflow pools list
```

A **pool** is a named bucket of slots you assign tasks to with `pool="…"`
(Admin → Pools), and it is how you stop twenty concurrent tasks from opening
twenty connections to a database that tolerates five. Everything lands in
`default_pool` unless you say otherwise. §10 sets the fourth limit on a single
task (`max_active_tis_per_dag=2`), and you will see the effect: four tuning
tasks that run two at a time instead of all at once.

### The Metadata database

Every fact Airflow knows lives in one relational database: Dag definitions as
parsed, every Dag run, every task instance and its state and try count, XComs,
Connections, Variables, pools, asset events, the audit log. In this stack it is
the `postgres` container (Postgres 16). It is Airflow's own database, not one
your tasks use: when a task reads the BigQuery warehouse (§9), or an
application's Postgres, that is a different database, reached through a
Connection, a hook or the tool's own client (§6). The same Postgres container
also holds a second database, `optuna`, for §10; it sits next to Airflow's,
and is not part of it.

The UI is a view of the Metadata database, and the Scheduler's loop is a
query against it. Restart the containers and nothing is lost, because the
state is in the Metadata database's volume, not in the containers;
`scripts/reset.sh` deletes that volume, which is why it is the last resort.
So the Metadata database is the thing to back up, and it grows forever unless
you run `airflow db clean` (§16): a year of runs, task instances and XComs
does not remove itself.

### The lifecycle of one task

These terms have precise meanings, and mixing them up is what makes error
messages confusing. A **Dag** is the graph you wrote. A **Dag run** is one execution of
it, identified by a `run_id`, and it is `queued`, `running`, `success` or
`failed` — those four, no others. A **task instance** is one task inside one Dag
run, and it has many more states, because more can happen to it:

```
                                                 ┌─► success              done
no status ──► scheduled ──► queued ──► running ──┼─► failed               done — out of retries
                   ▲                             │
                   │                             ├─► up_for_retry         waiting out retry_delay
                   │                             ├─► up_for_reschedule    a sensor between pokes
                   │                             ├─► deferred             parked on the Triggerer
                   │                             └─► awaiting_input       parked until a person answers
                   └─────────────────────────────────┘                    (or the timeout does)
                      those four are all "parked, will come back"

  never started: skipped (a branch chose otherwise) · upstream_failed
```

The distinction that catches everyone first is **`queued` vs `running`**:

- **no status** — when a Dag run is created, the Scheduler creates one task
  instance for every task in the Dag at once, all without a state: in the
  Metadata database their `state` is `null`, and the UI shows it as "no status", an
  uncoloured box. A task stays like that while its upstream tasks have not
  finished. This is what "waiting for another task" looks like: there is no
  state with that name.
- **`scheduled`** — the Scheduler has decided this task *may* run: its upstream
  tasks are done and its trigger rule is satisfied. Nothing has started.
- **`queued`** — the Scheduler has handed it to the Executor. Still nothing is
  executing. A task that sits here is waiting on a *slot*, not on your code, so
  the place to look is the four limits above, not the task log — which will be
  empty, because no process has written one yet.
- **`running`** — a worker has it and your Python is executing. Now there are
  logs.

**A task only waits for another one before it starts.** If `b` depends on `a`,
`b` has no state while `a` runs. When `a` finishes, the Scheduler checks `b`'s
trigger rule (by default: every upstream task succeeded) and, if it is met,
moves `b` to `scheduled`. Once `b` is `running`, it cannot pause
halfway through its code to wait for something from `a` — tasks do not talk to
each other while they run. Anything `b` needs from `a`, an XCom (§6) or a file,
has to exist before `b` starts. If two steps must coordinate midway, they are
two tasks with a dependency between them.

The rest you will meet as you go: `up_for_retry` between attempts (§4),
`skipped` from a branch (§10), `up_for_reschedule` from a sensor that exits
between pokes and `deferred` from one suspended onto the Triggerer (§11),
`awaiting_input` while a human decides (§10), and `upstream_failed` on
everything downstream of a failure.

### Task code never touches the Metadata database

A task talks to the Task Execution API over HTTP, never to the Metadata
database. That is what makes remote workers and the Edge Executor possible,
and why a task cannot open a session on the Metadata database and query
`DagRun` itself.

```bash
curl -s localhost:8080/api/v2/monitor/health
```

(If you are in PowerShell after all, `curl` there is an alias for
`Invoke-WebRequest` and rejects `-s`; call `curl.exe` instead.)

You should get `healthy` four times — one JSON object each for the
Metadata database, Scheduler, Triggerer and Dag processor, the last three with the
time of their most recent heartbeat. The interactive API docs are at
<http://localhost:8080/docs>.

---

← [2. Get it running](02-get-it-running.md) · [Index](README.md) · [4. Your first Dag](04-your-first-dag.md) →
