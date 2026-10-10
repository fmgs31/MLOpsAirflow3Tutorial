# Troubleshooting

Everything below was hit while building and running this repo against Airflow
**3.3.1**, not copied from an issue tracker.

---

## `The conn_id 'fs_default' isn't defined`

```
airflow.sdk.exceptions.AirflowNotFoundException: The conn_id `fs_default` isn't defined
```

`FileSensor` needs the `fs_default` connection, and **`airflow db migrate` does
not create any default connections** — the metadata database starts empty.
Create it yourself:

```bash
airflow connections add fs_default --conn-type fs --conn-extra '{"path": "/"}'
```

or, without touching the database — any `AIRFLOW_CONN_<CONN_ID>` env var defines
a connection, which is what `docker-compose.yaml` does here:

```yaml
AIRFLOW_CONN_FS_DEFAULT: '{"conn_type": "fs", "extra": {"path": "/"}}'
```

## Airflow 3.3.1 bug: `airflow dags test` crashes on a Dag that sets `deadline=`

```
psycopg2.ProgrammingError: can't adapt type 'dict'
[SQL: SELECT deadline_alert.id, ... WHERE deadline_alert.id = %(id_1)s::UUID]
```

Raised from `DagRun.update_state` → `DeadlineAlertModel.get_by_id`: the
`dag.test()` code path passes the serialized alert where an alert id is
expected. **The tasks all run to completion first** — only the CLI's final state
update fails, so the traceback is noisier than the actual damage.

Workaround: exercise deadline Dags through the scheduler, not `dags test`:

```bash
airflow dags unpause deadline_alerts
airflow dags trigger deadline_alerts
```

That path works correctly — `solutions/07_deadline_alerts.py` fires its callback at
exactly +30s. This is why the `mlops` Dag has no `deadline=`: it would make the
lesson's own "run it with `airflow dags test`" instruction fail.

## `KeyError: 'logical_date'` — but only on some runs

```
KeyError: 'logical_date'
  File "/opt/airflow/dags/01_hello_airflow3.py", line 54, in make_greeting
```

An asset-triggered run, or one triggered from the CLI or the REST API without
a logical date, has **no logical date**, and the key is then absent from the
task context entirely — not present-and-`None`. The Dag runs green under
`airflow dags test`, which always supplies a logical date, and fails the first
time such a run happens. The UI's **Trigger** form pre-fills a logical date, so
a run started there does not show the bug.

```python
context["logical_date"]        # KeyError
context.get("logical_date")    # None
context["run_id"]              # always present — use this for naming things
```

Since `mlops` is asset-triggered, every one of its runs is in this state.

## `jinja2.exceptions.UndefinedError: 'ds' is undefined`

The same cause, one level down: `ds`, `ts`, `data_interval_start` and the rest
are derived from `logical_date`, so on a run that has none they are **undefined**
in templates and the task fails while rendering, before it executes.

```jinja
{{ ds }}                        UndefinedError
{{ ds | default('none') }}      renders "none"
{{ dag_run.run_after }}         renders — every run has a run_after
{{ run_id }}                    renders
```

`airflow dags test` supplies a logical date, so this too only shows up on a
real run that has none. Both of these were live bugs in this tutorial's own §4 and §6
Dags until a manual trigger found them.

## `DagBag.__init__() got an unexpected keyword argument 'include_examples'`

`DagBag` lives in `airflow.dag_processing.dagbag` and takes no
`include_examples`. Whether the bundled example Dags are loaded is a config
setting (`[core] load_examples`, `False` here), not an argument:

```python
from airflow.dag_processing.dagbag import DagBag
DagBag(dag_folder=...)
```

A snippet using `from airflow.models.dagbag import DagBag` is out of date; that
path still resolves but emits `DeprecatedImportWarning`.

## `airflow dags list-runs -d mlops` prints the help text

`dag_id` is a positional argument:

```bash
airflow dags list-runs mlops -o plain
```

## `airflow.exceptions.X is deprecated ... Use airflow.sdk.exceptions.X`

Task code imports exceptions from the Task SDK. `airflow.exceptions` resolves
only because airflow-core happens to be installed in the same place — a
remote/Edge worker only has the SDK.

```python
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
```

## `image "airflow3-tutorial:3.3.1": already exists` during `docker compose build`

Every service shares one image tag, and Compose builds them in parallel, so the
exporters race. Build a single service — the others reuse the tag:

```bash
docker compose build airflow-init
```

`scripts/up.sh` and `scripts/up.ps1` already do this.

## `.\scripts\up.ps1` — "running scripts is disabled on this system"

Windows PowerShell's default execution policy refuses every `.ps1` file. Pass a
policy for that one invocation instead of changing the machine's setting:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\up.ps1
```

The README's PowerShell lines are written this way.

## `up` never finishes; `docker compose ps` shows the Airflow services `Restarting`

Their logs all say `Database migration required. Please run 'airflow db
migrate'`. Do not run it by hand — `airflow-init` is the service that migrates,
the other four wait for it to exit successfully, and its log says why it did
not:

```bash
docker compose logs airflow-init
```

Fix whatever it reports, then `docker compose up -d`: init runs again, migrates
the empty database, and the restarting services recover on their own.

One known cause, fixed in the compose file on 2026-09-14: `airflow-init` used
to bypass the image's `/entrypoint` with a bare `bash -c "airflow db migrate;
echo ..."`. With an `AIRFLOW_UID` other than `50000` in `.env` — `scripts/up.sh`
writes yours on Linux and WSL2 — that user has no `/etc/passwd` entry, Docker
sets `HOME=/`, and the `airflow` console script's interpreter looks for
Airflow's user-site install under `$HOME/.local`, the wrong place. The
entrypoint's job is to set `HOME=/home/airflow` for exactly that case; the
trailing `echo` then made the failed container exit 0, so the others started
anyway. Symptom in the init log: `ModuleNotFoundError: No module named
'airflow'`. Note that `.env` is shared by every `up` script run from the same
checkout, WSL2 or PowerShell — whichever ran last set the uid.

## My Dag does not show up in the UI

In Airflow 3 parsing is done by a **separate process**. If you started the
components by hand and skipped it, nothing is ever parsed:

```bash
airflow dag-processor
```

Then check for parse errors explicitly:

```bash
airflow dags list-import-errors
airflow dags reserialize
```

## A deferrable task or a HITL task hangs forever

`deferrable=True` sensors and operators, and `ApprovalOperator`, all
depend on the **triggerer**. Without `airflow triggerer` running they sit in
`deferred` / `awaiting_input` indefinitely.

## macOS: tasks die with `+[__NSCFConstantString initialize] may have been in progress in another thread when fork() was called`

Not an Airflow bug — macOS's Objective-C runtime aborts a forked child when a
class was mid-initialisation in another thread at `fork()` time, and Airflow's
`LocalExecutor` forks to run tasks. The long-standing workaround:

```bash
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
```

It silences the check rather than fixing the fork-safety violation, so treat it
as a local-development convenience only. On a Mac, running the stack in Docker
avoids the whole class of problem — which is what this repo does by default.

*(macOS-specific; not reproducible on the Windows/Docker stack everything else
here was verified on.)*

## `pytest /opt/airflow/tests` becomes `C:/Program Files/Git/opt/airflow/tests`

Git Bash on Windows rewrites POSIX-looking arguments. Prefix the command:

```bash
MSYS_NO_PATHCONV=1 docker compose run --rm --entrypoint pytest airflow-cli /opt/airflow/tests -q
```

PowerShell does not have this problem.

## `airflow users create` says the command does not exist

`airflow users` is a FAB provider command. It only exists when
`apache-airflow-providers-fab` is installed (the official image bundles it) and
is only meaningful with the FAB auth manager. Under the default Simple auth
manager, users come from config:

```ini
[core]
simple_auth_manager_users = "admin:admin,bob:viewer"
```

Passwords land in `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`.

## Tasks fail with `ModuleNotFoundError: No module named 'tagifai'`

The Dag processor parses the files, but the **executor** runs the tasks — both
need the package on `PYTHONPATH`. In this repo `PYTHONPATH=/opt/airflow/src` is
set on every service in `docker-compose.yaml`. Natively:

```bash
export PYTHONPATH="${PWD}/src"
```

## Everything is broken and I want a clean slate

```bash
./scripts/reset.sh          # containers + DB volume + artifacts
powershell -ExecutionPolicy Bypass -File .\scripts\reset.ps1   # same, Windows PowerShell
# or, natively:
airflow db reset -y
```
