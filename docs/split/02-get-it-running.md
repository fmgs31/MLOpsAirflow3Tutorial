## 2. Get it running

There are two ways to install, and the choice decides how far you can go:

| Install | Covers | Runs |
|---|---|---|
| **Docker** | the whole tutorial, §3 to §16 | Airflow, Postgres and MLflow, plus the dbt and alibi-detect virtualenvs, all from `docker-compose.yaml` |
| **Native** | §3 to §8 only | Airflow alone, in a Python virtualenv on your machine |

From §9 on, the pipeline needs more than Airflow: the MLflow server, Optuna's
database and two extra virtualenvs, which only the Docker stack sets up. If you
plan to do the whole tutorial, use Docker.

### Docker (recommended, and required from §9 on)

Airflow has no native Windows support. On Windows use Docker Desktop, or run the
native install inside WSL2.

```bash
git clone <this repo>
cd Airflow3Tutorial
```

Then start the stack. On Linux, macOS, WSL2 or Git Bash on Windows:

```bash
./scripts/up.sh
```

In Windows PowerShell. A default install refuses to run `.ps1` files, which is
why the command needs `-ExecutionPolicy Bypass`:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\up.ps1
```

**Checkpoint.** The script waits for the API server, then prints the service
table. Open <http://localhost:8080> — there is no login. The Dags list is
**empty**, and it should be: you have not written one yet. The Home page shows
the scheduler, triggerer and Dag processor as healthy. If the page does not
load, jump to [troubleshooting](../troubleshooting.md).

```bash
docker compose ps
docker compose exec airflow-scheduler airflow dags list
```

The second prints `No data found` — the same empty list. Those two lines are
the shape of every command from here on. `docker compose …` runs on your
machine; `airflow …` runs inside the scheduler container, which is what the
`docker compose exec airflow-scheduler` prefix does. Where this tutorial shows
an `airflow …` command bare — in a transcript, or as a one-liner in prose — it
means the same thing, prefix included.

The stack is deliberately smaller than the [official
docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/3.3.1/docker-compose.yaml):
`LocalExecutor` instead of Celery + Redis (§3 explains what an executor is), and
the Simple auth manager instead of FAB — Flask-AppBuilder, the provider that
gives Airflow users, roles and a login page. Fewer moving parts while you are
learning.

Two services in the list are not Airflow. `mlflow` is MLflow (§1), whose UI
is at <http://localhost:5000>; §10 is where you use it. `extra-databases` runs
once at start-up, creates a database for Optuna (§10) on the same Postgres,
and exits. The rest of the data stack, a Google
Cloud project and Airbyte, you set up in §9; nothing before §9 needs either.

> **Auth:** `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=true` is set, so the
> UI has no login, and port 8080 is published on `127.0.0.1` only — the loopback
> address — so nothing outside your machine can reach it. Do not deploy this
> configuration anywhere else.

One more script, for later. Everything the stack accumulates — runs, task
logs, the snapshots the Dags write, MLflow's runs and models — survives a
restart of the containers, because it lives in Docker volumes and in the
`data/` and `logs/` folders of this repo (§3 comes back to this). To
throw all of it away and start over from a clean slate, on Linux, macOS, WSL2
or Git Bash:

```bash
./scripts/reset.sh
```

or in Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\reset.ps1
```

It asks before deleting, and `up.sh` / `up.ps1` brings the stack back. It does
not touch `dags/` — the files you write survive a reset — nor anything outside
this repo's stack: your BigQuery tables and Airbyte stay as they are. No step in this
tutorial needs it; it is here for the day something is stuck and
[troubleshooting](../troubleshooting.md) has not helped.

> **One shell for the rest.** Every command after this point is written for
> bash: Linux, macOS, WSL2, or Git Bash on Windows. The two `.ps1` scripts
> exist so that PowerShell can start and reset the stack, but the tutorial's
> commands use `\` line continuations, `grep` and single-quoted JSON, all of
> which PowerShell reads differently. On Windows, open Git Bash for everything
> below.
>
> On **Git Bash**, a few later commands pass a `/opt/...` path into a
> container, and each carries the prefix `MSYS_NO_PATHCONV=1`. It is not
> decoration: without it Git rewrites the path into a Windows one before Docker
> sees it.

> **Your editor.** You will be creating files in `dags/` from §4 on. Any editor
> works; the container sees the folder through a bind mount, so a file saved on
> your machine is a file inside Airflow a moment later. Nothing needs copying.

### Native (Linux / macOS / WSL2; §3 to §8 only)

```bash
./scripts/setup-native.sh
```

> **The native install covers Airflow, and so §3 to §8.** From §9 on, the
> pipeline also needs the services in the compose file (MLflow, Optuna's
> Postgres database) and the two virtualenvs the image builds for dbt and
> alibi-detect (§9, §12). Recreating those by hand is possible, and the paths
> are settings in `config.py` (`PROJECT_TAGGER_DBT_BIN`,
> `PROJECT_TAGGER_ALIBI_PYTHON`, `MLFLOW_TRACKING_URI`, `OPTUNA_STORAGE`), but
> this tutorial does not walk through it: for §9 onwards, use the Docker stack.

What that script does, and why each line matters:

```bash
AIRFLOW_VERSION=3.3.1
PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install -r requirements.txt --constraint "${CONSTRAINT_URL}"   # constraints are NOT optional

export AIRFLOW_HOME=${PWD}/airflow
export PYTHONPATH=${PWD}/src                    # so tasks can import `project_tagger`
export AIRFLOW__CORE__DAGS_FOLDER=${PWD}/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=5            # see §3: pick up a new file in seconds
export AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL=5

airflow db migrate                              # create the metadata database
airflow connections add fs_default --conn-type fs --conn-extra '{"path": "/"}'
```

Airflow 3.3.1 needs Python ≥ 3.10 (and not 3.15). Install it **with the
constraint file** — Airflow has a large dependency tree and an unconstrained
install will resolve to something that does not work.

`airflow db migrate` populates `$AIRFLOW_HOME`:

```
airflow.cfg     every setting, with the defaults commented — see config/airflow.cfg.example
airflow.db      the SQLite metadata DB
logs/           task logs, one directory per dag_id / run_id / task_id
```

> **SQLite is for development only.** The Airflow docs are blunt about it: it
> "should NEVER be used for production". It is fine for working through this
> tutorial — `LocalExecutor` runs tasks in parallel against it, and needs
> SQLite ≥ 3.15 — but move to Postgres or MySQL for anything real. The Docker
> stack in this repo already runs Postgres 16, which is why the Docker path is
> the recommended one.

Any setting in `airflow.cfg` can also be given as an `AIRFLOW__SECTION__KEY`
environment variable. `SECTION` is the `[header]` the setting sits under and
`KEY` is the setting's own name, both uppercased and joined by **double**
underscores — an underscore *inside* either name stays single:

```
airflow.cfg                          environment variable
─────────────────────────────────    ────────────────────────────────────────
[core]
dags_folder = <repo>/dags            AIRFLOW__CORE__DAGS_FOLDER=<repo>/dags

[dag_processor]
refresh_interval = 5                 AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=5
```

Both are lines the script above exports: it configures Airflow through the
environment instead of editing `airflow.cfg`. The Docker stack uses the same
mechanism in [`docker-compose.yaml`](../../docker-compose.yaml).

> **Default connections are not created for you.** `fs_default` is added by hand
> above because a `FileSensor` uses it unless told otherwise (§6). In Docker
> the compose file provides it as `AIRFLOW_CONN_FS_DEFAULT`.

Then either the all-in-one process:

```bash
airflow standalone
```

…or the components separately, which is what a real deployment looks like:

```bash
airflow api-server --port 8080
airflow scheduler
airflow dag-processor            # a separate process — required
airflow triggerer                # deferrable tasks and async callbacks
```

---

← [1. What you're building](01-what-you-re-building.md) · [Index](README.md) · [3. The pieces of Airflow](03-the-pieces-of-airflow.md) →
