## 2. Get it running

There are two ways to install, and the choice decides how far you can go:

| Install | Covers | Runs |
|---|---|---|
| **Docker** | the whole tutorial, §3 to §17 | Airflow, Postgres and MLflow, plus the dbt and alibi-detect virtualenvs, all from `docker-compose.yaml` |
| **Native** | §3 to §8 only | Airflow alone, in a Python virtualenv on your machine |

### Docker (recommended, and required from §9 on)

Airflow has no native Windows support. On Windows use Docker Desktop, or run the
native install inside WSL2.

```bash
git clone https://github.com/fmgs31/MLOpsAirflow3Tutorial.git
cd MLOpsAirflow3Tutorial
```

Then start the stack. On Linux, macOS, WSL2 or Git Bash on Windows:

```bash
./scripts/up.sh
```

In Windows PowerShell (`-ExecutionPolicy Bypass` because a default install
refuses to run `.ps1` files):

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\up.ps1
```

**Checkpoint.** The script waits for the API server, then prints the service
table. Open <http://localhost:8080> — there is no login. The Dags list is
**empty**: you have not written one yet. The Home page shows the Scheduler,
Triggerer and Dag processor as healthy. If the page does not load, jump to
[troubleshooting](../troubleshooting.md).

```bash
docker compose ps
docker compose exec airflow-scheduler airflow dags list
```

The second prints `No data found`, the same empty list. Every later command has
one of those two shapes: `docker compose …` runs on your machine, and the
`docker compose exec airflow-scheduler` prefix runs `airflow …` inside the
Scheduler container. Any Airflow container would do, since they share image,
settings and mounted folders and differ only in the component they run (§3);
the Scheduler is used because it is always running. A bare `airflow …` in a
transcript or in prose means the same thing, prefix included.

To keep fewer moving parts while you learn, the stack is smaller than the
[official docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/3.3.1/docker-compose.yaml):
`LocalExecutor` instead of Celery + Redis (§3 explains what an Executor is), and
the Simple auth manager instead of FAB — Flask-AppBuilder, the provider that
gives Airflow users, roles and a login page.

Two services in the `docker compose ps` list are not Airflow. `mlflow` is
MLflow (§1), with its UI at <http://localhost:5000>; you use it in §10.
`extra-databases` runs once at start-up, creates a database for Optuna (§10)
on the same Postgres, and exits. The rest of the data stack, a Google Cloud
project and Airbyte, you set up in §9.

> **Auth:** `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=true` removes the UI
> login, and port 8080 is published only on `127.0.0.1` (loopback), so nothing
> outside your machine can reach it. Do not deploy this configuration anywhere
> else.

Everything the stack accumulates (runs, task logs, the snapshots the Dags
write, MLflow's runs and models) survives a container restart: it lives in
Docker volumes and in this repo's `data/` and `logs/` folders (more in §3). To
throw it all away and start over, on Linux, macOS, WSL2 or Git Bash:

```bash
./scripts/reset.sh
```

or in Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\reset.ps1
```

It asks before deleting; `up.sh` / `up.ps1` brings the stack back. It leaves
alone `dags/`, so your files survive, and everything outside this repo's stack,
such as your BigQuery tables and Airbyte. No step in the tutorial needs it: it
is for the day something is stuck and [troubleshooting](../troubleshooting.md)
has not helped.

> **One shell for the rest.** Every later command is bash: Linux, macOS, WSL2,
> or Git Bash on Windows. The `.ps1` scripts only start and reset the stack;
> the other commands use `\` line continuations, `grep`, `#` comments and
> single-quoted JSON, which PowerShell and the Command Prompt (`cmd`) read
> differently. On Windows, use Git Bash from here on.
>
> On **Git Bash**, a few later commands pass a `/opt/...` path into a
> container and carry the prefix `MSYS_NO_PATHCONV=1`. Without it, Git Bash
> rewrites the path into a Windows one before Docker sees it.

> **Your editor.** From §4 on you create files in `dags/`, with any editor: the
> folder is bind-mounted into the containers, so a file saved on your machine
> is inside Airflow a moment later.

### Native (Linux / macOS / WSL2; §3 to §8 only)

```bash
./scripts/setup-native.sh
```

> **Why only §3 to §8.** From §9 on, the pipeline also needs the compose
> services (MLflow, Optuna's Postgres database) and the two virtualenvs the
> image builds for dbt and alibi-detect (§9, §12). You can recreate them by
> hand and point `config.py` at them (`PROJECT_TAGGER_DBT_BIN`,
> `PROJECT_TAGGER_ALIBI_PYTHON`, `MLFLOW_TRACKING_URI`, `OPTUNA_STORAGE`), but
> this tutorial does not cover it.

What that script runs:

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

Airflow 3.3.1 needs Python ≥ 3.10 (and not 3.15). The **constraint file** is
what makes the install work: Airflow's dependency tree is large, and an
unconstrained install resolves to versions that do not work together.

`airflow db migrate` populates `$AIRFLOW_HOME`:

```
airflow.cfg     every setting, with the defaults commented — see config/airflow.cfg.example
airflow.db      the SQLite metadata DB
logs/           task logs, one directory per dag_id / run_id / task_id
```

> **SQLite is for development only.** The Airflow docs say it "should NEVER be
> used for production". It is fine for this tutorial (`LocalExecutor` runs
> tasks in parallel against it, and needs SQLite ≥ 3.15), but use Postgres or
> MySQL for anything real. The Docker stack already runs Postgres 16.

Any `airflow.cfg` setting can also be given as an `AIRFLOW__SECTION__KEY`
environment variable: the `[header]` the setting sits under and the setting's
name, uppercased and joined by **double** underscores. An underscore *inside*
either name stays single:

```
airflow.cfg                          environment variable
─────────────────────────────────    ────────────────────────────────────────
[core]
dags_folder = <repo>/dags            AIRFLOW__CORE__DAGS_FOLDER=<repo>/dags

[dag_processor]
refresh_interval = 5                 AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=5
```

Both variables are exported by the script above, which configures Airflow
through the environment instead of editing `airflow.cfg`;
[`docker-compose.yaml`](../../docker-compose.yaml) does the same.

> **Default connections are not created for you.** The script adds
> `fs_default` because a `FileSensor` uses it unless told otherwise (§6). In
> Docker the compose file provides it as `AIRFLOW_CONN_FS_DEFAULT`.

Then either the all-in-one process:

```bash
airflow standalone
```

…or the components separately, as in a real deployment:

```bash
airflow api-server --port 8080
airflow scheduler
airflow dag-processor            # a separate process — required
airflow triggerer                # deferrable tasks and async callbacks
```

---

← [1. What you're building](01-what-you-re-building.md) · [Index](README.md) · [3. The pieces of Airflow](03-the-pieces-of-airflow.md) →
