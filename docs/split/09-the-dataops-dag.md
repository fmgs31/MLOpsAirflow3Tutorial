## 9. The DataOps Dag

`dags/03_dataops.py` loads the raw data into the warehouse, checks it, turns it
into the training table, and checks that. Each step is done by a tool built
for it; the Dag decides when each tool runs and stops the pipeline when a check
fails.

| Task | Tool | What it does |
|---|---|---|
| `extract_and_load_projects`, `extract_and_load_tags` | **Airbyte** | copies each source CSV into a table in the warehouse |
| — | **BigQuery** | the warehouse: stores the tables and runs SQL on them |
| `validate_projects`, `validate_tags`, `validate_transformed` | **Great Expectations** | checks a table against a list of rules |
| `transform` | **dbt** | builds the training table from the two raw tables, in SQL |

How the pieces talk to each other when the Dag runs. Each column is one
system, and what is written in a column happens in that system. An arrow is
one system calling another, or data going from one to the other:

```
 Airflow (Docker)              │ Airbyte (abctl)       │ BigQuery (Google Cloud)
 dataops Dag                   │ localhost:8000        │ dataset mlops_course
───────────────────────────────┼───────────────────────┼───────────────────────────
                               │                       │
 1 extract_and_load_projects ───► sync projects        │
   extract_and_load_tags ───────► sync tags            │
                               │  each sync reads its  │
                               │  CSV from GitHub and ──► tables projects, tags
                               │  writes it            │
                               │                       │
 2 validate_projects (GX) ◄────────────────────────────── rows of projects
   validate_tags (GX) ◄────────────────────────────────── rows of tags
                               │                       │
 3 transform: dbt build ────────────────────────────────► runs the model's SQL:
   dbt runs here, in the       │                       │  a JOIN that writes the
   Airflow container           │                       │  table labeled_projects
   then it sends the tests ─────────────────────────────► runs the tests
                               │                       │
 4 validate_transformed (GX) ◄─────────────────────────── rows of labeled_projects
   │ Asset event (§8)          │                       │
   ▼                           │                       │
 mlops Dag (§10)               │                       │
```

Airflow starts each step and waits for it to finish. Only the GX tasks bring
data into Airflow:

1. Airflow asks Airbyte's API to run a sync. Airbyte downloads the CSV and
   writes it to BigQuery; the rows never pass through Airflow.
2. Each GX task reads a table from BigQuery into the Airflow container, as a
   pandas DataFrame, and checks it there.
3. dbt wraps your `SELECT` in a `CREATE TABLE … AS` and sends it to BigQuery,
   which joins its own two tables and writes the third. Then dbt sends the
   project's tests, also as SQL. You create that dbt project below.
4. The same check as in step 2, on the new table. When it passes, Airflow
   records the Asset event that starts `mlops`.

GX and dbt are in Airflow's column, and Airbyte has its own, because they are
different kinds of software:

- **Airbyte** is a server, with its own API. Airflow does not run it; it asks
  it to do the work.
- **GX Core** is a Python library, installed in the Airflow image like pandas.
  The task imports it and validates inside its own process.
- **dbt Core** is a command-line program, also installed in the Airflow image.
  The task runs it as a command, and BigQuery does the heavy work.

GX and dbt also have paid, hosted versions, GX Cloud and dbt Cloud, that are
services. With those, the task would call an API, as it does with Airbyte
(see "Alternatives" at the end of this section).

Loading the raw data first and transforming it inside the warehouse is called
**ELT** (extract, load, transform). Because the raw tables are kept, a buggy
transformation can be fixed and re-run without extracting again.

It is also the first Dag here with a real cron, `schedule="0 3 * * *"` (§7).
Once you un-pause it in §10 it runs by itself: once immediately, for the most
recent slice its cron has already passed, then at 03:00 UTC every day, each
run triggering `mlops`. (`monitoring`, §12, runs at 06:00.) The `scheduled__`
runs you find after leaving the stack up overnight are these, not a bug.

### Set up the data stack

The tools need accounts, credentials and some configuration before a Dag can
call them. You do this once, in order: each step checks the one before it.

#### 1. A Google Cloud project with BigQuery

1. **Sign up for Google Cloud** at <https://cloud.google.com>. New accounts get
   the Free Trial: a 90-day credit. It asks for a card and does not charge it
   unless you upgrade the account yourself. After the trial, the free tier
   still covers 10 GB of storage and 1 TB of queries a month; this tutorial's
   tables are under 1 MB.

   BigQuery also has a *sandbox* that needs no card, but it does not allow
   writing rows into existing tables (streaming inserts, and SQL's `INSERT`,
   `UPDATE` and `MERGE`), which is how Airbyte loads data. So the project needs
   a billing account: the Free Trial's or your own.

2. **Create a project.** In the console's project picker, at the top of the
   page, choose **New project** and name it `mlops-tutorial`. Google appends a
   suffix to make the **Project ID** unique, for example `mlops-tutorial-123456`.
   Every tool asks for that ID, not the name.

3. **Create a service account**: an identity for programs, so the tools do not
   use your own Google account. Go to **IAM & Admin → Service Accounts → Create
   service account**, name it `airflow-tutorial`, and give it two roles:
   **BigQuery Data Editor** (create datasets and tables, write rows) and
   **BigQuery Job User** (run queries).

4. **Download its key.** In **IAM & Admin → Service Accounts**, click the
   service account's email, `airflow-tutorial@<project id>.iam.gserviceaccount.com`,
   to open it. Then open its **Keys** tab and choose **Add key → Create new
   key → JSON**. The browser downloads a file. Move it into this
   repo as `secrets/gcp-key.json`. Git ignores everything in `secrets/` except
   `.gitkeep`, so the key cannot be committed by accident.

   > **That file is a password.** Whoever has it can read and write your
   > BigQuery data and run queries billed to your project. Do not paste it into
   > chats or commit it anywhere. If it leaks, delete the key in **Keys** and
   > create a new one.

5. **Tell the stack which project.** Set `GCP_PROJECT_ID` in `.env` to your
   Project ID. `BQ_LOCATION` says where BigQuery keeps the data: `US` (the
   default), `EU`, or a single region near you, such as
   `australia-southeast1` ([the full list](https://cloud.google.com/bigquery/docs/locations)).
   This tutorial's data is small enough that any of them works. Whichever you
   pick, the Airbyte destination you create in "2. Airbyte" needs the same
   value in its **Dataset location** field. Choose before the first sync: it
   creates the dataset, and a dataset's location cannot change. Then recreate the containers so they read the new value:

   ```bash
   docker compose up -d
   ```

   The compose file passes `GCP_PROJECT_ID` and the key's path
   (`GOOGLE_APPLICATION_CREDENTIALS`) to every container. Every Google client
   in the stack reads those two: BigQuery's Python client, dbt, and the
   `google_cloud_default` Connection the compose file defines
   (`AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT`).

`dbt debug`, in "4. Create the dbt project", checks that the key works.

#### 2. Airbyte

Airbyte does not run from this repo's compose file. Its installer, `abctl`,
runs it in a small Kubernetes cluster that it creates inside Docker. Airbyte
asks for 4 CPUs and 8 GB of memory on top of what the Airflow stack uses
(about 4 GB), so Docker needs about 12 GB in all. See what it has now, in the
`CPUs` and `Total Memory` lines:

```bash
docker info
```

If it is short, where to raise it depends on how Docker runs:

- **Docker Desktop on macOS or Linux:** **Settings → Resources → Advanced**,
  the CPU and memory limits.
- **Docker Desktop on Windows with the WSL2 backend** (the default): that page
  has no limits to set, because Docker gets what WSL2 gets: every CPU, and half
  the machine's memory. To give WSL2 more, create or edit the file
  `.wslconfig` in your Windows user folder (`%UserProfile%`):

  ```ini
  [wsl2]
  memory=12GB
  ```

  Then stop WSL2 so it restarts with the new limit, and start Docker Desktop
  again:

  ```powershell
  wsl --shutdown
  ```
- **Docker Engine on Linux:** there is no limit; containers can use the whole
  machine.

1. **Install `abctl`.** On Linux or macOS:

   ```bash
   curl -LsfS https://get.airbyte.com | bash -
   ```

   On Windows, download the `windows-amd64` zip (`windows-arm64` on an ARM
   machine) from the [abctl releases page](https://github.com/airbytehq/abctl/releases/latest),
   extract it, add the folder to your `Path`, and open a new terminal, which
   is when `Path` is read.

2. **Install Airbyte.** It downloads several GB and takes a few minutes:

   ```bash
   abctl local install
   ```

   Airbyte is then at <http://localhost:8000>. On a machine short of memory,
   `abctl local install --low-resource-mode` runs it with 2 CPUs.

3. **Get the credentials and log in:**

   ```bash
   abctl local credentials
   ```

   It prints four values. `Password` is the UI's password. `Client-Id` and
   `Client-Secret` let Airflow use Airbyte's API ("3. Tell Airflow how to
   reach Airbyte"). `Email` says `[not set]` until the first login sets it.

   Open <http://localhost:8000>. The first time, Airbyte asks for an email and
   an organization name: enter your email and any name, and choose **Get
   Started**. Then log in with that email and the `Password` value.

4. **Create two sources**, one per CSV. A *source* is where Airbyte reads data
   from. **Sources → New source → File**, then:

   | Field | First source | Second source |
   |---|---|---|
   | Dataset name | `projects` | `tags` |
   | File format | `csv` | `csv` |
   | Storage provider | HTTPS: Public Web | HTTPS: Public Web |
   | URL | `https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/projects.csv` | `https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/tags.csv` |

   The dataset name becomes the table name in BigQuery. The files are in this
   repo's [`datasets/`](../../datasets/): the projects, and their tags in a
   separate table, joined by `id`.

5. **Create the destination**, where Airbyte writes. **Destinations → New
   destination → BigQuery**:

   | Field | Value |
   |---|---|
   | Project ID | your Project ID |
   | Dataset location | the same value as `BQ_LOCATION` |
   | Default dataset ID | `mlops_course` |
   | Loading method | Batched Standard Inserts |
   | Service account key JSON | the whole content of `secrets/gcp-key.json` |

   Airbyte tests the destination when you save it. The `mlops_course` dataset
   (BigQuery's name for a group of tables) is created on the first sync.

6. **Create two connections**: a *connection* links one source to one
   destination, and each run of it is a *sync*. Create `projects → BigQuery`
   and `tags → BigQuery`, the same way: **Connections → New connection**,
   choose the source, then the BigQuery destination. Two screens follow:
   - **The streams screen.** A *stream* is one table the source offers; a
     File source offers one, named after its dataset. Leave it selected and
     set its sync mode to **Full Refresh Overwrite**, so that each sync
     replaces the table instead of appending to it. Choose **Next**.
   - **Configure connection.** Set **Schedule type** to **Manual**, so that
     Airflow decides when it runs, and **Destination Namespace** to
     **Destination-defined**, so the table lands in `mlops_course`. Leave the
     rest as it is, and choose **Set up connection**.

7. **Sync each connection once by hand**, with **Sync now**. Then open BigQuery
   in the Google Cloud console: `mlops_course` has a `projects` table and a
   `tags` table, 764 rows each. Besides the CSV's columns, each has a few
   beginning `_airbyte_`, which Airbyte uses to track loads; this repo's code
   reads only the CSV's columns.

8. **Copy each connection's ID.** Airbyte does not show it on the page: it
   is in the browser's address bar. Open the connection from **Connections**,
   and the ID is the UUID right after `/connections/`:

   ```
   http://localhost:8000/workspaces/<workspace id>/connections/<connection id>/status
   ```

   Keep both IDs, `projects → BigQuery`'s and `tags → BigQuery`'s, for step 3.

#### 3. Tell Airflow how to reach Airbyte

Airflow reaches Airbyte through a Connection (§6), with the `Client-Id` and
`Client-Secret` that `abctl local credentials` printed:

```bash
docker compose exec airflow-scheduler airflow connections add airbyte \
    --conn-type airbyte \
    --conn-host http://host.docker.internal:8000/api/public/v1/ \
    --conn-login <client-id> --conn-password <client-secret>
```

**Checkpoint.** The last line is ``Successfully added `conn_id`=airbyte``.
Above it come many lines of warnings. Most are `DeprecatedImportWarning` and
`SyntaxWarning`, raised while the CLI loads every provider in the image; they
are noise. The one before the last, `empty cryptography key - values will not
be stored encrypted`, is the missing Fernet key from §6: on this stack,
Connections are stored unencrypted.

Inside a container, `localhost` is the container itself;
`host.docker.internal` is your machine. Docker Desktop defines that name, and
the compose file's `extra_hosts` line defines it on Linux.

The two connection IDs you copied in step 8 of "2. Airbyte" go in Variables (§6), so the Dag file contains no ID
that exists only in your Airbyte:

```bash
docker compose exec airflow-scheduler airflow variables set airbyte_projects_connection_id <projects connection id>
docker compose exec airflow-scheduler airflow variables set airbyte_tags_connection_id <tags connection id>
```

#### 4. Create the dbt project

**dbt** runs SQL `SELECT` statements, called **models**, and saves each result
as a table in the warehouse. A **dbt project** is the folder dbt works from:
the models, plus files saying how to connect and what to check. You create one
now, in `dbt/`, the folder the compose file mounts for it. Here you write it and run it once by hand; when
the Dag runs, its `transform` task runs the same `dbt build` (step 3 in the
diagram at the start of §9).

dbt is installed in its own virtualenv in the image, `/opt/venvs/dbt`, because
its dependencies pin versions that clash with Airflow's (the
[`Dockerfile`](../../Dockerfile) explains). Every dbt command below runs it
from there.

**Create the skeleton.** `dbt init` writes a project folder with the standard
layout; `--skip-profile-setup` skips its interactive questions, because you
write the connection file yourself:

```bash
# Git Bash needs the MSYS_NO_PATHCONV prefix on any /opt/... path (§2)
MSYS_NO_PATHCONV=1 docker compose exec -w /opt/airflow/dbt airflow-scheduler \
    /opt/venvs/dbt/bin/dbt init dbt_transforms --skip-profile-setup
```

**Checkpoint.** On your machine, `dbt/dbt_transforms/` now exists, with
`dbt_project.yml`, a `models/` folder and some empty folders this tutorial
does not use (`macros/`, `seeds/`, `snapshots/`, `tests/`, `analyses/`).

**Remove the example models** dbt adds. From the repo root:

```bash
rm -r dbt/dbt_transforms/models/example
```

**Make every model a table.** `dbt_project.yml` holds the project's settings.
At its end, replace the `models:` block that configures the deleted examples
with:

```yaml
models:
  dbt_transforms:
    +materialized: table
```

A model can be saved as a *view* (a stored query, re-run each time it is read)
or a *table* (the result, stored). A table is rebuilt from scratch on every
run, so running dbt twice leaves the same table. A third option, *incremental*,
adds only the new rows to the existing table instead of replacing it; this
tutorial does not use it.

**Say how to connect.** dbt reads its connection from a *profile*. Create
`dbt/dbt_transforms/profiles.yml`:

```yaml dbt/dbt_transforms/profiles.yml
dbt_transforms:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      keyfile: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
      project: "{{ env_var('GCP_PROJECT_ID') }}"
      dataset: "{{ env_var('BQ_DATASET', 'mlops_course') }}"
      location: "{{ env_var('BQ_LOCATION', 'US') }}"
      threads: 4
```

The top key, `dbt_transforms`, is the profile name `dbt_project.yml` asks for.
`{{ env_var(...) }}` is Jinja, the same templating Airflow uses (§6): each value
is read from the container's environment, so the file holds no secret and no
project ID. Check the connection, which also checks your key,
`secrets/gcp-key.json`:

```bash
MSYS_NO_PATHCONV=1 docker compose exec airflow-scheduler /opt/venvs/dbt/bin/dbt debug \
    --project-dir /opt/airflow/dbt/dbt_transforms --profiles-dir /opt/airflow/dbt/dbt_transforms
```

**Checkpoint.** The last line is `All checks passed!`. A failure names the
cause: a wrong project ID, no file at `secrets/gcp-key.json`, or a service
account missing a role.

**Declare the raw tables.** A *source* in dbt is a table already in BigQuery
that dbt reads but does not build. Here the sources are `projects` and `tags`
in the `mlops_course` dataset, the tables you loaded in step 2. Declaring them
once lets models refer to them by name. Create
`dbt/dbt_transforms/models/sources.yml`:

```yaml dbt/dbt_transforms/models/sources.yml
version: 2

sources:
  - name: mlops_course
    database: "{{ env_var('GCP_PROJECT_ID') }}"
    schema: "{{ env_var('BQ_DATASET', 'mlops_course') }}"
    tables:
      - name: projects
      - name: tags
```

In BigQuery terms, `database` is the project and `schema` is the dataset.

**Write the model.** A model's file name is the name of the table it builds.
Create `dbt/dbt_transforms/models/labeled_projects/labeled_projects.sql`:

```sql dbt/dbt_transforms/models/labeled_projects/labeled_projects.sql
SELECT
    p.id,
    CAST(p.created_on AS TIMESTAMP) AS created_on,
    p.title,
    p.description,
    t.tag
FROM {{ source('mlops_course', 'projects') }} AS p
LEFT JOIN {{ source('mlops_course', 'tags') }} AS t
    ON p.id = t.id
```

`{{ source('mlops_course', 'projects') }}` compiles to the full table name,
`` `<project id>.mlops_course.projects` ``. In this data every project has a
tag. If a later load brings one without a tag, a plain `JOIN` would drop it
without a trace; the `LEFT JOIN` keeps it with `tag` set to `NULL`, so the
`not_null` test below fails.

**Add tests.** A dbt *test* is a check on a model's columns, run after the
model is built. Create `dbt/dbt_transforms/models/labeled_projects/schema.yml`:

```yaml dbt/dbt_transforms/models/labeled_projects/schema.yml
version: 2

models:
  - name: labeled_projects
    columns:
      - name: id
        data_tests:
          - unique
          - not_null
      - name: title
        data_tests:
          - not_null
      - name: tag
        data_tests:
          - not_null
```

**Build it.** `dbt build` builds every model, then runs its tests:

```bash
MSYS_NO_PATHCONV=1 docker compose exec airflow-scheduler /opt/venvs/dbt/bin/dbt build \
    --project-dir /opt/airflow/dbt/dbt_transforms --profiles-dir /opt/airflow/dbt/dbt_transforms
```

**Checkpoint.** `Done. PASS=5 WARN=0 ERROR=0 SKIP=0 TOTAL=5`: one model and
four tests. BigQuery has a third table, `labeled_projects`, with 764 rows. Run
the command again: same result, same table, because `+materialized: table`
makes dbt replace the table on every run. A failing test makes `dbt build`
exit with an error, which is what will fail the Dag's `transform` task.

Stuck? The finished project is in
[`solutions/dbt_transforms/`](../../solutions/dbt_transforms/); copy it with
`cp -r solutions/dbt_transforms dbt/`.

#### 5. Great Expectations

dbt's tests check the table dbt builds. **Great Expectations** (GX) checks the
raw tables too, before anything is built on them, with a wider choice of
rules. An **expectation** is one rule ("`id` is unique"); a **suite** is a
named list of expectations; **validating** a table against a suite gives a
pass or fail per expectation.

The suites are code you call, not write, in
[`src/project_tagger/validate.py`](../../src/project_tagger/validate.py), one
per table. The one for `projects`:

```python
# src/project_tagger/validate.py — build_suite("projects")
e.ExpectTableColumnsToMatchSet(column_set=["id", "created_on", "title", "description"]),
e.ExpectColumnValuesToBeUnique(column="id"),
e.ExpectColumnValuesToNotBeNull(column="id"),
e.ExpectColumnValuesToNotBeNull(column="title"),
e.ExpectCompoundColumnsToBeUnique(column_list=["title", "description"]),  # one project, two ids
e.ExpectTableRowCountToBeBetween(min_value=500),
```

`validate.run_suite(df, "projects")` validates a dataframe against that suite
and returns a result with `.passed`, `.failures`, `.results`, `.suite` and a
`.summary()` line. (GX also has Airflow operators, but their package cannot be
installed next to Airflow 3.3.1, so the tasks call GX directly.)

Each expectation is *critical* unless marked otherwise. A failed critical
expectation fails the suite; a failed non-critical one is only reported. The
`tags` suite has one non-critical expectation: every tag is one of the six
known ones. A new tag is a reason to look at the data, not to stop the
pipeline, so it shows up in `result.failures` but leaves `result.passed` true.

### Build it

The functions this Dag calls, in `src/project_tagger/`: `data.load_projects()`,
`data.load_tags()` and `data.load_labeled()` read a warehouse table into a
dataframe (CSV columns only); `validate.run_suite` runs a suite; `config`
holds names and paths, from table names to the dbt executable.

Create `dags/03_dataops.py`:

```python dags/03_dataops.py
"""Lesson 3 - the DataOps Dag: extract & load -> validate -> transform -> validate."""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task, task_group
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk.types import OutletEventAccessorsProtocol

from project_tagger import config, data, validate
from project_tagger.assets import LABELED_PROJECTS, RAW_PROJECTS, RAW_TAGS

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}


@dag(
    dag_id="dataops",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "dataops"],
    doc_md=__doc__,
)
def dataops():
```

`AirbyteTriggerSyncOperator` comes from the Airbyte provider and `BashOperator`
from the standard one (§6); the image installs the Airbyte and Google
providers (`requirements-image.txt`). `AirflowFailException` and
`OutletEventAccessorsProtocol`, the type of `outlet_events` (§8), come from the
Task SDK (§4). `from project_tagger import …` works because the compose file
puts `/opt/airflow/src` on every service's `PYTHONPATH`: the Dag processor
needs it to parse the file, and the processes that run tasks need it too.

`max_active_runs=1` caps the Dag at one run at a time; a second run waits in
`queued` until the first is done.

The first two tasks start the Airbyte syncs and wait for them to finish. A sync
can take minutes, so they are **deferrable**: while waiting, the task is in
the `deferred` state (§3) and the triggerer checks on Airbyte instead of a
worker slot (§11 covers deferral in full). Add them inside `dataops()`:

```python dags/03_dataops.py
    extract_and_load_projects = AirbyteTriggerSyncOperator(
        task_id="extract_and_load_projects",
        airbyte_conn_id="airbyte",
        connection_id="{{ var.value.airbyte_projects_connection_id }}",
        deferrable=True,
        timeout=3600,
        outlets=[RAW_PROJECTS],
    )
    extract_and_load_tags = AirbyteTriggerSyncOperator(
        task_id="extract_and_load_tags",
        airbyte_conn_id="airbyte",
        connection_id="{{ var.value.airbyte_tags_connection_id }}",
        deferrable=True,
        timeout=3600,
        outlets=[RAW_TAGS],
    )
```

`airbyte_conn_id` names the `airbyte` Connection you added with
`airflow connections add`. `connection_id` is the
Airbyte connection to sync, read from a Variable by a template (§6), rendered
when the task runs. A failed sync fails the task. Each task's outlet is a raw
table's asset (§8); no Dag is scheduled on those, because raw data has not
been checked yet.

Then a **gate** on each raw table: a task that fails, stopping everything
downstream, when the table fails its suite:

```python dags/03_dataops.py
    @task
    def validate_projects() -> dict[str, Any]:
        result = validate.run_suite(data.load_projects(), "projects")
        print(result.summary())
        if not result.passed:
            raise AirflowFailException(f"projects failed validation: {result.failures}")
        return {"suite": result.suite, "results": result.results}

    @task
    def validate_tags() -> dict[str, Any]:
        result = validate.run_suite(data.load_tags(), "tags")
        print(result.summary())
        if not result.passed:
            raise AirflowFailException(f"tags failed validation: {result.failures}")
        return {"suite": result.suite, "results": result.results}
```

`AirflowFailException` fails the task without retrying, whatever `retries`
says. Bad data stays bad on a retry, so retrying would only delay the alert.
The tasks return per-expectation results, small dicts; the tables themselves
never go through XCom (§6).

Then `transform`, which runs the `dbt build` you ran by hand, and the final
gate, whose outlet is the asset `mlops` will be scheduled on:

```python dags/03_dataops.py
    transform = BashOperator(
        task_id="transform",
        bash_command=(
            f"{config.DBT_BIN} build"
            f" --project-dir {config.DBT_PROJECT_DIR}"
            f" --profiles-dir {config.DBT_PROJECT_DIR}"
        ),
    )

    @task(outlets=[LABELED_PROJECTS])
    def validate_transformed(*, outlet_events: OutletEventAccessorsProtocol) -> dict[str, Any]:
        df = data.load_labeled()
        result = validate.run_suite(df, "labeled_projects")
        print(result.summary())
        if not result.passed:
            raise AirflowFailException(f"labeled_projects failed validation: {result.failures}")
        outlet_events[LABELED_PROJECTS].extra = {
            "rows": len(df),
            "tags": sorted(df["tag"].unique().tolist()),
            "warnings": [r["expectation"] for r in result.failures],
        }
        return {"suite": result.suite, "rows": len(df), "results": result.results}
```

A `BashOperator` fails when its command exits with an error, so a failing dbt
test fails `transform`, and dbt's output is the task log. The outlet is on the
final validation, not on `transform`, as §8 explained: the event then means
"good data arrived". The GX suite checks what dbt's tests do not, such as the
row count and the set of tags.

Last, a **task group** and the wiring. `@task_group` draws several tasks as one
collapsible node in the Graph view:

```python dags/03_dataops.py
    @task_group(group_id="quality_report")
    def quality_report(reports: list[dict[str, Any]]):

        @task
        def summarize(reports: list[dict[str, Any]]) -> None:
            for report in reports:
                failed = [r["expectation"] for r in report["results"] if not r["passed"]]
                print(f"{report['suite']}: {len(report['results'])} checks, failed={failed or 'none'}")

        summarize(reports)

    projects_report = validate_projects()
    tags_report = validate_tags()
    extract_and_load_projects >> projects_report
    extract_and_load_tags >> tags_report

    curated_report = validate_transformed()
    [projects_report, tags_report] >> transform >> curated_report

    quality_report([projects_report, tags_report, curated_report])


dataops()
```

No data passes from the Airbyte tasks to the gates, or from the gates to
`transform`: each reads the warehouse itself. So those edges are drawn with
`>>` (§6). `[projects_report, tags_report] >> transform` is a **fan-in**:
`transform` joins the two tables, so it waits for both gates. `quality_report`
receives the three results as arguments, and that data flow draws its edges.

### See the assets appear

Save the file. Within seconds `airflow dags list` has a third row, and the
asset list has changed too:

```bash
docker compose exec airflow-scheduler airflow assets list -o plain
```

**Checkpoint.** Three rows, `labeled_projects`, `raw_projects` and `raw_tags`,
each with a URI such as `bigquery://mlops-tutorial-123456/mlops_course/projects`:
the table that asset stands for. Nothing has run; parsing a file with `outlets=` was
enough. `production_model`, also in `assets.py`, is missing because no Dag
refers to it yet. The Assets page shows the three, produced by `dataops`, with
no consumer.

### Run it

```bash
docker compose exec airflow-scheduler airflow dags test dataops
```

It takes a minute or two, mostly the two Airbyte syncs, which run one after
the other because `airflow dags test` runs one task at a time.

**Checkpoint.** In the log, in order: each Airbyte task reporting its job done;
`suite=projects passed=6/6` and `suite=tags passed=5/5`; dbt's
`Done. PASS=5 WARN=0 ERROR=0 SKIP=0 TOTAL=5` in `transform`'s log;
`suite=labeled_projects passed=6/6`; and from the task group,
`projects: 6 checks, failed=none` and the same for the other two.

`airflow dags test` has no triggerer, so it runs the Airbyte tasks' triggers
itself and logs `[DAG TEST] running trigger in line` (§4). In a normal
run the triggerer runs those triggers, and the tasks sit in `deferred` while
Airbyte works.

A failed task? Compare with [`solutions/03_dataops.py`](../../solutions/03_dataops.py).
A failed Airbyte task usually means a wrong Connection or Variable: check with
`airflow connections get airbyte` and `airflow variables list`.

> **`airflow dags test` is not a dry run.** Airbyte re-synced both tables, dbt
> rebuilt `labeled_projects`, and a real `labeled_projects` asset event is in
> the metadata database, with your `extra`, on the Assets page. Nothing is
> scheduled on that asset yet, so nothing else happens, and per §8 nothing
> will: when `mlops` appears in §10 this event is already in the past, and
> Airflow does not replay past events.

Running the Dag twice leaves the warehouse as running it once did: each sync
replaces its table, and dbt rebuilds `labeled_projects` from scratch. That
property, **idempotence** (§6), is what makes `retries=2` safe here.

### Backfills

Now that you have a Dag with a schedule, backfills make sense. A backfill
creates the runs for every slice in a date range, after the fact. In Airflow a
backfill is an object of its own, with its own API, UI form and reprocess
policy (further down).
**Dry-run it first** — a backfill over the wrong date range is the easiest way
to create thirty runs by accident:

```bash
docker compose exec airflow-scheduler airflow backfill create \
    --dag-id dataops --from-date 2026-02-01 --to-date 2026-02-04 --dry-run
```

**Checkpoint.** A table headed `Runs to be attempted:` with three rows —
2026-02-01, 02 and 03, each at 03:00 — and nothing created. Each row is one
slice of `dataops`'s daily cron. The range includes both ends, but
`--to-date 2026-02-04` means 00:00 that day, before the cron fires at 03:00, so
02-04 is not in it.

Drop `--dry-run` and the same command creates those three runs, with run ids
beginning `backfill__`. Know what follows before you do it:

- The runs are created **queued**, and if the Dag is paused they stay that way
  — the §7 rule — until you un-pause it. Then they start, at most as many at
  a time as `backfill create`'s `--max-active-runs` option says.
- Every `dataops` backfill run that succeeds emits a `labeled_projects` asset
  event exactly as a normal run does (§8), so an un-paused `mlops` trains after
  them.
- Each of them re-syncs both tables from the source as it is *now*. Nothing in
  this pipeline reads a date: the source has no history to go back to, so a
  backfill here re-runs the work rather than recovering past data. A pipeline
  whose source can be queried by date passes the run's `data_interval_start`
  and `data_interval_end` (§7) to it, and that is where backfills earn their keep.
- The CLI in 3.3.1 only *creates* backfills. While one is unfinished, the
  Dag's page shows a *Backfill in progress* banner with a pause and a stop
  button; stop cancels it. So does the API — `PUT
  /api/v2/backfills/{id}/cancel`, no token needed on this all-admins stack.
  Cancelling marks the backfill's still-queued runs `failed`. Pausing the Dag is the emergency
  brake: a paused Dag starts nothing.

`--reprocess-behavior` decides what to do with slices that already have a run —
`none`, `failed` or `completed` — which is the difference between "fill the
gaps" and "redo the month". In the UI: Dag → **Trigger** → **Backfill**, which
dry-runs the range and tells you how many runs it will create before you
commit. This is also why an asset-scheduled Dag cannot be backfilled (§5): it
has no slices, only events.

### Alternatives

Each tool here is one choice among several, and swapping one changes the
operator in its task, not the shape of the Dag:

| Here | Alternatives | In Airflow |
|---|---|---|
| Airbyte | Fivetran, Meltano, dlt | `FivetranOperator`, a `BashOperator`, a `@task` |
| BigQuery | Snowflake, Redshift, Databricks, Postgres | each has a provider with hooks and operators |
| Great Expectations | Soda, Pandera, dbt tests alone | `SodaScanOperator`, a `@task` |
| dbt Core in a `BashOperator` | dbt with astronomer-cosmos, dbt Cloud | Cosmos turns each model and test into its own task; `DbtCloudRunJobOperator` starts a dbt Cloud job |

Cosmos is worth knowing: with `dbt build` in one task, a single failing test
fails the whole task, and a retry rebuilds everything; with one task per model,
the Graph shows which model broke and a retry re-runs only that one.

---

← [8. Assets: scheduling on data, not on time](08-assets-scheduling-on-data-not-on-time.md) · [Index](README.md) · [10. The MLOps Dag](10-the-mlops-dag.md) →
