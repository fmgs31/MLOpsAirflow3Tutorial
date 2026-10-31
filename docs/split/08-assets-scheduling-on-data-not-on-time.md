## 8. Assets: scheduling on data, not on time

An **Asset** is a name you give to a piece of data; for example,
`labeled_projects`, the training table `dataops` builds in the warehouse. Airflow does not read
that data: it only knows the name, and records each time the data is updated.

`dataops` and `mlops` share their assets, so you define them once, in a
package both import. It is orchestration code, so it goes with the Dags, not
in `src/project_tagger/`, the ML project, which imports no Airflow (§1).

### Where the assets live

When Airflow parses a Dag file, it puts the Dags folder on `sys.path`, the
list of folders Python imports from, so a package inside `dags/` can hold the
code several Dags share. Airflow's documentation on
[modules management](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/modules_management.html)
sets three rules for it:

- The package gets a name of its own: here, `project_tagger_orchestration`.
  Every folder on `sys.path` shares one set of top-level names, so a generic
  name such as `common` or `utils` can clash with an installed module of the
  same name.
- Dags import it by its full path,
  `from project_tagger_orchestration.assets import …`, never with a relative import.
- It is listed in `.airflowignore`, so the Dag processor does not parse it
  looking for Dags.

Create `dags/project_tagger_orchestration/` with an `__init__.py`, the file
that makes a folder a package:

```python dags/project_tagger_orchestration/__init__.py
"""Code shared by this tutorial's Dags. Not a Dag: `.airflowignore` keeps the Dag processor out."""
```

Then create `dags/.airflowignore`. Each line is a pattern of files or
folders for the Dag processor to skip; this one skips the package:

```text dags/.airflowignore
project_tagger_orchestration/
```

### Define the assets

The assets go in `dags/project_tagger_orchestration/assets.py`, starting with
`labeled_projects`:

```python dags/project_tagger_orchestration/assets.py
"""The Assets the tutorial's Dags produce and consume."""

from __future__ import annotations

from airflow.sdk import Asset

from project_tagger import config

LABELED_PROJECTS = Asset(
    name="labeled_projects",
    uri=config.bigquery_uri(config.LABELED_PROJECTS_TABLE),  # bigquery://<project>/mlops_course/labeled_projects
    group="dataset",
    extra={"layer": "warehouse", "owner": "data-engineering"},
)
```

`name` identifies the asset in your Dags. `uri` is where the real data is:
here the BigQuery table, in the `bigquery://` scheme the Google provider
registers. `config.bigquery_uri` builds it from your Project ID (§9) and the
table name, which live in `project_tagger.config` because the ML code reads
them too. `from project_tagger import config` works because the compose file
puts `/opt/airflow/src` on every service's `PYTHONPATH`. `group` and `extra`
are descriptions the UI shows on the asset's page; this `extra` describes the
asset, not any one event.

A URI is optional. `production_model`, the asset `mlops` emits when it
promotes a model (§10), has only a name: the model lives in MLflow's registry,
and Airflow knows no URI scheme (like `bigquery://` or `gs://`) that points
there. When an asset's data does sit somewhere with a known scheme, give the
asset a URI: the UI then shows where that data lives, and tools that read
Airflow's lineage can trace it.

Add `production_model` at the end of `assets.py`, with the two raw tables
Airbyte loads, which `dataops` produces in §9:

```python dags/project_tagger_orchestration/assets.py
RAW_PROJECTS = Asset(
    name="raw_projects",
    uri=config.bigquery_uri(config.PROJECTS_TABLE),
    group="dataset",
    extra={"layer": "raw", "owner": "data-engineering"},
)
RAW_TAGS = Asset(
    name="raw_tags",
    uri=config.bigquery_uri(config.TAGS_TABLE),
    group="dataset",
    extra={"layer": "raw", "owner": "data-engineering"},
)

PRODUCTION_MODEL = Asset(
    name="production_model",
    group="model",
    extra={"owner": "ml-engineering"},
)
```

That makes four assets, and Airflow knows none of them yet: an asset exists
for Airflow only once a parsed Dag refers to it, as an outlet or a schedule,
and `assets.py` is not a Dag. Right now `airflow assets list`
prints `No data found`, and the Assets page is empty; §9 changes that as soon
as the Dag processor parses the first file you save there.

### Producers and consumers

In a Dag you refer to an asset by its object, imported from the package:
`from project_tagger_orchestration.assets import LABELED_PROJECTS`. Using it
takes two sides.

**Who produces the asset.** A task declares that it produces an asset by
listing it in its decorator's `outlets=` argument:

```python
@task(outlets=[LABELED_PROJECTS])
```

The assets in that list are the task's **outlets**: the data the task declares
it writes. (The opposite, `inlets=`, declares what it reads; this tutorial does
not use it.) The task is its outlets' **producer**. Each time the task
succeeds, Airflow records an **asset event**: the record that the asset has
just been updated.

**Who consumes it.** A Dag declares that it consumes an asset by putting it in
its `schedule=`:

```python
@dag(schedule=[LABELED_PROJECTS])
```

That Dag is the asset's **consumer**, and each new asset event starts a run of
it. That replaces any "run 30 minutes after the other Dag, and hope" cron.

The asset event itself is a row in the Metadata database: which asset, which
Dag run produced it, when, and an optional `extra` dict the producing task can
fill in. A task that fails writes no event, so a consumer cannot be started by
a producer that did not finish.

In this tutorial the producer of `labeled_projects` is `validate_transformed`,
the last step of `dataops` in §1's diagram, which you write in §9.
The outlet goes on that **validation** task, not on `transform`, the task
before it that builds the table: if the validation fails, the task writes no
event, so `mlops` cannot be started by data that did not pass it. **The
consumer is scheduled on "good data arrived", not on a cron that guesses when
the producer's Dag will have finished.**

While a task runs, its asset event does not exist yet: Airflow writes it once
the task succeeds. **`outlet_events`** is how the task prepares that event
beforehand. It works like a dict with one key per outlet of the task:
`outlet_events[LABELED_PROJECTS]` stands for the event the task is about to
emit for `labeled_projects`, and the dict you assign to its `.extra` is stored
inside that event: here, how many rows the task validated and which tags it
found. The task receives `outlet_events` by declaring a parameter with that
name, the way it receives `ti` in §6; a task that never touches it still emits
its events, with an empty `extra`:

```python
@task(outlets=[LABELED_PROJECTS])
def validate_transformed(*, outlet_events: OutletEventAccessorsProtocol) -> dict:
    df = data.load_labeled()
    result = validate.run_suite(df, "labeled_projects")
    if not result.passed:
        raise AirflowFailException(...)
    outlet_events[LABELED_PROJECTS].extra = {"rows": len(df), "tags": sorted(df["tag"].unique())}
    return {...}
```

The type of `outlet_events` is `OutletEventAccessorsProtocol`, from
`airflow.sdk.types`. This `extra` is different on each event, unlike the
asset's fixed one. `AirflowFailException` fails the task without retries; §9
explains it.

And the consumer, `mlops`, which you write in §10. There
`triggering_asset_events` — also injected by name — holds the events that
started the run:

```python
@dag(schedule=[LABELED_PROJECTS], ...)
def mlops():
    @task
    def prepare_data(*, triggering_asset_events: Mapping[Asset, Sequence[Any]]):
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")
```

`triggering_asset_events` is a read-only mapping from each asset in the
`schedule` to the list of its events that started this run. It is a list
because one run can pick up several events of the same asset: if `dataops`
emits three times while `mlops` is busy, the next `mlops` run starts with all
three ("When the consumer runs", at the end of this section, explains it).
Airflow's public API has no type for those events, so the annotation says
`Sequence[Any]`; the fields worth knowing are `extra`,
`timestamp`, and `source_dag_id` and `source_run_id`, the Dag run that emitted
the event. As with `ti`, these annotations are for you and your editor:
Airflow injects the arguments by name and ignores their types.

So far, one asset, one producer and one consumer. A bigger pipeline needs
more pieces; two of them come up later in this tutorial:

| If you need… | Use |
|---|---|
| `mlops` to wait for two assets, or to make do with either one | `AssetAll(a, b)` / `AssetAny(a, b)` in the `schedule` (§10) |
| a Dag to start on a message from an outside system, with no producer in Airflow | `AssetWatcher` (§11) |

There are more; §16 shows how to use four of them.

### When the consumer runs

When Airflow writes an asset event, it also adds a queue entry for **each
unpaused Dag** scheduled on that asset. On its next loop, the Scheduler turns
those entries into Dag runs.

Events that arrive faster than the consumer can run are neither lost nor run
one by one. `mlops` allows a single active run (`max_active_runs=1`), so if
three `dataops` runs finish while it is busy, the *next* `mlops` run takes all
three events, and `triggering_asset_events` in `prepare_data` lists every one
of them. A consumer catches up in one run, not three.

> **A paused consumer does not queue, and does not catch up.** If `mlops` is
> paused when `dataops` emits, the asset event is still written — you will see
> it on the Assets page — but no run is queued for `mlops`, and **un-pausing
> later does not replay it**. That is the opposite of §7, where a *manual*
> trigger creates the run and the pause only holds it. The same goes for a
> consumer that did not exist yet when the event was written: the `dataops`
> run you make in §9 will emit an event that `mlops`, written in §10, never
> sees. (Checked all three ways on this stack — see [what was actually
> verified](../verified.md).)

---

← [7. Scheduling and logical dates](07-scheduling-and-logical-dates.md) · [Index](README.md) · [9. The DataOps Dag](09-the-dataops-dag.md) →
