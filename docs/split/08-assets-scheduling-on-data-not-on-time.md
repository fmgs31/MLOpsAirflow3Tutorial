## 8. Assets: scheduling on data, not on time

An **Asset** is a name you give to a piece of data; for example,
`labeled_projects`, the training table `dataops` builds in the warehouse. Airflow does not read
that data: it only knows the name, and records each time the data is updated.

The assets this tutorial uses are already defined, in
[`src/project_tagger/assets.py`](../../src/project_tagger/assets.py), so that `dataops` and `mlops`
import the same objects:

```python
from airflow.sdk import Asset

LABELED_PROJECTS = Asset(
    name="labeled_projects",
    uri=_bigquery_uri("labeled_projects"),   # bigquery://<project>/mlops_course/labeled_projects
    group="dataset",
    extra={"layer": "warehouse", "owner": "data-engineering"},
)
```

`name` is what identifies the asset in your Dags. `uri` is the address of the
real data: here the BigQuery table, in the `bigquery://` scheme the Google
provider registers, built from your Project ID (§9). `group` and `extra` are
descriptions the UI shows on the asset's page; this `extra` describes the
asset itself, not any one event.

In Dag code you refer to the asset by that object, `LABELED_PROJECTS`. Using
it takes two sides.

**Who produces the asset.** A task declares that it produces an asset by
listing it in its decorator's `outlets=` argument:

```python
@task(outlets=[LABELED_PROJECTS])
```

The assets in that list are the task's **outlets**: the data the task declares
it writes. (The opposite, `inlets=`, declares what it reads; this tutorial does
not use it.) The task is its outlets' **producer**. Each time
the task succeeds, Airflow records an **asset event**: the record that the
asset has just been updated.

**Who consumes it.** A Dag declares that it consumes an asset by putting it in
its `schedule=`:

```python
@dag(schedule=[LABELED_PROJECTS])
```

That Dag is the asset's **consumer**, and each new asset event starts a run of
it.

That is the whole mechanism, and it replaces every "run 30 minutes after the
other Dag, and hope" cron you have ever seen.

The asset event itself is a row in the metadata database: which asset, which
Dag run produced it, when, and an optional `extra` dict the producing task can
fill in. That row is all it takes to start the consumer. A task that fails
writes no event, and that is the entire safety property: a consumer cannot be
started by a producer that did not finish.

In this tutorial the producer of `labeled_projects` is `validate_transformed`,
the last step of `dataops` in §1's diagram, which you write in §9.
The outlet goes on that **validation** task, not on `transform`, the task
before it that builds the table: if the validation fails, the task writes no
event, so `mlops` cannot be started by data that did not pass it. That is the
idea in one line: **the consumer is scheduled on "good
data arrived", not on a cron that guesses when the producer's Dag will have
finished.**

While a task runs, its asset event does not exist yet: Airflow writes it once
the task succeeds. **`outlet_events`** is how the task prepares that event
beforehand. It works like a dict with one key per outlet of the task:
`outlet_events[LABELED_PROJECTS]` stands for the event the task is about to
emit for `labeled_projects`, and the dict you assign to its `.extra` is stored
inside that event — here, how many rows the task validated and which tags it
found. A task that never touches `outlet_events`
still emits its events, with an empty `extra`. The task receives it by
declaring a parameter with that name, the way it receives `ti` in §6:

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
Airflow's public API has no type for those events, so the
annotation says `Sequence[Any]`; the fields worth knowing are `extra`,
`timestamp`, and `source_dag_id` and `source_run_id`, the Dag run that emitted
the event. As with `ti`, these annotations are for you and your editor:
Airflow injects the arguments by name and ignores their types.

So far, one asset, one producer and one consumer. As soon as a pipeline grows,
other needs appear, and each has its piece. Two of them come up later in this
tutorial:

| If you need… | Use |
|---|---|
| `mlops` to wait for two assets, or to make do with either one | `AssetAll(a, b)` / `AssetAny(a, b)` in the `schedule` (§10) |
| a Dag to start on a message from an outside system, with no producer in Airflow | `AssetWatcher` (§11) |

There are more; §16 shows how to use four of them.

A URI is optional. `production_model`, the asset `mlops` emits when it
promotes a model (§10), has only a name: the model lives in MLflow's registry,
and Airflow knows no URI scheme (like `bigquery://` or `gs://`) that points
there. When an asset's data does sit somewhere with a known scheme, give the
asset a URI: the UI then shows where that data lives, and tools that read
Airflow's lineage can trace it.

An asset exists in Airflow's eyes only once a parsed Dag refers to it — as an
outlet or a schedule. Right now `airflow assets list` prints `No data found`,
and the Assets page is empty; §9 changes that as soon as the Dag processor
parses the first file you save there.

### When the consumer runs

When Airflow writes an asset event, it also adds a queue entry for **each
unpaused Dag** scheduled on that asset. The scheduler then does the second half: on its next
loop it turns those entries into Dag runs.

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
