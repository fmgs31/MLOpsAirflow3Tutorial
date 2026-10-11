## 8. Assets: scheduling on data, not on time

An **Asset** is a named piece of data. A task declares that it produces one by
listing it in `outlets=` — the assets in that list are the task's **outlets**,
and the task is their **producer** — and each time that task succeeds Airflow
records an **asset event**. A Dag declares that it consumes one by putting it
in `schedule=` — the Dag is the asset's **consumer** — and each new event
starts a run of it. That is the whole mechanism, and it replaces every "run 30
minutes after the other Dag, and hope" cron you have ever seen.

The assets this tutorial uses are already defined, in
[`src/project_tagger/assets.py`](../../src/project_tagger/assets.py), so that `dataops` and `mlops`
import the same objects:

```python
from airflow.sdk import Asset

LABELED_PROJECTS = Asset(
    name="labeled_projects",
    group="dataset",
    extra={"layer": "warehouse", "owner": "data-engineering"},
)
```

`name` is what identifies the asset. `group` and `extra` are descriptions the
UI shows on the asset's page; this `extra` describes the asset itself, not any
one event.

In this tutorial the producer of `labeled_projects` is `validate_transformed`,
the last of the four `dataops` tasks in §1's diagram, which you write in §9.
The outlet goes on that **validation** task, not on `transform`, the task
before it that writes the data:

```python
@task(outlets=[LABELED_PROJECTS])
def validate_transformed(fp: str, *, outlet_events) -> dict:
    df = data.load_labeled(fp)
    result = validate.run_suite(df, "labeled_projects")
    if not result.passed:
        raise AirflowFailException(...)
    outlet_events[LABELED_PROJECTS].extra = {"rows": len(df), "path": fp}
    return {...}
```

`outlet_events` is injected by name, like `ti` in §6, and lets the task attach
an `extra` dict to the event it is about to emit — a different `extra` on each
event, unlike the asset's fixed one. `AirflowFailException` fails the task
without retries; §9 explains it.

And the consumer, `mlops`, which you write in §10. There
`triggering_asset_events` — also injected by name — holds the events that
started the run:

```python
@dag(schedule=[LABELED_PROJECTS], ...)
def mlops():
    @task
    def prepare_data(*, triggering_asset_events):
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")
```

That is the idea in one line: **the consumer is scheduled on "good data
arrived", not on a cron that guesses when the producer's Dag will have finished.**
The event is emitted by the task that validated the data, so "finished
successfully" means exactly that. The event even
carries the number of rows the task validated.

More asset machinery worth knowing:

| Feature | What it does |
|---|---|
| `AssetAny(a, b)` / `AssetAll(a, b)` | OR / AND conditions in a `schedule` |
| `AssetAlias` | an outlet whose actual asset the task decides while it runs (one per date, say) |
| `@asset` / `@asset.multi` | define asset + Dag + task in one decorator |
| `Metadata(asset, {...})` | another way to set an event's `extra`: the task does `yield Metadata(...)` instead of using `outlet_events` |
| `AssetWatcher` + `BaseEventTrigger` | **event-driven** runs (§11) |
| `CronPartitionTimetable` | a cron that creates runs per partition: each run carries a key (a date, say) that says which slice of the data it is for |

Assets here are **name-only**. An asset can also have a **URI**, the address
of its real data (`s3://bucket/labeled_projects.csv`, say). This tutorial
gives them none because the path of `data/` is not the same inside the
container as in §2's native install. In production give each asset the URI of
its data, so the UI says where it lives.

An asset exists in Airflow's eyes only once a parsed Dag refers to it — as an
outlet or a schedule. Right now `airflow assets list` prints `No data found`,
and the Assets page is empty; §9 changes that as soon as the Dag processor
parses the first file you save there.

### What an asset event actually is

It is a row. When a task with `outlets=[…]` **succeeds**, Airflow records an
asset event — which asset, which Dag run produced it, when, and whatever you put
in `outlet_events[…].extra`. That row is all it takes to start the consumer.
A task that fails
writes no event, which is the entire safety property: `mlops` cannot be started
by data that did not pass validation, because the task that would have announced
it never finished.

In the same step, Airflow adds a queue entry for **each unpaused Dag**
scheduled on that asset. The scheduler then does the second half: on its next
loop it turns those entries into Dag runs.

Events that arrive faster than the consumer can run are neither lost nor run
one by one. `mlops` allows a single active run (`max_active_runs=1`), so if
three `dataops` runs finish while it is busy, the *next* `mlops` run takes all
three events, and `triggering_asset_events` in `prepare_data` lists every one
of them. A consumer catches up in one run, not three.

> **A paused consumer does not queue, and does not catch up.** The word
> *unpaused* in "each unpaused Dag" matters. If `mlops` is paused when `dataops` emits,
> the asset event is still written — you will see it on the Assets page — but no run is queued for `mlops`, and **un-pausing later does
> not replay it**. Airflow filters paused Dags at the moment it decides what to
> enqueue, so there is nothing left to find afterwards. The same goes for a
> consumer that did not exist yet when the event was written: the `dataops`
> run you make in §9 will emit an event that `mlops`, written in §10, never
> sees. (Checked all three ways on this stack — see [what was actually
> verified](../verified.md).)
>
> This is the opposite of the paused-Dag behaviour in §7, where a *manual*
> trigger creates the run first and the pause only holds it. An asset event
> creates no run for a paused Dag, so there is nothing to hold and nothing to
> start when you unpause.

---

← [7. Scheduling and logical dates](07-scheduling-and-logical-dates.md) · [Index](README.md) · [9. The DataOps Dag](09-the-dataops-dag.md) →
