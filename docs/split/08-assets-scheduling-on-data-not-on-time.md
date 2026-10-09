## 8. Assets: scheduling on data, not on time

An **Asset** is a named piece of data. A task declares it produces one with
`outlets=`, and each time that task succeeds Airflow records an **asset event**;
a Dag declares it consumes one by putting it in `schedule=`, and an event starts
a run of it. That is the whole mechanism, and it replaces every "run 30 minutes after the other Dag,
and hope" cron you have ever seen.

The assets this tutorial uses are already defined, in
[`src/tagifai/assets.py`](../../src/tagifai/assets.py), so that `dataops` and `mlops`
import the same objects:

```python
from airflow.sdk import Asset

LABELED_PROJECTS = Asset(
    name="labeled_projects",
    group="dataset",
    extra={"layer": "warehouse", "owner": "data-engineering"},
)
```

The producer side is what you will write in §9 — note that the outlet goes on
the **validation** task, not the transform task:

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
an `extra` dict to the event it is about to emit.

And the consumer side, §10, where `triggering_asset_events` — also injected by
name — holds the events that started the run:

```python
@dag(schedule=[LABELED_PROJECTS], ...)
def mlops():
    @task
    def prepare_data(*, triggering_asset_events):
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")
```

That is the idea in one line: **downstream Dags are scheduled on "good data
arrived", never on "a task finished" and never on a cron guess.** The event even
carries the row count that caused it.

More asset machinery worth knowing:

| Feature | What it does |
|---|---|
| `AssetAny(a, b)` / `AssetAll(a, b)` | OR / AND conditions in a `schedule` |
| `AssetAlias` | outlets whose URI is only known at runtime |
| `@asset` / `@asset.multi` | define asset + Dag + task in one decorator |
| `Metadata(asset, {...})` | attach metadata by yielding from the task |
| `AssetWatcher` + `BaseEventTrigger` | **event-driven** runs (§11) |
| `CronPartitionTimetable` | partitioned assets |

Assets here are **name-only**, which keeps their identity the same in Docker and
native runs. In production give them the real URI of the data (`s3://…`,
`bq://…`) so the lineage in the UI points somewhere.

An asset exists in Airflow's eyes only once a parsed Dag refers to it — as an
outlet or a schedule. Right now `airflow assets list` prints `No data found`,
and the Assets page is empty; §9 changes that the moment you save a file.

### What an asset event actually is

It is a row. When a task with `outlets=[…]` **succeeds**, Airflow records an
asset event — which asset, which Dag run produced it, when, and whatever you put
in `outlet_events[…].extra`. That is all a "trigger" is. A task that fails
writes no event, which is the entire safety property: `mlops` cannot be started
by data that did not pass validation, because the task that would have announced
it never finished.

The scheduler then does the second half. On its next loop it reads new asset
events and queues a Dag run for **each unpaused Dag** scheduled on that asset.

Events that arrive faster than the consumer can run are neither lost nor run
one by one. `mlops` allows a single active run (`max_active_runs=1`), so if
three `dataops` runs finish while it is busy, the *next* `mlops` run takes all
three events, and `triggering_asset_events` in `prepare_data` lists every one
of them. A consumer catches up in one run, not three.

> **A paused consumer does not queue, and does not catch up.** The word
> *unpaused* above matters. If `mlops` is paused when `dataops` emits,
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
