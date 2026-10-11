## 10. The MLOps Dag

`dags/04_mlops.py` — no time schedule: each `labeled_projects` asset event
triggers it.

```
prepare_data ─► validate_training_data ─┐
                            candidates ─┴─► train_candidate×N ─► select_best
─► evaluate_best ─► promotion_gate ─► promote_model ──────► done
                                  └─► keep_current_model ─┘
```

| Task | Production option |
|---|---|
| the asset trigger + `prepare_data` | your warehouse client — or, as here, let the asset event carry the data's path |
| `validate_training_data` | Great Expectations / Pandera / Soda, plus split checks |
| `candidates` + `train_candidate` (one per hyperparameter set) | Optuna / Ray Tune / a service that distributes the hyperparameter search |
| `select_best` + `evaluate_best` | your experiment tracker (MLflow, W&B) |
| `promotion_gate` → `promote_model` | the move to production in your model registry (MLflow, say) |

### Build it

New workload functions: `data.split(df)` returns a `(train, test)` pair with
the same share of each tag in both; `validate.check_split(train, test)` is a
second suite, on the split; `train_module.SEARCH_SPACE` is a list of
hyperparameter dicts and `train_module.train_from_files(hparams)` fits one
model, returning `(model, metrics)`; `registry.save_run`, `load_metrics`,
`promote` and `get_production` are the repo's *model registry*, on the
filesystem: it stores each trained model with its metrics and records which
one is in production; `config.MIN_F1_TO_PROMOTE` is the minimum f1 for moving a
model to production (0.65, or the value of `PROJECT_TAGGER_MIN_F1` if set).

Create `dags/04_mlops.py`:

```python dags/04_mlops.py
"""Lesson 4 - the MLOps Dag: asset-triggered training, tuning, and promotion."""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException

from project_tagger import config, data, registry, train as train_module, validate
from project_tagger.assets import LABELED_PROJECTS, PRODUCTION_MODEL

DEFAULT_ARGS = {
    "owner": "ml-engineering",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=2),
}


@dag(
    dag_id="mlops",
    schedule=[LABELED_PROJECTS],
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "mlops"],
    doc_md=__doc__,
)
def mlops():
```

`schedule=[LABELED_PROJECTS]` is the key line: a list of assets, and the Dag
runs when they update. Several assets in the list mean *all* of them;
`AssetAny(a, b)` means either.

**Validate before you train.** `dataops` validated `labeled_projects` when it
wrote it to the warehouse, and this Dag validates it again before training on
it. That is not
redundant: it was a different Dag run, possibly hours ago, this run may have been
triggered by hand, and what the model is fitted on is the snapshot on disk *now*.
Add the first two tasks:

```python dags/04_mlops.py
    @task
    def prepare_data(*, triggering_asset_events) -> dict[str, int]:
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")
        df = data.load_labeled()
        train_df, test_df = data.split(df)
        return {"num_train": len(train_df), "num_test": len(test_df)}

    @task
    def validate_training_data(split_sizes: dict[str, int]) -> dict[str, Any]:
        df = data.load_labeled()
        table = validate.run_suite(df, "labeled_projects")
        split = validate.check_split(*data.split(df))
        print(f"{table.summary()} {split.summary()}")
        if not (table.passed and split.passed):
            raise AirflowFailException(
                f"Training snapshot failed validation: {table.failures + split.failures}"
            )
        return {
            "rows": len(df),
            "split_sizes": split_sizes,
            "warnings": [r["expectation"] for r in table.failures + split.failures],
            "results": table.results + split.results,
        }
```

`triggering_asset_events` is injected by name — the events that caused this
run, keyed by asset, each carrying the `extra` the producer attached. On a run
you start by hand it is empty. `validate_training_data` re-reads the table
rather than receiving it from `prepare_data`: XComs are for metadata, not
payloads (§6). The second half of its check is one a table-level suite cannot do at
all — it looks at the **split**:

```python
# src/project_tagger/validate.py — check_split()
("split_is_non_empty",          ...,  critical)
("classes_in_both_splits",      ...,  critical)   # a class only in train
("min_rows_per_class:20",       ...,  critical)   # 3 rows of a class in train
("no_id_leakage",               ...,  critical)   # the same project in both
("test_fraction_between_0.1_and_0.4", ..., non-critical)
```

Each problem in those comments — a class only in train, three rows of a class,
the same project in both splits — passes a column-level suite without a
warning, and still produces a model whose f1 on the test set means nothing. That f1 is
exactly what `promotion_gate`, later in this section, compares against the
production model.

**Dynamic task mapping** creates, while the Dag runs, one task instance per
hyperparameter set, and a later task gathers all the results. Three tasks:

```python dags/04_mlops.py
    @task
    def candidates() -> list[dict[str, Any]]:
        return train_module.SEARCH_SPACE

    @task(max_active_tis_per_dag=2)
    def train_candidate(hparams: dict[str, Any], **context) -> dict[str, Any]:
        config.ensure_dirs()
        run_id = f"{context['run_id']}__{context['ti'].map_index}".replace(":", "-").replace("+", "-")
        model, metrics = train_module.train_from_files(hparams)
        registry.save_run(run_id, model, metrics)
        print(f"run={run_id} hparams={hparams} f1={metrics['f1']:.4f}")
        return {"run_id": run_id, "f1": metrics["f1"], "hparams": hparams}

    @task
    def select_best(results: list[dict[str, Any]]) -> dict[str, Any]:
        best = max(results, key=lambda r: r["f1"])
        print(f"best={best['run_id']} f1={best['f1']:.4f} of {len(results)} candidates")
        return best
```

`candidates` returns a list; in the wiring below,
`train_candidate.expand(hparams=candidates())` creates one task instance per
item when the Dag runs — four here, one per hyperparameter set — and each is
retried, logged and inspected on its own. `context['ti'].map_index` tells the
task which of the four it is (0 to 3). The task that gathers the results,
`select_best`, just takes a `list` of the four return values. Note the return value of `train_candidate`: a run id, an
f1 and the hparams — small and JSON-serialisable. The model itself stays in the
registry.

**How many of the four train at once is a choice, and it is written in
`train_candidate`'s decorator.**
Nothing about `.expand()` says "in parallel"; it says "these are four separate
task instances". Whether they overlap is the executor's business, bounded by the
narrowest of the limits from §3 — and `max_active_tis_per_dag=2` on that task is
deliberately the narrowest one here. So the fan-out runs **two at a time, in two
waves**. Raise the cap and all four overlap; remove it and `[core]
max_active_tasks_per_dag` (16) is the next limit up. A cap on a training task
is not a toy setting: four models fitting at once is four copies of the data in
memory. When the limited resource is shared — say, a database that accepts only
a few connections — a `pool` (§3) sets the same kind of cap across every task
and Dag that uses it.

> **Gotcha:** the parameter is named `hparams`, not `params`. A `@task`
> receives context keys by parameter name (§4), and `params` is reserved — a task argument
> called `params` gets the Dag's params, not your data.

The **promotion gate**, `promotion_gate`, decides whether the best candidate
goes to production. Unlike the gates in §9, it does not fail the task when the
answer is no: it is a *branch task*, a task that picks which of the Dag's two
**branches** continues. Add `evaluate_best`, `promotion_gate`, `promote_model`
and the two `EmptyOperator`s the branches need:

```python dags/04_mlops.py
    @task
    def evaluate_best(best: dict[str, Any]) -> dict[str, Any]:
        metrics = registry.load_metrics(best["run_id"])
        for name, slice_metrics in metrics.get("slices", {}).items():
            print(f"slice {name}: f1={slice_metrics['f1']:.4f} n={slice_metrics['support']}")
        return {"run_id": best["run_id"], "metrics": metrics}

    @task.branch
    def promotion_gate(evaluation: dict[str, Any]) -> str:
        f1 = evaluation["metrics"]["f1"]
        threshold = config.MIN_F1_TO_PROMOTE
        current = registry.get_production()
        current_f1 = current["metrics"]["f1"] if current else 0.0

        if f1 < threshold:
            print(f"f1={f1:.4f} below threshold {threshold} - not promoting")
            return "keep_current_model"
        if f1 < current_f1:
            print(f"f1={f1:.4f} below production {current_f1:.4f} - not promoting")
            return "keep_current_model"
        return "promote_model"

    @task(outlets=[PRODUCTION_MODEL])
    def promote_model(evaluation: dict[str, Any], *, outlet_events) -> dict[str, Any]:
        pointer = registry.promote(evaluation["run_id"])
        outlet_events[PRODUCTION_MODEL].extra = {
            "run_id": pointer["run_id"],
            "f1": pointer["metrics"]["f1"],
            "previous_run_id": pointer["previous_run_id"],
        }
        print(f"promoted {pointer['run_id']} (was {pointer['previous_run_id']})")
        return pointer

    keep_current_model = EmptyOperator(task_id="keep_current_model")
    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")
```

A `@task.branch` returns the `task_id` to run; the other branch is **skipped**,
not failed. A candidate that misses the threshold is a normal outcome — the run
succeeds and production is left alone. `promote_model` emits the second asset,
`production_model`, which nothing consumes yet; point a Dag's `schedule` at it
and the edge appears in the Assets graph.

**Trigger rules** are how a task states what its upstream tasks must have done
before it runs, and
branching is where you first need one. The join, `done`, needs
`none_failed_min_one_success`, because the default `all_success` would skip it as
soon as either branch is skipped. The useful ones: `all_success` (default),
`all_done` (run whatever happened upstream — cleanup tasks),
`none_failed_min_one_success` (joins after a branch), `none_failed` (allows skips
but not failures), `one_success`, `one_failed` (alerting paths), `all_skipped`,
and `always`.

Finish the file with the wiring:

```python dags/04_mlops.py
    snapshot = validate_training_data(prepare_data())
    results = train_candidate.expand(hparams=candidates())
    snapshot >> results

    evaluation = evaluate_best(select_best(results))
    gate = promotion_gate(evaluation)

    gate >> promote_model(evaluation) >> done
    gate >> keep_current_model >> done


mlops()
```

`snapshot >> results` wires `validate_training_data` upstream of *every*
mapped instance of `train_candidate`, carrying no data, only the order. The two
`gate >>` lines give `promotion_gate` its two branches and join them at `done`.

### See the wiring

Save it. You have now written both Dags of the pipeline, `dataops` and
`mlops`, and before running anything you can ask Airflow what it knows:

```bash
docker compose exec airflow-scheduler airflow assets details --name labeled_projects -o plain
```

**Checkpoint.** Among the rows:

```
scheduled_dags    [{'dag_id': 'mlops', ...}]
producing_tasks   [{'dag_id': 'dataops', 'task_id': 'validate_transformed', ...}]
```

`producing_tasks` names the exact task that emits it; `scheduled_dags` names
what will start when it does. Neither Dag mentions the other anywhere. The
Assets page now draws the whole graph, `dataops → labeled_projects → mlops →
production_model`.

`mlops`'s own page can show the same connections. Open it, switch to **Graph**,
and change **Options → Dependencies** (§5):

- **Only tasks** — the ten tasks you just wrote, nothing else.
- **External conditions** — the same tasks, plus `labeled_projects` flowing
  into the Dag and `production_model` coming out of `promote_model`.
- **All Dag Dependencies** — the chain the Assets page draws: `dataops` with
  the two assets it produces, then `mlops` with its tasks inside it, then
  `production_model`.

### Watch the whole chain fire

This is the payoff of §8. Un-pause the consumer, `mlops`, first, so it is
listening, then the producer's Dag, `dataops`:

```bash
docker compose exec airflow-scheduler airflow dags unpause mlops
docker compose exec airflow-scheduler airflow dags unpause dataops
```

Nothing happens on the first line: `mlops`'s schedule is an asset, and no
event has arrived. The second line is enough to start everything, and you did not
trigger anything — that is the §7 rule: `dataops` has a cron and a 2025
`start_date`, so the moment it is un-paused the scheduler creates the run for
the most recent 03:00 UTC it missed, and starts it. Give it about half a
minute, then:

```bash
docker compose exec airflow-scheduler airflow dags list-runs dataops -o plain
docker compose exec airflow-scheduler airflow dags list-runs mlops -o plain
```

**Checkpoint.** `dataops` has a run whose id begins `scheduled__` and ends in
`T03:00:00+00:00` — today's date, or yesterday's if it is before 03:00 UTC —
with that as its `logical_date` and a `start_date` of a moment ago. (The
`manual__` run from `airflow dags test` in §9 is there too.) And `mlops` has a
run nobody triggered: id beginning `asset_triggered__`, `running` if you were
quick, `success` once its four candidates have trained, with an **empty**
`logical_date` — asset-triggered runs have no logical date, like a run
triggered from the CLI without one (§7). In the UI, open **Assets** and you will see the edge that
caused it.

Know where the wait comes from. Almost all of it is `dataops` itself running
its five tasks — `extract_projects` downloads the source file, so that part depends on
your connection. The Airflow part is small: one scheduler loop to create the
`dataops` run after the un-pause, and one more to create the `mlops` run once
the asset event is recorded (§3, §8). If nothing has appeared after a minute, check that both Dags
are un-paused and that the `dataops` run succeeded — a failed
`validate_transformed` emits no event, by design.

Now the two waves of `train_candidate`. Open that `mlops` run in the UI, click `train_candidate`,
and read its **Task Instances** tab — four rows, map index 0 to 3, each with
its own start and end time. Two start together; each of the other two starts
only when one of the first pair has finished. The *Show Gantt* view (§5),
available now that a run is selected, draws the same thing as two waves of bars. On the **Graph**, the four are one collapsed node until you raise the
**Min. map index** filter (§5). Or read it off the CLI:

```bash
docker compose exec airflow-scheduler airflow tasks states-for-dag-run mlops <the asset_triggered__ run id> -o plain
```

Four `train_candidate` rows with `map_index` 0–3; 2 and 3 each start after one
of 0 and 1 has ended. And `keep_current_model` is `skipped`
while `promote_model` is `success`: `promotion_gate` took the `promote_model`
branch, because
the best f1 cleared the threshold and there was no production model to beat.

> **Try it, once more by hand:**
>
> ```bash
> docker compose exec airflow-scheduler airflow dags trigger dataops
> ```
>
> A `manual__` run this time. When it succeeds, one more event, one more
> `asset_triggered__` run of `mlops`. Every success of the producer emits an
> event, whichever way the producer was started — and the consumer runs on it
> (events that arrive while `mlops` is busy share its next run, §8).

> **Try it, the other branch.** Set the bar impossibly high and watch
> `promotion_gate` take the `keep_current_model` branch. The threshold is read from an environment variable inside
> the task's own process, and `airflow dags test` runs the tasks in its own
> process, so pass the variable to that command with `-e`:
>
> ```bash
> docker compose exec -e PROJECT_TAGGER_MIN_F1=0.99 airflow-scheduler airflow dags test mlops
> ```
>
> `promotion_gate` logs `below threshold 0.99 - not promoting`, `promote_model`
> is skipped, `keep_current_model` runs, and the Dag run still succeeds.

Stuck anywhere? [`solutions/04_mlops.py`](../../solutions/04_mlops.py).

---

← [9. The DataOps Dag](09-the-dataops-dag.md) · [Index](README.md) · [11. Waiting: sensors, deferrable tasks, events](11-waiting-sensors-deferrable-tasks-events.md) →
