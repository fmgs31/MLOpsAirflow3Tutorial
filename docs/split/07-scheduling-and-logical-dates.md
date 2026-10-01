## 7. Scheduling and logical dates

Both Dags so far have `schedule=None`. The other values:

```python
schedule=None                       # manual / API only
schedule="0 3 * * *"                # cron
schedule=timedelta(days=1)          # interval
schedule=[some_asset]               # data-aware (§8)
schedule=CronTriggerTimetable(...)  # a Timetable, named directly
```

Every one of the first four is shorthand for a **Timetable**, the object that
owns the two decisions a schedule makes; the last line names one directly, and
writing your own Timetable class is how you get a cadence cron cannot express.

The two decisions: *when* to create a run, and which slice of time that run is
*for* — its **data interval**, exposed to every task as `data_interval_start`
and `data_interval_end`, and labelled by the run's `logical_date`. None of
those is "when it ran"; `run_after` is that. How the slice is cut depends on
the form of `schedule=`, and the two common forms differ:

- **A cron string** — `"0 3 * * *"`, which is the schedule you will give
  `dataops` in §9 — fires at the moment it names, and the slice *is* that
  moment: `logical_date`, `data_interval_start` and `data_interval_end` are
  all 03:00.
- **A `timedelta`** — `timedelta(days=1)` — is a span. The run *for* 2026-09-10
  is created once that day is over, at 2026-09-11 00:00, and its `logical_date`
  is 2026-09-10 00:00: the start of the span, a day before the run happens.
  Use this form when a task must process "yesterday's data", because the
  interval tells it exactly which day that is.

`start_date` is where the slices begin: no run is ever scheduled before it. A
Dag file without one still parses without error, so this is something you
remember, not something Airflow reminds you of.

Keep that picture in mind: the rest of this section follows from it. Three
things about scheduling that will save you an afternoon:

**`catchup` defaults to `False`** (`[scheduler] catchup_by_default`), and what
it does on un-pause is precise. Un-pausing a cron Dag with a 2025 `start_date`
does not create a run for every slice since then. It creates **one**, for the
most recent slice the cron has already passed — today's 03:00 if that is
behind you, yesterday's if not — and it creates it immediately, because that
slice is overdue. From there on, one per slice as they come due. You will
watch this happen in §10: un-pausing `dataops` is enough to start it. If you
*want* the history, ask for it with `catchup=True` — or backfill just the range
you need (§9).

**A cron string fires at the time it says.** `schedule="0 3 * * *"` runs at
03:00 and the run's `logical_date` is 03:00. Timetables are free to define that
differently, which is why the Dag's **Calendar** tab and the *"Next run"* column
are worth checking after you write one.

**`logical_date` is nullable.** It labels the slice a scheduled run is for —
and manual and asset-triggered runs, which are for no slice, simply have none.
This is what `mlops`'s run list will look like in §10, one run of each kind:

```bash
$ airflow dags list-runs mlops -o plain
dag_id  run_id                                      state    run_after             logical_date
mlops   asset_triggered__2026-09-09T08:23:16+00:00  success  2026-09-09T08:23:16              ← none
mlops   manual__2026-09-09T08:22:31+00:00           success  2026-09-09T08:22:32   2026-09-09T08:22:31
```

Use `run_after` when you mean "when did this actually start". Context keys
available to every task: `ds`, `ts`, `logical_date`, `data_interval_start`,
`data_interval_end`, `run_id`, `dag_run`, `ti`, `params`, `var`, `conn`,
`macros`, `inlets`/`outlets`, `triggering_asset_events`.

### Manual runs

A schedule is not the only way to start a Dag. Two commands cover most of it,
and `hello_airflow3` is the Dag to try them on:

```bash
docker compose exec airflow-scheduler airflow dags unpause hello_airflow3
docker compose exec airflow-scheduler airflow dags trigger hello_airflow3 -c '{"greeting": "hola"}' -r manual-demo-1
```

- **`-c/--conf`** is a JSON object. It lands in `dag_run.conf`, and any key that
  matches a declared `Param` overrides it for that run.
- **`-r/--run-id`** names the run. Without it you get `manual__<timestamp>`.
- **`-l/--logical-date`** sets one. Without it the run has none — see the trap
  below.
- In the UI the same thing is the **Trigger** button, with the form generated
  from your `params` (§5) — you pressed it in §5.

A new Dag arrives **paused** (`[core] dags_are_paused_at_creation`), so
`unpause` is not optional — and it is the step everyone forgets:

**A paused Dag still accepts a trigger — the run is created and then sits in
`queued` until you unpause.** Nothing fails, nothing warns:

```console
$ airflow dags pause hello_airflow3 && airflow dags trigger hello_airflow3 -r demo
$ airflow dags list-runs hello_airflow3 -o plain
dag_id          run_id  state   run_after                         logical_date
hello_airflow3  demo    queued  2026-09-09T12:19:29.483234+00:00             ← stuck
$ airflow dags unpause hello_airflow3     # now it starts
```

**Checkpoint.** Run those four commands yourself against `hello_airflow3`. While
the Dag is paused the run is `queued` and stays there however long you wait —
the scheduler sees it every loop and deliberately passes over it. Shortly after
the `unpause`, `list-runs` shows the same `run_id` as `success`, and its
`start_date` is later than its `run_after`: it started when you un-paused, not
when you triggered. Nothing was lost and nothing warned you; it was simply held.

The UI's trigger form has an *"Unpause … on trigger"* checkbox for exactly this.

> **The trap worth knowing before you trigger anything:** a manual run has **no
> logical date**, so `logical_date` is missing from the task context and `ds`,
> `ts` and friends are *undefined* in templates. Both fail at runtime, and
> neither fails under `airflow dags test`, which always supplies a logical date:
>
> ```python
> context["logical_date"]          # KeyError on a manual run
> context.get("logical_date")      # None — fine
> "{{ ds }}"                       # UndefinedError: 'ds' is undefined
> "{{ ds | default('none') }}"     # fine
> "{{ dag_run.run_after }}"        # fine — every run has one
> ```
>
> You wrote the safe form of both in lessons 1 and 2, which is why the runs
> you just triggered succeeded. Both were live bugs in this tutorial's own
> Dags until a manual trigger found them. Asset-triggered runs have no logical
> date either, so `mlops` is exposed to the same thing.

Two more ways in: `airflow dags test <dag_id>` runs the whole Dag in-process
(§14), and the REST API does it over HTTP —
`POST /api/v2/dags/{dag_id}/dagRuns`, which is what the UI button calls.

---

← [6. Operators, XComs and relationships](06-operators-xcoms-and-relationships.md) · [Index](README.md) · [8. Assets: scheduling on data, not on time](08-assets-scheduling-on-data-not-on-time.md) →
