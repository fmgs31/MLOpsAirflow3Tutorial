## 7. Scheduling and logical dates

Both Dags so far have `schedule=None`. The other values:

```python
schedule=None                       # manual / API only
schedule="0 3 * * *"                # cron
schedule=timedelta(days=1)          # every 24 hours
schedule=[some_asset]               # data-aware (§8)
schedule=CronTriggerTimetable(...)  # a Timetable, named directly
```

Every one of the first four is shorthand for a **Timetable**, the object that
makes the two decisions a schedule involves; the last line names one directly, and
writing your own Timetable class is how you get a cadence cron cannot express.

The two decisions: *when* to create a run, and which slice of time that run is
*for* — its **data interval**, exposed to every task as `data_interval_start`
and `data_interval_end`, and labelled by the run's `logical_date`. None of
those is "when it ran"; `run_after` is that. How the slice is cut depends on
the timetable, and there are two kinds:

- **A cron string or a `timedelta`** — `"0 3 * * *"`, the schedule you will
  give `dataops` in §9, or `timedelta(days=1)` — fires at a moment, and the
  slice *is* that moment: `logical_date`, `data_interval_start` and
  `data_interval_end` are all the same instant, 03:00 for the cron string.
- **A data-interval timetable** — `CronDataIntervalTimetable("0 0 * * *",
  timezone="UTC")`, imported from `airflow.sdk` — cuts real spans. The run
  *for* 2026-09-10 is created once that day is over, at 2026-09-11 00:00; its
  data interval runs from 2026-09-10 00:00 to 2026-09-11 00:00, and its
  `logical_date` is 2026-09-10 00:00: the start of the span, a day before the
  run happens. Use this kind when a task must process "yesterday's data",
  because the interval tells it exactly which day that is.

`start_date` is where the slices begin: no run is ever scheduled before it. It
is optional — without one, a scheduled Dag starts from the moment you un-pause
it — and it only governs the runs the scheduler creates; a manual trigger is
never held back by it.

Keep that picture in mind: the rest of this section follows from it. Three
things about scheduling that will save you an afternoon:

**`catchup` defaults to `False`** (`[scheduler] catchup_by_default`), and it is
worth knowing exactly what that means when you un-pause a Dag. Un-pausing a cron Dag with a 2025 `start_date`
does not create a run for every slice since then. It creates **one**, for the
most recent slice the cron has already passed — today's 03:00 if that is
behind you, yesterday's if not — and it creates it immediately, because that
slice is overdue. From there on, one per slice as they come due. You will
watch this happen in §10: un-pausing `dataops` is enough to start it. If you
*want* the history, ask for it with `catchup=True` — or backfill just the range
you need (§9).

**A cron string fires at the time it says.** `schedule="0 3 * * *"` runs at
03:00 and the run's `logical_date` is 03:00. A data-interval timetable puts the
`logical_date` a whole span before the run, which is why the Dag's **Calendar**
tab and the **Next Run** column of the Dags list are worth checking after you
write one.

**`logical_date` can be empty.** It labels the slice a scheduled run is for.
An asset-triggered run is for no slice and has none, and neither does a run
triggered from the CLI or the REST API without one. The UI's Trigger form is
the exception: it pre-fills the logical date with the current time, so a run
triggered from the form has one unless you clear the field. This is what
`mlops`'s run list will look like in §10, one run of each kind — the manual one
triggered from the form — trimmed to the columns that matter here:

```bash
$ docker compose exec airflow-scheduler airflow dags list-runs mlops -o plain
dag_id  run_id                                      state    run_after             logical_date
mlops   asset_triggered__2026-09-09T08:23:16+00:00  success  2026-09-09T08:23:16              ← none
mlops   manual__2026-09-09T08:22:31+00:00           success  2026-09-09T08:22:32   2026-09-09T08:22:31
```

Use `run_after` when you mean "when did this actually start". Context keys
available to every task: `ds`, `ts`, `logical_date`, `data_interval_start`,
`data_interval_end`, `run_id`, `dag_run`, `ti`, `params`, `var`, `conn`,
`macros`, `inlets`/`outlets`, `triggering_asset_events`.

> **The trap worth knowing before you trigger anything:** a run triggered from
> the CLI or the REST API has **no logical date** unless you give it one, so `logical_date` is missing from the task context and `ds`,
> `ts` and friends are *undefined* in templates. Both fail at runtime, and
> neither fails under `airflow dags test`, which always supplies a logical date:
>
> ```python
> context["logical_date"]          # KeyError when the run has none
> context.get("logical_date")      # None — fine
> "{{ ds }}"                       # UndefinedError: 'ds' is undefined
> "{{ ds | default('none') }}"     # fine
> "{{ dag_run.run_after }}"        # fine — every run has one
> ```
>
> You wrote the safe form of both in §4 and §6, which is why the runs
> you are about to trigger succeed. Both were live bugs in this tutorial's own
> Dags until a manual trigger found them. Asset-triggered runs have no logical
> date either, so `mlops` is exposed to the same thing.

### Manual runs

A schedule is not the only way to start a Dag. Two commands cover most of it,
and `hello_airflow3` is the Dag to try them on:

```bash
docker compose exec airflow-scheduler airflow dags unpause hello_airflow3
docker compose exec airflow-scheduler airflow dags trigger hello_airflow3 -c '{"greeting": "hola"}' -r manual-demo-1
```

- **`-c/--conf`** is a JSON object. It ends up in `dag_run.conf`, and any key that
  matches a declared `Param` overrides it for that run.
- **`-r/--run-id`** names the run. Without it you get `manual__<timestamp>`.
- **`-l/--logical-date`** sets one. Without it the run has none — the trap
  under *`logical_date` can be empty*.
- In the UI the same thing is the **Trigger** button, with the form generated
  from your `params` — you used it in §5.

A new Dag arrives **paused** (`[core] dags_are_paused_at_creation`), so
`unpause` is not optional — and it is the step everyone forgets:

**A paused Dag still accepts a trigger — the run is created and then sits in
`queued` until you unpause.** Nothing fails, nothing warns:

```bash
docker compose exec airflow-scheduler airflow dags pause hello_airflow3
docker compose exec airflow-scheduler airflow dags trigger hello_airflow3 -r demo
docker compose exec airflow-scheduler airflow dags list-runs hello_airflow3 -o plain
```

```
dag_id          run_id  state   run_after                         logical_date  start_date  end_date
hello_airflow3  demo    queued  2026-09-09T12:19:29.483234+00:00                                       ← stuck
```

```bash
docker compose exec airflow-scheduler airflow dags unpause hello_airflow3     # now it starts
```

**Checkpoint.** Run those four commands yourself against `hello_airflow3`. While
the Dag is paused the run is `queued` and stays there however long you wait —
the scheduler sees it every loop and deliberately passes over it. Run
`list-runs` again shortly after the `unpause`: it shows the same `run_id` as
`success`, and its
`start_date` — the run's, the moment it actually began, not the Dag's
`start_date` from earlier in §7 — is later than its `run_after`: it started when
you un-paused, not when you triggered. Nothing was lost and nothing warned you;
it was simply held.

The UI's trigger form has an *"Unpause … on trigger"* checkbox for exactly this.

Two more ways to start a run: `airflow dags test <dag_id>` runs the whole Dag in-process
(§14), and the REST API does it over HTTP —
`POST /api/v2/dags/{dag_id}/dagRuns`, which is what the UI button calls.

---

← [6. Operators, XComs and relationships](06-operators-xcoms-and-relationships.md) · [Index](README.md) · [8. Assets: scheduling on data, not on time](08-assets-scheduling-on-data-not-on-time.md) →
