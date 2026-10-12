## 7. Scheduling and logical dates

A scheduled run has several dates, and each one answers a different question.
They are easiest to tell apart on a Dag that, every night at midnight,
processes the previous day's data. Its `start_date` is 2025-01-01, and this is
its run for 10 September 2026:

| Date | Question it answers | In the example |
|---|---|---|
| `logical_date` | Which run is this? The date in "the run of 10 September" | 2026-09-10 00:00 |
| `data_interval_start` | From when is the data it processes? | 2026-09-10 00:00 |
| `data_interval_end` | Until when is the data it processes? | 2026-09-11 00:00 |
| `run_after` | From when may it start? | 2026-09-11 00:00 |
| `start_date` of the run | When did it actually start? | a few seconds after `run_after`, or hours later if the Dag was paused or busy |
| `start_date` of the Dag | From what date does the scheduler schedule runs? | 2025-01-01 00:00: no earlier slice gets a run |

The run *of* the 10th starts on the 11th: it cannot process a day's data until
that day is over. Its tasks know which data to process from
`data_interval_start` and `data_interval_end`, and the logical date names the
run: it is the start of its interval. `ds`, `ts` and the `run_id` are that same
logical date written as text: `2026-09-10`, `2026-09-10T00:00:00+00:00`,
`scheduled__2026-09-10T00:00:00+00:00`.

The two `start_date`s share a name and nothing else. The run's is recorded by
Airflow when that run starts, and differs from run to run. The Dag's is an
argument you write (§4), is the same for every run, and fixes from when there
are runs at all; more on it further down in §7.

With a plain cron string, like the `"0 3 * * *"` that `dataops` uses, there is
no period to wait for: the run is "the 03:00 run", so the first four dates in
the table are all 03:00, and only the run's `start_date` can be later. That is
why they usually look like one date.

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
*for* — its **data interval**. How the slice is cut depends on the timetable,
and there are two kinds:

- **A cron string or a `timedelta`** — `"0 3 * * *"`, the schedule you will
  give `dataops` in §9, or `timedelta(days=1)` — fires at a moment, and the
  slice *is* that moment: `logical_date`, `data_interval_start`,
  `data_interval_end` and `run_after` are all the same instant, 03:00 for the
  cron string.
- **A data-interval timetable** — `CronDataIntervalTimetable("0 0 * * *",
  timezone="UTC")`, imported from `airflow.sdk` — cuts real spans. It is the
  one behind the table at the top of §7. Use this kind when a task must
  process "yesterday's data", because the interval tells it exactly which day
  that is.

The Dag's `start_date` is where the slices begin: no run is ever scheduled for
an earlier slice. It only governs the runs the scheduler creates; a manual
trigger works even with a `start_date` in the future.

It is also optional, and without one there is no past to catch up on. When you
un-pause the Dag, a cron schedule waits for its next time, unless the last one
passed very recently: within a tenth of the interval, five minutes at least
(2.4 hours on a daily schedule). A `timedelta` schedule starts as soon as you
un-pause.

Three things about scheduling that follow from the above and will save you an
afternoon:

**`catchup` defaults to `False`** (`[scheduler] catchup_by_default`), and it is
worth knowing exactly what that means when you un-pause a Dag. Un-pausing a cron Dag with a 2025 `start_date`
does not create a run for every slice since then. It creates **one**, for the
most recent slice the cron has already passed — if it is now later than
03:00, today's; if earlier, yesterday's — and it creates it immediately, because that
slice is overdue. From there on, one per slice as they come due. You will
watch this happen in §10: un-pausing `dataops` is enough to start it. If you
*want* the history, ask for it with `catchup=True` — or backfill just the range
you need (§9).

**A cron string fires at the time it says.** `schedule="0 3 * * *"` runs at
03:00 and the run's `logical_date` is 03:00. A data-interval timetable puts the
`logical_date` a whole span before the run, which is why the Dag's **Calendar**
tab and the **Next Run** column of the Dags list are worth checking after you
write one.

**`logical_date` can be empty.** It is the date that names the run, and for a
scheduled run it comes from the run's slice. An asset-triggered run is for no slice and has none, and neither does a run
triggered from the CLI or the REST API without one. The UI's Trigger form is
the exception: it pre-fills the logical date with the current time, so a run
triggered from the form has one unless you clear the field. This is what
`mlops`'s run list will look like in §10, one run of each kind — the manual one
triggered from the form — trimmed to the columns that matter here. The asset-triggered run leaves the
`logical_date` column empty:

```bash
$ docker compose exec airflow-scheduler airflow dags list-runs mlops -o plain
dag_id  run_id                                      state    run_after             logical_date
mlops   asset_triggered__2026-09-09T08:23:16+00:00  success  2026-09-09T08:23:16
mlops   manual__2026-09-09T08:22:31+00:00           success  2026-09-09T08:22:32   2026-09-09T08:22:31
```

Use `run_after` when you mean "when was this run due to start": every run has
one, with or without a logical date.

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
> you are about to trigger from the CLI succeed. Asset-triggered runs have no
> logical date either, which is why `mlops` (§10), which runs on an asset,
> reads neither `logical_date` nor `ds` anywhere.

### Manual runs

A schedule is not the only way to start a Dag. `airflow dags trigger` creates a
run right now, without waiting for any schedule. `hello_airflow3` is the Dag to
try it on; it needs `unpause` first, and what pausing does is explained after
the `trigger` options:

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

Pausing a Dag does two things: the scheduler creates none of its scheduled
runs, and it starts none of its runs, not even the ones you create by hand. The
first is what *`catchup` defaults to `False`* is about. The second is why
`hello_airflow3`, which has no schedule, needs `unpause` all the same. A new Dag
arrives **paused** (`[core] dags_are_paused_at_creation`), so `unpause` is not
optional — and it is the step everyone forgets:

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

Besides `airflow dags trigger` and the UI's **Trigger** button, there are two
more ways to start a run. The REST API does what `trigger` does, over HTTP —
`POST /api/v2/dags/{dag_id}/dagRuns`, which is what the UI button calls.
`airflow dags test <dag_id>` is different: it runs the whole Dag inside the
command itself, without the scheduler (§4). That is why pausing does not hold
it back, and why it worked in §4 while `hello_airflow3` was still paused. Use
`test` while you write a Dag, and `trigger` to run it the way the scheduler
will.

---

← [6. Operators, XComs and relationships](06-operators-xcoms-and-relationships.md) · [Index](README.md) · [8. Assets: scheduling on data, not on time](08-assets-scheduling-on-data-not-on-time.md) →
