## 7. Scheduling and logical dates

A scheduled run has several dates, each answering a different question. Take
a Dag that, every night at midnight, processes the previous day's data. Its
`start_date` is 2025-01-01, and this is its run for 10 September 2026:

| Date | Question it answers | In the example |
|---|---|---|
| `logical_date` | Which run is this? The date in "the run of 10 September" | 2026-09-10 00:00 |
| `data_interval_start` | From when is the data it processes? | 2026-09-10 00:00 |
| `data_interval_end` | Until when is the data it processes? | 2026-09-11 00:00 |
| `run_after` | From when may it start? | 2026-09-11 00:00 |
| `start_date` of the run | When did it actually start? | a few seconds after `run_after`, or hours later if the Dag was paused or busy |
| `start_date` of the Dag | From what date does the Scheduler schedule runs? | 2025-01-01 00:00: no earlier slice gets a run |

The run *of* the 10th starts on the 11th: it cannot process a day's data until
that day is over. Its tasks take which data to process from
`data_interval_start` and `data_interval_end`; the logical date names the run
and is the start of its interval. `ds`, `ts` and the `run_id` are that logical
date as text: `2026-09-10`, `2026-09-10T00:00:00+00:00`,
`scheduled__2026-09-10T00:00:00+00:00`.

The two `start_date`s share only the name. The run's is recorded by Airflow
when that run starts, and differs from run to run. The Dag's is an argument you
write (§4), the same for every run; more on it below.

Both Dags so far have `schedule=None`. The other values:

```python
schedule=None                       # manual / API only
schedule="0 3 * * *"                # cron
schedule=timedelta(days=1)          # every 24 hours
schedule=[some_asset]               # data-aware (§8)
schedule=CronTriggerTimetable(...)  # a Timetable, named directly
```

Each of the first four is shorthand for a **Timetable**, the object that
decides *when* to create a run and which slice of time that run is *for*: its
**data interval**. The last line names one directly; writing your own Timetable
class is how you get a cadence cron cannot express. There are two kinds of
slice:

- **A cron string or a `timedelta`** (`"0 3 * * *"`, the schedule `dataops`
  gets in §9, or `timedelta(days=1)`) fires at a moment, and the slice *is*
  that moment: `logical_date`, `data_interval_start`, `data_interval_end` and
  `run_after` are the same instant, 03:00 for the cron string. Only the run's
  `start_date` can be later, which is why they usually look like one date.
- **A data-interval timetable** (`CronDataIntervalTimetable("0 0 * * *",
  timezone="UTC")`, imported from `airflow.sdk`) cuts real spans, as in the
  table at the top of §7. Use it when a task must process "yesterday's data":
  the interval tells it which day that is. Its `logical_date` falls a whole
  span before the run, so after writing one, check the Dag's **Calendar** tab
  and the **Next Run** column of the Dags list.

The Dag's `start_date` is where the slices begin: no run is ever scheduled for
an earlier slice. It only governs the runs the Scheduler creates; a manual
trigger works even with a `start_date` in the future. It is also optional, and
without one there is no past to catch up on: on un-pause, a cron schedule waits
for its next time, unless the last one passed within a tenth of the interval,
five minutes at least (2.4 hours on a daily schedule). A `timedelta` schedule
starts as soon as you un-pause.

Two things about scheduling that will save you an afternoon:

**`catchup` defaults to `False`** (`[scheduler] catchup_by_default`).
Un-pausing a cron Dag with a 2025 `start_date` does not create a run for every
slice since then. It creates **one**, immediately because it is overdue, for
the most recent slice the cron has already passed (today's if it is now later
than 03:00, yesterday's if earlier), and then one per slice as they come due.
You will watch this in §10: un-pausing `dataops` is enough to start it. If you
*want* the history, ask for it with `catchup=True`, or backfill just the range
you need (§9).

**`logical_date` can be empty.** A scheduled run takes it from its slice. An
asset-triggered run is for no slice and has none, and neither does a run
triggered from the CLI or the REST API without one. The UI's Trigger form
pre-fills it with the current time, so a run triggered from the form has one
unless you clear the field. In §10, `mlops` gets one run of each kind, the
manual one from the form; its run list, trimmed to the columns that matter
here, shows the asset-triggered run with no `logical_date`:

```bash
$ docker compose exec airflow-scheduler airflow dags list-runs mlops -o plain
dag_id  run_id                                      state    run_after             logical_date
mlops   asset_triggered__2026-09-09T08:23:16+00:00  success  2026-09-09T08:23:16
mlops   manual__2026-09-09T08:22:31+00:00           success  2026-09-09T08:22:32   2026-09-09T08:22:31
```

Use `run_after` when you mean "when was this run due to start": every run has
one.

> **The trap, before you trigger anything:** in a run with no logical date,
> `logical_date` is missing from the task context and `ds`, `ts` and friends
> are *undefined* in templates. Both fail at runtime, and neither fails under
> `airflow dags test`, which always supplies a logical date:
>
> ```python
> context["logical_date"]          # KeyError when the run has none
> context.get("logical_date")      # None — fine
> "{{ ds }}"                       # UndefinedError: 'ds' is undefined
> "{{ ds | default('none') }}"     # fine
> "{{ dag_run.run_after }}"        # fine — every run has one
> ```
>
> You wrote the safe form of both in §4 and §6, which is why the runs you are
> about to trigger from the CLI succeed. `mlops` (§10), which runs on an asset,
> reads neither `logical_date` nor `ds` anywhere.

### Manual runs

`airflow dags trigger` creates a run right now, without waiting for any
schedule. Try it on `hello_airflow3`; the `unpause` first is explained after
the `trigger` options:

```bash
docker compose exec airflow-scheduler airflow dags unpause hello_airflow3
docker compose exec airflow-scheduler airflow dags trigger hello_airflow3 -c '{"greeting": "hola"}' -r manual-demo-1
```

- **`-c/--conf`** is a JSON object. It ends up in `dag_run.conf`, and any key that
  matches a declared `Param` overrides it for that run.
- **`-r/--run-id`** names the run. Without it you get `manual__<timestamp>`.
- **`-l/--logical-date`** sets one. Without it the run has none (see
  *`logical_date` can be empty*).
- In the UI it is the **Trigger** button, with the form generated from your
  `params` (§5).

Pausing a Dag stops the Scheduler from creating its scheduled runs (the
subject of *`catchup` defaults to `False`*) and from starting any of its runs,
even the ones you create by hand. That is why `hello_airflow3`, which has no
schedule, needs `unpause` all the same. A new Dag arrives **paused** (`[core]
dags_are_paused_at_creation`), so `unpause` is not optional, and it is the step
everyone forgets:

**A paused Dag still accepts a trigger: the run is created and then sits in
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

**Checkpoint.** Run those four commands yourself against `hello_airflow3`.
While the Dag is paused the run stays `queued` however long you wait: the
Scheduler sees it every loop and deliberately passes over it. Shortly after the
`unpause`, `list-runs` shows the same `run_id` as `success`, with a
`start_date` (the run's, not the Dag's) later than its `run_after`: it started
when you un-paused, not when you triggered.

The UI's trigger form has an *"Unpause … on trigger"* checkbox for this.

Two more ways start a run. The REST API does what `trigger` does, over HTTP:
`POST /api/v2/dags/{dag_id}/dagRuns`, which is what the UI button calls.
`airflow dags test <dag_id>` instead runs the whole Dag inside the command
itself, without the Scheduler (§4), so pausing does not hold it back; that is
why it worked in §4 while `hello_airflow3` was still paused. Use `test` while
you write a Dag, and `trigger` to run it the way the Scheduler will.

---

← [6. Operators, XComs and relationships](06-operators-xcoms-and-relationships.md) · [Index](README.md) · [8. Assets: scheduling on data, not on time](08-assets-scheduling-on-data-not-on-time.md) →
