# What was actually verified

> Part of the [Airflow 3 tutorial](../README.md), split out of it to keep the
> tutorial itself readable. `§N` below refers to a numbered section of that
> README.

The body of the tutorial says what you will see. This document says how that
is known: every figure and claim there was read off a live stack, and this is
the record of when, and how it was measured. Nothing here is a step to run.
Skip it, or come back when you want to know how far to trust a number.

Against a live `apache/airflow:3.3.1` stack (Postgres 16, LocalExecutor,
api-server + scheduler + dag-processor + triggerer), on 2026-09-09:

- All 7 Dags parse with **zero import errors** (`airflow dags list-import-errors`).
- `airflow dags test` runs green for `hello_airflow3`, `operators_and_xcoms`,
  `dataops`, `mlops`, `sensors_and_events`, `monitoring`.
- The DataOps → Asset → MLOps chain fires for real: triggering `dataops`
  produced an `asset_triggered__…` run of `mlops` with no manual trigger.
- `mlops` validated the snapshot (`suite=labeled_projects passed=6/6
  suite=training_split passed=5/5`), trained 4 mapped candidates, selected the
  best (f1 = 0.884) and promoted it.
- The HITL path resolved: `awaiting_input` → default `Approve` after the
  timeout → `TriggerDagRunOperator` fired.
- The deadline callback fired at exactly +30s with the full `dag_run` context.
- Manual triggers were exercised: a trigger against a **paused** Dag produced a
  run stuck in `queued` until `airflow dags unpause`, and
  `-c '{"greeting": "hola"}'` reached the task as a `params` override.
- The §5 UI walkthrough was checked against this stack's api-server: nav, tabs,
  trigger/backfill form fields and the version selector come from the shipped UI
  strings, and the endpoints behind them (`/api/v2/assets`, `/backfills`,
  `/dags/{id}/dagVersions`, `/hitlDetails`) from its OpenAPI spec.
- `pytest`: **30 passed**.

Re-verified on 2026-09-11, against the same stack, for the numbers and claims
the body prints back to you:

- §4's checkpoint: the first Dag logs `(7 words)` — the greeting carries the
  run id *and* the date.
- §8's asset rules, tested both ways. With `mlops` **unpaused**, `airflow dags
  test dataops` triggered a real `asset_triggered__` run of `mlops` — `dags
  test` writes genuine asset events, it is not a dry run. With `mlops`
  **paused**, the asset event was written but no run was queued, and
  un-pausing afterwards did **not** replay it. Airflow filters paused Dags when
  it decides what to enqueue.
- §10's wait, decomposed from database timestamps over three triggers: the
  `dataops` run itself took 5.2–5.9s from queued to finished; the gap from its
  asset event being written to the `mlops` run being queued was 0.12–0.65s —
  one scheduler loop. The text quotes no end-to-end figure because the
  `dataops` part depends on the network.
- §11's `settle` (`TimeDeltaSensor(deferrable=True)`, 5s) under `airflow dags
  test`: its target is measured from the run's `data_interval_end` (or
  `run_after` when there is none, per `TimeDeltaSensor._derive_base_time`), which
  the command sets when it starts. If the moment has passed by the time the task
  runs, it returns at once (`sensors/time_delta.py`: `if utcnow() > target_dttm:
  return True`); if not, it defers, and the command runs the trigger inline —
  log line `[DAG TEST] running trigger in line` — never reaching the triggerer.
  Which of the two happens depends on how long the task took to start: on
  2026-10-10 a one-task probe Dag started 3s after `run_after` and deferred. Sent
  through the scheduler with `airflow dags trigger`, the same task sat in
  `deferred` for six seconds, went to `scheduled`, then `success` — polled every
  half-second from the `task_instance` table.
- §12's `ApprovalOperator` on 3.3 parks the task in `awaiting_input` with no
  trigger involved; the scheduler's sweep resolves it — `AWAITING_INPUT timeout
  sweep: 1 resolved (response/defaults)` in the scheduler log — and the task
  resumes in `execute_complete`. The triggerer is not needed for HITL.
- §10's fan-out is **two at a time, in two waves** (`max_active_tis_per_dag=2`),
  read off the four mapped task instances' start and end times — not four at
  once.
- §11's reschedule mode: with the curated file moved away, both sensors sat in
  `up_for_reschedule`; restoring the file let the next poke succeed and the run
  finished.
- §12's timing: **69 seconds**, of which **60** were `request_retraining` in
  `awaiting_input` before the `Approve` default resolved and
  `TriggerDagRunOperator` created a real `mlops` run. Without the `--conf`
  override the same Dag took 7 seconds and branched to `continue_serving`.
- §3's numbers are this stack's live config: `parallelism=32`,
  `max_active_tasks_per_dag=16`, `default_pool=128` slots,
  `scheduler_idle_sleep_time=1`, `min_file_process_interval=30`,
  `default_task_retries=0`.
- §7's data-interval shapes, read off the timetables themselves: a cron string
  is a `CronTriggerTimetable` here (`create_cron_data_intervals=False`) and
  gives `run_after == data_interval_start == data_interval_end`;
  `timedelta(days=1)` is a `DeltaTriggerTimetable` here
  (`create_delta_data_intervals=False`) and gives the same zero-length shape;
  `CronDataIntervalTimetable("0 0 * * *", timezone="UTC")` from a 2026-09-10
  `start_date` gives `run_after` 2026-09-11 00:00 with `logical_date ==
  data_interval_start` 2026-09-10 00:00 and `data_interval_end` 2026-09-11
  00:00.
- §4/§7's `start_date`: a Dag file with none parses with **no import error**
  under `DagBag`, for `schedule=None` and `@daily` alike.
- §4's `dags test` box: `hello_airflow3` ran `make_greeting → shout →
  count_words → report`, each finishing before the next started.
- §13, unpause then trigger: `DEADLINE MISSED` at +31s, `slow_step` still
  running.
- §10's Try-it with the variable exported in the container (`bash -c 'export
  PROJECT_TAGGER_MIN_F1=0.99; airflow dags test mlops'`): `promote_model` skipped,
  `keep_current_model` success. The form §10 now uses, `docker compose exec -e
  PROJECT_TAGGER_MIN_F1=0.99 …`, sets the same variable: `config.MIN_F1_TO_PROMOTE`
  reads `0.99` in that process.
- §3's `curl` note: on a Windows machine PowerShell resolves `curl` to
  `Invoke-WebRequest`; `curl.exe` is in `System32`.
- §11's `enough_rows` reads the table with pandas and reports **764** rows, the
  number `dataops` wrote.

Finally, a cold run on 2026-09-11 — `reset.sh`, `up.sh`, then every command
in §2–§14 in order:

- 28s from `up.sh` to seven Dags listed, all paused; the health endpoint
  `healthy` four times.
- §4 `(7 words)`, and `HOLA` with `--conf`. Tasks ran one at a time —
  `count_words` before `shout` this time; the order between siblings varies.
- §6 `product=42`. §7 `manual-demo-1` success; `demo` queued while paused,
  success after un-pause with `start_date` 14s after `run_after`.
- §9's backfill (then in §7), both ways. Without `--dry-run`, `2026-01-01`→`01-31` created
  30 queued `backfill__` runs against the paused `dataops`; at §10 they ran
  three at a time and re-triggered `mlops` behind them. The CLI has only
  `backfill create`; `PUT /api/v2/backfills/1/cancel` returned 200 and turned
  the six still-queued runs `failed`. With `--dry-run`, `02-01`→`02-04` listed
  three rows and created nothing — which is why §9 leads with it.
- §8's coalescing: 22 asset events became 6 `mlops` runs, consuming 2–6 each
  (`dagrun_asset_event`).
- §9 `passed=4/4`, `6/6`, `Wrote 764 rows`; with `mlops` paused, one event and
  zero runs.
- §11 `deferred → scheduled → success` through the scheduler; both sensors
  `up_for_reschedule` with the file moved; `764 rows` once restored.
- §12 76s with `--conf` — `awaiting_input`, `Approved`, `trigger_dag_id=mlops`,
  one new `manual__` run; 9s and `continue_serving` without it.
- §13 `DEADLINE MISSED` at +33s with `slow_step` still `running`. §14 30
  passed, listings in sync.

Added on 2026-09-14, same stack: §4's `list-import-errors`. With the seven
lesson files, `No data found`. With an eighth file whose second line imports a
module that does not exist, one row carrying the traceback — and `airflow dags
list` still showed seven, because a file that fails to import is no Dag, not a
broken one. Deleting the file and restarting the dag-processor cleared the row.

Also on 2026-09-14: §2's start-up, cold, with `AIRFLOW_UID=1000` in `.env`
(what `up.sh` writes on WSL2). With the previous compose file, `airflow-init`
failed inside 0.2 s — `ModuleNotFoundError: No module named 'airflow'` — and
exited 0 anyway; the four services then restarted forever on `Database
migration required`, and `up.ps1` waited forever. Replayed in a one-off
container under the same conditions, the failure is deterministic: `HOME=/`,
so the `airflow` script's interpreter never sees `/home/airflow/.local`. With
init routed through the image entrypoint, the same `.env` and the same empty
database: `Database migration done!`, health endpoint `healthy` 33 s after
`up`, seven Dags listed.

Restructured on 2026-09-15 into a write-it-yourself tutorial — `dags/`
empty, the finished files moved to `solutions/`, every lesson rebuilt as typed
pieces — and cold-run again in the new order: `reset.sh`, `up.sh`, the seven
files typed from the "Build it" blocks, every command in §2–§14 as written.

- §2 with an empty `dags/`: `airflow dags list`, `list-import-errors` and
  `assets list` all print `No data found`; the health endpoint `healthy` four
  times.
- §3's parsing intervals are this stack's live config: `refresh_interval=5`,
  `min_file_process_interval=5`, `scheduler_idle_sleep_time=1`. A file saved
  into `dags/` was listed by `airflow dags list` **2 seconds** later.
- §4's broken file: with the comment `# An airflow dag file that does not
  import`, the row was in `list-import-errors` 6 s after saving and gone 6 s
  after deleting the file, no restart. **Without** the words `airflow` and
  `dag` in the file, nothing appeared in 90 s and the Dag processor's log never
  mentioned the file. `[core] dag_discovery_safe_mode` is `True` here, and
  `might_contain_dag_via_default_heuristic` (`airflow/utils/file.py`) requires
  the lowercased contents to contain `airflow` and one of `dag` / `asset`. The
  2026-09-14 check above passed because its comment happened to contain both.
- §4 `(7 words)`, `HOLA` with `--conf`. §6 `product=42`, then `TaskFlow saw
  product=42`.
- §7's catch-up rule, read off `CronTriggerTimetable.next_dagrun_info`
  (`airflow/timetables/trigger.py`): with `catchup=False` and no previous run,
  the start is `max(align_to_prev(utcnow()), align_to_next(start_date))` — the
  most recent tick before now. On this stack, un-pausing `dataops` at 12:22
  UTC created `scheduled__2026-09-15T03:00:00+00:00` with a `start_date` of
  12:22:28, five seconds after the un-pause. The earlier text's "you get the
  next one only" was wrong and is gone.
- §9 with `dataops` saved and nothing run: `assets list` shows
  `labeled_projects` and `raw_projects` only; `assets details` has
  `producing_tasks [dataops.validate_transformed]` and `scheduled_dags []`.
  `dags test dataops`: `passed=4/4`, `Wrote 764 rows`, `passed=6/6`, both
  `checks, failed=none` lines, 5.3 s. Backfill dry-run `02-01`→`02-04`: three
  rows, nothing created.
- §8's third case, a consumer that did not exist yet. The asset event from
  that `dags test dataops` (id 2 in `asset_event`) was never consumed: after
  `mlops` was written and un-paused, `dagrun_asset_event` links its runs to
  events 4 and 6 — the `scheduled__` and a later `manual__` `dataops` run —
  and never to 2.
- §10 with `mlops` saved: `scheduled_dags [mlops]`. Un-pausing `mlops`, then
  `dataops`, no trigger: one `scheduled__…T03:00:00` `dataops` run; one
  `asset_triggered__` `mlops` run queued 1.5 s after its event, with an empty
  `logical_date`; `train_candidate` map indexes 2 and 3 started after 0 and 1
  ended; `promote_model` success, `keep_current_model` skipped. The Try-it
  with `PROJECT_TAGGER_MIN_F1=0.99`: `below threshold 0.99 - not promoting`, `Branch
  into keep_current_model`.
- §11 `dags test`: `curated table has 764 rows`, `Success criteria met`,
  `Data is ready (764 rows)`, 5.6 s. Through the scheduler, polled every half
  second: `settle` `deferred` at +2 s, `success` at +8 s.
- §12 with `--conf`: 74 s; `Branch into request_retraining`, `Waiting for
  response`, `Approved` 65 s later, `trigger_dag_id=mlops`, and one new
  `manual__` run of `mlops`.
- §13: `DEADLINE MISSED` at +31 s, `slow_step` still running.
- §14: the seven typed files pass the integrity suite (27 with the workload
  tests); the same suite with `DAGS_DIR=solutions` passes; the "Build it"
  blocks in both READMEs assemble into files that import; **36 passed**
  in total.
- §5's two "Do it now" steps and the UI tour rest on the shipped UI strings
  and the API endpoints behind them, as before — nobody clicked through them
  for this run.

Checked on 2026-10-10 against the 3.3.1 source in this stack's image, after
a reader caught a wrong claim about `run_after`:

- `run_after` is "the date before which dag run won't start"
  (`airflow/models/dagrun.py`): the earliest start, not the actual one.
- §7's `start_date`-less schedule: `CronTriggerTimetable` defaults to
  `run_immediately=False`, and `next_dagrun_info` with no `start_date` gave
  tomorrow 03:00 for `"0 3 * * *"` at 08:04 UTC, while `DeltaTriggerTimetable`
  gave now. A paused probe Dag without `start_date` on `*/2 * * * *` had its
  next run recomputed at every parse (08:00, then 08:02), so the first run is
  the next tick after the un-pause, not after the first parse.
- §8's queueing: `AssetManager.register_asset_change` writes the queue entry
  for every unpaused consumer in the same step as the event; the scheduler
  only turns those entries into runs.
- §9's backfill range: `iter_dagrun_infos_between` includes both ends. For
  `dataops`, `2026-02-01` → `2026-02-04` gives three runs and `2026-02-01` →
  `2026-02-04T03:00:00` gives four.
- §4's backoff: `retry_exponential_backoff` is a multiplier (`float`, default
  0); `True` still deserializes to `2.0`.

Six real defects were found while building this, all documented in
[troubleshooting](troubleshooting.md):

- `fs_default` is not created for you.
- `airflow dags test` breaks on Dags that set `deadline=` (an Airflow 3.3.1
  bug).
- `DagBag` needs importing from `airflow.dag_processing.dagbag`.
- `context["logical_date"]` raises `KeyError` on any run without a logical
  date — found by a manual trigger, after every test in §14 had passed.
- `{{ ds }}` raises `UndefinedError` on the same runs — found the same way.
- `airflow-init` bypassed the image entrypoint, could not import Airflow, and
  still exited 0 — found by a cold start with a WSL2 uid in `.env`.
