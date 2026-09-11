# Handoff

Working dir: `D:\Proyectos\Airflow3Tutorial`. Last session: 2026-09-11.
A note for whoever picks the work up next, not documentation — delete it once
it stops being true.

## What this is

A hands-on **Airflow 3.3.1** tutorial: seven Dags that build into one system
(DataOps → Asset → MLOps → monitoring/retraining), all in Docker, no cloud
account. The ML project being orchestrated is `src/tagifai/` and imports no
Airflow. The pipeline's shape follows the Made With ML orchestration lesson;
the Airflow is 3.x, written from scratch.

## The reader, and the rule that follows

**Francisco is learning Airflow 3 as his first Airflow** and does not care
about Airflow 2. So: **no Airflow 2 anywhere in the tutorial** — no "used to
be", no migration framing; behaviour is explained as how Airflow works and a
gotcha is written as a gotcha. Applies to README prose, Dag comments and
docstrings, cheatsheet, troubleshooting. The one exception is
`docs/airflow2-to-airflow3.md`, deliberately unlinked. This rule is also in
project memory.

## The standard this tutorial is held to

Trust broke mid-session on 2026-09-11 and was rebuilt; the lesson is now the
working rule:

- **Every sentence in the README body must stand on its own for a reader.**
  A number they cannot see by running the command in front of them is junk
  (the case in point: "the run appeared 11 seconds later", from a stopwatch
  around a shell loop that matched the wrong run).
- **Measured evidence lives in §17 ("What was actually verified")**, with how
  it was measured. The body says what the reader will see; §17 says how we
  know.
- **Mechanism explanations are fine without numbers**; specifics must be read
  off the stack, the source, or the config — never recalled.
- **A cold run is the acceptance test**: `reset.sh`, `up.sh`, every command in
  §2–§14 in order. Two have been done (both 2026-09-11); the second, after all
  edits, held except for the backfill finding below, which is now fixed.

## State

- Branch `airflow3-only-tutorial`, last commit `e48eba4`. **Uncommitted:**
  `README.md` (+~660/−50), `dags/01_hello_airflow3.py` (one comment),
  `dags/05_sensors_and_events.py` (docstring, one comment, `enough_rows` now
  counts rows with pandas). Committing and merging to `main` are Francisco's
  call — ask.
- **30 tests pass**, 0 import errors, listings in sync.
- Docker stack running on <http://localhost:8080>, no login. It carries the
  second cold run's debris (30 `backfill__` dataops runs, ~8 mlops runs);
  `./scripts/reset.sh` then `up.sh` gives a clean one in ~30s. Docker Desktop
  gets closed overnight — a missing engine pipe means shut down, not broken.
- Pause state after the cold run: `dataops`, `mlops`, `hello_airflow3`,
  `deadline_alerts` un-paused; `monitoring`, `operators_and_xcoms`,
  `sensors_and_events` paused.

## What the 2026-09-11 session did

1. **Fixed the four cold-run defects** (§5 `(7 words)`; missing checkpoints in
   §7/§8/§11/§12/§14; §12's 60s `awaiting_input` warning). The fourth was
   misdiagnosed in the previous handoff: a paused consumer does **not** queue
   an asset event for later — it is filtered at enqueue time
   (`assets/manager.py:354`) and un-pausing replays nothing. The extra run
   came from `airflow dags test dataops` emitting a real asset event while
   `mlops` was un-paused. Both facts are in §8/§9 now.
2. **Added the depth the previous handoff asked for** — executor, scheduler
   loop, workers, metadata DB, task lifecycle (`queued` vs `running`), retries,
   pools/concurrency — in §3/§5/§10, then re-verified every claim after the
   trust break.
3. **Walked §1–§15 as a first-time reader** and fixed 23 findings: Jinja
   templating (§6), data intervals and what a scheduled run is *for* (§7),
   what `dags test` does differently (§5 box), "trigger" the noun (§11),
   `dataops`/`monitoring` have real crons (§9), TaskFlow/bundle/owner/Timetable
   named at first use, `start_date` is not required (it isn't — verified with
   `DagBag`), PowerShell `curl.exe`, §13 unpause-before-trigger, §10 Try-it as
   one runnable command, XCom-vs-"never touches the DB" contradiction, FAB
   expanded, and more — all listed in §17.
4. **Second cold run** found one real defect: §7's backfill example
   (`2026-01-01`→`01-31`) created 30 queued runs against the paused `dataops`,
   which all ran at §10 and re-triggered `mlops` behind them; the 3.3.1 CLI has
   only `backfill create`. §7 now leads with `--dry-run`, states `--to-date` is
   exclusive, and gives the cancel route (`PUT /api/v2/backfills/{id}/cancel`,
   the Backfills tab) and the brake (pause the Dag).

## Do not break: the generated appendix

README §16 lists every Dag in full, injected between `<!-- BEGIN FILE -->` /
`<!-- END FILE -->` markers by `python scripts/sync_readme.py`. Never hand-edit
inside the markers. **After editing any Dag, re-run the script**, or
`tests/test_readme_listings.py` fails. Dag comments are tutorial text — they
render into the README.

## Hard-won facts — do not re-derive

All verified on the running 3.3.1 stack or in its source.

- **`airflow dags test`** runs tasks one at a time; always supplies a logical
  date (a real manual/asset run has none — `context["logical_date"]` raises
  `KeyError`, `{{ ds }}` raises `UndefinedError`); is **not a dry run** (real
  files, real asset events); runs triggers inline (`_run_inline_trigger`), so
  `settle` never reaches the triggerer under it — and never even defers,
  because its 5s target is measured from the logical date the command set
  earlier; crashes on any Dag with `deadline=` (`can't adapt type 'dict'`).
- **Asset events**: written when the outlet task succeeds; a **paused**
  consumer is skipped at enqueue and never catches up; events that arrive
  while the consumer is busy **coalesce** into the next run (22 events → 6
  `mlops` runs, 2–6 each).
- **Scheduler**: 1s idle sleep; event→queued run gap observed 0.12–0.65s.
  `min_file_process_interval=30`, `refresh_interval=300`.
- **LocalExecutor** is a `multiprocessing` component of the scheduler process;
  no executor container. Limits here: `parallelism=32`,
  `max_active_tasks_per_dag=16`, `default_pool=128`; `train_candidate` is
  capped by `max_active_tis_per_dag=2` → two waves of two.
- **Timetables**: cron string → `CronTriggerTimetable`
  (`create_cron_data_intervals=False`), `run_after == start == end`;
  `timedelta` → `DeltaDataIntervalTimetable`, run at interval end,
  `logical_date` = start.
- **HITL on 3.3**: `ApprovalOperator` parks in `awaiting_input`, no trigger;
  the scheduler's sweep resolves it after `response_timeout`; `responded_by`
  is empty for a timeout default.
- **Backfill**: `--to-date` exclusive; runs created `queued`; CLI has only
  `create` (with `--dry-run`); cancel via `PUT /api/v2/backfills/{id}/cancel`
  (no token on this stack; `/auth/token` hands anyone an admin JWT).
- `default_task_retries=0`; the integrity test demands `retries>=1` except
  `hello_airflow3`, `sensors_and_events`, `deadline_alerts`, and a non-`airflow`
  owner everywhere.
- `dataops` is `0 3 * * *`, `monitoring` `0 6 * * *`; both `catchup=False`.
- `[a, b] >> [c, d]` is a TypeError; `chain` pairs; `cross_downstream` is
  all-to-all. A TaskFlow argument named `params` collides with the context.
- No default connections; `fs_default` comes from `AIRFLOW_CONN_FS_DEFAULT`.
- `airflow dags list` prints one row per Dag version. Git Bash mangles
  `/opt/...` paths: prefix `MSYS_NO_PATHCONV=1`. PowerShell's `curl` is
  `Invoke-WebRequest`; use `curl.exe`. Build one service
  (`docker compose build airflow-init`) — parallel builds race.
- `select_best` logs a benign `XCom not found … offset=4`. Assets are
  name-only so identity matches in Docker and native runs. `extract` downloads
  the CSV (764 rows) over HTTPS — the one task that needs internet.

## Verify after any change

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags test <dag_id>
MSYS_NO_PATHCONV=1 docker compose run --rm --entrypoint pytest airflow-cli /opt/airflow/tests -q
python scripts/sync_readme.py            # after editing a Dag
# and trigger it the way it will really run:
docker compose exec airflow-scheduler bash -c 'airflow dags unpause <dag_id> && airflow dags trigger <dag_id> -r smoke-1'
```

## Open

1. **Commit / merge** — Francisco's call. Three files uncommitted.
2. **A human in the UI.** Everything CLI-observable has been cold-run twice.
   Nobody has clicked **Approve** on the Required Actions tab, opened the
   Gantt to see the two waves, or used the Backfills tab's cancel; those
   sentences rest on §4's verified UI strings, not on a click.
3. No CI. One `docker compose run pytest` job would carry all 30 tests.
4. **Length.** README ≈ 2,950 lines, ~1,000 of them the appendix. If it must
   shrink, move §§9–13 to `docs/`; keep the appendix.
5. `05`'s `settle` only defers through the scheduler; if that demo should
   work under `dags test` too, measure its target from `run_after` instead —
   a Dag design change, not done.

## Conventions

- Prose on disk is normal English; the chat "caveman" mode never applies to
  files.
- `src/tagifai/` never imports Airflow. Temp files go in the session
  scratchpad. `env.example` is the template; do not create `.env.example`.
