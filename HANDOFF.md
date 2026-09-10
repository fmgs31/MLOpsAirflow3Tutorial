# Handoff — Airflow 3 rewrite of the Made With ML orchestration lesson

Sessions: 2026-09-09 (build), 2026-09-09 UTC (second pass — TOC gaps closed).
Working dir: `D:\Proyectos\Airflow3Tutorial`.

## What this project is

An updated, runnable version of
<https://madewithml.com/courses/mlops/orchestration/> (written for Airflow
2.3.3) rewritten for **Airflow 3.3.1**. The lesson structure is kept — DataOps →
MLOps → continual learning — but it runs locally with no Airbyte / dbt Cloud /
BigQuery / GCP.

## Current state: built, verified, and TOC-complete

Everything below was executed against a live `apache/airflow:3.3.1` stack
(Postgres 16, LocalExecutor, api-server + scheduler + dag-processor + triggerer):

- 7 Dags, **0 import errors**
- `airflow dags test` green on all 6 testable Dags
- Asset chain re-verified after the second pass: triggering `dataops` produced an
  `asset_triggered__…` run of `mlops` with no manual trigger, which validated the
  snapshot, trained 4 mapped candidates and promoted f1 = 0.884
- Manual triggers exercised for real (this is what found the two new bugs below)
- HITL verified: `awaiting_input` → default `Approve` after timeout →
  `TriggerDagRunOperator` fired
- Deadline callback fired at exactly +30s with full `dag_run` context
- **pytest: 27 passed**

The Docker stack was left **running** at <http://localhost:8080> (no login —
`SIMPLE_AUTH_MANAGER_ALL_ADMINS=true`, port bound to 127.0.0.1). It may since
have been stopped; `./scripts/up.sh` or `.\scripts\up.ps1` restarts it.

## Layout

```
README.md                   the tutorial itself (15 sections + verification)
HANDOFF.md                  this file
dags/01..07_*.py            seven lesson Dags, numbered in reading order
src/tagifai/                the ML project Airflow orchestrates (imports no Airflow)
  config.py data.py validate.py train.py evaluate.py
  registry.py monitoring.py assets.py callbacks.py predict.py
tests/test_dag_integrity.py Dag integrity + "no Airflow 2 constructs" grep tests
tests/test_tagifai.py       unit tests on the ML code
docs/airflow2-to-airflow3.md  full migration reference (the highest-value doc)
docs/cheatsheet.md            Airflow 3 CLI cheatsheet
docs/troubleshooting.md       every real defect hit, with the fix
docker-compose.yaml         LocalExecutor + Postgres + Simple auth
Dockerfile                  extends apache/airflow:3.3.1 with pandas/sklearn/pytest
scripts/up.sh up.ps1 reset.sh setup-native.sh
config/airflow.cfg.example  annotated Airflow 3 settings reference
env.example                 copy to .env (note: `.env.example` is blocked by a
                            permission deny rule — do not recreate it)
```

The Dags: `hello_airflow3`, `operators_and_xcoms`, `dataops`, `mlops`,
`sensors_and_events`, `monitoring`, `deadline_alerts`.

## Original TOC coverage: closed

The four gaps listed in the previous handoff were the work of this session and
are all done:

1. **UI walkthrough** — new README **§4 The UI**. Every claim in it was checked
   against the running api-server rather than remembered: nav, Dag-page tabs,
   Grid/Graph panel controls, the trigger and backfill form fields and the
   version selector come from the shipped UI strings at
   `airflow/ui/dist/i18n/locales/en/*.json` inside the container, and the
   endpoints behind them from `GET /openapi.json`. Re-verify the same way if the
   image is bumped.
2. **Validation before training** — `validate_training_data` in
   `dags/04_mlops.py`, upstream of every mapped `train_candidate`. It re-runs the
   `labeled_projects` suite on the snapshot on disk and adds
   `validate.check_split()` (new in `src/tagifai/validate.py`): non-empty splits,
   every class in both splits, `MIN_ROWS_PER_CLASS` (new in `config.py`, default
   20), no id leakage, and a non-critical test-fraction band. README §10 and the
   §1 graph updated.
3. **Relationships** — new README §6 subsection (`>>`, `a >> [b, c] >> d`,
   `chain` + `Label`, `cross_downstream`, and the two shapes that do *not* work),
   demonstrated at the tail of `dags/02_operators_and_xcoms.py`.
4. **Manual runs** — new README §7 subsection: `unpause` / `trigger -c -r -l`,
   the paused-Dag-stays-queued behaviour with a real transcript, the UI trigger
   form, and the logical-date trap below.

Also added: a "what Airflow is / when not to use it" opening to §3.

### Second comparison pass — against the live original page

The original lesson was re-fetched and diffed heading by heading (not from
memory). Nine smaller gaps turned up and were closed in the same pass:

| Original content | Where it now lives |
|---|---|
| the orchestrator landscape (Prefect, Dagster, Luigi, Metaflow, Flyte, Kubeflow, Vertex) and why Airflow | §1 "Why Airflow and not one of the others" |
| what `airflow db init` generates in `$AIRFLOW_HOME` | §2, incl. what is *gone* in 3 (`unittests.cfg`, `webserver_config.py`) |
| the `with DAG(...)` context-manager form | §5 (we only showed `@dag`) |
| the provider ecosystem, and writing a custom operator | §6, subclassing `airflow.sdk.BaseOperator` |
| `set_upstream` / `set_downstream` | §6 Relationships |
| Dags are created paused (`dags_are_paused_at_creation`) | §7 Manual runs |
| the Airbyte / GE / dbt-Cloud operators *and their Connections* | §9 "Swapping in the real vendors" |
| trigger rules | §10, the full list of useful ones |
| the original's `extract_from_dwh >> validate >> optimize >> train` mapping | §10 mapping table (incl. Optuna → dynamic mapping) |
| drift types: data / target / concept | §12 table, mapped to what `monitoring.py` actually measures |
| retraining: labeling, active learning, QA, augmentation, sampling, evaluation, catastrophic forgetting | §12 "What 'retrain' leaves out" |

Everything in the original's table of contents is now present. The deliberate
*non*-matches are the vendor swaps (Airbyte/GE/dbt/BigQuery → local equivalents,
documented slot for slot) and everything Airflow 2 that no longer exists.

### Third comparison pass — the original's asides, not its headings

Re-fetched asking for every callout, aside and named-but-not-coded concept.
Its headings were already covered; five things mentioned only in passing were
not:

| Original aside | Where it now lives |
|---|---|
| "sensors and **hooks** to external databases, file systems" | §6 "Hooks, Connections and Variables" — hook = the client under an operator, with `FSHook`/`BaseHook` |
| **Variables** and **Connections** as concepts (not just Admin pages) | same subsection: `Variable.get`, `Connection.get`, `AIRFLOW_VAR_*`/`AIRFLOW_CONN_*`, `{{ var.value.x }}`, and the Airflow 3 change (fetched over the Task Execution API, not the DB) |
| "explore … **smart sensors**" | §11 callout: removed in 2.4, `airflow.sensors.smart_sensor` is gone, deferrable operators replaced them. Also a migration-doc row |
| "explore … **monitoring**" | §15 Observability, rewritten: StatsD/OTel metrics, OTel traces, the health endpoint, and the 3.x metric-naming change (`legacy_names_on`, tags instead of name suffixes) |
| "we use the word **continual** (repeat with breaks) instead of continuous" | §12 opening paragraph — it is the justification for the HITL gate |

Plus a §1 note mapping the four *other* course lessons the original depends on
(data stack, testing, experiment tracking, monitoring) onto the `src/tagifai/`
module that stands in for each here.

Verified in the container for this pass: `airflow.sdk.BaseHook` (old
`airflow.hooks.base` warns), `Variable.get/set/delete`, `Connection.get`,
`FSHook.get_path()`, `airflow.sensors.smart_sensor` absent, the four metric
names in the emitting code, and `[metrics] legacy_names_on` default `True`
(added 3.2). Also found: `airflow.utils.trigger_rule` → `airflow.task.trigger_rule`.

### Fourth pass — identifier checklist + document audit

Asked the original for a flat checklist of every literal it shows the reader
(commands, flags, config keys, env vars, file paths, pinned packages, task ids)
and walked it. Everything had a counterpart except three, now added:

- macOS `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` → troubleshooting entry,
  flagged macOS-only and **not** reproducible on this Windows/Docker stack.
  It masks a real fork-safety violation; Docker avoids the class of problem.
- Where `checkpoint_name=` and the dbt model paths come from → §9 now shows the
  `great_expectations init / datasource new / suite new / checkpoint new` and
  `dbt init` steps that happen *outside* Airflow.
- `description=` vs `doc_md=` on a Dag, and `@task(doc_md=...)` → §5 *(verified)*.

The README was also audited as a document after four rounds of edits: code
fences balanced, section numbering 1–15 with no gaps, every TOC anchor resolves,
every `§n` reference in range, every relative link points at a file that exists,
no duplicate headings. The same fence/link check passes on all three `docs/`
files. Re-run that audit after any structural edit — the script is trivial to
rewrite, or just check TOC anchors and `§` refs by hand.

**Do not add more coverage passes.** Four have been done (headings, asides,
identifier checklist, document audit) and the last one found only three
one-line gaps. The remaining risk is not missing content, it is length.

## The README now contains every Dag in full — generated, not copied

Measured before this change: the README quoted **20%** of the repo's code
(423-line Dags at 21–63% each). The original lesson, by contrast, shows enough
of its DAG code that a reader can assemble the file. Closing that gap by
pasting listings in would have rotted on the first Dag edit, so it is generated:

- **`scripts/sync_readme.py`** — fills `<!-- BEGIN FILE: path -->` /
  `<!-- END FILE: path -->` marker pairs in README.md with the file's current
  contents, in a fenced block. `--check` writes nothing and exits 1, naming the
  stale listings. Add a listing by pasting an empty marker pair and re-running.
- **README §16 "Appendix: every Dag in full"** — the 7 Dags plus
  `src/tagifai/assets.py`, each inside a `<details>` block so the tour above
  stays readable. Dags are now **100% present** (423/423 lines). The rest of
  `src/tagifai/` is deliberately not inlined: it is the ML workload, not the
  orchestration, and the original does not show its equivalent either.
- **`tests/test_readme_listings.py`** — three tests: listings in sync, every
  `dags/*.py` has a listing, and a tampered block is actually detected (that
  last one exists because the first version of it passed for the wrong reason —
  it edited a teaching excerpt outside the markers).
- **`docker-compose.yaml`** — `./README.md` and `./scripts` mounted read-only,
  because the suite runs inside the container and neither was visible there. The
  test skips rather than fails if they are absent.

Verified end to end: appending a line to `dags/07_deadline_alerts.py` made
`--check` exit 1 naming that file; restoring it returned to green.

**The README is now 2310 lines**, ~1000 of them the generated appendix. That is
the trade that was chosen: completeness with no drift risk, at the cost of a
long file. Never hand-edit anything between the markers — run the script.

Sections renumbered — the UI section is §4, so everything after shifted by one.
Cross-references (`§7`, `§8`, `§11`, …) were updated; check them if you insert
another section.

## Two new defects found — in our own Dags

Both were live bugs that **every existing test passed**, because `airflow dags
test` always supplies a logical date and a real trigger does not:

- `dags/01` did `context["logical_date"]` → **`KeyError: 'logical_date'`** on a
  manual trigger. The key is absent, not `None`. Fixed with `.get()`.
- `dags/02` templated `{{ ds }}` → **`UndefinedError: 'ds' is undefined`** on the
  same run, failing at render time. Fixed with `{{ ds | default(...) }}` plus
  `{{ dag_run.run_after }}`.

Documented in `docs/troubleshooting.md`, `docs/airflow2-to-airflow3.md` §4 and
README §7. **The lesson generalises: trigger every Dag once for real before
believing it.** `mlops` is asset-triggered, so all of its runs lack a logical
date too.

## Hard-won facts — do not re-derive

Verified against the running stack; several contradict what the docs imply.

- **Latest stable is 3.3.1**, Python `>=3.10, !=3.15`. Standard provider 1.18.0.
- **`fs_default` is no longer auto-created.** Airflow 2's `db init` seeded default
  connections; `db migrate` does not. `FileSensor` fails with
  `AirflowNotFoundException: The conn_id 'fs_default' isn't defined`. Fixed in
  compose via `AIRFLOW_CONN_FS_DEFAULT`.
- **A run without a logical date drops the context key and undefines `ds`/`ts`.**
  See above.
- **`airflow dags test` crashes on any Dag with `deadline=`** on 3.3.1:
  `psycopg2.ProgrammingError: can't adapt type 'dict'` from
  `DagRun.update_state` → `DeadlineAlertModel.get_by_id`. The tasks all run to
  completion first; only the CLI's final state update fails. This is why
  deadlines live in their own Dag (`07`) and `mlops` has none. Trigger deadline
  Dags through the scheduler instead.
- **`DagBag` moved** to `airflow.dag_processing.dagbag` and **dropped
  `include_examples`**. The old `airflow.models.dagbag` path warns.
- **`airflow dags list-runs` takes `dag_id` positionally** — `-d` is gone.
- **`airflow backfill` has only `create`** — no `list` subcommand.
- **`airflow.exceptions.X` is deprecated for task code** → use
  `airflow.sdk.exceptions.X`. Confirmed by a runtime warning.
- **`airflow users` / `roles` exist in the official image** because it bundles the
  FAB provider — but they are FAB commands, not core ones.
- **A TaskFlow argument named `params` collides with the context key.** The mlops
  training task uses `hparams` for this reason.
- **`[a, b] >> [c, d]` is a TypeError** (no list-to-list operator);
  `chain([a, b], [c, d])` pairs element-wise and requires equal lengths;
  `cross_downstream` is the all-to-all form. All three checked in the container.
- **`airflow dags list` prints one row per Dag version**, so a Dag edited four
  times appears four times. Not a bug — it is Dag versioning.
- **Parallel `docker compose build` races** on the shared image tag
  (`image "airflow3-tutorial:3.3.1": already exists`). Build one service:
  `docker compose build airflow-init`.
- **Git Bash mangles container paths.** Prefix with `MSYS_NO_PATHCONV=1`, e.g.
  `MSYS_NO_PATHCONV=1 docker compose run --rm --entrypoint pytest airflow-cli /opt/airflow/tests -q`.
- Assets here are **name-only** (`Asset(name="labeled_projects")`) so identity is
  the same in Docker and native runs.
- `select_best` logs a benign `XCom not found … offset=4` error: that is how the
  lazy mapped-XCom sequence detects its end after 4 candidates.

## Commands to re-verify after any change

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags test <dag_id>
MSYS_NO_PATHCONV=1 docker compose run --rm --entrypoint pytest airflow-cli /opt/airflow/tests -q

# after editing any Dag — regenerates the README §16 listings:
python scripts/sync_readme.py

# and the one the tests cannot replace:
docker compose exec airflow-scheduler bash -c '
  airflow dags unpause <dag_id> && airflow dags trigger <dag_id> -r smoke-1'
docker compose exec airflow-scheduler airflow dags list-runs <dag_id> -o plain
```

The integrity tests enforce, for every Dag: no import errors, no cycles, tags +
`doc_md` + an explicit non-`airflow` owner, `retries >= 1` (except the three demo
Dags), the `mlops`↔`labeled_projects` asset wiring, `validate_training_data`
upstream of `train_candidate`, and a source grep banning `schedule_interval=`,
`days_ago(`, `airflow.operators.`, `execution_date`, `SubDagOperator`, `sla=`.

## Nothing is open

The original lesson's table of contents is now fully mapped. Optional polish, in
rough order of value if someone picks this up again:

- Nothing in the repo is committed yet — `git status` shows the whole tree staged
  as `A`. A first commit is the obvious next move.
- The README is long (~1000 lines). Splitting §§9–13 into `docs/` would help a
  reader who wants only the migration material, at the cost of the single-file
  read-through this was written for.
- No CI workflow. `tests/` is designed for it: one `docker compose run pytest`
  job would catch the Airflow-2-construct regressions the greps look for.

## Conventions used

- Docs are written in normal prose; the chat mode for this session was a
  compressed "caveman" style that does **not** apply to files.
- Comments in the Dags explain the *Airflow 2 → 3 delta* at the point where it
  matters, not generic Python.
- `src/tagifai/` never imports Airflow, so the ML code stays testable without a
  scheduler.
- Temporary files go in the session scratchpad, not the repo (a `tmp/` dir exists
  in the project and is gitignored; nothing was written to it).
