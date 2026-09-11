# Handoff

Working dir: `D:\Proyectos\Airflow3Tutorial`. Last session: 2026-09-09/10.
This is a note for whoever picks the work up next, not documentation — delete it
once it stops being true.

## What this is

A hands-on **Airflow 3.3.1** tutorial. Seven Dags that build into one system:
DataOps → Asset → MLOps → monitoring/retraining. Runs entirely in Docker with no
cloud account. The ML project being orchestrated lives in `src/tagifai/` and
imports no Airflow.

The pipeline's shape follows the [Made With ML orchestration
lesson](https://madewithml.com/courses/mlops/orchestration/); the Airflow is all
3.x and written from scratch.

## The reader, and the one rule that follows from it

**The reader is learning Airflow 3 as their first Airflow.** They have said
plainly that they know nothing about Airflow 2 and do not care about it.

So: **no Airflow 2 anywhere in the tutorial.** No "this used to be called…", no
"replaces the old…", no migration framing. Behaviour that would once have been
explained as a *change* is explained as how Airflow works, and a gotcha is
written as a gotcha ("`xcom_pull` without `task_ids` reads the current task's own
XComs") rather than as a comparison. This applies to README prose, Dag comments
and docstrings, the cheatsheet and troubleshooting.

The one exception is `docs/airflow2-to-airflow3.md`, kept deliberately and
**unlinked from everything**, with a banner at the top saying it is not part of
the tutorial. It is there for the day someone inherits an Airflow 2 codebase.
Do not link it from the README.

## State

- Branch **`airflow3-only-tutorial`**, off `main` at commit `f95aa9a` (the
  pre-rewrite snapshot). The rewrite is **not committed** — `git diff` on the
  branch is the whole change.
- **30 tests pass** in the container; **0 import errors**; `dags test` green on
  `hello_airflow3` and `operators_and_xcoms` after the comment rewrite.
- Docker stack was left running on <http://localhost:8080> (no login;
  `./scripts/up.sh` or `.\scripts\up.ps1` restarts it).

### What the last session did

1. Rewrote `README.md` as a follow-along tutorial: every section has something to
   run and a **Checkpoint** telling the reader what they should see, several have
   a **Try it** exercise. 17 sections; §16 is the generated appendix.
2. Stripped every Airflow 2 reference from README, all seven Dags,
   `src/tagifai/assets.py`, `src/tagifai/validate.py`, the cheatsheet and
   troubleshooting.
3. Renamed the integrity test's `AIRFLOW_2_PATTERNS` → `STALE_CONSTRUCTS` and
   `test_no_airflow_2_constructs` → `test_no_stale_constructs`, reframed as
   "most Airflow code online predates Airflow 3, keep it out of this repo" —
   which is a real risk for this reader and worth keeping.

## Do not break: the generated appendix

README §16 lists every Dag **in full**, and those listings are generated:

```bash
python scripts/sync_readme.py            # inject file contents into the markers
python scripts/sync_readme.py --check    # exit 1 if stale; this is what pytest runs
```

Never hand-edit anything between `<!-- BEGIN FILE: path -->` and
`<!-- END FILE: path -->`. **After editing any Dag, re-run the script**, or
`tests/test_readme_listings.py` fails. Add a listing by pasting an empty marker
pair into the README and running the script. `docker-compose.yaml` mounts
`./README.md` and `./scripts` read-only so the check works inside the container.

Consequence worth remembering: **Dag comments are tutorial text.** They are
rendered into the README verbatim, so write them for a learner.

## Hard-won facts — do not re-derive

Verified against the running stack.

- **`airflow dags test` always supplies a logical date; real triggers may not.**
  A manual or asset-triggered run has none, so `context["logical_date"]` raises
  `KeyError` (the key is absent, not `None`) and `{{ ds }}` raises
  `UndefinedError` while rendering. Both were live bugs here, found only by
  triggering for real. Use `.get()`, `{{ ds | default(...) }}` or
  `{{ dag_run.run_after }}`. `mlops` is asset-triggered, so every one of its runs
  is in this state.
- **`airflow dags test` crashes on any Dag with `deadline=`** on 3.3.1:
  `psycopg2.ProgrammingError: can't adapt type 'dict'`. The tasks complete
  first; only the CLI's final state update fails. This is why deadlines live
  only in `dags/07` and `mlops` has none.
- **No default connections are created.** `FileSensor` needs `fs_default`;
  compose supplies it via `AIRFLOW_CONN_FS_DEFAULT`.
- **A TaskFlow argument named `params` collides with the context key** — the
  mlops training task uses `hparams`.
- **`[a, b] >> [c, d]` is a TypeError**; `chain([a, b], [c, d])` pairs
  element-wise; `cross_downstream` is the all-to-all form.
- **`airflow dags list` prints one row per Dag version**, so an edited Dag
  appears several times. Not a bug — that is Dag versioning.
- **Parallel `docker compose build` races** on the shared image tag; build one
  service: `docker compose build airflow-init`.
- **Git Bash mangles container paths.** Prefix with `MSYS_NO_PATHCONV=1`.
- `select_best` logs a benign `XCom not found … offset=4`: that is how the lazy
  mapped-XCom sequence finds its end.
- Assets are **name-only** so identity matches in Docker and native runs.

## Verify after any change

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags test <dag_id>
MSYS_NO_PATHCONV=1 docker compose run --rm --entrypoint pytest airflow-cli /opt/airflow/tests -q
python scripts/sync_readme.py            # after editing a Dag

# what tests cannot replace — trigger it the way it will really run:
docker compose exec airflow-scheduler bash -c '
  airflow dags unpause <dag_id> && airflow dags trigger <dag_id> -r smoke-1'
```

The integrity tests enforce, per Dag: no import errors, no cycles, tags +
`doc_md` + an explicit non-`airflow` owner, `retries >= 1` (except the three demo
Dags), the `mlops`↔`labeled_projects` asset wiring, `validate_training_data`
upstream of `train_candidate`, and the stale-construct grep.

## Open

- **The rewrite is uncommitted on the branch.** Commit it, or merge to `main`,
  once the reader has read it.
- **Length.** README is ~2340 lines, ~1000 of them the generated appendix. If it
  needs to shrink, move §§9–13 (the pipeline walkthrough) to `docs/`; do not
  delete the appendix, it is the only complete code in the docs.
- No CI. One `docker compose run pytest` job would carry all 30 tests.
- The tutorial has never been read end-to-end by a beginner. The checkpoints are
  the part most likely to be wrong — they were written from real output, but a
  reader following in order is the real test.

## Conventions

- Prose in files is normal English. The chat mode used in these sessions
  ("caveman") does **not** apply to anything written to disk.
- Dag comments explain the thing at the point it matters, for a learner.
- `src/tagifai/` never imports Airflow.
- Temporary files go in the session scratchpad, not the repo.
- `env.example` is the template; `.env.example` is blocked by a permission deny
  rule — do not recreate it.
