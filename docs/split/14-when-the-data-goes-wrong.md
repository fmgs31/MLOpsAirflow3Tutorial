## 14. When the data goes wrong

Every run so far has passed, because every run has read the same clean files.
Real data arrives in pieces, late, re-sent and reformatted, and the job
becomes answering the pipeline when a check fails. This section loads a
second batch of projects over three deliveries, each with a problem real loads
have. For the first two it shows where the problem appears, how to find the
rows behind it, where the fix belongs and how much to re-run; the third is
yours. The same batch then sets off the drift monitor of §12.

| Delivery | What arrives | What is wrong | Where it shows up |
|---|---|---|---|
| Day 1 | 191 new projects | their tags have not arrived | `transform`: a dbt test fails |
| Day 2 | the tags, and the projects again | four old projects re-sent under new ids | `validate_projects`: a GX suite fails |
| Day 3 | the projects, corrected | for you to find | for you to find |
| — | (the same data) | the tags use a different vocabulary from 11 September 2026 on | `monitoring`: drift |

### The batch

The files are in [`datasets/batch_2/`](../../datasets/batch_2/). The 191
projects are real: Made With ML's holdout set, projects from the same site
that are not in `datasets/projects.csv`. Their dates are moved to the five
weeks after the newest arXiv paper of §12, making them the newest rows in the
warehouse, and the first three problems in the table are added on top. The
fourth is not added: Made With ML published these tags with the three rarest
already merged into one.

Each sync in §9 is **Full Refresh Overwrite**, so each file is the whole table
as the source would export it that day: the 1564 rows it already sent, then
the new ones. A delivery is a change of URL in an Airbyte source: open
**Sources**, choose the source, replace its **URL**, and save.

### Before you start

Pause `mlops`:

```bash
docker compose exec airflow-scheduler airflow dags pause mlops
```

A passing `dataops` run emits the asset event that starts `mlops` (§8), which
here would train a model on the new projects before `monitoring` compares
them with what the production model knows. That comparison is "The drift
alarm". `dataops` stays un-paused, as §10 left it.

### Day 1: projects without tags

Change the `projects` source's URL to:

```
https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/batch_2/day1_projects.csv
```

Leave `tags` as it is, and start a run:

```bash
docker compose exec airflow-scheduler airflow dags trigger dataops
```

**Checkpoint.** In the UI, after a few minutes, the run fails at `transform`:
both syncs and both `validate_*` tasks are green, and `validate_transformed`
and `quality_report.summarize` are `upstream_failed`.

**Read the failure.** Open `transform`'s log. Near the end:

```
FAIL 191 not_null_labeled_projects_tag
...
Got 191 results, configured to fail if != 0
Done. PASS=5 WARN=0 ERROR=1 SKIP=0 NO-OP=0 REUSED=0 TOTAL=6
```

A dbt test is a query that returns the rows breaking the rule; it fails when
any come back. dbt writes each test, compiled, to
`dbt/dbt_transforms/target/compiled/dbt_transforms/models/labeled_projects/schema.yml/not_null_labeled_projects_tag.sql`;
this one selects `tag` where it is null. To see *which* projects, ask for
more columns in the BigQuery console (§9):

```sql
SELECT id, created_on, title
FROM `<project id>.mlops_course.labeled_projects`
WHERE tag IS NULL
ORDER BY created_on
```

191 rows, all dated from 11 September 2026 on: the new batch. And `tags` did
not grow:

```sql
SELECT COUNT(*) AS n FROM `<project id>.mlops_course.tags`
```

`n` is 1564. The projects arrived before their labels: labelling follows
submission, and it is behind. Nothing is broken.

**Where the fix belongs: nowhere, yet.**

Wrong fixes:

- **Re-running.** It gives the same failure on the same data.
- **Relaxing the test.** It would let unlabelled projects reach training,
  where `preprocess` silently drops them.

Wait for the labels.

Check that re-running does not help, with a tool you have not used yet:
re-running a run from any of its tasks. Open the run in the UI, click
`transform`, and click the circular-arrow button at the top right of the task
panel, left of **✓ / ✗** (or press **Shift+C**). The **Clear Task Instance**
dialog opens. **Downstream** is preselected, so the tasks after it are cleared
too. Choose **Confirm**: `transform` runs again in the same run, and fails the
same way, after its retries.

The failure shows two more things. First, `labeled_projects` already has the
191 untagged rows: `dbt build` writes the table, then tests it. What kept
them from training is the next gate: `validate_transformed` did not run, so
no asset event was emitted. That gate protects only the Dags scheduled on the
asset: they run when the event is emitted, and none was. But the table already
holds the untagged rows, and any Dag that reads it without waiting for the
event gets them. There are two such cases: `monitoring` (§12), which runs on
its cron and reads the table anyway, and `mlops` triggered by hand, because a
manual trigger does not wait for the asset. Then the only thing that stops it
is its own check, `validate_training_data` (§10).
§16 shows the pattern that keeps a table that fails its tests from being
published at all. Second, the wait: each time, `transform` ran three times, a
minute apart, because `dataops` gives every task `retries=2`. Retries help
when the cause goes away by itself (a network error, a busy API); a data
problem fails the same way every time. That is why the GX gates raise
`AirflowFailException`, which skips the retries (§9). `transform`, a
`BashOperator` running dbt, cannot tell a failed test from a lost
connection, so it retries both.

### Day 2: the same projects twice

The labels arrive. Change `projects` to:

```
https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/batch_2/day2_projects.csv
```

and `tags` to:

```
https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/batch_2/day2_tags.csv
```

A new delivery gets a new run:

```bash
docker compose exec airflow-scheduler airflow dags trigger dataops
```

**Checkpoint.** This time the run fails within a minute at
`validate_projects`, with no retry. Both syncs are green, and so is
`validate_tags`, though this time it checks new tags; `transform`,
`validate_transformed` and `quality_report.summarize` are `upstream_failed`.

**Read the failure.** `validate_projects`'s log ends with the failed
expectation and some of the rows that broke it:

```
projects failed validation: [{'expectation': 'expect_compound_columns_to_be_unique:title,description', ...
'details': "[{'title': '…', 'description': '…'}, ...
```

Two projects with the same title and description. Find all of them:

```sql
SELECT title, ARRAY_AGG(id ORDER BY id) AS ids, ARRAY_AGG(created_on ORDER BY id) AS created
FROM `<project id>.mlops_course.projects`
GROUP BY title, description
HAVING COUNT(*) > 1
```

Four rows, each a 2020 project paired with a copy under a new id, 3800 to
3803, all dated `2026-10-16 09:00:00`. The export re-sent four old projects
as new.

**Where the fix belongs: upstream.** The bug is in the source, and only its
owner can say which id is real.

Wrong fix: deleting the copies in BigQuery. The next sync overwrites the table
with the export, copies included.

Report it, with the query. Until a corrected export arrives, the gate holds
the pipeline.

`validate_tags`'s log has one more line, a warning, not a failure:
`expect_column_values_to_be_in_set:tag` with `'misc'` in its details. The new
tags include a value the source never used before. The expectation is
non-critical (§9), so the run went on. "The drift alarm" comes back to it.

### Day 3: the corrected export

Days 1 and 2 answered each question for you. Day 3 is yours: below are the
delivery and the goal; the diagnosis and the fix are up to you.

The corrected export arrives. Change the `projects` source's URL to:

```
https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/batch_2/day3_projects.csv
```

```bash
docker compose exec airflow-scheduler airflow dags trigger dataops
```

**Your task.** The run fails. The source's owner confirms this export is
right: it is how the source sends its data from now on. Get this run to
`success`, and leave a check behind so that next time something like this is
a dbt `FAIL` with a count of rows. Re-run it by clearing a task, as on day 1;
which task to clear is part of the exercise.

**Done when:**

- the run is `success`, and `validate_transformed` passes with the same
  warning about `misc` as `validate_tags`;
- `transform`'s log ends with
  `Done. PASS=7 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=7`, one test more
  than on day 1;
- `labeled_projects` has 1755 rows.

Open the hints one at a time, only when you are stuck.

<details>
<summary>Hint 1: what kind of failure is it?</summary>

Compare the last line of `transform`'s log with day 1's,
`PASS=5 WARN=0 ERROR=1 SKIP=0`. What does a skip mean, and why would five
things be skipped? Then find the line that names a value.

</details>

<details>
<summary>Hint 2: which rows?</summary>

The model reads the raw `projects` table (§9). BigQuery's `SAFE_CAST` returns
`NULL` where `CAST` would stop the query: use it to find the rows `CAST`
cannot read.

</details>

<details>
<summary>Hint 3: where does the fix go?</summary>

Day 2 showed why changing a raw table does not last. The fix goes in
`dbt/dbt_transforms/models/labeled_projects/labeled_projects.sql`. BigQuery's
`SAFE.PARSE_TIMESTAMP` reads a timestamp written in a format you give it, and
`COALESCE` returns the first of its arguments that is not `NULL`.

</details>

<details>
<summary>Hint 4: which check?</summary>

After your fix, what does a date written in a third format become in
`labeled_projects`? Which test, added to
`dbt/dbt_transforms/models/labeled_projects/schema.yml` (§9), would catch it?

</details>

<details>
<summary>Hint 5: which task to clear?</summary>

The raw tables are right. Which is the first task whose output was wrong?

</details>

<details>
<summary>Solution</summary>

**Read the failure.** Both `validate_*` tasks pass, with the warning about
`misc`, and `transform` again fails after its retries, with a log different
from day 1's:

```
ERROR creating sql table model mlops_course.labeled_projects
...
Invalid timestamp: '09/11/2026 17:19:07'
Done. PASS=0 WARN=0 ERROR=1 SKIP=5 NO-OP=0 REUSED=0 TOTAL=6
```

An `ERROR` is not a `FAIL`: a FAIL is a test that ran and found rows; an
ERROR is SQL that could not run. Here `CAST(p.created_on AS TIMESTAMP)` met a
value it cannot read, so the model was not built and its five tests were
skipped.

```sql
SELECT created_on
FROM `<project id>.mlops_course.projects`
WHERE SAFE_CAST(created_on AS TIMESTAMP) IS NULL
```

191 rows, all written `MM/DD/YYYY HH:MM:SS`: the new projects. The export now
writes dates differently, and no check saw it: the `projects` suite checks
the columns, the ids and the titles, not how a date is written.

**Where the fix belongs: the transform.** The raw table holds what the source
sends, and every row has a valid date, written one of two ways. Reading both
is the transform's job. Replace
`dbt/dbt_transforms/models/labeled_projects/labeled_projects.sql` with:

```sql
SELECT
    p.id,
    COALESCE(
        SAFE_CAST(p.created_on AS TIMESTAMP),
        SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', p.created_on)
    ) AS created_on,
    p.title,
    p.description,
    t.tag
FROM {{ source('mlops_course', 'projects') }} AS p
LEFT JOIN {{ source('mlops_course', 'tags') }} AS t
    ON p.id = t.id
```

`SAFE_CAST` returns `NULL` where `CAST` would stop the query, and `COALESCE`
takes the first non-`NULL` value: the old format, then the new one. A third
format would silently give `NULL`. So add the check this incident was
missing, in `dbt/dbt_transforms/models/labeled_projects/schema.yml`, before
`title`:

```yaml
      - name: created_on
        data_tests:
          - not_null
```

A problem no check caught should leave a check behind: next time it is a FAIL
with a count of rows, not an error in the middle of the SQL.

**Re-run only what changed.** The raw tables are right, so nothing needs
extracting again: re-run `transform` and what follows it. This is what ELT
buys (§9). Clear `transform` in this failed run; `validate_transformed` and
`quality_report.summarize` are cleared with it. `transform` runs again in the
same run, and `TOTAL=7` in its log is now one model and six tests.

</details>

The three days asked three questions: should anything be fixed (day 1, no),
whose fix is it (day 2, the source's), and where does it go (day 3, the
transform). How much to re-run follows from the answer: a new delivery is a
new run; a fix on your side re-runs from the task it changed.

### The drift alarm

The data is in and checked. Now ask the monitor of §12 what it makes of it:

```bash
docker compose exec airflow-scheduler airflow dags test monitoring
```

Search the output as in §12; the `pendulum` traceback is the same harmless
check.

**Checkpoint.** `prepare_windows` logs `'num_current': 191`. `decide` logs
target drift at a p-value around `6e-41`, length drift around `5e-19`, and
`new_labels=['misc']`, and takes neither `continue_serving` nor the
retraining branch: `Branch into inspect`. Then:

```
MANUAL INSPECTION NEEDED
  new labels: ['misc']
  current distribution: {'natural-language-processing': 0.408..., 'computer-vision': 0.371..., 'misc': 0.136..., 'mlops': 0.083...}
  f1 baseline=… cumulative=…
  computer-vision: 71 rows, f1 …
  mlops: 16 rows, f1 …
  natural-language-processing: 78 rows, f1 …
  other: 26 rows, f1 …
  short_text: 11 rows, f1 …
  mentions_transformer: 28 rows, f1 …
```

and `inspect` fails with `manual inspection needed - nothing was retrained
or rolled back`. The run ends there, in seconds: it needs a person, and
nothing was retrained. That is the rule `decide` was written with in §12: a
label the model has never seen means the label space moved, and retraining on
data nobody understands yet is how a worse model ships. A drift alarm is a
question; the inspection is yours.

The current window is exactly the 191 new projects: none of their ids is in
the production model's `split_ids.json`. In it, `misc` is about 14%, though
the rows the model learned from never have it, and `reinforcement-learning`,
`graph-learning` and `time-series` do not appear at all. Look at when each of
those four tags was used:

```sql
SELECT tag, COUNT(*) AS n, DATE(MIN(created_on)) AS first, DATE(MAX(created_on)) AS last
FROM `<project id>.mlops_course.labeled_projects`
WHERE tag IN ('misc', 'reinforcement-learning', 'graph-learning', 'time-series')
GROUP BY tag
ORDER BY first
```

```
graph-learning          41  2020-02-24  2026-02-09
reinforcement-learning  73  2020-02-28  2026-09-10
time-series             58  2020-04-06  2026-08-11
misc                    26  2026-09-11  2026-10-13
```

The three tags stop where `misc` starts: from 11 September 2026 on, the
source writes them as `misc`. The model already merges them: any tag outside
its four classes (§10) becomes `other`, `misc` as well as the three old ones,
which is why the per-class lines above show `other`, not `misc`. The
`f1 baseline=… cumulative=…` line agrees: no drop, so the model does as well on
the new projects as on its test split.

Wrong fixes:

- **Retraining.** The model has nothing to fix: f1 does not drop and all four
  tags already end up as `other`. The data would still use two names for the
  same thing.
- **Accepting `misc` as a known tag and leaving the transform alone.** The
  monitor compares the tags in `labeled_projects`, not the model's classes, so
  `misc` would stay a new label on every run and `decide` would always stop at
  `inspect`. Even past that, target drift would measure the rename, not a
  change in the data.
- **Mapping `misc` rows back to the three old tags.** A new `misc` row could
  come from any of the three, and the source no longer says which: it would be
  guessing.

The fix is to give the old rows the new vocabulary, in the transform, so
history and new data share the same labels. In `labeled_projects.sql`, replace
the last column, `t.tag`, so that the model reads:

```sql
SELECT
    p.id,
    COALESCE(
        SAFE_CAST(p.created_on AS TIMESTAMP),
        SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', p.created_on)
    ) AS created_on,
    p.title,
    p.description,
    CASE
        WHEN t.tag IN ('reinforcement-learning', 'graph-learning', 'time-series') THEN 'misc'
        ELSE t.tag
    END AS tag
FROM {{ source('mlops_course', 'projects') }} AS p
LEFT JOIN {{ source('mlops_course', 'tags') }} AS t
    ON p.id = t.id
```

A missing tag is still `NULL` (a `CASE` with no match takes its `ELSE`), so
day 1's test still guards against it. Clear `transform` again, on the same
run, with **Downstream**, then ask the monitor again:

```bash
docker compose exec airflow-scheduler airflow dags test monitoring
```

**Checkpoint.** `new_labels=[]`, so `decide` no longer stops at `inspect`.
Both drift tests have a p-value below 0.05 and an effect size over
`min_effect_size`:

| Test | p-value | Effect size | Rows the model learned from | New rows |
|---|---|---|---|---|
| Target drift | around `0.006` | 0.10 | `mlops` 4% | `mlops` 8% |
| Data drift | around `5e-19` | 0.35 | median text of 22 words | median text of 17 words |

`decide` takes `trigger_retraining`, as in §12, and queues a run of `mlops`
that waits while `mlops` is paused.

Both tests still report drift, and both are right. These 191 are Made With ML
projects, like the 764 of §9, but the model learned from those plus the
arXiv papers of §12: against that mix, the new projects are shorter and have
twice the share of `mlops`. Yet the f1 did not drop. Drift is a difference, not damage: a drift test says
the data moved; only the performance check says whether that hurt. The §12
policy retrains on either, at the cost of one training run, and the gate in
`mlops` decides whether the result ships; a policy that retrained only on an
f1 drop would take `continue_serving` here.

One loose end: every `dataops` run still warns about `misc`, in
`validate_tags` and `validate_transformed`. A warning that is always there
stops being read, and the next one gets missed. The source now emits `misc`
on purpose, so add it to the tags it is known to emit, `KNOWN_RAW_TAGS` in
[`src/project_tagger/config.py`](../../src/project_tagger/config.py):

```python
KNOWN_RAW_TAGS = [
    "natural-language-processing",
    "computer-vision",
    "mlops",
    "reinforcement-learning",
    "graph-learning",
    "time-series",
    "misc",
]
```

The tasks import it at run time, so the next run reads the new list. Clear
`validate_tags` on the same run, with **Downstream**.

**Checkpoint.** `validate_tags`, `transform` and `validate_transformed` pass,
and neither validation log has the warning any more.

### Back to normal

```bash
docker compose exec airflow-scheduler airflow dags unpause mlops
```

The queued `mlops` run starts and trains on all 1755 projects. Its gate may
well keep the current model: the f1 did not drop, so there is little to win
on the 49 new test rows, and `below the champion's … on the 49 new test rows
- not promoting` is a normal outcome; if the gate lets it through,
`approve_promotion` waits for you, as in §10. The new projects are in,
checked and labelled like the old ones, and every model from now on trains
on them. The sources keep their day 2 and day 3 URLs: that is your data now.

---

← [13. Deadline alerts](13-deadline-alerts.md) · [Index](README.md) · [15. Testing Dags](15-testing-dags.md) →
