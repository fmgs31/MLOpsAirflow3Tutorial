## 14. When the data goes wrong

Every run so far has passed, because every run has read the same clean files.
Real data arrives in pieces, late, re-sent and reformatted, and then the job
is no longer writing the pipeline but answering it when a check fails. This
section loads a second batch of projects over three deliveries, each with a
problem real loads have, and works through each one: where it shows up, how
to find the rows behind it, where the fix belongs, and how much to re-run.
Then the same batch sets off the drift monitor of §12.

| Delivery | What arrives | What is wrong | Where it shows up |
|---|---|---|---|
| Day 1 | 191 new projects | their tags have not arrived | `transform`: a dbt test fails |
| Day 2 | the tags, and the projects again | four old projects re-sent under new ids | `validate_projects`: a GX suite fails |
| Day 3 | the projects, corrected | the new projects' dates are in another format | `transform`: the model cannot be built, and no check saw it coming |
| — | (the same data) | the tags use a different vocabulary from 2021 on | `monitoring`: drift |

### The batch

The files are in [`datasets/batch_2/`](../../datasets/batch_2/). The 191
projects are real: Made With ML's holdout set, projects from the same site
that are not in `datasets/projects.csv`. Their dates are moved forward one
year, so they are the newest rows in the warehouse, and the first three
problems in the table are added on top. The fourth is not added: Made With ML
published these tags with the three rarest ones already merged into `other`.
[`scripts/make_batch_2.py`](../../scripts/make_batch_2.py) builds the files,
and [`datasets/README.md`](../../datasets/README.md) lists what each one
contains.

Each sync in §9 is **Full Refresh Overwrite**, so each file is the whole table
as the source would export it that day: the 764 original rows, then the new
ones. A delivery is a change of URL in an Airbyte source: in Airbyte, open
**Sources**, choose the source, replace its **URL**, and save.

### Before you start

Pause `mlops`:

```bash
docker compose exec airflow-scheduler airflow dags pause mlops
```

A `dataops` run that passes emits the asset event that starts `mlops` (§8).
Here that would train a model on the new projects before `monitoring` has
compared them with what the production model knows, and that comparison is
what "The drift alarm" is about. `dataops` stays un-paused, as §10 left it.

### Day 1: projects without tags

Change the `projects` source's URL to:

```
https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/batch_2/day1_projects.csv
```

Leave `tags` as it is, and start a run:

```bash
docker compose exec airflow-scheduler airflow dags trigger dataops
```

**Checkpoint.** In the UI, the run fails at `transform` after a few
minutes. Both syncs and both `validate_*` tasks are green, `transform` is red,
and `validate_transformed` is `upstream_failed`.

**Read the failure.** Open `transform`'s log. Near the end:

```
FAIL 191 not_null_labeled_projects_tag
...
Got 191 results, configured to fail if != 0
Done. PASS=5 WARN=0 ERROR=1 SKIP=0 NO-OP=0 REUSED=0 TOTAL=6
```

A dbt test is a query that returns the rows breaking the rule, and it fails
when any come back. dbt writes each one, compiled, to
`dbt/dbt_transforms/target/compiled/dbt_transforms/models/labeled_projects/schema.yml/not_null_labeled_projects_tag.sql`;
this one selects `tag` where it is null. To see *which* projects, ask for
more columns. In the BigQuery console (§9):

```sql
SELECT id, created_on, title
FROM `<project id>.mlops_course.labeled_projects`
WHERE tag IS NULL
ORDER BY created_on
```

191 rows, all dated 2021: the new batch. And `tags` did not grow:

```sql
SELECT COUNT(*) AS n FROM `<project id>.mlops_course.tags`
```

`n` is 764. The projects arrived and their labels did not: labelling comes
after the projects are submitted, and it is behind. Nothing is broken.

**Where the fix belongs: nowhere, yet.** Re-running gives the same failure,
because the data is the same. Relaxing the test would let unlabelled projects
reach training, where `preprocess` drops them without a word. Wait for the
labels.

Two things the failure shows. First, `labeled_projects` now has the 191
untagged rows: `dbt build` writes the table, then tests it. What kept them away
from training is the gate after it: `validate_transformed` did not run, so no
asset event was emitted. Second, the wait: `transform` ran three times, a
minute apart, because `dataops` gives every task `retries=2`. Retries help
when the cause goes away by itself, such as a network error or a busy API; a data
problem fails the same way every time. That is why the GX gates raise
`AirflowFailException`, which skips the retries (§9). `transform` is a
`BashOperator` running dbt, so it cannot tell a failed test from a lost
connection, and retries both.

### Day 2: the same projects twice

The labels arrive. Change both URLs, `projects` to:

```
https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/batch_2/day2_projects.csv
```

and `tags` to:

```
https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/batch_2/day2_tags.csv
```

This is a new delivery, so it gets a new run:

```bash
docker compose exec airflow-scheduler airflow dags trigger dataops
```

**Checkpoint.** This time the run fails within a minute, at
`validate_projects`, with no retry. `validate_tags` is green, and `transform`
and `validate_transformed` are `upstream_failed`.

**Read the failure.** `validate_projects`'s log ends with the failed
expectation and a few of the rows that broke it:

```
projects failed validation: [{'expectation': 'expect_compound_columns_to_be_unique:title,description', ...
'details': "[{'title': 'Coloring Greyscale Images', ...
```

Two projects with the same title and description. Find all of them:

```sql
SELECT title, ARRAY_AGG(id ORDER BY id) AS ids, ARRAY_AGG(created_on ORDER BY id) AS created
FROM `<project id>.mlops_course.projects`
GROUP BY title, description
HAVING COUNT(*) > 1
```

Four rows. Each pairs a 2020 project with a copy under a new id, 2438 to 2441,
all four dated `2021-10-06 09:00:00`. The export sent four old projects again
as if they were new.

**Where the fix belongs: upstream.** The source has the bug, and it is the
source's owner who can say which id is the real one. Report it, with the
query. Deleting the copies in BigQuery does not help: the next sync replaces
the whole table with the export again, copies included. Until a corrected
export arrives, the gate holds the pipeline.

One more line in `validate_tags`'s log, a warning, not a failure:
`expect_column_values_to_be_in_set:tag` with `'other'` in its details. The new
tags include a value the source never used before. The expectation is
non-critical (§9), so the run went on. "The drift alarm" comes back to it.

### Day 3: a new date format

The corrected export arrives. Change the `projects` source's URL to:

```
https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/batch_2/day3_projects.csv
```

```bash
docker compose exec airflow-scheduler airflow dags trigger dataops
```

**Checkpoint.** Both `validate_*` tasks pass, the warning about `other`
included, and `transform` fails after its retries again. Its log is
different:

```
ERROR creating sql table model mlops_course.labeled_projects
...
Invalid timestamp: '06/30/2021 03:58:46'
Done. PASS=0 WARN=0 ERROR=1 SKIP=5 NO-OP=0 REUSED=0 TOTAL=6
```

An `ERROR` is not a `FAIL`. A FAIL is a test that ran and found rows; an
ERROR is SQL that could not run. Here `CAST(p.created_on AS TIMESTAMP)` met a
value it cannot read, the model was not built, and its five tests were skipped.

```sql
SELECT created_on
FROM `<project id>.mlops_course.projects`
WHERE SAFE_CAST(created_on AS TIMESTAMP) IS NULL
```

191 rows, all written `MM/DD/YYYY HH:MM:SS`: the new projects. The export now
writes dates differently. No check saw it: the `projects` suite checks the
columns, the ids and the titles, and nothing about how a date is written.

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
takes the first value that is not `NULL`: the old format, then the new one. A
third format would give `NULL`, silently. So add the check this incident was
missing, in `dbt/dbt_transforms/models/labeled_projects/schema.yml`, before
`title`:

```yaml
      - name: created_on
        data_tests:
          - not_null
```

A problem no check caught should leave a check behind. Next time it is a
FAIL with a count of rows, not an error in the middle of the SQL.

**Re-run only what changed.** The raw tables are right, so there is nothing
to extract again: re-run `transform` and what comes after it. This is what
ELT buys (§9). In the UI, open this failed run, click `transform`, and choose
**Clear Task Instance**. **Downstream** is already selected, so
`validate_transformed` is cleared too. Choose **Confirm**.

**Checkpoint.** `transform` runs again in the same run and its log ends with
`Done. PASS=7 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=7`: one model and
six tests now. `validate_transformed` passes, with the same warning about
`other`, and the run is `success`. `labeled_projects` has 955 rows.

The three days asked three different questions: should anything be fixed
(day 1, no), whose fix is it (day 2, the source's), and where does it go
(day 3, the transform). Re-running follows from the answer: a new delivery is
a new run; a fix on your side re-runs from the task it changed.

### The drift alarm

The data is in and checked. Now ask what the monitor of §12 makes of it:

```bash
docker compose exec airflow-scheduler airflow dags test monitoring
```

**Checkpoint.** `decide` logs label drift at a p-value around `1e-17`,
length drift around `0.91`, and `new_labels=['other']`, and takes neither
`continue_serving` nor the retraining branch: `Branch into inspect`. Then:

```
MANUAL INSPECTION NEEDED
  new labels: ['other']
  current distribution: {'mlops': 0.092..., 'other': 0.108..., 'time-series': 0.016..., ...}
```

The run ends there, in seconds, with nothing retrained. That is the rule
`decide` was written with in §12: a label the model has never seen means the
label space moved, and retraining on data nobody understands yet is how a
worse model ships. A drift alarm is a question; the inspection is yours.

The current window is the newest quarter, the 191 new projects and 48 of the
newest old ones. In its distribution, `other` is about 11%, where it never
appeared before, and `reinforcement-learning`, `graph-learning` and
`time-series` fall to 1–2%. Look at those four tags by year:

```sql
SELECT EXTRACT(YEAR FROM created_on) AS year, tag, COUNT(*) AS n
FROM `<project id>.mlops_course.labeled_projects`
WHERE tag IN ('other', 'reinforcement-learning', 'graph-learning', 'time-series')
GROUP BY year, tag
ORDER BY year, tag
```

```
2020  graph-learning           33
2020  reinforcement-learning   45
2020  time-series              28
2021  other                    26
```

The projects did not change; the labels did. From 2021 on, the source writes
those three tags as `other`, which is the same merge the model already makes:
it learns four classes, and every other tag becomes `other` (§10). The
`f1 at promotion=… now=…` line, logged before `decide`, agrees: no drop, so
the model is as good on the new projects as on the old ones. Retraining would
fix nothing. The fix is to give the old rows the new vocabulary, in the
transform, so the history and the new data use the same labels. In
`labeled_projects.sql`, replace the last column, `t.tag`, so that the model
reads:

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
        WHEN t.tag IN ('reinforcement-learning', 'graph-learning', 'time-series') THEN 'other'
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

**Checkpoint.** Label drift around `0.46`, length drift around `0.91`,
`new_labels=[]`, and `decide` takes `continue_serving`. The run is over in
seconds.

One loose end: every run of `dataops` still warns about `other`, in
`validate_tags` and `validate_transformed`. A warning that is always there
stops being read, and the next one gets missed. The source now emits `other`
on purpose, so it belongs in the list of tags it is known to emit. Add it to
`KNOWN_RAW_TAGS` in [`src/project_tagger/config.py`](../../src/project_tagger/config.py):

```python
KNOWN_RAW_TAGS = [
    "natural-language-processing",
    "computer-vision",
    "mlops",
    "reinforcement-learning",
    "graph-learning",
    "time-series",
    "other",
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

The new projects are in, checked and labelled the same way as the old ones,
and the next model will train on them. The sources keep their day 2 and day 3
URLs: that is your data now.

---

← [13. Deadline alerts](13-deadline-alerts.md) · [Index](README.md) · [15. Testing Dags](15-testing-dags.md) →
