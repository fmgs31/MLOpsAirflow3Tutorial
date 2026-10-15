# Source data

The two tables Airbyte loads into BigQuery in §9. Both come from Made With ML
([GokuMohandas/Made-With-ML](https://github.com/GokuMohandas/Made-With-ML/tree/main/datasets),
MIT licence): 764 ML projects added to the site between February and October 2020.

| File | Columns | Notes |
|---|---|---|
| `projects.csv` | `id, created_on, title, description` | Byte-for-byte copy of upstream `datasets/projects.csv`. |
| `tags.csv` | `id, tag` | Upstream `datasets/tags.csv` has only `tag`, one row per project in the same order as `projects.csv`. The `id` column is added here so the two tables can be joined on a key, the way a warehouse keeps them. |

The rows line up exactly: joining the two on `id` reproduces upstream
`datasets/dataset.csv` once the three rarest tags (`reinforcement-learning`,
`graph-learning`, `time-series`) are collapsed into `other`.

## `batch_2/`: the later deliveries §14 loads

191 more projects, from upstream `datasets/holdout.csv` (pinned to commit
`030573f`): real projects from the same site, none of them in `projects.csv`.
[`scripts/make_batch_2.py`](../scripts/make_batch_2.py) builds every file from
it. Each file is a whole table, as a Full Refresh sync expects: the original
rows byte for byte, then the new ones.

| File | Rows | What it contains |
|---|---|---|
| `day1_projects.csv` | 955 | the 191 new projects, `created_on` moved forward one year (2020 → 2021) |
| `day2_projects.csv` | 959 | day 1, plus four old projects (ids 65, 847, 1668, 2268) re-sent under ids 2438–2441 |
| `day2_tags.csv` | 955 | `tags.csv`, plus the new projects' tags |
| `day3_projects.csv` | 955 | day 1, with the new projects' `created_on` written `MM/DD/YYYY HH:MM:SS` |

The date shift, the re-sent rows and the date format are added for §14. The
tags are not changed: upstream published the holdout with
`reinforcement-learning`, `graph-learning` and `time-series` already
collapsed into `other`, a value `tags.csv` never uses.
