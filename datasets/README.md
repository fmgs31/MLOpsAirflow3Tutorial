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
