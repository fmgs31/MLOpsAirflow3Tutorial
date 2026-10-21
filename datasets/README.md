# Source data

The tutorial's data combines Made With ML's original projects with papers
extracted from arXiv for this tutorial, in 2026:

| Delivery | Section | Rows | Source |
|---|---|---|---|
| `projects.csv`, `tags.csv` | §9 | 764 | Made With ML, as published |
| `batch_1/` | §12 | 800 more | arXiv papers, January 2025 – September 2026 |
| `batch_2/` | §14 | 191 more | Made With ML's holdout set, with problems added |

The arXiv papers are there so §12 has drift that matters. More Made With ML
projects change too little: when the 191 newest of the 764 were held back and
delivered in §12, the production model lost 0.05 of f1 on them, and the
retrained model beat it by 0.002, which is noise. The papers are real data
from another source, not altered: the drift tests fire on them, the
production model loses f1 on them, and a model retrained with them wins it
back. They are also newer, but date and source
changed together, so this data cannot say how much of the drift is down to
the date.

## `projects.csv` and `tags.csv`: what §9 loads

Both come from Made With ML
([GokuMohandas/Made-With-ML](https://github.com/GokuMohandas/Made-With-ML/tree/main/datasets),
MIT licence): 764 ML projects added to the site between February and October 2020.

| File | Columns | Notes |
|---|---|---|
| `projects.csv` | `id, created_on, title, description` | Byte-for-byte copy of upstream `datasets/projects.csv`. |
| `tags.csv` | `id, tag` | Upstream `datasets/tags.csv` has only `tag`, one row per project in the same order as `projects.csv`. The `id` column is added here so the two tables can be joined on a key, the way a warehouse keeps them. |

The rows line up exactly: joining the two on `id` reproduces upstream
`datasets/dataset.csv` once the three rarest tags (`reinforcement-learning`,
`graph-learning`, `time-series`) are collapsed into `other`.

## `batch_1/`: the delivery §12 loads

800 more projects, written from real arXiv papers in computer science
submitted between January 2025 and September 2026. They are listed in
[`arxiv.csv`](arxiv.csv) (`arxiv_id, created, categories, title, abstract,
tag`). arXiv's metadata is public domain
([CC0 1.0](https://info.arxiv.org/help/license/index.html)).
[`scripts/make_batch_1.py`](../scripts/make_batch_1.py) turns each paper into
a project. Each file is a whole table, as a Full Refresh sync expects: the
original rows byte for byte, then the new ones.

| File | Rows | What it contains |
|---|---|---|
| `batch_1/projects.csv` | 1564 | `projects.csv`, plus the 800 papers as projects: ids 3000–3799, `created_on` the date of the paper's first version (at `00:00:00`: arXiv gives no time), the title, and the first sentence of the abstract as the description |
| `batch_1/tags.csv` | 1564 | `tags.csv`, plus the papers' tags |

**How the papers were chosen.** The records were harvested from arXiv's OAI-PMH
interface (set `cs`, records dated the 10th to the 12th of each month),
keeping only papers first submitted in the month harvested. A paper was
tagged from its primary category (the first one listed) and its title:

| Tag | Rule |
|---|---|
| `natural-language-processing` | primary category `cs.CL` |
| `computer-vision` | primary category `cs.CV` |
| `reinforcement-learning`, `graph-learning`, `time-series` | primary category `cs.LG`, `stat.ML` or `cs.AI`, and a title that names exactly one of the three (`reinforcement learning`; `graph neural`, `GNN`, `graph learning`, …; `time series`) |

A paper whose title also names another tag's topic (a vision-language model
in `cs.CL`, a time-series paper in `cs.CV`) is ambiguous and was left out.
Papers on serving or deploying a model of one domain are tagged by that
domain, as Made With ML does: `mlops` is generic tooling, and no paper in the
sample qualified as that.

The 800 are a uniform random sample of the papers that qualified, so the tag
mix is the source's own: 460 `computer-vision`, 274
`natural-language-processing`, 30 `time-series`, 28 `reinforcement-learning`,
8 `graph-learning`. A person then reviewed the ones the rules could have got
wrong; 14 that fit two tags, or none, were replaced with other qualifying
papers.

The batch differs from `projects.csv` in two ways no one added: the tag mix
(more vision, no `mlops`), and the descriptions, which are first sentences of
abstracts, longer than the site's one-line blurbs. §12 measures both.

## `batch_2/`: the later deliveries §14 loads

191 more projects, from upstream `datasets/holdout.csv` (pinned to commit
`030573f`): real projects from Made With ML, none of them in `projects.csv`.
[`scripts/make_batch_2.py`](../scripts/make_batch_2.py) builds every file from
it. Each file is a whole table, as a Full Refresh sync expects: the rows of
`batch_1/` byte for byte, then the new ones.

| File | Rows | What it contains |
|---|---|---|
| `day1_projects.csv` | 1755 | `batch_1/projects.csv`, plus the 191 new projects, `created_on` moved to 11 September – 15 October 2026 |
| `day2_projects.csv` | 1759 | day 1, plus four old projects (ids 65, 847, 1668, 2268) re-sent under ids 3800–3803 |
| `day2_tags.csv` | 1755 | `batch_1/tags.csv`, plus the new projects' tags |
| `day3_projects.csv` | 1755 | day 1, with the new projects' `created_on` written `MM/DD/YYYY HH:MM:SS` |

The dates are moved so the batch is the newest data in the warehouse: the
holdout's March–October 2020 is mapped onto the five weeks after the newest
paper in `batch_1/`, in the same order and with the same relative spacing.
The new dates, the re-sent rows and the date format are made for §14. The tags
are not changed: upstream published the holdout with
`reinforcement-learning`, `graph-learning` and `time-series` already
collapsed into `other`, a value neither `tags.csv` nor `batch_1/` uses.
