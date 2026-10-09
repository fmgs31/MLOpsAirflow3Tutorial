## 16. Appendix and provenance

The sections before this one build each Dag in pieces. Two longer documents
live outside the sections, so the tutorial stays readable and the reference material stays
findable:

- **[The finished Dags in full](../appendix-dags.md)** — every solution end to
  end, in reading order, injected from [`solutions/`](../../solutions/) by
  [`scripts/sync_readme.py`](../../scripts/sync_readme.py) and verified by
  [`tests/test_readme_listings.py`](../../tests/test_readme_listings.py), so the
  listings cannot drift from the code that actually runs.
- **[What was actually verified](../verified.md)** — the record of which
  claims in this tutorial were read off a live `apache/airflow:3.3.1` stack,
  when, and how each figure was measured. Nothing there is a step to run.

---

← [15. Taking it to production](15-taking-it-to-production.md) · [Index](README.md)
