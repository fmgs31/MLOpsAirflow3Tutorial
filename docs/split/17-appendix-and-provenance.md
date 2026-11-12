## 17. Appendix and provenance

The sections before this one build each Dag in pieces.
**[The finished Dags in full](../appendix-dags.md)** has every solution end to
end, in reading order, injected from [`solutions/`](../../solutions/) by
[`scripts/sync_readme.py`](../../scripts/sync_readme.py) and verified by
[`tests/test_readme_listings.py`](../../tests/test_readme_listings.py), so the
listings cannot drift from the code that runs.
[`tests/test_readme_build_steps.py`](../../tests/test_readme_build_steps.py)
checks that the "Build it" blocks, concatenated in order, produce Dags that
import cleanly.

§1 to §12 were last run end to end on 2026-11-05, every command as written,
against `apache/airflow:3.3.1`.

---

← [16. Taking it to production](16-taking-it-to-production.md) · [Index](README.md)
