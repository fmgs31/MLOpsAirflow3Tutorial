## 17. Apéndice y procedencia

Las secciones anteriores construyen cada Dag por partes.
**[Los Dags terminados, completos](../appendix-dags_ES.md)** tiene cada
solución de principio a fin, en orden de lectura, inyectada desde
[`solutions/`](../../solutions/) por
[`scripts/sync_readme.py`](../../scripts/sync_readme.py) y verificada por
[`tests/test_readme_listings.py`](../../tests/test_readme_listings.py), de modo
que los listados no pueden quedar desfasados respecto al código que se ejecuta.
[`tests/test_readme_build_steps.py`](../../tests/test_readme_build_steps.py)
comprueba que los bloques de "Constrúyelo", concatenados en orden, dan Dags que
se importan sin errores.

La última ejecución completa de la §1 a la §12, con cada comando tal como está
escrito, fue el 2026-11-05, contra `apache/airflow:3.3.1`.

---

← [16. Llevarlo a producción](16-llevarlo-a-produccion.md) · [Índice](README.md)
