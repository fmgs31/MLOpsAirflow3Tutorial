# Orquestación de MLOps con Apache Airflow 3

Un tutorial práctico de MLOps: un sistema de machine learning construido con
las herramientas que usa un equipo real, y orquestado con **Airflow 3.3.1**.
Airbyte carga los datos en BigQuery, Great Expectations los comprueba, dbt los
transforma, Optuna busca los hiperparámetros del modelo, MLflow lo registra y decide qué está en
producción, y alibi-detect vigila el drift. Tú escribes los siete Dags que lo
ejecutan todo, un archivo cada vez: el pipeline de datos publica un **Asset**,
el Asset dispara el entrenamiento, el mejor modelo se promociona, y un Dag de
monitorización decide si hay que reentrenar.

Sigue la [lección de orquestación](https://madewithml.com/courses/mlops/orchestration/)
de Made With ML, reconstruida para Airflow 3 y preparada para ejecutarse, paso
a paso, en tu máquina. Airflow, MLflow y Airbyte corren en local con Docker; el
warehouse es BigQuery, en un proyecto de Google Cloud que creas en la §9. Los
comandos se comprueban contra un stack real — ver [Qué se verificó de
verdad](../verified_ES.md) para saber cuáles, y cuándo.

**Se asume:** Python, la línea de comandos, Docker instalado, y una cuenta de
Google para el Free Trial de Google Cloud (pide una tarjeta, y no cobra nada
salvo que pases a una cuenta de pago; la §9 lo explica). No hace falta experiencia
con Airflow — esto enseña Airflow 3 tal y como es hoy, y nada sobre versiones
anteriores.

**Tiempo:** ~30 minutos hasta tu primer Dag run. Más o menos una hora para
montar el stack de datos de la §9. Uno o dos días para los siete Dags si los
escribes tú, que es la idea.

---

## Contenidos

1. [Qué vas a construir](01-que-vas-a-construir.md)
2. [Ponlo en marcha](02-ponlo-en-marcha.md)
3. [Las piezas de Airflow](03-las-piezas-de-airflow.md)
4. [Tu primer Dag](04-tu-primer-dag.md)
5. [La UI](05-la-ui.md)
6. [Operators, XComs y relaciones](06-operators-xcoms-y-relaciones.md)
7. [Scheduling y logical dates](07-scheduling-y-logical-dates.md)
8. [Assets: programar por datos, no por tiempo](08-assets-programar-por-datos-no-por-tiempo.md)
9. [El Dag de DataOps](09-el-dag-de-dataops.md)
10. [El Dag de MLOps](10-el-dag-de-mlops.md)
11. [Esperar: sensors, deferrable tasks, eventos](11-esperar-sensors-deferrable-tasks-eventos.md)
12. [Continual learning: monitorizar, decidir, actuar](12-continual-learning-monitorizar-decidir-actuar.md)
13. [Deadline alerts](13-deadline-alerts.md)
14. [Testear Dags](14-testear-dags.md)
15. [Llevarlo a producción](15-llevarlo-a-produccion.md)
16. [Apéndice y procedencia](16-apendice-y-procedencia.md)

Material de referencia: [CLI cheatsheet](../cheatsheet.md) ·
[Troubleshooting](../troubleshooting.md) ·
[Los Dags terminados, completos](../appendix-dags_ES.md) ·
[Qué se verificó de verdad](../verified_ES.md)

---

## Estructura del repositorio

```
dags/                       your workspace - empty until you write the seven Dags
dbt/                        your dbt workspace - empty until you create the dbt project (§9)
solutions/                  the finished version of each Dag, and of the dbt project
src/project_tagger/         the ML project Airflow orchestrates (imports no Airflow)
  config.py                 names and settings: tables, MLflow, Optuna, thresholds
  data.py                   read the warehouse, snapshots, preprocessing, split
  validate.py               Great Expectations suites and the split checks
  train.py  evaluate.py     the classifier, the Optuna search, MLflow logging
  registry.py               MLflow model registry: register, promote, roll back
  monitoring.py  drift.py   drift windows, alibi-detect tests, live performance
  predict.py                inference against the promoted model
  assets.py                 the Airflow Assets the Dags share
  callbacks.py              deadline-alert callbacks
datasets/                   the two source CSVs Airbyte loads
secrets/                    where your GCP key goes (ignored by git)
tests/                      Dag integrity (your dags/), unit tests, README checks
docs/                       cheatsheet, troubleshooting, the Dag appendix (§16)
scripts/                    up / reset / native setup / README sync
docker-compose.yaml         Airflow (LocalExecutor) + Postgres + MLflow
```

## Para seguir leyendo

- [Documentación de Airflow 3](https://airflow.apache.org/docs/apache-airflow/stable/)
- [Referencia del Task SDK](https://airflow.apache.org/docs/task-sdk/stable/api.html)
- [Assets y scheduling data-aware](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html)
- [Índice de paquetes de providers](https://airflow.apache.org/docs/apache-airflow-providers/)
- [El curso de MLOps de Made With ML](https://madewithml.com/courses/mlops/),
  cuya lección de orquestación sigue este tutorial, y cuyas lecciones
  anteriores (el stack de datos, testing, experiment tracking, monitorización)
  profundizan en cada herramienta. Su código de Airflow es de una versión
  anterior.
- La documentación de cada herramienta: [Airbyte](https://docs.airbyte.com/),
  [BigQuery](https://cloud.google.com/bigquery/docs), [dbt](https://docs.getdbt.com/),
  [Great Expectations](https://docs.greatexpectations.io/),
  [Optuna](https://optuna.readthedocs.io/), [MLflow](https://mlflow.org/docs/latest/),
  [alibi-detect](https://docs.seldon.io/projects/alibi-detect/).
