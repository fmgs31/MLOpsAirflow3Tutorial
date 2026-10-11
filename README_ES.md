# Aprende Apache Airflow 3 construyendo un pipeline de ML

Un tutorial práctico para **Airflow 3.3.1**. Vas a escribir siete Dags, un
archivo cada vez, que crecen hasta formar un sistema que funciona: se extraen
datos, se validan y se transforman, eso publica un **Asset**, el Asset dispara
el entrenamiento del modelo, el mejor modelo se promociona a producción, y un
Dag de monitorización decide si hay que reentrenar.

Todo corre en tu máquina con Docker. Sin cuenta en la nube, sin warehouse, sin
registrarte en ningún sitio. Cada comando de aquí abajo se ejecutó contra un
stack real de Airflow 3.3.1 — ver [Qué se verificó de verdad](docs/verified_ES.md).

**Se asume:** Python, la línea de comandos y Docker instalado. No hace falta
experiencia con Airflow — esto enseña Airflow 3 tal y como es hoy, y nada sobre
versiones anteriores.

**Tiempo:** ~30 minutos hasta tu primer Dag run. Un día para los siete si los
escribes tú, que es la idea.

---

## Contenidos

1. [Qué vas a construir](docs/split_ES/01-que-vas-a-construir.md)
2. [Ponlo en marcha](docs/split_ES/02-ponlo-en-marcha.md)
3. [Las piezas de Airflow](docs/split_ES/03-las-piezas-de-airflow.md)
4. [Tu primer Dag](docs/split_ES/04-tu-primer-dag.md)
5. [La UI](docs/split_ES/05-la-ui.md)
6. [Operators, XComs y relaciones](docs/split_ES/06-operators-xcoms-y-relaciones.md)
7. [Scheduling y logical dates](docs/split_ES/07-scheduling-y-logical-dates.md)
8. [Assets: programar por datos, no por tiempo](docs/split_ES/08-assets-programar-por-datos-no-por-tiempo.md)
9. [El Dag de DataOps](docs/split_ES/09-el-dag-de-dataops.md)
10. [El Dag de MLOps](docs/split_ES/10-el-dag-de-mlops.md)
11. [Esperar: sensors, deferrable tasks, eventos](docs/split_ES/11-esperar-sensors-deferrable-tasks-eventos.md)
12. [Continual learning: monitorizar, decidir, actuar](docs/split_ES/12-continual-learning-monitorizar-decidir-actuar.md)
13. [Deadline alerts](docs/split_ES/13-deadline-alerts.md)
14. [Testear Dags](docs/split_ES/14-testear-dags.md)
15. [Llevarlo a producción](docs/split_ES/15-llevarlo-a-produccion.md)
16. [Apéndice y procedencia](docs/split_ES/16-apendice-y-procedencia.md)

Material de referencia: [CLI cheatsheet](docs/cheatsheet.md) ·
[Troubleshooting](docs/troubleshooting.md) ·
[Los Dags terminados, completos](docs/appendix-dags_ES.md) ·
[Qué se verificó de verdad](docs/verified_ES.md)

---

## Estructura del repositorio

```
dags/                       your workspace - empty until you write the seven Dags
solutions/                  the finished version of each, numbered in reading order
src/project_tagger/         the ML project Airflow orchestrates (imports no Airflow)
  config.py                 paths and thresholds
  data.py                   extract / clean / transform / split
  validate.py               a tiny expectation-suite runner
  train.py  evaluate.py     TF-IDF + SGD classifier, metrics and slices
  registry.py               filesystem stand-in for a model registry
  monitoring.py             drift and live-performance checks
  predict.py                inference against the promoted model
  assets.py                 the Airflow Assets the Dags share
  callbacks.py              deadline-alert callbacks
tests/                      Dag integrity (your dags/), unit tests, README checks
docs/                       cheatsheet, troubleshooting, the Dag appendix (§16)
scripts/                    up / reset / native setup / README sync
docker-compose.yaml         LocalExecutor + Postgres + Simple auth
```

## Para seguir leyendo

- [Documentación de Airflow 3](https://airflow.apache.org/docs/apache-airflow/stable/)
- [Referencia del Task SDK](https://airflow.apache.org/docs/task-sdk/stable/api.html)
- [Assets y scheduling data-aware](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html)
- [Índice de paquetes de providers](https://airflow.apache.org/docs/apache-airflow-providers/)
- El pipeline de ML que orquesta este tutorial sigue la [lección de orquestación
  de Made With ML](https://madewithml.com/courses/mlops/orchestration/) — útil
  por el razonamiento de MLOps; su código de Airflow es de una versión anterior.
