## 1. Qué vas a construir

La orquestación convierte un montón de scripts que lanzas a mano en un sistema:
el trabajo se declara como un grafo de tasks, otra cosa decide cuándo ejecutarlo,
reintenta las partes que fallan y registra qué pasó.

Un workflow en Airflow es un **Dag** — un grafo dirigido acíclico. *Dirigido*:
las aristas tienen dirección. *Acíclico*: sin bucles, así que una task terminada
nunca se vuelve a ejecutar por algo que hay aguas abajo. Cada nodo es una
**task**, y una ejecución del grafo entero es un **Dag run**.

Merece la pena separar esas dos últimas palabras ya, porque cada línea de log y
cada mensaje de error dan por hecho que lo has hecho. Una **task** es lo que
escribiste una vez, en el archivo. Una **task instance** es esa task dentro de un
Dag run concreto — lo que de verdad tiene un estado, una duración, un log y un
número de intentos. Tú escribes tasks; tú depuras task instances. La §3 sigue una
a lo largo de toda su vida.

Tres Dags hacen el trabajo de verdad aquí, y lo interesante es cómo se conectan:

```
dataops:   extract ─► validate_raw ─► transform ─► validate_transformed ──┐
                                                                          │ emite
                                                            Asset: labeled_projects
                                                                          │ dispara
mlops:                  prepare ─► validate ─► train×N ─► select ─► gate ─┴─► promote
                                                                          │ emite
                                                            Asset: production_model

monitoring:  measure ─► decide ─► {continue | inspect | rollback | approve ─► retrain}
```

`mlops` **no tiene schedule**. Se ejecuta cuando `dataops` publica datos buenos.
Eso es scheduling data-aware, y es la idea sobre la que está construido todo este
tutorial.

Hay cuatro Dags más, cada uno para enseñar una cosa: `hello_airflow3` (un primer
Dag), `operators_and_xcoms` (tipos de task y cómo hablan entre ellas),
`sensors_and_events` (esperar al mundo exterior) y `deadline_alerts` (que te
avisen cuando un run va demasiado lento).

### Cómo funciona este tutorial

Los siete Dags los escribes tú. La carpeta que lee Airflow, `dags/`, está
**vacía** cuando clonas este repo. Cada sección explica una pieza de Airflow y
después te hace construir el Dag que la usa — unas pocas líneas cada vez, con el
porqué de cada trozo al lado — y luego ejecutarlo y comprobar qué ha pasado. El
código se da; el trabajo es teclearlo, verlo parsear y verlo correr.

Dos redes de seguridad:

- **[`solutions/`](../../solutions/)** tiene una copia terminada de cada archivo, con
  los comentarios didácticos dentro. Compara cuando algo no parsee, o sáltate
  una lección copiando su archivo: `cp solutions/03_dataops.py dags/`. El
  [apéndice](../appendix-dags_ES.md) imprime cada solución completa.
- **Los tests de la §14** leen *tu* carpeta `dags/`. Cuando los siete estén
  escritos, te dicen si construiste lo que el tutorial quería decir.

Las lecciones se apoyan unas en otras a partir de la §9 — `mlops` se programa con
un asset que produce `dataops`, `sensors_and_events` espera un archivo que
escribe `dataops` — así que hazlas en orden, o copia antes las soluciones
anteriores.

### Por qué Airflow

Airflow no es el único orquestador — Prefect, Dagster, Luigi, Metaflow, Flyte,
Kubeflow Pipelines y Vertex AI Pipelines cubren el mismo terreno. Airflow merece
la pena como primero porque:

- **Adopción.** Es el estándar en la mayoría de plataformas de datos, así que los
  conceptos se transfieren y todos los vendors traen integración.
- **Python puro, sin DSL.** Los workflows son funciones normales, testeables con
  pytest (§14).
- **El mismo código corre en tu portátil y en un clúster** — cambias el executor,
  no el Dag.
- **Apache governance**, un ecosistema grande de providers, y ningún vendor
  único.

### El proyecto de ML que orquesta

Airflow orquesta código; no lo contiene. El código orquestado vive en
`src/tagifai/` y no importa Airflow en absoluto — un pequeño clasificador de
texto que etiqueta proyectos de ML. Esta parte no la escribes tú; los Dags la
llaman:

| Módulo | Papel |
|---|---|
| `data.py` | extract, clean, transform, split train/test |
| `validate.py` | expectations de calidad de datos, lo que ejecutan los gates |
| `train.py`, `evaluate.py` | clasificador TF-IDF + SGD, métricas y slices |
| `registry.py` | sustituto en sistema de archivos de un model registry |
| `monitoring.py` | comprobaciones de drift y de rendimiento en vivo |
| `predict.py` | etiqueta texto nuevo con lo que esté promocionado ahora mismo |
| `assets.py` | los Assets de Airflow que comparten los Dags |
| `callbacks.py` | callbacks de deadline alerts |

Cada uno es un hueco que puedes cambiar por una herramienta real — las tablas del
§9 y de la §10 dicen cuál.

---

[Índice](README.md) · [2. Ponlo en marcha](02-ponlo-en-marcha.md) →
