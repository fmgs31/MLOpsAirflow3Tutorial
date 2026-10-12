## 1. Qué vas a construir

La orquestación convierte un montón de scripts que lanzas a mano en un sistema:
el trabajo se declara como un grafo de tasks, un componente aparte decide cuándo ejecutarlo,
reintenta las partes que fallan y registra qué pasó.

Un workflow en Airflow es un **Dag** — un grafo dirigido acíclico. *Dirigido*:
las aristas tienen dirección. *Acíclico*: sin bucles, así que ningún camino de
aristas vuelve a una task por la que ya pasó. Cada nodo es una
**task**, y una ejecución del grafo entero es un **Dag run**.

Merece la pena distinguir ya dos palabras, *task* y *task instance*, porque cada
línea de log y cada mensaje de error dan por hecho que las distingues. Una **task** es lo que
escribiste una vez, en el archivo. Una **task instance** es esa task dentro de un
Dag run concreto — lo que de verdad tiene un estado, una duración, un log y un
número de intentos. Tú escribes tasks; tú depuras task instances. La §3 sigue una
a lo largo de toda su vida.

Este es un tutorial de MLOps: construyes un pequeño sistema de machine
learning, desde los datos en bruto hasta un modelo en producción que se
reentrena cuando hace falta, y Airflow lo ejecuta. Cada paso lo hace la herramienta que usaría un equipo real;
Airflow decide cuándo se ejecuta cada una:

| Herramienta | Su trabajo aquí | Sección |
|---|---|---|
| **Airbyte** | copia los datos de origen al warehouse, la base de datos donde se guardan los datos para analizarlos | §9 |
| **BigQuery** | el warehouse, en Google Cloud | §9 |
| **dbt** | convierte las tablas en bruto en la tabla de entrenamiento, en SQL | §9 |
| **Great Expectations** (GX) | comprueba los datos antes de que los use cada paso | §9, §10 |
| **Optuna** | busca los mejores hiperparámetros: los ajustes del modelo que se fijan antes de entrenar | §10 |
| **MLflow** | registra cada entrenamiento, y lleva el model registry: las versiones numeradas del modelo entrenado, y cuál está en producción | §10, §12 |
| **alibi-detect** | detecta drift: datos nuevos que ya no se parecen a aquellos con los que aprendió el modelo | §12 |

Todas salvo BigQuery son open source y corren en tu máquina. BigQuery necesita
una cuenta de Google Cloud con facturación activada; los datos de este
tutorial quedan muy por dentro del free tier (la §9 lo explica).

Tres Dags hacen el trabajo de verdad, y lo interesante es cómo se conectan:

```
dataops:  extract & load ─► validate ─► transform ─► validate_transformed ─┐
          (Airbyte)          (GX)       (dbt)         (GX)                  │ emite
                                                              Asset: labeled_projects
                                                                            │ dispara
mlops:    prepare ─► validate ─► tune×N ─► train ─► evaluate ─► gate ───────┴─► promote
                     (GX)        (Optuna)  (MLflow)                             (registry de MLflow)
                                                                            │ emite
                                                              Asset: production_model

monitoring:  measure ─► decide ─► {continue | inspect | rollback | approve ─► retrain}
             (alibi-detect)
```

Un **asset** es un conjunto de datos con nombre: `labeled_projects` son los datos
etiquetados que produce `dataops`, y `production_model` es el modelo que
promueve `mlops`. Cada vez que la task que produce un asset tiene éxito, Airflow
registra un evento, y cualquier Dag que tenga ese asset como schedule arranca un
run (§8). Por eso `mlops` **no tiene un schedule de tiempo**: su schedule es un
asset, y se ejecuta cuando `dataops` publica datos buenos.
Eso es scheduling data-aware, y es lo que une los tres Dags.

Hay cuatro Dags más, cada uno para enseñar una cosa: `hello_airflow3` (un primer
Dag), `operators_and_xcoms` (tipos de task y cómo hablan entre ellas),
`sensors_and_events` (esperar al mundo exterior) y `deadline_alerts` (que te
avisen cuando un run va demasiado lento).

### Cómo funciona este tutorial

Los siete Dags los escribes tú, y en la §9 también el proyecto de dbt. La
carpeta que lee Airflow, `dags/`, está **vacía** cuando clonas este repo, y
`dbt/` también. Cada sección explica una pieza de Airflow y
después te hace construir el Dag que la usa — unas pocas líneas cada vez, con el
porqué de cada fragmento al lado — y luego ejecutarlo y comprobar qué ha pasado.
El código te lo damos; tu trabajo es teclearlo, comprobar que Airflow lo parsea
y verlo ejecutarse.

Dos redes de seguridad:

- **[`solutions/`](../../solutions/)** tiene una copia terminada de cada archivo, con
  los comentarios didácticos dentro. Compara cuando algo no se parsee, o sáltate
  una lección copiando su archivo: `cp solutions/03_dataops.py dags/`. El
  [apéndice](../appendix-dags_ES.md) imprime cada solución completa.
- **Los tests de la §14** leen *tu* carpeta `dags/`. Cuando los siete estén
  escritos, te dicen si construiste lo que el tutorial pretendía.

Las lecciones se apoyan unas en otras a partir de la §9 — `mlops` se programa con
un asset que produce `dataops`, `sensors_and_events` espera una tabla que
construye `dataops` — así que hazlas en orden, o copia antes las soluciones
anteriores. La §9 monta además el stack de datos del que depende el resto: un
proyecto de Google Cloud, Airbyte, y las conexiones entre ellos.

### Por qué Airflow

Airflow no es el único orquestador — Prefect, Dagster, Luigi, Metaflow, Flyte,
Kubeflow Pipelines y Vertex AI Pipelines cubren el mismo terreno. Airflow merece
la pena como primer orquestador porque:

- **Adopción.** Es el estándar en la mayoría de plataformas de datos, así que lo
  que aprendas te sirve en otros sitios y todos los vendors ofrecen integración.
- **Apache governance**, un ecosistema grande de providers, y ningún vendor
  único.

Y tiene lo que se espera de un orquestador moderno, como Prefect o Dagster:
workflows escritos en Python normal, testeables con pytest (§14), y el mismo
código ejecutándose en tu portátil y en un clúster — cambias el executor, no el
Dag.

### El proyecto de ML que orquesta

Airflow orquesta código; no lo contiene. El código que orquesta este tutorial
es un pequeño proyecto de ML en `src/project_tagger/`. No lo escribes tú; los
Dags lo llaman.

**Los datos** son 764 proyectos de ML que recopiló Made With ML entre febrero y
octubre de 2020, en dos tablas, como los guardaría un sistema de origen
([`datasets/`](../../datasets/)): `projects.csv`, con el título, la descripción
y la fecha de alta de cada proyecto, y `tags.csv`, con el **tag** de cada
proyecto — la etiqueta que el modelo aprende a predecir. Unidas por `id`, una
fila:

| title | description | tag |
|---|---|---|
| Rethinking Batch Normalization in Transformers | We found that NLP batch statistics exhibit large variance throughout training, which leads to poor BN performance. | `natural-language-processing` |

Hay seis tags. Los tres más raros (`reinforcement-learning`,
`graph-learning`, `time-series`) tienen muy pocos proyectos para aprender de
ellos, así que el modelo los ve como una sola clase, `other`, junto a
`natural-language-processing`, `computer-vision` y `mlops`.

**El modelo** lee el título y la descripción de un proyecto y predice su tag.
Es un clasificador de texto de scikit-learn (features TF-IDF y un clasificador
lineal SGD) que entrena en más o menos un segundo, así que una búsqueda sobre
decenas de combinaciones de hiperparámetros cabe en un Dag run. `dataops` (§9)
carga, comprueba y une los datos en el warehouse; `mlops` (§10) busca los
hiperparámetros, entrena el mejor modelo y decide si sustituye al que está en
producción.

**El modelo en producción** es el modelo entrenado que se usa ahora mismo para
etiquetar texto nuevo. Aquí nada lo sirve por red: el model registry de MLflow
marca una versión como `champion`, y `predict.py` carga esa versión y etiqueta
el texto que le des:

```
"Transfer learning with BERT for text classification"  →  natural-language-processing
"Real-time object detection on a webcam with YOLO"     →  computer-vision
```

**El drift** es que los datos cambien por debajo del modelo en producción. El
Dag `monitoring` (§12) compara el cuarto más reciente de los proyectos, por
fecha de alta, con los anteriores: si ha cambiado la proporción de cada tag, si
ha aparecido un tag que los proyectos anteriores no tienen, si el modelo en
producción sigue puntuando igual de bien.

Los módulos, de los que solo `assets.py` importa Airflow:

| Módulo | Papel |
|---|---|
| `config.py` | nombres y ajustes: tablas, MLflow, Optuna, umbrales |
| `data.py` | leer el warehouse, snapshots, preprocesado del texto, split train/test |
| `validate.py` | las suites de Great Expectations que ejecutan los gates, y las comprobaciones del split |
| `train.py`, `evaluate.py` | el clasificador, la búsqueda con Optuna, el registro en MLflow; métricas globales y por slices de evaluación (§10) |
| `registry.py` | el model registry de MLflow: registrar, promocionar, hacer rollback |
| `monitoring.py`, `drift.py` | las ventanas sobre las que se mide el drift, los tests de alibi-detect, el rendimiento en vivo |
| `predict.py` | etiqueta texto nuevo con lo que esté promocionado ahora mismo |
| `assets.py` | los Assets de Airflow que comparten los Dags |
| `callbacks.py` | callbacks de deadline alerts |

El SQL no está en ese paquete: es el proyecto de dbt que creas en la §9, en
`dbt/`.

---

[Índice](README.md) · [2. Ponlo en marcha](02-ponlo-en-marcha.md) →
