## 1. Qué vas a construir

La orquestación convierte un montón de scripts que lanzas a mano en un sistema:
el trabajo se declara como un grafo de tasks, un componente aparte decide cuándo ejecutarlo,
reintenta las partes que fallan y registra qué pasó.

Un workflow en Airflow es un **Dag**, un grafo dirigido acíclico: *dirigido*,
las aristas tienen dirección; *acíclico*, ningún camino vuelve a una task por la
que ya pasó. Cada nodo es una **task**; una ejecución del grafo entero es un
**Dag run**.

Los logs y los mensajes de error dan por hecho que distingues dos palabras. Una
**task** es lo que escribiste una vez, en el archivo. Una **task instance** es
esa task en un Dag run: tiene un estado, una duración, un log y un número de
intentos. Tú escribes tasks; tú depuras task instances. La §3 sigue una a lo
largo de toda su vida.

Este es un tutorial de MLOps: construyes un pequeño sistema de machine
learning, desde los datos sin procesar hasta un modelo en producción que se
reentrena cuando hace falta. Cada paso usa la herramienta que usaría un equipo
real; Airflow decide cuándo se ejecuta cada una:

| Herramienta | Su trabajo aquí | Sección |
|---|---|---|
| **Airbyte** | copia los datos de origen al warehouse, la base de datos donde se guardan los datos para analizarlos | §9 |
| **BigQuery** | el warehouse, en Google Cloud | §9 |
| **dbt** | convierte las tablas sin procesar en la tabla de entrenamiento, en SQL | §9 |
| **Great Expectations** (GX) | comprueba los datos antes de que los use cada paso | §9, §10 |
| **Optuna** | busca los mejores hiperparámetros: los ajustes del modelo que se fijan antes de entrenar | §10 |
| **MLflow** | registra cada entrenamiento, y lleva el model registry: las versiones numeradas del modelo entrenado, y cuál está en producción | §10, §12 |
| **alibi-detect** | detecta drift: datos nuevos que ya no se parecen a aquellos con los que aprendió el modelo | §12 |

Todas salvo BigQuery son open source y corren en tu máquina. BigQuery necesita
una cuenta de Google Cloud con facturación activada; los datos de este tutorial
quedan muy por dentro del free tier (§9).

Tres Dags hacen el trabajo de verdad; lo que importa es cómo se conectan:

```
dataops:  extract & load ─► validate ─► transform ─► validate_transformed ─┐
          (Airbyte)          (GX)       (dbt)         (GX)                  │ emite
                                                              Asset: labeled_projects
                                                                            │ dispara
mlops:    prepare ─► validate ─► tune×N ─► train ─► evaluate ─► gate ───────┴─► approve ─► promote
                     (GX)        (Optuna)  (MLflow)                             (una persona) (registry de MLflow)
                                                                            │ emite
                                                              Asset: production_model

monitoring:  measure ─► decide ─► {continue | inspect | rollback | retrain}
             (alibi-detect)
```

Un **asset** es un conjunto de datos con nombre: `labeled_projects` son los
datos etiquetados que produce `dataops`, y `production_model`, el modelo que
promueve `mlops`. Cada vez que la task que produce un asset tiene éxito, Airflow
registra un evento, y todo Dag programado con ese asset arranca un run (§8). Por
eso `mlops` **no tiene un schedule de tiempo**: se ejecuta cuando `dataops`
publica datos buenos. Este scheduling data-aware une los tres Dags.

Otros cuatro Dags enseñan una cosa cada uno: `hello_airflow3` (un primer Dag),
`operators_and_xcoms` (tipos de task y cómo hablan entre ellas),
`sensors_and_events` (esperar al mundo exterior) y `deadline_alerts` (que te
avisen cuando un run va demasiado lento).

### Cómo funciona este tutorial

Los siete Dags los escribes tú, además del paquete que comparten (§8) y del
proyecto de dbt (§9). `dags/`, la carpeta que lee Airflow, y `dbt/` están
**vacías** cuando clonas este repo. Cada sección explica una pieza de Airflow,
te hace construir el Dag que la usa, unas pocas líneas cada vez con el porqué de
cada una, y luego ejecutarlo y comprobar el resultado.

Dos redes de seguridad:

- **[`solutions/`](../../solutions/)** tiene una copia terminada de cada archivo, con
  los comentarios didácticos dentro. Compara cuando algo no se parsee, o sáltate
  una sección copiando su archivo: `cp solutions/03_dataops.py dags/`. El
  [apéndice](../appendix-dags_ES.md) imprime cada solución completa.
- **Los tests de la §15** leen *tu* carpeta `dags/`. Cuando los siete estén
  escritos, te dicen si construiste lo que el tutorial pretendía.

A partir de la §9, las secciones se apoyan unas en otras — `mlops` se programa
con un asset que produce `dataops`, `sensors_and_events` espera una tabla que
construye `dataops` — así que hazlas en orden, o copia antes las soluciones
anteriores. La §9 monta además el stack de datos del que depende el resto: un
proyecto de Google Cloud, Airbyte y las conexiones entre ellos.

### Por qué Airflow

Airflow no es el único orquestador — Prefect, Dagster, Luigi, Metaflow, Flyte,
Kubeflow Pipelines y Vertex AI Pipelines cubren el mismo terreno. Merece la pena
como primer orquestador por:

- **Adopción.** Es el estándar en la mayoría de plataformas de datos, así que lo
  que aprendas te sirve en otros sitios y todos los vendors ofrecen integración.
- **Apache governance**, un ecosistema grande de providers, y ningún vendor
  único.

Como Prefect o Dagster, tiene además lo que se espera de un orquestador moderno:
workflows en Python normal, testeables con pytest (§15), y el mismo código en tu
portátil y en un clúster — cambias el Executor, no el Dag.

### El proyecto de ML que orquesta

Airflow orquesta código; no lo contiene. Aquí ese código es un pequeño proyecto
de ML en `src/project_tagger/` al que llaman los Dags; no lo escribes tú.

**Los datos** son 764 proyectos de ML que recopiló Made With ML entre febrero y
octubre de 2020, en dos tablas como las guardaría un sistema de origen
([`datasets/`](../../datasets/)): `projects.csv`, con el título, la descripción
y la fecha de alta de cada proyecto, y `tags.csv`, con el **tag** de cada
proyecto, la etiqueta que el modelo aprende a predecir. Unidas por `id`, una
fila:

| title | description | tag |
|---|---|---|
| Rethinking Batch Normalization in Transformers | We found that NLP batch statistics exhibit large variance throughout training, which leads to poor BN performance. | `natural-language-processing` |

Hay seis tags. Los tres más raros (`reinforcement-learning`,
`graph-learning`, `time-series`) tienen muy pocos proyectos para aprender de
ellos, así que el modelo los ve como una sola clase, `other`, junto a
`natural-language-processing`, `computer-vision` y `mlops`.

Dos entregas posteriores los amplían. En la §12, 800 papers de arXiv de
2025–2026, extraídos para este tutorial: datos más nuevos y de otro origen, así
que el drift baja el f1 del modelo en producción, y reentrenar lo recupera. En
la §14, 191 proyectos más de Made With ML, con problemas añadidos para este
tutorial.

**El modelo** lee el título y la descripción de un proyecto y predice su tag: un
clasificador de texto de scikit-learn (features TF-IDF y un clasificador lineal
SGD) que entrena en más o menos un segundo, así que una búsqueda sobre decenas
de combinaciones de hiperparámetros cabe en un Dag run. `dataops` (§9) carga,
comprueba y une los datos en el warehouse; `mlops` (§10) busca los
hiperparámetros, entrena el mejor modelo y decide si sustituye al que está en
producción.

**El modelo en producción** es el modelo entrenado que se usa ahora mismo para
etiquetar texto nuevo. Nada lo sirve por red: el model registry de MLflow marca
una versión como `champion`, y `predict.py` carga esa versión y etiqueta el
texto que le des:

```
"Transfer learning with BERT for text classification"  →  natural-language-processing
"Real-time object detection on a webcam with YOLO"     →  computer-vision
```

**El drift** es que los datos cambien mientras el modelo en producción sigue
igual. El Dag `monitoring` (§12) compara los proyectos que llegaron después de
entrenar el modelo en producción con aquellos de los que aprendió: si ha
cambiado la proporción de cada tag, si ha aparecido un tag que el modelo nunca
vio, si el modelo en producción sigue puntuando igual de bien.

Los módulos, ninguno de los cuales importa Airflow:

| Módulo | Papel |
|---|---|
| `config.py` | nombres y ajustes: tablas, MLflow, Optuna, umbrales |
| `data.py` | leer el warehouse, snapshots, preprocesado del texto, el split train/validación/test |
| `validate.py` | las suites de Great Expectations que ejecutan los gates, y las comprobaciones del split |
| `train.py`, `evaluate.py` | el clasificador, la búsqueda con Optuna, el registro en MLflow; métricas globales y por slices de evaluación (§10) |
| `registry.py` | el model registry de MLflow: registrar, promocionar, hacer rollback |
| `monitoring.py`, `drift.py` | las ventanas sobre las que se mide el drift, los tests de alibi-detect, el rendimiento en vivo |
| `predict.py` | etiqueta texto nuevo con lo que esté promocionado ahora mismo |
| `callbacks.py` | callbacks de deadline alerts |

Dos cosas no están en ese paquete: el SQL, que es el proyecto de dbt que creas
en `dbt/` (§9), y los Assets de Airflow que comparten los Dags, que son
orquestación y que escribes junto a los Dags (§8).

### Qué cambia respecto a Made With ML

Este tutorial parte de la
[lección de orquestación](https://madewithml.com/courses/mlops/orchestration/)
de Made With ML: el mismo proyecto, los mismos datos de partida y el mismo
pipeline de DataOps, MLOps y continual learning. Algunas piezas vienen de otras
lecciones del mismo [curso de MLOps](https://madewithml.com/courses/mlops/):
la puesta en marcha del data stack de *Data engineering*, Optuna de *Tuning*,
MLflow de *Experiment tracking*, los slices de evaluación de *Evaluation* y los
tests de drift de *Monitoring*. Lo nuevo es cómo se combinan:

- **Airflow 3.** Todo el código está escrito para Airflow 3.3.1; el original
  usa una versión anterior.
- **Para ejecutarlo, no solo para leerlo.** Escribes tú los siete Dags, paso a
  paso, sobre un stack de Docker que levantas con un comando, con soluciones y
  tests para comprobar tu trabajo.
- **Un problema que resuelves por tu cuenta.** Hasta la §14, este tutorial,
  como la lección de orquestación, te guía en cada paso. En la §14 una entrega
  rompe el pipeline y solo recibes el objetivo: encontrar la causa, decidir
  dónde va el arreglo, escribirlo, añadir el control que faltaba y volver a
  ejecutar lo mínimo, con pistas si te atascas y una solución con la que
  comparar.
- **Un pipeline que se dispara solo.** Los datos nuevos arrancan el
  entrenamiento a través de un asset, sin triggers manuales (§8).
- **Un paso de promoción.** El Dag `mlops` de la lección de orquestación
  termina en el entrenamiento. Aquí el modelo se evalúa, se compara con el que
  está en producción, lo aprueba una persona y se promociona en el registry de
  MLflow (§10).
- **Continual learning como un Dag.** La lección de orquestación describe las
  respuestas al drift y la lección *Monitoring* mide el drift; aquí un solo Dag
  mide el drift y decide si seguir, reentrenar, inspeccionar o hacer rollback
  (§12).
- **Datos que cambian y que fallan.** Papers de arXiv que provocan drift de
  verdad (§12), y entregas con los fallos típicos de una carga real (§14).
- **Temas que el original no cubre:** las piezas de Airflow y su UI (§3, §5),
  logical dates y assets (§7, §8), deferrable tasks y eventos (§11), deadline
  alerts (§13), qué hacer cuando los datos fallan (§14), testear Dags (§15) y
  llevarlo a producción (§16).

---

[Índice](README.md) · [2. Ponlo en marcha](02-ponlo-en-marcha.md) →
