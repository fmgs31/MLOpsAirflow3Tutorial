## 10. El Dag de MLOps

`dags/04_mlops.py` no tiene schedule de tiempo: lo dispara cada asset event de
`labeled_projects`.

```
prepare_data ─► validate_training_data ─┐
create_study ───────────────────────────┤
trial_budget ───────────────────────────┴─► run_trials×N ─► best_params ─► train_best
─► compare_with_champion ─┬─► summarize_candidate ─┐
                          └─► promotion_gate ─┬────┴─► approve_promotion ─► promote_model ──► done
                                              └─► keep_current_model ──────────────────────┘
```

| Task | Herramienta | Qué hace |
|---|---|---|
| `prepare_data` | BigQuery | copia `labeled_projects` a archivos que este run usará de principio a fin, partida una sola vez en train, validación y test |
| `validate_training_data` | Great Expectations | la suite de la §9 otra vez, más comprobaciones del split |
| `create_study`, `trial_budget`, `run_trials`×N, `best_params` | **Optuna**, con tracking en **MLflow** | busca los hiperparámetros, con el trabajo repartido entre varias tasks, y registra cada trial |
| `train_best` | tracking de **MLflow** | entrena el modelo final y lo registra: parámetros, métricas, el archivo del modelo |
| `compare_with_champion` | Model Registry de **MLflow** | puntúa el modelo nuevo y el modelo en producción sobre el split de test de este run |
| `summarize_candidate` | Airflow | resume los números del modelo nuevo para la persona que lo aprueba |
| `promotion_gate` → `approve_promotion` → `promote_model` | Model Registry de **MLflow**, y una persona | el gate decide si el modelo nuevo puede pasar a producción, una persona lo aprueba en la UI de Airflow, y el registry lo promociona |

### El stack de ML

Dos herramientas más, y ninguna necesita cuenta: Optuna es una librería que
importan las tasks, y MLflow es un servicio del stack de compose.

**Optuna** busca hiperparámetros. Un **study** es una búsqueda, con un objetivo
(aquí: maximizar el f1). Un **trial** es un intento: Optuna elige un valor para
cada hiperparámetro, el código entrena y puntúa un modelo con ellos, y la
puntuación se guarda en el study. Optuna no prueba todas las combinaciones:
usa las puntuaciones obtenidas hasta el momento para elegir los siguientes
valores. El study se guarda en una base de datos, aquí la base de datos
`optuna` del Postgres del stack, así que varios procesos pueden añadir trials
al mismo study a la vez, y eso es lo que permite a Airflow repartir la
búsqueda entre varias tasks.

**MLflow** registra experimentos. Cada entrenamiento es un **run**, con sus
**parameters** (los hiperparámetros usados), sus **metrics** (f1,
precision...) y sus **artifacts** (archivos: aquí `metrics.json`,
`split_ids.json` y el propio modelo). Los runs pertenecen a un **experiment**,
`project_tagger`. El **Model Registry**, también en MLflow, da a un modelo un nombre y **versiones**
numeradas. Un **alias** es una etiqueta que puedes mover de una versión a otra.
Este proyecto usa dos:

```
champion             la versión en producción: la que carga predict.py
previous_champion    la versión a la que apuntaba champion antes de la última promoción
```

Promocionar un modelo es mover `champion`; hacer rollback es devolverlo a donde
estaba. No se copia ningún archivo: quien consume el modelo pide
`models:/project_tagger@champion` y recibe la versión a la que apunte el alias.

El servidor de MLflow es el servicio `mlflow` del archivo de compose. Guarda
los runs y el registry en un archivo SQLite y los archivos de los modelos en
una carpeta propia, en un volumen de Docker, y las tasks lo encuentran en
`http://mlflow:5000` (`MLFLOW_TRACKING_URI`). Abre su UI:

**Comprobación.** <http://localhost:5000> muestra MLflow con un experiment,
`Default`, y ningún run. El experiment `project_tagger` todavía no existe: lo
crea el código del Dag `mlops` (`mlflow.set_experiment` en
`src/project_tagger/train.py`) la primera vez que registra un run.

### Cómo se hablan las piezas

Cada columna es un sistema, como en el diagrama de la §9:

```
 Airflow (Docker)            │ Postgres          │ MLflow (Docker)     │ BigQuery
 Dag mlops                   │ base de datos     │ localhost:5000      │ mlops_course
                             │ optuna            │                     │
─────────────────────────────┼───────────────────┼─────────────────────┼───────────────────
                             │                   │                     │
 1 prepare_data ◄──────────────────────────────────────────────────────── labeled_projects
   guarda un snapshot y sus  │                   │                     │
   tres splits en            │                   │                     │
   data/snapshots/           │                   │                     │
 2 validate_training_data    │                   │                     │
   (GX, sobre el snapshot)   │                   │                     │
 3 create_study ──────────────► un study nuevo   │                     │
   y su run ──────────────────────────────────────► el run del study   │
 4 run_trials ×N ─────────────► pide params,     │                     │
   cada task entrena unos    │  guarda scores    │                     │
   modelos y registra cada ───────────────────────► un run hijo del    │
                             │                   │  study por trial    │
 5 best_params ◄─────────────── el mejor trial   │                     │
   y lo anota en ─────────────────────────────────► el run del study   │
 6 train_best ────────────────────────────────────► run final, con     │
                             │                   │  el modelo;         │
                             │                   │  registrado como    │
                             │                   │  versión nueva      │
 7 compare_with_champion ◄───────────────────────── el modelo champion │
   puntúa los dos sobre el   │                   │  y el nuevo         │
   test: nuevas y viejas     │                   │                     │
 8 summarize_candidate       │                   │                     │
   los números para el       │                   │                     │
   aviso de aprobación       │                   │                     │
 9 promotion_gate            │                   │                     │
   umbral, después champion  │                   │                     │
10 promote_model ─────────────────────────────────► mueve champion     │
   (o keep_current_model)    │                   │  a la versión nueva │
```

BigQuery se lee una sola vez, en el paso 1; todas las tasks siguientes leen los
archivos del snapshot, así que el run entero trabaja con los mismos datos. En
MLflow queda un run para el study, con los trials como runs hijos debajo, más
el run final; el archivo del modelo solo se sube con el run final.

### Constrúyelo

Crea `dags/04_mlops.py`:

```python dags/04_mlops.py
"""Lesson 4 - the MLOps Dag: asset-triggered tuning, training, and promotion."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.sdk import Asset, Param, dag, task
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk.types import OutletEventAccessorsProtocol

from project_tagger import config, data, evaluate, registry, train as train_module, validate
from project_tagger_orchestration.assets import LABELED_PROJECTS, PRODUCTION_MODEL

DEFAULT_ARGS = {
    "owner": "ml-engineering",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=2),
}


def run_key(run_id: str) -> str:
    return run_id.replace(":", "-").replace("+", "-")


@dag(
    dag_id="mlops",
    schedule=[LABELED_PROJECTS],
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "mlops"],
    doc_md=__doc__,
    params={
        "n_trials": Param(20, type="integer", minimum=1, description="Optuna trials in total."),
        "n_workers": Param(4, type="integer", minimum=1, description="Tasks the trials are spread over."),
    },
)
def mlops():
```

`schedule=[LABELED_PROJECTS]` es lo esencial: una lista de assets, y el Dag
corre cuando se actualizan. Varios assets en la lista significan *todos*,
como `AssetAll(a, b)`; `AssetAny(a, b)` significa cualquiera.

`params` declara el presupuesto de la búsqueda como dos params del Dag (§4):
20 trials en total, repartidos entre 4 tasks. Fijan cuánto se busca, no qué se
busca. Los hiperparámetros que ajusta Optuna, y sus rangos, están fijos en
`objective()`, en `src/project_tagger/train.py`:

| Hiperparámetro | Rango | Qué controla |
|---|---|---|
| `ngram_max` | de 1 a 3 | la secuencia de palabras más larga que TF-IDF convierte en feature (1 = solo palabras sueltas) |
| `min_df` | de 1 a 3 | los términos que aparecen en menos proyectos que este número se descartan |
| `alpha` | de 1e-6 a 1e-2, en escala logarítmica | la fuerza de la regularización del clasificador SGD: más alta da un modelo más simple |

`run_key` convierte un run id como
`manual__2026-02-01T03:00:00+00:00` en algo que se puede usar sin problema
como nombre de archivo y de study.

**Guarda un snapshot de los datos.** Añade la primera task:

```python dags/04_mlops.py
    @task
    def prepare_data(
        *, triggering_asset_events: Mapping[Asset, Sequence[Any]], run_id: str
    ) -> dict[str, str]:
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")
        config.ensure_dirs()
        # Writes data/snapshots/<key>/; returns the paths "snapshot", "train", "val", "test".
        paths = data.save_snapshot(data.load_labeled(), run_key(run_id))
        print(f"snapshot: {paths}")
        return paths
```

`triggering_asset_events` funciona como en la §8 (de ahí los imports de
`Mapping`, `Sequence` y `Asset`); en un run que arranques a mano está vacío.
`run_id` es otra clave del contexto (§4), recibida por nombre.

`prepare_data` lee la tabla del warehouse una vez y la escribe en un
**snapshot**: archivos en una carpeta con el nombre del run, que leen todas las
tasks posteriores. La tabla puede cambiar mientras un run está en marcha (si un
run de `dataops` termina mientras tanto), y con el snapshot todas las tasks de
este run trabajan con las mismas filas, incluida una task que se reintenta una
hora después. Los archivos son Parquet, no CSV: un archivo Parquet guarda el
tipo de cada columna, así que cada columna vuelve tal como se escribió,
`created_on` como timestamp y no como string. Un CSV es solo texto, y los
tipos se vuelven a adivinar en cada lectura. La task devuelve las rutas de los
archivos, un dict pequeño: los XComs llevan un puntero a los datos, no los
datos (§6).

`data.save_snapshot` escribe cuatro archivos: la tabla tal como se leyó
(`labeled_projects.parquet`), que comprueba la suite de la §9, y la entrada del
modelo partida en tres: `train.parquet`, `val.parquet` y `test.parquet`. La
entrada del modelo es lo que `data.preprocess` hace con la tabla: descarta las
filas sin título o sin tag, une título y descripción en una sola columna de
texto limpio, `text`, y convierte en `other` todo tag fuera de las cuatro
clases que aprende el modelo (`config.ACCEPTED_TAGS`). La búsqueda entrena con train y puntúa cada trial con
validación; el modelo final se entrena con las dos y se puntúa con test, que
ningún paso ha usado hasta entonces.

El split se **escribe una sola vez**. Si cada task lo recalculara, todas
dependerían de que el código, las librerías y la semilla fueran idénticos donde
corre cada una: una task reintentada tras un despliegue podría entrenar con
unas filas y evaluar con otras, sin ningún aviso. Escrito una vez, todas las
tasks leen las mismas filas, y quedan guardadas las filas de cada modelo.

El split de cada fila lo **decide su id**. `data.assign_split` convierte con un
hash el id de cada proyecto en un número del 0 al 99: por debajo de 20 es test,
de 20 a 35 validación, el resto train (`config.TEST_PCT` y `config.VAL_PCT`).
Así un proyecto cae en el mismo split en todos los runs, y ningún modelo entrena
con una fila que es de test para otro. Eso permite a `promotion_gate`, más
adelante en esta sección, puntuar el modelo nuevo y el de producción sobre las
mismas filas de test, y a la monitorización (§12) saber qué filas no ha visto
nunca el modelo en producción. El coste: el split deja de ser estratificado (la
misma proporción de cada tag en cada split). Con cientos de filas las
proporciones salen parecidas, y la comprobación del split detecta una clase
ausente o escasa.

**Valida antes de entrenar.** `dataops` validó `labeled_projects` al
construirla, y este Dag la valida otra vez antes de entrenar con ella. Eso no
es redundante: aquella validación fue en otro Dag run, posiblemente hace horas;
este run puede haberse disparado a mano; y el modelo se entrena con los datos tal como están
*ahora*. Añade la segunda task:

```python dags/04_mlops.py
    @task
    def validate_training_data(paths: dict[str, str]) -> dict[str, Any]:
        # The table as read from the warehouse, against the labeled_projects suite.
        df = data.load_snapshot(paths["snapshot"])
        table = validate.run_suite(df, "labeled_projects")
        # The three preprocessed splits (train, val, test), checked against each other.
        split = validate.check_split(*(data.load_snapshot(paths[name]) for name in data.SPLITS))
        print(f"{table.summary()} {split.summary()}")
        if not (table.passed and split.passed):
            raise AirflowFailException(
                f"Training snapshot failed validation: {table.failures + split.failures}"
            )
        return {
            "rows": len(df),
            "warnings": [r["expectation"] for r in table.failures + split.failures],
        }
```

Una suite a nivel de tabla no puede hacer la comprobación del split:

```python
# src/project_tagger/validate.py — check_split()
("split_is_non_empty",          ...,  critical)
("classes_in_every_split",      ...,  critical)   # a class missing from test
("min_rows_per_class:20",       ...,  critical)   # 3 rows of a class in train + val
("no_id_leakage",               ...,  critical)   # the same project in two splits
("fractions_near_target",       ...,  non-critical)
```

Cada problema de esos comentarios — una clase que falta en test, tres filas de
una clase, el mismo proyecto en dos splits — supera sin ningún aviso una suite
que valida columna a columna, y aun así deja sin sentido el f1 de test, que es
lo que `promotion_gate` compara con el modelo en producción. El mínimo de filas por
clase cuenta train y validación juntos, porque el modelo final aprende de los
dos. Estas comprobaciones abarcan tres dataframes y una suite de GX valida un
lote cada vez, así que son Python normal, con resultados en el mismo formato
que la suite.

**Búsqueda distribuida.** Un study de Optuna por run, y después **dynamic
task mapping**: mientras corre el Dag, Airflow crea una task instance por cada
elemento de una lista, y cada una añade trials al mismo study. Primero el
study y la lista:

```python dags/04_mlops.py
    @task
    def create_study(*, run_id: str) -> str:
        return train_module.create_study(f"{config.REGISTERED_MODEL}-{run_key(run_id)}")

    @task
    def trial_budget(*, params: Mapping[str, Any]) -> list[int]:
        base, extra = divmod(params["n_trials"], params["n_workers"])
        return [base + (1 if i < extra else 0) for i in range(params["n_workers"]) if base or i < extra]
```

`create_study` crea el study con `load_if_exists=True` (en `train.py`), así que
una task reintentada carga el study en vez de fallar porque ya existe. También
abre el run del study en MLflow, el **run padre** de todos los trials, y guarda
el id de ese run en el propio study, como *user attribute* (un valor que Optuna
guarda junto al study). Un reintento encuentra ahí el id y no abre un segundo
run.

`trial_budget` lee los params del Dag a través de la clave de contexto `params`
y reparte los trials entre `n_workers` tasks: 20 entre 4 da `[5, 5, 5, 5]`.
Esa lista es sobre la que se mapea la siguiente task.

> **`params` está reservado.** Un `@task` recibe claves del contexto por nombre
> de parámetro (§4), y `params` es una de ellas: `trial_budget` recibe los
> params del Dag porque su argumento se llama `params`. Si por error llamas
> `params` a un argumento de datos, recibirá los params del Dag en vez de tus
> datos.

Ahora la task que se mapea:

```python dags/04_mlops.py
    @task(max_active_tis_per_dag=2)
    def run_trials(n_trials: int, study_name: str, paths: dict[str, str]) -> list[dict[str, Any]]:
        train_df, val_df = data.load_snapshot(paths["train"]), data.load_snapshot(paths["val"])
        results = train_module.run_trials(study_name, n_trials, train_df, val_df)
        print(f"{len(results)} trials, best here f1={max(r['f1'] for r in results):.4f}")
        return results
```

Nada en ella menciona el mapeo. `run_trials` está escrita para una sola
instancia: su `n_trials` es un número, no la lista. El mapeo está en cómo la
llama el cableado del final del archivo:

```python
run_trials.partial(study_name=study, paths=paths).expand(n_trials=trial_budget())
```

- `.partial()` recibe los argumentos que todas las instancias reciben iguales:
  el nombre del study y las rutas del snapshot.
- `.expand()` recibe el argumento que cambia, como una lista, y crea una
  instancia por cada elemento.

Con `trial_budget` devolviendo `[5, 5, 5, 5]`, esa línea se convierte en cuatro
task instances, que se distinguen por su **map index**:

| Map index | `n_trials` | `study_name`, `paths` |
|---|---|---|
| 0 | `5` (elemento 0) | los mismos para las cuatro |
| 1 | `5` (elemento 1) | los mismos para las cuatro |
| 2 | `5` (elemento 2) | los mismos para las cuatro |
| 3 | `5` (elemento 3) | los mismos para las cuatro |

La lista solo existe cuando `trial_budget` ha corrido, así que el número de
instancias se decide cuando corre el Dag, no cuando se parsea: pon `n_workers`
a 2 en los params de un run y ese run tendrá dos. Cada instancia se reintenta,
tiene su propio log y se inspecciona por separado; una instancia reintentada
solo repite sus propios trials.

Cada instancia entrena sus trials con el split de entrenamiento, los puntúa sobre
el split de validación, y registra cada trial en MLflow como un **run hijo**:
un run con padre, que la UI de MLflow pliega debajo de él. El split de test no se ve
en ningún momento de la búsqueda, así que la puntuación de test del modelo
final sigue siendo una estimación honesta.

Un run hijo se abre normalmente con `mlflow.start_run(nested=True)` dentro del
bloque `with mlflow.start_run():` del padre. Eso solo funciona en el proceso
que abrió el padre, y aquí el padre lo abrió `create_study` y cada trial corre
en una de las instancias de `run_trials`. Así que `train.py` nombra el padre
explícitamente, `mlflow.start_run(parent_run_id=...)`, con el id leído del
study.

**Cuántas instancias de `run_trials` corren a la vez es una decisión, escrita
en el decorador de `run_trials`.** Nada en `.expand()` dice "en paralelo";
dice "estas son cuatro task instances separadas". Que se solapen o no es
asunto del executor, acotado por el más restrictivo de los límites de la §3, y
`max_active_tis_per_dag=2` en esa task es deliberadamente el más restrictivo
aquí. Así que el fan-out corre **de dos en dos, en dos oleadas**. Sube el tope y las cuatro se solapan; quítalo y `[core]
max_active_tasks_per_dag` (16) es el siguiente límite. Un tope en una task de
entrenamiento no es un ajuste de juguete: cuatro instancias a la vez son cuatro
copias de los datos en memoria. Cuando el recurso limitado es compartido
— por ejemplo, una base de datos que solo acepta unas pocas conexiones —, un
`pool` (§3) pone el mismo tipo de tope a todas las tasks y Dags que lo usan.

Por último, la task que lee el resultado de la búsqueda:

```python dags/04_mlops.py
    @task
    def best_params(study_name: str, results: list[list[dict[str, Any]]]) -> dict[str, Any]:
        # Also records it on the study's run: metric best_f1, tag best_trial_run_id.
        best = train_module.best_trial(study_name)
        print(f"best trial #{best['number']} f1={best['f1']:.4f} of {best['num_trials']}: {best['params']}")
        return best
```

`best_params` le pide al study su mejor trial. Aun así recibe los resultados
de las instancias mapeadas: ese argumento es lo que la obliga a esperar a todas
las instancias de `run_trials`, y llega como una lista, un elemento por
instancia, en orden de map index.

Ahora el modelo final. Añade `train_best`:

```python dags/04_mlops.py
    @task
    def train_best(best: dict[str, Any], paths: dict[str, str], *, run_id: str) -> dict[str, Any]:
        splits = {name: data.load_snapshot(paths[name]) for name in data.SPLITS}
        trained = train_module.train_model(
            best["params"], splits,
            run_name=f"final-{run_key(run_id)}",
            tags={
                "airflow_run_id": run_id,
                "best_trial_run_id": best["mlflow_run_id"],
                "snapshot_path": paths["snapshot"],
            },
        )
        version = registry.register(trained["model_uri"])
        print(f"run={trained['run_id']} version={version} f1={trained['metrics']['f1']:.4f}")
        return {
            "run_id": trained["run_id"],
            "version": version,
            "f1": trained["metrics"]["f1"],
            "slices": trained["metrics"]["slices"],
        }
```

`train_best` entrena el modelo final con los hiperparámetros del mejor trial
sobre los splits de entrenamiento y validación juntos (el de validación ya ha
cumplido su función, elegir esos hiperparámetros, y más filas dan un modelo
mejor), lo puntúa sobre el split de test y lo guarda en MLflow, con el archivo
del modelo incluido, como un run propio, fuera del study: no es un intento más
de la búsqueda. Sus tags lo enlazan con el run de Airflow que lo creó, con el
trial del que salen sus hiperparámetros y con el snapshot con el que se
entrenó (`snapshot_path`). El run guarda además `split_ids.json`, los ids de
proyecto de cada split: aquello con lo que se construyó el modelo se queda con
él en MLflow aunque la carpeta del snapshot ya no exista, y la §12 lo lee.
Después la task **registra** el modelo en
el registry: cada modelo final pasa a ser una versión nueva de `project_tagger`,
se promocione o no, así que un candidato rechazado sigue ahí para
inspeccionarlo. La task devuelve un run id, un número de versión, el f1 y las
métricas por slice: poca cosa, y serializable a JSON. El modelo en sí se queda
en MLflow.

Las métricas por slice son el f1 en cada **slice de evaluación**: un subgrupo
del conjunto de test que selecciona una regla de `evaluate.py` (textos cortos,
y textos que mencionan transformers, BERT o LLMs). No son las franjas de tiempo
de la §7; muestran un modelo cuyo f1 global parece bueno mientras falla con un
tipo de entrada. Son pequeños: de las 157 filas de test de esta sección, 4 son
textos cortos y 23 mencionan transformers, así que una sola predicción mueve
mucho el f1 de un slice. Demasiado ruido para que decida una regla fija; una
persona sí puede sopesarlos, y para eso está `approve_promotion`.

Después, la comparación con el modelo en producción. Añade `compare_with_champion`:

```python dags/04_mlops.py
    @task
    def compare_with_champion(trained: dict[str, Any], paths: dict[str, str]) -> dict[str, Any] | None:
        champion = registry.get_production()
        if champion is None:
            print("no champion yet")
            return None
        seen = [i for ids in registry.load_split_ids(champion["run_id"]).values() for i in ids]
        comparison = evaluate.compare(
            registry.load_model(version=trained["version"]),
            registry.load_model(),
            data.load_snapshot(paths["test"]),
            seen,
        )
        for name, part in comparison.items():
            if part["rows"]:
                print(
                    f"{name} test rows: {part['rows']}, f1={part['f1']:.4f}, "
                    f"champion version {champion['version']} f1={part['champion_f1']:.4f}"
                )
        return comparison
```

`compare_with_champion` carga el modelo en producción y la versión que
`train_best` acaba de registrar, y puntúa los dos sobre el split de test de
este run. El champion también tiene el f1 que registró cuando se entrenó, pero
ese se midió sobre el split de test de otro run, con otras filas; compararlo
con el f1 del modelo nuevo sería comparar dos exámenes, no dos modelos. Sobre
las mismas filas la comparación es justa, y el split por hash hace que esas
filas sean honestas para los dos: ninguna fila de test ha estado nunca en los
datos de entrenamiento de ningún modelo, tampoco en los del champion.

`evaluate.compare` además separa las filas de test según el `split_ids.json`
del champion, y puntúa los dos modelos en cada parte:

| Parte | Filas | Qué dice |
|---|---|---|
| `new` | filas de test cuyo id no está en ninguno de los splits del champion: llegaron al warehouse después de que se entrenara | cómo le va a cada modelo con datos como los que producción recibe a continuación |
| `old` | las filas de test del propio champion | si el modelo nuevo conserva lo que el champion sabía hacer |
| `all` | las dos | el split entero |

Un reentrenamiento existe para mejorar con los datos nuevos, así que deciden
las filas `new`: el f1 del modelo nuevo sobre ellas no puede quedar por debajo
del del champion. Con menos de 30 filas nuevas (`config.MIN_NEW_TEST_ROWS`)
hay demasiado pocos datos nuevos para decidir, y decide el split entero. Es el
caso de esta sección, en la que cada run ve las mismas 764 filas; en la §12
llegan filas nuevas. La parte `old` está en el log para que la leas; la §12
vuelve a ella.

`compare_with_champion` es una task propia para que el Graph la muestre y su
log diga con qué versión comparó, parte por parte. Recibe la salida de
`train_best`, que lleva la versión nueva, así que corre después de ella.

A continuación, el **promotion gate**, que decide si el modelo nuevo pasa a
producción. A diferencia de los gates de la §9, el gate no hace fallar la task cuando
la respuesta es no: es un *branch*, una task que elige cuál de dos **ramas**
del Dag sigue. Añade `summarize_candidate` y `promotion_gate`:

```python dags/04_mlops.py
    @task
    def summarize_candidate(trained: dict[str, Any], comparison: dict[str, Any] | None) -> str:
        summary = evaluate.summarize(trained["version"], trained["f1"], trained["slices"], comparison)
        print(summary)
        return summary

    @task.branch
    def promotion_gate(trained: dict[str, Any], comparison: dict[str, Any] | None) -> str:
        promote, reason = evaluate.should_promote(
            trained["f1"],
            comparison,
            config.MIN_F1_TO_PROMOTE,
            config.MIN_NEW_TEST_ROWS,
        )
        print(reason)
        return "approve_promotion" if promote else "keep_current_model"
```

`summarize_candidate` redacta, con `evaluate.summarize`, lo que una persona
necesita para juzgar el modelo nuevo: su f1, el f1 de los dos modelos en cada
parte del split de test, y el f1 de cada slice junto a su número de filas. El
texto es Markdown, y la task lo devuelve, así que va a XCom para que
`approve_promotion` lo muestre.

`promotion_gate` aplica la regla de `evaluate.should_promote`: el f1 del
modelo nuevo tiene que superar el umbral, `config.MIN_F1_TO_PROMOTE` (0.65, o
el valor de la variable de entorno `PROJECT_TAGGER_MIN_F1` si está definida),
y, frente a un champion, no puede
quedar por debajo de él en las filas nuevas, o en el split entero cuando hay
demasiado pocas. Un `@task.branch` devuelve el `task_id` que hay que ejecutar; la otra
rama queda **skipped**, no failed. Un candidato que no llega al umbral, o que
pierde contra el champion, es un desenlace normal: el run tiene éxito y
producción se queda como estaba.

Después, las dos ramas del gate y la task que las une. Añade
`approve_promotion`, `promote_model` y dos `EmptyOperator`,
`keep_current_model` y `done`:

```python dags/04_mlops.py
    approve_promotion = ApprovalOperator(
        task_id="approve_promotion",
        subject="Promote the new tag classifier?",
        body=(
            "The new version passed promotion_gate.\n\n"
            "{{ ti.xcom_pull(task_ids='summarize_candidate') }}\n\n"
            "Approve to move `champion` to it."
        ),
        defaults="Approve",
        response_timeout=timedelta(seconds=60),
    )

    @task(outlets=[PRODUCTION_MODEL])
    def promote_model(
        trained: dict[str, Any], *, outlet_events: OutletEventAccessorsProtocol
    ) -> dict[str, Any]:
        champion = registry.promote(trained["version"])
        outlet_events[PRODUCTION_MODEL].extra = {
            "version": champion["version"],
            "f1": champion["metrics"]["f1"],
            "previous_version": champion["previous_version"],
        }
        print(f"champion -> version {champion['version']} (was {champion['previous_version']})")
        return champion

    keep_current_model = EmptyOperator(task_id="keep_current_model")
    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")
```

Un modelo que el gate deja pasar todavía espera a una persona.
`approve_promotion` es una task **human-in-the-loop**, una task que espera a
que una persona responda. `ApprovalOperator` la aparca en `awaiting_input` y
muestra un aviso en la UI, en la pestaña **Required Actions** de la task
instance y bajo Browse → **Required Actions** (§5). Con **Approve**, corre
`promote_model`; con **Reject**, queda skipped y producción se queda como
estaba. `defaults` es la respuesta que se aplica si nadie responde antes de
`response_timeout`: sesenta segundos aquí para que los runs de la lección
terminen; dale horas en producción. `body` es el texto del aviso, en Markdown,
y es un template field (§6): `ti.xcom_pull` mete en él el texto de
`summarize_candidate` cuando la task corre. El gate comprueba los números para
los que tiene una regla; la persona comprueba lo que las reglas no ven, como un
slice que ha caído, con ese resumen y la versión nueva en MLflow delante.
Airflow registra la respuesta: la
decisión, cuándo, y *quién*. Un run que agotó el timeout y aplicó sus
`defaults` guarda la decisión con el "quién" vacío, que es cómo distingues
"una persona aprobó esto" de "no había nadie mirando".

`promote_model` mueve el alias `champion` y
emite el asset propio de `mlops`, `production_model`, que todavía no consume nadie.
Si apuntas el `schedule` de un Dag hacia él, la arista aparece en el grafo de
Assets.

Las **trigger rules** son la forma en que una task declara qué tienen que haber
hecho sus tasks aguas arriba antes de que ella corra, y el branching es donde
necesitas una por primera vez. El join, `done`, necesita
`none_failed_min_one_success`, porque el `all_success` por defecto lo saltaría
en cuanto cualquiera de las ramas quedara skipped. Las útiles: `all_success` (por
defecto), `all_done` (corre pasara lo que pasara aguas arriba — tasks de
limpieza), `none_failed_min_one_success` (joins después de un branch),
`none_failed` (permite skips pero no fallos), `one_success`, `one_failed`
(caminos de alerta), `all_skipped` y `always`.

Termina el archivo conectando las tasks:

```python dags/04_mlops.py
    paths = prepare_data()
    approved_data_gate = validate_training_data(paths)
    study = create_study()
    results = run_trials.partial(study_name=study, paths=paths).expand(
        n_trials=trial_budget()
    )
    approved_data_gate >> results

    trained = train_best(best_params(study, results), paths)
    comparison = compare_with_champion(trained, paths)
    summary = summarize_candidate(trained, comparison)
    decision = promotion_gate(trained, comparison)

    [summary, decision] >> approve_promotion >> promote_model(trained) >> done
    decision >> keep_current_model >> done


mlops()
```

`approved_data_gate >> results` coloca `validate_training_data` aguas arriba
de *todas* las instancias mapeadas de `run_trials`, sin llevar datos: solo fija
el orden. Ningún trial arranca sobre datos que no han pasado sus
comprobaciones. `promotion_gate` recibe los resultados de `train_best` y de
`compare_with_champion`, que a su vez espera a `train_best`, así que las tres
corren en ese orden; `summarize_candidate` recibe los mismos dos, así que corre
a la vez que `promotion_gate`. `[summary, decision] >> approve_promotion` es un
fan-in (§9): `approve_promotion` espera a los dos. La arista desde `summary` es
del mismo tipo que `approved_data_gate >> results`: `approve_promotion` lee el
resumen a través de un template, no de un argumento, así que Airflow no ve la
dependencia por sí solo, y sin esta arista el prompt podría construirse antes
de que el resumen exista. La arista desde `decision`, que sigue hasta
`promote_model` y `done`, es la primera rama de `promotion_gate`; la última
línea es la segunda, y `done` las une.

### Comprueba las conexiones

Guárdalo. Ya has escrito los dos Dags del pipeline, `dataops` y `mlops`.
Airflow los enlaza en cuanto analiza los dos archivos, sin ejecutar ninguno:
registra qué tarea produce `labeled_projects` y qué Dag espera por él. Pide los
detalles de ese asset:

```bash
docker compose exec airflow-scheduler airflow assets details --name labeled_projects -o plain
```

**Comprobación.** Entre las filas:

```
scheduled_dags    [{'dag_id': 'mlops', ...}]
producing_tasks   [{'dag_id': 'dataops', 'task_id': 'validate_transformed', ...}]
```

`producing_tasks` nombra la task exacta que lo emite; `scheduled_dags` nombra
qué arrancará cuando lo haga. Ninguno de los dos Dags menciona al otro en ninguna
parte. La página Assets dibuja ahora el grafo entero, `dataops →
labeled_projects → mlops → production_model`.

La página del propio `mlops` puede mostrar las mismas conexiones. Ábrela, cambia
a **Graph** y ve cambiando **Options → Dependencies** (§5):

- **Only tasks** — las catorce tasks que acabas de escribir, nada más.
- **External conditions** — las mismas tasks, más `labeled_projects` entrando
  en el Dag y `production_model` saliendo de `promote_model`. La flecha de
  `labeled_projects` llega solo a la primera task, pero arranca el Dag entero.
- **All Dag Dependencies** — la cadena que dibuja la página Assets: `dataops`
  con los assets que produce, luego `mlops` con sus tasks dentro, y luego
  `production_model`.

### Mira cómo se dispara la cadena entera

Esta es la recompensa de la §8. Quita la pausa primero al consumidor, `mlops`,
para que esté a la escucha, y después al Dag del productor, `dataops`:

```bash
docker compose exec airflow-scheduler airflow dags unpause mlops
docker compose exec airflow-scheduler airflow dags unpause dataops
```

Con la primera línea no pasa nada: el schedule de `mlops` es un asset, y no ha
llegado ningún evento. La segunda línea lo arranca todo sin ningún trigger,
por la regla de la §7: `dataops` tiene un cron y un `start_date` de 2025, así
que en el momento en que se le quita la pausa el scheduler crea el run que se
perdió, el de las 03:00 UTC más recientes, y lo arranca. Espera unos minutos.
Cerca del final, el run de `mlops` se detiene en `approve_promotion`: abre el
run en la UI, haz clic en `approve_promotion` y responde al aviso en su
pestaña **Required Actions**, debajo de los números de `summarize_candidate`,
o déjalo y el default lo aprueba a los 60 segundos. Luego:

```bash
docker compose exec airflow-scheduler airflow dags list-runs dataops -o plain
docker compose exec airflow-scheduler airflow dags list-runs mlops -o plain
```

**Comprobación.** `dataops` tiene un run cuyo id empieza por `scheduled__` y
termina en `T03:00:00+00:00` — la fecha de hoy, o la de ayer si aún no son las
03:00 UTC — con eso como `logical_date` y un `start_date` de hace unos minutos.
(El run `manual__` del `airflow dags test` de la §9 también está.) Y `mlops` tiene
un run que nadie disparó: id que empieza por `asset_triggered__`, `running` si
has sido rápido, `success` en cuanto terminen sus trials y el modelo final, con
una `logical_date` **vacía**: los runs disparados por asset no tienen logical
date, igual que un run disparado desde la CLI sin ella (§7). En la UI,
**Assets** muestra la arista que lo causó.

Ten claro de dónde viene la espera. Casi toda la ponen las herramientas: los
dos syncs de Airbyte en `dataops`, y luego veinte trials y un entrenamiento
final en `mlops`, y hasta un minuto en `approve_promotion`. La parte de
Airflow es pequeña: un bucle del scheduler para
crear el run de `dataops` tras quitar la pausa, y uno más para crear el run de `mlops`
cuando se registra el asset event (§3, §8). Si tras unos minutos no ha aparecido
nada, comprueba que ambos Dags están sin pausa y que el run de `dataops` tuvo
éxito — un `validate_transformed` fallido no emite evento, por diseño.

Ahora las dos oleadas de `run_trials`. Abre ese run de `mlops` en la UI, haz
clic en `run_trials` y lee su pestaña **Task Instances**: cuatro filas, map
index del 0 al 3, cada una con su hora de inicio y de fin. Dos arrancan
juntas; cada una de las otras dos arranca solo cuando ha terminado una del
primer par. La vista *Show Gantt* (§5), disponible ahora que hay un run
seleccionado, dibuja lo mismo como dos oleadas de barras. En el **Graph**, prueba
el filtro **Min. map index** (§5): ponlo en 3 y `run_trials` se sigue viendo igual
mientras todos los demás nodos se atenúan, porque es la única mapped task y
tiene una instancia con map index 3; ponlo en 4 y también se atenúa. O léelo
desde la CLI:

```bash
docker compose exec airflow-scheduler airflow tasks states-for-dag-run mlops <the asset_triggered__ run id> -o plain
```

Cuatro filas de `run_trials` con `map_index` de 0 a 3; la 2 y la 3 arrancan
cada una después de que haya terminado una de la 0 y la 1. Y
`keep_current_model` está `skipped` mientras `approve_promotion` y
`promote_model` están `success`: `promotion_gate` eligió la rama de
`approve_promotion`, porque el f1 del modelo final superó el umbral y no había
modelo en producción al que batir (`compare_with_champion` escribió `no
champion yet`), y la aprobación vino de ti o del default.

Después mira lo que el run ha dejado en MLflow, en <http://localhost:5000>:

**Comprobación.** El experiment `project_tagger` muestra dos runs:
`project_tagger-asset_triggered__…`, el study, y `final-asset_triggered__…`.
Despliega el run del study para ver sus veinte hijos, de `…-trial-0` a
`-trial-19`, y ordena por la columna `f1` para ver la búsqueda en acción. Abre
el run del study: `best_f1` es la puntuación del mejor trial, y el tag
`best_trial_run_id` lo identifica. Abre el run final: sus parameters son los del mejor trial,
sus metrics incluyen el f1 por tag y por slice, sus tags incluyen
`snapshot_path`, y en **Artifacts** están `metrics.json`, `split_ids.json` y el
modelo. En **Models**, `project_tagger` tiene la versión 1, con el alias
`champion`.

> **Pruébalo, una vez más a mano:**
>
> ```bash
> docker compose exec airflow-scheduler airflow dags trigger dataops
> ```
>
> Esta vez, un run `manual__`. Cuando tenga éxito, habrá un evento más, un run
> `asset_triggered__` más de `mlops` y una versión más del modelo en MLflow.
> Cada éxito del productor emite un evento, sin importar cómo se arrancó el
> productor — y el consumidor se ejecuta con él (los eventos que llegan
> mientras `mlops` está ocupado comparten su siguiente run, §8). Que la versión
> 2 pase a ser `champion` depende de su f1 frente al de la versión 1, los dos
> sobre el split de test de este run. Mismos datos, así que el mismo split (los
> `test.parquet` de los dos runs tienen los mismos ids), y ninguna fila nueva:
> `compare_with_champion` escribe `all test rows` y `old test rows`, las mismas
> filas, y `promotion_gate` decide con el split entero, `… on the same … test
> rows`. Otra búsqueda, así que puede superar el gate o no. Si el gate la deja pasar,
> `approve_promotion` vuelve a esperarte.

> **Pruébalo: la otra rama.** Pon el listón imposiblemente alto y mira cómo
> `promotion_gate` elige la rama de `keep_current_model`. El umbral se lee de
> una variable de entorno dentro del proceso de la propia task, y `airflow dags test` ejecuta las tasks en su
> propio proceso, así que pásale la variable a ese comando con `-e`:
>
> ```bash
> docker compose exec -e PROJECT_TAGGER_MIN_F1=0.99 airflow-scheduler airflow dags test mlops
> ```
>
> `promotion_gate` escribe en el log `below threshold 0.99 - not promoting`,
> `approve_promotion` y `promote_model` quedan skipped, así que no se le
> pregunta a nadie, `keep_current_model` corre, y el Dag run termina en
> success igualmente. En MLflow la versión nueva queda registrada, y `champion` no se
> ha movido.

¿Atascado en algún punto? [`solutions/04_mlops.py`](../../solutions/04_mlops.py).

---

← [9. El Dag de DataOps](09-el-dag-de-dataops.md) · [Índice](README.md) · [11. Esperar: sensors, deferrable tasks, eventos](11-esperar-sensors-deferrable-tasks-eventos.md) →
