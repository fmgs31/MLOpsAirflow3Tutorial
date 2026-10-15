## 10. El Dag de MLOps

`dags/04_mlops.py` no tiene schedule de tiempo: lo dispara cada asset event de
`labeled_projects`.

```
prepare_data ─► validate_training_data ─┐
create_study ───────────────────────────┤
trial_budget ───────────────────────────┴─► run_trials×N ─► best_params ─► train_best
─► evaluate_best ─► promotion_gate ─► promote_model ──────► done
                                  └─► keep_current_model ─┘
```

| Task | Herramienta | Qué hace |
|---|---|---|
| `prepare_data` | BigQuery | copia `labeled_projects` a un archivo que este run usará de principio a fin |
| `validate_training_data` | Great Expectations | la suite de la §9 otra vez, más comprobaciones del split train/test |
| `create_study`, `trial_budget`, `run_trials`×N, `best_params` | **Optuna** | busca los hiperparámetros, con el trabajo repartido entre varias tasks |
| `train_best`, `evaluate_best` | tracking de **MLflow** | entrena el modelo final y lo registra: parámetros, métricas, el archivo del modelo |
| `promotion_gate` → `promote_model` | Model Registry de **MLflow** | decide si el modelo nuevo pasa a producción |

### El stack de ML

Dos herramientas más. Las dos corren en el stack de compose y no necesitan
cuenta.

**Optuna** busca hiperparámetros. Un **study** es una búsqueda, con un objetivo
(aquí: maximizar el f1). Un **trial** es un intento: Optuna elige un valor para
cada hiperparámetro, el código entrena y puntúa un modelo con ellos, y la
puntuación se guarda en el study. Optuna no prueba todas las combinaciones: usa las
puntuaciones obtenidas hasta el momento para elegir los siguientes valores. El
study se guarda en
una base de datos, aquí la base de datos `optuna` del Postgres del stack, así
que varios procesos pueden añadir trials al mismo study a la vez. Eso es lo
que permite a Airflow repartir la búsqueda entre varias tasks.

**MLflow** registra experimentos. Cada entrenamiento es un **run**, con sus
**parameters** (los hiperparámetros usados), sus **metrics** (f1,
precision...) y sus **artifacts** (archivos: aquí un `metrics.json` y el propio
modelo). Los runs pertenecen a un **experiment**, `project_tagger`. El **Model
Registry**, también en MLflow, da a un modelo un nombre y **versiones**
numeradas. Un **alias** es una etiqueta que puedes mover de una versión a otra.
Este proyecto usa dos:

```
champion             la versión en producción: la que carga predict.py
previous_champion    la versión a la que apuntaba champion antes de su último cambio
```

Promocionar un modelo es mover `champion`; hacer rollback es devolverlo a donde
estaba. No se copia ningún archivo: quien consume el modelo pide
`models:/project_tagger@champion` y recibe la versión a la que apunte el alias.

El servidor de MLflow es el servicio `mlflow` del archivo de compose. Guarda
los runs y el registry en un archivo SQLite y los archivos de los modelos en
una carpeta propia, en un volumen de Docker, y las tasks lo encuentran en
`http://mlflow:5000` (`MLFLOW_TRACKING_URI`). Abre su UI:

**Comprobación.** <http://localhost:5000> muestra MLflow con un experiment,
`Default`, y ningún run. `project_tagger` aparece con el primer run.

### Cómo se hablan las piezas

Cada columna es un sistema, como en el diagrama de la §9:

```
 Airflow (Docker)            │ Postgres          │ MLflow (Docker)     │ BigQuery
 Dag mlops                   │ base de datos     │ localhost:5000      │ mlops_course
                             │ optuna            │                     │
─────────────────────────────┼───────────────────┼─────────────────────┼───────────────────
                             │                   │                     │
 1 prepare_data ◄──────────────────────────────────────────────────────── labeled_projects
   guarda un snapshot en     │                   │                     │
   data/snapshots/           │                   │                     │
 2 validate_training_data    │                   │                     │
   (GX, sobre el snapshot)   │                   │                     │
 3 create_study ──────────────► un study nuevo   │                     │
 4 run_trials ×N ─────────────► pide params,     │                     │
   cada task entrena unos    │  guarda scores    │                     │
   modelos y registra cada ───────────────────────► un run por trial   │
 5 best_params ◄─────────────── el mejor trial   │                     │
 6 train_best ────────────────────────────────────► run final, con     │
                             │                   │  el modelo;         │
                             │                   │  registrado como    │
                             │                   │  versión nueva      │
 7 evaluate_best ◄───────────────────────────────── su metrics.json    │
 8 promotion_gate ◄──────────────────────────────── f1 del champion    │
 9 promote_model ─────────────────────────────────► mueve champion     │
   (o keep_current_model)    │                   │  a la versión nueva │
```

BigQuery se lee una sola vez, en el paso 1; todas las tasks siguientes leen el
archivo del snapshot, así que el run entero trabaja con los mismos datos. En
MLflow queda un run por trial más el final, y el archivo del modelo solo se
sube con el run final.

### Constrúyelo

Funciones nuevas de la carga de trabajo: `data.save_snapshot(df, key)` escribe
un dataframe en `data/snapshots/<key>/` y `data.load_snapshot(fp)` lo vuelve a
leer; `data.preprocess(df)` limpia el texto y agrupa en `other` todo tag fuera
de las cuatro clases que aprende el modelo (`config.ACCEPTED_TAGS`); `data.split(df)` devuelve un par `(train, test)` con la misma
proporción de cada tag en los dos; `validate.check_split(train, test)` es una
segunda suite, sobre el split. En `train_module` (`train.py`): `create_study`,
`run_trials`, `best_trial` y `train_model`. En `registry`: `register`,
`load_metrics`, `get_production` y `promote`. `config.MIN_F1_TO_PROMOTE` es el
f1 mínimo para pasar un modelo a producción (0.65, o el valor de
`PROJECT_TAGGER_MIN_F1` si está definida).

Crea `dags/04_mlops.py`:

```python dags/04_mlops.py
"""Lesson 4 - the MLOps Dag: asset-triggered tuning, training, and promotion."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, Param, dag, task
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk.types import OutletEventAccessorsProtocol

from project_tagger import config, data, registry, train as train_module, validate
from project_tagger.assets import LABELED_PROJECTS, PRODUCTION_MODEL

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
corre cuando se actualizan. Varios assets en la lista significan *todos*;
`AssetAny(a, b)` significa cualquiera.

`params` declara el presupuesto de la búsqueda como dos params del Dag (§4):
20 trials en total, repartidos entre 4 tasks. `run_key` convierte un run id como
`manual__2026-02-01T03:00:00+00:00` en algo que se puede usar sin problema
como nombre de archivo y de study.

**Valida antes de entrenar.** `dataops` validó `labeled_projects` al
construirla, y este Dag la valida otra vez antes de entrenar con ella. Eso no
es redundante: fue otro Dag run, posiblemente hace horas, este run puede
haberse disparado a mano, y el modelo se entrena con los datos tal como están
*ahora*. Añade las dos primeras tasks:

```python dags/04_mlops.py
    @task
    def prepare_data(
        *, triggering_asset_events: Mapping[Asset, Sequence[Any]], run_id: str
    ) -> str:
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")
        config.ensure_dirs()
        fp = data.save_snapshot(data.load_labeled(), run_key(run_id))
        print(f"snapshot: {fp}")
        return str(fp)

    @task
    def validate_training_data(snapshot_fp: str) -> dict[str, Any]:
        df = data.load_snapshot(snapshot_fp)
        table = validate.run_suite(df, "labeled_projects")
        split = validate.check_split(*data.split(data.preprocess(df)))
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

`triggering_asset_events` funciona como en la §8 (de ahí los imports de
`Mapping`, `Sequence` y `Asset`); en un run que arranques a mano está vacío.
`run_id` es otra clave del contexto (§4), recibida por nombre.

`prepare_data` lee la tabla del warehouse una vez y la escribe en un
**snapshot**, un archivo con el nombre del run, y todas las tasks posteriores
leen ese archivo. La tabla puede cambiar mientras un run está en marcha (si un
run de `dataops` termina mientras tanto), y con el snapshot todas las tasks de
este run trabajan con las mismas filas, incluida una task que se reintenta una
hora después. La task devuelve la ruta del archivo: los XComs llevan un puntero a
los datos, no los datos (§6).

La segunda mitad de la comprobación de `validate_training_data` es algo que una
suite a nivel de tabla no puede hacer. Mira el **split**:

```python
# src/project_tagger/validate.py — check_split()
("split_is_non_empty",          ...,  critical)
("classes_in_both_splits",      ...,  critical)   # a class only in train
("min_rows_per_class:20",       ...,  critical)   # 3 rows of a class in train
("no_id_leakage",               ...,  critical)   # the same project in both
("test_fraction_between_0.1_and_0.4", ..., non-critical)
```

Cada problema de esos comentarios — una clase solo en train, tres filas de una
clase, el mismo proyecto en los dos splits — supera una suite a nivel de columna
sin ningún aviso, y aun así produce un modelo cuyo f1 sobre el conjunto de test
no significa nada. Ese f1 es exactamente lo que `promotion_gate`, más adelante
en esta sección, compara con el modelo en producción. Estas comprobaciones
abarcan dos dataframes, algo que una suite de GX no puede expresar (valida un
lote de datos cada vez), así que son Python normal, con resultados en el mismo
formato.

**Búsqueda distribuida.** Un study de Optuna por run, y después **dynamic
task mapping**: mientras corre el Dag, Airflow crea una task instance por cada
elemento de una lista, y cada una añade trials al mismo study. Cuatro tasks:

```python dags/04_mlops.py
    @task
    def create_study(*, run_id: str) -> str:
        return train_module.create_study(f"{config.REGISTERED_MODEL}-{run_key(run_id)}")

    @task
    def trial_budget(*, params: Mapping[str, Any]) -> list[int]:
        base, extra = divmod(params["n_trials"], params["n_workers"])
        return [base + (1 if i < extra else 0) for i in range(params["n_workers"]) if base or i < extra]

    @task(max_active_tis_per_dag=2)
    def run_trials(n_trials: int, study_name: str, snapshot_fp: str) -> list[dict[str, Any]]:
        train_df, _ = data.split(data.preprocess(data.load_snapshot(snapshot_fp)))
        results = train_module.run_trials(study_name, n_trials, train_df)
        print(f"{len(results)} trials, best here f1={max(r['f1'] for r in results):.4f}")
        return results

    @task
    def best_params(study_name: str, results: list[list[dict[str, Any]]]) -> dict[str, Any]:
        best = train_module.best_trial(study_name)
        print(f"best trial #{best['number']} f1={best['f1']:.4f} of {best['num_trials']}: {best['params']}")
        return best
```

`create_study` crea el study con `load_if_exists=True` (en `train.py`), así que
una task reintentada carga el study en vez de fallar porque ya existe.

`trial_budget` lee los params del Dag a través de la clave de contexto `params`
y reparte los trials entre `n_workers` tasks: 20 entre 4 da `[5, 5, 5, 5]`. Al
conectar las tasks, más abajo, `run_trials.partial(...).expand(n_trials=trial_budget())`
crea una instancia de `run_trials` por cada elemento de esa lista, cuando corre
el Dag. Cada instancia se reintenta, tiene su propio log y se inspecciona por
separado. `.partial()` recibe los argumentos que comparten todas las
instancias; `.expand()`, el que cambia en cada una.

`run_trials` puntúa cada trial sobre un split de validación sacado de los datos
de entrenamiento, y registra cada trial en MLflow como un run propio. El split
de test no se ve en ningún momento de la búsqueda, así que la puntuación de
test del modelo final sigue siendo una estimación honesta.

`best_params` le pide al study su mejor trial. Aun así recibe los resultados
de las instancias mapeadas: ese argumento es lo que la obliga a esperar a todas
las instancias de `run_trials`, y llega como una lista, un elemento por
instancia.

**Cuántas instancias de `run_trials` corren a la vez es una decisión, y está escrita en el
decorador de `run_trials`.** Nada en `.expand()` dice "en paralelo"; dice "estas son cuatro task
instances separadas". Que se solapen o no es asunto del executor, acotado por el
más restrictivo de los límites de la §3 — y `max_active_tis_per_dag=2` en esa task es
deliberadamente el más restrictivo aquí. Así que el fan-out corre **de dos en dos,
en dos oleadas**. Sube el tope y las cuatro se solapan; quítalo y `[core]
max_active_tasks_per_dag` (16) es el siguiente límite. Un tope en una task de
entrenamiento no es un ajuste de juguete: cuatro instancias a la vez son cuatro
copias de los datos en memoria. Cuando el recurso limitado es compartido
— por ejemplo, una base de datos que solo acepta unas pocas conexiones —, un
`pool` (§3) pone el mismo tipo de tope a todas las tasks y Dags que lo usan.

> **`params` está reservado.** Un `@task` recibe claves del contexto por nombre
> de parámetro (§4), y `params` es una de ellas: `trial_budget` recibe los
> params del Dag porque su argumento se llama `params`. Si por error llamas
> `params` a un argumento de datos, recibirá los params del Dag en vez de tus
> datos.

Ahora el modelo final, y el **promotion gate**, que decide si pasa a
producción. A diferencia de los gates de la §9, el gate no falla la task cuando
la respuesta es no: es un *branch*, una task que elige cuál de dos **ramas**
del Dag sigue. Añade `train_best`, `evaluate_best`, `promotion_gate`,
`promote_model` y los dos `EmptyOperator` que necesitan las ramas:

```python dags/04_mlops.py
    @task
    def train_best(best: dict[str, Any], snapshot_fp: str, *, run_id: str) -> dict[str, Any]:
        train_df, test_df = data.split(data.preprocess(data.load_snapshot(snapshot_fp)))
        trained = train_module.train_model(
            best["params"], train_df, test_df,
            run_name=f"final-{run_key(run_id)}",
            tags={"airflow_run_id": run_id},
        )
        version = registry.register(trained["model_uri"])
        print(f"run={trained['run_id']} version={version} f1={trained['metrics']['f1']:.4f}")
        return {"run_id": trained["run_id"], "version": version, "f1": trained["metrics"]["f1"]}

    @task
    def evaluate_best(trained: dict[str, Any]) -> dict[str, Any]:
        metrics = registry.load_metrics(trained["run_id"])
        for name, slice_metrics in metrics.get("slices", {}).items():
            print(f"slice {name}: f1={slice_metrics['f1']:.4f} n={slice_metrics['support']}")
        return {**trained, "metrics": metrics}

    @task.branch
    def promotion_gate(evaluation: dict[str, Any]) -> str:
        f1 = evaluation["metrics"]["f1"]
        threshold = config.MIN_F1_TO_PROMOTE
        current = registry.get_production()
        current_f1 = current["metrics"]["f1"] if current else 0.0

        if f1 < threshold:
            print(f"f1={f1:.4f} below threshold {threshold} - not promoting")
            return "keep_current_model"
        if f1 < current_f1:
            print(f"f1={f1:.4f} below production {current_f1:.4f} - not promoting")
            return "keep_current_model"
        return "promote_model"

    @task(outlets=[PRODUCTION_MODEL])
    def promote_model(
        evaluation: dict[str, Any], *, outlet_events: OutletEventAccessorsProtocol
    ) -> dict[str, Any]:
        champion = registry.promote(evaluation["version"])
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

`train_best` entrena el modelo final sobre todo el split de entrenamiento con
los hiperparámetros del mejor trial, lo puntúa sobre el split de test y lo
guarda en MLflow, con el archivo del modelo incluido. Después lo **registra** en
el registry: cada modelo final pasa a ser una versión nueva de `project_tagger`,
se promocione o no, así que un candidato rechazado sigue ahí para
inspeccionarlo. La task devuelve un run id, un número de versión y un f1:
poca cosa, y serializable a JSON. El modelo en sí se queda en MLflow.

`evaluate_best` vuelve a leer el `metrics.json` que registró el run e imprime
el f1 en cada **slice de evaluación**: un subgrupo del conjunto de test que
selecciona una regla de `evaluate.py` (textos cortos, y textos que mencionan
transformers, BERT o LLMs). No son las franjas de tiempo de la §7; sirven para
pillar un modelo cuyo f1 global parece bueno mientras falla con un tipo de
entrada.

Un `@task.branch` devuelve el `task_id` que hay que ejecutar; la otra rama queda
**skipped**, no failed. Un candidato que no llega al umbral, o que puntúa por
debajo del champion actual, es un desenlace normal: el run tiene éxito y
producción se queda como estaba. `promote_model` mueve el alias `champion` y
emite el segundo asset, `production_model`, que todavía no consume nadie;
apunta el `schedule` de un Dag hacia él y la arista aparece en el grafo de
Assets.

Las **trigger rules** son la forma en que una task declara qué tienen que haber
hecho sus tasks aguas arriba antes de que ella corra, y el
branching es donde necesitas una por primera vez. El join, `done`, necesita
`none_failed_min_one_success`, porque el `all_success` por defecto lo saltaría en
cuanto cualquiera de las ramas quedara skipped. Las útiles: `all_success` (por
defecto), `all_done` (corre pasara lo que pasara aguas arriba — tasks de
limpieza), `none_failed_min_one_success` (joins después de un branch),
`none_failed` (permite skips pero no fallos), `one_success`, `one_failed`
(caminos de alerta), `all_skipped` y `always`.

Termina el archivo conectando las tasks:

```python dags/04_mlops.py
    snapshot = prepare_data()
    gate = validate_training_data(snapshot)
    study = create_study()
    results = run_trials.partial(study_name=study, snapshot_fp=snapshot).expand(
        n_trials=trial_budget()
    )
    gate >> results

    evaluation = evaluate_best(train_best(best_params(study, results), snapshot))
    decision = promotion_gate(evaluation)

    decision >> promote_model(evaluation) >> done
    decision >> keep_current_model >> done


mlops()
```

`gate >> results` coloca `validate_training_data` aguas arriba de *todas* las
instancias mapeadas de `run_trials`, sin llevar datos: solo fija el orden.
Ningún trial arranca sobre datos que no han pasado sus comprobaciones. Las dos
líneas `decision >>` le dan a `promotion_gate` sus dos ramas y las unen en
`done`.

### Comprueba las conexiones

Guárdalo. Ya has escrito los dos Dags del pipeline, `dataops` y `mlops`, y
antes de ejecutar nada puedes preguntarle a Airflow qué sabe:

```bash
docker compose exec airflow-scheduler airflow assets details --name labeled_projects -o plain
```

**Comprobación.** Entre las filas:

```
scheduled_dags    [{'dag_id': 'mlops', ...}]
producing_tasks   [{'dag_id': 'dataops', 'task_id': 'validate_transformed', ...}]
```

`producing_tasks` nombra la task exacta que lo emite; `scheduled_dags` nombra qué
arrancará cuando lo haga. Ninguno de los dos Dags menciona al otro en ninguna
parte. La página Assets dibuja ahora el grafo entero, `dataops →
labeled_projects → mlops → production_model`.

La página del propio `mlops` puede mostrar las mismas conexiones. Ábrela, cambia
a **Graph** y ve cambiando **Options → Dependencies** (§5):

- **Only tasks** — las doce tasks que acabas de escribir, nada más.
- **External conditions** — las mismas tasks, más `labeled_projects` entrando
  en el Dag y `production_model` saliendo de `promote_model`.
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
llegado ningún evento. La segunda línea basta para arrancarlo todo, y no has disparado
nada — es la regla de la §7: `dataops` tiene un cron y un `start_date` de 2025,
así que en el momento en que se le quita la pausa el scheduler crea el run de
las 03:00 UTC más recientes que se perdió, y lo arranca. Espera unos minutos, y
luego:

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
una `logical_date` **vacía** — los runs disparados por asset no tienen logical
date, igual que un run disparado desde la CLI sin ella (§7). En la UI, abre **Assets** y verás
la arista que lo causó.

Ten claro de dónde viene la espera. Casi toda la ponen las herramientas: los
dos syncs de Airbyte en `dataops`, y luego veinte trials y un entrenamiento
final en `mlops`. La parte de Airflow es pequeña: un bucle del scheduler para crear el
run de `dataops` tras quitar la pausa, y uno más para crear el run de `mlops`
cuando se registra el asset event (§3, §8). Si tras unos minutos no ha aparecido
nada, comprueba que ambos Dags están sin pausa y que el run de `dataops` tuvo
éxito — un `validate_transformed` fallido no emite evento, por diseño.

Ahora las dos oleadas de `run_trials`. Abre ese run de `mlops` en la UI, haz clic en
`run_trials` y lee su pestaña **Task Instances** — cuatro filas, map index
del 0 al 3, cada una con su hora de inicio y de fin. Dos arrancan juntas; cada una de
las otras dos arranca solo cuando ha terminado una del primer par. La vista *Show
Gantt* (§5), disponible ahora que hay un run seleccionado, dibuja lo mismo como
dos oleadas de barras. En el **Graph**, las cuatro
son un único nodo colapsado hasta que subes el filtro **Min. map index** (§5). O
léelo desde la CLI:

```bash
docker compose exec airflow-scheduler airflow tasks states-for-dag-run mlops <the asset_triggered__ run id> -o plain
```

Cuatro filas de `run_trials` con `map_index` de 0 a 3; la 2 y la 3 arrancan
cada una después de que haya terminado una de la 0 y la 1. Y
`keep_current_model` está `skipped` mientras `promote_model` está `success`:
`promotion_gate` eligió la rama de `promote_model`, porque el f1 del modelo final
superó el umbral y no había modelo en producción al que batir.

Después mira lo que el run ha dejado en MLflow, en <http://localhost:5000>:

**Comprobación.** El experiment `project_tagger` tiene 21 runs: veinte
llamados de `project_tagger-asset_triggered__…-trial-0` a `-trial-19`, y uno
llamado `final-asset_triggered__…`. Ordena por la columna `f1` para ver la
búsqueda en acción. Abre el run final: sus parameters son los del mejor trial,
sus metrics incluyen el f1 por tag y por slice, y en **Artifacts** están
`metrics.json` y el modelo. En **Models**, `project_tagger` tiene la versión 1,
con el alias `champion`.

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
> 2 pase a ser `champion` depende de su f1 frente al de la versión 1: mismos
> datos, otra búsqueda, así que puede pasar o no, y el log de
> `promotion_gate` lo dice.

> **Pruébalo: la otra rama.** Pon el listón imposiblemente alto y mira cómo
> `promotion_gate` elige la rama de `keep_current_model`. El umbral se lee de una variable de entorno dentro
> del proceso de la propia task, y `airflow dags test` ejecuta las tasks en su
> propio proceso, así que pásale la variable a ese comando con `-e`:
>
> ```bash
> docker compose exec -e PROJECT_TAGGER_MIN_F1=0.99 airflow-scheduler airflow dags test mlops
> ```
>
> `promotion_gate` escribe en el log `below threshold 0.99 - not promoting`, `promote_model`
> queda skipped, `keep_current_model` corre, y el Dag run termina en success
> igualmente. En MLflow la versión nueva queda registrada, y `champion` no se
> ha movido.

¿Atascado en algún punto? [`solutions/04_mlops.py`](../../solutions/04_mlops.py).

---

← [9. El Dag de DataOps](09-el-dag-de-dataops.md) · [Índice](README.md) · [11. Esperar: sensors, deferrable tasks, eventos](11-esperar-sensors-deferrable-tasks-eventos.md) →
