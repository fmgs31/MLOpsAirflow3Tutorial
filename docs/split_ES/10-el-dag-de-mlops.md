## 10. El Dag de MLOps

`dags/04_mlops.py` — sin schedule de tiempo: lo dispara el asset.

```
prepare_data ─► validate_training_data ─┐
                            candidates ─┴─► train_candidate×N ─► select_best
─► evaluate_best ─► promotion_gate ─► promote_model ──────► done
                                  └─► keep_current_model ─┘
```

| Task | Opción en producción |
|---|---|
| el trigger por asset + `prepare_data` | el cliente de tu warehouse — o, como aquí, deja que el asset event lleve la ruta de los datos |
| `validate_training_data` | Great Expectations / Pandera / Soda, más comprobaciones del split |
| `candidates` + `train_candidate` (uno por conjunto de hiperparámetros) | Optuna / Ray Tune / un servicio que reparte la búsqueda de hiperparámetros |
| `select_best` + `evaluate_best` | tu experiment tracker (MLflow, W&B) |
| `promotion_gate` → `promote_model` | el paso a producción en tu model registry (MLflow, por ejemplo) |

### Constrúyelo

Funciones nuevas de la carga de trabajo: `data.split(df)` devuelve un par
`(train, test)` con la misma proporción de cada tag en los dos;
`validate.check_split(train, test)` es una segunda suite, sobre el split;
`train_module.SEARCH_SPACE` es una lista de dicts de hiperparámetros y
`train_module.train_from_files(hparams)` ajusta un modelo, devolviendo
`(model, metrics)`; `registry.save_run`, `load_metrics`, `promote` y
`get_production` son el *model registry* del repo, en sistema de archivos: guarda
cada modelo entrenado con sus métricas y apunta cuál está en producción;
`config.MIN_F1_TO_PROMOTE` es el f1 mínimo para pasar un modelo a producción
(0.65, o el valor de `TAGIFAI_MIN_F1` si está definida).

Crea `dags/04_mlops.py`:

```python dags/04_mlops.py
"""Lesson 4 - the MLOps Dag: asset-triggered training, tuning, and promotion."""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException

from tagifai import config, data, registry, train as train_module, validate
from tagifai.assets import LABELED_PROJECTS, PRODUCTION_MODEL

DEFAULT_ARGS = {
    "owner": "ml-engineering",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=2),
}


@dag(
    dag_id="mlops",
    schedule=[LABELED_PROJECTS],
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "mlops"],
    doc_md=__doc__,
)
def mlops():
```

`schedule=[LABELED_PROJECTS]` es lo esencial: una lista de assets, y el Dag
corre cuando se actualizan. Varios assets en la lista significan *todos*;
`AssetAny(a, b)` significa cualquiera.

**Valida antes de entrenar.** `dataops` validó `labeled_projects` a su salida del
warehouse, y este Dag lo valida otra vez a la entrada. Eso no es redundante: fue
otro Dag run, posiblemente hace horas, este run puede haberse disparado a mano, y
lo que el modelo ajusta es el snapshot que hay en disco *ahora*. Añade las dos
primeras tasks:

```python dags/04_mlops.py
    @task
    def prepare_data(*, triggering_asset_events) -> dict[str, int]:
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")
        df = data.load_labeled()
        train_df, test_df = data.split(df)
        return {"num_train": len(train_df), "num_test": len(test_df)}

    @task
    def validate_training_data(split_sizes: dict[str, int]) -> dict[str, Any]:
        df = data.load_labeled()
        table = validate.run_suite(df, "labeled_projects")
        split = validate.check_split(*data.split(df))
        print(f"{table.summary()} {split.summary()}")
        if not (table.passed and split.passed):
            raise AirflowFailException(
                f"Training snapshot failed validation: {table.failures + split.failures}"
            )
        return {
            "rows": len(df),
            "split_sizes": split_sizes,
            "warnings": [r["expectation"] for r in table.failures + split.failures],
            "results": table.results + split.results,
        }
```

`triggering_asset_events` se inyecta por nombre — los eventos que causaron este
run, indexados por asset, cada uno con el `extra` que adjuntó el productor. En un
run que arranques a mano está vacío. `validate_training_data` vuelve a leer la
tabla en vez de recibirla de `prepare_data`: los XComs llevan metadatos, nunca
payloads. La segunda mitad de su comprobación es una que una suite a nivel de
tabla no puede hacer en absoluto — mira el **split**:

```python
# src/tagifai/validate.py — check_split()
("split_is_non_empty",          ...,  critical)
("classes_in_both_splits",      ...,  critical)   # a class only in train
("min_rows_per_class:20",       ...,  critical)   # 3 rows of a class in train
("no_id_leakage",               ...,  critical)   # the same project in both
("test_fraction_between_0.1_and_0.4", ..., non-critical)
```

Cada problema de esos comentarios — una clase solo en train, tres filas de una
clase, el mismo proyecto en los dos splits — pasa una suite a nivel de columna
sin ningún aviso, y aun así produce un modelo cuyo f1 sobre el conjunto de test
no significa nada. Ese f1 es exactamente lo que el promotion gate compara contra producción.

**El dynamic task mapping** crea, mientras corre el Dag, una task instance por
conjunto de hiperparámetros, y una task posterior reúne todos los resultados.
Tres tasks:

```python dags/04_mlops.py
    @task
    def candidates() -> list[dict[str, Any]]:
        return train_module.SEARCH_SPACE

    @task(max_active_tis_per_dag=2)
    def train_candidate(hparams: dict[str, Any], **context) -> dict[str, Any]:
        config.ensure_dirs()
        run_id = f"{context['run_id']}__{context['ti'].map_index}".replace(":", "-").replace("+", "-")
        model, metrics = train_module.train_from_files(hparams)
        registry.save_run(run_id, model, metrics)
        print(f"run={run_id} hparams={hparams} f1={metrics['f1']:.4f}")
        return {"run_id": run_id, "f1": metrics["f1"], "hparams": hparams}

    @task
    def select_best(results: list[dict[str, Any]]) -> dict[str, Any]:
        best = max(results, key=lambda r: r["f1"])
        print(f"best={best['run_id']} f1={best['f1']:.4f} of {len(results)} candidates")
        return best
```

`candidates` devuelve una lista; en el cableado de abajo,
`train_candidate.expand(hparams=candidates())` crea una task instance por
elemento cuando corre el Dag — cuatro aquí, una por conjunto de
hiperparámetros — y cada una se reintenta, se loguea y se inspecciona por su
cuenta. `context['ti'].map_index` le dice a la task cuál de las cuatro es
(0 a 3). La task que reúne los resultados, `select_best`, simplemente recibe
una `list` con los cuatro valores devueltos. Fíjate en el valor de retorno de
`train_candidate`: un run id, un f1 y los hparams — pequeño y serializable a
JSON. El modelo en sí se queda en el registry.

**Cuántas de las cuatro entrenan a la vez es una decisión, y está escrita en el
decorador de `train_candidate`.** Nada en `.expand()` dice "en paralelo"; dice "estas son cuatro task
instances separadas". Que se solapen o no es asunto del executor, acotado por el
más estrecho de los límites de la §3 — y `max_active_tis_per_dag=2` en esa task es
deliberadamente el más estrecho aquí. Así que el fan-out corre **de dos en dos,
en dos oleadas**. Sube el tope y las cuatro se solapan; quítalo y `[core]
max_active_tasks_per_dag` (16) es el siguiente límite. Un tope en una task de
entrenamiento no es un ajuste de juguete: cuatro modelos ajustándose a la vez son
cuatro copias de los datos en memoria. Cuando el recurso limitado es compartido
— por ejemplo, una base de datos que solo acepta unas pocas conexiones —, un
`pool` (§3) pone el mismo tipo de tope a todas las tasks y Dags que lo usan.

> **Ojo:** el parámetro se llama `hparams`, no `params`. Un `@task` recibe
> claves del contexto por nombre de parámetro (§4), y `params` está reservado — un argumento
> de task llamado `params` recibe los params del Dag, no tus datos.

El **promotion gate** decide si el mejor candidato pasa a producción. A
diferencia de los gates de la §9, no falla la task cuando la respuesta es no: es
un *branch*, una task que elige cuál de dos ramas del Dag sigue. Añade la
evaluación, el gate, la promoción y los dos `EmptyOperator` que el branch
necesita:

```python dags/04_mlops.py
    @task
    def evaluate_best(best: dict[str, Any]) -> dict[str, Any]:
        metrics = registry.load_metrics(best["run_id"])
        for name, slice_metrics in metrics.get("slices", {}).items():
            print(f"slice {name}: f1={slice_metrics['f1']:.4f} n={slice_metrics['support']}")
        return {"run_id": best["run_id"], "metrics": metrics}

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
    def promote_model(evaluation: dict[str, Any], *, outlet_events) -> dict[str, Any]:
        pointer = registry.promote(evaluation["run_id"])
        outlet_events[PRODUCTION_MODEL].extra = {
            "run_id": pointer["run_id"],
            "f1": pointer["metrics"]["f1"],
            "previous_run_id": pointer["previous_run_id"],
        }
        print(f"promoted {pointer['run_id']} (was {pointer['previous_run_id']})")
        return pointer

    keep_current_model = EmptyOperator(task_id="keep_current_model")
    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")
```

Un `@task.branch` devuelve el `task_id` que hay que ejecutar; la otra rama queda
**skipped**, no failed. Un candidato que no llega al umbral es un desenlace
normal — el run tiene éxito y producción se queda como estaba. `promote_model`
emite el segundo asset, `production_model`, que todavía no consume nadie; apunta
el `schedule` de un Dag hacia él y la arista aparece en el grafo de Assets.

Las **trigger rules** son cómo una task declara qué tienen que haber hecho sus
tasks aguas arriba antes de que ella corra, y el
branching es donde necesitas una por primera vez. El join, `done`, necesita
`none_failed_min_one_success`, porque el `all_success` por defecto lo saltaría en
cuanto cualquiera de las ramas quedara skipped. Las útiles: `all_success` (por
defecto), `all_done` (corre pasara lo que pasara aguas arriba — tasks de
limpieza), `none_failed_min_one_success` (joins después de un branch),
`none_failed` (permite skips pero no fallos), `one_success`, `one_failed`
(caminos de alerta), `all_skipped` y `always`.

Termina el archivo con el cableado:

```python dags/04_mlops.py
    snapshot = validate_training_data(prepare_data())
    results = train_candidate.expand(hparams=candidates())
    snapshot >> results

    evaluation = evaluate_best(select_best(results))
    gate = promotion_gate(evaluation)

    gate >> promote_model(evaluation) >> done
    gate >> keep_current_model >> done


mlops()
```

`snapshot >> results` cablea el gate de validación aguas arriba de *todas* las
instancias mapeadas, sin llevar datos: solo fija el orden. Las dos líneas `>>`
del gate le dan al branch sus dos ramas y las unen en `done`.

### Mira el cableado

Guárdalo. Ya has escrito las dos mitades del pipeline, y antes de ejecutar nada
puedes preguntarle a Airflow qué sabe:

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

- **Only tasks** — las diez tasks que acabas de escribir, nada más.
- **External conditions** — las mismas tasks, más `labeled_projects` entrando
  en el Dag y `production_model` saliendo de `promote_model`.
- **All Dag Dependencies** — la cadena que dibuja la página Assets: `dataops`
  con los dos assets que produce, luego `mlops` con sus tasks dentro, y luego
  `production_model`.

### Mira cómo dispara la cadena entera

Esta es la recompensa de la §8. Quita la pausa primero al consumidor, para que esté
escuchando, y después al productor:

```bash
docker compose exec airflow-scheduler airflow dags unpause mlops
docker compose exec airflow-scheduler airflow dags unpause dataops
```

Con la primera línea no pasa nada: `mlops` solo corre con un asset event, y no
ha llegado ninguno. La segunda línea basta para arrancarlo todo, y no has disparado
nada — es la regla de la §7: `dataops` tiene un cron y un `start_date` de 2025,
así que en el momento en que se le quita la pausa el scheduler crea el run de
las 03:00 UTC más recientes que se perdió, y lo arranca. Dale como medio minuto,
y luego:

```bash
docker compose exec airflow-scheduler airflow dags list-runs dataops -o plain
docker compose exec airflow-scheduler airflow dags list-runs mlops -o plain
```

**Comprobación.** `dataops` tiene un run cuyo id empieza por `scheduled__` y
termina en `T03:00:00+00:00` — la fecha de hoy, o la de ayer si aún no son las
03:00 UTC — con eso como `logical_date` y un `start_date` de hace un momento.
(El run `manual__` del `airflow dags test` de la §9 también está.) Y `mlops` tiene
un run que nadie disparó: id que empieza por `asset_triggered__`, `running` si
has sido rápido, `success` en cuanto sus cuatro candidatos hayan entrenado, con
una `logical_date` **vacía** — los runs disparados por asset no tienen logical
date, igual que un run disparado desde la CLI sin ella (§7). En la UI, abre **Assets** y verás
la arista que lo causó.

Ten claro de dónde viene la espera. Casi toda es `dataops` corriendo sus cinco tasks
— `extract` descarga el archivo de origen, así que esa parte depende de tu
conexión. La parte de Airflow es pequeña: un bucle del scheduler para crear el
run de `dataops` tras quitar la pausa, y uno más para encolar `mlops` cuando
se registra el asset event (§3). Si tras un minuto no ha aparecido nada, comprueba
que ambos Dags están sin pausa y que el run de `dataops` tuvo éxito — un
`validate_transformed` fallido no emite evento, por diseño.

Ahora las dos oleadas. Abre ese run de `mlops` en la UI, haz clic en
`train_candidate` y lee su pestaña **Task Instances** — cuatro filas, map index
del 0 al 3, cada una con su hora de inicio y de fin. Dos arrancan juntas; cada una de
las otras dos arranca solo cuando ha terminado una del primer par. La vista *Show
Gantt* (§5), disponible ahora que hay un run seleccionado, dibuja lo mismo como
dos oleadas de barras. En el **Graph**, las cuatro
son un único nodo colapsado hasta que subes el filtro **Min. map index** (§5). O
léelo desde la CLI:

```bash
docker compose exec airflow-scheduler airflow tasks states-for-dag-run mlops <the asset_triggered__ run id> -o plain
```

Cuatro filas de `train_candidate` con `map_index` de 0 a 3; la 2 y la 3 arrancan
cada una después de que haya terminado una de la 0 y la 1. Y
`keep_current_model` está `skipped` mientras `promote_model` está `success`: el
gate eligió la rama de promoción, porque no había modelo en producción al que
batir.

> **Pruébalo, una vez más a mano:**
>
> ```bash
> docker compose exec airflow-scheduler airflow dags trigger dataops
> ```
>
> Esta vez, un run `manual__`. Cuando tenga éxito, un evento más, un run
> `asset_triggered__` más de `mlops`. Cada éxito del productor emite un evento,
> se haya arrancado el productor como se haya arrancado — y el consumidor corre
> con él (los eventos que llegan mientras `mlops` está ocupado comparten su
> siguiente run, §8).

> **Pruébalo, la otra rama.** Pon el listón imposiblemente alto y mira cómo el
> gate elige la otra rama. El umbral se lee de una variable de entorno dentro
> del proceso de la propia task, y `airflow dags test` ejecuta las tasks en su
> propio proceso, así que pásale la variable a ese comando con `-e`:
>
> ```bash
> docker compose exec -e TAGIFAI_MIN_F1=0.99 airflow-scheduler airflow dags test mlops
> ```
>
> `promotion_gate` loguea `below threshold 0.99 - not promoting`, `promote_model`
> queda skipped, `keep_current_model` corre, y el Dag run termina en success
> igualmente.

¿Atascado en algún punto? [`solutions/04_mlops.py`](../../solutions/04_mlops.py).

---

← [9. El Dag de DataOps](09-el-dag-de-dataops.md) · [Índice](README.md) · [11. Esperar: sensors, deferrable tasks, eventos](11-esperar-sensors-deferrable-tasks-eventos.md) →
