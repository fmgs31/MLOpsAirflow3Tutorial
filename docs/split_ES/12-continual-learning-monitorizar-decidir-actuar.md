## 12. Continual learning: monitorizar, decidir, actuar

**Continual**, no continuo: el sistema se repite *con pausas*, y las pausas son
deliberadas. Un monitor dispara, se decide algo — por una persona donde importa —
y entonces corre el pipeline. El gate de aprobación que construye esta
sección, `request_retraining`, es esa decisión, convertida en una task del
Dag.

Lo que estás vigilando es el **drift**, de tres tipos:

| Drift | Qué se movió | Aquí lo detecta |
|---|---|---|
| **data drift** | la distribución de entrada — el texto en sí | `length_drift`: un test de Kolmogorov–Smirnov sobre el número de palabras de cada texto |
| **target drift** | la distribución de etiquetas — un tag domina, o aparece uno nuevo | `label_drift`: un test chi-cuadrado sobre los recuentos de tags, y `new_labels` |
| **concept drift** | la relación entre entrada y etiqueta — la misma entrada ahora merece otra etiqueta | solo la comprobación de rendimiento en vivo, `monitoring.production_performance()`, que puntúa el modelo en producción con filas etiquetadas recientemente: el f1 cae mientras las entradas parecen iguales |

Los dos tests vienen de **alibi-detect**, una librería de detección de drift.
Cada uno recibe una ventana de **referencia** (los proyectos más antiguos, con
los que se construyó el modelo) y una ventana **actual** (el cuarto más
reciente, por fecha de alta), y responde con un **p-valor**: la probabilidad de
ver una diferencia al menos así de grande si en realidad nada hubiera cambiado.
Un p-valor pequeño significa que es poco probable que la diferencia sea
casualidad. Por debajo de un umbral que eliges tú, por convención 0.05, el test
informa de drift. El test de Kolmogorov–Smirnov (KS) compara dos distribuciones
de un número; el chi-cuadrado compara dos conjuntos de recuentos por categoría.

Un evento de monitorización tiene cuatro respuestas sensatas, y
`dags/06_monitoring_and_retraining.py` las convierte en branches:

| Respuesta | Cuándo | Qué pasa |
|---|---|---|
| **continue** | drift y rendimiento dentro de los límites | nada; el modelo actual sigue en producción |
| **improve** | el f1 bajó, o se movió la distribución de entrada o la de etiquetas | preguntar a una persona, y luego disparar `mlops` |
| **inspect** | apareció una etiqueta no vista | parar y avisar a alguien — el espacio de etiquetas cambió |
| **rollback** | el rendimiento se desplomó | volver a apuntar producción al modelo anterior |

Cómo se hablan las piezas cuando se ejecuta el Dag. Cada columna es un
sistema, como en el diagrama de la §9:

```
 Airflow (Docker)               │ BigQuery          │ MLflow (Docker)
 Dag monitoring                 │ mlops_course      │ localhost:5000
────────────────────────────────┼───────────────────┼───────────────────────
                                │                   │
 1 prepare_windows ◄────────────── labeled_projects │
   escribe reference.csv y      │                   │
   current.csv en data/         │                   │
 2 detect_drift (alibi-detect,  │                   │
   en su propio virtualenv)     │                   │
   lee los dos archivos         │                   │
 3 check_performance ◄──────────────────────────────── el modelo champion
   lo puntúa sobre current.csv  │                   │
 4 decide: una rama de          │                   │
   continue_serving             │                   │
   inspect                      │                   │
   rollback ─────────────────────────────────────────► devuelve champion
   request_retraining:          │                   │  a previous_champion
     una persona aprueba en     │                   │
     la UI, y luego             │                   │
     trigger_retraining         │                   │
       │ arranca un run del     │                   │
       ▼                        │                   │
     Dag mlops (§10)            │                   │
```

### Constrúyelo

La carga de trabajo: `monitoring.prepare_windows(key)` lee
`labeled_projects`, la divide en las dos ventanas y escribe cada una en un
archivo, devolviendo sus rutas; `drift.detect_drift(reference_fp, current_fp,
p_val)` ejecuta los dos tests y devuelve `label_drift` y `length_drift` (cada
uno con `is_drift`, `p_val` y `distance`), `new_labels` y las dos
distribuciones de tags; `monitoring.production_performance(current_fp)` puntúa
al champion sobre la ventana actual y devuelve `f1_at_promotion` y `f1_now`, o
`None` cuando no hay nada promocionado; `registry.rollback()` devuelve
`champion` a `previous_champion` (§10).

Crea `dags/06_monitoring_and_retraining.py`. Los umbrales son **params** del
Dag, así que la política es visible en la UI, se puede sobrescribir por run, y
queda registrada con el run que la usó:

```python dags/06_monitoring_and_retraining.py
"""Lesson 6 - continual learning: monitor, decide, and act."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task

from project_tagger import config, monitoring, registry

DEFAULT_ARGS = {"owner": "ml-engineering", "retries": 1}

POLICY_PARAMS = {
    "drift_p_val": Param(
        0.05, type="number", description="A drift test with a p-value below this reports drift."
    ),
    "max_f1_drop": Param(0.10, type="number", description="f1 drop that triggers retraining."),
    "critical_f1_drop": Param(0.25, type="number", description="f1 drop that triggers rollback."),
}


@dag(
    dag_id="monitoring",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "mlops", "monitoring"],
    doc_md=__doc__,
    params=POLICY_PARAMS,
)
def monitoring_dag():
```

(La función se llama `monitoring_dag` porque `monitoring` ya es el nombre del
módulo importado. El `dag_id` es lo que ve Airflow.)

La task de drift, `detect_drift`, corre en **otro Python**. alibi-detect
necesita `numpy<2`, algo que el entorno propio de Airflow no puede tener, así que la imagen trae un
segundo virtualenv solo para él, `/opt/venvs/alibi`, y `config.ALIBI_PYTHON` es
su intérprete. `@task.external_python` envía la función allí: Airflow escribe
en archivos el código fuente de la función y sus argumentos, ejecuta ese
intérprete sobre ellos en un subproceso, y recoge el valor que devuelve.
Eso tiene consecuencias que verás en el código de abajo:

- Todo lo que usa la función se **importa dentro de ella**. Los imports del
  nivel superior de este archivo no existen en ese otro Python.
- Los argumentos y el valor devuelto tienen que ser **datos simples**: se
  serializan con pickle para pasar de un proceso a otro. Por eso entran rutas
  y sale un dict de números; nunca un dataframe.
- `expect_airflow=False`, porque ese virtualenv no tiene Airflow. La función
  tampoco recibe el contexto. El umbral llega como template,
  `"{{ params.drift_p_val }}"`, que se renderiza antes de que arranque el
  subproceso, así que llega como string, y `float()` lo convierte.

Es la versión ligera de lo que la §16 recomienda para equipos con dependencias
que chocan: una imagen de contenedor por task, con `KubernetesPodOperator`.

Primero, medir. Las ventanas se escriben una sola vez, y las leen tanto los
tests de drift como la comprobación de rendimiento:

```python dags/06_monitoring_and_retraining.py
    @task
    def prepare_windows(*, run_id: str) -> dict[str, str]:
        key = run_id.replace(":", "-").replace("+", "-")
        windows = monitoring.prepare_windows(f"monitoring-{key}")
        print(f"windows: {windows}")
        return windows

    @task.external_python(python=config.ALIBI_PYTHON, expect_airflow=False)
    def detect_drift(windows: dict, p_val: str) -> dict:
        from project_tagger import drift

        return drift.detect_drift(windows["reference"], windows["current"], float(p_val))

    @task
    def check_performance(windows: dict[str, str]) -> dict[str, Any] | None:
        performance = monitoring.production_performance(windows["current"])
        if performance:
            print(f"f1 at promotion={performance['f1_at_promotion']:.4f} now={performance['f1_now']:.4f}")
        return performance
```

Después, decidir:

```python dags/06_monitoring_and_retraining.py
    @task.branch
    def decide(
        drift: dict[str, Any], performance: dict[str, Any] | None, *, params: dict[str, Any]
    ) -> str:
        label, length = drift["label_drift"], drift["length_drift"]
        print(
            f"label drift p={label['p_val']:.4g} length drift p={length['p_val']:.4g} "
            f"new_labels={drift['new_labels']} policy={dict(params)}"
        )

        if performance is None:
            print("No production model yet - train one.")
            return "request_retraining"

        f1_drop = performance["f1_at_promotion"] - performance["f1_now"]
        print(f"f1_drop={f1_drop:.4f}")

        if f1_drop >= params["critical_f1_drop"]:
            return "rollback"
        if drift["new_labels"]:
            return "inspect"
        if (
            f1_drop >= params["max_f1_drop"]                    # concept drift
            or label["p_val"] < params["drift_p_val"]           # target drift
            or length["p_val"] < params["drift_p_val"]          # data drift
        ):
            return "request_retraining"
        return "continue_serving"
```

`params` es la política — los defaults de `POLICY_PARAMS`, o lo que se le haya
dado a este run. Cada condición de reentrenamiento vigila uno de los tres tipos
de drift de la tabla. Fíjate en el orden: **una etiqueta nueva lleva a
`inspect`, no a `request_retraining`.** Reentrenar con datos cuyo espacio de
etiquetas todavía no has entendido es la receta para publicar un modelo peor
convencido de que es mejor.

Ahora las cuatro ramas que puede elegir `decide`. Tres son simples; la cuarta
es un gate **human-in-the-loop** (una task que espera a que una persona
responda),
`ApprovalOperator`, seguido de la task que arranca otro Dag:

```python dags/06_monitoring_and_retraining.py
    continue_serving = EmptyOperator(task_id="continue_serving")

    @task
    def inspect(drift: dict[str, Any]) -> None:
        print("MANUAL INSPECTION NEEDED")
        print(f"  new labels: {drift['new_labels']}")
        print(f"  current distribution: {drift['current_distribution']}")

    @task
    def rollback() -> dict[str, Any] | None:
        champion = registry.rollback()
        print(f"champion -> version {champion['version']}" if champion else "nothing to roll back to")
        return champion

    request_retraining = ApprovalOperator(
        task_id="request_retraining",
        subject="Retrain the tag classifier?",
        body=(
            "Monitoring detected drift or a performance drop.\n\n"
            "Approve to trigger the `mlops` Dag on the current data."
        ),
        defaults="Approve",
        response_timeout=timedelta(seconds=60),
    )

    trigger_retraining = TriggerDagRunOperator(
        task_id="trigger_retraining",
        trigger_dag_id="mlops",
        conf={"reason": "monitoring_drift"},
        wait_for_completion=False,
        reset_dag_run=True,
    )
```

`ApprovalOperator` aparca la task en `awaiting_input` y muestra un aviso en la
UI, en la pestaña **Required Actions** de la task instance y bajo Browse →
**Required Actions**. Con **Reject**, todo lo de aguas abajo queda skipped.
`defaults` es la respuesta que se aplica si nadie responde antes de
`response_timeout`: sesenta segundos aquí para que la lección termine; dale
horas en producción. `TriggerDagRunOperator` arranca otro Dag: crea un run `manual__` de `mlops` con el `conf` dado, y con
`wait_for_completion=False` no se queda a mirarlo.

`rollback` devuelve el alias `champion` del registry a `previous_champion`. No
se vuelve a desplegar nada ni se copia ningún archivo: todo lo que cargue
`models:/project_tagger@champion` recibirá la versión anterior la próxima vez
que lo cargue. Eso es lo que hace un rollback lo bastante barato como para
automatizarlo.

Termina conectando las tasks:

```python dags/06_monitoring_and_retraining.py
    windows = prepare_windows()
    drift = detect_drift(windows, "{{ params.drift_p_val }}")
    branch = decide(drift, check_performance(windows))

    branch >> continue_serving
    branch >> inspect(drift)
    branch >> rollback()
    branch >> request_retraining >> trigger_retraining


monitoring_dag()
```

Cuatro ramas que salen de `decide`, que devuelve el `task_id` de la rama que
hay que ejecutar. No hay join al final, porque nada tiene que correr después de
todas ellas.

### Ejecútalo

```bash
docker compose exec airflow-scheduler airflow dags test monitoring
```

> **Este comando tarda unos 70 segundos y parece congelado durante 60 de ellos.**
> `request_retraining` es el gate human-in-the-loop: entra en
> `awaiting_input` y agota su `response_timeout` completo de 60 segundos antes
> de resolverse por defecto con Approve, porque nadie va a pulsar el botón.
> Déjalo terminar.

**Comprobación.** `decide` escribe en el log los dos p-valores: label drift en
torno a `7.7e-05`, length drift en torno a `0.92`. La mezcla de tags del cuarto
más reciente se movió. `mlops` pasa de ser cerca del 6 % de los
proyectos antiguos a cerca del 16 % de los más recientes, y
`natural-language-processing` baja del 44 % al 30 %; la longitud del texto no
cambió. El test de etiquetas queda muy por debajo de 0.05, así que `decide`
elige la rama de reentrenamiento. Después, por orden, con la espera de un
minuto entre la segunda y la tercera línea:

```
Branch into request_retraining
Waiting for response                         ← el estado pasa a awaiting_input
Approved. Proceeding with downstream tasks…  ← 60s después, resuelve el default
Dag Run triggered successfully.  trigger_dag_id=mlops
```

Comprueba esa última línea:

```bash
docker compose exec airflow-scheduler airflow dags list-runs mlops -o plain
```

Tiene ahora un run `manual__…` recién creado por este Dag. Un Dag decidió
que otro Dag debía correr, y a una persona se le dio la oportunidad de decir que
no.

Mientras está en `awaiting_input`, abre el run en la UI y haz clic en
`request_retraining`: el aviso te espera en su pestaña **Required Actions**
(§5), y responderlo ahí resuelve la task sin esperar al timeout. Airflow
registra la respuesta en los dos casos: la decisión, cuándo, y *quién*. Un run
que agotó el timeout y aplicó sus `defaults` guarda la decisión con el "quién"
vacío, que es cómo distingues "una persona aprobó esto" de "no había nadie
mirando".

> **Cambia la política, cambia la decisión.** El umbral es un param, así que a
> un run se le puede dar uno más estricto:
>
> ```bash
> docker compose exec airflow-scheduler \
>     airflow dags test monitoring --conf '{"drift_p_val": 0.00001}'
> ```
>
> El p-valor del test de etiquetas queda ahora por encima del umbral, así que
> ningún test informa de drift. Si la §10 promocionó un modelo, `decide()`
> devuelve `continue_serving`, la rama de reentrenamiento queda skipped, y el
> run termina en segundos. Si todavía no hay modelo en producción, `decide()`
> lo dice (`No production model yet - train one.`) y elige la rama de
> reentrenamiento igualmente, timeout incluido. Ambas son correctas: el
> desenlace depende del estado y de la política, que es para lo que sirve un
> monitor.

Una salvedad sobre `f1_now`. La ventana actual es el cuarto más reciente de la
misma tabla con la que entrenó `mlops`, así que el champion ya ha visto la
mayoría de esas filas, y su f1 ahí sale inflado. En producción, la ventana
actual son filas que llegaron *después* del entrenamiento y se etiquetaron
desde entonces, y eso es lo que hace de `f1_now` una medida honesta del
concept drift.

Referencia: [`solutions/06_monitoring_and_retraining.py`](../../solutions/06_monitoring_and_retraining.py).

### Lo que "reentrenar" deja fuera

`trigger_retraining` dispara `mlops`, que entrena con lo que haya en el
warehouse. Eso es la orquestación; no es el problema completo del
reentrenamiento. Cada uno de estos sería una task más en una versión real de
este Dag:

| Paso | Por qué no es automático |
|---|---|
| **labeling** | las filas nuevas necesitan etiquetas reales, no las predicciones del propio modelo realimentadas como verdad |
| **active learning** | etiqueta las filas que más enseñarían al modelo — rara vez hay presupuesto para todas |
| **QA** | audita las etiquetas antes de entrenar con ellas: falsos positivos, desacuerdo entre etiquetadores, subgrupos de datos donde el modelo falla |
| **augmentation** | amplía los datos nuevos para que una ventana pequeña no quede ahogada por el histórico |
| **sampling** | sube/baja muestreo para arreglar el desbalanceo que introdujo el drift |
| **evaluation** | construye el conjunto de test para que se parezca a los datos que verá el modelo en producción |

La otra restricción es el **catastrophic forgetting**: entrenar solo con la
ventana nueva hace que el modelo olvide patrones que antes acertaba, así que
corrige los errores de hoy y vuelve a cometer los de antes. El Dag de este
tutorial lo evita entrenando siempre con la tabla etiquetada completa: vale con
este tamaño, y es lo primero que hay que revisar cuando la tabla deje de caber
en una task.

Por eso también `promotion_gate`, en la §10, compara con el modelo que está en
producción en vez de solo con un umbral fijo: un modelo reentrenado que es peor
que el actual no debería publicarse solo porque saltó un monitor.

### Hacer rollback menos a menudo

Si hace falta un rollback, es que antes un modelo malo llegó a producción. Las
**estrategias de despliegue** reducen el daño que puede hacer antes de que
alguien se dé cuenta:

| Estrategia | Cómo recibe tráfico el modelo nuevo |
|---|---|
| **canary** | una parte pequeña de las peticiones (por ejemplo, el 5 %) va al modelo nuevo; sus métricas se comparan con las del champion antes de aumentar esa parte |
| **shadow** (o *dark canary*) | el modelo nuevo recibe una copia de las peticiones reales y sus predicciones se registran, nunca se devuelven; se evalúa con datos en vivo sin afectar a ningún usuario |
| **blue-green** | la versión nueva corre junto a la antigua, y el tráfico cambia de una a otra de golpe, con la antigua lista para volver a ella |

Pertenecen a la capa de serving, el servicio que responde a las peticiones
de predicción, que este proyecto no tiene: aquí `champion` cambia de golpe.
Pero el punto de partida está en el registry: con un alias `challenger` junto a
`champion`, el sistema de serving sabe qué versión recibe la parte del canary
o la copia del shadow, y el promotion gate pasa a ser "las métricas en vivo del
challenger superan a las del champion" en vez de una puntuación sobre un
conjunto de test.

---

← [11. Esperar: sensors, deferrable tasks, eventos](11-esperar-sensors-deferrable-tasks-eventos.md) · [Índice](README.md) · [13. Deadline alerts](13-deadline-alerts.md) →
