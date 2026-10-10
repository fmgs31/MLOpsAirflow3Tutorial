## 12. Continual learning: monitorizar, decidir, actuar

**Continual**, no continuo: el sistema se repite *con pausas*, y las pausas son
deliberadas. Un monitor dispara, se decide algo — por una persona donde importa —
y entonces corre el pipeline. El gate de aprobación que construye esta sección,
`request_retraining`, es esa decisión,
convertida en una task del Dag.

Lo que estás vigilando es el **drift**, de tres tipos:

| Drift | Qué se movió | Aquí lo detecta |
|---|---|---|
| **data drift** | la distribución de entrada — el texto en sí | `mean_length_shift` en `monitoring.drift_report()` |
| **target drift** | la distribución de etiquetas — un tag domina, o aparece uno nuevo | `label_tvd` (total variation distance: cuánto cambió la proporción de los tags, de 0 a 1) y `new_labels` |
| **concept drift** | la relación entre entrada y etiqueta — la misma entrada ahora merece otra etiqueta | solo la comprobación de rendimiento en vivo: el f1 cae mientras las entradas parecen iguales |

El concept drift es el que ninguna estadística del lado de la entrada puede ver,
y por eso `monitoring.production_performance()` puntúa filas etiquetadas
recientemente en vez de fiarse solo de una comparación de distribuciones.

Un evento de monitorización tiene cuatro respuestas sensatas, y
`dags/06_monitoring_and_retraining.py` las convierte en branches:

| Respuesta | Cuándo | Qué pasa |
|---|---|---|
| **continue** | drift y rendimiento dentro de los límites | nada; seguimos sirviendo |
| **improve** | el f1 bajó, o se movió la distribución de entrada o la de etiquetas | preguntar a una persona, y luego disparar `mlops` |
| **inspect** | apareció una etiqueta no vista | parar y avisar a alguien — el espacio de etiquetas cambió |
| **rollback** | el rendimiento se desplomó | volver a apuntar producción al modelo anterior |

### Constrúyelo

La carga de trabajo: `monitoring.monitor()` devuelve un informe con un dict
`drift` (`label_tvd`, `new_labels`, `mean_length_shift`, `current_distribution`,
…) y un dict
`performance` (`f1_at_promotion`, `f1_now`) — o `performance=None` cuando no hay
nada promocionado; `registry.rollback()` vuelve a promocionar el modelo
anterior.

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

from tagifai import monitoring, registry

DEFAULT_ARGS = {"owner": "ml-engineering", "retries": 1}

POLICY_PARAMS = {
    "max_label_tvd": Param(0.15, type="number", description="Label-drift budget."),
    "max_length_shift": Param(
        0.25, type="number", description="Input-drift budget: relative change in mean text length."
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

Medir, y luego decidir:

```python dags/06_monitoring_and_retraining.py
    @task
    def measure() -> dict[str, Any]:
        report = monitoring.monitor()
        drift = report["drift"]
        print(
            f"label TVD={drift['label_tvd']:.4f} new_labels={drift['new_labels']} "
            f"length shift={drift['mean_length_shift']:+.4f}"
        )
        if report["performance"]:
            perf = report["performance"]
            print(f"f1 at promotion={perf['f1_at_promotion']:.4f} now={perf['f1_now']:.4f}")
        return report

    @task.branch
    def decide(report: dict[str, Any], **context) -> str:
        drift, performance = report["drift"], report["performance"]
        policy = context["params"]

        if performance is None:
            print("No production model yet - train one.")
            return "request_retraining"

        f1_drop = performance["f1_at_promotion"] - performance["f1_now"]
        print(f"f1_drop={f1_drop:.4f} label_tvd={drift['label_tvd']:.4f} policy={dict(policy)}")

        if f1_drop >= policy["critical_f1_drop"]:
            return "rollback"
        if drift["new_labels"]:
            return "inspect"
        if (
            f1_drop >= policy["max_f1_drop"]                                 # concept drift
            or drift["label_tvd"] >= policy["max_label_tvd"]                 # target drift
            or abs(drift["mean_length_shift"]) >= policy["max_length_shift"] # data drift
        ):
            return "request_retraining"
        return "continue_serving"
```

`context["params"]` es la política — los defaults de arriba, o lo que se le haya
dado a este run. Cada condición de reentrenamiento vigila uno de los tres tipos
de drift de la tabla. `mean_length_shift` es el cambio relativo de la longitud
media del texto (`+0.10` = un 10 % más largo), y va con `abs()` porque un texto
que se acorta es tanto drift como uno que se alarga. Fíjate en el orden: **una etiqueta nueva enruta a `inspect`, no
a `request_retraining`.** Reentrenar con datos cuyo espacio de etiquetas todavía no has
entendido es la forma de publicar un modelo peor con plena confianza.

Ahora las cuatro ramas. Tres son simples; la cuarta es un gate
**human-in-the-loop** (una task que espera a que una persona responda),
`ApprovalOperator`, seguido de la task que arranca otro Dag:

```python dags/06_monitoring_and_retraining.py
    continue_serving = EmptyOperator(task_id="continue_serving")

    @task
    def inspect(report: dict[str, Any]) -> None:
        print("MANUAL INSPECTION NEEDED")
        print(f"  new labels: {report['drift']['new_labels']}")
        print(f"  current distribution: {report['drift']['current_distribution']}")

    @task
    def rollback() -> dict[str, Any] | None:
        pointer = registry.rollback()
        print(f"rolled back to {pointer['run_id']}" if pointer else "nothing to roll back")
        return pointer

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
**Required Actions**. Con **Reject**, todo lo de
aguas abajo queda skipped. `defaults` es la respuesta que se aplica si nadie
responde antes de `response_timeout` — sesenta segundos aquí para que la lección
termine; dale horas en producción. `TriggerDagRunOperator` arranca otro
Dag: crea un run `manual__` de `mlops` con el `conf` dado, y con
`wait_for_completion=False` no se queda a mirarlo.

Termina con el cableado:

```python dags/06_monitoring_and_retraining.py
    report = measure()
    branch = decide(report)

    branch >> continue_serving
    branch >> inspect(report)
    branch >> rollback()
    branch >> request_retraining >> trigger_retraining


monitoring_dag()
```

Cuatro ramas a partir de un branch; `decide` devuelve el `task_id` de la rama
que hay que ejecutar. No hay join al final, porque nada tiene que correr después de
todos ellos.

### Ejecútalo

Forzar a cero el límite de label drift garantiza que `decide()` elija la rama
de reentrenamiento, así que ves el camino interesante en vez del que toque con
tus datos actuales:

```bash
docker compose exec airflow-scheduler \
    airflow dags test monitoring --conf '{"max_label_tvd": 0.0}'
```

> **Este comando tarda unos 70 segundos y parece congelado durante 60 de ellos.**
> No es un cuelgue. `request_retraining` es el gate human-in-the-loop: entra en
> `awaiting_input` y agota su `response_timeout` completo de 60 segundos antes
> de resolverse por defecto a Approve, porque nadie va a pulsar el botón. Déjalo
> estar.

**Comprobación.** Por orden, con la espera de un minuto entre la segunda y la
tercera línea:

```
Branch into request_retraining
Waiting for response                         ← el estado pasa a awaiting_input
Approved. Proceeding with downstream tasks…  ← 60s después, resuelve el default
Dag Run triggered successfully.  trigger_dag_id=mlops
```

Esa última línea es real. Compruébalo tú mismo:

```bash
docker compose exec airflow-scheduler airflow dags list-runs mlops -o plain
```

Tiene ahora un run `manual__…` recién creado por este Dag. Un Dag decidió
que otro Dag debía correr, y a una persona se le dio la oportunidad de decir que
no.

Mientras está en `awaiting_input`, abre el run en la UI y haz clic en
`request_retraining`: el aviso te espera en su pestaña **Required Actions**
(§5), y responderlo ahí resuelve la task sin esperar
al timeout. Airflow registra la respuesta en cualquiera de los dos casos — la
decisión que se tomó, cuándo, y *quién*. Un run que agotó el timeout y aplicó sus
`defaults` guarda la decisión con el "quién" vacío, que es cómo distingues "una persona
aprobó esto" de "no había nadie mirando".

> **Sin el override de `--conf`** el desenlace depende del estado, que es el
> sentido de un monitor. Si la §10 promocionó un modelo y los datos no se han
> movido, `decide()` devuelve `continue_serving`, la rama de reentrenamiento
> queda skipped, y el run termina en segundos. Si todavía no hay modelo en
> producción, `decide()` lo dice — `No production model yet - train one.` — y
> elige la rama de reentrenamiento igualmente, timeout incluido. Ambas son
> correctas. Ejecútalo sin `--conf` para ver qué rama elige tu estado actual.

Referencia: [`solutions/06_monitoring_and_retraining.py`](../../solutions/06_monitoring_and_retraining.py).

### Lo que "reentrenar" deja fuera

`trigger_retraining` dispara `mlops`, que ajusta sobre lo que haya en el
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

La otra restricción es el **catastrophic forgetting**: ajustar solo sobre la ventana
nueva hace que el modelo olvide patrones que antes acertaba, así que corrige los
errores de hoy y recupera los de antes. El Dag de aquí lo esquiva
entrenando siempre con la tabla etiquetada completa — vale a este tamaño, y es lo
primero que revisar cuando la tabla deje de caber en una task.

Que es también por lo que el promotion gate de la §10 compara contra el modelo en
producción actual en vez de solo contra un umbral fijo: un modelo reentrenado que
es peor que el que está sirviendo no debería publicarse solo porque disparó un
monitor.

---

← [11. Esperar: sensors, deferrable tasks, eventos](11-esperar-sensors-deferrable-tasks-eventos.md) · [Índice](README.md) · [13. Deadline alerts](13-deadline-alerts.md) →
