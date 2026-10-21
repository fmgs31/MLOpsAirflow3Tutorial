## 12. Continual learning: monitorizar, decidir, actuar

**Continual**, no continuo: el sistema se repite *con pausas*, y las pausas son
deliberadas. Un monitor dispara, se decide algo, y entonces corre el
pipeline. Donde importa, decide una persona: lo que llega a producción espera
a `approve_promotion`, la task de aprobación de la §10.

Lo que estás vigilando es el **drift**, de tres tipos:

| Drift | Qué se movió | Aquí lo detecta |
|---|---|---|
| **data drift** | la distribución de entrada — el texto en sí | `length_drift`: un test de Kolmogorov–Smirnov sobre el número de palabras de cada texto |
| **target drift** | la distribución de etiquetas — un tag domina, o aparece uno nuevo | `label_drift`: un test chi-cuadrado sobre los recuentos de tags, y `new_labels` |
| **concept drift** | la relación entre entrada y etiqueta — la misma entrada ahora merece otra etiqueta | solo la comprobación de rendimiento en vivo, `monitoring.production_performance()`, que puntúa el modelo en producción con filas etiquetadas que no ha visto nunca: el f1 cae mientras las entradas parecen iguales |

Los dos tests vienen de **alibi-detect**, una librería de detección de drift.
Cada uno recibe una ventana de **referencia** (las filas de las que aprendió el
modelo en producción) y una ventana **actual** (las filas que llegaron al
warehouse después de entrenarlo, que no ha visto nunca), y responde con un **p-valor**: la probabilidad de
ver una diferencia al menos así de grande si en realidad nada hubiera cambiado.
Un p-valor pequeño significa que es poco probable que la diferencia sea
casualidad. Por debajo de un umbral que eliges tú, por convención 0.05, el test
informa de drift. El test de Kolmogorov–Smirnov (KS) compara dos distribuciones
de un número; el chi-cuadrado compara dos conjuntos de recuentos por categoría.

Las dos ventanas salen del propio run de entrenamiento del modelo en
producción. En la §10, `train_best` guardó con el run `split_ids.json`: los ids
de proyecto de cada uno de sus splits. La **referencia** es cada fila de
`labeled_projects` cuyo id estaba en su split de entrenamiento o de validación.
La **actual** es cada fila cuyo id no está en ninguno de sus splits. Sus filas
de test no están en ninguna de las dos ventanas: no aprendió de ellas, pero se
eligió con ellas. Con menos de 50 filas actuales (`config.MIN_MONITOR_ROWS`)
todavía no hay nada que medir, y el Dag mantiene el modelo y vuelve a mirar en
su siguiente run.

Un evento de monitorización tiene cuatro respuestas sensatas, y
`dags/06_monitoring_and_retraining.py` las convierte en branches:

| Respuesta | Cuándo | Qué pasa |
|---|---|---|
| **continue** | drift y rendimiento dentro de los límites | nada; el modelo actual sigue en producción |
| **improve** | el f1 bajó, o se movió la distribución de entrada o la de etiquetas | disparar `mlops`, cuyo gate y cuya aprobación deciden qué se publica (§10) |
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
   y ◄──────────────────────────────────────────────── el split_ids.json
                                │                   │  del champion
   escribe reference.parquet    │                   │
   y current.parquet en data/   │                   │
 2 detect_drift (alibi-detect,  │                   │
   en su propio virtualenv)     │                   │
   lee los dos archivos         │                   │
 3 check_performance ◄──────────────────────────────── el modelo champion
   lo puntúa sobre              │                   │
   current.parquet              │                   │
 4 decide: una rama de          │                   │
   continue_serving             │                   │
   inspect                      │                   │
   rollback ─────────────────────────────────────────► devuelve champion
   trigger_retraining           │                   │  a previous_champion
     │ arranca un run del       │                   │
     ▼                          │                   │
   Dag mlops (§10)              │                   │
```

### Llegan datos nuevos

El modelo en producción de la §10 se entrenó con los 764 proyectos de Made
With ML, así que cada fila del warehouse está en uno de sus splits y no hay
nada que monitorizar. Ahora llegan 800 más: papers reales de informática de
arXiv, enviados entre enero de 2025 y septiembre de 2026, cada uno escrito como
un proyecto (su título, y la primera frase de su abstract como descripción) y
etiquetado según su categoría de arXiv.
[`datasets/README.md`](../../datasets/README.md) explica cómo se eligieron y
etiquetaron.

Nadie los alteró para que aparezca drift. Se diferencian de los proyectos de
Made With ML en dos cosas: más computer vision y ningún `mlops`, y
descripciones más largas, porque la primera frase de un paper dice más que una
reseña de una línea. La segunda es un cambio en cómo escribe el origen, no en
de qué tratan los proyectos; un test sobre la longitud del texto no distingue
una cosa de la otra. Los papers también son más nuevos, pero fecha y origen
cambian a la vez, así que estos datos no dicen cuánto del drift se debe a la
fecha.

Antes, pausa `mlops`:

```bash
docker compose exec airflow-scheduler airflow dags pause mlops
```

Un run de `dataops` que pasa emite el asset event que arranca `mlops` (§8).
Aquí eso entrenaría un modelo con los proyectos nuevos antes de que
`monitoring` los haya comparado con lo que conoce el modelo en producción, y
esa comparación es de lo que trata esta sección. `dataops` sigue sin pausa,
como lo dejó la §10.

Una entrega es un cambio de URL en una source de Airbyte. En Airbyte, abre
**Sources**, elige `projects`, sustituye su **URL** por:

```
https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/batch_1/projects.csv
```

y guarda. Haz lo mismo con `tags`, con:

```
https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/batch_1/tags.csv
```

Después, cárgala:

```bash
docker compose exec airflow-scheduler airflow dags trigger dataops
```

**Comprobación.** El run termina con éxito al cabo de unos minutos, y en la
consola de BigQuery (§9) `labeled_projects` tiene 1564 filas. `mlops` no tiene
ningún run nuevo: un Dag en pausa no arranca ningún run por un asset event.

### Constrúyelo

La carga de trabajo: `monitoring.prepare_windows(key)` lee
`labeled_projects` y el `split_ids.json` del champion, escribe las dos
ventanas en archivos, y devuelve sus rutas, `num_current` (el número de filas
actuales) y `version` (la del champion); sin champion, o con menos de 50 filas
actuales, no escribe ningún archivo y las rutas son `None`.
`drift.detect_drift(reference_fp, current_fp, p_val)` ejecuta los dos tests y
devuelve `label_drift` y `length_drift` (cada uno con `is_drift`, `p_val` y
`distance`), `new_labels` y las dos distribuciones de tags, o `None` cuando no
hay archivos. `monitoring.production_performance(current_fp)` puntúa al
champion sobre la ventana actual y devuelve `f1_at_promotion` y `f1_now`, o
`None` cuando no hay ventana. `registry.rollback(reason)` devuelve `champion`
a `previous_champion` (§10).

Crea `dags/06_monitoring_and_retraining.py`. Los umbrales son **params** del
Dag, así que la política es visible en la UI, se puede sobrescribir por run, y
queda registrada con el run que la usó:

```python dags/06_monitoring_and_retraining.py
"""Lesson 6 - continual learning: monitor, decide, and act."""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task
from airflow.sdk.exceptions import AirflowSkipException
from airflow.sdk.types import OutletEventAccessorsProtocol

from project_tagger import config, monitoring, registry
from project_tagger_orchestration.assets import PRODUCTION_MODEL

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
    def prepare_windows(*, run_id: str) -> dict[str, Any]:
        key = run_id.replace(":", "-").replace("+", "-")
        windows = monitoring.prepare_windows(f"monitoring-{key}")
        print(f"windows: {windows}")
        return windows

    @task.external_python(python=config.ALIBI_PYTHON, expect_airflow=False)
    def detect_drift(windows: dict, p_val: str) -> dict | None:
        from project_tagger import drift

        return drift.detect_drift(windows["reference"], windows["current"], float(p_val))

    @task
    def check_performance(windows: dict[str, Any]) -> dict[str, Any] | None:
        performance = monitoring.production_performance(windows["current"])
        if performance:
            print(f"f1 at promotion={performance['f1_at_promotion']:.4f} now={performance['f1_now']:.4f}")
        return performance
```

Después, decidir:

```python dags/06_monitoring_and_retraining.py
    @task.branch
    def decide(
        windows: dict[str, Any],
        drift: dict[str, Any] | None,
        performance: dict[str, Any] | None,
        *,
        params: dict[str, Any],
    ) -> str:
        if windows["version"] is None:
            print("No production model yet - train one.")
            return "trigger_retraining"
        if drift is None or performance is None:
            print(
                f"only {windows['num_current']} new rows since version {windows['version']} "
                "was trained - nothing to measure"
            )
            return "continue_serving"

        label, length = drift["label_drift"], drift["length_drift"]
        print(
            f"label drift p={label['p_val']:.4g} length drift p={length['p_val']:.4g} "
            f"new_labels={drift['new_labels']} policy={dict(params)}"
        )
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
            return "trigger_retraining"
        return "continue_serving"
```

`params` es la política — los defaults de `POLICY_PARAMS`, o lo que se le haya
dado a este run. Las dos primeras comprobaciones van antes de cualquier
política: sin modelo en producción hay uno que entrenar, y sin ventana actual
todavía no hay nada que juzgar. Después, cada condición de reentrenamiento
vigila uno de los tres tipos de drift de la tabla. Fíjate en el orden: **una etiqueta nueva lleva a
`inspect`, no a `trigger_retraining`.** Reentrenar con datos cuyo espacio de
etiquetas todavía no has entendido es la receta para publicar un modelo peor
convencido de que es mejor.

Ahora las cuatro ramas que puede elegir `decide`. Tres son simples; la cuarta
arranca otro Dag:

```python dags/06_monitoring_and_retraining.py
    continue_serving = EmptyOperator(task_id="continue_serving")

    @task
    def inspect(drift: dict[str, Any]) -> None:
        print("MANUAL INSPECTION NEEDED")
        print(f"  new labels: {drift['new_labels']}")
        print(f"  current distribution: {drift['current_distribution']}")

    @task(outlets=[PRODUCTION_MODEL])
    def rollback(
        performance: dict[str, Any], *, outlet_events: OutletEventAccessorsProtocol
    ) -> dict[str, Any]:
        reason = f"f1 fell from {performance['f1_at_promotion']:.4f} to {performance['f1_now']:.4f}"
        champion = registry.rollback(reason)
        if champion is None:
            raise AirflowSkipException("no previous_champion - nothing to roll back to")
        outlet_events[PRODUCTION_MODEL].extra = {
            "version": champion["version"],
            "rolled_back_version": champion["rolled_back_version"],
            "reason": "rollback",
        }
        print(f"champion -> version {champion['version']} (retired {champion['rolled_back_version']}: {reason})")
        return champion

    trigger_retraining = TriggerDagRunOperator(
        task_id="trigger_retraining",
        trigger_dag_id="mlops",
        conf={"reason": "monitoring_drift"},
        wait_for_completion=False,
        reset_dag_run=True,
    )
```

`TriggerDagRunOperator` arranca otro Dag: crea un run `manual__` de `mlops`
con el `conf` dado, y con `wait_for_completion=False` no se queda a mirarlo.
No hay aprobación antes: un reentrenamiento no cambia nada en producción, y el
modelo que produce todavía tiene que pasar `promotion_gate` y
`approve_promotion` (§10).

`rollback` devuelve el alias `champion` del registry a `previous_champion`. No
se vuelve a desplegar nada ni se copia ningún archivo: todo lo que cargue
`models:/project_tagger@champion` recibirá la versión anterior la próxima vez
que lo cargue. Eso es lo que hace un rollback lo bastante barato como para
automatizarlo. `registry.rollback` además borra el alias `previous_champion`,
para que un segundo rollback no pueda volver a poner el modelo malo, y
etiqueta la versión retirada con `rolled_back_reason`, que la UI de MLflow
muestra. La task emite `production_model`, como `promote_model` en la §10: un
rollback cambia el modelo en producción tanto como una promoción. Si no hay a
dónde volver, lanza `AirflowSkipException`, y una task skipped no emite asset
event.

Termina conectando las tasks:

```python dags/06_monitoring_and_retraining.py
    windows = prepare_windows()
    drift = detect_drift(windows, "{{ params.drift_p_val }}")
    performance = check_performance(windows)
    branch = decide(windows, drift, performance)

    branch >> continue_serving
    branch >> inspect(drift)
    branch >> rollback(performance)
    branch >> trigger_retraining


monitoring_dag()
```

Cuatro ramas que salen de `decide`, que devuelve el `task_id` de la rama que
hay que ejecutar. No hay join al final, porque nada tiene que correr después de
todas ellas.

### Ejecútalo

```bash
docker compose exec airflow-scheduler airflow dags test monitoring
```

**Comprobación.** `prepare_windows` escribe en el log `'num_current': 800`,
con `version` la del champion: los 800 papers son las filas que no ha visto
nunca. `check_performance` escribe su f1 en la promoción y ahora, en torno a
0.89 y 0.82: una caída de entre 0.05 y 0.10, según la búsqueda, por debajo de
`max_f1_drop`.
`decide` escribe los dos p-valores: label drift en torno a `1e-22`, length
drift `0`. Los dos quedan muy por debajo de 0.05, y dicen cosas distintas:

- **Target drift.** `computer-vision` es el 38 % de las filas de las que
  aprendió el modelo y el 57 % de las nuevas; `mlops`, el 8 % de lo que
  aprendió, no aparece.
- **Data drift.** La mediana de un texto es de 16 palabras en las filas de las
  que aprendió el modelo, y de 27 en las nuevas: un p-valor demasiado pequeño
  para mostrarse como algo distinto de `0`.
- **Rendimiento.** El f1 bajó, pero menos que `max_f1_drop`, así que la
  comprobación de concept drift no habría saltado sola.

Dos tests informan de drift, así que `decide` elige la rama de
reentrenamiento. Después:

```
Branch into trigger_retraining
Dag Run triggered successfully.  trigger_dag_id=mlops
```

Comprueba esa última línea:

```bash
docker compose exec airflow-scheduler airflow dags list-runs mlops -o plain
```

Tiene ahora un run `manual__…` recién creado por este Dag, en `queued`:
`mlops` está en pausa, así que el run espera. Un Dag decidió que otro Dag
debía correr.

> **Cambia la política, cambia la decisión.** El umbral es un param, así que a
> un run se le puede dar otro:
>
> ```bash
> docker compose exec airflow-scheduler \
>     airflow dags test monitoring --conf '{"drift_p_val": 0}'
> ```
>
> Ningún p-valor queda por debajo de 0, así que ningún test informa de drift,
> y la política solo mira el f1, cuya caída está por debajo de `max_f1_drop`:
> `decide()` devuelve `continue_serving`, la rama de reentrenamiento queda
> skipped, y el run termina en segundos. Los mismos datos y el mismo modelo en
> producción, y otra decisión: el desenlace depende de la política, y para eso
> es un param. Aquí, además, es la peor política: el f1 sí bajó, y el
> reentrenamiento de abajo lo recupera.

**Deja correr el reentrenamiento.** El run que creó `trigger_retraining` está
esperando a `mlops`, que sigue en pausa. Quítale la pausa:

```bash
docker compose exec airflow-scheduler airflow dags unpause mlops
```

**Comprobación.** El run en cola arranca en unos segundos y termina con éxito
un par de minutos después. Su snapshot tiene las 1564 filas, y su split de
test 319 de ellas: las 157 con las que se evaluó el champion, más 162 de los
papers, por el mismo hash. `compare_with_champion` escribe tres líneas, `all`,
`new` y `old`. Sobre las 162 filas de test nuevas el modelo nuevo saca
entre 0.92 y 0.96 y el champion entre 0.82 y 0.85; sobre las 157 viejas los
dos quedan cerca, el modelo nuevo normalmente uno o dos puntos por debajo.
`promotion_gate` escribe `f1=… clears the champion's … on the 162 new test
rows - promoting`, y `approve_promotion` te espera, como en la §10. Esta es la
decisión para la que existe la aprobación: los números están en el log de
`compare_with_champion`, y el modelo en MLflow. Apruébalo en su pestaña
**Required Actions**, o deja que el default lo apruebe a los 60 segundos, y
`promote_model` mueve `champion` a la versión nueva.

Eso es el drift tratado: el modelo nuevo aprendió de 638 de los papers, y en
los que no ha visto ninguno de los dos modelos es entre 8 y 14 puntos mejor.

Pregunta al monitor una vez más:

```bash
docker compose exec airflow-scheduler airflow dags test monitoring
```

**Comprobación.** `decide` escribe `only 0 new rows since version … was
trained - nothing to measure`, con la versión que acaba de promocionar
`promote_model`, y elige `continue_serving`, en segundos. El champion nuevo
se construyó con las 1564 filas, así que cada fila está en uno de sus splits. La
siguiente entrega, en la §14, vuelve a darle al monitor algo que medir.

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
producción, sobre las mismas filas de test, en vez de solo con un umbral fijo:
un modelo reentrenado que es peor que el actual no debería publicarse solo
porque saltó un monitor.

La línea `old` de `compare_with_champion` es donde se ve el olvido: el f1 del
modelo nuevo sobre las filas de test del propio champion. En el run de
reentrenamiento de esta sección costó uno o dos puntos. Un equipo que quiera
ponerle límite añade un **guardarraíl** al gate: no se promociona si el f1
sobre las filas viejas cae más de un margen fijado.

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
