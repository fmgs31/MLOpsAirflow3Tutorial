## 12. Continual learning: monitorizar, decidir, actuar

**Continual**, no continuo: el sistema se repite *con pausas*, y las pausas son
deliberadas. Un monitor salta, se decide algo, y entonces corre el
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
warehouse después de entrenarlo, que no ha visto nunca), y responde con un **p-value**: la probabilidad de
ver una diferencia al menos así de grande si en realidad nada hubiera cambiado.
Un p-value pequeño significa que es poco probable que la diferencia sea
casualidad. Por debajo de un umbral que eliges tú, por convención 0.05, el test
informa de drift. El test de Kolmogorov–Smirnov (KS) compara cómo se reparte un
valor numérico, aquí las palabras por texto, en las dos ventanas; el
chi-cuadrado compara dos conjuntos de recuentos por categoría.

Un p-value dice cuán seguro está el test de que algo se movió, no cuánto. Con
suficientes filas, un cambio demasiado pequeño para importar sigue siendo
significativo, y un monitor que reentrena con cada cambio así arranca runs de
entrenamiento que nadie necesita. Por eso cada test informa también de un
**shift**, de 0 a 1: para los tags, la proporción de filas cuyo tag tendría que
cambiar para que las dos mezclas coincidan (la *distancia de variación
total*); para la longitud, la mayor separación entre las dos distribuciones
acumuladas (la *distancia KS*). La política solo cuenta drift cuando el p-value
es pequeño *y* el shift llega al menos a `min_drift_shift`.

Las dos ventanas salen del propio run de entrenamiento del modelo en
producción. En la §10, `train_best` guardó con el run `split_ids.json`: los ids
de proyecto de cada uno de sus splits. La **referencia** es cada fila de
`labeled_projects` cuyo id estaba en su split de entrenamiento o de validación.
La **actual** es cada fila cuyo id no está en ninguno de sus splits. Sus filas
de test no están en ninguna de las dos ventanas: no aprendió de ellas, pero se
eligió con ellas. Con menos de 50 filas actuales (`config.MIN_MONITOR_ROWS`)
todavía no hay nada que medir, y el Dag mantiene el modelo y vuelve a mirar en
su siguiente run.

El f1 se mide dos veces sobre la ventana actual: sobre toda ella, y solo sobre
sus filas más nuevas, las creadas en sus últimos 90 días
(`config.RECENT_DAYS`), cuando son al menos 50. Un f1 sobre todo lo que el
modelo no ha visto puede esconder una caída reciente: un buen año compensa un
mal mes. La política toma la mayor de las dos caídas.

Cada run de monitorización acaba en una de cuatro acciones. En
`dags/06_monitoring_and_retraining.py`, la task `decide`, un `@task.branch`
(§10), elige una, y cada acción es su propia task:

| Acción | Cuándo | Qué pasa |
|---|---|---|
| **continue** | drift y rendimiento dentro de los límites, o ya hay un reentrenamiento con estos datos en cola, en marcha o rechazado | nada; el modelo actual sigue en producción |
| **improve** | el f1 bajó, o se movió la distribución de entrada o la de etiquetas | disparar `mlops`, cuyo gate y cuya aprobación deciden qué se publica (§10) |
| **inspect** | apareció una etiqueta no vista, o el rendimiento se desplomó y el modelo anterior no es mejor | hacer fallar el run, para que una persona lo mire; no se reentrena ni se hace rollback |
| **rollback** | el rendimiento se desplomó, y el modelo anterior lo hace mejor con las mismas filas | volver a apuntar producción al modelo anterior |

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
 3 check_performance ◄──────────────────────────────── los modelos champion
   puntúa los dos sobre         │                   │  y previous_champion
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

Nadie los alteró para que apareciera drift. Se diferencian de los proyectos de
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

Un run de `dataops` que tiene éxito emite el asset event que arranca `mlops` (§8).
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
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk.types import OutletEventAccessorsProtocol

from project_tagger import config, monitoring, registry
from project_tagger_orchestration.assets import PRODUCTION_MODEL

DEFAULT_ARGS = {"owner": "ml-engineering", "retries": 1}

POLICY_PARAMS = {
    "drift_p_val": Param(
        0.05, type="number", description="A drift test with a p-value below this reports drift."
    ),
    "min_drift_shift": Param(
        0.05, type="number", description="...and only if at least this much of the distribution moved (0-1)."
    ),
    "max_f1_drop": Param(0.15, type="number", description="f1 drop that triggers retraining."),
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
        # Writes reference.parquet and current.parquet; returns their paths, num_current,
        # version (the champion's) and already_trained_on. With no champion, or fewer than
        # config.MIN_MONITOR_ROWS current rows, it writes nothing and the paths are None.
        windows = monitoring.prepare_windows(f"monitoring-{key}")
        print(f"windows: {windows}")
        return windows

    @task.external_python(python=config.ALIBI_PYTHON, expect_airflow=False)
    def detect_drift(windows: dict, p_val: str) -> dict | None:
        from project_tagger import drift

        # label_drift and length_drift (each with is_drift, p_val, distance, shift),
        # new_labels and both tag distributions; None when there are no files.
        return drift.detect_drift(windows["reference"], windows["current"], float(p_val))

    @task
    def check_performance(windows: dict[str, Any]) -> dict[str, Any] | None:
        # The champion's f1_at_promotion; its f1_now on the current window and
        # f1_recent on the newest rows of it; f1_previous, the previous
        # champion's on the window; per_class and slices. None when there is no window.
        performance = monitoring.production_performance(windows["current"])
        if performance:
            f1s = {k: performance[k] for k in ("f1_at_promotion", "f1_now", "f1_recent", "f1_previous")}
            print({k: None if f1 is None else round(f1, 4) for k, f1 in f1s.items()})
            print(f"recent: the last {config.RECENT_DAYS} days, {performance['recent_rows']} rows")
        return performance
```

Después, decidir:

```python dags/06_monitoring_and_retraining.py
    def retrain_or_wait(windows: dict[str, Any], ti) -> str:
        if ti.get_dr_count(dag_id="mlops", states=["queued", "running"]):
            print("an mlops run is already queued or running - not starting another")
            return "continue_serving"
        if windows["already_trained_on"]:
            print("a model was already trained on every current row and not promoted - waiting for new data")
            return "continue_serving"
        return "trigger_retraining"

    @task.branch
    def decide(
        windows: dict[str, Any],
        drift: dict[str, Any] | None,
        performance: dict[str, Any] | None,
        *,
        params: dict[str, Any],
        ti,
    ) -> str:
        if windows["version"] is None:
            print("No production model yet - train one.")
            return retrain_or_wait(windows, ti)
        if drift is None or performance is None:
            print(
                f"only {windows['num_current']} new rows since version {windows['version']} "
                "was trained - nothing to measure"
            )
            return "continue_serving"

        label, length = drift["label_drift"], drift["length_drift"]
        print(
            f"label drift p={label['p_val']:.4g} shift={label['shift']:.3f} "
            f"length drift p={length['p_val']:.4g} shift={length['shift']:.3f} "
            f"new_labels={drift['new_labels']} policy={dict(params)}"
        )
        f1_worst = min(f1 for f1 in (performance["f1_now"], performance["f1_recent"]) if f1 is not None)
        f1_drop = performance["f1_at_promotion"] - f1_worst
        print(f"f1_drop={f1_drop:.4f}")

        def drifted(test: dict[str, Any]) -> bool:
            return test["p_val"] < params["drift_p_val"] and test["shift"] >= params["min_drift_shift"]

        if f1_drop >= params["critical_f1_drop"]:
            f1_previous = performance["f1_previous"]
            if f1_previous is not None and f1_previous > performance["f1_now"]:
                return "rollback"
            return "inspect"
        if drift["new_labels"]:
            return "inspect"
        if (
            f1_drop >= params["max_f1_drop"]    # concept drift
            or drifted(label)                   # target drift
            or drifted(length)                  # data drift
        ):
            return retrain_or_wait(windows, ti)
        return "continue_serving"
```

`params` es la política — los defaults de `POLICY_PARAMS`, o lo que se le haya
dado a este run. Las dos primeras comprobaciones van antes de cualquier
política: sin modelo en producción hay uno que entrenar, y sin ventana actual
todavía no hay nada que juzgar. Después, `f1_drop` es la mayor caída de las
dos ventanas de f1, `drifted` le pide a un test a la vez un p-value pequeño y
un shift suficiente, y cada condición de reentrenamiento vigila uno de los
tres tipos de drift de la tabla. Fíjate en el orden: **una etiqueta nueva lleva a
`inspect`, no a `trigger_retraining`.** Reentrenar con datos cuyo espacio de
etiquetas todavía no has entendido es la receta para publicar un modelo peor
creyendo que es mejor.

Un desplome tampoco hace rollback por sí solo. **Un rollback solo ayuda si el
problema es el modelo en producción**, así que `decide` lo comprueba antes: el
champion anterior tampoco ha visto nunca las filas actuales, y si puntúa mejor
con ellas, el modelo nuevo es peor, y `rollback` vuelve a poner el anterior.
Si no puntúa mejor, los datos se movieron bajo los dos modelos, un rollback no
arreglaría nada, y el run va a `inspect`. Lo mismo pasa con un desplome sin
champion anterior al que volver.

Los dos caminos a `trigger_retraining` pasan antes por `retrain_or_wait`. El
monitor corre cada día; los datos nuevos no llegan cada día, y hasta que
llegan, el drift que encontró sigue ahí. Sin esta comprobación, cada mañana
arrancaría un run más de `mlops` con las mismas filas. Mantiene el modelo, en
vez de reentrenar, en dos casos:

- **Ya hay un run de `mlops` en cola o en marcha.** `ti.get_dr_count` le
  pregunta a Airflow cuántos runs de otro Dag están en los estados dados.
- **Ya se entrenó un modelo con estas filas y no se promocionó.**
  `prepare_windows` busca en MLflow el run de entrenamiento más reciente; si no
  es el del champion y su `split_ids.json` tiene todas las filas actuales, el
  gate o la aprobación lo rechazaron. Otra búsqueda con las mismas filas es un
  reintento, no evidencia nueva. Lo que cambia eso es una entrega nueva, y esa
  arranca `mlops` de todos modos, por su asset (§8).

Ahora las cuatro ramas que puede elegir `decide`. Tres son simples; la cuarta
arranca otro Dag:

```python dags/06_monitoring_and_retraining.py
    continue_serving = EmptyOperator(task_id="continue_serving")

    @task
    def inspect(drift: dict[str, Any], performance: dict[str, Any]) -> None:
        print("MANUAL INSPECTION NEEDED")
        print(f"  new labels: {drift['new_labels']}")
        print(f"  current distribution: {drift['current_distribution']}")
        print(f"  f1 at promotion={performance['f1_at_promotion']:.4f} now={performance['f1_now']:.4f}")
        for name, m in {**performance["per_class"], **performance["slices"]}.items():
            print(f"  {name}: {m['support']} rows, f1 {m['f1']:.4f}")
        raise AirflowFailException("manual inspection needed - nothing was retrained or rolled back")

    @task(outlets=[PRODUCTION_MODEL])
    def rollback(
        performance: dict[str, Any], *, outlet_events: OutletEventAccessorsProtocol
    ) -> dict[str, Any]:
        reason = f"f1 fell from {performance['f1_at_promotion']:.4f} to {performance['f1_now']:.4f}"
        champion = registry.rollback(reason)
        if champion is None:
            raise AirflowFailException("previous_champion is gone - nothing to roll back to")
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

`inspect` escribe lo que una persona necesita para empezar a mirar: qué se
movió, y dónde falla el modelo, por tag y en cada slice de evaluación de la
§10. Después falla a propósito, con `AirflowFailException` (§9). Un run que necesita a una
persona no debe acabar en verde: un éxito se lee como "todo bien", y un fallo
es a lo que se conectan las alertas (§16).

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
rollback cambia el modelo en producción tanto como una promoción. `decide` solo
elige `rollback` después de ver un champion anterior; si el alias ya no está
cuando la task se ejecuta, falla en vez de saltarse, porque el modelo
desplomado sigue en producción y alguien tiene que saberlo. Una task fallida
no emite asset event.

Termina conectando las tasks:

```python dags/06_monitoring_and_retraining.py
    windows = prepare_windows()
    drift = detect_drift(windows, "{{ params.drift_p_val }}")
    performance = check_performance(windows)
    branch = decide(windows, drift, performance)

    branch >> continue_serving
    branch >> inspect(drift, performance)
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
0.89 y 0.80: una caída de entre 0.05 y 0.11, según la búsqueda, por debajo de
`max_f1_drop` (0.15). `f1_recent`, sobre los 122 papers de los últimos 90 días, es
más alto que `f1_now`: los papers más nuevos no son los que se le dan mal, así
que la ventana entera da la mayor caída. `f1_previous` es `None`: es el primer
modelo que se promociona, así que no hay con qué compararlo.
`decide` escribe los dos p-values, label drift en torno a `1e-22` y length
drift `0`, los dos muy por debajo de 0.05, y los dos shifts muy por encima de
0.05. Dicen cosas distintas:

- **Target drift**, shift 0.19. `computer-vision` es el 38 % de las filas de
  las que aprendió el modelo y el 57 % de las nuevas; `mlops`, el 8 % de lo
  que aprendió, no aparece.
- **Data drift**, shift 0.63. La longitud mediana de un texto es de 16
  palabras en las filas de las que aprendió el modelo, y de 27 en las nuevas:
  un p-value demasiado pequeño para mostrarse como algo distinto de `0`.
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
> Ningún p-value queda por debajo de 0, así que ningún test informa de drift,
> y la política solo mira el f1, cuya caída está por debajo de `max_f1_drop`:
> `decide()` devuelve `continue_serving`, la rama de reentrenamiento queda
> skipped, y el run termina en segundos. Los mismos datos y el mismo modelo en
> producción, y otra decisión: el desenlace depende de la política, y para eso
> es un param. Aquí, además, es la peor política: el f1 sí bajó, y el
> reentrenamiento de abajo lo recupera.

**Deja correr el reentrenamiento.** El run creado por `trigger_retraining`
está esperando, porque `mlops` sigue en pausa. Quítale la pausa:

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
decisión para la que existe la aprobación: los números están en el aviso, y el
modelo en MLflow. Apruébalo en su pestaña
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

### Lo que el monitor no ve

La comprobación de rendimiento necesita el tag real de cada fila actual, y
aquí cada fila llega con su tag, elegido por quien envió el proyecto. En la
mayoría de los sistemas la verdad llega tarde, o nunca: un préstamo entra en
impago meses después de la decisión, y casi nadie informa de vuelta sobre una
recomendación. Hasta que llega, hay tres maneras de suplirla: un **proxy** de
la etiqueta, una señal que llega antes; **etiquetar a mano una muestra
pequeña** de las filas nuevas; o el **importance weighting**, que estima el f1
en producción a partir de filas que sí tienen etiqueta, con cada slice
ponderado según lo común que es ahora en producción. Los tests de drift no
necesitan etiquetas, y por eso vale la pena ejecutarlos incluso donde todavía
no se puede medir el f1.

Tampoco vigila el sistema en sí: latencia, throughput, tasa de errores, CPU y
memoria. Eso pertenece a la capa de serving, el servicio que responde a las
peticiones de predicción, que este proyecto no tiene.

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
| **sampling** | sobremuestrea o submuestrea para arreglar el desbalanceo que introdujo el drift |
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
