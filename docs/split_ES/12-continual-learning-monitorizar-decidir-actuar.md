## 12. Continual learning: monitorizar, decidir, actuar

**Continual**, no continuo: el sistema se repite *con pausas deliberadas*. Un
monitor salta, se decide algo y entonces corre el pipeline. Donde importa,
decide una persona: lo que llega a producción espera a `approve_promotion`, la
task de aprobación de la §10.

El monitor vigila el **drift**, de tres tipos:

| Drift | Qué se movió | Aquí lo detecta |
|---|---|---|
| **data drift** | la distribución de entrada — el texto en sí | `length_drift`: un test de Kolmogorov–Smirnov sobre el número de palabras de cada texto |
| **target drift** | la distribución de etiquetas — un tag domina, o aparece uno nuevo | `target_drift`: un test chi-cuadrado sobre los recuentos de tags, y `new_labels` |
| **concept drift** | la relación entre entrada y etiqueta — la misma entrada ahora merece otra etiqueta | solo la comprobación de rendimiento, `monitoring.production_performance()`, que puntúa el modelo en producción con filas etiquetadas que no ha visto nunca: el f1 cae mientras las entradas parecen iguales |

Los dos tests vienen de **alibi-detect**, una librería de detección de drift.
Cada uno compara una ventana de **referencia** (las filas de las que aprendió
el modelo en producción) con una ventana **actual** (las filas que llegaron al
warehouse después de entrenarlo, que no ha visto nunca) y devuelve un
**p-value**: la probabilidad de una diferencia al menos así de grande si en
realidad nada hubiera cambiado. Por debajo de un umbral que eliges tú, por
convención 0.05, es poco probable que la diferencia sea casualidad y el test
informa de drift. El test de Kolmogorov–Smirnov (KS) compara cómo se reparte
un valor numérico, aquí las palabras por texto, en las dos ventanas; el
chi-cuadrado compara dos conjuntos de recuentos por categoría.

Un p-value dice cuán seguro está el test de que algo se movió, no cuánto. Con
suficientes filas, un cambio demasiado pequeño para importar sigue siendo
significativo, y reentrenar con cada cambio así arranca runs que nadie
necesita. Por eso cada test informa también de un **effect size** (tamaño del
efecto), de 0 a 1: para los tags, la proporción de filas cuyo tag tendría que
cambiar para que las dos mezclas coincidan (la *distancia de variación
total*); para la longitud, la mayor separación entre las dos distribuciones
acumuladas (la *distancia KS*). La política solo cuenta drift cuando el
p-value es pequeño *y* el effect size llega al menos a `min_effect_size`.

Las dos ventanas salen del run de entrenamiento del modelo en producción. En
la §10, `train_best` guardó con el run `split_ids.json`: los ids de proyecto de
cada split. La **referencia** es cada fila de `labeled_projects` cuyo id estaba
en su split de entrenamiento o de validación; la **actual**, cada fila cuyo id
no está en ninguno. Sus filas de test no están en ninguna ventana: no aprendió
de ellas, pero se eligió con ellas. Con menos de 50 filas actuales
(`config.MIN_MONITOR_ROWS`) todavía no hay nada que medir; el Dag mantiene el
modelo y vuelve a mirar en su siguiente run.

El champion se puntúa sobre la ventana actual de las dos formas habituales: un
f1 **acumulado** (cumulative), sobre toda ella, y un f1 de **ventana
deslizante** (sliding window), sobre sus filas creadas en los 90 días
(`config.SLIDING_WINDOW_DAYS`) hasta la más nueva, cuando son al menos 50. La
ventana deslizante acaba donde acaban los datos, no hoy: en cada run termina en
la fila más nueva, así que avanza a medida que llegan datos. Un f1 acumulado puede
esconder una caída reciente: un buen año compensa un mal mes. La política toma
la mayor de las dos caídas respecto al **baseline**, el f1 del champion sobre
su propio split de test. Con el del champion anterior, `check_performance`
escribe en el log cuatro f1:

| f1 | Modelo | Filas que puntúa |
|---|---|---|
| `f1_baseline` | champion | su split de test, cuando la §10 lo entrenó |
| `f1_cumulative` | champion | toda la ventana actual |
| `f1_sliding_window` | champion | las filas actuales creadas en los 90 días hasta la más nueva; `None` si son menos de 50 |
| `f1_previous` | champion anterior | toda la ventana actual; `None` si no hay champion anterior |

Cada run de monitorización acaba en una de cuatro acciones. En
`dags/06_monitoring_and_retraining.py`, la task `decide`, un `@task.branch`
(§10), elige una; cada acción es su propia task:

| Acción | Cuándo | Qué pasa |
|---|---|---|
| `continue_serving` | drift y rendimiento dentro de los límites, o ya hay un reentrenamiento con estos datos en cola, en marcha o rechazado | nada; el modelo actual sigue en producción |
| `trigger_retraining` | el f1 bajó, o se movió la distribución de entrada o la de etiquetas | disparar `mlops`, cuyo gate y cuya aprobación deciden qué se publica (§10) |
| `inspect` | apareció una etiqueta no vista, o el rendimiento se desplomó y el modelo anterior no es mejor | hacer fallar el run, para que una persona lo mire; no se reentrena ni se hace rollback |
| `rollback` | el rendimiento se desplomó, y el modelo anterior lo hace mejor con las mismas filas | volver a apuntar producción al modelo anterior |

Cómo se hablan las piezas cuando se ejecuta el Dag, una columna por sistema,
como en el diagrama de la §9:

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
un proyecto (su título, y la primera frase del abstract como descripción) y
etiquetado según su categoría de arXiv.
[`datasets/README.md`](../../datasets/README.md) explica cómo se eligieron y
etiquetaron.

Nadie los alteró para que apareciera drift. Se diferencian de los proyectos de
Made With ML en dos cosas: más computer vision y ningún `mlops`; y
descripciones más largas, porque la primera frase de un paper dice más que una
reseña de una línea. Lo segundo es un cambio en cómo escribe el origen, no en
de qué tratan los proyectos, y un test sobre la longitud del texto no
distingue una cosa de la otra. Los papers también son más nuevos, pero fecha y
origen cambian a la vez, así que estos datos no dicen cuánto del drift se debe
a la fecha.

Antes, pausa `mlops`:

```bash
docker compose exec airflow-scheduler airflow dags pause mlops
```

Un run de `dataops` que tiene éxito emite el asset event que arranca `mlops` (§8).
Aquí eso entrenaría un modelo con los proyectos nuevos antes de que
`monitoring` los compare con lo que conoce el modelo en producción: justo la
comparación de la que trata esta sección. `dataops` sigue sin pausa, como lo
dejó la §10.

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
from project_tagger_orchestration.links import MlflowModelVersionLink

DEFAULT_ARGS = {"owner": "ml-engineering", "retries": 1}

POLICY_PARAMS = {
    "drift_p_val": Param(
        0.05, type="number", description="A drift test with a p-value below this reports drift."
    ),
    "min_effect_size": Param(
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
módulo importado; el `dag_id` es lo que ve Airflow.)

La task de drift, `detect_drift`, corre en **otro Python**. alibi-detect
necesita `numpy<2`, que el entorno de Airflow no puede tener, así que la
imagen trae un segundo virtualenv para él, `/opt/venvs/alibi`, cuyo intérprete
es `config.ALIBI_PYTHON`. `@task.external_python` envía la función allí:
Airflow escribe en archivos su código fuente y sus argumentos, ejecuta ese
intérprete sobre ellos en un subproceso y recoge el valor devuelto.
Consecuencias, visibles en el código de abajo:

- Todo lo que usa la función se **importa dentro de ella**; los imports del
  nivel superior de este archivo no existen en ese Python.
- Los argumentos y el valor devuelto tienen que ser **datos simples**,
  serializados con pickle para pasar de un proceso a otro: entran rutas y sale
  un dict de números, nunca un dataframe.
- `expect_airflow=False`, porque ese virtualenv no tiene Airflow; la función
  tampoco recibe el contexto. El umbral llega como template,
  `"{{ params.drift_p_val }}"`, renderizado antes de que arranque el
  subproceso, así que llega como string, y `float()` lo convierte.

Es la versión ligera de lo que la §16 recomienda para equipos con dependencias
que chocan: una imagen de contenedor por task, con `KubernetesPodOperator`.

Primero, medir. Las ventanas se escriben una vez, y las leen tanto los tests
de drift como la comprobación de rendimiento:

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

        # target_drift and length_drift (each with is_drift, p_val, distance, effect_size),
        # new_labels and both tag distributions; None when there are no files.
        return drift.detect_drift(windows["reference"], windows["current"], float(p_val))

    @task
    def check_performance(windows: dict[str, Any]) -> dict[str, Any] | None:
        # The champion's f1_baseline; its f1_cumulative on the whole current
        # window and f1_sliding_window on its newest rows; f1_previous, the
        # previous champion's on the window; per_class and slices. None when
        # there is no window.
        performance = monitoring.production_performance(windows["current"])
        if performance:
            names = ("f1_baseline", "f1_cumulative", "f1_sliding_window", "f1_previous")
            print({k: None if performance[k] is None else round(performance[k], 4) for k in names})
            print(
                f"sliding window: the {config.SLIDING_WINDOW_DAYS} days up to the newest row, "
                f"{performance['sliding_window_rows']} rows"
            )
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

        target, length = drift["target_drift"], drift["length_drift"]
        print(
            f"target drift p={target['p_val']:.4g} effect_size={target['effect_size']:.3f} "
            f"length drift p={length['p_val']:.4g} effect_size={length['effect_size']:.3f} "
            f"new_labels={drift['new_labels']} policy={dict(params)}"
        )
        f1s = (performance["f1_cumulative"], performance["f1_sliding_window"])
        f1_worst = min(f1 for f1 in f1s if f1 is not None)
        f1_drop = performance["f1_baseline"] - f1_worst
        print(f"f1_drop={f1_drop:.4f}")
        for key, value in {
            "target_drift_p": target["p_val"],
            "target_drift_effect_size": target["effect_size"],
            "length_drift_p": length["p_val"],
            "length_drift_effect_size": length["effect_size"],
            "f1_drop": f1_drop,
        }.items():
            ti.xcom_push(key=key, value=value)

        def drifted(test: dict[str, Any]) -> bool:
            return test["p_val"] < params["drift_p_val"] and test["effect_size"] >= params["min_effect_size"]

        if f1_drop >= params["critical_f1_drop"]:
            f1_previous = performance["f1_previous"]
            if f1_previous is not None and f1_previous > performance["f1_cumulative"]:
                return "rollback"
            return "inspect"
        if drift["new_labels"]:
            return "inspect"
        if (
            f1_drop >= params["max_f1_drop"]    # concept drift
            or drifted(target)                   # target drift
            or drifted(length)                  # data drift
        ):
            return retrain_or_wait(windows, ti)
        return "continue_serving"
```

`retrain_or_wait` no es una task sino una función normal, así
que nadie le inyecta nada: recibe el `ti` de `decide` como argumento.

`decide` solo devuelve el `task_id` de la rama, así que los números con los que
decide se quedarían en el log. `ti.xcom_push` (§6) guarda cada uno como un XCom
propio: los dos p-values, los dos effect sizes y `f1_drop`, una fila cada uno
en la UI.

`params` es la política: los defaults de `POLICY_PARAMS`, o lo que se le haya
dado a este run. Las dos primeras comprobaciones van antes de cualquier
política: sin modelo en producción hay que entrenar uno; sin ventana actual no
hay nada que juzgar todavía. Después, `f1_drop` es la mayor de las dos caídas
de f1, `drifted` exige a un test un p-value pequeño y un effect size suficiente, y
cada condición de reentrenamiento vigila uno de los tres tipos de drift de la
tabla. Fíjate en el orden: **una etiqueta nueva lleva a `inspect`, no a
`trigger_retraining`.** Reentrenar con datos cuyo espacio de etiquetas todavía
no has entendido es la receta para publicar un modelo peor creyendo que es
mejor.

Un desplome tampoco hace rollback por sí solo. **Un rollback solo ayuda si el
problema es el modelo en producción**, así que `decide` lo comprueba antes: el
champion anterior tampoco ha visto las filas actuales, y si puntúa mejor con
ellas, el modelo nuevo es peor y `rollback` vuelve a poner el anterior. Si no,
los datos se movieron bajo los dos modelos, un rollback no arreglaría nada y
el run va a `inspect`, igual que un desplome sin champion anterior al que
volver.

Los dos caminos a `trigger_retraining` pasan por `retrain_or_wait`. El monitor
corre cada día; los datos nuevos no llegan cada día, y hasta que llegan, el
drift encontrado sigue ahí. Sin esta comprobación, cada mañana arrancaría otro
run de `mlops` con las mismas filas. Mantiene el modelo en dos casos:

- **Ya hay un run de `mlops` en cola o en marcha.** `ti.get_dr_count` le
  pregunta a Airflow cuántos runs de otro Dag están en los estados dados.
- **Ya se entrenó un modelo con estas filas y no se promocionó.**
  `prepare_windows` busca en MLflow el run de entrenamiento más reciente; si no
  es el del champion y su `split_ids.json` tiene todas las filas actuales, el
  gate o la aprobación lo rechazaron. Reentrenar con las mismas filas solo
  repite la búsqueda de Optuna sobre los mismos datos, así que un resultado
  mejor sería suerte. Solo filas nuevas cambian eso, y las filas nuevas ya
  arrancan `mlops` por sí solas a través de su asset (§8).

Ahora las cuatro ramas que puede elegir `decide`. Tres son simples; la cuarta
arranca otro Dag:

```python dags/06_monitoring_and_retraining.py
    continue_serving = EmptyOperator(task_id="continue_serving")

    @task
    def inspect(drift: dict[str, Any], performance: dict[str, Any]) -> None:
        print("MANUAL INSPECTION NEEDED")
        print(f"  new labels: {drift['new_labels']}")
        print(f"  current distribution: {drift['current_distribution']}")
        print(f"  f1 baseline={performance['f1_baseline']:.4f} cumulative={performance['f1_cumulative']:.4f}")
        for name, m in {**performance["per_class"], **performance["slices"]}.items():
            print(f"  {name}: {m['support']} rows, f1 {m['f1']:.4f}")
        raise AirflowFailException("manual inspection needed - nothing was retrained or rolled back")

    @task(outlets=[PRODUCTION_MODEL])
    def rollback(
        performance: dict[str, Any], *, outlet_events: OutletEventAccessorsProtocol
    ) -> dict[str, Any]:
        reason = f"f1 fell from {performance['f1_baseline']:.4f} to {performance['f1_cumulative']:.4f}"
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
§10. Después falla a propósito, con `AirflowFailException` (§9): un run que
necesita a una persona no debe acabar en verde, porque un éxito se lee como
"todo bien" y un fallo es a lo que se conectan las alertas (§16).

`rollback` devuelve el alias `champion` del registry a `previous_champion`. No
se despliega ni se copia nada: todo lo que cargue
`models:/project_tagger@champion` recibirá la versión anterior en su próxima
carga, y eso hace un rollback lo bastante barato como para automatizarlo.
`registry.rollback` además borra el alias `previous_champion`, para que un
segundo rollback no pueda volver a poner el modelo malo, y etiqueta la versión
retirada con `rolled_back_reason`, visible en la UI de MLflow. La task emite
`production_model`, como `promote_model` en la §10: un rollback cambia el
modelo en producción tanto como una promoción. `decide` solo elige `rollback`
tras ver un champion anterior; si el alias ya no está cuando la task se
ejecuta, falla en vez de saltarse, porque el modelo desplomado sigue en
producción y alguien tiene que saberlo. Una task fallida no emite asset event.
El cableado le da a `rollback` un extra link (§10), **MLflow model version**, a
la versión que volvió a poner en producción.

`TriggerDagRunOperator` arranca otro Dag: crea un run `manual__` de `mlops`
con el `conf` dado, y con `wait_for_completion=False` no se queda a esperarlo.
Trae su propio extra link, **Triggered DAG**, a ese run.
No hay aprobación antes: un reentrenamiento no cambia nada en producción, y su
modelo todavía tiene que pasar `promotion_gate` y `approve_promotion` (§10).

Termina conectando las tasks:

```python dags/06_monitoring_and_retraining.py
    windows = prepare_windows()
    drift = detect_drift(windows, "{{ params.drift_p_val }}")
    performance = check_performance(windows)
    branch = decide(windows, drift, performance)
    restored = rollback(performance)
    restored.operator.operator_extra_links = [MlflowModelVersionLink()]

    branch >> continue_serving
    branch >> inspect(drift, performance)
    branch >> restored
    branch >> trigger_retraining


monitoring_dag()
```

No hay join al final, porque nada tiene que correr después de ninguna de las
ramas.

### Ejecútalo

```bash
docker compose exec airflow-scheduler airflow dags test monitoring
```

La salida tiene unas cien líneas. Para encontrar las que lee la Comprobación,
busca en tu terminal `windows:`, `f1_baseline`, `target drift` y `Branch into`.
Los números también están en la UI: abre el run, haz clic en `prepare_windows`
o en `decide` y luego en **Storage** y **XCom** (§5). `num_current`, los
p-values, los effect sizes y `f1_drop` tienen cada uno su propia fila.

> La salida también muestra un traceback que termina en
> `ModuleNotFoundError: No module named 'pendulum'`. No ha fallado nada. Antes
> de ejecutar `detect_drift`, Airflow ejecuta `python -c "import pendulum"` en
> el virtualenv de alibi, para ver si puede pasarle las fechas del run:
> `logical_date` y las demás fechas del contexto son objetos de pendulum. El
> import falla, así que Airflow deja fuera esas fechas, y `detect_drift` no las
> usa.

**Comprobación.** `prepare_windows` escribe en el log `'num_current': 800`,
con `version` la del champion: los 800 papers son las filas que no ha visto
nunca. `check_performance` escribe:

| Métrica | Valor | Qué significa |
|---|---|---|
| `f1_baseline` | en torno a 0.89 | |
| `f1_cumulative` | en torno a 0.80 | una caída de entre 0.05 y 0.11 según la búsqueda, por debajo de `max_f1_drop` (0.15) |
| `f1_sliding_window` | por encima de `f1_cumulative` | los 122 papers más nuevos no son los que se le dan mal, así que el f1 acumulado da la mayor caída |
| `f1_previous` | `None` | es el primer modelo que se promociona: no hay con qué compararlo |

El f1 bajó, pero menos que `max_f1_drop`, así que la comprobación de concept
drift no habría saltado sola. `decide` escribe los dos tests de drift, los dos
con un p-value muy por debajo de 0.05 y un effect size muy por encima de 0.05:

| Test | p-value | Effect size | Filas de las que aprendió el modelo | Filas nuevas |
|---|---|---|---|---|
| Target drift | en torno a `1e-22` | 0.19 | `computer-vision` 38 %, `mlops` 8 % | `computer-vision` 57 %, sin `mlops` |
| Data drift | `0` (demasiado pequeño para mostrarse) | 0.63 | mediana de 16 palabras por texto | mediana de 27 palabras por texto |

Dos tests informan de drift, así que `decide` elige la rama de
reentrenamiento:

```
Branch into trigger_retraining
Dag Run triggered successfully.  trigger_dag_id=mlops
```

Comprueba esa última línea:

```bash
docker compose exec airflow-scheduler airflow dags list-runs mlops -o plain
```

`mlops` tiene ahora un run `manual__…` recién creado por este Dag, en `queued`
porque `mlops` está en pausa.

> **Cambia la política, cambia la decisión.** El umbral es un param, así que a
> un run se le puede dar otro:
>
> ```bash
> docker compose exec airflow-scheduler \
>     airflow dags test monitoring --conf '{"drift_p_val": 0}'
> ```
>
> Ningún p-value queda por debajo de 0, así que ningún test informa de drift y
> solo cuenta el f1, cuya caída está por debajo de `max_f1_drop`: `decide()`
> devuelve `continue_serving`, la rama de reentrenamiento queda skipped y el
> run termina en segundos. Aquí es la peor política: el f1 sí bajó, y el
> reentrenamiento de abajo lo recupera.

**Deja correr el reentrenamiento.** El run creado por `trigger_retraining`
espera porque `mlops` sigue en pausa. Quítale la pausa:

```bash
docker compose exec airflow-scheduler airflow dags unpause mlops
```

**Comprobación.** El run en cola arranca en unos segundos y termina con éxito
un par de minutos después. Su snapshot tiene las 1564 filas, y su split de
test 319: las 157 con las que se evaluó el champion, más 162 de los papers,
por el mismo hash. `compare_with_champion` escribe tres líneas, `all`, `new` y
`old`:

| Filas de test | Modelo nuevo | Champion |
|---|---|---|
| `new`: los 162 papers | entre 0.92 y 0.96 | entre 0.82 y 0.85 |
| `old`: las 157 con las que se evaluó el champion | normalmente uno o dos puntos por debajo | en torno a 0.89, su `f1_baseline` |

`promotion_gate` escribe `f1=… clears the champion's … on the 162 new test
rows - promoting`, y `approve_promotion` te espera, como en la §10. Apruébalo en su pestaña **Required Actions**, o deja que el
default lo apruebe a los 60 segundos, y `promote_model` mueve `champion` a la
versión nueva.

Pregunta al monitor una vez más:

```bash
docker compose exec airflow-scheduler airflow dags test monitoring
```

**Comprobación.** `decide` escribe `only 0 new rows since version … was
trained - nothing to measure`, con la versión recién promocionada, y elige
`continue_serving` en segundos. El champion nuevo se construyó con las 1564
filas, así que cada fila está en uno de sus splits. La siguiente entrega, en
la §14, vuelve a darle al monitor algo que medir.

Referencia: [`solutions/06_monitoring_and_retraining.py`](../../solutions/06_monitoring_and_retraining.py).

### Lo que el monitor no ve

La comprobación de rendimiento necesita el tag real de cada fila actual; aquí
cada fila llega con su tag, elegido por quien envió el proyecto. En la mayoría
de los sistemas la verdad llega tarde, o nunca: un préstamo entra en impago
meses después de la decisión, y casi ninguna recomendación recibe respuesta.
Hasta que llega, hay tres maneras de suplirla: un **proxy** de la etiqueta,
una señal que llega antes; **etiquetar a mano una muestra pequeña** de las
filas nuevas; o el **importance weighting**, que estima el f1 en producción a
partir de filas etiquetadas, con cada slice ponderado según lo común que es
ahora en producción. Los tests de drift no necesitan etiquetas, y por eso vale
la pena ejecutarlos incluso donde todavía no se puede medir el f1.

Tampoco vigila el sistema en sí: latencia, throughput, tasa de errores, CPU y
memoria. Eso pertenece a la capa de serving, el servicio que responde a las
peticiones de predicción, que este proyecto no tiene.

### Lo que "reentrenar" deja fuera

`trigger_retraining` dispara `mlops`, que entrena con lo que haya en el
warehouse. Eso es la orquestación, no el problema completo del
reentrenamiento. Cada uno de estos pasos sería una task más en una versión
real de este Dag:

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
corrige los errores de hoy y vuelve a cometer los de antes. Este Dag lo evita
entrenando siempre con la tabla etiquetada completa. Vale con este tamaño, pero
cuando la tabla sea demasiado grande para cargarla y entrenar con ella en una
sola task, habrá que entrenar con una parte y el olvido volverá a ser un riesgo.

La línea `old` de `compare_with_champion` es donde se ve el olvido: el f1 del
modelo nuevo sobre las filas de test del propio champion. En el run de
reentrenamiento de esta sección costó uno o dos puntos. Un equipo que quiera
limitarlo añade un **guardarraíl** al gate: no se promociona si el f1 sobre
las filas viejas cae más de un margen fijado.

### Hacer rollback menos a menudo

Si hace falta un rollback, es que antes un modelo malo llegó a producción. Las
**estrategias de despliegue** limitan el daño que puede hacer antes de que
alguien se dé cuenta:

| Estrategia | Cómo recibe tráfico el modelo nuevo |
|---|---|
| **canary** | una parte pequeña de las peticiones (por ejemplo, el 5 %) va al modelo nuevo; sus métricas se comparan con las del champion antes de aumentar esa parte |
| **shadow** (o *dark canary*) | el modelo nuevo recibe una copia de las peticiones reales y sus predicciones se registran, nunca se devuelven; se evalúa con datos en vivo sin afectar a ningún usuario |
| **blue-green** | la versión nueva corre junto a la antigua, y el tráfico cambia de una a otra de golpe, con la antigua lista para volver a ella |

Pertenecen a la capa de serving, que este proyecto no tiene: aquí `champion`
cambia de golpe. Pero empiezan en el registry: un alias `challenger` junto a
`champion` le dice al sistema de serving qué versión recibe la parte del
canary o la copia del shadow, y el promotion gate pasa a ser "las métricas en
vivo del challenger superan a las del champion" en vez de una puntuación sobre
un conjunto de test.

### Ejercicio: entrena solo con algo nuevo

`mlops` corre con cada asset event de `labeled_projects`, y `dataops` emite
uno en cada run que pasa, haya filas nuevas o no. Sin pausa, `dataops` corre
cada día a las 03:00 (§9), así que `mlops` entrena cada día: la misma búsqueda
de Optuna sobre las mismas filas, una versión registrada más cada vez y, cuando
la búsqueda saca por casualidad más nota sobre las mismas filas de test, una
aprobación para un modelo cuya única ventaja es la suerte. `retrain_or_wait`
evita que `monitoring` haga esto; para el asset, nada lo evita.

**Tu tarea.** Haz que `mlops` no entrene cuando el último entrenamiento ya
tenía todas las filas, y añade un Param `force` que haga que un run entrene
igual.

"Todas las filas" se cuenta por id de proyecto. Un cambio en filas que ya
existen, como un tag corregido en el origen o la fusión de tags que la §14 hace
en el transform, no trae ningún id nuevo; para entrenar con él está `force`.

**Terminaste cuando:**

1. Con todas las filas en los splits del champion, como las dejó "Ejecútalo",
   arranca `dataops`:

   ```bash
   docker compose exec airflow-scheduler airflow dags trigger dataops
   ```

   Cuando pasa, su asset event arranca `mlops`, que termina en segundos: la
   primera task registra `0 rows the newest training run did not have`,
   `no_new_rows` y `done` están en verde, y todas las demás tasks quedan
   skipped. MLflow no tiene ningún run nuevo.
2. Un run que lanzas tú tampoco entrena:

   ```bash
   docker compose exec airflow-scheduler airflow dags trigger mlops
   ```

La otra rama se ve en la §14: el run que deja en cola `monitoring` ahí trae
191 filas nuevas, y entrena.

Abre las pistas de una en una, solo cuando te atasques.

<details>
<summary>Pista 1: ¿qué tipo de task?</summary>

El mismo que `promotion_gate` (§10): un `@task.branch` que devuelve el
`task_id` que hay que ejecutar, aquí al principio del Dag. La rama que no
entrena necesita una task propia que lleve a `done`, cuya trigger rule ya deja
pasar cualquiera de las dos ramas.

</details>

<details>
<summary>Pista 2: ¿qué ids tenía el último entrenamiento?</summary>

`prepare_windows` responde la misma pregunta en `src/project_tagger/monitoring.py`:
`registry.latest_training_run()` y `registry.load_split_ids()`. Los splits
guardan los ids después de `data.preprocess`, que descarta las filas sin
título o sin tag, así que compara con `data.preprocess(data.load_labeled())`,
no con la tabla tal como se lee.

</details>

<details>
<summary>Pista 3: ¿qué más tiene que esperar?</summary>

Un branch salta las tasks que no eligió, y todo lo que espera por ellas.
`create_study` y `trial_budget` no esperan por nada, así que correrían igual,
y `create_study` dejaría un run en MLflow.

</details>

<details>
<summary>Solución</summary>

En `dags/04_mlops.py`, añade el Param:

```python
        "force": Param(False, type="boolean", description="Train even if no row is new."),
```

Añade el branch, y la task a la que lleva la rama que no entrena:

```python
    @task.branch
    def check_new_rows(*, params: Mapping[str, Any]) -> str:
        """Train only if the newest training run is missing some row, or if forced."""
        latest = registry.latest_training_run()
        if params["force"] or latest is None:
            return "prepare_data"
        trained_on = {i for ids in registry.load_split_ids(latest).values() for i in ids}
        new = set(data.preprocess(data.load_labeled())["id"]) - trained_on
        print(f"{len(new)} rows the newest training run did not have")
        return "prepare_data" if new else "no_new_rows"

    no_new_rows = EmptyOperator(task_id="no_new_rows")
```

Y conéctalos, con `create_study` y `trial_budget` después de los datos:

```python
    new_rows = check_new_rows()
    paths = prepare_data()
    new_rows >> [paths, no_new_rows]
    no_new_rows >> done

    approved_data_gate = validate_training_data(paths)
    study = create_study()
    budget = trial_budget()
    # The two tasks with no input wait for the data too, so a skip reaches them.
    paths >> [study, budget]
    results = run_trials.partial(study_name=study, paths=paths).expand(n_trials=budget)
```

La regla compara con el último entrenamiento, no con el del champion, por la
razón que da `retrain_or_wait`: si ese run se rechazó, entrenar otra vez con
las mismas filas solo lo repite.

</details>

---

← [11. Esperar: sensors, deferrable tasks, eventos](11-esperar-sensors-deferrable-tasks-eventos.md) · [Índice](README.md) · [13. Deadline alerts](13-deadline-alerts.md) →
