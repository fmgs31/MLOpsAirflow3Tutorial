## 9. El Dag de DataOps

`dags/03_dataops.py` — extract → validate → transform → validate. El primer Dag
de aquí que hace trabajo de verdad, y cada task es una pieza que puedes
sustituir por la herramienta de un vendor real:

| Task aquí | Opción en producción |
|---|---|
| `extract_projects` | Airbyte / Fivetran / un job EL |
| `validate_raw`, `validate_transformed` | Great Expectations / Soda / tests de dbt |
| `transform` | dbt Cloud / astronomer-cosmos |

Es también el primer Dag de aquí con un cron de verdad — `schedule="0 3 * * *"`,
el ejemplo de la §7 — así que en cuanto le quites la pausa en la §10 correrá solo:
una vez inmediatamente, para el último trozo que su cron ya ha dejado atrás,
y luego a las 03:00 UTC cada día, disparando cada run a su vez `mlops`.
(`monitoring`, §12, es el otro programado, a las 06:00.) Deja el stack levantado
toda la noche y los runs `scheduled__` que encuentres por la mañana son eso, no
un bug.

> **Sobre los datos de origen.** `projects.csv` llega ya etiquetado — una tabla
> de `id, created_on, title, description, tag` — así que `transform` limpia el
> texto y agrupa los tags raros, y ya está. Un warehouse real suele guardar las
> etiquetas en una tabla aparte, lo que convierte `transform` en un join y le da
> al Dag una segunda rama de extract y validate que lo alimenta:
>
> ```python
> [validate_projects, validate_tags] >> transform >> validate_transformed
> ```
>
> Eso es el fan-in de la §6, y es la única diferencia estructural. Nada más en este
> Dag cambia con el número de fuentes.

### Constrúyelo

La carga de trabajo que llama este Dag, toda en `src/tagifai/`: `data.extract()`
descarga el CSV a la landing zone y devuelve su ruta; `data.load_raw(fp)` y
`data.load_labeled(fp)` leen una tabla; `data.transform(df)` produce la tabla
curada y `data.save_labeled(df)` la escribe, devolviendo la ruta;
`validate.run_suite(df, suite_name)` ejecuta un conjunto de expectations con
nombre y devuelve un resultado con `.passed`, `.failures`, `.results`, `.suite`
y una línea `.summary()`. `config.ensure_dirs()` crea las carpetas.

Crea `dags/03_dataops.py`:

```python dags/03_dataops.py
"""Lesson 3 - the DataOps Dag: extract -> validate -> transform -> validate."""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.sdk import dag, task, task_group
from airflow.sdk.exceptions import AirflowFailException

from tagifai import config, data, validate
from tagifai.assets import LABELED_PROJECTS, RAW_PROJECTS

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}


@dag(
    dag_id="dataops",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "dataops"],
    doc_md=__doc__,
)
def dataops():
```

Dos imports son nuevos. `AirflowFailException` viene de
`airflow.sdk.exceptions` — el código de task importa las excepciones del Task
SDK, porque ese es el namespace que tiene un worker remoto. Y `from tagifai import …`
funciona solo porque el archivo de compose pone `/opt/airflow/src` en `PYTHONPATH`
para todos los servicios: el Dag processor lo necesita para parsear el archivo,
y el executor para ejecutar las tasks.

`max_active_runs=1` limita el Dag a un run a la vez — un segundo, venga de un
trigger o de un schedule, espera en `queued` hasta que el primero termine.

La primera task produce un asset. Añádela dentro de `dataops()`:

```python dags/03_dataops.py
    @task(outlets=[RAW_PROJECTS])
    def extract_projects(*, outlet_events) -> str:
        config.ensure_dirs()
        fp = data.extract()
        df = data.load_raw(fp)
        outlet_events[RAW_PROJECTS].extra = {"rows": len(df), "path": str(fp)}
        return str(fp)
```

`outlets=` y `outlet_events` funcionan como en la §8: la declaración, y los
metadatos adjuntos al evento que esta task emite cuando tiene éxito. La task
devuelve una **ruta**, no el dataframe: los XComs llevan una referencia
a los datos, no los datos en sí.

Ahora el gate sobre los datos crudos, y el transform:

```python dags/03_dataops.py
    @task
    def validate_raw(fp: str) -> dict[str, Any]:
        result = validate.run_suite(data.load_raw(fp), "raw_projects")
        print(result.summary())
        if not result.passed:
            raise AirflowFailException(f"Raw data failed validation: {result.failures}")
        return {"suite": result.suite, "results": result.results}

    @task
    def transform(fp: str) -> str:
        df = data.transform(data.load_raw(fp))
        out = data.save_labeled(df)
        print(f"Wrote {len(df)} rows to {out}")
        return str(out)
```

El gate de validación es la parte que merece la pena copiar, y hay dos
detalles en los que es fácil equivocarse:

- **`AirflowFailException`, no `AirflowException`.** Un fallo de calidad de datos
  no es transitorio; con `retries=2` en los defaults, una excepción normal
  ejecutaría la misma comprobación tres veces y retrasaría la alerta dos
  minutos. `AirflowFailException` falla la task al momento, con o sin
  reintentos.
- **Las expectations son no-críticas cuando significan "mira esto", no "para".**
  Que aparezca un tag inesperado es una señal para inspeccionar el espacio de
  etiquetas, no un motivo para detener el pipeline. `validate.py` marca esa con
  `critical=False`, así que aparece en `result.failures` pero no
  pone `result.passed` a falso.

El gate final — el que emite el asset con el que se programará `mlops`:

```python dags/03_dataops.py
    @task(outlets=[LABELED_PROJECTS])
    def validate_transformed(fp: str, *, outlet_events) -> dict[str, Any]:
        df = data.load_labeled(fp)
        result = validate.run_suite(df, "labeled_projects")
        print(result.summary())
        if not result.passed:
            raise AirflowFailException(f"Labeled data failed validation: {result.failures}")
        outlet_events[LABELED_PROJECTS].extra = {
            "rows": len(df),
            "tags": sorted(df["tag"].unique().tolist()),
            "path": fp,
            "warnings": [r["expectation"] for r in result.failures],
        }
        return {"suite": result.suite, "rows": len(df), "results": result.results}
```

Poner el outlet en la task de *validación*, no en la de transform, es todo el
truco: el evento se escribe solo cuando esta task tiene éxito, así que los Dags
aguas abajo se programan con "llegaron datos buenos", nunca con "terminó una
task".

Una task más, dentro de un **task group** — `@task_group` anida tasks bajo un
único nodo plegable en la vista Graph, algo a lo que recurrir en cuanto un Dag
pasa de una docena de tasks — y después el cableado:

```python dags/03_dataops.py
    @task_group(group_id="quality_report")
    def quality_report(raw_report: dict[str, Any], curated_report: dict[str, Any]):

        @task
        def summarize(raw: dict[str, Any], curated: dict[str, Any]) -> None:
            for report in (raw, curated):
                failed = [r["expectation"] for r in report["results"] if not r["passed"]]
                print(f"{report['suite']}: {len(report['results'])} checks, failed={failed or 'none'}")

        summarize(raw_report, curated_report)

    raw_fp = extract_projects()
    raw_report = validate_raw(raw_fp)

    curated_fp = transform(raw_fp)
    raw_report >> curated_fp

    curated_report = validate_transformed(curated_fp)
    quality_report(raw_report, curated_report)


dataops()
```

Lee el cableado dos veces. `transform` recibe `raw_fp`, así que el flujo de
datos le dibuja su arista desde `extract_projects` — pero nada de lo que consume
viene de `validate_raw`, así que `raw_report >> curated_fp` añade la arista a
mano: el transform debe esperar al gate aunque no use su salida. Eso es la regla
de la §6 en la práctica — aristas implícitas cuando fluyen datos, `>>` cuando solo importa el orden.

### Mira aparecer los assets

Guarda el archivo. A los pocos segundos `airflow dags list` tiene una tercera
fila — y algo más ha cambiado:

```bash
docker compose exec airflow-scheduler airflow assets list -o plain
```

**Comprobación.** Dos filas, `labeled_projects` y `raw_projects`, cada una con
su grupo y su extra. No ha corrido nada; el Dag processor parseó un archivo con
`outlets=` dentro, y con eso bastó. `production_model`, definido en el mismo
`assets.py`, no aparece — ningún Dag se refiere a él todavía. La página Assets
de la UI muestra ahora los dos, con `dataops` como productor y sin consumidor.

### Ejecútalo

```bash
docker compose exec airflow-scheduler airflow dags test dataops
```

**Comprobación.** Busca `suite=raw_projects passed=4/4`, `Wrote 764 rows to
/opt/airflow/data/warehouse/labeled_projects.csv`, `suite=labeled_projects
passed=6/6`, y del task group, `raw_projects: 4 checks, failed=none` y
`labeled_projects: 6 checks, failed=none`. El archivo
`data/warehouse/labeled_projects.csv` ya existe en tu máquina. (`extract`
descarga el CSV de origen por HTTPS — es la única task del pipeline que
necesita conexión a internet, y el recuento de filas es el que tenga ese archivo
cuando lo descargas.)

Si una task falla, compara con [`solutions/03_dataops.py`](../../solutions/03_dataops.py).

> **`airflow dags test` no es un dry run.** Ejecutó las tasks reales y escribió
> las filas reales: el archivo en disco, y un asset event `labeled_projects`
> genuino en la metadata database — abre la página Assets y ahí está, con el
> `extra` que le adjuntaste. Todavía no hay nada programado con ese asset, así
> que no pasa nada más. Y según la §8, no pasará: cuando `mlops` aparezca en la
> §10 este evento ya será historia, y Airflow no reproduce la historia.

### Backfills

Ahora que tienes un Dag con schedule, los backfills tienen sentido. Un backfill
crea los runs de cada trozo de un rango de fechas, a posteriori. Son objetos de
primera clase, con su propia API, su formulario en la UI y su política de
reprocesado. **Haz un dry-run primero** — un backfill con el rango de fechas equivocado es
la forma más fácil de crear treinta runs por accidente:

```bash
docker compose exec airflow-scheduler airflow backfill create \
    --dag-id dataops --from-date 2026-02-01 --to-date 2026-02-04 --dry-run
```

**Comprobación.** Una tabla encabezada `Runs to be attempted:` con tres filas —
2026-02-01, 02 y 03, cada una a las 03:00 — y nada creado. `--to-date` es
exclusivo, y cada fila es un trozo del cron diario de `dataops`.

Quita `--dry-run` y el mismo comando crea esos tres runs, con run ids que empiezan
por `backfill__`. Ten claro qué viene después antes de hacerlo:

- Los runs se crean **queued**, y si el Dag está en pausa se quedan así — la
  regla de la §7 — hasta que le quites la pausa, momento en que arrancan todos,
  `--max-active-runs` a la vez.
- Cada run de backfill de `dataops` que tenga éxito emite un asset event de
  `labeled_projects` exactamente igual que un run normal (§8), así que un `mlops`
  sin pausa entrena detrás de ellos. Hacer backfill de un productor también
  dispara runs de sus consumidores.
- La CLI en 3.3.1 solo *crea* backfills. Mientras uno no ha terminado, la
  página del Dag muestra un banner *Backfill in progress* con un botón de pausa
  y otro de stop; stop lo cancela. También la API — `PUT
  /api/v2/backfills/{id}/cancel`, sin token en este stack, donde todos son
  admin. Cancelarlo marca como `failed` los runs del backfill aún en cola. Pausar el Dag es el freno de emergencia: un Dag en pausa no arranca
  nada.

`--reprocess-behavior` decide qué hacer con los trozos que ya tienen un run —
`none`, `failed` o `completed` — que es la diferencia entre "rellena los huecos"
y "rehaz el mes". En la UI: Dag → **Trigger** → **Backfill**, que hace un dry-run
del rango y te dice cuántos runs va a crear antes de confirmar. Esto es también
por lo que a un Dag programado con assets no se le puede hacer backfill (§5): no
tiene trozos, solo eventos.

### Cambiar a los vendors reales

Cada cambio es un operator más una **Connection** — la credencial nunca aparece
en el archivo del Dag. Añádela bajo Admin → Connections, desde la CLI, o como
variable de entorno `AIRFLOW_CONN_*`; el Dag solo la nombra:

```bash
airflow connections add airbyte \
    --conn-type http --conn-host localhost --conn-port 8000
```

```python
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

extract_projects = AirbyteTriggerSyncOperator(
    task_id="extract_projects",
    airbyte_conn_id="airbyte",            # ← the Connection, not a credential
    connection_id="XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
    asynchronous=False, timeout=3600, wait_seconds=3,
)

validate_raw = GreatExpectationsOperator(
    task_id="validate_raw",
    checkpoint_name="projects",
    data_context_root_dir=GE_ROOT_DIR,
    fail_task_on_validation_failure=True,  # the gate: a failed suite fails the task
)

transform = DbtCloudRunJobOperator(
    task_id="transform", job_id=118680,
    wait_for_termination=True, check_interval=10, timeout=300,
)
# ...or dbt Core in-process: BashOperator(bash_command=f"cd {DBT_DIR} && dbt run && dbt test")
```

De dónde salen esos nombres — cada herramienta se configura **fuera** de Airflow,
y el Dag solo referencia el artefacto:

```bash
great_expectations init                        # creates the context directory
great_expectations datasource new              # point it at your warehouse
great_expectations suite new                   # the expectations ("projects")
great_expectations checkpoint new projects     # ← this name is `checkpoint_name=`

dbt init dbt_transforms                        # then models/<name>/<name>.sql + schema.yml
```

Las expectations dentro de esa suite son el vocabulario propio de Great
Expectations, y se corresponden una a una con las comprobaciones de
`validate.py`: `expect_column_values_to_be_unique`,
`expect_column_values_to_not_be_null`, `expect_column_values_to_be_of_type`,
`expect_compound_columns_to_be_unique`, `expect_table_row_count_to_be_between`.

La estructura del Dag no cambia: `outlets=[LABELED_PROJECTS]` sigue yendo en la última
task de validación, y todo lo de aguas abajo sigue programándose con el asset.
Esos tres providers **no** están instalados aquí, y por eso este tutorial no
necesita ninguna cuenta de vendor.

---

← [8. Assets: programar por datos, no por tiempo](08-assets-programar-por-datos-no-por-tiempo.md) · [Índice](README.md) · [10. El Dag de MLOps](10-el-dag-de-mlops.md) →
