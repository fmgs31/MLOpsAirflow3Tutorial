## 8. Assets: programar por datos, no por tiempo

Un **Asset** es un nombre que le das a un conjunto de datos; por ejemplo,
`labeled_projects`, la tabla de entrenamiento que construye `dataops` en el
warehouse. Airflow no lee esos
datos: solo conoce el nombre y registra cada vez que se actualizan.

`dataops` y `mlops` comparten sus assets, así que los defines una sola vez,
en un paquete que importan los dos. Es código de orquestación: va con los
Dags, no en `src/project_tagger/`, el proyecto de ML, que no importa Airflow
(§1).

### Dónde viven los assets

Cuando Airflow parsea un archivo de Dag, pone la carpeta de Dags en
`sys.path`, la lista de carpetas desde las que importa Python, así que un
paquete dentro de `dags/` puede contener el código que comparten varios Dags.
La documentación de Airflow sobre
[gestión de módulos](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/modules_management.html)
le pone tres reglas:

- El paquete lleva un nombre propio: aquí, `project_tagger_orchestration`.
  Todas las carpetas de `sys.path` comparten un mismo conjunto de nombres de
  primer nivel, así que un nombre genérico como `common` o `utils` puede
  chocar con un módulo instalado que se llame igual.
- Los Dags lo importan con su ruta completa,
  `from project_tagger_orchestration.assets import …`, nunca con un import
  relativo.
- Aparece en `.airflowignore`, para que el Dag processor no lo parsee buscando
  Dags.

Crea `dags/project_tagger_orchestration/` con un `__init__.py`, el archivo que
convierte una carpeta en un paquete:

```python dags/project_tagger_orchestration/__init__.py
"""Code shared by this tutorial's Dags. Not a Dag: `.airflowignore` keeps the Dag processor out."""
```

Después crea `dags/.airflowignore`. Cada línea es un patrón de archivos o
carpetas que el Dag processor debe saltarse; esta salta el paquete:

```text dags/.airflowignore
project_tagger_orchestration/
```

### Define los assets

Los assets van en `dags/project_tagger_orchestration/assets.py`, empezando por
`labeled_projects`:

```python dags/project_tagger_orchestration/assets.py
"""The Assets the tutorial's Dags produce and consume."""

from __future__ import annotations

from airflow.sdk import Asset

from project_tagger import config

LABELED_PROJECTS = Asset(
    name="labeled_projects",
    uri=config.bigquery_uri(config.LABELED_PROJECTS_TABLE),  # bigquery://<project>/mlops_course/labeled_projects
    group="dataset",
    extra={"layer": "warehouse", "owner": "data-engineering"},
)
```

`name` identifica al asset en tus Dags. `uri` es dónde están los datos
reales: aquí la tabla de BigQuery, en el esquema `bigquery://` que registra el
provider de Google. `config.bigquery_uri` la construye con tu Project ID (§9)
y el nombre de la tabla, que están en `project_tagger.config` porque el código
de ML también los lee. `from project_tagger import config` funciona porque el
archivo de compose pone `/opt/airflow/src` en el `PYTHONPATH` de todos los
servicios. `group` y `extra` son descripciones que la UI enseña en la página
del asset; este `extra` describe el asset, no un evento concreto.

La URI es opcional. `production_model`, el asset que emite `mlops` cuando
promociona un modelo (§10), solo tiene nombre: el modelo vive en el registry de
MLflow, y Airflow no conoce ningún esquema de URI (como `bigquery://` o
`gs://`) que apunte allí. Cuando los datos de un asset sí estén en un sitio con
esquema conocido, dale una URI: la UI muestra entonces dónde están esos datos,
y las herramientas que leen el linaje de Airflow pueden rastrearlos.

Añade `production_model` al final de `assets.py`, junto con los assets de las
dos tablas sin procesar que carga Airbyte (`dataops` los produce en la §9):

```python dags/project_tagger_orchestration/assets.py
RAW_PROJECTS = Asset(
    name="raw_projects",
    uri=config.bigquery_uri(config.PROJECTS_TABLE),
    group="dataset",
    extra={"layer": "raw", "owner": "data-engineering"},
)
RAW_TAGS = Asset(
    name="raw_tags",
    uri=config.bigquery_uri(config.TAGS_TABLE),
    group="dataset",
    extra={"layer": "raw", "owner": "data-engineering"},
)

PRODUCTION_MODEL = Asset(
    name="production_model",
    group="model",
    extra={"owner": "ml-engineering"},
)
```

Con eso tienes cuatro assets, y Airflow todavía no conoce ninguno: para
Airflow, un asset solo existe cuando un Dag parseado se refiere a él, como
outlet o como schedule, y `assets.py` no es un Dag. Ahora mismo `airflow assets list`
imprime `No data found`, y la página Assets está vacía; la §9 cambia eso en
cuanto el Dag processor parsee el primer archivo que guardes en esa sección.

### Productores y consumidores

En un Dag, un asset se nombra con su objeto, importado del paquete:
`from project_tagger_orchestration.assets import LABELED_PROJECTS`. Usarlo tiene
dos lados.

**Quién produce el asset.** Una task declara que produce un asset poniéndolo en
el argumento `outlets=` de su decorador:

```python
@task(outlets=[LABELED_PROJECTS])
```

Los assets de esa lista son los **outlets** de la task: los datos que la task
declara que escribe. (Su contrario, `inlets=`, declara los que lee; este
tutorial no lo usa.) La task es el **productor** de sus outlets. Cada vez que
la task termina con éxito, Airflow registra un **asset event**: la constancia
de que ese asset acaba de actualizarse.

**Quién lo consume.** Un Dag declara que consume un asset poniéndolo en su
`schedule=`:

```python
@dag(schedule=[LABELED_PROJECTS])
```

Ese Dag es el **consumidor** del asset, y cada asset event nuevo arranca un run
suyo. Eso sustituye a cualquier cron del tipo "corre 30 minutos después del
otro Dag, y cruza los dedos".

El asset event en sí es una fila en la metadata database: qué asset, qué Dag run
lo produjo, cuándo, y un dict `extra` opcional que la task productora puede
rellenar. Una task que falla no escribe evento, así que a un consumidor no lo
puede arrancar un productor que no terminó.

En este tutorial, el productor de `labeled_projects` es `validate_transformed`,
el último paso de `dataops` en el diagrama de la §1, que escribirás en la §9.
El outlet va en esa task de **validación**, no en `transform`, la task
anterior, que es la que construye la tabla: si la validación falla, la task no
escribe evento, así que a `mlops` no lo pueden arrancar datos que no la
pasaron. **El consumidor se programa con "llegaron datos buenos", no con un
cron que adivina cuándo habrá terminado el Dag del productor.**

Mientras una task corre, su asset event todavía no existe: Airflow lo escribe
cuando la task termina con éxito. **`outlet_events`** es lo que permite a la
task preparar ese evento antes. Funciona como un dict con una clave por cada
outlet de la task: `outlet_events[LABELED_PROJECTS]` representa el evento que la
task va a emitir para `labeled_projects`, y el dict que asignes a su `.extra`
se guarda dentro de ese evento: aquí, cuántas filas validó la task y qué tags
encontró. La task recibe `outlet_events` declarando un parámetro con ese
nombre, igual que recibe `ti` en la §6; una task que no lo toca emite sus
eventos igualmente, con el `extra` vacío:

```python
@task(outlets=[LABELED_PROJECTS])
def validate_transformed(*, outlet_events: OutletEventAccessorsProtocol) -> dict:
    df = data.load_labeled()
    result = validate.run_suite(df, "labeled_projects")
    if not result.passed:
        raise AirflowFailException(...)
    outlet_events[LABELED_PROJECTS].extra = {"rows": len(df), "tags": sorted(df["tag"].unique())}
    return {...}
```

El tipo de `outlet_events` es `OutletEventAccessorsProtocol`, de
`airflow.sdk.types`. Este `extra` es distinto en cada evento, a diferencia del
`extra` fijo del asset. `AirflowFailException` hace fallar la task sin
reintentos; la §9 lo explica.

Y el consumidor, `mlops`, que escribirás en la §10. Ahí
`triggering_asset_events` — también inyectado por nombre — contiene los eventos
que arrancaron el run:

```python
@dag(schedule=[LABELED_PROJECTS], ...)
def mlops():
    @task
    def prepare_data(*, triggering_asset_events: Mapping[Asset, Sequence[Any]]):
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")
```

`triggering_asset_events` es un mapping de solo lectura que asocia cada asset
del `schedule` con la lista de sus eventos que arrancaron este run. Es una
lista porque un run puede recoger varios eventos del mismo asset: si `dataops`
emite tres veces mientras `mlops` está ocupado, el siguiente run de `mlops`
arranca con los tres (el apartado "Cuándo arranca el consumidor", al final de
esta sección, lo explica). La API pública de Airflow no tiene un tipo para
esos eventos, así que la anotación dice `Sequence[Any]`; los campos que conviene
conocer son `extra`, `timestamp`, y `source_dag_id` y `source_run_id`, el Dag
run que emitió el evento. Como con `ti`, estas anotaciones son para ti y para
tu editor: Airflow inyecta los argumentos por nombre e ignora sus tipos.

Hasta aquí, un asset, un productor y un consumidor. Un pipeline más grande
necesita más piezas; dos de ellas salen más adelante en este tutorial:

| Si necesitas… | Usa |
|---|---|
| que `mlops` espere a dos assets, o que le baste con uno de los dos | `AssetAll(a, b)` / `AssetAny(a, b)` en el `schedule` (§10) |
| que un Dag arranque con un mensaje de un sistema externo, sin ningún productor en Airflow | `AssetWatcher` (§11) |

Hay más; la §16 explica cómo se usan cuatro de ellas.

### Cuándo arranca el consumidor

Cuando Airflow escribe un asset event, añade también una entrada a una cola por
**cada Dag sin pausa** programado con ese asset. En su siguiente bucle, el
scheduler convierte esas entradas en Dag runs.

Los eventos que llegan más rápido de lo que el consumidor puede procesarlos no
se pierden ni se procesan de uno en uno. `mlops` permite un único run activo
(`max_active_runs=1`), así que si terminan tres runs de `dataops` mientras está
ocupado, el *siguiente* run de `mlops` se lleva los tres eventos, y
`triggering_asset_events` en `prepare_data` los lista todos. Un consumidor se
pone al día en un run, no en tres.

> **Un consumidor en pausa no encola, y no se pone al día.** Si `mlops` está en
> pausa cuando `dataops` emite, el asset event se escribe igualmente — lo verás
> en la página Assets —, pero no se encola ningún run para `mlops`, y **quitarle
> la pausa después no lo recupera**. Es lo contrario de la §7, donde un trigger
> *manual* crea el run y la pausa solo lo retiene. Lo mismo vale para un
> consumidor que todavía no existía cuando se escribió el evento: el run de
> `dataops` que harás en la §9 emitirá un evento que `mlops`, escrito en la §10,
> nunca verá. (Comprobado de las tres formas en este stack — ver [qué se
> verificó de verdad](../verified_ES.md).)

---

← [7. Scheduling y logical dates](07-scheduling-y-logical-dates.md) · [Índice](README.md) · [9. El Dag de DataOps](09-el-dag-de-dataops.md) →
