## 6. Operators, XComs y relaciones

La lección 1 usó funciones decoradas con `@task`. Esta usa **operators**, los
tipos de task prefabricados, y enseña que los dos estilos se mezclan sin problema
en un mismo Dag. Los más comunes vienen en `apache-airflow-providers-standard`,
que ya está instalado:

```python
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
```

Más allá de los estándar hay un provider para casi cualquier sistema con el que
quieras hablar — S3, Snowflake, BigQuery, Postgres, Kubernetes, Databricks,
Slack, HTTP, Docker, dbt Cloud, Airbyte — y cada uno trae operators, hooks y
sensors, se instala como `apache-airflow-providers-<nombre>` y aparece en la UI
bajo Admin → Providers. Usa uno antes de escribir llamadas HTTP a mano: el
operator de un provider ya gestiona connections, reintentos y búsqueda de
secretos.

Cuando ninguno encaja, hereda de `BaseOperator` — no en esta lección, pero esta
es la forma:

```python
from airflow.sdk import BaseOperator

class TagifaiTrainOperator(BaseOperator):
    template_fields = ("experiment_name",)      # these get Jinja-rendered

    def __init__(self, *, experiment_name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.experiment_name = experiment_name

    def execute(self, context):                 # return value becomes the XCom
        return train_module.train_from_files()[1]["f1"]
```

Es la opción correcta cuando un paso se reutiliza en varios Dags y
necesita sus propios parámetros, reintentos o template fields. Para todo lo
demás, `@task` es menos código y es lo que usa este repo de principio a fin.

### Constrúyelo: primero los XComs

El Dag que vas a escribir, `dags/02_operators_and_xcoms.py`, empuja un número a
través de cinco tasks de tres clases distintas. Lo que enseña es cómo las tasks
se pasan valores entre sí.

Los **XComs** ("cross-communications") son cómo las tasks se pasan valores
pequeños. Se guardan en la metadata database — y, como todo lo que hace una
task, una task los lee y escribe a través de la Task Execution API, nunca tocando
la base de datos. TaskFlow los usó implícitamente en la lección 1: cada `return`
era un push de XCom, cada argumento un pull de XCom. Con operators haces push y
pull explícitos.

Crea `dags/02_operators_and_xcoms.py` con los imports y dos funciones Python
normales que llamará un `PythonOperator`:

```python dags/02_operators_and_xcoms.py
"""Lesson 2 - operators, XComs, and relationships."""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Label, chain, cross_downstream, dag, task

DEFAULT_ARGS = {
    "owner": "mlops-course",
    "retries": 1,
    "retry_delay": pendulum.duration(seconds=30),
}


def _push_seed(ti) -> None:
    ti.xcom_push(key="seed", value=7)


def _multiply(ti, factor: int) -> None:
    seed = ti.xcom_pull(task_ids="push_seed", key="seed")
    ti.xcom_push(key="product", value=seed * factor)
```

`ti` — la task instance — se inyecta porque el parámetro se llama `ti`, la misma
inyección por nombre que hizo `**context` en la lección 1. Dos cosas en las que
fijarse:

**`xcom_pull` sin `task_ids` lee los XComs de la propia task actual** y devuelve
`None` en silencio. Casi todos los pull que escribas necesitan `task_ids=`; el
`_multiply` de arriba nombra su origen.

> **Los XComs son para metadatos, no para payloads.** Viven en la metadata
> database. Pasa una ruta, un run id, un recuento de filas; pon el dataframe en
> object storage y el modelo en un registry. Los valores tienen que ser
> serializables a JSON.

Ahora la cabecera del Dag y los tres primeros operators:

```python dags/02_operators_and_xcoms.py

@dag(
    dag_id="operators_and_xcoms",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "basics"],
    doc_md=__doc__,
)
def operators_and_xcoms():
    start = EmptyOperator(task_id="start")
    push_seed = PythonOperator(task_id="push_seed", python_callable=_push_seed)
    multiply = PythonOperator(
        task_id="multiply",
        python_callable=_multiply,
        op_kwargs={"factor": 6},
    )
```

Un operator se instancia, no se llama: `task_id` lo nombra, y el resto son sus
propios argumentos. `EmptyOperator` no hace nada y se usa como marcador o como
punto de unión; `PythonOperator` llama a la función que le des, con `op_kwargs`
como argumentos con nombre.

### Templates

El siguiente operator es un `BashOperator`, y su comando no es una cadena
normal. Algunos argumentos de operator — `bash_command`, y cualquier campo que un
operator nombre en sus `template_fields` — son **plantillas Jinja**: cadenas en
las que `{{ … }}` se rellena *justo antes de que la task instance corra*, con el
contexto de ese run. Es lo que hace que una task, escrita una vez, actúe cada vez
sobre un run id, una fecha o un valor de aguas arriba distinto. Añádelo debajo de
`multiply`:

```python dags/02_operators_and_xcoms.py
    describe = BashOperator(
        task_id="describe",
        bash_command=(
            'echo "run={{ run_id }} ds={{ ds | default(\'none (manual run)\') }} '
            'run_after={{ dag_run.run_after }} '
            'product={{ ti.xcom_pull(task_ids=\'multiply\', key=\'product\') }}"'
        ),
    )
```

Tres consecuencias. El renderizado ocurre por task instance, en el proceso de la
propia task, así que una plantilla puede acceder a cualquier cosa del task context
— `run_id`, `ds`, `ti`, `params`, `var`, `conn`, `dag_run` — y el resultado se
guarda: es la pestaña **Rendered Templates** de la task instance (§5), el sitio
donde mirar cuando un comando no dijo lo que querías. Una cadena que *no* está en
`template_fields` no se renderiza nunca, por muchas llaves que lleve. Y una
función `@task` no necesita nada de esto — recibe el contexto como Python
(`**context`, o un argumento con nombre como `ti`) — así que el templating es
sobre todo cosa de operators.

El `| default(...)` sobre `ds` es el gemelo, en plantilla, del `.get()` de la
lección 1: `ds` deriva de `logical_date`, y un run manual o disparado por asset
no tiene ninguna. Sin el default la task falla con `UndefinedError: 'ds' is
undefined` mientras renderiza, antes de ejecutarse — y, como con `KeyError`,
`airflow dags test` nunca lo enseña. La §7 es donde se explican ambas trampas;
`{{ dag_run.run_after }}` es la alternativa segura, porque todo run tiene uno.

Ahora una task de TaskFlow en el mismo Dag, leyendo el XCom de un operator:

```python dags/02_operators_and_xcoms.py
    @task
    def taskflow_reader(**context) -> int:
        product = context["ti"].xcom_pull(task_ids="multiply", key="product")
        print(f"TaskFlow saw product={product}")
        return product
```

Las tasks de TaskFlow y los operators se mezclan sin problema: un `@task` puede
leer el XCom de un operator y viceversa, y el mismo `ti.xcom_pull` funciona
desde cualquiera de los dos lados.

### Relaciones

Nada de lo de arriba está conectado todavía. Una arista entre dos tasks se
declara de una de estas cuatro formas, y este Dag usa todas. Termina el archivo
con el cableado:

```python dags/02_operators_and_xcoms.py
    chain(start, push_seed, Label("seed -> product"), multiply, describe)

    product = taskflow_reader()
    describe >> product

    check_parity = BashOperator(task_id="check_parity", bash_command='echo "parity checked"')
    check_range = BashOperator(task_id="check_range", bash_command='echo "range checked"')
    product >> [check_parity, check_range] >> EmptyOperator(task_id="checks_done")

    publish = EmptyOperator(task_id="publish")
    notify = EmptyOperator(task_id="notify")
    cross_downstream([check_parity, check_range], [publish, notify])


operators_and_xcoms()
```

1. **Implícitamente, pasando la salida de una task a otra.** Solo TaskFlow, y
   la lección 1 estaba hecha de eso. Aquí `taskflow_reader` no recibe ningún
   argumento, así que nada le dibuja su arista.
2. **Los operadores de desplazamiento**, cuando no fluyen datos —
   `describe >> product` es "describe, y luego product". `product << describe`
   es la misma arista al revés, y también lo son
   `describe.set_downstream(product)` y `product.set_upstream(describe)`, los
   métodos que los operators llaman por debajo; úsalos cuando las tasks se
   construyan en un bucle y `>>` se lea mal.
3. **Fan-out y fan-in**: una lista a cualquier lado de `>>` crea una arista por cada elemento,
   así que `product >> [check_parity, check_range] >> checks_done` son cuatro
   dependencias, no dos.
4. **`chain()`** — la forma legible de una línea larga de `>>`, y la única que
   puede llevar un `Label`, que anota la arista en la vista Graph.

Y una quinta, `cross_downstream`, para conectar cada task de una lista con cada
task de otra — algo que parece que debería funcionar con `>>` y no funciona:

```python
[a, b] >> [c, d]              # TypeError: no operator between two lists
chain([a, b], [c, d])         # legal, but PAIRS them: a->c and b->d only,
                              # and raises if the lists differ in length
cross_downstream([a, b], [c, d])   # what you actually wanted: all-to-all
```

`cross_downstream` es el producto cartesiano — cada task de la izquierda aguas
arriba de cada task de la derecha. `chain_linear` es su hermano para encadenar
varios de esos grupos en secuencia.

### Ejecútalo y mira el grafo

```bash
docker compose exec airflow-scheduler airflow dags test operators_and_xcoms
```

**Comprobación.** La línea de la task `describe`, `run=manual__… ds=…
product=42`, y luego `TaskFlow saw product=42` del reader. Después abre la vista
**Graph** del Dag en la UI: la etiqueta `seed -> product` en una arista, un
fan-out de dos ramas hacia `check_parity`/`check_range`, y esas dos abriéndose
hacia `publish` y `notify`. Si el grafo no coincide, compara con
[`solutions/02_operators_and_xcoms.py`](../../solutions/02_operators_and_xcoms.py).

### Hooks, Connections y Variables

Un operator es la *task*; un **hook** es el cliente que hay debajo. Los hooks
reciben un `conn_id`, resuelven la Connection, construyen el cliente autenticado
y exponen métodos — `S3Hook.load_file`, `PostgresHook.get_pandas_df`. Usa uno
directamente dentro de un `@task` cuando quieras la gestión de la connection pero
no un operator entero. Ninguno de los Dags del tutorial necesita uno; la forma:

```python
from airflow.sdk import BaseHook, Connection, Variable
from airflow.providers.standard.hooks.filesystem import FSHook

@task
def land_file() -> str:
    base = FSHook(fs_conn_id="fs_default").get_path()       # "/" here
    threshold = Variable.get("min_rows", default=500)
    conn = BaseHook.get_connection("fs_default")            # or Connection.get(...)
    ...
```

Las **Connections** guardan credenciales (`conn_type`, host, login, password,
extra); las **Variables** guardan configuración. Ambas son editables bajo Admin,
se pueden fijar desde la CLI y se pueden sobrescribir con una variable de
entorno `AIRFLOW_CONN_*` / `AIRFLOW_VAR_*` — que es como se define `fs_default`
en el archivo de compose de este repo. Las plantillas acceden a ellas con
`{{ conn.fs_default.host }}` y `{{ var.value.my_key }}`.

> Una task que consulta cualquiera de las dos no toca la metadata database: la
> petición va por la Task Execution API, que es lo que permite a un worker remoto
> resolver una Connection sin tener acceso a la base de datos.
>
> Prefiere un **param** (§5) a una Variable global cuando el valor sea un ajuste
> que cambia lo que decide el Dag, como un umbral: los params quedan registrados
> con el run que los usó, las Variables no.

---

← [5. La UI](05-la-ui.md) · [Índice](README.md) · [7. Scheduling y logical dates](07-scheduling-y-logical-dates.md) →
