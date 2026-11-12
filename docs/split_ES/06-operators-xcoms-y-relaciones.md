## 6. Operators, XComs y relaciones

El Dag de la §4 usó funciones `@task`; este añade **operators**, los tipos de
task prefabricados, y mezcla los dos estilos en un mismo Dag. Con cualquiera de
los dos obtienes **tasks**, los pasos de un Dag; una escrita como función
`@task` es una *task de TaskFlow* (por debajo, Airflow la convierte en un
operator, como explica *¿De qué clase heredar?*). Los operators más comunes
vienen en `apache-airflow-providers-standard`, ya instalado:

```python
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
```

Aparte de estos, casi cualquier sistema tiene un provider — S3, Snowflake,
BigQuery, Postgres, Kubernetes, Databricks, Slack, HTTP, Docker, dbt Cloud,
Airbyte —, que se instala como `apache-airflow-providers-<nombre>`, aparece en
la UI bajo Admin → Providers y trae operators, hooks (los clientes sobre los
que se apoyan, *Hooks, Connections y Variables*) y sensors (tasks que esperan,
§11). Prefiere uno a escribir llamadas HTTP a mano: sus operators ya gestionan
connections, reintentos y búsqueda de secretos. Cuando ninguno encaja, escribe
el tuyo (*Escribir tu propio operator*).

### Constrúyelo: primero los XComs

El Dag, `dags/02_operators_and_xcoms.py`, conecta diez tasks de cuatro clases —
`EmptyOperator`, `PythonOperator`, `BashOperator` y `@task` de TaskFlow — y les
pasa un número de una a otra para enseñar cómo las tasks se pasan valores.

Los **XComs** ("cross-communications") son el mecanismo con el que las tasks
se pasan valores pequeños. Viven en la Metadata database, pero una task nunca
accede a ella directamente: los lee y escribe a través de la Task Execution API
del API server (§3). Las funciones `@task` de la §4 ya los usaban sin que se
viera — cada `return` un push de XCom, cada argumento un pull. Con operators
haces push y pull explícitos.

Crea el archivo con los imports y dos funciones Python normales para que las
llamen los `PythonOperator`:

```python dags/02_operators_and_xcoms.py
"""Lesson 2 - operators, XComs, and relationships."""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Label, chain, cross_downstream, dag, task
from airflow.sdk.types import RuntimeTaskInstanceProtocol

DEFAULT_ARGS = {
    "owner": "mlops-course",
    "retries": 1,
    "retry_delay": pendulum.duration(seconds=30),
}


def _push_seed(ti: RuntimeTaskInstanceProtocol) -> None:
    ti.xcom_push(key="seed", value=7)


def _multiply(ti: RuntimeTaskInstanceProtocol, factor: int) -> None:
    seed = ti.xcom_pull(task_ids="push_seed", key="seed")
    ti.xcom_push(key="product", value=seed * factor)
```

`ti`, la task instance, se inyecta por el nombre del parámetro, como
`**context` en la §4; su tipo, `RuntimeTaskInstanceProtocol` de
`airflow.sdk.types`, tiene `xcom_push` y `xcom_pull` entre sus métodos. Dos
cosas en las que fijarse:

**`xcom_pull` sin `task_ids` lee los XComs de la propia task actual** y devuelve
`None` en silencio. Casi todos los pull que escribas necesitan `task_ids=`;
`_multiply` nombra su origen.

**Los XComs son para metadatos, no para payloads**: cada valor acaba en la
Metadata database. Pasa una ruta, un run id, un recuento de filas; pon el
dataframe en object storage y el modelo en un registry. Airflow serializa más
que JSON — un `datetime`, un `set`, incluso un dataframe —, pero que un valor
quepa no significa que convenga.

**Toda task tiene que poder ejecutarse dos veces sin problema.** Con
`"retries": 1` en `DEFAULT_ARGS`, una task que falla se vuelve a ejecutar, así
que cada task tiene que ser **idempotente**: ejecutarla dos veces deja el mismo
resultado que ejecutarla una. Una que añade filas, incrementa un contador o
envía un email en cada intento no lo es: un reintento duplica las filas, la
cuenta o el email. Las formas habituales de conseguirlo:

- **Sobrescribe, no añadas.** Reemplaza la tabla o el archivo entero, o borra
  primero lo que vas a escribir.
- **Incluye el run en el nombre de las salidas.** Un archivo cuya ruta contiene el run
  id es el mismo archivo en cada intento.
- **Crea solo si no existe.** `CREATE TABLE IF NOT EXISTS`, por ejemplo, o el
  servicio `extra-databases` del archivo de compose (§2), que crea su base de
  datos solo si todavía no está.

La §9 y la §10 señalan dónde hacen esto sus tasks.

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

Un operator se instancia, no se llama: `task_id` lo nombra, el resto son sus
propios argumentos. `EmptyOperator` no hace nada y sirve de marcador o de punto
de unión; `PythonOperator` llama a la función que le des, con `op_kwargs` como
argumentos con nombre.

### Templates

El siguiente operator, un `BashOperator`, recibe un comando que no es un string
cualquiera. Los argumentos que un operator nombra en sus `template_fields`,
`bash_command` entre ellos, son **plantillas Jinja**. Jinja es la librería de
plantillas de Python que usan Flask y dbt: una plantilla es texto con huecos, y
*renderizarla* sustituye cada `{{ expresión }}` por su valor. Dentro de las
llaves puedes leer un atributo (`dag_run.run_after`), llamar a un método
(`ti.xcom_pull(...)`) o pasar un valor por un **filtro** con `|` —
`default('x')` da `'x'` cuando el valor no está definido. Airflow renderiza
estos argumentos *justo antes de que la task instance corra*, con el contexto
del run como variables, así que una task, escrita una vez, actúa cada vez sobre
el run id, la fecha o los valores de aguas arriba de ese run. Añádelo debajo de
`multiply`:

```python dags/02_operators_and_xcoms.py
    describe = BashOperator(
        task_id="describe",
        bash_command=(
            'echo "run={{ run_id }} ds={{ ds | default(\'none\') }} '
            'run_after={{ dag_run.run_after }} '
            'product={{ ti.xcom_pull(task_ids=\'multiply\', key=\'product\') }}"'
        ),
    )
```

Tres cosas que saber sobre las plantillas:

1. **El renderizado ocurre por task instance**, en el proceso de la propia
   task, así que una plantilla puede acceder a cualquier cosa del task context
   — las mismas claves que `**context` le dio a un `@task` en la §4:

   - `run_id` — el id de este run, p. ej. `manual__2026-09-09T08:22:31+00:00`.
   - `dag_run` — el run en sí: su `run_id`, `run_after` (cuándo le tocaba
     arrancar), `conf` (el JSON pasado con un trigger), `state`.
   - `ti` — esta task instance, el objeto con el que `_multiply` hizo push y
     pull de XComs.
   - `params` — las entradas declaradas del Dag (§4), con lo que se sobrescriba
     en cada run.
   - `ds` — la logical date del run (§7) como `YYYY-MM-DD`; el párrafo sobre
     `| default(...)` que sigue a esta lista explica por qué necesita un
     default.
   - `var` y `conn` — las Variables y Connections de Airflow, leídas por
     nombre: `{{ var.value.min_rows }}`, `{{ conn.warehouse.host }}`. *Hooks,
     Connections y Variables* cuenta qué guardan.

   El resultado se guarda en la pestaña **Rendered Templates** de la task
   instance (§5): mira ahí cuando un comando no dijo lo que querías.

2. **Solo se renderizan los `template_fields`** (en `BashOperator`:
   `bash_command`, `env`, `cwd`). Cualquier otro string — otro argumento, o uno
   en el cuerpo de una función `@task` — conserva sus llaves (`{{ }}`) como
   texto literal: Airflow nunca lo mira, y Python no sabe nada de Jinja. Un log
   que muestra `{{ … }}` sin sustituir suele significar eso.

3. **Una función `@task` rara vez necesita esto**: recibe el contexto como
   Python (`**context`, o un argumento con nombre como `ti`). Los argumentos
   con los que la llamas sí se renderizan: `@task` convierte la función en un
   operator que los guarda en sus atributos `op_args` y `op_kwargs`, nombrados
   los dos en sus `template_fields`. Si `show` es una función `@task` que
   imprime su argumento, `show("run={{ run_id }}")` imprime el run id real.

El `| default(...)` sobre `ds` es la versión en plantilla del `.get()` de la
§4. `ds` deriva de `logical_date`, y un run disparado por asset, o desde la CLI
sin logical date, no la tiene (la §7 explica por qué). Sin el default, la task
falla al renderizar, antes de ejecutarse, con `UndefinedError: 'ds' is
undefined`, que `airflow dags test` nunca enseña (§4).

Si solo necesitas una fecha del run, por ejemplo para nombrar un archivo, usa
`{{ dag_run.run_after }}`: todo run tiene una, así que no hace falta ningún
default. No siempre es la logical date; la §7 explica cuándo difieren.

Un template field también puede contener el nombre de un archivo, para que un
script o una consulta SQL larga viva en su propio archivo y no en un string
dentro del Dag. Cuando el valor termina en una extensión que el operator nombra
en `template_ext` — `(".sh", ".bash")` en `BashOperator` —, Airflow renderiza
ese archivo, que busca junto al archivo del Dag: `bash_command="describe.sh"`
ejecuta `describe.sh` con sus `{{ run_id }}` ya sustituidos. Un `@task` hace lo
mismo con dos argumentos del decorador:
`@task(templates_exts=[".sql"], templates_dict={"query": "query.sql"})` le pasa
el contenido renderizado de `query.sql` al parámetro `templates_dict` de la
función.

Ahora una task de TaskFlow en el mismo Dag, leyendo el XCom de un operator:

```python dags/02_operators_and_xcoms.py
    @task
    def taskflow_reader(**context: Any) -> int:
        product = context["ti"].xcom_pull(task_ids="multiply", key="product")
        print(f"TaskFlow saw product={product}")
        return product
```

El mismo `ti.xcom_pull` funciona desde los dos lados: un `@task` lee el XCom de
un operator y viceversa. Para llamar a una función Python, `@task` y
`PythonOperator` son equivalentes — la documentación de Airflow presenta
`@task` como el equivalente de `PythonOperator` en TaskFlow y no recomienda
ninguno —, así que elegir es cuestión de estilo: `PythonOperator` recibe la
función y sus argumentos por separado (`python_callable`, `op_kwargs`, como
`push_seed` y `multiply`); con `@task`, llamar a la función crea la task.

### Relaciones

Nada de lo de arriba está conectado todavía. Termina el archivo con las tasks
que faltan y las aristas entre todas:

```python dags/02_operators_and_xcoms.py
    chain(start, push_seed, Label("seed -> product"), multiply, describe)

    product = taskflow_reader()
    describe >> product

    check_parity = BashOperator(
        task_id="check_parity",
        bash_command="test $(( {{ ti.xcom_pull(task_ids='taskflow_reader') }} % 2 )) -eq 0 && echo even",
    )
    check_range = BashOperator(
        task_id="check_range",
        bash_command="test {{ ti.xcom_pull(task_ids='taskflow_reader') }} -lt 100 && echo 'under 100'",
    )
    product >> [check_parity, check_range] >> EmptyOperator(task_id="checks_done")

    publish = EmptyOperator(task_id="publish")
    notify = EmptyOperator(task_id="notify")
    cross_downstream([check_parity, check_range], [publish, notify])


operators_and_xcoms()
```

`check_parity` y `check_range` leen de XCom el valor que devolvió
`taskflow_reader` y lo comprueban con `test`, que sale con error si la
condición es falsa, y eso hace fallar la task.

Una arista entre dos tasks se declara de una de estas cinco formas, y este
código usa las cuatro últimas:

1. **Implícitamente, pasando la salida de una task a otra**, como hacía el Dag
   de la §4 con los valores que devolvían los `@task`. La salida de un operator
   también sirve: pasar `describe.output` como argumento crearía la misma
   arista. `taskflow_reader` no recibe ningún argumento, así que su arista con
   `describe` necesita la forma 2.
2. **Los operadores de desplazamiento**, cuando no fluyen datos:
   `describe >> product` es "describe, y luego product". `>>` solo fija el
   orden y no pasa nada aguas abajo, y por eso las comprobaciones leen su valor
   de XCom. `product << describe` es la misma arista al revés, y también lo son
   `describe.set_downstream(product)` y `product.set_upstream(describe)`, los
   métodos a los que llaman `>>` y `<<` por debajo.
3. **Fan-out y fan-in**: una lista a un lado de `>>` (solo a uno; ver la forma
   5) crea una arista por cada task de la lista, así que
   `product >> [check_parity, check_range] >> checks_done` son cuatro
   dependencias, no dos: `product` → `check_parity`, `product` → `check_range`,
   `check_parity` → `checks_done` y `check_range` → `checks_done`. Las dos
   comprobaciones arrancan cuando termina `product`; `checks_done` espera a que
   ambas terminen con éxito.
4. **`chain()`**, la forma legible de una línea larga de `>>`:
   `chain(start, push_seed, Label("seed -> product"), multiply, describe)` es
   `start >> push_seed >> Label("seed -> product") >> multiply >> describe`. Un
   `Label` no es una task: colocado entre dos tasks (aquí `push_seed` y
   `multiply`), muestra su texto en la arista entre ellas en la vista Graph,
   igual con `chain()` que con `>>`.
5. **`cross_downstream()`**, para conectar cada task de una lista con cada task
   de otra, algo que parece que `>>` debería hacer y no hace:

   ```python
   [a, b] >> [c, d]              # TypeError: no operator between two lists
   chain([a, b], [c, d])         # legal, but PAIRS them: a->c and b->d only,
                                 # and raises if the lists differ in length
   cross_downstream([a, b], [c, d])   # what you actually wanted: all-to-all
   ```

`chain_linear` hace lo mismo que `cross_downstream` con más de dos grupos
seguidos: `chain_linear([a, b], [c, d], [e, f])` conecta cada task de un grupo
con cada task del siguiente.

### Ejecútalo y mira el grafo

```bash
docker compose exec airflow-scheduler airflow dags test operators_and_xcoms
```

**Comprobación.** En la salida busca la línea de `describe`,
`run=manual__… ds=… run_after=… product=42`, luego `TaskFlow saw product=42` de
`taskflow_reader`, y `even` y `under 100` de las comprobaciones. Después abre
la vista **Graph** del Dag en la UI: la etiqueta `seed -> product` en una
arista, un fan-out de dos ramas hacia `check_parity`/`check_range`, y esas dos
abriéndose hacia `checks_done`, `publish` y `notify`. Si el grafo no coincide,
compara con
[`solutions/02_operators_and_xcoms.py`](../../solutions/02_operators_and_xcoms.py).

### Escribir tu propio operator

Cuando ningún provider tiene un operator para un paso, puedes heredar de
`BaseOperator` y escribir su método `execute`. Normalmente no hace falta: una
función `@task` hace el mismo trabajo con menos código y se comparte igual de
bien — defínela en un módulo, impórtala en cada Dag que la necesite y llámala
con argumentos distintos, que incluso se renderizan como plantillas (el tercer
punto de *Templates*).

Una clase compensa su código extra cuando el paso necesita algo que una función
no tiene:

- **Valores de los argumentos comprobados al parsear el archivo.** Una llamada
  a un `@task` falla al parsear si falta un argumento, pero no puede comprobar
  los valores. El `__init__` de un operator corre cada vez que el Dag
  processor parsea el archivo (§3), porque parsear instancia todos los
  operators, y puede hacer cualquier comprobación: un valor erróneo aparece
  enseguida como error de import en la UI, no como un fallo de la task horas
  después.
- **Métodos que Airflow llama mientras corre la task.** `on_kill` corre cuando
  se detiene la task (por ejemplo, cuando marcas la task instance como failed
  en la UI), así que puede cancelar el trabajo que la task lanzó en otro
  servicio, como una consulta en BigQuery. `self.defer(...)` suspende la task y
  le pasa la espera al Triggerer (§3); así están construidos los operators
  deferrable de la §11.
- **Botones propios en la UI.** El atributo de clase `operator_extra_links`
  añade una sección **Extra Links** a la vista de la task instance, con un
  botón por enlace. Cada enlace es una subclase de `BaseOperatorLink` cuyo
  método `get_link` calcula la URL para cada task instance, normalmente la
  página del trabajo que la task lanzó en el servicio con el que habla: el job
  de Databricks, la consulta de BigQuery. `@task` no tiene argumento para
  esto: `@task(operator_extra_links=...)` falla con `TypeError` (la §10
  muestra cómo sortearlo).

Los operators de los providers necesitan estas cosas, y por eso son clases. El
paso de entrenamiento de abajo necesita la primera: falla cuando el f1 del
modelo (su métrica de calidad, de 0 a 1) queda por debajo de un umbral,
`min_f1`, y un `min_f1` escrito como `65` en vez de `0.65` debería detectarse
cuando el Dag processor parsea el archivo, no después de un entrenamiento:

```python
from airflow.sdk import BaseOperator

from project_tagger import train as train_module


class ProjectTaggerTrainOperator(BaseOperator):
    template_fields = ("experiment_name",)

    def __init__(self, *, experiment_name: str, min_f1: float, **kwargs) -> None:
        super().__init__(**kwargs)          # task_id, retries, ... go to BaseOperator
        if not 0 < min_f1 <= 1:             # runs at parse time
            raise ValueError(f"min_f1 must be between 0 and 1, got {min_f1}")
        self.experiment_name = experiment_name
        self.min_f1 = min_f1

    def execute(self, context):
        print(f"experiment={self.experiment_name}")   # already rendered here
        _, metrics = train_module.train_from_files()
        if metrics["f1"] < self.min_f1:
            raise ValueError(f"f1={metrics['f1']:.4f} is below min_f1={self.min_f1}")
        return metrics["f1"]                # pushed as an XCom
```

Un Dag lo instancia como cualquier otro operator:

```python
train = ProjectTaggerTrainOperator(
    task_id="train",
    experiment_name="project_tagger_{{ run_id }}",
    min_f1=0.65,
)
```

`__init__` corre cuando se parsea el archivo del Dag, comprueba `min_f1` y
guarda los argumentos: con `min_f1=65` el Dag no llega a cargarse, y la UI
muestra el error de import `ValueError: min_f1 must be between 0 and 1, got 65`.
`execute` corre cuando corre la task instance, después de que Airflow haya
renderizado cada atributo nombrado en `template_fields`, así que
`self.experiment_name` contiene `project_tagger_manual__…`, no las llaves. Lo
que devuelva `execute` se guarda como XCom, igual que el `return` de una
función `@task`.

`template_fields` une el argumento con el renderizado. Es un atributo de clase,
una tupla vacía en `BaseOperator` que cada operator sobrescribe, y contiene
**nombres de atributos**: `"experiment_name"` significa `self.experiment_name`,
así que `__init__` tiene que guardar el argumento exactamente con ese nombre.
Antes de llamar a `execute`, Airflow lee de `self` cada atributo de la lista,
lo renderiza y vuelve a guardar el resultado. Se declara en la clase, no se
pasa a `super().__init__()`, porque describe el tipo de operator, igual para
todas sus instancias; una subclase amplía la lista de su padre con
`template_fields = (*Parent.template_fields, "new_field")`. `min_f1` queda
fuera a propósito: al parsear, una plantilla todavía es texto sin renderizar,
que `__init__` no podría comprobar.

El mismo paso como `@task` solo puede hacer la comprobación dentro de la
función:

```python
@task
def train(experiment_name: str, min_f1: float) -> float:
    if not 0 < min_f1 <= 1:                       # runs only when the task runs
        raise ValueError(f"min_f1 must be between 0 and 1, got {min_f1}")
    print(f"experiment={experiment_name}")        # rendered before the call
    _, metrics = train_module.train_from_files()
    if metrics["f1"] < min_f1:
        raise ValueError(f"f1={metrics['f1']:.4f} is below min_f1={min_f1}")
    return metrics["f1"]

train(experiment_name="project_tagger_{{ run_id }}", min_f1=0.65)
```

Con `min_f1=65` este Dag carga sin errores, y el fallo solo aparece cuando un
run llega a `train`.

> Esto vale para un valor escrito en el archivo del Dag. Un param (§4) llega
> al disparar el run, después de parsear, así que ni la clase ni el `@task`
> pueden comprobarlo antes de que corra la task. Para validarlo antes, pon el
> límite en el propio `Param`:
> `Param(0.65, type="number", exclusiveMinimum=0, maximum=1)` hace que Airflow
> rechace un trigger con `min_f1=65` antes de crear el run.

**¿De qué clase heredar?** De la clase cuyo comportamiento quieras conservar.
`BaseOperator` trae lo que comparten todas las tasks (`task_id`, reintentos,
plantillas) y deja `execute` para ti: es el padre para un operator que hace
algo nuevo, como `ProjectTaggerTrainOperator`. Para tener lo que hace un
operator ya hecho más algo, hereda de ese operator, que es lo habitual: la
mayoría de los operators de los providers derivan de otro operator, no de
`BaseOperator` directamente.

`PythonOperator`, por ejemplo, es el padre para llamar a una función más algo
extra: `BranchPythonOperator`, `ShortCircuitOperator`,
`PythonVirtualenvOperator` y `_PythonDecoratedOperator`, la clase que crea
`@task`, están hechos sobre él — por eso los logs muestran
`<Task(_PythonDecoratedOperator): taskflow_reader>`, aunque la UI la etiquete
como `@task`. No es el padre adecuado para `ProjectTaggerTrainOperator`, cuyo
propio `execute` reemplazaría lo único que hace `PythonOperator`, y aun así
tendría que recibir `python_callable`, que es obligatorio.

### Hooks, Connections y Variables

Un operator es una task entera que hace una cosa fija: `SQLExecuteQueryOperator`
ejecuta una consulta, `SqlToS3Operator` copia el resultado de una consulta a un
archivo en S3. Un **hook** es el cliente sobre el que se apoya, y todo operator
que habla con un servicio fuera de Airflow (una base de datos, S3, una API)
pasa por uno. Recibe un `conn_id` — el nombre de una **Connection**, las
credenciales de un servicio que Airflow guarda con ese nombre (se explican más
abajo) —, construye el cliente autenticado y te da métodos a los que llamar:
`PostgresHook.get_df` ejecuta una consulta y devuelve un dataframe,
`S3Hook.load_string` escribe un string en S3.

Llamas a un hook tú mismo, dentro de un `@task`, cuando una task tiene que hacer
varias llamadas con tu propio código entre ellas, algo que ningún operator hace
por sí solo. Ninguno de los Dags del tutorial necesita uno; este ejemplo supone
que los proyectos están en una base de datos Postgres y exporta un CSV a S3:

```python
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

@task
def export_labeled_projects(day: str) -> str:
    df = PostgresHook(postgres_conn_id="warehouse").get_df(
        "SELECT * FROM projects WHERE created_on::date = %(day)s",
        parameters={"day": day},
    )
    df = df[df["tag"].notna()]                  # your own code, between the two calls
    key = f"exports/labeled_projects_{day}.csv"
    S3Hook(aws_conn_id="aws_default").load_string(
        df.to_csv(index=False), key=key, bucket_name="project-tagger-exports"
    )
    return key
```

La task nombra dos Connections, `warehouse` y `aws_default`, nunca una
contraseña. Un servicio sin hook también puede usar
una Connection: `BaseHook.get_connection("warehouse")`, de la clase de la que
derivan todos los hooks, devuelve la Connection en sí — host, login, password,
extra — para que se la pases a cualquier librería cliente.

Una **Connection** es un conjunto de credenciales con nombre para un servicio
(`conn_type`, host, login, password, extra). Una **Variable** es un par
clave-valor global a toda la instalación, para configuración que no quieres
escribir en el código, como un límite o el nombre de un bucket; cualquier task
de cualquier Dag la lee con `Variable.get("min_rows", default=500)`. Las dos se
definen de tres maneras:

- **En la UI** (Admin → Connections / Variables) **o con la CLI**
  (`airflow connections add`, `airflow variables set`). Se guardan en las
  tablas `connection` y `variable` de la Metadata database, con la contraseña,
  el extra y el valor de cada Variable cifrados con la clave de
  `[core] fernet_key`; sin clave, como en este stack, se guardan en texto
  plano.
- **Como variable de entorno** `AIRFLOW_CONN_<ID>` / `AIRFLOW_VAR_<CLAVE>`, sin
  pasar por la base de datos ni aparecer en la UI. Así define el archivo de
  compose `google_cloud_default`, la Connection que usa el sensor de BigQuery
  de la §11, y `fs_default`, la que usa por defecto un `FileSensor`.
- **En un secrets backend** (Vault, AWS Secrets Manager…), lo habitual en
  producción (§16).

Las plantillas acceden a ellas con `{{ conn.warehouse.host }}` y
`{{ var.value.min_rows }}`.

> Una task que consulta cualquiera de las dos nunca toca la Metadata database.
> Primero mira las variables de entorno `AIRFLOW_CONN_*` / `AIRFLOW_VAR_*`,
> que es donde encuentra `google_cloud_default`, y si no, se la pide al API
> server por la Task Execution API, que es lo que permite a un worker remoto
> resolver una Connection sin acceso a la Metadata database.
>
> Prefiere un **param** (§4) a una Variable global cuando el valor sea un ajuste
> que cambia lo que decide el Dag, como un umbral: los params quedan registrados
> con el run que los usó, las Variables no.

---

← [5. La UI](05-la-ui.md) · [Índice](README.md) · [7. Scheduling y logical dates](07-scheduling-y-logical-dates.md) →
