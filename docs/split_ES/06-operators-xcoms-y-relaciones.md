## 6. Operators, XComs y relaciones

El Dag de la §4 usó funciones decoradas con `@task`. Este usa **operators**, los
tipos de task prefabricados, y enseña cómo los dos estilos se mezclan sin problema
en un mismo Dag. Los dos estilos producen **tasks**: una task es cualquier paso de
un Dag, y una *task de TaskFlow* es una escrita como función `@task` en lugar de
instanciando un operator a mano (por debajo, Airflow la convierte igualmente en
un operator; lo explica *¿De qué clase heredar?*, más abajo). Los operators más comunes vienen en
`apache-airflow-providers-standard`, que ya está instalado:

```python
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
```

Aparte de estos hay un provider para casi cualquier sistema con el que
quieras hablar — S3, Snowflake, BigQuery, Postgres, Kubernetes, Databricks,
Slack, HTTP, Docker, dbt Cloud, Airbyte — y cada uno trae operators, hooks (los
clientes sobre los que se apoyan, *Hooks, Connections y Variables* de la §6) y
sensors (tasks que esperan, §11), se instala como `apache-airflow-providers-<nombre>` y aparece en la UI
bajo Admin → Providers. Usa uno antes de escribir llamadas HTTP a mano: el
operator de un provider ya gestiona connections, reintentos y búsqueda de
secretos. Cuando ninguno encaja, puedes escribir el tuyo; la subsección
*Escribir tu propio operator* de la §6 enseña cómo, una vez construido este Dag.

### Constrúyelo: primero los XComs

El Dag que vas a escribir, `dags/02_operators_and_xcoms.py`, conecta diez tasks
de cuatro clases — `EmptyOperator`, `PythonOperator`, `BashOperator` y `@task`
de TaskFlow — y les pasa un número de una a otra. Lo que enseña es cómo las tasks
se pasan valores entre sí.

Los **XComs** ("cross-communications") son el mecanismo con el que las tasks se
pasan valores pequeños. Se guardan en la metadata database, pero una task nunca accede a la
metadata database directamente: lee y escribe los XComs pidiéndoselo al API
server, a través de la Task Execution API (§3).
Las funciones `@task` de la §4 ya los usaban sin que se viera: cada `return` era un push de XCom,
cada argumento un pull de XCom. Con operators haces push y pull explícitos.

Crea `dags/02_operators_and_xcoms.py` con los imports y dos funciones Python
normales que llamará un `PythonOperator`:

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

`ti` — la task instance — se inyecta porque el parámetro se llama `ti`, la misma
inyección por nombre que hizo `**context` en la §4. Su tipo,
`RuntimeTaskInstanceProtocol` de `airflow.sdk.types`, describe ese objeto, con
`xcom_push` y `xcom_pull` entre sus métodos. Dos cosas en las que fijarse:

**`xcom_pull` sin `task_ids` lee los XComs de la propia task actual** y devuelve
`None` en silencio. Casi todos los pull que escribas necesitan `task_ids=`; el
`_multiply` de arriba nombra su origen.

**Los XComs son para metadatos, no para payloads.** Viven en la metadata
database. Pasa una ruta, un run id, un recuento de filas; pon el dataframe en
object storage y el modelo en un registry. Airflow serializa el valor al
guardarlo y acepta más que JSON — un `datetime`, un `set`, incluso un dataframe
—, pero que quepa no significa que convenga.

**Toda task tiene que poder ejecutarse dos veces sin problema.** `"retries": 1`
en `DEFAULT_ARGS` significa que una task que falla se vuelve a ejecutar. Así
que escribe cada task para que sea **idempotente**: ejecutarla dos veces deja
el mismo resultado que ejecutarla una. Una task que añade filas, incrementa un
contador o envía un email en cada intento no lo es: un reintento duplica las
filas, la cuenta o el email. Las formas habituales de conseguirlo:

- **Sobrescribe, no añadas.** Reemplaza la tabla o el archivo entero, o borra
  primero lo que vas a escribir.
- **Incluye el run en el nombre de las salidas.** Un archivo cuya ruta contiene el run
  id es el mismo archivo en cada intento.
- **Crea solo si no existe.** `CREATE TABLE IF NOT EXISTS`, por ejemplo. El
  servicio `extra-databases` del archivo de compose (§2) funciona así: crea su
  base de datos solo si todavía no está.

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

Un operator se instancia, no se llama: `task_id` lo nombra, y el resto son sus
propios argumentos. `EmptyOperator` no hace nada y se usa como marcador o como
punto de unión; `PythonOperator` llama a la función que le des, con `op_kwargs`
como argumentos con nombre.

### Templates

El siguiente operator es un `BashOperator`, y su comando no es un string
cualquiera. Algunos argumentos de operator — los que el operator nombra en sus
`template_fields`, `bash_command` entre ellos — son **plantillas Jinja**. Jinja es
una librería de plantillas de Python, la misma que usan Flask y dbt: una
plantilla es texto normal con huecos, y *renderizarla* sustituye cada
`{{ expresión }}` por el valor de esa expresión. Dentro de las llaves puedes
leer un atributo (`dag_run.run_after`), llamar a un método (`ti.xcom_pull(...)`)
o pasar un valor por un **filtro** con `|` — `default('x')` da `'x'` cuando el
valor no está definido. Airflow renderiza estos argumentos *justo antes de que
la task instance corra*, con el contexto de ese run como variables. Es lo que
hace que una task, escrita una vez, actúe cada vez sobre un run id, una fecha o
un valor de aguas arriba distinto. Añádelo debajo de `multiply`:

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
   - `ds` — la logical date del run como `YYYY-MM-DD`. La §7 explica las
     logical dates; el párrafo sobre `| default(...)` que sigue a esta lista
     explica por qué `ds` necesita un default.
   - `var` y `conn` — las Variables y Connections de Airflow, leídas por
     nombre: `{{ var.value.min_rows }}`, `{{ conn.warehouse.host }}`. *Hooks,
     Connections y Variables*, al final de la §6, cuenta qué guardan.

   Y el resultado se guarda: es la pestaña **Rendered Templates** de la task
   instance (§5), el sitio donde mirar cuando un comando no dijo lo que
   querías.

2. **Solo se renderizan los `template_fields`.** Airflow no revisa todos los
   strings del Dag: renderiza solo los argumentos que el operator nombra en sus
   `template_fields` (en `BashOperator`: `bash_command`, `env`, `cwd`). Un
   string en cualquier otro lugar — otro argumento, o un string escrito en el
   cuerpo de una función `@task` — no se renderiza nunca: Python no sabe nada de Jinja y
   Airflow nunca mira ese string, así que sus llaves (`{{ }}`) quedan como
   texto literal. Un log que muestra `{{ … }}` sin sustituir suele significar
   justo eso.

3. **Una función `@task` rara vez necesita esto.** Recibe el contexto como
   Python (`**context`, o un argumento con nombre como `ti`), así que el
   templating es sobre todo cosa de operators. Los argumentos que le pasas al
   llamar a un `@task` sí se renderizan: el decorador `@task` convierte la
   función en un operator, guarda esos argumentos en sus atributos `op_args` y
   `op_kwargs`, y nombra los dos en sus `template_fields`. Si
   `show` es una función `@task` que imprime su argumento,
   `show("run={{ run_id }}")` imprime el run id real.

El `| default(...)` sobre `ds` cumple en la plantilla el papel que cumplía
`.get()` en la §4. `ds` deriva de `logical_date`, y un run disparado por asset,
o desde la CLI sin logical date, no tiene ninguna. Sin el default, la task falla
al renderizar la plantilla, antes de ejecutarse, con `UndefinedError: 'ds' is
undefined`. `airflow dags test` nunca enseña este fallo, por la misma razón que
no enseña el `KeyError` de la §4: siempre le da una logical date al run. La §7
explica por qué algunos runs no tienen logical date.

Si solo necesitas una fecha del run, por ejemplo para nombrar un archivo, usa
`{{ dag_run.run_after }}` en lugar de `ds`: todo run tiene `run_after`, así que
no hace falta ningún default. No siempre es la misma fecha que la logical date;
la §7 explica cuándo difieren.

Un template field también puede contener el nombre de un archivo en lugar de la
plantilla. Cada operator declara en `template_ext` las extensiones que
reconoce — `BashOperator` declara `(".sh", ".bash")` —, y cuando el valor
termina en una de ellas, Airflow busca ese archivo junto al archivo del Dag y
renderiza su contenido. Con `bash_command="describe.sh"`, la task ejecuta
`describe.sh` con sus `{{ run_id }}` ya sustituidos. Así un script o una
consulta SQL larga vive en su propio archivo, no en un string dentro del Dag.
Un `@task` hace lo mismo con dos argumentos del decorador:
`@task(templates_exts=[".sql"], templates_dict={"query": "query.sql"})` le pasa
a la función, en su parámetro `templates_dict`, el contenido renderizado de
`query.sql`.

Ahora una task de TaskFlow en el mismo Dag, leyendo el XCom de un operator:

```python dags/02_operators_and_xcoms.py
    @task
    def taskflow_reader(**context: Any) -> int:
        product = context["ti"].xcom_pull(task_ids="multiply", key="product")
        print(f"TaskFlow saw product={product}")
        return product
```

Las tasks de TaskFlow y los operators se mezclan sin problema: un `@task` puede
leer el XCom de un operator y viceversa, y el mismo `ti.xcom_pull` funciona
desde cualquiera de los dos lados. Para llamar a una función Python, `@task` y
`PythonOperator` son equivalentes — la documentación de Airflow presenta
`@task` como el equivalente de `PythonOperator` en TaskFlow, sin recomendar
uno sobre el otro —, y elegir es cuestión de estilo: `PythonOperator` recibe la
función y sus argumentos por separado (`python_callable`, `op_kwargs`, como
`push_seed` y `multiply`); con `@task`, llamar a la función es crear la task.

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

1. **Implícitamente, pasando la salida de una task a otra.** El Dag de la §4
   estaba hecho de eso: un `@task` llamado con el valor que devolvió otra task.
   La salida de un operator también sirve — pasar `describe.output` como
   argumento crearía la misma arista. Aquí `taskflow_reader` no recibe ningún
   argumento, así que su arista con `describe` hay que declararla con la forma
   2.
2. **Los operadores de desplazamiento**, cuando no fluyen datos —
   `describe >> product` es "describe, y luego product". `>>` solo fija el
   orden: no le pasa nada a la task de aguas abajo, y por eso las comprobaciones
   leen su valor de XCom. `product << describe` es la misma arista al revés, y
   también lo son `describe.set_downstream(product)` y
   `product.set_upstream(describe)`, los métodos que los operators llaman por
   debajo.
3. **Fan-out y fan-in**: una lista a un lado de `>>` (a un lado, no a los dos:
   ver la forma 5) crea una arista por cada elemento,
   así que `product >> [check_parity, check_range] >> checks_done` son cuatro
   dependencias, no dos: `product` → `check_parity`, `product` → `check_range`,
   `check_parity` → `checks_done` y `check_range` → `checks_done`. Las dos
   comprobaciones arrancan cuando termina `product`, y `checks_done` espera a
   que ambas terminen con éxito.
4. **`chain()`** — la forma legible de una línea larga de `>>`. Sus argumentos
   son las tasks en orden, así que `chain(start, push_seed, Label("seed ->
   product"), multiply, describe)` es `start >> push_seed >> Label("seed ->
   product") >> multiply >> describe`. Un `Label` no es una task: va entre las
   dos tasks cuya arista nombra — aquí `push_seed` → `multiply` — y esa arista
   muestra su texto en la vista Graph. `>>` acepta un `Label` de la misma forma.
5. **`cross_downstream()`**, para conectar cada task de una lista con cada task
   de otra — algo que parece que debería funcionar con `>>` y no funciona:

   ```python
   [a, b] >> [c, d]              # TypeError: no operator between two lists
   chain([a, b], [c, d])         # legal, but PAIRS them: a->c and b->d only,
                                 # and raises if the lists differ in length
   cross_downstream([a, b], [c, d])   # what you actually wanted: all-to-all
   ```

`cross_downstream` es el producto cartesiano — cada task de la izquierda aguas
arriba de cada task de la derecha. `chain_linear` hace lo mismo con más de dos
grupos seguidos: `chain_linear([a, b], [c, d], [e, f])` conecta cada task de un
grupo con cada task del siguiente.

### Ejecútalo y mira el grafo

```bash
docker compose exec airflow-scheduler airflow dags test operators_and_xcoms
```

**Comprobación.** En la salida busca la línea de la task `describe`,
`run=manual__… ds=… run_after=… product=42`, luego `TaskFlow saw product=42` de
`taskflow_reader`, y `even` y `under 100` de las comprobaciones. Después abre la vista
**Graph** del Dag en la UI: la etiqueta `seed -> product` en una arista, un
fan-out de dos ramas hacia `check_parity`/`check_range`, y esas dos abriéndose
hacia `checks_done`, `publish` y `notify`. Si el grafo no coincide, compara con
[`solutions/02_operators_and_xcoms.py`](../../solutions/02_operators_and_xcoms.py).

### Escribir tu propio operator

Cuando ningún provider tiene un operator para un paso, puedes heredar de
`BaseOperator` y escribir su método `execute`. La mayoría de las veces no hace
falta: una función `@task` hace el mismo trabajo con menos código, y se puede
compartir igual de bien — defínela en un módulo, impórtala en cada Dag que la
necesite y pásale argumentos distintos en cada llamada. Sus argumentos incluso
se renderizan como plantillas, como dice el tercer punto de *Templates*.

Una clase compensa su código extra cuando el paso necesita algo que una función
no tiene:

- **Valores de los argumentos comprobados al parsear el archivo.** Una llamada
  a un `@task` ya falla al parsear si falta un argumento, pero no puede mirar
  los valores. El `__init__` de un operator corre cada vez que se instancia el
  operator, y el Dag processor instancia todos los operators del Dag cada vez
  que parsea su archivo. Como `__init__` puede hacer cualquier comprobación, un
  valor erróneo aparece como error de import en la UI en cuanto el Dag
  processor vuelve a parsear el archivo (§3), no como un fallo de la task horas
  después.
- **Métodos que Airflow llama mientras corre la task.** `on_kill` corre cuando
  se detiene la task (por ejemplo, cuando marcas la task instance como failed
  en la UI), así que puede cancelar el trabajo que la task lanzó en otro
  servicio, como una consulta en BigQuery. `self.defer(...)` suspende la task y
  le pasa la espera al triggerer (§3); así están construidos los operators
  deferrable de la §11.
- **Botones propios en la UI.** `operator_extra_links`, un atributo de clase,
  añade una sección **Extra Links** a la vista de la task instance en la UI,
  con un botón por enlace. Cada enlace es una subclase de `BaseOperatorLink`
  cuyo método `get_link` calcula la URL para cada task instance, normalmente la
  página del trabajo que la task lanzó en el servicio con el que habla: el job
  de Databricks, la consulta de BigQuery. Ninguna de las clases
  que hay debajo de un `@task` (`_PythonDecoratedOperator`, `PythonOperator`,
  `BaseOperator`) lo acepta como argumento — `@task(operator_extra_links=...)`
  falla con `TypeError` —, y tampoco ninguno de los operators de los providers
  instalados en este stack. Para tener botones propios hace falta una clase
  propia.

Los operators de los providers necesitan estas cosas, y por eso son clases. El
paso de entrenamiento de abajo necesita la primera. Recibe un umbral, `min_f1`,
y falla cuando el f1 del modelo (su métrica de calidad, de 0 a 1) queda por
debajo. Un umbral escrito como `65` en vez de `0.65` debería detectarse en
cuanto el Dag processor parsea el archivo, no después de un entrenamiento:

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

`__init__` corre cuando se parsea el archivo del Dag: comprueba `min_f1` y
guarda los argumentos. Con `min_f1=65` el Dag no llega a cargarse, y la UI
muestra el error de import `ValueError: min_f1 must be between 0 and 1, got 65`.
`execute` corre cuando corre la task instance, después de que
Airflow haya renderizado cada atributo nombrado en `template_fields` — así que
`self.experiment_name` contiene `project_tagger_manual__…`, no las llaves. Lo que
devuelva `execute` se guarda como XCom, exactamente igual que el `return` de una
función `@task`.

`template_fields` es lo que une el argumento con el renderizado. Es un atributo
de clase que `BaseOperator` define como una tupla vacía y que cada operator
sobrescribe, y contiene **nombres de atributos**: `"experiment_name"` se
refiere a `self.experiment_name`, y por eso `__init__` tiene que guardar el
argumento exactamente con ese nombre. Antes de llamar a `execute`, Airflow lee
de `self` cada atributo de la lista, lo renderiza y vuelve a guardar el
resultado. Se declara en la clase en vez de pasarse a `super().__init__()`
porque describe el tipo de operator, igual para todas sus instancias; una
subclase amplía la lista de su padre con
`template_fields = (*Parent.template_fields, "new_field")`.
`min_f1` queda fuera de `template_fields` a propósito: al parsear, una
plantilla todavía es texto sin renderizar, así que `__init__` no podría
comprobarla.

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
run llega a `train`. Esa diferencia es la única razón para usar la clase. Un paso sin
esa necesidad queda mejor como `@task`, que es lo que usa este repo de
principio a fin.

> Esto vale para un valor escrito en el archivo del Dag. Si el valor llega al
> disparar el run, como un param (§4), al parsear todavía no existe, y ni la
> clase ni el `@task` pueden comprobarlo al parsear, solo cuando corre la task.
> Si hace falta validarlo, pon el límite en el propio `Param`:
> `Param(0.65, type="number", exclusiveMinimum=0, maximum=1)` hace que Airflow
> rechace un trigger con `min_f1=65` antes de crear el run.

**¿De qué clase heredar?** De la clase cuyo comportamiento quieras conservar.
La guía de Airflow para operators propios parte de `BaseOperator`, que trae lo
que comparten todas las tasks (`task_id`, reintentos, plantillas) y deja
`execute` para ti: es el punto de partida cuando tu operator hace algo nuevo,
como `ProjectTaggerTrainOperator`. Pero heredar de un operator ya hecho es lo
habitual cuando quieres lo que hace más algo: la misma guía lo usa para añadir
`template_fields` a un operator existente, y la mayoría de los operators de los
providers derivan de otro operator, no de `BaseOperator` directamente.

`PythonOperator`, por ejemplo, es el padre adecuado cuando el trabajo es llamar
a una función más algo extra: así están hechos `BranchPythonOperator`,
`ShortCircuitOperator`, `PythonVirtualenvOperator` y la clase que crea `@task`,
`_PythonDecoratedOperator` — por eso los logs muestran
`<Task(_PythonDecoratedOperator): taskflow_reader>`, aunque la UI la etiquete
como `@task`. Para `ProjectTaggerTrainOperator` no lo es: escribe su propio
`execute`, así que reemplazaría lo único que hace `PythonOperator`, y aun así
tendría que recibir `python_callable`, que es obligatorio.

### Hooks, Connections y Variables

Un operator es una task entera que hace una cosa fija: `SQLExecuteQueryOperator`
ejecuta una consulta, `SqlToS3Operator` copia el resultado de una consulta a un
archivo en S3. Un **hook** es el cliente sobre el que se apoya un operator. Recibe un
`conn_id` — el nombre de una **Connection**, las credenciales de un servicio
que Airflow guarda con ese nombre (se explican más abajo) —, construye el
cliente autenticado y te da métodos a los que llamar — `PostgresHook.get_df`
ejecuta una consulta y devuelve un dataframe, `S3Hook.load_string` escribe un
string en S3. Todo operator que habla con un servicio fuera de Airflow (una
base de datos, S3, una API) lo hace a través de un hook.

Llamas a un hook tú mismo, dentro de un `@task`, cuando una task tiene que hacer
varias llamadas con tu propio código entre ellas — algo que ningún operator
hace por sí solo. Ninguno de los Dags del tutorial necesita uno. El ejemplo
supone que los proyectos están en una base de datos Postgres y que la task
exporta un CSV a S3:

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

La task nombra dos Connections, `warehouse` y `aws_default`, y nunca una
contraseña. Si el paso fuera solo "copia el resultado de la consulta a S3",
`SqlToS3Operator` lo haría como una única task sin código; la versión con hooks
es para cuando tu propia lógica va en medio. Un servicio para el que no hay hook
también puede usar una Connection: `BaseHook`, la clase de la que derivan todos los hooks,
tiene `BaseHook.get_connection("warehouse")`, que devuelve la Connection en sí —
host, login, password, extra — para que se la pases a cualquier librería
cliente.

Una **Connection** es un conjunto de credenciales con nombre para un servicio
(`conn_type`, host, login, password, extra). Una **Variable** es un par
clave-valor global a toda la instalación: cualquier task de cualquier Dag la
lee con `Variable.get("min_rows", default=500)`, y sirve para configuración que
no quieres escribir en el código, como un límite o el nombre de un bucket.
Las dos se definen de tres maneras:

- **En la UI** (Admin → Connections / Variables) **o con la CLI**
  (`airflow connections add`, `airflow variables set`). Se guardan en la
  metadata database, en las tablas `connection` y `variable`. Airflow cifra la
  contraseña, el extra y el valor de cada Variable con la clave de
  `[core] fernet_key`; sin clave, los guarda en texto plano, y es lo que pasa en
  este stack, que no define ninguna.
- **Como variable de entorno** `AIRFLOW_CONN_<ID>` / `AIRFLOW_VAR_<CLAVE>`. No
  pasan por la base de datos ni aparecen en la UI. Así define el archivo de
  compose `google_cloud_default`, la Connection que usa el sensor de BigQuery
  de la §11, y `fs_default`, la que usa por defecto un `FileSensor`.
- **En un secrets backend** (Vault, AWS Secrets Manager…), lo habitual en
  producción (§16).

Las plantillas acceden a ellas con `{{ conn.warehouse.host }}` y
`{{ var.value.min_rows }}`.

> Una task que consulta cualquiera de las dos no toca la metadata database.
> Primero mira las variables de entorno `AIRFLOW_CONN_*` / `AIRFLOW_VAR_*` — así
> encuentra `google_cloud_default` sin preguntar a nadie — y, si no está ahí, se la pide
> al API server por la Task Execution API, que es lo que permite a un worker
> remoto resolver una Connection sin tener acceso a la metadata database.
>
> Prefiere un **param** (§4) a una Variable global cuando el valor sea un ajuste
> que cambia lo que decide el Dag, como un umbral: los params quedan registrados
> con el run que los usó, las Variables no.

---

← [5. La UI](05-la-ui.md) · [Índice](README.md) · [7. Scheduling y logical dates](07-scheduling-y-logical-dates.md) →
