## 4. Tu primer Dag

Lo que necesita el autor de un Dag para definir Dags, tasks y assets está en
`airflow.sdk`, el Task SDK (los operators vienen de los providers, §6):

```python
from airflow.sdk import dag, task, DAG, Param, Asset, chain, Label
```

Vas a escribir `dags/01_hello_airflow3.py`: cuatro tasks que construyen un
saludo, lo gritan, cuentan sus palabras e imprimen el resultado. Lo bastante
pequeño para abarcarlo de un vistazo, y cada idea que contiene reaparece en todos
los Dags posteriores.

### Constrúyelo

Crea el archivo `dags/01_hello_airflow3.py` y empiézalo con un docstring, los
imports y un diccionario de valores por defecto:

```python dags/01_hello_airflow3.py
"""Lesson 1 - a Dag, tasks, and dependencies."""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.sdk import Param, dag, task

DEFAULT_ARGS = {
    "owner": "mlops-course",
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=30),
}
```

El docstring no es decorativo: dentro de un momento lo vas a pasar como la
documentación del Dag en la UI. `pendulum` es la librería de datetimes con zona
horaria que Airflow usa en todas partes. Un `datetime` normal sin zona horaria
(uno "naive") se interpreta en la zona horaria por defecto de Airflow, que puede
no ser la que querías — y entonces tus runs arrancan a horas que no esperabas.

**`default_args` es un diccionario que se aplica a todas las tasks del Dag.**
Puede llevar cualquier cosa que acepte una task. Sus valores son solo valores por
defecto: si una task pasa ese mismo argumento, el valor de la task sustituye al
de `default_args`, solo para esa task. Con `"retries": 2` aquí, una task
declarada con `@task(retries=5)` se reintenta cinco veces, y las demás siguen
reintentándose dos. Existe para que no repitas `owner=` y `retries=` en veinte
tasks.

`owner` es una etiqueta: sale en la lista de Dags, puedes filtrar por ella, y
dice a quién preguntar cuando el Dag se rompe. Los tests de integridad de este
repo (§14) rechazan el valor por defecto, `airflow`, porque un owner que no es
nadie significa que no se avisa a nadie.

Las otras dos deciden qué pasa cuando una task falla:

- **`retries=2`** — si la task lanza una excepción, Airflow la ejecutará hasta
  dos veces más. La task instance pasa a `up_for_retry`, espera y se vuelve a
  programar. Es la *misma* task instance todo el rato: su `try_number` sube, cada
  intento guarda su propio log, y solo el resultado del último intento decide si
  corren las tasks de aguas abajo.
- **`retry_delay`** — cuánto espera antes del siguiente intento. Treinta segundos
  es generoso para una demo y corto para trabajo real; una API inestable suele
  necesitar minutos, y `retry_exponential_backoff=2.0` duplica la espera tras
  cada intento.

**El valor por defecto es `retries=0`** (`[core] default_task_retries`), lo que
significa que una task que sufre un corte de red puntual simplemente falla y se lleva el
run por delante. Los reintentos hay que activarlos explícitamente, y por eso los tests de integridad de este
repo (§14) insisten en que todo Dag que no sea una demo lo fije.

Los reintentos solo ayudan con fallos que podrían no repetirse — un timeout, una
API con throttling, un nodo que murió. Para un fallo que *sí* se va a repetir,
lanzar `AirflowFailException` (§9) se salta los intentos restantes y falla al
momento: los datos malos no se vuelven buenos en la tercera lectura.

Ahora el Dag en sí. Añade el decorador y la función que decora:

```python dags/01_hello_airflow3.py

@dag(
    dag_id="hello_airflow3",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "basics"],
    doc_md=__doc__,
    params={"greeting": Param("hello", type="string", description="What to say.")},
)
def hello_airflow3():
```

Línea a línea:

- **`@dag` convierte una función en un Dag.** Este estilo de decoradores tiene
  nombre, **TaskFlow**, y el nombre va a seguir apareciendo.
- **`schedule=None`** — este Dag corre solo cuando alguien lo dispara: tú, desde
  la UI o la CLI, o un programa a través de la API. La §7 tiene los otros
  valores que puede tomar un schedule.
- **`start_date`** — el primer momento para el que el scheduler crea runs por su
  cuenta; nunca programa uno antes. No te impide lanzar un run tú mismo — un
  trigger manual funciona aunque el `start_date` esté en el futuro —, así que en
  este Dag con `schedule=None` todavía no cambia nada; está puesto para que el
  Dag esté listo el día que tenga un schedule. Es opcional; la §7 explica
  cuándo arranca un Dag con schedule que no lo tiene.
- **`catchup=False`** — decide qué pasa cuando quitas la pausa a un Dag con
  schedule cuyo `start_date` ya pasó. Con `False`, el valor por defecto, el
  scheduler ignora el pasado perdido y crea un solo run, para la vez más
  reciente en que el schedule debería haberse disparado. Con `True`, crea un run
  por *cada* vez que el schedule se habría disparado desde el `start_date` —
  meses de runs, todos de golpe. Está escrito aquí, aunque sea el valor por
  defecto, para que nunca tengas que adivinar cuál está en vigor (§7).
- **`tags`** — etiquetas libres; la lista de Dags filtra por ellas.
- **`doc_md=__doc__`** — el docstring del módulo que acabas de escribir,
  renderizado como markdown en la página del Dag en la UI. `description=` es la
  versión corta, una línea que se muestra junto al Dag en la lista de Dags.
- **`params`** — entradas declaradas, con tipos. La UI las convierte en un
  formulario cuando disparas el Dag (§5), y un run registra los valores que usó.

Dentro de la función, las tasks. Indéntalas — son el cuerpo de
`hello_airflow3()`:

```python dags/01_hello_airflow3.py
    @task
    def make_greeting(**context: Any) -> str:
        greeting = context["params"]["greeting"]
        when = context.get("logical_date") or "no logical date"
        return f"{greeting} from run {context['run_id']} at {when}"

    @task
    def shout(message: str) -> str:
        return message.upper()

    @task
    def count_words(message: str) -> int:
        return len(message.split())

    @task
    def report(loud: str, words: int) -> None:
        print(f"{loud}  ({words} words)")
```

**`@task` convierte una función en una task.** Llamarla *no* la ejecuta:
devuelve un `XComArg`, una referencia perezosa a la futura salida de esa task. Lo
que devuelva la función se convierte en el XCom de la task (§6), y lo que
imprima acaba en el log de la task, visible en la UI.

`**context` es cómo una task ve el run al que pertenece: `run_id`, `params`, `ti`
(la task instance), `dag_run`, `ds`, `data_interval_start`/`end`, `var`, `conn`,
`macros`. Son objetos de tipos distintos, de ahí `**context: Any`; Airflow
rellena el contexto por nombre y no lee la anotación. Lee `logical_date` — la fecha que da nombre a un run, como en "el
run del 10 de septiembre" (§7) — con `.get()`, nunca con `context["logical_date"]`:
un run disparado por asset, o desde la CLI sin logical date, no tiene ninguna, y
entonces la clave
no existe en el contexto — indexarla lanza `KeyError` en tiempo de
ejecución, mientras `airflow dags test` (que siempre aporta una) sigue en verde.
La §7 explica el porqué. `run_id` siempre está; prefiérelo para nombrar cosas.

Termina el archivo conectando las tasks entre sí y registrando el Dag:

```python dags/01_hello_airflow3.py
    message = make_greeting()
    report(loud=shout(message), words=count_words(message))


hello_airflow3()
```

**Pasar la salida de una task a otra crea la arista.** `shout(message)` es lo
que hace que `shout` corra después de `make_greeting`. El grafo se construye
cuando se parsea el archivo; el código corre después, posiblemente en otra
máquina. Nada hace que `shout` y `count_words` se esperen entre sí, así que en
un run dirigido por el scheduler pueden ejecutarse a la vez (la §3 dice qué lo
decide):

```
make_greeting ─┬─► shout ──────┬─► report
               └─► count_words ┘
```

Y la última línea: **el decorador solo define el Dag; llamar a la función lo
registra.** Un archivo de Dag que define un Dag y nunca lo llama no produce nada
— se importa sin errores y no aparece en ningún sitio. Es el error de principiante
más común y el más difícil de ver, porque no hay ningún error que leer.

Guarda el archivo. Compáralo con
[`solutions/01_hello_airflow3.py`](../../solutions/01_hello_airflow3.py) si quieres —
es el mismo código con los comentarios dentro.

### Comprueba que se parsea

El Dag processor (§3) escanea `dags/` cada cinco segundos en este stack,
importa cada archivo `.py` que parece un archivo de Dag (qué significa eso
viene más abajo), y escribe lo que parseó en la metadata database. A los pocos segundos de guardar:

```bash
docker compose exec airflow-scheduler airflow dags list -o plain
```

**Comprobación.** Una fila: `hello_airflow3`, owner `['mlops-course']`,
`is_paused True`, `bundle_name dags-folder`. Recarga la UI y el Dag está en la
página **Dags**, en pausa. (Todos los Dags empiezan en pausa — §7.)

Si la fila no está y ya pasaron esos segundos, pueden estar pasando tres cosas,
y en `airflow dags list` parecen idénticas. La primera es un archivo que lanza una excepción al importarse — una
errata, un paquete que falta, una función `@dag` que revienta. Eso no es un Dag
roto en la lista, es **ningún Dag**: ausente de la UI, ausente de `airflow dags
list`. Aparece como error de import en las páginas Home y Dags (§5) y con este
comando:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors -o plain
```

**Comprobación.** `No data found` — tu archivo se importa sin errores. Mira cómo se ve el otro
caso provocándolo: guarda esto como `dags/zz_broken_on_purpose.py`,

```python
# An airflow dag file that does not import
import a_module_that_does_not_exist
```

espera unos segundos, y ejecuta el comando otra vez:

```
bundle_name    filepath                 error
dags-folder    zz_broken_on_purpose.py  Traceback (most recent call last):
                                          File "<frozen importlib._bootstrap>", line 488, in _call_with_frames_removed
                                          File "/opt/airflow/dags/zz_broken_on_purpose.py", line 2, in <module>
                                            import a_module_that_does_not_exist
                                        ModuleNotFoundError: No module named 'a_module_that_does_not_exist'
```

El archivo, la línea y el traceback completo. Borra el archivo y la fila
desaparece sola en el siguiente escaneo.

Esa primera línea de comentario no es relleno. El Dag processor no importa
todos los `.py` que encuentra: con `[core] dag_discovery_safe_mode` activado —
el valor por defecto — primero lee el archivo como texto y se lo salta salvo que
el contenido en minúsculas contenga `airflow` *y* además `dag` o `asset`. Quita
el comentario de ese archivo roto y no se parsea nunca, no se lista nunca y no
se reporta nunca — silencio, no un error. Un archivo de Dag real siempre pasa
esta prueba: basta con `from airflow.sdk import dag` para que contenga las dos
palabras, así que no tienes que preocuparte por ello. Los archivos que se salta
son los que nunca las mencionan — un módulo auxiliar como un `utils.py` sin
ningún Dag dentro, que es justo lo correcto, o un experimento desechable como el
archivo roto de arriba. Así que cuando un archivo simplemente no está, esto es
lo segundo que hay que comprobar.

La tercera forma de no obtener nada es el error de principiante descrito bajo el título
"Constrúyelo" en esta sección — un archivo que define un Dag y nunca lo llama. Se
importa sin errores, así que `list-import-errors` también está vacío, y el único síntoma
es un Dag que no está. Mira primero `list-import-errors`, y después el final del
archivo.

### Ejecútalo

Esto ejecuta un Dag run completo en tu terminal, sin scheduler de por medio:

```bash
docker compose exec airflow-scheduler airflow dags test hello_airflow3
```

**Comprobación.** Busca la línea de la task `report`, que se ve así — siete
palabras, porque `make_greeting` añade el run id *y* la logical date, y la fecha
impresa lleva un espacio en medio:

```
HELLO FROM RUN MANUAL__2026-09-15T12:18:32.454459+00:00 AT 2026-09-15 12:18:33.500053+00:00  (7 words)
```

y, al final, `DagRun Finished: ... state=success`. Es la forma más rápida de
probar un Dag mientras lo escribes: un run completo con logs reales y sin UI. Lo vas a usar
después de cada Dag que escribas.

> **Qué es `airflow dags test`, y qué no.** Ejecuta el Dag entero dentro de este
> único comando, sin scheduler, executor ni triggerer. Hay tres cosas que
> funcionan distinto que en un run real, y te las encontrarás más adelante:
>
> - Las tasks corren **de una en una**, en un orden que el grafo permita. En la
>   salida de ese run, una de `shout`/`count_words` terminó antes de que empezara la
>   otra; cuál va primero varía de un run a otro.
> - **Siempre aporta una logical date.** Un trigger desde la CLI no (§7), y por
>   eso un Dag puede pasar aquí y fallar en producción.
> - Una task **deferrable** (§11), cuando tiene que esperar, normalmente se
>   suspende y deja la espera en manos de otro servicio, el triggerer. Aquí no
>   hay triggerer, así que la espera la hace el propio `airflow dags test`, y el
>   comando no termina hasta que acaba.
>
> Eso sí, **no es un dry run**: las tasks escriben archivos reales y asset
> events reales (§9). Es la forma más rápida de ver funcionar un Dag; la §14
> dice cuándo dejar de fiarse de él.

> **Pruébalo.** Cambia el saludo sin tocar el archivo:
>
> ```bash
> docker compose exec airflow-scheduler airflow dags test hello_airflow3 --conf '{"greeting": "hola"}'
> ```
>
> La línea de `report` empieza ahora por `HOLA`. Ese `--conf` llegó a la task a través de
> `context["params"]` — el `Param` que declaraste, sobrescrito para un run.

**Otra forma de declarar un Dag** es el context manager, que está plenamente
soportado y se lee mejor cuando las tasks son operators (§6) en vez de funciones.
No para este archivo — solo para que lo reconozcas:

```python
from airflow.sdk import DAG

with DAG(
    dag_id="hello_operators",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    BashOperator(task_id="hello", bash_command="echo hi")
```

Cualquier cosa creada dentro del bloque `with` se asocia a ese Dag
automáticamente. Ambas formas producen el mismo objeto, y un archivo puede tener
varios Dags.

---

← [3. Las piezas de Airflow](03-las-piezas-de-airflow.md) · [Índice](README.md) · [5. La UI](05-la-ui.md) →
