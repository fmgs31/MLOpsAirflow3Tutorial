## 4. Tu primer Dag

Todo lo que necesitas para definir Dags, tasks y assets está en `airflow.sdk`,
el Task SDK (los operators vienen de los providers, §6).

Vas a escribir `dags/01_hello_airflow3.py`: cuatro tasks que construyen un
saludo, lo gritan, cuentan sus palabras e imprimen el resultado. Es pequeño, y
cada idea que contiene reaparece en todos los Dags posteriores.

### Constrúyelo

Crea `dags/01_hello_airflow3.py` con un docstring, los imports y un diccionario
de valores por defecto:

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

El docstring será la documentación del Dag en la UI (`doc_md`). `pendulum` es
la librería de datetimes con zona horaria que Airflow usa en todas partes. Un
`datetime` sin zona horaria (uno "naive") se interpreta en la zona horaria por
defecto de Airflow, que puede no ser la tuya, y tus runs arrancan a horas que
no esperabas.

**`default_args` es un diccionario que se aplica a todas las tasks del Dag.**
Puede llevar cualquier cosa que acepte una task. Sus valores son solo valores
por defecto: una task que pase el mismo argumento lo sustituye solo para ella.
Con `"retries": 2` aquí, una task declarada con `@task(retries=5)` se reintenta
cinco veces, y las demás dos. Evita repetir `owner=` y `retries=` en veinte
tasks.

`owner` es una etiqueta: sale en la lista de Dags, puedes filtrar por ella, y
dice a quién preguntar cuando el Dag se rompe. Los tests de integridad de este
repo (§15) rechazan el valor por defecto, `airflow`: un owner que no es nadie
significa que no se avisa a nadie.

Las otras dos deciden qué pasa cuando una task falla:

- **`retries=2`** — si la task lanza una excepción, Airflow la ejecuta hasta
  dos veces más. La task instance pasa a `up_for_retry`, espera y se vuelve a
  programar. Sigue siendo la *misma* task instance: su `try_number` sube, cada
  intento guarda su propio log, y solo el resultado del último intento decide
  si corren las tasks de aguas abajo.
- **`retry_delay`** — la espera antes del siguiente intento. Treinta segundos
  es generoso para una demo y corto para trabajo real; una API inestable suele
  necesitar minutos, y `retry_exponential_backoff=2.0` duplica la espera tras
  cada intento.

**El valor por defecto es `retries=0`** (`[core] default_task_retries`), así
que un corte de red puntual hace fallar la task y, con ella, el run. Los
reintentos hay que activarlos, y por eso los tests de integridad de este repo
(§15) exigen que todo Dag que no sea una demo los fije.

Los reintentos solo ayudan con fallos que podrían no repetirse — un timeout, una
API con throttling, un nodo caído. Para uno que *sí* se va a repetir, lanza
`AirflowFailException` (§9): se salta los intentos restantes y falla al
momento. Los datos malos no se vuelven buenos en la tercera lectura.

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

- **`@dag` convierte una función en un Dag.** Este estilo de decoradores se
  llama **TaskFlow**.
- **`schedule=None`** — el Dag corre solo cuando alguien lo dispara: tú, desde
  la UI o la CLI, o un programa a través de la API. La §7 cubre los demás
  valores.
- **`start_date`** — el primer momento para el que el Scheduler crea runs por
  su cuenta. Los triggers manuales lo ignoran — funcionan aunque el
  `start_date` esté en el futuro —, así que en este Dag con `schedule=None` aún
  no cambia nada; está puesto para que el Dag esté listo cuando tenga un
  schedule. Es opcional; la §7 explica cuándo arranca un Dag con schedule que
  no lo tiene.
- **`catchup=False`** — qué pasa cuando quitas la pausa a un Dag con schedule
  cuyo `start_date` ya pasó. Con `False`, el valor por defecto, el Scheduler
  ignora el pasado perdido y crea un solo run, para la vez más reciente en que
  el schedule debería haberse disparado. Con `True`, crea un run por *cada* vez
  que el schedule se habría disparado desde el `start_date` — meses de runs de
  golpe. Se escribe aunque sea el valor por defecto, para que nunca tengas que
  adivinar cuál está en vigor (§7).
- **`tags`** — etiquetas libres; la lista de Dags filtra por ellas.
- **`doc_md=__doc__`** — el docstring del módulo, renderizado como markdown en
  la página del Dag. `description=` es la versión corta: una línea junto al Dag
  en la lista de Dags.
- **`params`** — entradas declaradas, con tipos. La UI las convierte en un
  formulario cuando disparas el Dag (§5), y un run registra los valores que usó.

Dentro de la función van las tasks, indentadas como cuerpo de
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
devuelve un `XComArg`, una referencia perezosa a la futura salida de la task.
Lo que devuelve la función se convierte en el XCom de la task (§6); lo que
imprime va al log de la task, visible en la UI.

`**context` es cómo una task ve su run: `run_id`, `params`, `ti` (la task
instance), `dag_run`, `ds`, `data_interval_start`/`end`, `var`, `conn`,
`macros`. Son de tipos distintos, de ahí `**context: Any`; Airflow rellena el
contexto por nombre e ignora la anotación.

Lee `logical_date`, la fecha que da nombre a un run ("el run del 10 de
septiembre", §7), con `.get()`, nunca con `context["logical_date"]`. Los runs
disparados por asset, o desde la CLI sin logical date, no tienen, y la clave no
existe: indexarla lanza `KeyError` en tiempo de ejecución, mientras
`airflow dags test`, que siempre aporta una, sigue en verde. La §7 explica por
qué. `run_id` siempre está; prefiérelo para nombrar cosas.

Termina el archivo conectando las tasks entre sí y registrando el Dag:

```python dags/01_hello_airflow3.py
    message = make_greeting()
    report(loud=shout(message), words=count_words(message))


hello_airflow3()
```

**Pasar la salida de una task a otra crea la arista.** `shout(message)` hace
que `shout` corra después de `make_greeting`. El grafo se construye al parsear
el archivo; el código corre después, posiblemente en otra máquina. Nada ordena
`shout` y `count_words` entre sí, así que un run dirigido por el Scheduler
puede ejecutarlas a la vez (la §3 dice qué lo decide):

```
make_greeting ─┬─► shout ──────┬─► report
               └─► count_words ┘
```

La última línea importa: **el decorador solo define el Dag; llamar a la función
lo registra.** Un archivo que define un Dag sin llamarlo se importa sin errores
y no aparece en ningún sitio — el error de principiante más común, y el más
difícil de ver, porque no hay ningún error que leer.

Guarda el archivo. Si quieres, compáralo con
[`solutions/01_hello_airflow3.py`](../../solutions/01_hello_airflow3.py): el
mismo código, con comentarios.

### Comprueba que se parsea

El Dag processor (§3) escanea `dags/` cada cinco segundos en este stack,
importa cada archivo `.py` que parece un archivo de Dag (qué significa eso
viene más abajo) y escribe el resultado en la Metadata database. A los pocos
segundos de guardar:

```bash
docker compose exec airflow-scheduler airflow dags list -o plain
```

**Comprobación.** Una fila: `hello_airflow3`, owner `['mlops-course']`,
`is_paused True`, `bundle_name dags-folder`. Recarga la UI y el Dag está en la
página **Dags**, en pausa. (Todos los Dags empiezan en pausa — §7.)

Si la fila sigue sin aparecer, hay tres causas, y en `airflow dags list`
parecen idénticas. La primera es un archivo que lanza una excepción al
importarse: una errata, un paquete que falta, una función `@dag` que revienta.
El resultado no es un Dag roto en la lista sino **ningún Dag**, ausente de la
UI y de la lista. Aparece como error de import en las páginas Home y Dags (§5)
y con este comando:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors -o plain
```

**Comprobación.** `No data found`: tu archivo se importa. Para ver el otro
caso, provócalo: guarda esto como `dags/zz_broken_on_purpose.py`,

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

La línea de comentario importa. El Dag processor no importa todos los `.py` que
encuentra: con `[core] dag_discovery_safe_mode` activado (el valor por
defecto), primero lee el archivo como texto y se lo salta salvo que el
contenido en minúsculas contenga `airflow` *y* además `dag` o `asset`. Sin el
comentario, `zz_broken_on_purpose.py` no se parsea, no se lista ni se reporta
nunca: silencio, no un error. Un archivo de Dag real siempre pasa, porque
`from airflow.sdk import dag` ya contiene las dos palabras. Los que se salta son
los que no tienen alguna: un módulo auxiliar sin Dag, que es lo correcto, o un
experimento desechable como `zz_broken_on_purpose.py`. Así que cuando un
archivo no está, esto es lo segundo que hay que comprobar.

La tercera causa es el error de principiante de "Constrúyelo": un Dag definido
pero nunca llamado. Se importa sin errores, así que `list-import-errors`
también está vacío; el único síntoma es un Dag que no está. Mira primero
`list-import-errors`, y después el final del archivo.

### Ejecútalo

Esto ejecuta un Dag run completo en tu terminal, sin Scheduler:

```bash
docker compose exec airflow-scheduler airflow dags test hello_airflow3
```

**Comprobación.** Busca la línea de la task `report`. Tiene siete palabras,
porque `make_greeting` añade el run id *y* la logical date, y la fecha impresa
lleva un espacio en medio:

```
HELLO FROM RUN MANUAL__2026-09-15T12:18:32.454459+00:00 AT 2026-09-15 12:18:33.500053+00:00  (7 words)
```

y, al final, `DagRun Finished: ... state=success`: un run completo con logs
reales y sin UI, la forma más rápida de probar un Dag mientras lo escribes. Lo
usarás después de cada Dag.

> **Qué es `airflow dags test`, y qué no.** Ejecuta el Dag entero dentro del
> comando, sin Scheduler, Executor ni Triggerer. Tres cosas funcionan distinto
> que en un run real, y te las encontrarás más adelante:
>
> - Las tasks corren **de una en una**, en un orden que el grafo permita. En tu
>   run, una de `shout`/`count_words` terminó antes de que empezara la otra; el
>   orden varía de un run a otro.
> - **Siempre aporta una logical date.** Un trigger desde la CLI no (§7), así
>   que un Dag puede pasar aquí y fallar en producción.
> - Una task **deferrable** (§11) que tiene que esperar normalmente se suspende
>   y deja la espera a otro servicio, el Triggerer. Aquí no hay Triggerer, así
>   que espera el propio `airflow dags test`, y el comando no termina hasta que
>   acaba la espera.
>
> Eso sí, **no es un dry run** (un ensayo que informa de lo que haría un
> comando sin hacerlo): las tasks escriben archivos reales y asset events
> reales (§9). La §15 dice cuándo dejar de fiarse de él.

> **Pruébalo.** Cambia el saludo sin tocar el archivo:
>
> ```bash
> docker compose exec airflow-scheduler airflow dags test hello_airflow3 --conf '{"greeting": "hola"}'
> ```
>
> La línea de `report` empieza ahora por `HOLA`: `--conf` llegó a la task a
> través de `context["params"]`, sobrescribiendo para un run el `Param` que
> declaraste.

**Otra forma de declarar un Dag** es el context manager, plenamente soportado y
más legible cuando las tasks son operators (§6) en vez de funciones. No es para
este archivo; está aquí para que lo reconozcas:

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
automáticamente. Ambas formas producen el mismo objeto, y un archivo puede
tener varios Dags.

---

← [3. Las piezas de Airflow](03-las-piezas-de-airflow.md) · [Índice](README.md) · [5. La UI](05-la-ui.md) →
