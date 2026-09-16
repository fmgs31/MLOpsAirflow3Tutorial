# Aprende Apache Airflow 3 construyendo un pipeline de ML

Un tutorial práctico para **Airflow 3.3.1**. Vas a escribir siete Dags, un
archivo cada vez, que crecen hasta formar un sistema que funciona: se extraen
datos, se validan y se transforman, eso publica un **Asset**, el Asset dispara
el entrenamiento del modelo, el mejor modelo se promociona a producción, y un
Dag de monitorización decide si hay que reentrenar.

Todo corre en tu máquina con Docker. Sin cuenta en la nube, sin warehouse, sin
registrarte en ningún sitio. Cada comando de aquí abajo se ejecutó contra un
stack real de Airflow 3.3.1 — ver [Qué se verificó de verdad](#17-qué-se-verificó-de-verdad).

**Se asume:** Python, la línea de comandos y Docker instalado. No hace falta
experiencia con Airflow — esto enseña Airflow 3 tal y como es hoy, y nada sobre
versiones anteriores.

**Tiempo:** ~30 minutos hasta tu primer Dag run. Un día para los siete si los
escribes tú, que es la idea.

---

## Contenidos

1. [Qué vas a construir](#1-qué-vas-a-construir)
2. [Ponlo en marcha](#2-ponlo-en-marcha)
3. [Las piezas de Airflow](#3-las-piezas-de-airflow)
4. [Tu primer Dag](#4-tu-primer-dag)
5. [La UI](#5-la-ui)
6. [Operators, XComs y relaciones](#6-operators-xcoms-y-relaciones)
7. [Scheduling y logical dates](#7-scheduling-y-logical-dates)
8. [Assets: programar por datos, no por tiempo](#8-assets-programar-por-datos-no-por-tiempo)
9. [El Dag de DataOps](#9-el-dag-de-dataops)
10. [El Dag de MLOps](#10-el-dag-de-mlops)
11. [Esperar: sensors, deferrable tasks, eventos](#11-esperar-sensors-deferrable-tasks-eventos)
12. [Continual learning: monitorizar, decidir, actuar](#12-continual-learning-monitorizar-decidir-actuar)
13. [Deadline alerts](#13-deadline-alerts)
14. [Testear Dags](#14-testear-dags)
15. [Llevarlo a producción](#15-llevarlo-a-producción)
16. [Apéndice y procedencia](#16-apéndice-y-procedencia)

Material de referencia: [CLI cheatsheet](docs/cheatsheet.md) ·
[Troubleshooting](docs/troubleshooting.md) ·
[Los Dags terminados, completos](docs/appendix-dags_ES.md) ·
[Qué se verificó de verdad](docs/verified_ES.md)

---

## 1. Qué vas a construir

La orquestación convierte un montón de scripts que lanzas a mano en un sistema:
el trabajo se declara como un grafo de tasks, otra cosa decide cuándo ejecutarlo,
reintenta las partes que fallan y registra qué pasó.

Un workflow en Airflow es un **Dag** — un grafo dirigido acíclico. *Dirigido*:
las aristas tienen dirección. *Acíclico*: sin bucles, así que una task terminada
nunca se vuelve a ejecutar por algo que hay aguas abajo. Cada nodo es una
**task**, y una ejecución del grafo entero es un **Dag run**.

Merece la pena separar esas dos últimas palabras ya, porque cada línea de log y
cada mensaje de error dan por hecho que lo has hecho. Una **task** es lo que
escribiste una vez, en el archivo. Una **task instance** es esa task dentro de un
Dag run concreto — lo que de verdad tiene un estado, una duración, un log y un
número de intentos. Tú escribes tasks; tú depuras task instances. La §3 sigue una
a lo largo de toda su vida.

Tres Dags hacen el trabajo de verdad aquí, y lo interesante es cómo se conectan:

```
dataops:   extract ─► validate_raw ─► transform ─► validate_transformed ──┐
                                                                          │ emite
                                                            Asset: labeled_projects
                                                                          │ dispara
mlops:                  prepare ─► validate ─► train×N ─► select ─► gate ─┴─► promote
                                                                          │ emite
                                                            Asset: production_model

monitoring:  measure ─► decide ─► {continue | inspect | rollback | approve ─► retrain}
```

`mlops` **no tiene schedule**. Se ejecuta cuando `dataops` publica datos buenos.
Eso es scheduling data-aware, y es la idea sobre la que está construido todo este
tutorial.

Hay cuatro Dags más, cada uno para enseñar una cosa: `hello_airflow3` (un primer
Dag), `operators_and_xcoms` (tipos de task y cómo hablan entre ellas),
`sensors_and_events` (esperar al mundo exterior) y `deadline_alerts` (que te
avisen cuando un run va demasiado lento).

### Cómo funciona este tutorial

Los siete Dags los escribes tú. La carpeta que lee Airflow, `dags/`, está
**vacía** cuando clonas este repo. Cada sección explica una pieza de Airflow y
después te hace construir el Dag que la usa — unas pocas líneas cada vez, con el
porqué de cada trozo al lado — y luego ejecutarlo y comprobar qué ha pasado. El
código se da; el trabajo es teclearlo, verlo parsear y verlo correr.

Dos redes de seguridad:

- **[`solutions/`](solutions/)** tiene una copia terminada de cada archivo, con
  los comentarios didácticos dentro. Compara cuando algo no parsee, o sáltate
  una lección copiando su archivo: `cp solutions/03_dataops.py dags/`. El
  [apéndice](docs/appendix-dags_ES.md) imprime cada solución completa.
- **Los tests de la §14** leen *tu* carpeta `dags/`. Cuando los siete estén
  escritos, te dicen si construiste lo que el tutorial quería decir.

Las lecciones se apoyan unas en otras a partir de la §9 — `mlops` se programa con
un asset que produce `dataops`, `sensors_and_events` espera un archivo que
escribe `dataops` — así que hazlas en orden, o copia antes las soluciones
anteriores.

### Por qué Airflow

Airflow no es el único orquestador — Prefect, Dagster, Luigi, Metaflow, Flyte,
Kubeflow Pipelines y Vertex AI Pipelines cubren el mismo terreno. Airflow merece
la pena como primero porque:

- **Adopción.** Es el estándar en la mayoría de plataformas de datos, así que los
  conceptos se transfieren y todos los vendors traen integración.
- **Python puro, sin DSL.** Los workflows son funciones normales, testeables con
  pytest (§14).
- **El mismo código corre en tu portátil y en un clúster** — cambias el executor,
  no el Dag.
- **Apache governance**, un ecosistema grande de providers, y ningún vendor
  único.

### El proyecto de ML que orquesta

Airflow orquesta código; no lo contiene. El código orquestado vive en
`src/tagifai/` y no importa Airflow en absoluto — un pequeño clasificador de
texto que etiqueta proyectos de ML. Esta parte no la escribes tú; los Dags la
llaman:

| Módulo | Papel |
|---|---|
| `data.py` | extract, clean, transform, split train/test |
| `validate.py` | expectations de calidad de datos, lo que ejecutan los gates |
| `train.py`, `evaluate.py` | clasificador TF-IDF + SGD, métricas y slices |
| `registry.py` | sustituto en sistema de archivos de un model registry |
| `monitoring.py` | comprobaciones de drift y de rendimiento en vivo |
| `predict.py` | etiqueta texto nuevo con lo que esté promocionado ahora mismo |
| `assets.py` | los Assets de Airflow que comparten los Dags |
| `callbacks.py` | callbacks de deadline alerts |

Cada uno es un hueco que puedes cambiar por una herramienta real — las tablas del
§9 y de la §10 dicen cuál.

## 2. Ponlo en marcha

### Docker (recomendado, y en Windows la única opción fuera de WSL2)

Airflow no tiene soporte nativo en Windows. En Windows usa Docker Desktop, o haz
la instalación nativa dentro de WSL2.

```bash
git clone <this repo>
cd Airflow3Tutorial
```

Luego levanta el stack. En Linux, macOS, WSL2 o Git Bash en Windows:

```bash
./scripts/up.sh
```

En Windows PowerShell, donde una instalación por defecto se niega a ejecutar
archivos `.ps1`, de ahí el flag:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\up.ps1
```

**Comprobación.** El script espera al API server y luego imprime la tabla de
servicios. Abre <http://localhost:8080> — no hay login. La lista de Dags está
**vacía**, y así debe ser: todavía no has escrito ninguno. La página Home
muestra el scheduler, el triggerer y el Dag processor como healthy. Si la página
no carga, salta a [troubleshooting](docs/troubleshooting.md).

```bash
docker compose ps
docker compose exec airflow-scheduler airflow dags list
```

El segundo imprime `No data found` — la misma lista vacía. Esas dos líneas son
la forma de todos los comandos de aquí en adelante. `docker compose …` corre en
tu máquina; `airflow …` corre dentro del contenedor del scheduler, que es lo que
hace el prefijo `docker compose exec airflow-scheduler`. Donde este tutorial
muestre un comando `airflow …` a secas — en una transcripción, o como una línea
suelta en el texto — significa lo mismo, prefijo incluido.

El stack es deliberadamente más pequeño que el [docker-compose.yaml
oficial](https://airflow.apache.org/docs/apache-airflow/3.3.1/docker-compose.yaml):
`LocalExecutor` en vez de Celery + Redis (la §3 explica qué es un executor), y el
Simple auth manager en vez de FAB — Flask-AppBuilder, el provider que da a
Airflow usuarios, roles y una página de login. Menos piezas móviles mientras
estás aprendiendo.

> **Auth:** está puesto `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=true`, así
> que la UI no tiene login, y el puerto 8080 se publica solo en `127.0.0.1` — la
> dirección de loopback — así que nada de fuera de tu máquina puede llegar a él.
> No despliegues esta configuración en ningún otro sitio.

Un script más, para más adelante. Todo lo que el stack acumula — runs, logs de
tasks, los datos y modelos que escriben los Dags — sobrevive a un reinicio de
los contenedores, porque vive en un volumen de Docker y en las carpetas
`data/`, `model_registry/` y `logs/` de este repo (la §3 vuelve sobre esto).
Para tirarlo todo y empezar de cero, en Linux, macOS, WSL2 o Git Bash:

```bash
./scripts/reset.sh
```

o en Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\reset.ps1
```

Pregunta antes de borrar, y `up.sh` / `up.ps1` vuelve a levantar el stack. No
toca `dags/` — los archivos que escribas sobreviven a un reset. Ningún paso de
este tutorial lo necesita; está aquí para el día en que algo se atasque y
[troubleshooting](docs/troubleshooting.md) no haya ayudado.

> **Una sola shell para el resto.** Todos los comandos a partir de aquí están
> escritos para bash: Linux, macOS, WSL2 o Git Bash en Windows. Los dos scripts
> `.ps1` existen para que PowerShell pueda levantar y resetear el stack, pero
> los comandos del tutorial usan continuaciones de línea con `\`, `grep` y JSON
> entre comillas simples, y PowerShell lee todo eso de otra manera. En Windows,
> abre Git Bash para todo lo que sigue.
>
> En **Git Bash**, unos pocos comandos de más adelante pasan una ruta `/opt/...`
> a un contenedor, y cada uno lleva el prefijo `MSYS_NO_PATHCONV=1`. No es
> decorativo: sin él, Git reescribe la ruta a una de Windows antes de que Docker
> la vea.

> **Tu editor.** A partir de la §4 vas a crear archivos en `dags/`. Vale cualquier
> editor; el contenedor ve la carpeta a través de un bind mount, así que un
> archivo guardado en tu máquina es un archivo dentro de Airflow un momento
> después. No hay que copiar nada.

### Nativo (Linux / macOS / WSL2)

```bash
./scripts/setup-native.sh
```

Lo que hace ese script, y por qué importa cada línea:

```bash
AIRFLOW_VERSION=3.3.1
PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install -r requirements.txt --constraint "${CONSTRAINT_URL}"   # constraints are NOT optional

export AIRFLOW_HOME=${PWD}/airflow
export PYTHONPATH=${PWD}/src                    # so tasks can import `tagifai`
export AIRFLOW__CORE__DAGS_FOLDER=${PWD}/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=5            # see §3: pick up a new file in seconds
export AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL=5

airflow db migrate                              # create the metadata database
airflow connections add fs_default --conn-type fs --conn-extra '{"path": "/"}'
```

Airflow 3.3.1 necesita Python ≥ 3.10 (y no 3.15). Instálalo **con el archivo de
constraints** — Airflow tiene un árbol de dependencias enorme y una instalación
sin constraints resolverá a algo que no funciona.

`airflow db migrate` rellena `$AIRFLOW_HOME`:

```
airflow.cfg     every setting, with the defaults commented — see config/airflow.cfg.example
airflow.db      the SQLite metadata DB
logs/           task logs, one directory per dag_id / run_id / task_id
```

> **SQLite es solo para desarrollo.** La documentación de Airflow es tajante:
> "should NEVER be used for production". Vale para seguir este tutorial —
> `LocalExecutor` ejecuta tasks en paralelo contra ella, y necesita SQLite ≥ 3.15
> — pero pásate a Postgres o MySQL para cualquier cosa real. El stack de Docker
> de este repo ya corre Postgres 16, y por eso el camino con Docker es el
> recomendado.

Cualquier ajuste de `airflow.cfg` también se puede dar como variable de entorno
`AIRFLOW__SECTION__KEY`. `SECTION` es la `[cabecera]` bajo la que está el ajuste
y `KEY` es el nombre del propio ajuste, ambos en mayúsculas y unidos por **doble**
guion bajo — un guion bajo *dentro* de cualquiera de los dos nombres sigue siendo
simple. El nombre de la variable va siempre en ASCII y en inglés: no se traduce,
ni lleva tildes.

```
airflow.cfg                          variable de entorno
─────────────────────────────────    ────────────────────────────────────────
[core]
executor = LocalExecutor             AIRFLOW__CORE__EXECUTOR=LocalExecutor

[dag_processor]
refresh_interval = 5                 AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=5
```

Es lo que hace el archivo de compose de este repo en vez de traer un archivo de
configuración: las dos de arriba son líneas reales de
[`docker-compose.yaml`](docker-compose.yaml).

> **Las connections por defecto no se crean solas.** `fs_default` se añade a mano
> arriba porque `FileSensor` (§11) la necesita. En Docker el archivo de compose
> la aporta como `AIRFLOW_CONN_FS_DEFAULT`.

Y después, o bien el proceso todo-en-uno:

```bash
airflow standalone
```

…o los componentes por separado, que es a lo que se parece un despliegue real:

```bash
airflow api-server --port 8080
airflow scheduler
airflow dag-processor            # a separate process — required
airflow triggerer                # deferrable tasks and async callbacks
```

## 3. Las piezas de Airflow

Airflow es un scheduler de workflows batch escritos en Python. Tú describes el
grafo en código, y Airflow decide cuándo corre cada nodo, lo reintenta, registra
el intento y te enseña el resultado. No mueve tus datos y no entrena tu modelo —
arranca lo que sí lo hace, espera y reacciona.

Eso también te dice cuándo buscar otra cosa. Airflow encaja mal con trabajo
sub-segundo o por evento (usa una cola o un procesador de streams), con cualquier
cosa que necesite un ciclo o un bucle `while` entre runs, y con cómputo pesado
dentro del worker — lanza un job de Spark/Ray/Kubernetes y haz seguimiento de él.
Si tu pipeline son tres scripts que corren una vez por semana, cron sigue estando
permitido.

Cuatro procesos, una base de datos y un executor — que, como explica la sección
siguiente, no siempre es un proceso propio. Todo eso está corriendo en tu stack
ahora mismo:

| Componente | Trabajo |
|---|---|
| **API server** | Sirve la REST API, la UI de React y la **Task Execution API** que llaman los workers |
| **Scheduler** | Decide qué ejecutar y entrega el trabajo al executor |
| **Dag processor** | Parsea tus archivos de Dag a la base de datos — su propio proceso |
| **Triggerer** | Ejecuta triggers async para tasks deferred (§11) y callbacks async |
| **Executor** | Donde corren de verdad las tasks. `LocalExecutor` aquí; Celery o Kubernetes en producción |
| **Metadata DB** | El estado de todo: runs, task instances, XComs, connections |

Esa tabla es el sistema entero, pero unas cuantas de esas filas deciden casi todo
sobre cómo se comportan tus Dags, y valen más que una fila cada una.

### El scheduler, y por qué las cosas pasan "un momento después"

El scheduler es un bucle. Se despierta, le hace unas preguntas a la metadata
database — qué Dag runs tocan, qué task instances tienen todas sus dependencias
aguas arriba satisfechas, qué asset events han llegado —, marca lo que encuentra
como `queued`, se lo pasa al executor y se vuelve a dormir. Y luego lo repite.
`[scheduler] scheduler_idle_sleep_time` es cuánto duerme cuando no encontró nada
que hacer; en este stack es **1 segundo**.

Por eso nada en Airflow es instantáneo, y por eso tampoco nada es muy lento.
Cuando `dataops` publica su asset en la §10, el run de `mlops` no aparece *porque*
la task terminó — aparece porque el siguiente bucle del scheduler vio el evento
que la task registró. Con un segundo de sueño entre bucles, ese hueco es como
mucho un segundo, y no lo vas a notar. Lo que *sí* vas a notar es lo que tarden
las propias tasks de aguas arriba, y eso no tiene nada que ver con el scheduler.

El parseo es otro reloj distinto, y es del que vas a depender enseguida. El Dag
processor vuelve a leer un archivo cambiado como mucho cada `[dag_processor]
min_file_process_interval` segundos y re-escanea la carpeta buscando archivos
nuevos cada `refresh_interval`. Este stack pone ambos a **5**, así que un
archivo que guardas aparece en la UI en unos segundos. Los valores por defecto
son 30 y 300: en un stack que los conserve, un archivo recién creado puede
tardar cinco minutos en aparecer, lo que se ve exactamente igual que un Dag roto
si no sabes que ese número existe.

Los tres números son settings, y puedes imprimir los tuyos:

```bash
docker compose exec airflow-scheduler airflow config get-value scheduler scheduler_idle_sleep_time
docker compose exec airflow-scheduler airflow config get-value dag_processor min_file_process_interval
docker compose exec airflow-scheduler airflow config get-value dag_processor refresh_interval
```

### El executor, y qué significa "a la vez"

El executor es la respuesta a "dónde corre de verdad el Python de esta task". El
scheduler nunca ejecuta tu código; solo decide *que* ese código debería
ejecutarse y entrega la task al executor, que la arranca en algún sitio e informa
de vuelta.

`LocalExecutor` — el de este stack — arranca cada task como **su propio
subproceso en la máquina que corre el scheduler**, usando `multiprocessing` de
Python. Paralelismo real, del tamaño de una máquina. `CeleryExecutor` en cambio
deja la task en una cola para un pool de máquinas worker de larga vida, y
`KubernetesExecutor` le pide a Kubernetes un pod nuevo por task. Cambiar entre
ellos no cambia ni una línea del código del Dag — esa es la afirmación de la §1, y
este es el mecanismo que hay detrás.

Y por eso `docker compose ps` no te muestra ningún contenedor de executor.
`LocalExecutor` es un *componente del proceso del scheduler*, no un servicio: el
scheduler lo importa y desde ahí forkea hijos. Cambia `AIRFLOW__CORE__EXECUTOR` a
`CeleryExecutor` y los workers pasan a ser máquinas reales que despliegas y
escalas por separado. El executor es la costura entre "un portátil" y "un
clúster", y es un valor de configuración.

Así que "worker" significa algo ligeramente distinto según el executor: aquí un
subproceso forkeado, allí un proceso worker de Celery, allá un pod en Kubernetes.
Lo constante es que el worker recibe la task, la ejecuta y responde por la Task
Execution API.

Cuántas corren a la vez lo decide el **más estrecho** de cuatro límites, todos
ellos valores por defecto en este stack:

| Límite | Valor aquí | Alcance |
|---|---|---|
| `[core] parallelism` | 32 | task instances corriendo en toda la instalación |
| `[core] max_active_tasks_per_dag` | 16 | task instances corriendo en un Dag |
| slots de `default_pool` | 128 | tasks no asignadas a otro pool |
| `max_active_tis_per_dag` en una task | sin fijar | esa task concreta, en todos sus runs |

(`airflow config get-value core parallelism` imprime el primero; `airflow pools
list`, el tercero.)

Un **pool** es un conjunto con nombre de slots al que asignas tasks con `pool="…"`
(Admin → Pools), y es la forma de evitar que veinte tasks concurrentes abran
veinte conexiones a una base de datos que tolera cinco. Todo cae en
`default_pool` salvo que digas otra cosa. La §10 pone el cuarto límite en una
sola task y lo verás morder.

### La metadata database

Cada hecho que Airflow conoce vive en una base de datos relacional: las
definiciones de Dag tal y como se parsearon, cada Dag run, cada task instance con
su estado y su número de intentos, XComs, Connections, Variables, pools, asset
events, el audit log. La UI es una vista de eso. El bucle del scheduler es una
consulta contra eso. Reinicia los contenedores y no se pierde nada, porque el
estado está en el volumen de la base de datos, no en los contenedores —
`scripts/reset.sh` borra ese volumen, y por eso es la opción nuclear.

De ahí salen dos consecuencias. Es lo que hay que respaldar, y crece para siempre
salvo que ejecutes `airflow db clean` (§15) — un año de runs, task instances y
XComs no se borra solo.

### El ciclo de vida de una task

El vocabulario es exacto, y confundirlo es lo que hace que los mensajes de error
sean confusos. Un **Dag** es el grafo que escribiste. Un **Dag run** es una
ejecución de él, identificada por un `run_id`, y está `queued`, `running`,
`success` o `failed` — esos cuatro, ninguno más. Una **task instance** es una
task dentro de un Dag run, y tiene muchos más estados, porque le pueden pasar más
cosas:

```
                                   ┌─► success              terminada
scheduled ──► queued ──► running ──┼─► failed               terminada — sin reintentos
     ▲                             │
     │                             ├─► up_for_retry         esperando retry_delay
     │                             ├─► up_for_reschedule    un sensor entre pokes
     │                             ├─► deferred             aparcada en el triggerer
     │                             └─► awaiting_input       aparcada hasta que responda alguien
     └─────────────────────────────────┘                    (o hasta el timeout)
        esas cuatro son "aparcada, volverá"

  nunca arrancó: skipped (un branch eligió otra cosa) · upstream_failed
```

La distinción que pilla a todo el mundo primero es **`queued` vs `running`**:

- **`scheduled`** — el scheduler ha decidido que esta task *puede* correr: sus
  tasks aguas arriba están hechas y su trigger rule está satisfecha. No ha
  arrancado nada.
- **`queued`** — el scheduler se la ha entregado al executor. Sigue sin
  ejecutarse nada. Una task que se queda aquí está esperando un *slot*, no a tu
  código, así que el sitio donde mirar son los cuatro límites de arriba, no el
  log de la task — que estará vacío, porque ningún proceso ha escrito uno
  todavía.
- **`running`** — un worker la tiene y tu Python se está ejecutando. Ahora sí hay
  logs.

El resto los irás conociendo sobre la marcha: `up_for_retry` entre intentos (§4),
`skipped` desde un branch (§10), `up_for_reschedule` desde un sensor que sale
entre pokes y `deferred` desde uno suspendido en el triggerer (§11),
`awaiting_input` mientras decide una persona (§12), y `upstream_failed` en todo
lo que hay aguas abajo de un fallo.

### El código de una task nunca toca la base de datos

El punto arquitectónico que merece la pena interiorizar: **el código de una task
nunca toca la metadata database.** Una task habla con la Task Execution API por
HTTP, que es lo que hace posibles los workers remotos y el Edge executor — y por
lo que una task no puede abrir una sesión de base de datos y consultar `DagRun`
por su cuenta.

```bash
curl -s localhost:8080/api/v2/monitor/health
```

(Si aun así estás en PowerShell, ahí `curl` es un alias de `Invoke-WebRequest`
y rechaza `-s`; llama a `curl.exe` en su lugar.)

Deberías obtener `healthy` cuatro veces — un objeto JSON por cada uno: la
metadatabase, el scheduler, el triggerer y el dag processor, los tres últimos con
la hora de su heartbeat más reciente. La documentación interactiva de la API está
en <http://localhost:8080/docs>.

## 4. Tu primer Dag

Todo lo que necesita el autor de un Dag está en `airflow.sdk`, el Task SDK:

```python
from airflow.sdk import dag, task, DAG, Param, Asset, chain, Label
```

Vas a escribir `dags/01_hello_airflow3.py`: cuatro tasks que construyen un
saludo, lo gritan, cuentan sus palabras e imprimen el resultado. Lo bastante
pequeño para tenerlo en la cabeza, y cada idea que contiene vuelve en todos los
Dags posteriores.

### Constrúyelo

Crea el archivo `dags/01_hello_airflow3.py` y empiézalo con un docstring, los
imports y un diccionario de valores por defecto:

```python dags/01_hello_airflow3.py
"""Lesson 1 - a Dag, tasks, and dependencies."""

from __future__ import annotations

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
horaria que Airflow usa en todas partes; un `datetime` naive en un `start_date`
es un error que preferirás no descubrir a las 03:00 UTC.

**`default_args` es un diccionario que se aplica a todas las tasks del Dag.**
Puede llevar cualquier cosa que acepte una task, y una task que fije el mismo
argumento por su cuenta gana. Existe para que no repitas `owner=` y `retries=` en
veinte tasks.

`owner` es una etiqueta: sale en la lista de Dags, puedes filtrar por ella, y
dice a quién preguntar cuando el Dag se rompe. Los tests de integridad de este
repo (§14) rechazan el valor por defecto, `airflow`, porque un owner que no es
nadie significa que no se avisa a nadie.

Los otros dos son lo primero que vas a desear haber puesto:

- **`retries=2`** — si la task lanza una excepción, Airflow la ejecutará hasta
  dos veces más. La task instance pasa a `up_for_retry`, espera y se vuelve a
  programar. Es la *misma* task instance todo el rato: su `try_number` sube, cada
  intento guarda su propio log, y solo el resultado del último intento decide si
  corren las tasks de aguas abajo.
- **`retry_delay`** — cuánto espera antes del siguiente intento. Treinta segundos
  es generoso para una demo y corto para trabajo real; una API inestable suele
  querer minutos, y `retry_exponential_backoff=True` los separa progresivamente.

**El valor por defecto es `retries=0`** (`[core] default_task_retries`), lo que
significa que una task con un mal momento de red simplemente falla y se lleva el
run por delante. Reintentar es opt-in, y por eso los tests de integridad de este
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
- **`schedule=None`** — este Dag corre solo cuando algo se lo pide: tú, la UI o
  la API. La §7 tiene los otros valores que puede tomar un schedule.
- **`start_date`** — nunca se programa un run antes de este momento (§7). Un Dag
  sin él parsea igual, así que Airflow no te lo va a recordar.
- **`catchup=False`** — el valor por defecto, puesto explícitamente porque es el
  flag que decide si quitar la pausa a un Dag te inunda de runs históricos (§7).
- **`tags`** — etiquetas libres; la lista de Dags filtra por ellas.
- **`doc_md=__doc__`** — el docstring del módulo que acabas de escribir,
  renderizado como markdown en la página del Dag en la UI. `description=` es el
  primo corto, una línea que se muestra junto al Dag en la lista.
- **`params`** — entradas declaradas, con tipos. La UI las convierte en un
  formulario cuando disparas el Dag (§5), y un run registra los valores que usó.

Dentro de la función, las tasks. Indéntalas — son el cuerpo de
`hello_airflow3()`:

```python dags/01_hello_airflow3.py
    @task
    def make_greeting(**context) -> str:
        greeting = context["params"]["greeting"]
        when = context.get("logical_date") or "no logical date (manual run)"
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
imprima aterriza en el log de la task, visible en la UI.

`**context` es cómo una task ve el run al que pertenece: `run_id`, `params`, `ti`
(la task instance), `dag_run`, `ds`, `data_interval_start`/`end`, `var`, `conn`,
`macros`. Lee `logical_date` con `.get()`, nunca con `context["logical_date"]`:
un run manual o disparado por asset no tiene logical date, y entonces la clave
falta del contexto por completo — indexarla lanza `KeyError` en tiempo de
ejecución, mientras `airflow dags test` (que siempre aporta una) sigue en verde.
La §7 explica el porqué. `run_id` siempre está; prefiérelo para nombrar cosas.

Termina el archivo cableando las tasks entre sí y registrando el Dag:

```python dags/01_hello_airflow3.py
    message = make_greeting()
    report(loud=shout(message), words=count_words(message))


hello_airflow3()
```

**Pasar la salida de una task a otra dibuja la arista.** `shout(message)` es lo
que hace que `shout` corra después de `make_greeting`. El grafo se construye
cuando se parsea el archivo; el código corre después, posiblemente en otra
máquina. Nada hace que `shout` y `count_words` se esperen entre sí, así que un
run dirigido por el scheduler es libre de ejecutarlas a la vez (la §3 dice qué lo
decide):

```
make_greeting ─┬─► shout ──────┬─► report
               └─► count_words ┘
```

Y la última línea: **el decorador solo define el Dag; llamar a la función lo
registra.** Un archivo de Dag que define un Dag y nunca lo llama no produce nada
— importa limpiamente y no aparece en ningún sitio. Es el error de principiante
más común y el más difícil de ver, porque no hay ningún error que leer.

Guarda el archivo. Compáralo con
[`solutions/01_hello_airflow3.py`](solutions/01_hello_airflow3.py) si quieres —
es el mismo código con los comentarios dentro.

### Míralo parsear

El Dag processor (§3) escanea `dags/` cada cinco segundos en este stack,
importa cada archivo `.py` que encuentra, y escribe lo que parseó en la metadata
database. A los pocos segundos de guardar:

```bash
docker compose exec airflow-scheduler airflow dags list -o plain
```

**Comprobación.** Una fila: `hello_airflow3`, owner `['mlops-course']`,
`is_paused True`, `bundle_name dags-folder`. Recarga la UI y el Dag está en la
lista, en pausa. (Todos los Dags empiezan en pausa — §7.)

Si la fila no está, pueden estar pasando dos cosas, y desde aquí parecen
idénticas. La primera es un archivo que lanza una excepción al importarse — una
errata, un paquete que falta, una función `@dag` que revienta. Eso no es un Dag
roto en la lista, es **ningún Dag**: ausente de la UI, ausente de `airflow dags
list`. Aparece en la página Home (§5) y con este comando:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors -o plain
```

**Comprobación.** `No data found` — tu archivo importa. Mira cómo se ve el otro
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
se reporta nunca — silencio, no un error. Todo archivo de Dag real pasa esta
prueba por importar de `airflow.sdk`, así que solo muerde a módulos auxiliares y
experimentos; pero es lo segundo que comprobar cuando un archivo simplemente no
está.

La otra clase de nada es el error 1 de arriba — un archivo que define un Dag y
nunca lo llama. Importa limpiamente, así que `list-import-errors` también está
vacío, y el único síntoma es un Dag que no está. Mira primero esta lista, y
después el final del archivo.

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

y, al final, `DagRun Finished: ... state=success`. Ese es el bucle de autoría más
rápido que existe: un run completo con logs reales y sin UI. Lo vas a usar
después de cada Dag que escribas.

> **Qué es `airflow dags test`, y qué no.** Ejecuta el Dag dentro de este único
> comando — sin scheduler, sin executor, sin triggerer — y eso cambia cuatro
> cosas que te encontrarás más adelante:
>
> - Las tasks corren **de una en una**, en un orden que el grafo permita. En el
>   log de arriba, una de `shout`/`count_words` terminó antes de que empezara la
>   otra; cuál va primero varía de un run a otro.
> - **Siempre aporta una logical date.** Un trigger manual de verdad no (§7), que
>   es la forma en la que un Dag puede pasar aquí y fallar en producción.
> - **No es un dry run**: las tasks escriben archivos reales y asset events
>   reales (§9).
> - Una task que se defiere ejecuta su trigger dentro del mismo proceso (§11), y
>   en 3.3.1 el comando revienta con cualquier Dag que tenga un `deadline=`
>   (§13).
>
> Es la forma más rápida de ver funcionar un Dag; la §14 dice cuándo dejar de
> fiarse de él.

> **Pruébalo.** Cambia el saludo sin tocar el archivo:
> `airflow dags test hello_airflow3 --conf '{"greeting": "hola"}'`. La línea de
> `report` empieza ahora por `HOLA`. Ese `--conf` llegó a la task a través de
> `context["params"]` — el `Param` que declaraste, sobrescrito para un run.

**La otra forma de declarar un Dag** es el context manager, que está plenamente
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

Cualquier cosa creada dentro del bloque `with` se engancha a ese Dag
automáticamente. Ambas formas producen el mismo objeto, y un archivo puede tener
varios Dags.

## 5. La UI

La UI es una aplicación React encima de la REST API, servida por el api-server en
<http://localhost:8080>. La mitad de lo que enseña este tutorial se ve más fácil
de lo que se lee, así que haz el recorrido ahora que hay un Dag que mirar.

La navegación es **Home · Dags · Assets · Browse · Admin · Plugins · Security ·
Docs**.

**Home** — salud del scheduler, el triggerer y el Dag processor, errores de
import, runs y tasks fallidos, asset events recientes, deadlines y slots de
pools. Ábrela primero cuando algo no corre: un Dag que nunca aparece en la lista
suele ser un error de import esperándote en esta página.

**Dags** — la lista, como tarjetas o como tabla, filtrable por tag, owner o
estado, con el toggle de pausa y un botón **Trigger** en cada fila. La tuya
muestra `hello_airflow3`, su owner, sus dos tags y el único run que hizo
`airflow dags test`. Un Dag programado con assets muestra su condición de asset
donde un Dag con cron muestra su schedule; el tuyo, con `schedule=None`, no
muestra ninguna de las dos.

**La página de un Dag** pone el histórico de runs a la izquierda y las pestañas
de detalle a la derecha. El panel izquierdo tiene **Grid** y **Graph** — `g`
alterna entre ambos — más **Show Gantt**, un control de dirección del grafo y
filtros que conviene conocer:

- **Dependencies: All Dag Dependencies / External conditions / Only tasks.**
  Cámbialo de "Only tasks" en `mlops` (§10) y el grafo dibuja el asset
  `labeled_projects` que disparó el run, no solo las tasks de dentro.
- **Min. map index** — "shows mapped tasks expanded to at least this index". Una
  mapped task como `train_candidate` (§10) es un único nodo colapsado hasta que
  subes esto; súbelo y cada conjunto de hiperparámetros tiene el suyo.
- Indicadores de versión, y **Download graph image**.

Las pestañas son Overview, Runs, Tasks, Backfills, Asset Events, Audit Log,
Calendar, Code, Details y — cuando una task espera a una persona — **Required
Actions**. Haz clic en un run, luego en una task, y la misma tira pasa a ser la
de esa task instance: **Logs**, **XCom**, **Rendered Templates**, y **Task
Instances** para un fan-out mapeado.

**Hazlo ahora.** Abre `hello_airflow3`, mira el Graph — el fan-out y el fan-in
que dibujaste con dos llamadas a función — y luego haz clic en el run, en
`report`, y abre **Logs**: la línea `(7 words)`, esta vez en el navegador. Haz
clic en `make_greeting` y abre **XCom**: la cadena del saludo, guardada bajo la
clave `return_value`. Ese es el valor que recibieron `shout` y `count_words`.

**El selector de versión del Dag.** Cada Dag run registra la versión del código
con la que corrió, así que el selector (`v1`, `v2`, … o **All Versions**) te deja
mirar un run a través del código que de verdad lo produjo, con el nombre del
bundle, la versión del bundle y la hora de parseo en la pestaña **Code**. Un
*bundle* es un sitio del que el Dag processor carga Dags; este stack tiene uno,
la carpeta `dags/`, y se llama `dags-folder` — la columna `bundle_name` que
viste en `airflow dags list` (la §15 tiene los otros tipos). Cada vez que
guardas un cambio en un archivo de Dag su cuenta de versiones sube, y el run de
la semana pasada sigue mostrando el grafo de la semana pasada.

**El formulario de Trigger se genera a partir de tus `params`.** Por eso merece
la pena declarar los params con tipos:

```python
params={"greeting": Param("hello", type="string")}              # dags/01
POLICY_PARAMS = {"max_label_tvd": Param(0.15, type="number")}   # dags/06
```

Un param string se convierte en un campo de texto, un number en uno numérico, un
enum en un desplegable, y los campos obligatorios se validan antes de crear el
run. Bajo **Advanced Options** lo mismo es editable como **Configuration JSON**
en crudo, y el formulario ofrece además un run id, una logical date, un data
interval manual y una casilla *"Unpause … on trigger"* — que existe porque
disparar un Dag en pausa deja el run **queued** para siempre (§7).

**Hazlo ahora.** Pulsa **Trigger** en `hello_airflow3`. El formulario tiene un
campo, `greeting`, relleno con `hello` — escribe otra cosa, marca *Unpause
hello_airflow3 on trigger* y envía. Aparece un segundo run en el Grid; el log de
su `report` lleva tu palabra en mayúsculas. Es el mismo `Param` llegando a la
misma task por una tercera vía: un valor por defecto en el archivo, `--conf` en
la línea de comandos, un formulario en el navegador.

El mismo diálogo tiene un segundo modo, **Backfill**: un rango de fechas, un
**Reprocess Behavior** (missing runs / missing and errored runs / all runs), max
active runs, "run backwards", y un dry run que te dice *"N runs will be
triggered"* antes de confirmar. Necesita un Dag con schedule, así que espera al
§9. Los Dags programados con assets dicen *"This Dag's schedule does not support
backfills"* — a `mlops` (§10) no se le puede hacer backfill, porque no hay rango
de fechas que reproducir, solo asset events.

**Assets** — la pestaña para la que se construyó este tutorial, y vacía hasta el
§9. En cuanto existan los Dags que los usan, lista cada asset con sus tasks
productoras, los Dags consumidores y el histórico de eventos, y dibuja el grafo
entre Dags: `dataops` → `labeled_projects` → `mlops` → `production_model`. Ese
grafo es la única imagen del pipeline completo; la lista de Dags no puede
enseñarlo, porque la conexión no es una dependencia entre Dags, es un asset entre
ellos. **Create Event** sobre un asset o bien lo materializa (ejecuta el Dag
aguas arriba) o registra un evento manual — que es como disparas un Dag aguas
abajo sin fingir que has producido los datos.

**Browse** — Audit Log (quién disparó, pausó o limpió qué — tu trigger desde la
UI ya está ahí), **Deadlines** (deadline alerts pendientes y perdidas, §13) y
XComs.

**Admin** — Connections, Variables, Pools, Providers, Plugins, Jobs y la
**Config** renderizada de Airflow. Connections y Variables son las dos que vas a
usar: nunca metas una credencial en un archivo de Dag.

> **Security y Plugins** están casi vacías aquí. Este stack corre el Simple auth
> manager con todo el mundo como admin, así que no hay usuarios ni roles que
> gestionar.

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
bajo Admin → Providers. Tira de uno antes de escribir llamadas HTTP a mano: el
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

Ese es el movimiento correcto cuando un paso se reutiliza en varios Dags y
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
`None` en silencio. Casi todos los pull que escribas quieren `task_ids=`; el
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
propia task, así que una plantilla puede alcanzar cualquier cosa del task context
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
3. **Fan-out y fan-in**: una lista a cualquier lado de `>>` difunde la arista,
   así que `product >> [check_parity, check_range] >> checks_done` son cuatro
   dependencias, no dos.
4. **`chain()`** — la forma legible de una línea larga de `>>`, y la única que
   puede llevar un `Label`, que anota la arista en la vista Graph.

Y la quinta cosa, `cross_downstream`, para la forma que parece que debería
funcionar con una lista y no:

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
[`solutions/02_operators_and_xcoms.py`](solutions/02_operators_and_xcoms.py).

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
en el archivo de compose de este repo. Las plantillas las alcanzan como
`{{ conn.fs_default.host }}` y `{{ var.value.my_key }}`.

> Una task que consulta cualquiera de las dos no toca la metadata database: la
> petición va por la Task Execution API, que es lo que permite a un worker remoto
> resolver una Connection sin tener acceso a la base de datos.
>
> Prefiere un **param** (§5) a una Variable global cuando el valor sea una
> palanca de política: los params quedan registrados con el run que los usó, las
> Variables no.

## 7. Scheduling y logical dates

Los dos Dags que llevas tienen `schedule=None`. Los otros valores:

```python
schedule=None                       # manual / API only
schedule="0 3 * * *"                # cron
schedule=timedelta(days=1)          # interval
schedule=[some_asset]               # data-aware (§8)
schedule=CronTriggerTimetable(...)  # a Timetable, named directly
```

Cada una de las cuatro primeras es un atajo para un **Timetable**, el objeto que
posee las dos decisiones que toma un schedule; la última línea nombra uno
directamente, y escribir tu propia clase Timetable es como consigues una
cadencia que cron no sabe expresar.

Las dos decisiones: *cuándo* crear un run, y para qué trozo de tiempo es ese run
— su **data interval**, expuesto a todas las tasks como `data_interval_start` y
`data_interval_end`, y etiquetado por la `logical_date` del run. Ninguno de esos
es "cuándo corrió"; eso es `run_after`. Cómo se corta el trozo depende de la
forma de `schedule=`, y las dos formas comunes difieren:

- **Una cadena cron** — `"0 3 * * *"`, que es el schedule que le darás a
  `dataops` en la §9 — dispara en el momento que nombra, y el trozo *es* ese
  momento: `logical_date`, `data_interval_start` y `data_interval_end` son las
  tres las 03:00.
- **Un `timedelta`** — `timedelta(days=1)` — es un lapso. El run *para*
  2026-09-10 se crea una vez que ese día ha terminado, a las 2026-09-11 00:00, y
  su `logical_date` es 2026-09-10 00:00: el principio del lapso, un día antes de
  que ocurra el run. Usa esta forma cuando una task tenga que procesar "los datos
  de ayer", porque el intervalo le dice exactamente qué día es ese.

`start_date` es donde empiezan los trozos: nunca se programa un run antes de él.
Un archivo de Dag sin él parsea igual sin error, así que esto es algo que
recuerdas tú, no algo de lo que Airflow te avise.

Retén esa imagen y el resto de esta sección son consecuencias de ella. Tres cosas
sobre scheduling que te ahorrarán una tarde:

**`catchup` es `False` por defecto** (`[scheduler] catchup_by_default`), y lo
que hace al quitar la pausa es preciso. Quitar la pausa a un Dag con cron y un
`start_date` de 2025 no crea un run por cada trozo desde entonces. Crea **uno**,
para el trozo más reciente que el cron ya ha dejado atrás — las 03:00 de hoy si
ya han pasado, las de ayer si no — y lo crea inmediatamente, porque ese trozo
está vencido. A partir de ahí, uno por trozo según van tocando. Lo verás
ocurrir en la §10: quitar la pausa a `dataops` basta para arrancarlo. Si
*quieres* el histórico, pídelo con `catchup=True` — o backfillea solo el rango
que necesitas (§9).

**Una cadena cron dispara a la hora que dice.** `schedule="0 3 * * *"` corre a
las 03:00 y la `logical_date` del run es las 03:00. Los timetables son libres de
definir eso de otra forma, y por eso merece la pena mirar la pestaña **Calendar**
del Dag y la columna *"Next run"* después de escribir uno.

**`logical_date` es nullable.** Etiqueta el trozo para el que es un run
programado — y los runs manuales y disparados por asset, que no son para ningún
trozo, simplemente no tienen ninguna. Así se verá la lista de runs de `mlops` en
la §10, un run de cada clase:

```bash
$ airflow dags list-runs mlops -o plain
dag_id  run_id                                      state    run_after             logical_date
mlops   asset_triggered__2026-09-09T08:23:16+00:00  success  2026-09-09T08:23:16              ← ninguna
mlops   manual__2026-09-09T08:22:31+00:00           success  2026-09-09T08:22:32   2026-09-09T08:22:31
```

Usa `run_after` cuando quieras decir "cuándo arrancó esto de verdad". Claves del
contexto disponibles en toda task: `ds`, `ts`, `logical_date`,
`data_interval_start`, `data_interval_end`, `run_id`, `dag_run`, `ti`, `params`,
`var`, `conn`, `macros`, `inlets`/`outlets`, `triggering_asset_events`.

### Runs manuales

Un schedule no es la única forma de arrancar un Dag. Dos comandos cubren casi
todo, y `hello_airflow3` es el Dag con el que probarlos:

```bash
docker compose exec airflow-scheduler airflow dags unpause hello_airflow3
docker compose exec airflow-scheduler airflow dags trigger hello_airflow3 -c '{"greeting": "hola"}' -r manual-demo-1
```

- **`-c/--conf`** es un objeto JSON. Aterriza en `dag_run.conf`, y cualquier
  clave que coincida con un `Param` declarado lo sobrescribe para ese run.
- **`-r/--run-id`** nombra el run. Sin él obtienes `manual__<timestamp>`.
- **`-l/--logical-date`** fija una. Sin ella el run no tiene ninguna — ver la
  trampa de abajo.
- En la UI lo mismo es el botón **Trigger**, con el formulario generado a partir
  de tus `params` (§5) — lo pulsaste en la §5.

Un Dag nuevo llega **en pausa** (`[core] dags_are_paused_at_creation`), así que
`unpause` no es opcional — y es el paso que todo el mundo olvida:

**Un Dag en pausa acepta un trigger igualmente — el run se crea y luego se queda
en `queued` hasta que le quitas la pausa.** No falla nada, no avisa nada:

```console
$ airflow dags pause hello_airflow3 && airflow dags trigger hello_airflow3 -r demo
$ airflow dags list-runs hello_airflow3 -o plain
dag_id          run_id  state   run_after                         logical_date
hello_airflow3  demo    queued  2026-09-09T12:19:29.483234+00:00             ← atascado
$ airflow dags unpause hello_airflow3     # ahora arranca
```

**Comprobación.** Ejecuta esos cuatro comandos tú mismo contra `hello_airflow3`.
Mientras el Dag está en pausa el run está `queued` y ahí se queda por mucho que
esperes — el scheduler lo ve en cada bucle y lo salta deliberadamente. Poco
después del `unpause`, `list-runs` muestra el mismo `run_id` como `success`, y su
`start_date` es posterior a su `run_after`: arrancó cuando quitaste la pausa, no
cuando disparaste. No se perdió nada y nada te avisó; simplemente estaba
retenido.

El formulario de trigger de la UI tiene una casilla *"Unpause … on trigger"*
precisamente para esto.

> **La trampa que conviene conocer antes de disparar nada:** un run manual **no
> tiene logical date**, así que `logical_date` falta en el task context y `ds`,
> `ts` y compañía están *undefined* en las plantillas. Ambas fallan en tiempo de
> ejecución, y ninguna falla bajo `airflow dags test`, que siempre aporta una
> logical date:
>
> ```python
> context["logical_date"]          # KeyError on a manual run
> context.get("logical_date")      # None — fine
> "{{ ds }}"                       # UndefinedError: 'ds' is undefined
> "{{ ds | default('none') }}"     # fine
> "{{ dag_run.run_after }}"        # fine — every run has one
> ```
>
> Escribiste la forma segura de ambas en las lecciones 1 y 2, y por eso los runs
> que acabas de disparar tuvieron éxito. Las dos fueron bugs reales en los Dags
> de este mismo tutorial hasta que un trigger manual los encontró. Los runs
> disparados por asset tampoco tienen logical date, así que `mlops` está expuesto
> a lo mismo.

Dos vías más de entrada: `airflow dags test <dag_id>` ejecuta el Dag entero
in-process (§14), y la REST API lo hace por HTTP —
`POST /api/v2/dags/{dag_id}/dagRuns`, que es lo que llama el botón de la UI.

## 8. Assets: programar por datos, no por tiempo

Un **Asset** es un trozo de datos con nombre. Una task declara que lo produce con
`outlets=`; un Dag declara que lo consume poniéndolo en `schedule=`. Ese es todo
el mecanismo, y sustituye a cada cron del tipo "corre 30 minutos después del otro
Dag, y reza" que hayas visto en tu vida.

Los assets que usa este tutorial ya están definidos, en
[`src/tagifai/assets.py`](src/tagifai/assets.py), para que ambos Dags importen
los mismos objetos:

```python
from airflow.sdk import Asset

LABELED_PROJECTS = Asset(
    name="labeled_projects",
    group="dataset",
    extra={"layer": "warehouse", "owner": "data-engineering"},
)
```

El lado productor es lo que escribirás en la §9 — fíjate en que el outlet va en
la task de **validación**, no en la de transform:

```python
@task(outlets=[LABELED_PROJECTS])
def validate_transformed(fp: str, *, outlet_events) -> dict:
    df = data.load_labeled(fp)
    result = validate.run_suite(df, "labeled_projects")
    if not result.passed:
        raise AirflowFailException(...)
    outlet_events[LABELED_PROJECTS].extra = {"rows": len(df), "path": fp}
    return {...}
```

Y el lado consumidor, §10:

```python
@dag(schedule=[LABELED_PROJECTS], ...)
def mlops():
    @task
    def prepare_data(*, triggering_asset_events):
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")
```

Esa es la idea en una línea: **los Dags aguas abajo se programan con "llegaron
datos buenos", nunca con "terminó una task" y nunca con una adivinanza de cron.**
El evento incluso lleva el recuento de filas que lo causó.

Más maquinaria de assets que conviene conocer:

| Funcionalidad | Qué hace |
|---|---|
| `AssetAny(a, b)` / `AssetAll(a, b)` | condiciones OR / AND en un `schedule` |
| `AssetAlias` | outlets cuya URI solo se conoce en runtime |
| `@asset` / `@asset.multi` | define asset + Dag + task en un solo decorador |
| `Metadata(asset, {...})` | adjunta metadatos haciendo yield desde la task |
| `AssetWatcher` + `BaseEventTrigger` | runs **event-driven** (§11) |
| `CronPartitionTimetable` | assets particionados |

Aquí los assets son **solo nombre**, lo que mantiene su identidad igual en Docker
y en runs nativos. En producción dales la URI real de los datos (`s3://…`,
`bq://…`) para que el lineage en la UI apunte a algún sitio.

Un asset existe a ojos de Airflow solo cuando un Dag parseado se refiere a él —
como outlet o como schedule. Ahora mismo `airflow assets list` imprime `No data
found`, y la pestaña Assets está vacía; la §9 cambia eso en cuanto guardes un
archivo.

### Qué es en realidad un asset event

Es una fila. Cuando una task con `outlets=[…]` **tiene éxito**, Airflow registra
un asset event — qué asset, qué Dag run lo produjo, cuándo, y lo que hayas puesto
en `outlet_events[…].extra`. Eso es todo lo que es un "trigger". Una task que
falla no escribe evento, y esa es toda la propiedad de seguridad: a `mlops` no lo
puede arrancar un dato que no pasó la validación, porque la task que lo habría
anunciado nunca terminó.

El scheduler hace entonces la segunda mitad. En su siguiente bucle lee los asset
events nuevos y encola un Dag run por **cada Dag sin pausa** programado con ese
asset.

Los eventos que llegan más rápido de lo que el consumidor puede correr no se
pierden ni se ejecutan de uno en uno. `mlops` permite un único run activo
(`max_active_runs=1`), así que si terminan tres runs de `dataops` mientras está
ocupado, el *siguiente* run de `mlops` se lleva los tres eventos, y
`triggering_asset_events` en `prepare_data` los lista todos. Un consumidor se
pone al día en un run, no en tres.

> **Un consumidor en pausa no encola, y no se pone al día.** La palabra *sin
> pausa* de arriba sostiene todo el peso. Si `mlops` está en pausa cuando
> `dataops` emite, el asset event se escribe igualmente — lo verás en la tabla y
> en la página de Assets — pero no se encola ningún run para `mlops`, y **quitar
> la pausa después no lo reproduce**. Airflow filtra los Dags en pausa en el
> momento en que decide qué encolar, así que después ya no queda nada que
> encontrar. Lo mismo vale para un consumidor que todavía no existía cuando se
> escribió el evento: el run de `dataops` que harás en la §9 emitirá un evento
> que `mlops`, escrito en la §10, nunca verá. (Comprobado de las tres formas en
> este stack — ver [qué se verificó de verdad](docs/verified_ES.md).)
>
> Esto es lo contrario del comportamiento con Dags en pausa de la §7, donde un
> trigger *manual* crea el run primero y la pausa solo lo retiene. El scheduling
> por asset todavía no tiene ningún run que retener.

## 9. El Dag de DataOps

`dags/03_dataops.py` — extract → validate → transform → validate. El primer Dag
de aquí que hace trabajo de verdad, y cada task es un hueco que puedes cambiar
por un vendor real:

| Task aquí | Opción en producción |
|---|---|
| `extract_projects` | Airbyte / Fivetran / un job EL |
| `validate_raw`, `validate_transformed` | Great Expectations / Soda / tests de dbt |
| `transform` | dbt Cloud / astronomer-cosmos |

Es también el primer Dag de aquí con un cron de verdad — `schedule="0 3 * * *"`,
el ejemplo de la §7 — así que en cuanto le quites la pausa en la §10 correrá solo:
una vez inmediatamente, para el trozo que su cron dejó atrás más recientemente,
y luego a las 03:00 UTC cada día, disparando cada run a su vez `mlops`.
(`monitoring`, §12, es el otro programado, a las 06:00.) Deja el stack levantado
toda la noche y los runs `scheduled__` que encuentres por la mañana son eso, no
un bug.

> **Sobre los datos de origen.** `projects.csv` llega ya etiquetado — una tabla
> de `id, created_on, title, description, tag` — así que `transform` limpia el
> texto y agrupa los tags raros, y ya está. Un warehouse real suele guardar las
> etiquetas en una tabla aparte, lo que convierte `transform` en un join y le da
> al Dag un segundo brazo de extract-y-validate alimentándolo:
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
SDK, porque ese es el namespace que tiene un worker remoto. Y `tagifai` importa
siquiera porque el archivo de compose pone `/opt/airflow/src` en `PYTHONPATH`
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

`outlets=` es la declaración de la §8; `outlet_events` se inyecta por nombre, como
`ti`, y deja que la task adjunte metadatos al evento que está a punto de emitir.
La task devuelve una **ruta**, no el dataframe: los XComs llevan punteros.

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

El gate de validación es la parte que merece la pena copiar, y dos detalles
suyos son fáciles de equivocar:

- **`AirflowFailException`, no `AirflowException`.** Un fallo de calidad de datos
  no es transitorio; con `retries=2` en los defaults, una excepción normal
  ejecutaría la misma comprobación tres veces y retrasaría la alerta dos
  minutos. `AirflowFailException` falla la task al momento, con o sin
  reintentos.
- **Las expectations son no-críticas cuando significan "mira esto", no "para".**
  Que aparezca un tag inesperado es una señal para inspeccionar el espacio de
  etiquetas, no un motivo para detener el pipeline. `validate.py` marca esa con
  `critical=False`, así que aterriza en `result.failures` sin cambiar
  `result.passed`.

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
de la §6 en la práctica — aristas implícitas para los datos, `>>` para el permiso.

### Mira aparecer los assets

Guarda el archivo. A los pocos segundos `airflow dags list` tiene una tercera
fila — y algo más ha cambiado:

```bash
docker compose exec airflow-scheduler airflow assets list -o plain
```

**Comprobación.** Dos filas, `labeled_projects` y `raw_projects`, cada una con
su grupo y su extra. No ha corrido nada; el Dag processor parseó un archivo con
`outlets=` dentro, y con eso bastó. `production_model`, definido en el mismo
`assets.py`, no aparece — ningún Dag se refiere a él todavía. La pestaña Assets
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
necesita conexión a internet, y el recuento de filas sale de ese archivo.)

Si una task falla, compara con [`solutions/03_dataops.py`](solutions/03_dataops.py).

> **`airflow dags test` no es un dry run.** Ejecutó las tasks reales y escribió
> las filas reales: el archivo en disco, y un asset event `labeled_projects`
> genuino en la metadata database — abre la pestaña Assets y ahí está, con el
> `extra` que le adjuntaste. Todavía no hay nada programado con ese asset, así
> que no pasa nada más. Y según la §8, no pasará: cuando `mlops` aparezca en el
> §10 este evento ya será historia, y Airflow no reproduce la historia.

### Backfills

Ahora que tienes un Dag con schedule, los backfills tienen sentido. Un backfill
crea los runs de cada trozo de un rango de fechas, a posteriori. Son objetos de
primera clase, con su propia API, su formulario en la UI y su política de
reprocesado. **Haz un dry-run primero** — un backfill de un Dag concurrido es la
forma más fácil de crear treinta runs por accidente:

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
  sin pausa entrena detrás de ellos. Hacer backfill de un productor lo hace
  también de sus consumidores.
- La CLI en 3.3.1 solo *crea* backfills. Para parar uno, cancélalo en la pestaña
  **Backfills** del Dag, o por la API — `PUT /api/v2/backfills/{id}/cancel`, sin
  token en este stack de todos-admin — lo que marca sus runs aún en cola como
  `failed`. Pausar el Dag es el freno de emergencia: un Dag en pausa no arranca
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

La forma del Dag no cambia: `outlets=[LABELED_PROJECTS]` sigue yendo en la última
task de validación, y todo lo de aguas abajo sigue programándose con el asset.
Esos tres providers **no** están instalados aquí, y por eso este tutorial no
necesita ninguna cuenta de vendor.

## 10. El Dag de MLOps

`dags/04_mlops.py` — sin schedule, disparado por el asset.

```
prepare_data ─► validate_training_data ─► train_candidate×N ─► select_best
             ─► evaluate_best ─► promotion_gate ─► promote_model ──► done
                                               └─► keep_current_model ─┘
```

| Task | Opción en producción |
|---|---|
| el trigger por asset + `prepare_data` | el cliente de tu warehouse — o, como aquí, deja que el asset event lleve el puntero |
| `validate_training_data` | Great Expectations / Pandera / Soda, más comprobaciones del split |
| `candidates` + `train_candidate.expand()` | Optuna / Ray Tune / un servicio de sweeps |
| `select_best` + `evaluate_best` | tu experiment tracker (MLflow, W&B) |
| `promotion_gate` → `promote_model` | la transición de stage de tu registry |

### Constrúyelo

Funciones nuevas de la carga de trabajo: `data.split(df)` devuelve un par
estratificado `(train, test)`; `validate.check_split(train, test)` es una
segunda suite, sobre el split; `train_module.SEARCH_SPACE` es una lista de dicts
de hiperparámetros y `train_module.train_from_files(hparams)` ajusta un modelo,
devolviendo `(model, metrics)`; `registry.save_run`, `load_metrics`, `promote` y
`get_production` son el registry en sistema de archivos;
`config.MIN_F1_TO_PROMOTE` es el umbral de promoción, leído de `TAGIFAI_MIN_F1`
si está definida.

Crea `dags/04_mlops.py`:

```python dags/04_mlops.py
"""Lesson 4 - the MLOps Dag: asset-triggered training, tuning, and promotion."""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException

from tagifai import config, data, registry, train as train_module, validate
from tagifai.assets import LABELED_PROJECTS, PRODUCTION_MODEL

DEFAULT_ARGS = {
    "owner": "ml-engineering",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=2),
}


@dag(
    dag_id="mlops",
    schedule=[LABELED_PROJECTS],
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "mlops"],
    doc_md=__doc__,
)
def mlops():
```

`schedule=[LABELED_PROJECTS]` es todo el sentido: una lista de assets, y el Dag
corre cuando se actualizan. Varios assets en la lista significan *todos*;
`AssetAny(a, b)` significa cualquiera.

**Valida antes de entrenar.** `dataops` validó `labeled_projects` a su salida del
warehouse, y este Dag lo valida otra vez a la entrada. Eso no es redundante: fue
otro Dag run, posiblemente hace horas, este run puede haberse disparado a mano, y
lo que el modelo ajusta es el snapshot que hay en disco *ahora*. Añade las dos
primeras tasks:

```python dags/04_mlops.py
    @task
    def prepare_data(*, triggering_asset_events) -> dict[str, int]:
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")
        df = data.load_labeled()
        train_df, test_df = data.split(df)
        return {"num_train": len(train_df), "num_test": len(test_df)}

    @task
    def validate_training_data(split_sizes: dict[str, int]) -> dict[str, Any]:
        df = data.load_labeled()
        table = validate.run_suite(df, "labeled_projects")
        split = validate.check_split(*data.split(df))
        print(f"{table.summary()} {split.summary()}")
        if not (table.passed and split.passed):
            raise AirflowFailException(
                f"Training snapshot failed validation: {table.failures + split.failures}"
            )
        return {
            "rows": len(df),
            "split_sizes": split_sizes,
            "warnings": [r["expectation"] for r in table.failures + split.failures],
            "results": table.results + split.results,
        }
```

`triggering_asset_events` se inyecta por nombre — los eventos que causaron este
run, indexados por asset, cada uno con el `extra` que adjuntó el productor. En un
run que arranques a mano está vacío. `validate_training_data` vuelve a leer la
tabla en vez de recibirla de `prepare_data`: los XComs llevan metadatos, nunca
payloads. La segunda mitad de su comprobación es una que una suite a nivel de
tabla no puede hacer en absoluto — mira el **split**:

```python
# src/tagifai/validate.py — check_split()
("split_is_non_empty",          ...,  critical)
("classes_in_both_splits",      ...,  critical)   # a class only in train
("min_rows_per_class:20",       ...,  critical)   # 3 rows of a class in train
("no_id_leakage",               ...,  critical)   # the same project in both
("test_fraction_between_0.1_and_0.4", ..., non-critical)
```

Cada una de esas pasa una suite a nivel de columna y aun así produce un modelo
cuyo f1 en held-out no significa nada — y ese f1 es exactamente lo que el
promotion gate compara contra producción.

**El dynamic task mapping** despliega una task instance por conjunto de
hiperparámetros en runtime, y luego reduce. Tres tasks:

```python dags/04_mlops.py
    @task
    def candidates() -> list[dict[str, Any]]:
        return train_module.SEARCH_SPACE

    @task(max_active_tis_per_dag=2)
    def train_candidate(hparams: dict[str, Any], **context) -> dict[str, Any]:
        config.ensure_dirs()
        run_id = f"{context['run_id']}__{context['ti'].map_index}".replace(":", "-").replace("+", "-")
        model, metrics = train_module.train_from_files(hparams)
        registry.save_run(run_id, model, metrics)
        print(f"run={run_id} hparams={hparams} f1={metrics['f1']:.4f}")
        return {"run_id": run_id, "f1": metrics["f1"], "hparams": hparams}

    @task
    def select_best(results: list[dict[str, Any]]) -> dict[str, Any]:
        best = max(results, key=lambda r: r["f1"])
        print(f"best={best['run_id']} f1={best['f1']:.4f} of {len(results)} candidates")
        return best
```

`candidates` devuelve una lista; en el cableado de abajo,
`train_candidate.expand(hparams=candidates())` crea una task instance por
elemento en runtime — cuatro aquí, una por conjunto de hiperparámetros — y cada
una se reintenta, se loguea y se inspecciona por su cuenta.
`context['ti'].map_index` es cuál de ellas es esta. El paso de reduce,
`select_best`, simplemente recibe una `list`. Fíjate en el valor de retorno de
`train_candidate`: un run id, un f1 y los hparams — pequeño y serializable a
JSON. El modelo en sí se queda en el registry.

**Cuántas de las cuatro entrenan a la vez es una decisión, y está escrita ahí
arriba.** Nada en `.expand()` dice "en paralelo"; dice "estas son cuatro task
instances separadas". Que se solapen o no es asunto del executor, acotado por el
más estrecho de los límites de la §3 — y `max_active_tis_per_dag=2` en esa task es
deliberadamente el más estrecho aquí. Así que el fan-out corre **de dos en dos,
en dos oleadas**. Sube el tope y las cuatro se solapan; quítalo y `[core]
max_active_tasks_per_dag` (16) es el siguiente límite. Un tope en una task de
entrenamiento no es un ajuste de juguete: cuatro modelos ajustándose a la vez son
cuatro copias de los datos en memoria, y un `pool` es cómo dices lo mismo sobre
una base de datos que no aguanta cuatro conexiones.

> **Ojo:** el parámetro se llama `hparams`, no `params`. TaskFlow inyecta claves
> del contexto por nombre de parámetro, y `params` está reservado — un argumento
> de task llamado `params` recibe los params del Dag, no tus datos.

El **promotion gate** es un branch, no un fallo. Añade la evaluación, el gate, la
promoción y los dos `EmptyOperator` que el branch necesita:

```python dags/04_mlops.py
    @task
    def evaluate_best(best: dict[str, Any]) -> dict[str, Any]:
        metrics = registry.load_metrics(best["run_id"])
        for name, slice_metrics in metrics.get("slices", {}).items():
            print(f"slice {name}: f1={slice_metrics['f1']:.4f} n={slice_metrics['support']}")
        return {"run_id": best["run_id"], "metrics": metrics}

    @task.branch
    def promotion_gate(evaluation: dict[str, Any]) -> str:
        f1 = evaluation["metrics"]["f1"]
        threshold = config.MIN_F1_TO_PROMOTE
        current = registry.get_production()
        current_f1 = current["metrics"]["f1"] if current else 0.0

        if f1 < threshold:
            print(f"f1={f1:.4f} below threshold {threshold} - not promoting")
            return "keep_current_model"
        if f1 < current_f1:
            print(f"f1={f1:.4f} below production {current_f1:.4f} - not promoting")
            return "keep_current_model"
        return "promote_model"

    @task(outlets=[PRODUCTION_MODEL])
    def promote_model(evaluation: dict[str, Any], *, outlet_events) -> dict[str, Any]:
        pointer = registry.promote(evaluation["run_id"])
        outlet_events[PRODUCTION_MODEL].extra = {
            "run_id": pointer["run_id"],
            "f1": pointer["metrics"]["f1"],
            "previous_run_id": pointer["previous_run_id"],
        }
        print(f"promoted {pointer['run_id']} (was {pointer['previous_run_id']})")
        return pointer

    keep_current_model = EmptyOperator(task_id="keep_current_model")
    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")
```

Un `@task.branch` devuelve el `task_id` que hay que ejecutar; la otra rama queda
**skipped**, no failed. Un candidato que no llega al umbral es un desenlace
normal — el run tiene éxito y producción se queda como estaba. `promote_model`
emite el segundo asset, `production_model`, que todavía no consume nadie; apunta
el `schedule` de un Dag hacia él y la arista aparece en el grafo de Assets.

Las **trigger rules** son cómo una task declara qué espera de sus padres, y el
branching es donde necesitas una por primera vez. El join, `done`, necesita
`none_failed_min_one_success`, porque el `all_success` por defecto lo saltaría en
cuanto cualquiera de las ramas quedara skipped. Las útiles: `all_success` (por
defecto), `all_done` (corre pasara lo que pasara aguas arriba — tasks de
limpieza), `none_failed_min_one_success` (joins después de un branch),
`none_failed` (permite skips pero no fallos), `one_success`, `one_failed`
(caminos de alerta), `all_skipped` y `always`.

Termina el archivo con el cableado:

```python dags/04_mlops.py
    snapshot = validate_training_data(prepare_data())
    results = train_candidate.expand(hparams=candidates())
    snapshot >> results

    evaluation = evaluate_best(select_best(results))
    gate = promotion_gate(evaluation)

    gate >> promote_model(evaluation) >> done
    gate >> keep_current_model >> done


mlops()
```

`snapshot >> results` cablea el gate de validación aguas arriba de *todas* las
instancias mapeadas, sin llevar datos, solo permiso para ejecutarse. Las dos
líneas `>>` del gate le dan al branch sus dos brazos y los unen en `done`.

### Mira el cableado

Guárdalo. Ya has escrito las dos mitades del pipeline, y antes de ejecutar nada
puedes preguntarle a Airflow qué sabe:

```bash
docker compose exec airflow-scheduler airflow assets details --name labeled_projects -o plain
```

**Comprobación.** Entre las filas:

```
scheduled_dags    [{'dag_id': 'mlops', ...}]
producing_tasks   [{'dag_id': 'dataops', 'task_id': 'validate_transformed', ...}]
```

`producing_tasks` nombra la task exacta que lo emite; `scheduled_dags` nombra qué
arrancará cuando lo haga. Ninguno de los dos Dags menciona al otro en ninguna
parte. La pestaña Assets dibuja ahora el grafo entero, `dataops →
labeled_projects → mlops → production_model`.

### Mira cómo dispara la cadena entera

Esta es la recompensa de la §8. Quita la pausa primero al consumidor, para que esté
escuchando, y después al productor:

```bash
docker compose exec airflow-scheduler airflow dags unpause mlops
docker compose exec airflow-scheduler airflow dags unpause dataops
```

Con la primera línea no pasa nada: `mlops` no tiene schedule y no ha llegado
ningún evento. La segunda línea basta para arrancarlo todo, y no has disparado
nada — es la regla de la §7: `dataops` tiene un cron y un `start_date` de 2025,
así que en el momento en que se le quita la pausa el scheduler crea el run de
las 03:00 UTC más recientes que se perdió, y lo arranca. Dale como medio minuto,
y luego:

```bash
docker compose exec airflow-scheduler airflow dags list-runs dataops -o plain
docker compose exec airflow-scheduler airflow dags list-runs mlops -o plain
```

**Comprobación.** `dataops` tiene un run cuyo id empieza por `scheduled__` y
termina en `T03:00:00+00:00` — la fecha de hoy, o la de ayer si aún no son las
03:00 UTC — con eso como `logical_date` y un `start_date` de hace un momento.
(El run `manual__` del `airflow dags test` de la §9 también está.) Y `mlops` tiene
un run que nadie disparó: id que empieza por `asset_triggered__`, `running` si
has sido rápido, `success` en cuanto sus cuatro candidatos hayan entrenado, con
una `logical_date` **vacía** — los runs disparados por asset no tienen logical
date, exactamente igual que los manuales (§7). En la UI, abre **Assets** y verás
la arista que lo causó.

Ten claro de qué es la espera. Casi toda es `dataops` corriendo sus cuatro tasks
— `extract` descarga el archivo de origen, así que esa parte depende de tu
conexión. La parte de Airflow es pequeña: un bucle del scheduler para crear el
run de `dataops` tras quitar la pausa, y uno más para encolar `mlops` cuando
aterriza el asset event (§3). Si tras un minuto no ha aparecido nada, comprueba
que ambos Dags están sin pausa y que el run de `dataops` tuvo éxito — un
`validate_transformed` fallido no emite evento, por diseño.

Ahora las dos oleadas. Abre ese run de `mlops` en la UI, haz clic en
`train_candidate` y lee su pestaña **Task Instances** — cuatro filas, map index
del 0 al 3, cada una con su hora de inicio y de fin. Dos arrancan juntas; las
otras dos arrancan solo después de que el primer par haya terminado. **Show
Gantt** dibuja lo mismo como dos pares de barras. En el **Graph**, las cuatro
son un único nodo colapsado hasta que subes el filtro **Min. map index** (§5). O
léelo desde la CLI:

```bash
docker compose exec airflow-scheduler airflow tasks states-for-dag-run mlops <the asset_triggered__ run id> -o plain
```

Cuatro filas de `train_candidate` con `map_index` de 0 a 3; las horas de inicio
de la 2 y la 3 son posteriores a las de fin de la 0 y la 1. Y
`keep_current_model` está `skipped` mientras `promote_model` está `success`: el
gate cogió el brazo de promocionar, porque no había modelo en producción al que
batir.

> **Pruébalo, una vez más a mano.** `airflow dags trigger dataops` — esta vez
> un run `manual__`. Cuando tenga éxito, un evento más, un run
> `asset_triggered__` más de `mlops`. Cada éxito del productor es un run del
> consumidor, se haya arrancado el productor como se haya arrancado.

> **Pruébalo, la otra rama.** Pon el listón imposiblemente alto y mira cómo el
> gate coge el otro brazo. El umbral se lee de una variable de entorno dentro
> del proceso de la propia task, así que ponla en la misma shell que ejecuta el
> comando:
>
> ```bash
> docker compose exec airflow-scheduler bash -c 'export TAGIFAI_MIN_F1=0.99; airflow dags test mlops'
> ```
>
> `promotion_gate` loguea `below threshold 0.99 - not promoting`, `promote_model`
> queda skipped, `keep_current_model` corre, y el Dag run termina en success
> igualmente.

¿Atascado en algún punto? [`solutions/04_mlops.py`](solutions/04_mlops.py).

## 11. Esperar: sensors, deferrable tasks, eventos

`dags/05_sensors_and_events.py`. Tres formas de esperar al mundo exterior, la
más barata la última — y, después de ellas, la única forma de no esperar en
absoluto:

```python
# 1. Poking sensor — holds a worker slot the entire time. Seconds, not hours.
FileSensor(task_id="wait", filepath=..., poke_interval=10, timeout=300)

# 2. `reschedule` mode — the task instance exits between pokes and is re-queued.
FileSensor(..., mode="reschedule")

# 3. Deferrable — suspends onto the triggerer and frees the slot entirely.
TimeDeltaSensorAsync(task_id="settle", delta=timedelta(seconds=5))
```

La palabra **trigger** está sobrecargada, así que fíjala antes de que la tercera
opción pueda tener sentido. Tú *disparas* (trigger) un Dag (§7): un verbo. El
proceso triggerer de la §3 ejecuta **triggers**: sustantivos — pequeños trozos de
código asíncrono que esperan a una cosa (un momento en el tiempo, un archivo, un
mensaje) y lanzan un evento cuando ocurre. "Deferrable" significa que la task le
entrega al triggerer uno de esos triggers, sale, y se reanuda cuando llega el
evento. `TriggerDagRunOperator` (§12) vuelve a ser el verbo.

### Constrúyelo

Crea `dags/05_sensors_and_events.py` con la cabecera y los dos sensors de tipo
operator:

```python dags/05_sensors_and_events.py
"""Lesson 5 - waiting for things: sensors, deferrable operators, and events."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensorAsync
from airflow.sdk import PokeReturnValue, dag, task

from tagifai import config, data

DEFAULT_ARGS = {"owner": "data-engineering", "retries": 0}


@dag(
    dag_id="sensors_and_events",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["tutorial", "sensors"],
    doc_md=__doc__,
)
def sensors_and_events():
    settle = TimeDeltaSensorAsync(task_id="settle", delta=timedelta(seconds=5))

    wait_for_curated_data = FileSensor(
        task_id="wait_for_curated_data",
        filepath=str(config.LABELED_PROJECTS_FP),
        poke_interval=10,
        timeout=60 * 5,
        mode="reschedule",
    )
```

`settle` es deferrable: libera su worker slot mientras espera, lo que necesita
`airflow triggerer` corriendo. `wait_for_curated_data` vigila el archivo que
escribe `dataops`, a través de la connection `fs_default` que aporta el archivo
de compose. `mode="reschedule"` es la forma no-deferrable de liberar un slot: la
task instance sale y se vuelve a encolar cada `poke_interval` segundos, y se
rinde pasado `timeout`.

Un sensor también se puede escribir como función. Añádelo, con la task que usa su
valor, y el cableado:

```python dags/05_sensors_and_events.py
    @task.sensor(poke_interval=10, timeout=120, mode="reschedule")
    def enough_rows() -> PokeReturnValue:
        if not config.LABELED_PROJECTS_FP.exists():
            return PokeReturnValue(is_done=False)
        rows = len(data.load_labeled())
        print(f"curated table has {rows} rows")
        return PokeReturnValue(is_done=rows >= 500, xcom_value=rows)

    @task
    def proceed(rows: int) -> None:
        print(f"Data is ready ({rows} rows). Downstream work can start.")

    settle >> wait_for_curated_data >> proceed(enough_rows())


sensors_and_events()
```

`PokeReturnValue(is_done=..., xcom_value=...)` para el poking y a la vez entrega
un valor aguas abajo — `proceed` recibe el recuento de filas. La tabla se lee con
pandas en vez de contar líneas, porque las descripciones contienen saltos de
línea.

No en el archivo, porque necesita una cola que no tienes: la opción que elimina
la espera del todo — **event-driven scheduling**. Engancha un watcher a un asset
y un evento externo arranca el run; nada hace polling, nada ocupa un slot:

```python
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher

trigger = MessageQueueTrigger(queue="https://sqs.us-east-1.amazonaws.com/.../new-projects")
new_projects = Asset(name="new_projects",
                     watchers=[AssetWatcher(name="sqs_watcher", trigger=trigger)])

@dag(schedule=[new_projects])
def ingest_on_message(): ...
```

Solo los triggers que derivan de `BaseEventTrigger` pueden ser watchers — un
trigger que comprueba una condición permanente ("el archivo existe") se
dispararía una y otra vez para siempre.

> Tanto los sensors deferrable como los watchers necesitan `airflow triggerer`
> corriendo. Sin él se quedan en `deferred` indefinidamente.

### Ejecútalo

Este Dag espera el archivo que produjo `dataops`, así que la §9 tiene que haber
corrido o los sensors harán polling hasta agotar su timeout:

```bash
docker compose exec airflow-scheduler airflow dags test sensors_and_events
```

**Comprobación.** Termina en unos segundos, porque todo lo que espera ya ha
ocurrido. En el log:

```
curated table has 764 rows
Poking for file /opt/airflow/data/warehouse/labeled_projects.csv
Success criteria met. Exiting.
Data is ready (764 rows). Downstream work can start.
```

`wait_for_curated_data` hace un poke, encuentra el archivo y sale; `enough_rows`
cuenta las filas y supera su umbral en el primer poke. Y `settle`, el deferrable,
vuelve de inmediato sin diferirse en absoluto: su objetivo de cinco segundos se
mide desde la logical date del run, que `airflow dags test` fijó antes de que la
task arrancara, así que para cuando la task corrió el momento ya había pasado.
Nada de este run tocó el triggerer.

**Para ver una task diferirse de verdad**, manda el run por el scheduler, y luego
lee los estados de las tasks unas cuantas veces a lo largo de los siguientes diez
segundos:

```bash
docker compose exec airflow-scheduler bash -c '
  airflow dags unpause sensors_and_events
  airflow dags trigger sensors_and_events -r defer-demo'   # pick a new id if you rerun

docker compose exec airflow-scheduler airflow tasks states-for-dag-run sensors_and_events defer-demo -o plain
```

**Comprobación.** `settle` marca `deferred` durante unos cinco segundos — su
proceso ha salido y el triggerer sostiene el temporizador — y luego `success`,
con un `end_date` unos cinco segundos y pico después de su `start_date`. Ese
`deferred` es el estado de la §3, y es lo que "liberar el worker slot" parece
visto desde fuera. Sé rápido con el segundo comando; un `docker compose exec`
se lleva uno o dos segundos por su cuenta.

**Ahora mira el caso interesante** — un sensor que sí tiene que esperar. Aparta
el archivo y ejecuta el Dag otra vez:

```bash
# Git Bash needs the MSYS_NO_PATHCONV prefix on any /opt/... path (§2)
MSYS_NO_PATHCONV=1 docker compose exec airflow-scheduler \
    mv /opt/airflow/data/warehouse/labeled_projects.csv /tmp/labeled_projects.csv

docker compose exec airflow-scheduler airflow dags test sensors_and_events
```

Este no vuelve. Ambos sensors están en `mode="reschedule"`, así que sus task
instances **salen entre pokes** en vez de retener un slot, y aterrizan en
`up_for_reschedule` — el estado de la §3 que solo producen los sensors. El run lo
anuncia cada segundo:

```
No tasks to run. unrunnable tasks: {<TaskInstance: sensors_and_events.enough_rows ... [up_for_reschedule]>,
                                    <TaskInstance: sensors_and_events.wait_for_curated_data ... [up_for_reschedule]>}
```

En una **segunda terminal**, devuelve el archivo a su sitio:

```bash
MSYS_NO_PATHCONV=1 docker compose exec airflow-scheduler \
    mv /tmp/labeled_projects.csv /opt/airflow/data/warehouse/labeled_projects.csv
```

El siguiente poke lo encuentra y el run se completa. Déjalo movido y cada sensor
acabará fallando por su `timeout` declarado: 120 segundos para `enough_rows`,
300 para `wait_for_curated_data`, así que el run se acaba a los dos minutos.

Lo importante es lo que *no* estaba pasando durante la espera: la task instance
no estaba corriendo entre pokes — estaba en `up_for_reschedule`, que lo has
visto — así que no se retuvo ningún worker slot. Un poking sensor en el modo por
defecto habría ocupado un slot todo ese tiempo sin hacer nada, que es toda la
razón por la que existen los otros dos modos.

**Regla práctica:** segundos → un poking sensor vale. Minutos u horas →
deferrable, o estás pagando un worker slot para dormir. Un sistema externo capaz
de hacer push → un `AssetWatcher`, y ninguna task espera.

Referencia: [`solutions/05_sensors_and_events.py`](solutions/05_sensors_and_events.py).

## 12. Continual learning: monitorizar, decidir, actuar

**Continual**, no continuo: el sistema se repite *con pausas*, y las pausas son
deliberadas. Un monitor dispara, se decide algo — por una persona donde importa —
y entonces corre el pipeline. El gate de aprobación de abajo es esa coma hecha
ejecutable.

Lo que estás vigilando es el **drift**, en tres sabores:

| Drift | Qué se movió | Aquí lo detecta |
|---|---|---|
| **data drift** | la distribución de entrada — el texto en sí | `mean_length_shift` en `monitoring.drift_report()` |
| **target drift** | la distribución de etiquetas — un tag domina, o aparece uno nuevo | `label_tvd` (total variation distance) y `new_labels` |
| **concept drift** | el propio mapeo — la misma entrada ahora merece otra etiqueta | solo la comprobación de rendimiento en vivo: el f1 cae mientras las entradas parecen iguales |

El concept drift es el que ninguna estadística del lado de la entrada puede ver,
y por eso `monitoring.production_performance()` puntúa filas etiquetadas
recientemente en vez de fiarse solo de una comparación de distribuciones.

Un evento de monitorización tiene cuatro respuestas sensatas, y
`dags/06_monitoring_and_retraining.py` las convierte en branches:

| Respuesta | Cuándo | Qué pasa |
|---|---|---|
| **continue** | drift y rendimiento dentro del presupuesto | nada; seguimos sirviendo |
| **improve** | el f1 bajó o la distribución de entrada se movió | preguntar a una persona, y luego disparar `mlops` |
| **inspect** | apareció una etiqueta no vista | parar y avisar a alguien — el espacio de etiquetas cambió |
| **rollback** | el rendimiento se desplomó | volver a apuntar producción al modelo anterior |

### Constrúyelo

La carga de trabajo: `monitoring.monitor()` devuelve un informe con un dict
`drift` (`label_tvd`, `new_labels`, `current_distribution`, …) y un dict
`performance` (`f1_at_promotion`, `f1_now`) — o `performance=None` cuando no hay
nada promocionado; `registry.rollback()` vuelve a promocionar el modelo
anterior.

Crea `dags/06_monitoring_and_retraining.py`. Los umbrales son **params** del
Dag, así que la política es visible en la UI, se puede sobrescribir por run, y
queda registrada con el run que la usó:

```python dags/06_monitoring_and_retraining.py
"""Lesson 6 - continual learning: monitor, decide, and act."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task

from tagifai import monitoring, registry

DEFAULT_ARGS = {"owner": "ml-engineering", "retries": 1}

POLICY_PARAMS = {
    "max_label_tvd": Param(0.15, type="number", description="Label-drift budget."),
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

Medir, y luego decidir:

```python dags/06_monitoring_and_retraining.py
    @task
    def measure() -> dict[str, Any]:
        report = monitoring.monitor()
        drift = report["drift"]
        print(f"label TVD={drift['label_tvd']:.4f} new_labels={drift['new_labels']}")
        if report["performance"]:
            perf = report["performance"]
            print(f"f1 at promotion={perf['f1_at_promotion']:.4f} now={perf['f1_now']:.4f}")
        return report

    @task.branch
    def decide(report: dict[str, Any], **context) -> str:
        drift, performance = report["drift"], report["performance"]
        policy = context["params"]

        if performance is None:
            print("No production model yet - train one.")
            return "request_retraining"

        f1_drop = performance["f1_at_promotion"] - performance["f1_now"]
        print(f"f1_drop={f1_drop:.4f} label_tvd={drift['label_tvd']:.4f} policy={dict(policy)}")

        if f1_drop >= policy["critical_f1_drop"]:
            return "rollback"
        if drift["new_labels"]:
            return "inspect"
        if f1_drop >= policy["max_f1_drop"] or drift["label_tvd"] >= policy["max_label_tvd"]:
            return "request_retraining"
        return "continue_serving"
```

`context["params"]` es la política — los defaults de arriba, o lo que se le haya
dado a este run. Fíjate en el orden: **una etiqueta nueva enruta a `inspect`, no
a `improve`.** Reentrenar con datos cuyo espacio de etiquetas todavía no has
entendido es la forma de publicar un modelo peor con plena confianza.

Ahora los cuatro brazos. Tres son simples; el cuarto es un gate
**human-in-the-loop**, `ApprovalOperator`, seguido de la task que arranca otro
Dag:

```python dags/06_monitoring_and_retraining.py
    continue_serving = EmptyOperator(task_id="continue_serving")

    @task
    def inspect(report: dict[str, Any]) -> None:
        print("MANUAL INSPECTION NEEDED")
        print(f"  new labels: {report['drift']['new_labels']}")
        print(f"  current distribution: {report['drift']['current_distribution']}")

    @task
    def rollback() -> dict[str, Any] | None:
        pointer = registry.rollback()
        print(f"rolled back to {pointer['run_id']}" if pointer else "nothing to roll back")
        return pointer

    request_retraining = ApprovalOperator(
        task_id="request_retraining",
        subject="Retrain the tag classifier?",
        body=(
            "Monitoring detected drift or a performance drop.\n\n"
            "Approve to trigger the `mlops` Dag on the current data."
        ),
        defaults="Approve",
        response_timeout=timedelta(seconds=60),
    )

    trigger_retraining = TriggerDagRunOperator(
        task_id="trigger_retraining",
        trigger_dag_id="mlops",
        conf={"reason": "monitoring_drift"},
        wait_for_completion=False,
        reset_dag_run=True,
    )
```

`ApprovalOperator` aparca la task en `awaiting_input` y muestra un aviso en la
UI, bajo la pestaña **Required Actions** del Dag. Con **Reject**, todo lo de
aguas abajo queda skipped. `defaults` es a lo que se resuelve un run sin
respuesta pasado `response_timeout` — sesenta segundos aquí para que la lección
termine; dale horas en producción. `TriggerDagRunOperator` es el verbo entre
Dags: crea un run `manual__` de `mlops` con el `conf` dado, y con
`wait_for_completion=False` no se queda a mirarlo.

Termina con el cableado:

```python dags/06_monitoring_and_retraining.py
    report = measure()
    branch = decide(report)

    branch >> continue_serving
    branch >> inspect(report)
    branch >> rollback()
    branch >> request_retraining >> trigger_retraining


monitoring_dag()
```

Cuatro brazos a partir de un branch; `decide` devuelve el `task_id` del que hay
que ejecutar. No hay join al final, porque nada tiene que correr después de
todos ellos.

### Ejecútalo

Forzar a cero el presupuesto de label drift garantiza que `decide()` coja la
rama de reentrenamiento, así que ves el camino interesante en vez del que se
merezcan tus datos actuales:

```bash
docker compose exec airflow-scheduler \
    airflow dags test monitoring --conf '{"max_label_tvd": 0.0}'
```

> **Este comando tarda unos 70 segundos y parece congelado durante 60 de ellos.**
> No es un cuelgue. `request_retraining` es el gate human-in-the-loop: entra en
> `awaiting_input` y agota su `response_timeout` completo de 60 segundos antes
> de resolverse por defecto a Approve, porque nadie va a pulsar el botón. Déjalo
> estar.

**Comprobación.** Por orden, con aproximadamente un minuto de diferencia:

```
Branch into request_retraining
Waiting for response                         ← el estado pasa a awaiting_input
Approved. Proceeding with downstream tasks…  ← 60s después, resuelve el default
Dag Run triggered successfully.  trigger_dag_id=mlops
```

Esa última línea es real. Compruébalo tú mismo — `airflow dags list-runs mlops -o
plain` tiene ahora un run `manual__…` recién creado por este Dag. Un Dag decidió
que otro Dag debía correr, y a una persona se le dio la oportunidad de decir que
no.

Mientras está en `awaiting_input`, abre el Dag en la UI: el aviso te espera en la
pestaña **Required Actions** (§5), y responderlo ahí resuelve la task sin esperar
al timeout. Airflow registra la respuesta en cualquiera de los dos casos — la
decisión que se tomó, cuándo, y *quién*. Un run que expiró hacia sus `defaults`
guarda la decisión con el "quién" vacío, que es cómo distingues "una persona
aprobó esto" de "no había nadie mirando".

> **Sin el override de `--conf`** el desenlace depende del estado, que es el
> sentido de un monitor. Si la §10 promocionó un modelo y los datos no se han
> movido, `decide()` devuelve `continue_serving`, el brazo de reentrenamiento
> queda skipped, y el run termina en segundos. Si todavía no hay modelo en
> producción, `decide()` lo dice — `No production model yet - train one.` — y
> coge la rama de reentrenamiento igualmente, timeout incluido. Ambas son
> correctas. Ejecútalo para ver una rama *no* tomada.

Referencia: [`solutions/06_monitoring_and_retraining.py`](solutions/06_monitoring_and_retraining.py).

### Lo que "reentrenar" deja fuera

`trigger_retraining` dispara `mlops`, que ajusta sobre lo que haya en el
warehouse. Eso es la orquestación; no es el problema completo del
reentrenamiento. Cada uno de estos es un hueco de task en una versión real de
este Dag:

| Paso | Por qué no es automático |
|---|---|
| **labeling** | las filas nuevas necesitan etiquetas reales, no las predicciones del propio modelo realimentadas como verdad |
| **active learning** | etiqueta las filas que más enseñarían al modelo — rara vez hay presupuesto para todas |
| **QA** | audita las etiquetas antes de entrenar con ellas: falsos positivos, desacuerdo entre etiquetadores, slices débiles |
| **augmentation** | amplía los datos nuevos para que una ventana pequeña no quede ahogada por el histórico |
| **sampling** | sube/baja muestreo para arreglar el desbalanceo que introdujo el drift |
| **evaluation** | construye el conjunto held-out para que refleje las condiciones de *despliegue* |

La otra restricción es el **catastrophic forgetting**: ajustar solo sobre la ventana
nueva cambia el modo de fallo de ayer por el de mañana. El Dag de aquí lo esquiva
entrenando siempre con la tabla etiquetada completa — vale a este tamaño, y es lo
primero que revisar cuando la tabla deje de caber en una task.

Que es también por lo que el promotion gate de la §10 compara contra el modelo en
producción actual en vez de solo contra un umbral fijo: un modelo reentrenado que
es peor que el que está sirviendo no debería publicarse solo porque disparó un
monitor.

## 13. Deadline alerts

Un **deadline** responde a "avísame si este run está tardando demasiado". Se
adjunta al Dag, se aplica al run entero, y se evalúa *mientras el run sigue en
marcha* — así que te enteras de un pipeline atascado durante el incidente, no en
el informe de la mañana.

### Constrúyelo

`dags/07_deadline_alerts.py` es el archivo más corto de los siete, y todo él es
el decorador:

```python dags/07_deadline_alerts.py
"""Lesson 7 - deadline alerts: "tell me if this run is taking too long"."""

from __future__ import annotations

import time
from datetime import timedelta

import pendulum
from airflow.sdk import DeadlineAlert, DeadlineReference, SyncCallback, dag, task


@dag(
    dag_id="deadline_alerts",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "ml-engineering"},
    tags=["tutorial", "monitoring"],
    doc_md=__doc__,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(seconds=30),
        callback=SyncCallback(
            "tagifai.callbacks.deadline_missed",
            kwargs={"dag_id": "deadline_alerts", "budget": "30s"},
        ),
    ),
)
def deadline_alerts():
```

Tres piezas: una **reference** (dónde arranca el reloj), un **interval** (cuánto
después) y un **callback** — nombrado como ruta con puntos, porque corre en
algún sitio que no es la task, con `kwargs` que se le pasan. Pasa una lista a
`deadline=` para varios presupuestos.

References: `DAGRUN_QUEUED_AT`, `DAGRUN_LOGICAL_DATE`, `FIXED_DATETIME(dt)`,
`AVERAGE_RUNTIME(max_runs=10)` — esta última es "más lento que la propia media
reciente de este Dag", que suele ser lo que de verdad quieres decir. Callbacks:
`SyncCallback` (corre en el executor) o `AsyncCallback` (corre en el triggerer —
úsalo para Notifiers, p. ej. `AsyncCallback(SlackWebhookNotifier, kwargs={...})`).

El Dag se pasa su propio deadline a propósito:

```python dags/07_deadline_alerts.py
    @task
    def slow_step() -> str:
        time.sleep(75)
        return "finished, but late"

    slow_step()


deadline_alerts()
```

### Ejecútalo

```bash
# unpause first: the 30-second clock starts when the run is queued, i.e. at the trigger
docker compose exec airflow-scheduler airflow dags unpause deadline_alerts
docker compose exec airflow-scheduler airflow dags trigger deadline_alerts

# give it 40 seconds, then:
docker compose logs airflow-scheduler | grep "DEADLINE MISSED"
```

**Comprobación.** Una línea `DEADLINE MISSED`, unos 30 segundos después del
trigger, con el contexto del run — y `slow_step` sigue corriendo cuando dispara,
porque esa task duerme 75 segundos. Ese es el objetivo: te enteraste del run
lento antes de que terminara. Los deadlines perdidos y pendientes también se
listan bajo Browse → **Deadlines**.

> **Problema conocido en 3.3.1:** `airflow dags test` revienta con cualquier Dag
> que tenga un `deadline=` (`psycopg2.ProgrammingError: can't adapt type 'dict'`
> saliendo de `DagRun.update_state`). Las tasks corren bien; lo único que falla
> es la actualización final de estado de la CLI. Dispara esos Dags por el
> scheduler en su lugar — que es por lo que `mlops` no lleva deadline, ya que
> rompería la instrucción `airflow dags test mlops` de más arriba.
> [Detalles.](docs/troubleshooting.md)

Referencia: [`solutions/07_deadline_alerts.py`](solutions/07_deadline_alerts.py).

## 14. Testear Dags

Cuatro niveles, el más barato primero — y el primero de ellos es el corrector de
los siete archivos que escribiste.

**Tests de integridad de Dags** — [`tests/test_dag_integrity.py`](tests/test_dag_integrity.py).
Estos pillan los fallos que si no aparecen como un banner rojo en la UI horas más
tarde, y son lo de mayor valor que puedes meter en CI:

```python
from airflow.dag_processing.dagbag import DagBag

def test_no_import_errors(dagbag):
    assert not dagbag.import_errors

def test_mlops_is_scheduled_on_the_labeled_projects_asset(dagbag):
    asset_names = {a.name for a in dagbag.dags["mlops"].timetable.asset_condition.objects}
    assert "labeled_projects" in asset_names
```

Cargan **tu carpeta `dags/`**, y codifican lo que este tutorial quería decir:
siete `dag_id` concretos; todos los Dags con tags, un `doc_md` y un owner que no
sea `airflow`; `retries` de al menos uno en todo Dag que haga trabajo de verdad;
`dataops` produciendo ambos assets; `mlops` programado con `labeled_projects` y
validando antes de entrenar; sin ciclos. También hace grep en el código buscando
construcciones que no existen en esta versión de Airflow — `schedule_interval=`,
`days_ago(`, `airflow.operators.`, `execution_date`, `SubDagOperator`, `sla=`.
La mayoría del código de Airflow que hay en internet es anterior a Airflow 3, así
que un snippet copiado y pegado puede colar una de estas, y varias de ellas
incluso *importan* y solo fallan cuando corre la task.

**Tests unitarios sobre la carga de trabajo** — [`tests/test_tagifai.py`](tests/test_tagifai.py).
Nada en `src/tagifai/` importa Airflow, así que el código de ML es testeable sin
un scheduler de por medio. Esa separación es deliberada: Airflow orquesta el
código, no lo contiene.

**Tests de run completo** — `airflow dags test <dag_id>` ejecuta un Dag run real
in-process, y `airflow tasks test <dag_id> <task_id>` ejecuta una task sin
escribir estado.

**Tests sobre la documentación** — dos, porque un tutorial que cita código solo
es útil mientras las citas son verdad.
[`tests/test_readme_listings.py`](tests/test_readme_listings.py): el
[apéndice](docs/appendix-dags_ES.md) muestra cada solución completa, inyectada
desde `solutions/` con [`scripts/sync_readme.py`](scripts/sync_readme.py) en vez
de copiada a mano; el test vuelve a ejecutar la inyección en memoria y falla si
el resultado difiere.
[`tests/test_readme_build_steps.py`](tests/test_readme_build_steps.py): cada
bloque de "Constrúyelo" de arriba lleva la etiqueta del archivo al que
pertenece; el test los concatena en orden, carga el resultado con `DagBag`, y
falla si lo que se te dijo que teclearas no suma un Dag que importe.

Ejecútalo todo:

```bash
docker compose run --rm --entrypoint pytest airflow-cli /opt/airflow/tests -q
```

`airflow-cli` no es uno de los cinco servicios que lista `docker compose ps`: es
un contenedor de usar y tirar que el archivo de compose define bajo un profile,
que `run` crea para ese comando y que `--rm` elimina después.

**Comprobación.** Una fila de puntos y `36 passed`, en bastante menos de
un minuto:

```
....................................                                     [100%]
36 passed in 7.51s
```

No intervino ningún scheduler y no corrió ningún Dag. Ese es el objetivo: cada
uno de esos 36 es lo bastante rápido para correr en cada commit, que es
lo que hace que merezca la pena tenerlos.

Cuando uno falla, el mensaje dice qué archivo y qué falta — `mlops has no
retries`, `Dags not yet written in /opt/airflow/dags: ['monitoring']`, a
`dataops` le falta un outlet — y el arreglo está en la lección que construyó ese
archivo, o en `solutions/`. Para ver la suite pasar contra las versiones
terminadas, apúntala ahí:

```bash
docker compose run --rm -e DAGS_DIR=solutions --entrypoint pytest airflow-cli /opt/airflow/tests -q
```

> En **Git Bash**, antepón `MSYS_NO_PATHCONV=1` a ambos comandos o el argumento
> `/opt/airflow/tests` se reescribe a una ruta de Windows y pytest no recoge
> nada.

```bash
python scripts/sync_readme.py --check     # exit 1 when an appendix listing is stale
python scripts/sync_readme.py             # fix it
```

Ninguno de esos niveles habría pillado los dos bugs del recuadro de la "trampa"
de la §7: `airflow dags test` siempre aporta una logical date, así que un Dag que
se rompe con un trigger manual pasa todos los tests de arriba. **Dispara cada Dag
una vez de verdad** — por el scheduler, como va a correr realmente — antes de
creértelo.

## 15. Llevarlo a producción

**Executors.** `LocalExecutor` es el de por defecto y vale hasta una máquina. Más
allá: `CeleryExecutor` (pool de workers), `KubernetesExecutor` (un pod por task),
o **varios executors** configurados a la vez. También existe el **Edge
executor**, para workers fuera de tu clúster — lo que hizo posible la Task
Execution API (§3).

**Aislamiento.** No instales tus dependencias de ML junto a las de Airflow. Usa
`KubernetesPodOperator`, o `@task.virtualenv` / `@task.external_python` para un
aislamiento más ligero. El cómputo pesado pertenece a un clúster (Ray, Spark,
EMR, Dataproc) con Airflow disparándolo y siguiéndolo — Airflow es un
orquestador, no un motor de cómputo.

**Dag bundles.** `dags_folder` sigue funcionando, pero el Dag processor lee
*bundles*; el de por defecto es un `LocalDagBundle` llamado `dags-folder`.
Cambiar a `GitDagBundle` te da versionado: cada Dag run registra el commit con el
que corrió, así que un re-run usa el mismo código.

```ini
[dag_processor]
dag_bundle_config_list = [
    {"name": "team-ml",
     "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
     "kwargs": {"git_conn_id": "git_default", "subdir": "dags", "tracking_ref": "main"}}
  ]
```

**Intervalos de parseo.** Este stack escanea archivos cada 5 segundos para que
lo que guardas aparezca al momento. Devuelve los valores por defecto
(`refresh_interval` 300, `min_file_process_interval` 30) en cualquier cosa con
cientos de archivos de Dag — el trabajo del processor es parsear, no hacer
polling.

**Secretos.** Nunca en el código del Dag, nunca en los `kwargs` de un bundle,
nunca en la `uri` o el `extra` de un asset — los extras de asset se guardan en
texto plano. Usa Connections, Variables o un secrets backend.

**Auth.** Cambia el Simple auth manager por FAB o por un proveedor de identidad
real antes de que nadie más pueda llegar a la UI.

**Observabilidad.** Airflow emite sus propias métricas, y esta es la parte que la
gente se salta hasta que hay un incidente. Tres capas:

- **Metrics** — `[metrics] statsd_on = True` para StatsD/Datadog, o
  `[metrics] otel_on = True` para enviar OpenTelemetry a un collector. Vigila
  `scheduler.tasks.starving`, `executor.open_slots`, `dagrun.duration.failed` y
  `dag_processing.import_errors`. Los nombres de métrica llevan `dag_id`/`task_id`
  como **tags**; `[metrics] legacy_names_on` (por defecto `True`) también exporta
  la forma antigua con el nombre como sufijo.
- **Traces** — `[traces] otel_on = True` envía spans de los bucles del scheduler
  y de los runs de task, que es como encuentras *qué* task de un Dag lento se
  volvió más lenta.
- **Health** — `GET /api/v2/monitor/health` informa del estado del scheduler, el
  triggerer y el Dag processor; la página Home de la UI muestra lo mismo.

Y luego la higiene de siempre: `airflow db clean` programado (si no, la metadata
DB crece para siempre), deadline alerts en los Dags que importan (§13), y
Notifiers — `SlackWebhookNotifier`, `SmtpNotifier` — cableados a
`on_failure_callback` o a un `AsyncCallback`.

## 16. Apéndice y procedencia

Las lecciones de arriba construyen cada Dag por partes. Dos documentos más
largos viven fuera de este archivo, para que el tutorial siga siendo legible y
el material de referencia siga siendo localizable:

- **[Los Dags terminados, completos](docs/appendix-dags_ES.md)** — cada solución
  de principio a fin, en orden de lectura, inyectada desde
  [`solutions/`](solutions/) por
  [`scripts/sync_readme.py`](scripts/sync_readme.py) y verificada por
  [`tests/test_readme_listings.py`](tests/test_readme_listings.py), de modo que
  los listados no pueden desviarse del código que se ejecuta de verdad.
- **[Qué se verificó de verdad](docs/verified_ES.md)** — el registro de qué
  afirmaciones de este tutorial se leyeron de un stack `apache/airflow:3.3.1`
  vivo, cuándo, y cómo se midió cada cifra. Nada de eso es un paso que ejecutar.

## Estructura del repositorio

```
dags/                       your workspace - empty until you write the seven Dags
solutions/                  the finished version of each, numbered in reading order
src/tagifai/                the ML project Airflow orchestrates (imports no Airflow)
  config.py                 paths and thresholds
  data.py                   extract / clean / transform / split
  validate.py               a tiny expectation-suite runner
  train.py  evaluate.py     TF-IDF + SGD classifier, metrics and slices
  registry.py               filesystem stand-in for a model registry
  monitoring.py             drift and live-performance checks
  predict.py                inference against the promoted model
  assets.py                 the Airflow Assets the Dags share
  callbacks.py              deadline-alert callbacks
tests/                      Dag integrity (your dags/), unit tests, README checks
docs/                       cheatsheet, troubleshooting, the Dag appendix (§16)
scripts/                    up / reset / native setup / README sync
docker-compose.yaml         LocalExecutor + Postgres + Simple auth
```

## Para seguir leyendo

- [Documentación de Airflow 3](https://airflow.apache.org/docs/apache-airflow/stable/)
- [Referencia del Task SDK](https://airflow.apache.org/docs/task-sdk/stable/api.html)
- [Assets y scheduling data-aware](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html)
- [Índice de paquetes de providers](https://airflow.apache.org/docs/apache-airflow-providers/)
- El pipeline de ML que orquesta este tutorial sigue la [lección de orquestación
  de Made With ML](https://madewithml.com/courses/mlops/orchestration/) — útil
  por el razonamiento de MLOps; su código de Airflow es de una versión anterior.
