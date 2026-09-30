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

El parseo se hace con otro reloj distinto, y es del que vas a depender enseguida. El Dag
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

De ahí salen dos consecuencias. Es lo que require un backup, y crece para siempre
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

---

← [2. Ponlo en marcha](02-ponlo-en-marcha.md) · [Índice](README.md) · [4. Tu primer Dag](04-tu-primer-dag.md) →
