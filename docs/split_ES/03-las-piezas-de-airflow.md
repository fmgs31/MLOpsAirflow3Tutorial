## 3. Las piezas de Airflow

Airflow es un scheduler de workflows batch escritos en Python. Tú describes el
grafo en código, y Airflow decide cuándo corre cada nodo, lo reintenta, registra
el intento y te enseña el resultado. No mueve tus datos y no entrena tu modelo —
arranca lo que sí lo hace, espera y reacciona.

Eso también te dice cuándo usar otra cosa. Cada Dag run y cada task se
planifica, se escribe en la metadata database y se arranca como un proceso, y
eso cuesta unos segundos por task. No es nada para trabajo que se mide en
minutos, y es demasiado lento para trabajo que debe terminar en milisegundos.
Airflow puede *arrancar* un run cuando llega un evento (§11), pero un run por
cada mensaje, clic o petición, a miles por segundo, saturaría el scheduler y la
metadata database; para eso están un consumidor de colas o un procesador de streams
(Kafka, Flink…). Airflow también encaja mal con cualquier cosa que necesite un
ciclo o un bucle `while` entre runs, y con cómputo pesado dentro del worker —
lanza un job de Spark/Ray/Kubernetes y haz seguimiento de él. Si tu pipeline son
tres scripts que corren una vez por semana, cron sigue siendo una opción válida.

Cuatro procesos, la metadata database y un executor, que no siempre es un
proceso propio ("El executor", más abajo). Todos están corriendo ahora en tu
stack:

| Componente | Función |
|---|---|
| **API server** | Sirve la REST API, la UI de React y la **Task Execution API** que llaman los workers |
| **Scheduler** | Decide qué ejecutar y entrega el trabajo al executor |
| **Dag processor** | Parsea tus archivos de Dag a la metadata database — su propio proceso |
| **Triggerer** | Ejecuta triggers async para tasks deferred (§11) y callbacks async |
| **Executor** | Donde corren de verdad las tasks. `LocalExecutor` aquí; Celery o Kubernetes en producción |
| **Metadata DB** | El estado de todo: runs, task instances, XComs, connections |

El scheduler, el executor y la metadata database deciden casi todo sobre cómo
se comportan tus Dags, así que cada uno tiene su propio apartado.

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
que la task registró. Con una pausa de un segundo entre bucles, ese retraso es
como mucho un segundo. Lo que *sí* vas a notar es lo que tardan las tasks de
aguas arriba, que no tiene nada que ver con el scheduler.

El parseo sigue su propio ritmo, y es del que vas a depender enseguida. El Dag
processor vuelve a parsear cada archivo, haya cambiado o no, cada
`[dag_processor] min_file_process_interval` segundos, y vuelve a escanear la carpeta
buscando archivos nuevos cada `refresh_interval`. Este stack pone ambos a **5**, así que un
archivo que guardas aparece en la UI en unos segundos. Los valores por defecto
son 30 y 300: en un stack que los conserve, un archivo recién creado puede
tardar cinco minutos en aparecer, lo que parece un Dag roto si no sabes que
ese ajuste existe.

Los tres números son ajustes de configuración, y puedes imprimir los tuyos:

```bash
docker compose exec airflow-scheduler airflow config get-value scheduler scheduler_idle_sleep_time
docker compose exec airflow-scheduler airflow config get-value dag_processor min_file_process_interval
docker compose exec airflow-scheduler airflow config get-value dag_processor refresh_interval
```

### El executor, y qué significa "a la vez"

El executor responde a "¿dónde corre el Python de esta task?". El bucle del
scheduler no ejecuta tu código; solo decide *que* ese código debe ejecutarse y
entrega la task al executor, que la arranca en algún sitio e informa del
resultado.

`LocalExecutor` — el de este stack — arranca cada task como **su propio
subproceso en la máquina que corre el scheduler**, usando `multiprocessing` de
Python. Paralelismo real, limitado a una máquina. `CeleryExecutor` en cambio
deja la task en una cola para un conjunto de máquinas worker permanentes, y
`KubernetesExecutor` le pide a Kubernetes un pod nuevo por task. Cambiar entre
ellos no cambia ni una línea del Dag: ese es el mecanismo detrás del "cambias
el executor, no el Dag" de la §1.

Por eso `docker compose ps` no muestra ningún contenedor de executor.
`LocalExecutor` es un *componente del proceso del scheduler*, no un servicio: el
scheduler lo importa y desde ahí crea procesos hijo con `fork`. Cambia
`AIRFLOW__CORE__EXECUTOR` a `CeleryExecutor` y los workers pasan a ser máquinas
reales que despliegas y escalas por separado. El executor es lo que separa "un
portátil" de "un clúster", y es un valor de configuración.

Así que "worker" significa algo ligeramente distinto según el executor: aquí un
subproceso creado con `fork`, allí un proceso worker de Celery, allá un pod en Kubernetes.
Lo constante es que el worker recibe la task, la ejecuta y responde por la Task
Execution API.

Cuántas corren a la vez lo decide el **más restrictivo** de varios límites. Estos
cuatro son los de tasks que usa este tutorial, todos con su valor por defecto en
este stack (hay más: `max_active_tis_per_dagrun` en una task, `max_active_tasks`
en un Dag, y `max_active_runs`, que limita runs en vez de tasks; este último lo
usan la §9 y la §10):

| Límite | Valor aquí | Alcance |
|---|---|---|
| `[core] parallelism` | 32 | task instances corriendo en toda la instalación |
| `[core] max_active_tasks_per_dag` | 16 | task instances corriendo en un Dag |
| slots de `default_pool` | 128 | tasks no asignadas a otro pool |
| `max_active_tis_per_dag` en una task | sin fijar | esa task concreta, en todos sus runs |

Para ver el primero y el tercero en este stack:

```bash
docker compose exec airflow-scheduler airflow config get-value core parallelism
docker compose exec airflow-scheduler airflow pools list
```

Un **pool** es un conjunto con nombre de slots al que asignas tasks con `pool="…"`
(Admin → Pools), y es la forma de evitar que veinte tasks concurrentes abran
veinte conexiones a una base de datos que tolera cinco. Todo cae en
`default_pool` salvo que digas otra cosa. La §10 fija el cuarto límite en una
sola task (`max_active_tis_per_dag=2`) y verás el efecto: cuatro tasks de
búsqueda de hiperparámetros que corren de dos en dos en vez de todas a la vez.

### La metadata database

Todo lo que Airflow sabe vive en una base de datos relacional: las
definiciones de Dag tal y como se parsearon, cada Dag run, cada task instance con
su estado y su número de intentos, XComs, Connections, Variables, pools, asset
events, el audit log. En este stack es el contenedor `postgres` (Postgres 16).
Es la base de datos propia de Airflow, no una de las que usan tus tasks:
cuando una task lee el warehouse de BigQuery (§9), o el Postgres de una
aplicación, esa es otra base de datos, a la que llega con una Connection, un
hook o el cliente propio de la herramienta (§6). El mismo contenedor de
Postgres guarda también una segunda base de datos, `optuna`, para la §10;
convive con la de Airflow, pero no forma parte de ella.

La UI es una vista de la metadata database, y el bucle del scheduler es una
consulta contra ella. Reinicia los contenedores y no se pierde nada, porque el
estado está en el volumen de la metadata database, no en los contenedores;
`scripts/reset.sh` borra ese volumen, y por eso es el último recurso. Así que
la metadata database es lo que hay que incluir en las copias de seguridad, y
crece indefinidamente salvo que ejecutes `airflow db clean` (§16): un año de
runs, task instances y XComs no se borra solo.

### El ciclo de vida de una task

El vocabulario es preciso, y si mezclas los términos, los mensajes de error se
vuelven confusos. Un **Dag** es el grafo que escribiste. Un **Dag run** es una
ejecución de él, identificada por un `run_id`, y está `queued`, `running`,
`success` o `failed` — esos cuatro, ninguno más. Una **task instance** es una
task dentro de un Dag run, y tiene muchos más estados, porque le pueden pasar más
cosas:

```
                                                  ┌─► success              terminada
sin estado ──► scheduled ──► queued ──► running ──┼─► failed               terminada — sin reintentos
                    ▲                             │
                    │                             ├─► up_for_retry         esperando retry_delay
                    │                             ├─► up_for_reschedule    un sensor entre pokes
                    │                             ├─► deferred             aparcada en el triggerer
                    │                             └─► awaiting_input       aparcada hasta que responda alguien
                    └─────────────────────────────────┘                    (o hasta el timeout)
                       esas cuatro son "aparcada, volverá"

  nunca arrancó: skipped (un branch eligió otra cosa) · upstream_failed
```

La primera distinción en la que tropieza todo el mundo es **`queued` vs `running`**:

- **sin estado** — al crear un Dag run, el scheduler crea de una vez una task
  instance por cada task del Dag, todas sin estado: en la metadata database su
  `state` es `null`, y la UI la muestra como "no status", una casilla sin color.
  Una task sigue así mientras sus tasks aguas arriba no han terminado. Así se ve
  "esperar a otra task": no hay ningún estado con ese nombre.
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

**Una task solo espera a otra antes de arrancar.** Si `b` depende de `a`, `b` no
tiene estado mientras `a` corre. Cuando `a` termina, el scheduler comprueba la
trigger rule de `b` (por defecto: que todas las tasks aguas arriba hayan tenido
éxito) y, si se cumple, pasa `b` a `scheduled`. Una vez que `b`
está `running`, no puede pararse a mitad de su código para esperar algo de `a` —
las tasks no se hablan mientras corren. Lo que `b` necesite de `a`, un XCom (§6)
o un archivo, tiene que existir antes de que `b` arranque. Si dos pasos tienen
que coordinarse a mitad de camino, son dos tasks con una dependencia entre
ellas.

El resto los irás conociendo sobre la marcha: `up_for_retry` entre intentos (§4),
`skipped` desde un branch (§10), `up_for_reschedule` desde un sensor que libera
su slot entre pokes y `deferred` desde uno suspendido en el triggerer (§11),
`awaiting_input` mientras decide una persona (§10), y `upstream_failed` en todo
lo que hay aguas abajo de un fallo.

### El código de una task nunca toca la metadata database

Una task habla con la Task Execution API por HTTP, nunca con la metadata
database. Eso es lo que hace posibles los workers remotos y el Edge executor, y
la razón por la que una task no puede abrir una sesión contra la metadata database y
consultar `DagRun` por su cuenta.

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
