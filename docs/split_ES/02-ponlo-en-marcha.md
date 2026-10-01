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
no carga, salta a [troubleshooting](../troubleshooting.md).

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

Te pregunta antes de borrar, y `up.sh` / `up.ps1` vuelve a levantar el stack. No
toca `dags/` — los archivos que escribas sobreviven a un reset. Ningún paso de
este tutorial lo necesita; está aquí para el día en que algo se atasque y
[troubleshooting](../troubleshooting.md) no haya ayudado.

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
dags_folder = <repo>/dags            AIRFLOW__CORE__DAGS_FOLDER=<repo>/dags

[dag_processor]
refresh_interval = 5                 AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=5
```

Las dos son líneas que exporta el script de arriba: configura Airflow a través
del entorno en vez de editar `airflow.cfg`. El stack de Docker usa el mismo
mecanismo en [`docker-compose.yaml`](../../docker-compose.yaml).

> **Las connections por defecto no se crean solas.** `fs_default` se añade a mano
> arriba porque `FileSensor` (§11) la necesita. En Docker el archivo de compose
> la aporta como `AIRFLOW_CONN_FS_DEFAULT`.

Y después, o bien el proceso todo-en-uno:

```bash
airflow standalone
```

…o los componentes por separado, que es a lo que se parece un deployment real:

```bash
airflow api-server --port 8080
airflow scheduler
airflow dag-processor            # a separate process — required
airflow triggerer                # deferrable tasks and async callbacks
```

---

← [1. Qué vas a construir](01-que-vas-a-construir.md) · [Índice](README.md) · [3. Las piezas de Airflow](03-las-piezas-de-airflow.md) →
