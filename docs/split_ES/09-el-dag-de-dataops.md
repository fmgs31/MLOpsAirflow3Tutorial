## 9. El Dag de DataOps

`dags/03_dataops.py` carga los datos sin procesar (projects y tags) en el warehouse, los comprueba,
los convierte en la tabla de entrenamiento y comprueba también el resultado.
Cada paso lo hace una herramienta pensada para él; el Dag decide cuándo se
ejecuta cada una y detiene el pipeline cuando falla una comprobación.

| Task | Herramienta | Qué hace |
|---|---|---|
| `extract_and_load_projects`, `extract_and_load_tags` | **Airbyte** | copia cada CSV de origen a una tabla del warehouse |
| — | **BigQuery** | el warehouse: guarda las tablas y ejecuta SQL sobre ellas |
| `validate_projects`, `validate_tags`, `validate_transformed` | **Great Expectations** | comprueba una tabla contra una lista de reglas |
| `transform` | **dbt** | construye la tabla de entrenamiento a partir de las dos tablas sin procesar (`projects` y `tags`, tal como las carga Airbyte), en SQL |

Cómo se hablan las piezas cuando se ejecuta el Dag. Cada columna es un
sistema, y lo que está escrito en una columna ocurre en ese sistema; una flecha
es un sistema llamando a otro, o datos que pasan de uno a otro:

```
 Airflow (Docker)              │ Airbyte (abctl)       │ BigQuery (Google Cloud)
 Dag dataops                   │ localhost:8000        │ dataset mlops_course
───────────────────────────────┼───────────────────────┼───────────────────────────
                               │                       │
 1 extract_and_load_projects ───► sync projects        │
   extract_and_load_tags ───────► sync tags            │
                               │  cada sync lee su CSV │
                               │  de GitHub y lo ───────► tablas projects, tags
                               │  escribe              │
                               │                       │
 2 validate_projects (GX) ◄────────────────────────────── filas de projects
   validate_tags (GX) ◄────────────────────────────────── filas de tags
                               │                       │
 3 transform: dbt build ────────────────────────────────► ejecuta el SQL del model:
   dbt corre aquí, en el       │                       │  un JOIN que escribe la
   contenedor de Airflow       │                       │  tabla labeled_projects
   luego manda los tests ───────────────────────────────► ejecuta los tests
                               │                       │
 4 validate_transformed (GX) ◄─────────────────────────── filas de labeled_projects
   │ evento del Asset (§8)     │                       │
   ▼                           │                       │
 Dag mlops (§10)               │                       │
```

Airflow arranca cada paso y espera a que termine. Solo las tasks de GX traen
datos a Airflow:

1. Airflow le pide a la API de Airbyte que ejecute un sync. Airbyte descarga el
   CSV y lo escribe en BigQuery; las filas no pasan por Airflow.
2. Cada task de GX lee una tabla de BigQuery en el contenedor de Airflow, como
   un DataFrame de pandas, y la comprueba ahí.
3. dbt envuelve tu `SELECT` en un `CREATE TABLE … AS` y se lo manda a
   BigQuery, que une sus propias dos tablas y escribe la tercera. Después dbt
   manda los tests del proyecto, también como SQL. Ese proyecto de dbt lo creas
   más abajo.
4. La misma comprobación que en el paso 2, sobre la tabla nueva. Si pasa,
   Airflow registra el evento del Asset que arranca `mlops`.

GX y dbt están en la columna de Airflow, y Airbyte tiene la suya, porque son
tipos de software distintos:

- **Airbyte** es un servidor, con su propia API. Airflow no lo ejecuta: le
  pide que haga el trabajo.
- **GX Core** es una librería de Python, instalada en la imagen de Airflow como
  pandas. La task la importa y valida dentro de su propio proceso.
- **dbt Core** es un programa de línea de comandos, también instalado en la
  imagen de Airflow. La task lo lanza como un comando, y el trabajo pesado lo
  hace BigQuery.

GX y dbt tienen también versiones de pago, alojadas, GX Cloud y dbt Cloud, que
sí son servicios. Con ellas, la task llamaría a una API, como hace con Airbyte
(ver «Alternativas» al final de esta sección).

Cargar primero los datos sin procesar y transformarlos dentro del warehouse se
llama **ELT** (extract, load, transform). Como se conservan las tablas sin
procesar, una transformación con un bug se corrige y se vuelve a ejecutar sin
extraer de nuevo.

Es también el primer Dag del tutorial con un cron real, `schedule="0 3 * * *"`
(§7). En cuanto le quites la pausa en la §10 se ejecutará solo: una vez
inmediatamente, para la última franja que su cron ya ha dejado atrás, y luego a
las 03:00 UTC cada día, y cada run disparará `mlops`. (`monitoring`, §12, corre
a las 06:00.) Los runs `scheduled__` que encuentres tras dejar el stack
levantado toda la noche son estos, no un bug.

### Monta el stack de datos

Las herramientas necesitan cuentas, credenciales y algo de configuración antes
de que un Dag pueda llamarlas. Esto se hace una vez, en orden: cada paso
comprueba el anterior.

#### 1. Un proyecto de Google Cloud con BigQuery

1. **Date de alta en Google Cloud** en <https://cloud.google.com>. Las cuentas
   nuevas reciben el Free Trial: un crédito de 90 días. Pide una tarjeta, pero
   no te cobra nada salvo que tú mismo pases a una cuenta de pago. Tras el
   periodo de prueba, el free tier sigue cubriendo 10 GB de almacenamiento y 1 TB de consultas al
   mes; las tablas de este tutorial ocupan menos de 1 MB.

   BigQuery tiene también un *sandbox* que no pide tarjeta, pero no permite
   escribir filas en tablas existentes (streaming inserts, y los `INSERT`,
   `UPDATE` y `MERGE` de SQL), que es como Airbyte carga los datos. Así que el
   proyecto necesita una cuenta de facturación: la del Free Trial o la tuya.

2. **Crea un proyecto.** En el selector de proyectos de la consola, arriba de
   la página, elige **New project** y llámalo `mlops-tutorial`. Google añade un
   sufijo para que el **Project ID** sea único, por ejemplo
   `mlops-tutorial-123456`. Todas las herramientas piden ese ID, no el nombre.

3. **Crea una service account**: una cuenta para programas, no para personas.
   Así las herramientas no usan tu propia cuenta de Google. Ve a **IAM & Admin →
   Service Accounts → Create service account**, llámala `airflow-tutorial` y
   dale dos roles: **BigQuery Data Editor** (crear datasets y tablas, escribir
   filas) y **BigQuery Job User** (ejecutar consultas).

4. **Descarga su clave.** En **IAM & Admin → Service Accounts**, haz clic en
   el email de la service account,
   `airflow-tutorial@<project id>.iam.gserviceaccount.com`, para abrirla.
   Luego abre su pestaña **Keys** y elige **Add key → Create new key →
   JSON**. El navegador descarga un archivo. Muévelo a este repo como
   `secrets/gcp-key.json`. Git ignora todo lo que hay en `secrets/`
   salvo `.gitkeep`, así que no puedes hacer commit de la clave por accidente.

   > **Ese archivo es una contraseña.** Quien lo tenga puede leer y escribir
   > tus datos de BigQuery y lanzar consultas que se facturan a tu proyecto. No
   > lo pegues en chats ni lo subas a ningún sitio. Si se filtra, borra la
   > clave en **Keys** y crea otra.

5. **Indica al stack cuál es tu proyecto.** Pon tu Project ID en
   `GCP_PROJECT_ID`, en `.env`. `BQ_LOCATION` indica dónde guarda BigQuery los datos: `US` (el valor
   por defecto), `EU`, o una región concreta cerca de ti, como
   `australia-southeast1` ([la lista completa](https://cloud.google.com/bigquery/docs/locations)).
   Los datos de este tutorial son tan pequeños que cualquiera sirve. Elijas lo
   que elijas, la destination de Airbyte que creas en «2. Airbyte» necesita el
   mismo valor en su campo **Dataset location**. Decídelo antes del primer
   sync: es el que crea el dataset, y la location de un dataset no se puede
   cambiar. Después recrea los contenedores para que lean el valor nuevo:

   ```bash
   docker compose up -d
   ```

   El archivo de compose pasa `GCP_PROJECT_ID` y la ruta de la clave
   (`GOOGLE_APPLICATION_CREDENTIALS`) a todos los contenedores. Todos los
   clientes de Google del stack leen esas dos: el cliente de Python de
   BigQuery, dbt, y la Connection `google_cloud_default` que define el archivo
   de compose (`AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT`).

`dbt debug`, en «4. Crea el proyecto de dbt», comprueba que la clave funciona.

#### 2. Airbyte

Airbyte no forma parte del archivo de compose de este repo. Su instalador,
`abctl`, lo ejecuta en un pequeño clúster de Kubernetes que crea dentro de
Docker. Airbyte pide 4 CPUs y 8 GB de memoria además de lo que usa el stack de
Airflow (unos 4 GB), así que Docker necesita unos 12 GB en total. Mira lo que
tiene ahora, en las líneas `CPUs` y `Total Memory`:

```bash
docker info
```

Si no llega, dónde se aumenta depende de cómo corre Docker:

- **Docker Desktop en macOS o Linux:** **Settings → Resources → Advanced**,
  los límites de CPU y memoria.
- **Docker Desktop en Windows con el backend de WSL2** (el de por defecto): esa
  página no tiene límites que ajustar, porque Docker recibe lo que recibe
  WSL2: todas las CPUs y la mitad de la memoria de la máquina. Para darle más
  a WSL2, crea o edita el archivo `.wslconfig` en tu carpeta de usuario de
  Windows (`%UserProfile%`):

  ```ini
  [wsl2]
  memory=12GB
  ```

  Después detén WSL2 para que arranque con el límite nuevo, y vuelve a abrir
  Docker Desktop:

  ```powershell
  wsl --shutdown
  ```
- **Docker Engine en Linux:** no hay límite; los contenedores pueden usar toda
  la máquina.

1. **Instala `abctl`.** En Linux o macOS:

   ```bash
   curl -LsfS https://get.airbyte.com | bash -
   ```

   En Windows, descarga el zip `windows-amd64` (`windows-arm64` en una máquina
   ARM) de la [página de releases de abctl](https://github.com/airbytehq/abctl/releases/latest),
   descomprímelo, añade la carpeta a tu `Path` y abre una terminal nueva: el
   `Path` solo se lee al abrir la terminal.

2. **Instala Airbyte.** Descarga varios GB y tarda unos minutos:

   ```bash
   abctl local install
   ```

   Airbyte queda disponible en <http://localhost:8000>. En una máquina justa
   de memoria, `abctl local install --low-resource-mode` lo ejecuta con 2
   CPUs.

3. **Consigue las credenciales e inicia sesión:**

   ```bash
   abctl local credentials
   ```

   Imprime cuatro valores. `Password` es la contraseña de la UI. `Client-Id` y
   `Client-Secret` permiten a Airflow usar la API de Airbyte («3. Dile a
   Airflow cómo llegar a Airbyte»). `Email` dice `[not set]` hasta que lo fija
   el primer inicio de sesión.

   Abre <http://localhost:8000>. La primera vez, Airbyte pide un email y un
   nombre de organización: pon tu email y cualquier nombre, y elige **Get
   Started**. Después inicia sesión con ese email y el valor de `Password`.

4. **Crea dos sources**, una por CSV. Una *source* es el origen del que Airbyte
   lee los datos. **Sources → New source → File**, y luego:

   | Campo | Primera source | Segunda source |
   |---|---|---|
   | Dataset name | `projects` | `tags` |
   | File format | `csv` | `csv` |
   | Storage provider | HTTPS: Public Web | HTTPS: Public Web |
   | URL | `https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/projects.csv` | `https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/tags.csv` |

   El dataset name pasa a ser el nombre de la tabla en BigQuery. Los archivos
   están en la carpeta [`datasets/`](../../datasets/) de este repo: 764
   proyectos de Made With ML, y sus tags en una tabla aparte, unidos por `id`.
   Llegan más proyectos en la §12 y la §14.

5. **Crea la destination**, el sitio donde escribe Airbyte. **Destinations → New
   destination → BigQuery**:

   | Campo | Valor |
   |---|---|
   | Project ID | tu Project ID |
   | Dataset location | el mismo valor que `BQ_LOCATION` |
   | Default dataset ID | `mlops_course` |
   | Loading method | Batched Standard Inserts |
   | Service account key JSON | el contenido completo de `secrets/gcp-key.json` |

   Airbyte prueba la destination al guardarla. El dataset `mlops_course` (el
   nombre que da BigQuery a un grupo de tablas) se crea en el primer sync.

6. **Crea dos connections**: una *connection* une una source con una
   destination, y cada vez que se ejecuta es un *sync*. Crea `projects → BigQuery`
   y `tags → BigQuery`, las dos igual: **Connections → New connection**, elige
   la source y luego la destination de BigQuery. Siguen dos pantallas:
   - **La pantalla de streams.** Un *stream* es una tabla que ofrece la source;
     una source File ofrece una, con el nombre de su dataset. Déjalo
     seleccionado y pon su sync mode en **Full Refresh Overwrite**, para que
     cada sync reemplace la tabla en vez de añadirle filas. Elige **Next**.
   - **Configure connection.** Pon **Schedule type** en **Manual**, para que
     sea Airflow quien decida cuándo se ejecuta, y **Destination Namespace** en
     **Destination-defined**, para que la tabla acabe en `mlops_course`. Deja
     el resto como está, y elige **Set up connection**.

7. **Sincroniza cada connection una vez a mano**, con **Sync now**. Luego abre
   BigQuery en la consola de Google Cloud: `mlops_course` tiene una tabla
   `projects` y una tabla `tags`, de 764 filas cada una. Además de las columnas
   del CSV, cada una tiene unas cuantas que empiezan por `_airbyte_`, con las
   que Airbyte lleva el control de las cargas; el código de este repo lee solo las
   columnas del CSV.

8. **Copia el ID de cada connection.** Airbyte no lo muestra en la página:
   está en la barra de direcciones del navegador. Abre la connection desde
   **Connections**, y el ID es el UUID que va justo después de `/connections/`:

   ```
   http://localhost:8000/workspaces/<workspace id>/connections/<connection id>/status
   ```

   Guarda los dos IDs, el de `projects → BigQuery` y el de `tags → BigQuery`,
   para el paso 3.

#### 3. Dile a Airflow cómo llegar a Airbyte

Airflow llega a Airbyte a través de una Connection (§6), con el `Client-Id` y el
`Client-Secret` que imprimió `abctl local credentials`. `my_airbyte_conn` es el id de
la Connection: vale cualquier nombre, siempre que el Dag use el mismo.
`--conn-type airbyte` es fijo: le dice a Airflow que esta Connection la
gestiona el provider de Airbyte.

```bash
docker compose exec airflow-scheduler airflow connections add my_airbyte_conn \
    --conn-type airbyte \
    --conn-host http://host.docker.internal:8000/api/public/v1/ \
    --conn-login <client-id> --conn-password <client-secret>
```

**Comprobación.** La última línea es ``Successfully added `conn_id`=my_airbyte_conn``.
Antes salen muchas líneas de warnings. Casi todas son `DeprecatedImportWarning`
y `SyntaxWarning`, que aparecen mientras la CLI carga todos los providers de la
imagen; son ruido. La penúltima, `empty cryptography key - values will not be
stored encrypted`, es la falta de Fernet key de la §6: en este stack, las
Connections se guardan sin cifrar.

Dentro de un contenedor, `localhost` es el propio contenedor;
`host.docker.internal` es tu máquina. Docker Desktop define ese nombre, y la
línea `extra_hosts` del archivo de compose lo define en Linux.

Los dos IDs de connection que copiaste en el paso 8 de «2. Airbyte» van en
Variables (§6), para que el archivo del Dag no contenga ningún ID que solo
existe en tu Airbyte:

```bash
docker compose exec airflow-scheduler airflow variables set airbyte_projects_connection_id <projects connection id>
docker compose exec airflow-scheduler airflow variables set airbyte_tags_connection_id <tags connection id>
```

#### 4. Crea el proyecto de dbt

**dbt** ejecuta sentencias SQL `SELECT`, llamadas **models**, y guarda cada
resultado como una tabla del warehouse. Un **proyecto de dbt** es la carpeta
con la que trabaja dbt: los models, más archivos que dicen cómo conectarse y
qué comprobar. Vas a crear uno ahora, en `dbt/`, la carpeta que el archivo de
compose monta para ello, y a ejecutarlo una vez a mano; cuando se ejecute el
Dag, su task `transform` lanzará el mismo `dbt build` (el paso 3 del diagrama
del principio de la §9).

dbt está instalado en su propio virtualenv dentro de la imagen,
`/opt/venvs/dbt`, porque sus dependencias fijan versiones que chocan con las de
Airflow (el [`Dockerfile`](../../Dockerfile) lo explica). Todos los comandos de
dbt de abajo lo ejecutan desde ahí.

**Crea el esqueleto.** `dbt init` crea la carpeta de un proyecto con la
estructura estándar; `--skip-profile-setup` se salta sus preguntas
interactivas, porque el archivo de conexión lo escribes tú:

```bash
# Git Bash needs the MSYS_NO_PATHCONV prefix on any /opt/... path (§2)
MSYS_NO_PATHCONV=1 docker compose exec -w /opt/airflow/dbt airflow-scheduler \
    /opt/venvs/dbt/bin/dbt init dbt_transforms --skip-profile-setup
```

**Comprobación.** En tu máquina existe ya `dbt/dbt_transforms/`, con
`dbt_project.yml`, una carpeta `models/` y algunas carpetas vacías que este
tutorial no usa (`macros/`, `seeds/`, `snapshots/`, `tests/`, `analyses/`).

**Borra los models de ejemplo** que añade dbt. Desde la raíz del repo:

```bash
rm -r dbt/dbt_transforms/models/example
```

**Haz que cada model sea una tabla.** `dbt_project.yml` guarda los ajustes del
proyecto. Al final, sustituye el bloque `models:` que configura los ejemplos
borrados por:

```yaml
models:
  dbt_transforms:
    +materialized: table
```

Un model se puede guardar como *view* (se guarda la consulta, que se vuelve a
ejecutar cada vez que se lee) o como *table* (se guarda el resultado). Una
tabla se reconstruye desde cero en cada ejecución, así que ejecutar dbt dos
veces deja la misma tabla. Una tercera opción, *incremental*, agrega solo las
filas nuevas a la tabla existente en vez de reemplazarla; este tutorial no la
usa.

**Configura la conexión.** dbt lee su conexión de un *profile*. Crea
`dbt/dbt_transforms/profiles.yml`:

```yaml dbt/dbt_transforms/profiles.yml
dbt_transforms:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      keyfile: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
      project: "{{ env_var('GCP_PROJECT_ID') }}"
      dataset: "{{ env_var('BQ_DATASET', 'mlops_course') }}"
      location: "{{ env_var('BQ_LOCATION', 'US') }}"
      threads: 4
```

La primera línea, `dbt_transforms`, es el nombre del profile que pide
`dbt_project.yml`. `{{ env_var(...) }}` es Jinja, el mismo sistema de templates
que usa Airflow (§6): cada valor se lee del entorno del contenedor, así que el
archivo no guarda ningún secreto ni ningún ID de proyecto. Comprueba la
conexión; de paso, esto comprueba también tu clave, `secrets/gcp-key.json`:

```bash
MSYS_NO_PATHCONV=1 docker compose exec airflow-scheduler /opt/venvs/dbt/bin/dbt debug \
    --project-dir /opt/airflow/dbt/dbt_transforms --profiles-dir /opt/airflow/dbt/dbt_transforms
```

**Comprobación.** La última línea es `All checks passed!`. Si falla, el
mensaje indica la causa: un Project ID equivocado, ningún archivo en
`secrets/gcp-key.json`, o una service account a la que le falta un rol.

**Declara las tablas sin procesar.** Una *source* en dbt es una tabla que ya
está en BigQuery y que dbt lee pero no construye. Aquí las sources son `projects` y
`tags` del dataset `mlops_course`, las tablas que cargaste en el paso 2.
Declararlas una vez permite a los models referirse a ellas por su nombre. Crea
`dbt/dbt_transforms/models/sources.yml`:

```yaml dbt/dbt_transforms/models/sources.yml
version: 2

sources:
  - name: mlops_course
    database: "{{ env_var('GCP_PROJECT_ID') }}"
    schema: "{{ env_var('BQ_DATASET', 'mlops_course') }}"
    tables:
      - name: projects
      - name: tags
```

En términos de BigQuery, `database` es el proyecto y `schema` es el dataset.

**Escribe el model.** El nombre del archivo de un model es el nombre de la
tabla que construye. Crea
`dbt/dbt_transforms/models/labeled_projects/labeled_projects.sql`:

```sql dbt/dbt_transforms/models/labeled_projects/labeled_projects.sql
SELECT
    p.id,
    CAST(p.created_on AS TIMESTAMP) AS created_on,
    p.title,
    p.description,
    t.tag
FROM {{ source('mlops_course', 'projects') }} AS p
LEFT JOIN {{ source('mlops_course', 'tags') }} AS t
    ON p.id = t.id
```

`{{ source('mlops_course', 'projects') }}` se compila al nombre completo de la
tabla, `` `<project id>.mlops_course.projects` ``. En estos datos todos los
proyectos tienen tag. Si una carga posterior trae uno sin tag, un `JOIN` normal
lo descartaría sin dejar rastro; el `LEFT JOIN` lo conserva con `tag` en `NULL`,
y el test `not_null` de abajo falla.

**Añade tests.** Un *test* de dbt es una comprobación sobre las columnas de un
model, que se ejecuta después de construirlo. Crea
`dbt/dbt_transforms/models/labeled_projects/schema.yml`:

```yaml dbt/dbt_transforms/models/labeled_projects/schema.yml
version: 2

models:
  - name: labeled_projects
    columns:
      - name: id
        data_tests:
          - unique
          - not_null
      - name: title
        data_tests:
          - not_null
      - name: description
        data_tests:
          - not_null
      - name: tag
        data_tests:
          - not_null
```

**Construye el model.** `dbt build` construye todos los models y luego
ejecuta sus tests:

```bash
MSYS_NO_PATHCONV=1 docker compose exec airflow-scheduler /opt/venvs/dbt/bin/dbt build \
    --project-dir /opt/airflow/dbt/dbt_transforms --profiles-dir /opt/airflow/dbt/dbt_transforms
```

**Comprobación.** `Done. PASS=6 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=6`:
un model y cinco tests. BigQuery tiene una tercera tabla, `labeled_projects`, con 764
filas. Ejecuta el comando otra vez: mismo resultado, misma tabla, porque
`+materialized: table` hace que dbt reemplace la tabla en cada ejecución. Un
test fallido hace que `dbt build` termine con error, que es lo que hará fallar la
task `transform` del Dag.

¿Atascado? El proyecto terminado está en
[`solutions/dbt_transforms/`](../../solutions/dbt_transforms/); cópialo con
`cp -r solutions/dbt_transforms dbt/`.

#### 5. Great Expectations

Los tests de dbt comprueban la tabla que construye dbt. **Great Expectations**
(GX) comprueba además las tablas sin procesar, `projects` y `tags`, antes de que
dbt las lea, y tiene más tipos de reglas que los `unique` y `not_null` de
dbt, como un número mínimo de filas. Una **expectation** es una regla ("`id` es
único"); una **suite** es una lista de expectations con nombre; **validar** una
tabla contra una suite da un resultado, aprobado o fallido, por cada
expectation.

Las suites ya están escritas (no las tecleas tú), en
[`src/project_tagger/validate.py`](../../src/project_tagger/validate.py), una
por tabla. La de `projects`:

```python
# src/project_tagger/validate.py — build_suite("projects")
e.ExpectTableColumnsToMatchSet(column_set=["id", "created_on", "title", "description"]),
e.ExpectColumnValuesToBeUnique(column="id"),
e.ExpectColumnValuesToNotBeNull(column="id"),
e.ExpectColumnValuesToNotBeNull(column="title"),
e.ExpectCompoundColumnsToBeUnique(column_list=["title", "description"]),  # one project, two ids
e.ExpectTableRowCountToBeBetween(min_value=500),
```

`validate.run_suite(df, "projects")` valida un dataframe contra esa suite. En
su resultado, `.passed` dice si la suite ha pasado y `.failures` lista las
expectations que han fallado. (GX tiene también operators de Airflow, pero en octubre
de 2026 su paquete no se puede instalar junto a Airflow 3.3.1, así que las
tasks llaman a GX directamente.)

Cada expectation es *crítica* salvo que se marque lo contrario. Una
expectation crítica que falla hace fallar la suite; una no crítica solo
queda registrada. La suite `tags` tiene una expectation no crítica: cada tag
es uno de los seis conocidos. Un tag nuevo es motivo para mirar los datos, no para
detener el pipeline, así que aparece en `result.failures` pero
`result.passed` sigue siendo verdadero.

### Constrúyelo

Crea `dags/03_dataops.py`:

```python dags/03_dataops.py
"""Lesson 3 - the DataOps Dag: extract & load -> validate -> transform -> validate."""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task, task_group
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk.types import OutletEventAccessorsProtocol

from project_tagger import config, data, validate
from project_tagger_orchestration.assets import LABELED_PROJECTS, RAW_PROJECTS, RAW_TAGS

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

`AirbyteTriggerSyncOperator` viene del provider de Airbyte y `BashOperator`
del standard (§6); la imagen instala los providers de Airbyte y de Google
(`requirements-image.txt`). `AirflowFailException` y
`OutletEventAccessorsProtocol`, el tipo de `outlet_events` (§8), vienen del
Task SDK (§4). Los imports de `project_tagger_orchestration` y `project_tagger`
funcionan como en la §8, tanto cuando el Dag processor parsea el archivo
como cuando corre una task.

`max_active_runs=1` limita el Dag a un run a la vez; un segundo run espera en
`queued` hasta que termine el primero.

Las dos primeras tasks arrancan los syncs de Airbyte y esperan a que terminen.
Un sync puede tardar minutos, así que son **deferrable**: mientras esperan, la
task está en estado `deferred` (§3) y es el triggerer quien vigila Airbyte, no
un worker slot (la §11 lo explica a fondo). Añádelas dentro de `dataops()`:

```python dags/03_dataops.py
    extract_and_load_projects = AirbyteTriggerSyncOperator(
        task_id="extract_and_load_projects",
        airbyte_conn_id="my_airbyte_conn",
        connection_id="{{ var.value.airbyte_projects_connection_id }}",
        deferrable=True,
        timeout=3600,
        outlets=[RAW_PROJECTS],
    )
    extract_and_load_tags = AirbyteTriggerSyncOperator(
        task_id="extract_and_load_tags",
        airbyte_conn_id="my_airbyte_conn",
        connection_id="{{ var.value.airbyte_tags_connection_id }}",
        deferrable=True,
        timeout=3600,
        outlets=[RAW_TAGS],
    )
```

`airbyte_conn_id` nombra la Connection `my_airbyte_conn` que añadiste con
`airflow connections add`. `connection_id` es la connection de Airbyte que hay
que sincronizar; un template (§6) la lee de una Variable cuando corre la task.
Un sync fallido hace fallar la task. El outlet de cada task es el asset (§8) de una
tabla sin procesar (projects y tags) ; ningún Dag está programado con ellos, porque los datos
sin procesar aún no se han comprobado.

Después un **gate** sobre cada tabla sin procesar: una task que falla si la
tabla no pasa su suite, y con eso detiene todo lo que va aguas abajo:

```python dags/03_dataops.py
    @task
    def validate_projects() -> dict[str, Any]:
        result = validate.run_suite(data.load_projects(), "projects")
        print(result.summary())  # suite=projects passed=6/6
        if not result.passed:
            raise AirflowFailException(f"projects failed validation: {result.failures}")
        # The suite's name, and one dict per expectation with its outcome.
        return {"suite": result.suite, "results": result.results}

    @task
    def validate_tags() -> dict[str, Any]:
        result = validate.run_suite(data.load_tags(), "tags")
        print(result.summary())
        if not result.passed:
            raise AirflowFailException(f"tags failed validation: {result.failures}")
        return {"suite": result.suite, "results": result.results}
```

`AirflowFailException` hace fallar la task sin reintentos, diga lo que diga
`retries`. Los datos malos siguen siendo malos en un reintento, así que
reintentar solo retrasaría la alerta. Las tasks devuelven los resultados por
expectation, dicts pequeños; las tablas nunca pasan por XCom (§6).

Después `transform`, que ejecuta el `dbt build` que lanzaste a mano, y el gate
final, cuyo outlet es el asset con el que se programará `mlops`:

```python dags/03_dataops.py
    # config.DBT_BIN is /opt/venvs/dbt/bin/dbt; config.DBT_PROJECT_DIR, dbt/dbt_transforms.
    transform = BashOperator(
        task_id="transform",
        bash_command=(
            f"{config.DBT_BIN} build"
            f" --project-dir {config.DBT_PROJECT_DIR}"
            f" --profiles-dir {config.DBT_PROJECT_DIR}"
        ),
    )

    @task(outlets=[LABELED_PROJECTS])
    def validate_transformed(*, outlet_events: OutletEventAccessorsProtocol) -> dict[str, Any]:
        df = data.load_labeled()
        result = validate.run_suite(df, "labeled_projects")
        print(result.summary())
        if not result.passed:
            raise AirflowFailException(f"labeled_projects failed validation: {result.failures}")
        outlet_events[LABELED_PROJECTS].extra = {
            "rows": len(df),
            "tags": sorted(df["tag"].unique().tolist()),
            "warnings": [r["expectation"] for r in result.failures],
        }
        return {"suite": result.suite, "rows": len(df), "results": result.results}
```

Un `BashOperator` falla cuando su comando termina con error, así que un test de
dbt fallido hace fallar `transform`, y la salida de dbt queda como log de la
task. El outlet va en la validación final, no en `transform`, como explicó la §8: así
el evento significa "han llegado datos buenos". La suite de GX comprueba lo que
no comprueban los tests de dbt, como el número de filas y el conjunto de tags.

Por último, un **task group** y las conexiones entre tasks. `@task_group`
dibuja varias tasks como un único nodo plegable en la vista Graph:

```python dags/03_dataops.py
    @task_group(group_id="quality_report")
    def quality_report(reports: list[dict[str, Any]]):

        @task
        def summarize(reports: list[dict[str, Any]]) -> None:
            for report in reports:
                failed = [r["expectation"] for r in report["results"] if not r["passed"]]
                print(f"{report['suite']}: {len(report['results'])} checks, failed={failed or 'none'}")

        summarize(reports)

    projects_report = validate_projects()
    tags_report = validate_tags()
    extract_and_load_projects >> projects_report
    extract_and_load_tags >> tags_report

    curated_report = validate_transformed()
    [projects_report, tags_report] >> transform >> curated_report

    quality_report([projects_report, tags_report, curated_report])


dataops()
```

No pasan datos de las tasks de Airbyte a los gates, ni de los gates a
`transform`: cada una lee el warehouse por su cuenta. Así que esas aristas se
dibujan con `>>` (§6). `[projects_report, tags_report] >> transform` es un
**fan-in**: `transform` une las dos tablas, así que espera a los dos gates.
`quality_report` recibe los tres resultados como argumentos, y ese paso de
datos crea por sí solo las aristas de `validate_projects`, `validate_tags` y
`validate_transformed` hacia `quality_report`, sin `>>`.

### Comprueba que aparecen los assets

Guarda el archivo. A los pocos segundos `airflow dags list` tiene una tercera
fila, y la lista de assets también ha cambiado:

```bash
docker compose exec airflow-scheduler airflow assets list -o plain
```

**Comprobación.** Tres filas, `labeled_projects`, `raw_projects` y `raw_tags`,
cada una con una URI como `bigquery://mlops-tutorial-123456/mlops_course/projects`:
la tabla que representa ese asset. No ha corrido nada; ha bastado con parsear
un archivo con `outlets=`. `production_model`, también en `assets.py`, no
aparece porque ningún Dag se refiere a él todavía. La página Assets muestra los tres, producidos por
`dataops`, sin consumidor.

### Ejecútalo

```bash
docker compose exec airflow-scheduler airflow dags test dataops
```

Tarda uno o dos minutos, casi todo por los dos syncs de Airbyte, que corren uno
detrás de otro porque `airflow dags test` ejecuta las tasks de una en una.

**Comprobación.** En el log, por orden: cada task de Airbyte informando de que
su job ha terminado; `suite=projects passed=6/6` y `suite=tags passed=5/5`; el
`Done. PASS=6 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=6` de dbt en el log
de `transform`;
`suite=labeled_projects passed=6/6`; y del task group,
`projects: 6 checks, failed=none` y lo mismo para las otras dos.

`airflow dags test` no tiene triggerer, así que ejecuta él mismo los triggers
de las tasks de Airbyte y lo anuncia en el log con `[DAG TEST] running trigger
in line` (§4). En un run normal esos triggers los ejecuta el triggerer, y las
tasks se quedan en `deferred` mientras Airbyte trabaja.

¿Falla una task? Compara con [`solutions/03_dataops.py`](../../solutions/03_dataops.py).
Una task de Airbyte fallida suele indicar una Connection o una Variable mal
puesta: compruébalo con `airflow connections get my_airbyte_conn` y
`airflow variables list`.

> **`airflow dags test` no es un dry run.** Airbyte volvió a sincronizar las dos
> tablas, dbt reconstruyó `labeled_projects`, y hay un asset event
> `labeled_projects` real en la metadata database, visible en la página Assets
> con el `extra` que le pusiste. Todavía no hay nada programado con ese asset,
> así que no pasa nada más, y según la §8 no pasará: cuando `mlops` aparezca en la §10 este evento
> ya será historia, y Airflow no recupera eventos pasados.

Ejecutar el Dag dos veces deja el warehouse igual que ejecutarlo una: cada sync
reemplaza su tabla, y dbt reconstruye `labeled_projects` desde cero. Esa
propiedad, la **idempotencia** (§6), es lo que hace seguro `retries=2` aquí.

### Backfills

Ahora que tienes un Dag con schedule, los backfills tienen sentido. Un backfill
crea a posteriori los runs de cada franja de un rango de fechas. En Airflow un
backfill es un objeto propio, con su API, su formulario en la UI y su política
de reprocesado (`--reprocess-behavior`, más abajo). **Haz un dry-run
primero**: `--dry-run` lista los runs que crearía el backfill sin crearlos, y un
backfill con el rango de fechas equivocado es la forma más fácil de crear
treinta runs por accidente:

```bash
docker compose exec airflow-scheduler airflow backfill create \
    --dag-id dataops --from-date 2026-02-01 --to-date 2026-02-04 --dry-run
```

**Comprobación.** Una tabla encabezada `Runs to be attempted:` con tres filas —
2026-02-01, 02 y 03, cada una a las 03:00 — y nada creado. Cada fila es una
franja del cron diario de `dataops`. El rango incluye sus dos extremos, pero
`--to-date 2026-02-04` significa las 00:00 de ese día, antes de que el cron se
dispare a las 03:00, así que el 02-04 queda fuera.

Quita `--dry-run` y el mismo comando crea esos tres runs, con run ids que
empiezan por `backfill__`. Ten claro qué viene después antes de hacerlo:

- Los runs se crean **queued**, y si el Dag está en pausa se quedan así — la
  regla de la §7 — hasta que le quites la pausa. Entonces arrancan, como mucho
  tantos a la vez como diga la opción `--max-active-runs` de `backfill create`.
- Cada run de backfill de `dataops` que tenga éxito emite un asset event de
  `labeled_projects` igual que un run normal (§8), así que un `mlops` sin
  pausa entrena detrás de ellos.
- Cada uno vuelve a sincronizar las dos tablas desde el origen tal como está
  *ahora*. Nada en este pipeline lee una fecha: el origen no tiene historia a
  la que volver, así que aquí un backfill repite el trabajo en vez de recuperar
  datos pasados. Un pipeline cuyo origen se puede consultar por fecha le pasa
  el `data_interval_start` y el `data_interval_end` del run (§7), y ahí es
  donde los backfills justifican su existencia.
- La CLI en 3.3.1 solo *crea* backfills. Mientras uno no ha terminado, la
  página del Dag muestra un banner *Backfill in progress* con un botón de pausa
  y otro de stop; stop lo cancela. También la API — `PUT
  /api/v2/backfills/{id}/cancel`, sin token en este stack, donde todos son
  admin. Cancelarlo marca como `failed` los runs del backfill aún en cola.
  Pausar el Dag es el freno de emergencia: un Dag en pausa no arranca nada.

`--reprocess-behavior` decide qué hacer con las franjas que ya tienen un run —
`none`, `failed` o `completed` — que es la diferencia entre "rellena los huecos"
y "rehaz el mes". En la UI: Dag → **Trigger** → **Backfill**, que hace un dry-run
del rango y te dice cuántos runs va a crear antes de confirmar. Por esto mismo
no se puede hacer backfill de un Dag programado con assets (§5): no tiene
franjas, solo eventos.

### Alternativas

Cada herramienta es una opción entre varias; cambiar una supone cambiar el
operator de su task, no la forma del Dag:

| Aquí | Alternativas | En Airflow |
|---|---|---|
| Airbyte | Fivetran, Meltano, dlt | `FivetranOperator`, un `BashOperator`, un `@task` |
| BigQuery | Snowflake, Redshift, Databricks, Postgres | cada uno tiene un provider con hooks y operators |
| Great Expectations | Soda, Pandera, solo los tests de dbt | `SodaScanOperator`, un `@task` |
| dbt Core en un `BashOperator` | dbt con astronomer-cosmos, dbt Cloud | Cosmos convierte cada model y cada test en su propia task; `DbtCloudRunJobOperator` arranca un job de dbt Cloud |

Merece la pena conocer Cosmos: con `dbt build` en una sola task, un único test
fallido hace fallar la task entera, y un reintento lo reconstruye todo; con una
task por model, el Graph muestra qué model se rompió y un reintento solo
vuelve a ejecutar ese.

---

← [8. Assets: programar por datos, no por tiempo](08-assets-programar-por-datos-no-por-tiempo.md) · [Índice](README.md) · [10. El Dag de MLOps](10-el-dag-de-mlops.md) →
