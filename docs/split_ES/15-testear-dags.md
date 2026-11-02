## 15. Testear Dags

Cuatro niveles de tests sobre el código — el primero comprueba los siete
archivos que escribiste — y uno sobre los datos.

**Tests de integridad de Dags** — [`tests/test_dag_integrity.py`](../../tests/test_dag_integrity.py).
Detectan, antes de que el archivo llegue a Airflow, los fallos que de otro modo
verías como un error de import en la UI: lo más valioso que puedes meter en CI.

Se apoyan en `DagBag`, la clase que usa Airflow para parsear una carpeta de
Dags: `DagBag(dag_folder=...)` importa todos sus archivos y guarda los Dags
encontrados en `.dags` y los archivos que fallaron en `.import_errors`. El
argumento `dagbag` de cada test es un fixture de pytest, definido en el mismo
archivo, que construye uno a partir de tu carpeta `dags/` tras ponerla en
`sys.path` como hace Airflow (§8), para que tus Dags puedan importar
`project_tagger_orchestration`:

```python
from airflow.dag_processing.dagbag import DagBag

def test_no_import_errors(dagbag):
    assert not dagbag.import_errors

def test_mlops_is_scheduled_on_the_labeled_projects_asset(dagbag):
    asset_names = {a.name for a in dagbag.dags["mlops"].timetable.asset_condition.objects}
    assert "labeled_projects" in asset_names
```

Comprueban **tu carpeta `dags/`** contra lo que este tutorial espera: siete
`dag_id` concretos; todos los Dags con tags, un `doc_md` y un owner distinto de
`airflow`; `retries` de al menos uno en todo Dag que haga trabajo de verdad;
`dataops` produciendo sus tres assets; `mlops` programado con
`labeled_projects` y validando antes de entrenar; sin ciclos. La suite también
hace grep en el código buscando construcciones de versiones anteriores de
Airflow, que un fragmento copiado y pegado puede colar, porque la mayoría del
código de Airflow en internet es anterior a Airflow 3: `schedule_interval=`,
`days_ago(`, `airflow.operators.`, `execution_date`, `SubDagOperator`, `sla=`.
Algunas impiden que el archivo se importe (`schedule_interval=`, `days_ago(`,
`SubDagOperator`); el resto pasan sin avisar: `airflow.operators.` sigue
importando a través de un shim de deprecación, `sla=` se acepta y no hace nada,
y `execution_date` solo falla cuando corre la task.

**Tests unitarios sobre la carga de trabajo** — [`tests/test_project_tagger.py`](../../tests/test_project_tagger.py).
Nada en `src/project_tagger/` importa Airflow, así que el código de ML es
testeable sin un Scheduler. Esa separación es deliberada: Airflow orquesta el
código, no lo contiene.

Tampoco necesitan el warehouse ni el servidor de MLflow, también a propósito:
un test que consulta BigQuery es lento, necesita credenciales y falla cuando
falla la red. Así que cada test construye el dataframe que habría devuelto una
consulta y se lo pasa: las suites de GX, las comprobaciones del split y las
ventanas de drift reciben un dataframe, no un nombre de tabla. Optuna y MLflow
corren de verdad, contra archivos SQLite en una carpeta temporal: el
`monkeypatch` de pytest, que sustituye un valor mientras dura un test, apunta
allí `config.OPTUNA_STORAGE` y `config.MLFLOW_TRACKING_URI`; después el test
ejecuta la búsqueda, entrena, registra, promociona y hace rollback. Los tests
de monitorización reciben un modelo en producción de la misma forma:
`monkeypatch` sustituye `registry.get_production` y `registry.load_split_ids`
por funciones que devuelven un champion inventado y sus ids de split. El test
de drift necesita alibi-detect, que solo está instalado en su propio virtualenv
(§12), así que en el entorno de Airflow se **salta** (skip), no falla.

**Tests de datos** — las suites de GX y los tests de dbt (§9). Testean los
datos, no el código, así que corren donde están los datos: como gates en cada
ejecución del pipeline, no en CI. Un test de código demuestra que el pipeline
hace lo que pretendías; un test de datos demuestra que los datos de hoy son los
que el pipeline espera. Hacen falta los dos.

**Tests de run completo** — `airflow dags test <dag_id>` ejecuta un Dag run real
dentro del propio comando (§4), y `airflow tasks test <dag_id> <task_id>`
ejecuta una sola task sin guardar su estado en la Metadata database.

**Tests sobre la documentación** — un tutorial que cita código solo es útil
mientras las citas sean correctas.
[`tests/test_readme_listings.py`](../../tests/test_readme_listings.py): el
[apéndice](../appendix-dags_ES.md) muestra cada solución completa, inyectada
desde `solutions/` con [`scripts/sync_readme.py`](../../scripts/sync_readme.py),
no copiada a mano; el test vuelve a ejecutar la inyección en memoria y falla si
el resultado difiere.
[`tests/test_readme_build_steps.py`](../../tests/test_readme_build_steps.py):
cada bloque de "Constrúyelo" de este tutorial lleva la etiqueta de su archivo;
el test los concatena en orden, carga el resultado con `DagBag`, y falla si lo
que se te dijo que teclearas no da un Dag que se importe sin errores.

Los tests de integridad, unitarios y de documentación son una sola ejecución de
`pytest`:

```bash
docker compose run --rm --entrypoint pytest airflow-cli /opt/airflow/tests -q
```

`airflow-cli` no está entre los servicios que lista `docker compose ps`: es un
contenedor de usar y tirar definido bajo un profile del compose, que `run` crea
para el comando y `--rm` elimina después.

**Comprobación.** Una fila de puntos, una `s`, y `45 passed, 1 skipped`,
en bastante menos de un minuto:

```
..............................s...............                           [100%]
45 passed, 1 skipped in 23.78s
```

La `s` es el test de drift, que se salta por el motivo explicado en «Tests
unitarios sobre la carga de trabajo». Ningún Scheduler, ningún Dag run, nada
llega a BigQuery ni al servidor de MLflow: cada uno de estos tests es lo
bastante rápido para ejecutarse en cada commit.

Cuando uno falla, el mensaje dice qué archivo y qué falta (`mlops has no
retries`, `Dags not yet written in /opt/airflow/dags: ['monitoring']`, a
`dataops` le falta un outlet); el arreglo está en la sección que construyó ese
archivo, o en `solutions/`. Para ver la suite pasar con las versiones
terminadas, apúntala a esa carpeta:

```bash
docker compose run --rm -e DAGS_DIR=solutions --entrypoint pytest airflow-cli /opt/airflow/tests -q
```

> En **Git Bash**, antepón `MSYS_NO_PATHCONV=1` a ambos comandos o el argumento
> `/opt/airflow/tests` se reescribe a una ruta de Windows y pytest no recoge
> nada.

Cuando falla `test_readme_listings.py`, un listado del apéndice ya no coincide
con su archivo en `solutions/`. El script de inyección lo comprueba y lo
arregla:

```bash
python scripts/sync_readme.py --check     # exit 1 when an appendix listing is stale
python scripts/sync_readme.py             # fix it
```

Ninguno de estos niveles detecta los fallos del recuadro de la "trampa" de la
§7 (el `KeyError` y el `UndefinedError` de un run sin logical date):
`airflow dags test` siempre aporta una logical date, así que un Dag que se
rompe sin ella pasa todos los tests de esta sección. **Dispara cada Dag una vez
de verdad** antes de darlo por bueno: a través del Scheduler, con
`airflow dags trigger` y sin `-l`, para que el run no tenga logical date, igual
que uno disparado por asset.

---

← [14. Cuando los datos fallan](14-cuando-los-datos-fallan.md) · [Índice](README.md) · [16. Llevarlo a producción](16-llevarlo-a-produccion.md) →
