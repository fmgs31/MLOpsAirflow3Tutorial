## 14. Testear Dags

Cuatro niveles de tests sobre el código — el primero de ellos comprueba los
siete archivos que escribiste — y uno sobre los datos.

**Tests de integridad de Dags** — [`tests/test_dag_integrity.py`](../../tests/test_dag_integrity.py).
Detectan, antes de que el archivo llegue a Airflow, los fallos que de otro modo
verías como un error de import en la UI, y son lo más valioso que puedes meter
en CI.

Se apoyan en `DagBag`, la clase que usa Airflow para parsear una carpeta de
Dags: `DagBag(dag_folder=...)` importa todos sus archivos y guarda los Dags que
encontró en `.dags` y los archivos que fallaron en `.import_errors`. El
argumento `dagbag` de cada test es un fixture de pytest, definido en el mismo
archivo, que construye uno a partir de tu carpeta `dags/`:

```python
from airflow.dag_processing.dagbag import DagBag

def test_no_import_errors(dagbag):
    assert not dagbag.import_errors

def test_mlops_is_scheduled_on_the_labeled_projects_asset(dagbag):
    asset_names = {a.name for a in dagbag.dags["mlops"].timetable.asset_condition.objects}
    assert "labeled_projects" in asset_names
```

Comprueban **tu carpeta `dags/`**, y codifican lo que este tutorial espera:
siete `dag_id` concretos; todos los Dags con tags, un `doc_md` y un owner que no
sea `airflow`; `retries` de al menos uno en todo Dag que haga trabajo de verdad;
`dataops` produciendo sus tres assets; `mlops` programado con `labeled_projects` y
validando antes de entrenar; sin ciclos. La suite también hace grep en el código buscando
construcciones de versiones anteriores de Airflow — `schedule_interval=`,
`days_ago(`, `airflow.operators.`, `execution_date`, `SubDagOperator`, `sla=`.
La mayoría del código de Airflow que hay en internet es anterior a Airflow 3, así
que un fragmento copiado y pegado puede colar una de estas. Algunas impiden que el
archivo se importe (`schedule_interval=`, `days_ago(`, `SubDagOperator`); el
resto pasan sin avisar: `airflow.operators.` sigue importando a través de un
shim de deprecación, `sla=` se acepta y no hace nada, y `execution_date` solo
falla cuando corre la task.

**Tests unitarios sobre la carga de trabajo** — [`tests/test_project_tagger.py`](../../tests/test_project_tagger.py).
Salvo `assets.py`, que define los objetos Asset, nada en `src/project_tagger/` importa
Airflow, así que el código de ML es testeable sin un scheduler de por medio. Esa separación es deliberada: Airflow orquesta el
código, no lo contiene.

Tampoco necesitan el warehouse ni el servidor de MLflow, y eso también es
deliberado. Un test que consulta BigQuery es lento, necesita credenciales y
falla cuando falla la red. Así que cada test construye el dataframe que habría
devuelto una consulta y se lo pasa: las suites de GX, las comprobaciones del
split y las ventanas de drift reciben un dataframe, no un nombre de tabla.
Optuna y MLflow corren de verdad, pero contra archivos SQLite en una carpeta
temporal: el test apunta allí `config.OPTUNA_STORAGE` y
`config.MLFLOW_TRACKING_URI` con el `monkeypatch` de pytest, que sustituye un
valor mientras dura un test, y después ejecuta
la búsqueda, entrena, registra, promociona y hace rollback. El test de drift necesita
alibi-detect, que solo está instalado en su propio virtualenv (§12), así que en el
entorno de Airflow se **salta** (skip), no falla.

**Tests de datos** — las suites de GX y los tests de dbt (§9). También son
tests, pero sobre los datos en vez de sobre el código, así que corren donde
están los datos: en cada ejecución del pipeline, como gates, no en CI. Un test
de código demuestra que el pipeline hace lo que pretendías; un test de datos
demuestra que los datos de hoy son los que el pipeline espera. Hacen falta los
dos.

**Tests de run completo** — `airflow dags test <dag_id>` ejecuta un Dag run real
dentro del propio comando (§4), y `airflow tasks test <dag_id> <task_id>`
ejecuta una sola task sin guardar su estado en la metadata database.

**Tests sobre la documentación** — dos, porque un tutorial que cita código solo
es útil mientras las citas sean correctas.
[`tests/test_readme_listings.py`](../../tests/test_readme_listings.py): el
[apéndice](../appendix-dags_ES.md) muestra cada solución completa, inyectada
desde `solutions/` con [`scripts/sync_readme.py`](../../scripts/sync_readme.py) en vez
de copiada a mano; el test vuelve a ejecutar la inyección en memoria y falla si
el resultado difiere.
[`tests/test_readme_build_steps.py`](../../tests/test_readme_build_steps.py): cada
bloque de "Constrúyelo" de este tutorial lleva la etiqueta del archivo al que
pertenece; el test los concatena en orden, carga el resultado con `DagBag`, y
falla si lo que se te dijo que teclearas no da como resultado un Dag que se
importe sin errores.

Los tests de integridad, unitarios y de documentación son una sola ejecución de
`pytest`:

```bash
docker compose run --rm --entrypoint pytest airflow-cli /opt/airflow/tests -q
```

`airflow-cli` no es uno de los servicios que lista `docker compose ps`: es
un contenedor de usar y tirar que el archivo de compose define bajo un profile,
que `run` crea para ese comando y que `--rm` elimina después.

**Comprobación.** Una fila de puntos, una `s`, y `41 passed, 1 skipped`,
en bastante menos de un minuto:

```
............................s.............                               [100%]
41 passed, 1 skipped in 20.75s
```

La `s` es el test de drift, que se salta por el motivo que da «Tests unitarios sobre la carga
de trabajo». No
intervino ningún scheduler, no corrió ningún Dag, y nada llegó a BigQuery ni al
servidor de MLflow. De eso se trata: cada uno de esos tests es lo bastante
rápido para ejecutarse en cada commit, y eso es lo que hace que merezca la
pena tenerlos.

Cuando uno falla, el mensaje dice qué archivo y qué falta — `mlops has no
retries`, `Dags not yet written in /opt/airflow/dags: ['monitoring']`, a
`dataops` le falta un outlet — y el arreglo está en la sección que construyó ese
archivo, o en `solutions/`. Para ver la suite pasar con las versiones
terminadas, apúntala a esa carpeta:

```bash
docker compose run --rm -e DAGS_DIR=solutions --entrypoint pytest airflow-cli /opt/airflow/tests -q
```

> En **Git Bash**, antepón `MSYS_NO_PATHCONV=1` a ambos comandos o el argumento
> `/opt/airflow/tests` se reescribe a una ruta de Windows y pytest no recoge
> nada.

Cuando falla `test_readme_listings.py`, un listado del apéndice ya no coincide
con su archivo en `solutions/`. El script que inyecta los listados lo comprueba
y lo arregla:

```bash
python scripts/sync_readme.py --check     # exit 1 when an appendix listing is stale
python scripts/sync_readme.py             # fix it
```

Ninguno de esos niveles habría detectado los fallos del recuadro de la "trampa"
de la §7 (el `KeyError` y el `UndefinedError` de un run sin logical date):
`airflow dags test` siempre aporta una logical date, así que un Dag que
se rompe en un run sin ella pasa todos los tests de esta sección. **Dispara cada Dag
una vez de verdad** — a través del scheduler, con `airflow dags trigger` y sin `-l`,
para que el run no tenga logical date, igual que uno disparado por asset — antes
de darlo por bueno.

---

← [13. Deadline alerts](13-deadline-alerts.md) · [Índice](README.md) · [15. Llevarlo a producción](15-llevarlo-a-produccion.md) →
