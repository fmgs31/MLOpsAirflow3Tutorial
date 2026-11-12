## 15. Testear Dags

Tres niveles de tests sobre el código y uno sobre los datos.

**Tests de integridad de Dags** — [`tests/test_dag_integrity.py`](../../tests/test_dag_integrity.py).
Detectan, antes de que el archivo llegue a Airflow, los fallos que de otro modo
verías como un error de import en la UI: lo más valioso que puedes meter en CI.

Se apoyan en `DagBag`, la clase con la que Airflow parsea una carpeta de Dags:
importa cada archivo y guarda los Dags en `.dags` y los archivos que fallaron
en `.import_errors`. El fixture `dagbag` construye uno con tu carpeta `dags/`,
después de ponerla en `sys.path` como hace Airflow (§8) para que tus Dags
puedan importar `project_tagger_orchestration`:

```python
import sys
from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag

DAGS_FOLDER = Path(__file__).resolve().parents[1] / "dags"

@pytest.fixture(scope="session")
def dagbag():
    sys.path.insert(0, str(DAGS_FOLDER))
    return DagBag(dag_folder=str(DAGS_FOLDER))

def test_no_import_errors(dagbag):
    assert not dagbag.import_errors

def test_mlops_is_scheduled_on_the_labeled_projects_asset(dagbag):
    asset_names = {a.name for a in dagbag.dags["mlops"].timetable.asset_condition.objects}
    assert "labeled_projects" in asset_names
```

El resto comprueba lo que este tutorial espera de tus Dags: los siete
`dag_id`, tags, `doc_md`, owners, `retries`, los assets de `dataops`, el
schedule de `mlops` y que no haya ciclos. La suite también hace grep en el
código buscando construcciones de versiones anteriores de Airflow, porque la
mayoría del código de Airflow en internet es anterior a Airflow 3 y un
fragmento copiado puede colarlas. Hace falta el grep porque algunas se importan
sin error: `test_no_import_errors` no las ve, y fallan más tarde o no hacen
nada.

**Tests unitarios sobre la carga de trabajo** — [`tests/test_project_tagger.py`](../../tests/test_project_tagger.py).
Nada en `src/project_tagger/` importa Airflow, así que el código de ML se
testea sin Scheduler: Airflow orquesta el código, no lo contiene. Tampoco tocan
BigQuery ni el servidor de MLflow, que harían los tests lentos y dependientes
de credenciales y de la red. Cada test le pasa al código el dataframe que
habría devuelto la consulta, y Optuna y MLflow corren contra archivos SQLite en
una carpeta temporal, adonde los apunta el `monkeypatch` de pytest (que
sustituye un valor mientras dura un test). El test de drift necesita
alibi-detect, que solo está en su propio virtualenv (§12), así que en el
entorno de Airflow se **salta** (skip), no falla.

**Tests de datos** — las suites de GX y los tests de dbt (§9). Corren donde
están los datos: como gates en cada ejecución del pipeline, no en CI. Un test
de código demuestra que el pipeline hace lo que pretendías; uno de datos, que
los datos de hoy son los que el pipeline espera. Hacen falta los dos.

**Tests de run completo** — `airflow dags test <dag_id>` ejecuta un Dag run real
dentro del propio comando (§4), y `airflow tasks test <dag_id> <task_id>`
ejecuta una sola task sin guardar su estado en la Metadata database.

Los tests de integridad y unitarios son una sola ejecución de `pytest`:

```bash
docker compose run --rm --entrypoint pytest airflow-cli /opt/airflow/tests -q
```

`airflow-cli` no aparece en `docker compose ps`: es un contenedor de un solo
uso, definido bajo un profile del compose, que `run` crea y `--rm` elimina al
terminar.

**Comprobación.** Una fila de puntos, una `s`, y `45 passed, 1 skipped`,
en bastante menos de un minuto:

```
..............................s...............                           [100%]
45 passed, 1 skipped in 23.78s
```

La `s` es el test de drift. Ninguno necesita Scheduler, BigQuery ni MLflow, así
que son lo bastante rápidos para ejecutarse en cada commit.

Cuando uno falla, el mensaje dice qué archivo y qué falta (`mlops has no
retries`, `Dags not yet written in /opt/airflow/dags: ['monitoring']`); el
arreglo está en la sección que construyó ese archivo, o en `solutions/`. Para
ver la suite pasar con las versiones terminadas, apúntala a esa carpeta:

```bash
docker compose run --rm -e DAGS_DIR=solutions --entrypoint pytest airflow-cli /opt/airflow/tests -q
```

> En **Git Bash**, antepón `MSYS_NO_PATHCONV=1` a ambos comandos o el argumento
> `/opt/airflow/tests` se reescribe a una ruta de Windows y pytest no recoge
> nada.

Ningún test de esta sección detecta los fallos del recuadro de la "trampa" de
la §7 (el `KeyError` y el `UndefinedError` de un run sin logical date), porque
`airflow dags test` siempre aporta una. **Dispara cada Dag una vez de verdad**
antes de darlo por bueno: con `airflow dags trigger` sin `-l`, para que el run
no tenga logical date, igual que uno disparado por asset.

---

← [14. Cuando los datos fallan](14-cuando-los-datos-fallan.md) · [Índice](README.md) · [16. Llevarlo a producción](16-llevarlo-a-produccion.md) →
