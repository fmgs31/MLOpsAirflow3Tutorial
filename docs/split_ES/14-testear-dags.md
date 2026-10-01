## 14. Testear Dags

Cuatro niveles, el más barato primero — y el primero de ellos es el corrector de
los siete archivos que escribiste.

**Tests de integridad de Dags** — [`tests/test_dag_integrity.py`](../../tests/test_dag_integrity.py).
Detectan los fallos que, si no, verías horas después como un banner rojo en la
UI, y son lo de mayor valor que puedes meter en CI:

```python
from airflow.dag_processing.dagbag import DagBag

def test_no_import_errors(dagbag):
    assert not dagbag.import_errors

def test_mlops_is_scheduled_on_the_labeled_projects_asset(dagbag):
    asset_names = {a.name for a in dagbag.dags["mlops"].timetable.asset_condition.objects}
    assert "labeled_projects" in asset_names
```

Cargan **tu carpeta `dags/`**, y codifican lo que este tutorial espera:
siete `dag_id` concretos; todos los Dags con tags, un `doc_md` y un owner que no
sea `airflow`; `retries` de al menos uno en todo Dag que haga trabajo de verdad;
`dataops` produciendo ambos assets; `mlops` programado con `labeled_projects` y
validando antes de entrenar; sin ciclos. También hace grep en el código buscando
construcciones que no existen en esta versión de Airflow — `schedule_interval=`,
`days_ago(`, `airflow.operators.`, `execution_date`, `SubDagOperator`, `sla=`.
La mayoría del código de Airflow que hay en internet es anterior a Airflow 3, así
que un snippet copiado y pegado puede colar una de estas, y varias de ellas
incluso *importan* y solo fallan cuando corre la task.

**Tests unitarios sobre la carga de trabajo** — [`tests/test_tagifai.py`](../../tests/test_tagifai.py).
Nada en `src/tagifai/` importa Airflow, así que el código de ML es testeable sin
un scheduler de por medio. Esa separación es deliberada: Airflow orquesta el
código, no lo contiene.

**Tests de run completo** — `airflow dags test <dag_id>` ejecuta un Dag run real
in-process, y `airflow tasks test <dag_id> <task_id>` ejecuta una task sin
escribir estado.

**Tests sobre la documentación** — dos, porque un tutorial que cita código solo
es útil mientras las citas son verdad.
[`tests/test_readme_listings.py`](../../tests/test_readme_listings.py): el
[apéndice](../appendix-dags_ES.md) muestra cada solución completa, inyectada
desde `solutions/` con [`scripts/sync_readme.py`](../../scripts/sync_readme.py) en vez
de copiada a mano; el test vuelve a ejecutar la inyección en memoria y falla si
el resultado difiere.
[`tests/test_readme_build_steps.py`](../../tests/test_readme_build_steps.py): cada
bloque de "Constrúyelo" de arriba lleva la etiqueta del archivo al que
pertenece; el test los concatena en orden, carga el resultado con `DagBag`, y
falla si lo que se te dijo que teclearas no da como resultado un Dag que se
importe sin errores.

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

Ninguno de esos niveles habría detectado los dos bugs del recuadro de la "trampa"
de la §7: `airflow dags test` siempre aporta una logical date, así que un Dag que
se rompe con un trigger manual pasa todos los tests de arriba. **Dispara cada Dag
una vez de verdad** — por el scheduler, como va a correr realmente — antes de
darlo por bueno.

---

← [13. Deadline alerts](13-deadline-alerts.md) · [Índice](README.md) · [15. Llevarlo a producción](15-llevarlo-a-produccion.md) →
