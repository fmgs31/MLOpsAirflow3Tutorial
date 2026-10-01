## 5. La UI

La UI es una aplicación React encima de la REST API, servida por el api-server en
<http://localhost:8080>. La mitad de lo que enseña este tutorial se entiende mejor
viéndolo que leyéndolo, así que haz el recorrido ahora que hay un Dag que mirar.

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
Actions**. Haz clic en un run, luego en una task, y la misma barra de pestañas pasa a
ser la de esa task instance: **Logs**, **XCom**, **Rendered Templates**, y **Task
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
triggered"* antes de confirmar. Necesita un Dag con schedule, así que espera a la
§9. Los Dags programados con assets dicen *"This Dag's schedule does not support
backfills"* — a `mlops` (§10) no se le puede hacer backfill, porque no hay rango
de fechas que reproducir, solo asset events.

**Assets** — la pestaña para la que se construyó este tutorial, y vacía hasta la
§9. En cuanto existan los Dags que los usan, lista cada asset con sus tasks
productoras, los Dags consumidores y el histórico de eventos, y dibuja el grafo
entre Dags: `dataops` → `labeled_projects` → `mlops` → `production_model`. Ese
grafo es la única imagen del pipeline completo; la lista de Dags no puede
enseñarlo, porque la conexión no es una dependencia entre Dags, es un asset entre
ellos. **Create Event** sobre un asset o bien lo materializa (ejecuta el Dag
aguas arriba, que produce los datos de verdad) o registra un evento manual, que
dispara los Dags aguas abajo sin ejecutar el productor.

**Browse** — Audit Log (quién disparó, pausó o limpió qué — tu trigger desde la
UI ya está ahí), **Deadlines** (deadline alerts pendientes y perdidas, §13) y
XComs.

**Admin** — Connections, Variables, Pools, Providers, Plugins, Jobs y la
**Config** renderizada de Airflow. Connections y Variables son las dos que vas a
usar: nunca metas una credencial en un archivo de Dag.

> **Security y Plugins** están casi vacías aquí. Este stack corre el Simple auth
> manager con todo el mundo como admin, así que no hay usuarios ni roles que
> gestionar.

---

← [4. Tu primer Dag](04-tu-primer-dag.md) · [Índice](README.md) · [6. Operators, XComs y relaciones](06-operators-xcoms-y-relaciones.md) →
