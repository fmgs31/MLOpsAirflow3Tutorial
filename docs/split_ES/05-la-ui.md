## 5. La UI

La UI, en <http://localhost:8080>, es una aplicación React encima de la API
del api-server: la REST API pública más unos endpoints propios bajo `/ui/`.
Buena parte de lo que enseña este tutorial se entiende mejor viéndolo que
leyéndolo, así que haz el recorrido ahora que hay un Dag que mirar.

La navegación es **Home · Dags · Assets · Browse · Admin** arriba y, abajo,
**Docs**, un reloj y **User**.

**Home** — salud del scheduler, el triggerer y el Dag processor, errores de
import, runs y tasks fallidos, asset events recientes, deadlines y slots de
pools. Ábrela primero cuando algo no arranca.

**Dags** — la lista, como tarjetas o como tabla, filtrable por tag, owner o
estado, con el toggle de pausa y un botón **Trigger** en cada fila. La tuya
muestra `hello_airflow3`, su owner, sus dos tags y el único run que hizo
`airflow dags test`. Un Dag programado con assets muestra su condición de asset
donde un Dag con cron muestra su schedule; el tuyo, con `schedule=None`, no
muestra ninguna de las dos. Un Dag que nunca aparece aquí suele ser un Dag que
falló al importarse: junto al número de Dags aparece una insignia roja con el
número de fallos — haz clic en ella para ver los tracebacks.

**La página de un Dag** está dividida en dos: el Dag a la izquierda y las
pestañas de detalle a la derecha. El panel izquierdo tiene dos vistas, que se
eligen con los iconos de su esquina superior izquierda o se alternan con la
tecla `g`. **Grid** es el histórico de runs — una columna por run, una fila por
task, cada celda coloreada según su estado. **Graph** son las tasks y las
aristas entre ellas. En el otro extremo de esa fila están los controles que
conviene conocer:

- **Options** (el engranaje). En el Graph: la versión del Dag y el run a
  mostrar, la dirección del grafo y **Dependencies** — si el grafo muestra solo
  las tasks del propio Dag (el valor por defecto) o también los assets y Dags
  conectados con él. `hello_airflow3` no está conectado con nada, así que las
  tres opciones se ven igual; la §10 enseña la diferencia en `mlops`. En el
  Grid: cuántos runs mostrar, e indicadores de versión que marcan dónde cambió
  el código entre runs.
- **Filter** (un embudo): eliges una task y se muestra solo lo que está aguas
  arriba o aguas abajo de ella.
- **Task Filters** (una lupa, solo en el Graph). Solo aparece cuando hay algo
  que filtrar: hay un run seleccionado, o el Dag tiene task groups o más de un
  tipo de operator, así que en `hello_airflow3` la ves en cuanto abres un run.
  Filtra por estado, task group, duración y **Min. map index** — "shows mapped
  tasks expanded to at least this index". Una mapped task como `run_trials`
  (§10) es un único nodo colapsado hasta que subes ese valor; súbelo y cada
  instancia mapeada tiene su propio nodo.
- **Download graph image**, la flecha abajo a la derecha del Graph.

Las pestañas de la derecha dependen de lo que selecciones:

- **Nada** — las pestañas del propio Dag: Overview, Runs, Tasks, Calendar,
  Audit Log, Code y Details, más **Backfills** en un Dag con schedule.
- **Una task**, con clic en el Graph sin run seleccionado — esa task en todos
  sus runs: Overview, Task Instances, Audit Log.
- **Un run**, con clic en la pestaña **Runs** o en una barra del Grid — Task
  Instances, Asset Events, Audit Log, Code, Details. El panel izquierdo gana
  además un tercer icono de vista, entre Grid y Graph, cuyo tooltip dice *Show
  Gantt*: el Grid con una línea de tiempo de las tasks de ese run al lado.
- **Una task dentro de un run** — esa task instance: **Logs**, **Rendered
  Templates**, **Storage** (que contiene su **XCom**), Asset Events, Audit Log,
  Code, Details. Una mapped task añade **Task Instances**, una fila por copia
  mapeada, y una task que espera a una persona añade **Required Actions**.

**Hazlo ahora.** Abre `hello_airflow3` y mira el Graph: `make_greeting` se abre
hacia `shout` y `count_words`, y las dos vuelven a juntarse en `report`. Esa
forma sale de las últimas líneas de la §4 — pasar `message` a dos tasks, y los
resultados de ambas a `report`.

**El selector de versión del Dag.** Cada Dag run registra la versión del código
con la que corrió: cada vez que guardas un cambio en un archivo de Dag, Airflow
crea una versión nueva — `v1`, `v2`, … El selector, en **Options** del Graph y
en la pestaña **Code**, muestra cada versión con la fecha en que se creó, y te
permite ver un run con el código que lo produjo: el run de la
semana pasada sigue mostrando el grafo de la semana pasada. **Code** enseña
además cuándo se parseó el archivo y, en cuanto hay dos versiones, un botón para
compararlas.

Esas versiones salen del *bundle* del que se cargó el Dag: el sitio del que el
Dag processor lee archivos. Este stack tiene uno, la carpeta `dags/`, llamado
`dags-folder` — la columna `bundle_name` de `airflow dags list` en la §4. Una
carpeta local no tiene versión propia; un bundle de Git sí (el commit), y
entonces **Code** la muestra también (§16).

**Hazlo ahora.** Pulsa **Trigger**, arriba a la derecha de la página de
`hello_airflow3`. Se abre un formulario con un campo, `greeting`, relleno con
`hello`: es el `Param` que declaraste en la §4.

**El formulario de Trigger se genera a partir de tus `params`**, y por eso
merece la pena declararlos con tipos:

```python
params={"greeting": Param("hello", type="string")}              # dags/01
POLICY_PARAMS = {"drift_p_val": Param(0.05, type="number")}     # dags/06
```

Un param string se convierte en un campo de texto, un number en uno numérico, un
enum en un desplegable, y los valores se validan contra el tipo y los límites
de cada `Param` antes de crear el run. En **Advanced Options** puedes editar
los mismos valores como **Configuration JSON**, y el formulario ofrece además
un run id, una logical date y un data interval manual. Si el Dag está en pausa aparece también una casilla *"Unpause
… on trigger"*, marcada por defecto, que le quita la pausa al disparar.
Desmárcala y el run se crea igual, pero se queda **queued** hasta que quites la
pausa (§7).

Escribe otra palabra suelta en `greeting` — deja *Unpause* marcada si aparece —
y envía. Es el mismo `Param`, que llega a la misma task por una tercera
vía: un valor por defecto en el archivo, `--conf` en la línea de comandos, un
formulario en el navegador.

Ahora mira el resultado. Abre la pestaña **Runs**, haz clic en el run nuevo, haz
clic en `report` en el grafo y abre **Logs**: tu palabra en mayúsculas y
`(7 words)`, la línea que `airflow dags test` imprimió en tu terminal en la §4.
Haz clic en `make_greeting`, abre **Storage** y luego **XCom**: el string del
saludo, guardado bajo la clave `return_value` — el valor que recibieron `shout`
y `count_words`.

> El run que hizo `airflow dags test` no tiene logs aquí. Ese comando imprime
> la salida de las tasks en tu terminal y no escribe ningún archivo de log, así
> que su pestaña **Logs** solo dice que no encontró el archivo de log, y
> sugiere configurar remote logging. No hay nada roto; no hay archivo.

El formulario de Trigger tiene un segundo modo, **Backfill** — *"Run this Dag
for a range of dates"* — y en `hello_airflow3` aparece deshabilitado; pasa el
ratón por encima y dice *"This Dag's schedule does not support backfills"*. Un
backfill le pregunta al schedule qué runs tendrían que haber ocurrido entre dos
fechas y los crea: un schedule diario sobre cuatro días responde "cuatro runs,
uno por día". `schedule=None` no tiene respuesta: el Dag solo corre cuando
alguien lo dispara, así que en ningún rango de fechas falta un run que debería
haber ocurrido. Backfill se puede usar a partir de la §9, con `dataops`, que corre
a diario: un rango de fechas, un **Reprocess Behavior** (missing runs / missing
and errored runs / all runs), max active runs, "run backwards", y un dry run que
te dice *"N runs will be triggered"* antes de confirmar. `mlops` (§10) lo tiene
deshabilitado por la misma razón: corre cuando se actualiza un asset, no según
fechas.

**Assets** — la página en torno a la que gira este tutorial, y vacía hasta la
§9. En cuanto existan los Dags que los usan, lista cada asset con sus tasks
productoras, los Dags consumidores y el histórico de eventos, y dibuja el grafo
entre Dags: `dataops` → `labeled_projects` → `mlops` → `production_model`. La
lista de Dags no puede enseñar ese grafo, porque lo que conecta los Dags es un
asset, no una dependencia entre ellos; aparte de esta página, solo lo dibuja el
Graph de un Dag con **Options → Dependencies → All Dag Dependencies** (§10).
**Create Event** sobre un asset o bien lo materializa (ejecuta el Dag aguas
arriba, que produce los datos de verdad) o registra un evento manual, que
dispara los Dags aguas abajo sin ejecutar el productor.

**Browse** — Audit Log (quién disparó, pausó o limpió qué — tu trigger desde la
UI ya está ahí), **Deadlines** (deadline alerts pendientes y perdidas, §13), Jobs
(el scheduler, el triggerer y el Dag processor con sus heartbeats), XComs y
**Required Actions** (todas las tasks que esperan a una persona, en todos los
Dags).

**Admin** — Variables, Pools, Providers, Plugins, Connections y la **Config**
renderizada de Airflow. Connections y Variables son las dos que vas a usar:
nunca metas una credencial en un archivo de Dag.

**Docs** — la documentación de Airflow, el repo de GitHub, la referencia de la
REST API que sirve este api-server, y la versión de Airflow que estás usando,
enlazada a su propia documentación.

**El reloj** — su etiqueta es el desfase UTC de la zona horaria en la que se
muestran todas las fechas de la UI. Pasa el ratón por encima para ver la hora
actual en esa zona; haz clic para elegir otra zona horaria.

**User** — idioma, modo claro/oscuro, si un Dag se abre en Grid o en Graph, un
generador de tokens de la API y el logout.

> Hay dos menús que pueden aparecer y que este stack no muestra. **Security**
> viene con un auth manager que tiene usuarios y roles que gestionar; este
> stack usa el Simple auth manager con todo el mundo como admin. **Plugins**
> aparece cuando un plugin instalado añade páginas a la navegación; el único
> plugin de este stack, OpenLineage, no añade ninguna.

---

← [4. Tu primer Dag](04-tu-primer-dag.md) · [Índice](README.md) · [6. Operators, XComs y relaciones](06-operators-xcoms-y-relaciones.md) →
