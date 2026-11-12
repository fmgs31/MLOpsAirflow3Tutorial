## 5. La UI

La UI está en <http://localhost:8080>.

La navegación es **Home · Dags · Assets · Browse · Admin** arriba y, abajo,
**Docs**, un reloj y **User**.

**Home** — salud del Scheduler, el Triggerer y el Dag processor, errores de
import, runs y tasks fallidos, asset events recientes, deadlines y slots de
pools. Ábrela primero cuando algo no arranca.

**Dags** — la lista, como tarjetas o como tabla, filtrable por tag, owner o
estado, con el toggle de pausa y un botón **Trigger** en cada fila. La tuya
muestra `hello_airflow3`, su owner, sus dos tags y el único run que hizo
`airflow dags test`. Un Dag con cron muestra ahí su schedule, uno programado
con assets su condición de asset; el tuyo, con `schedule=None`, ninguna de las
dos. Un Dag que no aparece aquí suele haber fallado al importarse: la insignia
roja junto al número de Dags cuenta los fallos — haz clic en ella para ver los
tracebacks.

**La página de un Dag** — el Dag a la izquierda y las pestañas de detalle a la
derecha. El panel izquierdo tiene dos vistas, que se eligen con los iconos de
su esquina superior izquierda o se alternan con la tecla `g`: **Grid**, el
histórico de runs (una columna por run, una fila por task, cada celda coloreada
según su estado), y **Graph**, las tasks y las aristas entre ellas. En el otro
extremo de esa fila:

- **Options** (el engranaje). En el Graph: la versión del Dag y el run a
  mostrar, la dirección del grafo y **Dependencies** — solo las tasks del
  propio Dag (el valor por defecto) o también los assets y Dags conectados con
  él. `hello_airflow3` no está conectado con nada, así que las tres opciones se
  ven igual; la §10 enseña la diferencia en `mlops`. En el Grid: cuántos runs
  mostrar, e indicadores de versión que marcan dónde cambió el código entre
  runs.
- **Filter** (un embudo): eliges una task y se muestra solo lo que está aguas
  arriba o aguas abajo de ella.
- **Task Filters** (una lupa, solo en el Graph): filtra los nodos por estado,
  task group o duración. En `hello_airflow3` aparece en cuanto abres un run.

Las pestañas de la derecha dependen de lo que selecciones:

- **Nada** — las pestañas del propio Dag: Overview, Runs, Tasks, Calendar,
  Audit Log, Code y Details, más **Backfills** en un Dag con schedule.
- **Una task**, con clic en el Graph sin run seleccionado — esa task en todos
  sus runs: Overview, Task Instances, Audit Log.
- **Un run**, con clic en la pestaña **Runs** o en una barra del Grid — Task
  Instances, Asset Events, Audit Log, Code, Details. El panel izquierdo gana una
  tercera vista, el icono entre Grid y Graph cuyo tooltip dice *Show Gantt*: el
  Grid con una línea de tiempo de las tasks de ese run al lado.
- **Una task dentro de un run** — esa task instance: **Logs**, **Rendered
  Templates**, **Storage** (que contiene su **XCom**), Asset Events, Audit Log,
  Code, Details. Una mapped task añade **Task Instances**, una fila por copia
  mapeada, y una task que espera a una persona añade **Required Actions**.

**Hazlo ahora.** Abre `hello_airflow3` y mira el Graph: `make_greeting` se abre
hacia `shout` y `count_words`, y las dos vuelven a juntarse en `report`.

**El selector de versión del Dag.** Cada vez que guardas un cambio en un
archivo de Dag, Airflow crea una versión nueva — `v1`, `v2`, … — y cada Dag run
registra la versión con la que corrió. El selector, en **Options** del Graph y
en la pestaña **Code**, muestra cada versión con su fecha de creación y te
enseña un run con el código que lo produjo: el run de la semana pasada sigue
mostrando el grafo de la semana pasada. **Code** enseña además cuándo se parseó
el archivo y, en cuanto hay dos versiones, un botón para compararlas.

Esas versiones salen del *bundle* del que se cargó el Dag: el sitio del que el
Dag processor lee archivos. Este stack tiene uno, la carpeta `dags/`, llamado
`dags-folder` — la columna `bundle_name` de `airflow dags list` en la §4. Una
carpeta local no tiene versión propia; un bundle de Git sí (el commit), y
**Code** la muestra también (§16).

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
los mismos valores como **Configuration JSON**; el formulario ofrece además un
run id, una logical date y un data interval manual. Si el Dag está en pausa,
una casilla *"Unpause … on trigger"*, marcada por defecto, le quita la pausa al
crear el run; desmárcala y el run se queda **queued** hasta que quites la pausa
(§7).

Escribe otra palabra suelta en `greeting` — deja *Unpause* marcada si aparece —
y envía.

Ahora mira el resultado. En la pestaña **Runs**, haz clic en el run nuevo,
luego en `report` en el grafo, y abre **Logs**: tu palabra en mayúsculas y
`(7 words)`, la línea que `airflow dags test` imprimió en tu terminal en la §4.
Haz clic en `make_greeting`, abre **Storage** y luego **XCom**: el string del
saludo, bajo la clave `return_value` — el valor que recibieron `shout` y
`count_words`.

> El run que hizo `airflow dags test` no tiene logs aquí. Ese comando imprime
> la salida de las tasks en tu terminal y no escribe ningún archivo de log, así
> que su pestaña **Logs** dice que no encontró el archivo de log y sugiere
> configurar remote logging. No hay nada roto.

El segundo modo del formulario de Trigger, **Backfill** — *"Run this Dag for a
range of dates"* —, aparece deshabilitado en `hello_airflow3`; al pasar el
ratón por encima dice *"This Dag's schedule does not support backfills"*. Un
backfill le pregunta al schedule qué runs tendrían que haber ocurrido entre dos
fechas y los crea: un schedule diario sobre cuatro días responde "cuatro runs,
uno por día". `schedule=None` no tiene respuesta: el Dag solo corre cuando
alguien lo dispara, así que a ningún rango de fechas le falta un run. `mlops`
(§10) lo tiene deshabilitado por la misma razón: corre cuando se actualiza un
asset, no según fechas. Backfill se puede usar a partir de la §9, con
`dataops`, que corre a diario: un rango de fechas, un **Reprocess Behavior**
(missing runs / missing and errored runs / all runs), max active runs, "run
backwards", y un dry run que te dice *"N runs will be triggered"* antes de
confirmar.

**Assets** — la página en torno a la que gira este tutorial, vacía hasta la
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
(el Scheduler, el Triggerer y el Dag processor con sus heartbeats), XComs y
**Required Actions** (todas las tasks que esperan a una persona, en todos los
Dags).

**Admin** — Variables, Pools, Providers, Plugins, Connections y la **Config**
renderizada de Airflow. Connections y Variables son las dos que vas a usar:
nunca metas una credencial en un archivo de Dag.

**Docs** — la documentación de Airflow, el repo de GitHub, la referencia de la
REST API que sirve este api-server, y la versión de Airflow en uso, enlazada a
su documentación.

**El reloj** — su etiqueta es el desfase UTC de la zona horaria en la que se
muestran las fechas de la UI. Pasa el ratón por encima para ver la hora actual
en esa zona; haz clic para elegir otra.

**User** — idioma, modo claro/oscuro, si un Dag se abre en Grid o en Graph, un
generador de tokens de la API y el logout.

---

← [4. Tu primer Dag](04-tu-primer-dag.md) · [Índice](README.md) · [6. Operators, XComs y relaciones](06-operators-xcoms-y-relaciones.md) →
