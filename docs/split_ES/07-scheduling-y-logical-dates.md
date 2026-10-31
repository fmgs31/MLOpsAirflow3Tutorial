## 7. Scheduling y logical dates

Un run programado tiene varias fechas, y cada una responde a una pregunta
distinta. Se distinguen mejor en un Dag que cada noche, a medianoche, procesa
los datos del día anterior. Su `start_date` es 2025-01-01, y este es su run del
10 de septiembre de 2026:

| Fecha | Pregunta que responde | En el ejemplo |
|---|---|---|
| `logical_date` | ¿Qué run es este? La fecha de "el run del 10 de septiembre" | 2026-09-10 00:00 |
| `data_interval_start` | ¿Desde cuándo son los datos que procesa? | 2026-09-10 00:00 |
| `data_interval_end` | ¿Hasta cuándo son los datos que procesa? | 2026-09-11 00:00 |
| `run_after` | ¿Desde cuándo puede arrancar? | 2026-09-11 00:00 |
| `start_date` del run | ¿Cuándo arrancó de verdad? | unos segundos después de `run_after`, u horas después si el Dag estaba en pausa u ocupado |
| `start_date` del Dag | ¿Desde qué fecha programa runs el Scheduler? | 2025-01-01 00:00: ninguna franja anterior tiene run |

El run *del* día 10 arranca el día 11: no puede procesar los datos de un día
hasta que ese día ha terminado. Sus tasks saben qué datos procesar por
`data_interval_start` y `data_interval_end`, y la logical date le da nombre al
run: es el principio de su intervalo. `ds`, `ts` y el `run_id` son esa misma
logical date escrita como texto: `2026-09-10`, `2026-09-10T00:00:00+00:00`,
`scheduled__2026-09-10T00:00:00+00:00`.

Las dos `start_date` solo comparten el nombre. La del run la registra Airflow
cuando ese run arranca, y es distinta en cada run. La del Dag es un argumento
que escribes tú (§4), es la misma para todos los runs y fija desde cuándo hay
runs; se explica más abajo.

Con un string cron normal, como el `"0 3 * * *"` que usa `dataops`, no hay un
periodo que esperar: el run es "el run de las 03:00", así que las cuatro
primeras fechas de la tabla son las 03:00, y solo la `start_date` del run puede
ser posterior. Por eso normalmente parecen una sola fecha.

Los dos Dags que has escrito hasta ahora tienen `schedule=None`. Los otros
valores:

```python
schedule=None                       # manual / API only
schedule="0 3 * * *"                # cron
schedule=timedelta(days=1)          # every 24 hours
schedule=[some_asset]               # data-aware (§8)
schedule=CronTriggerTimetable(...)  # a Timetable, named directly
```

Cada una de las cuatro primeras es un atajo para un **Timetable**, el objeto que
se encarga de las dos decisiones que toma un schedule; la última línea usa uno
directamente, y escribir tu propia clase Timetable es la forma de conseguir una
cadencia que cron no puede expresar.

Las dos decisiones: *cuándo* crear un run, y a qué franja de tiempo corresponde
ese run — su **data interval**. Cómo se delimita la franja depende del
timetable, y hay dos tipos:

- **Un string cron o un `timedelta`** — `"0 3 * * *"`, el schedule que le darás
  a `dataops` en la §9, o `timedelta(days=1)` — se dispara en un instante
  concreto, y la franja *es* ese instante: `logical_date`,
  `data_interval_start`, `data_interval_end` y `run_after` son el mismo
  momento, las 03:00 en el caso del string cron.
- **Un timetable de data interval** — `CronDataIntervalTimetable("0 0 * * *",
  timezone="UTC")`, importado de `airflow.sdk` — delimita periodos reales. Es
  el que hay detrás de la tabla del principio de la §7. Usa este tipo cuando
  una task tenga que procesar "los datos de ayer", porque el intervalo le dice
  qué día es ese.

La `start_date` del Dag marca dónde empiezan las franjas: nunca se programa un
run para una franja anterior. Solo afecta a los runs que crea el Scheduler; un
trigger manual funciona aunque la `start_date` esté en el futuro.

También es opcional, y sin ella no hay pasado que recuperar. Al quitar la pausa,
un schedule cron espera a su siguiente hora, salvo que la última haya pasado hace
muy poco: menos de una décima parte del intervalo, con un mínimo de cinco
minutos (2,4 horas en un schedule diario). Un schedule `timedelta` arranca en
cuanto le quitas la pausa.

Tres cosas sobre scheduling que te ahorrarán una tarde de depuración:

**`catchup` es `False` por defecto** (`[scheduler] catchup_by_default`).
Quitar la pausa a un Dag con cron y un `start_date` de 2025 no crea un run por
cada franja desde entonces. Crea **uno**, para la franja más reciente que el
cron ya ha dejado atrás (la de hoy si ahora es más tarde de las 03:00, la de
ayer si es más temprano), y lo crea inmediatamente, porque esa franja ya
debería haberse ejecutado. A partir de ahí, uno por franja a medida que les
llega la hora. Lo verás en la §10: quitar la pausa a `dataops` basta para
arrancarlo. Si *quieres* el histórico, pídelo con `catchup=True` — o haz un
backfill solo del rango que necesitas (§9).

**Un string cron se dispara a la hora que indica.** `schedule="0 3 * * *"` se
ejecuta a las 03:00 y la `logical_date` del run es las 03:00. Un timetable de
data interval pone la `logical_date` un periodo entero antes del run, y por eso
merece la pena mirar la pestaña **Calendar** del Dag y la columna **Next Run**
de la lista de Dags después de escribir uno.

**`logical_date` puede estar vacía.** Es la fecha que da nombre al run, y en un
run programado sale de su franja. Un run disparado por un asset no corresponde
a ninguna franja y no tiene logical date, y tampoco la tiene un run disparado
desde la CLI o la REST API sin indicarla. El formulario de Trigger de la UI es
la excepción: rellena la logical date con la hora actual, así que un run
disparado desde el formulario tiene una salvo que vacíes el campo. En la §10,
`mlops` tiene un run de cada tipo, el manual disparado desde el formulario; su
lista de runs, con solo las columnas que importan aquí, muestra el run
disparado por el asset con la `logical_date` vacía:

```bash
$ docker compose exec airflow-scheduler airflow dags list-runs mlops -o plain
dag_id  run_id                                      state    run_after             logical_date
mlops   asset_triggered__2026-09-09T08:23:16+00:00  success  2026-09-09T08:23:16
mlops   manual__2026-09-09T08:22:31+00:00           success  2026-09-09T08:22:32   2026-09-09T08:22:31
```

Usa `run_after` cuando quieras decir "cuándo le tocaba arrancar a este run":
todo run lo tiene, con logical date o sin ella.

> **La trampa, antes de disparar nada:** un run disparado desde la CLI o la
> REST API **no tiene logical date** salvo que se la des, así que
> `logical_date` falta en el task context y `ds`, `ts` y compañía están
> *undefined* en las plantillas. Los dos accesos fallan en tiempo de
> ejecución, y ninguno falla bajo `airflow dags test`, que siempre aporta una
> logical date:
>
> ```python
> context["logical_date"]          # KeyError when the run has none
> context.get("logical_date")      # None — fine
> "{{ ds }}"                       # UndefinedError: 'ds' is undefined
> "{{ ds | default('none') }}"     # fine
> "{{ dag_run.run_after }}"        # fine — every run has one
> ```
>
> Escribiste la forma segura de ambas en la §4 y la §6, y por eso los runs
> que vas a disparar desde la CLI tienen éxito. Los runs disparados por asset
> tampoco tienen logical date, y por eso `mlops` (§10), que corre por asset, no
> lee `logical_date` ni `ds` en ninguna parte.

### Runs manuales

Un schedule no es la única forma de arrancar un Dag. `airflow dags trigger` crea
un run en el momento, sin esperar a ningún schedule. Pruébalo con
`hello_airflow3`; antes necesita `unpause`, por la razón que se explica después
de las opciones de `trigger`:

```bash
docker compose exec airflow-scheduler airflow dags unpause hello_airflow3
docker compose exec airflow-scheduler airflow dags trigger hello_airflow3 -c '{"greeting": "hola"}' -r manual-demo-1
```

- **`-c/--conf`** es un objeto JSON. Acaba en `dag_run.conf`, y cualquier
  clave que coincida con un `Param` declarado lo sobrescribe para ese run.
- **`-r/--run-id`** nombra el run. Sin él obtienes `manual__<timestamp>`.
- **`-l/--logical-date`** fija la logical date. Sin ella el run no tiene
  ninguna — la trampa descrita en *`logical_date` puede estar vacía*.
- En la UI, el equivalente es el botón **Trigger**, con el formulario generado
  a partir de tus `params` — lo usaste en la §5.

Pausar un Dag tiene dos efectos: el Scheduler no le crea runs programados, y no
arranca ninguno de sus runs, ni siquiera los que creas a mano. El primero es el
que explica *`catchup` es `False` por defecto*. El segundo es la razón de que
`hello_airflow3`, que no tiene schedule, necesite `unpause` igualmente. Un Dag
nuevo aparece **en pausa** (`[core] dags_are_paused_at_creation`), así que
`unpause` no es opcional — y es el paso que todo el mundo olvida:

**Un Dag en pausa acepta un trigger igualmente — el run se crea y luego se queda
en `queued` hasta que le quitas la pausa.** No falla nada, no avisa nada:

```bash
docker compose exec airflow-scheduler airflow dags pause hello_airflow3
docker compose exec airflow-scheduler airflow dags trigger hello_airflow3 -r demo
docker compose exec airflow-scheduler airflow dags list-runs hello_airflow3 -o plain
```

```
dag_id          run_id  state   run_after                         logical_date  start_date  end_date
hello_airflow3  demo    queued  2026-09-09T12:19:29.483234+00:00                                       ← atascado
```

```bash
docker compose exec airflow-scheduler airflow dags unpause hello_airflow3     # ahora arranca
```

**Comprobación.** Ejecuta esos cuatro comandos tú mismo contra
`hello_airflow3`. Mientras el Dag está en pausa el run está `queued` y ahí se
queda por mucho que esperes: el Scheduler lo ve en cada iteración del bucle y
lo omite a propósito. Ejecuta `list-runs` otra vez poco después del `unpause`:
muestra el mismo `run_id` como `success`, y su `start_date` (la del run, el
momento en que arrancó, no la del Dag) es posterior a su `run_after`: arrancó
cuando quitaste la pausa, no cuando lo disparaste. No se perdió nada y nada te
avisó; el run quedó retenido.

El formulario de trigger de la UI tiene una casilla *"Unpause … on trigger"*
para esto.

Además de `airflow dags trigger` y el botón **Trigger** de la UI, hay dos formas
más de arrancar un run. La REST API hace lo mismo que `trigger`, por HTTP —
`POST /api/v2/dags/{dag_id}/dagRuns`, que es lo que llama el botón de la UI.
`airflow dags test <dag_id>` es distinto: ejecuta el Dag entero dentro del
propio comando, sin pasar por el Scheduler (§4). Por eso la pausa no lo retiene,
y por eso funcionó en la §4 con `hello_airflow3` todavía en pausa. Usa `test`
mientras escribes un Dag, y `trigger` para ejecutarlo como lo hará el Scheduler.

---

← [6. Operators, XComs y relaciones](06-operators-xcoms-y-relaciones.md) · [Índice](README.md) · [8. Assets: programar por datos, no por tiempo](08-assets-programar-por-datos-no-por-tiempo.md) →
