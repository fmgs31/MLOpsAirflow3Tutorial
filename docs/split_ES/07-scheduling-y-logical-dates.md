## 7. Scheduling y logical dates

Los dos Dags que llevas tienen `schedule=None`. Los otros valores:

```python
schedule=None                       # manual / API only
schedule="0 3 * * *"                # cron
schedule=timedelta(days=1)          # every 24 hours
schedule=[some_asset]               # data-aware (§8)
schedule=CronTriggerTimetable(...)  # a Timetable, named directly
```

Cada una de las cuatro primeras es un atajo para un **Timetable**, el objeto que
se encarga de las dos decisiones que toma un schedule; la última línea nombra uno
directamente, y escribir tu propia clase Timetable es como consigues una
cadencia que cron no sabe expresar.

Las dos decisiones: *cuándo* crear un run, y para qué trozo de tiempo es ese run
— su **data interval**, expuesto a todas las tasks como `data_interval_start` y
`data_interval_end`, y etiquetado por la `logical_date` del run. Ninguno de esos
es "cuándo corrió"; eso es `run_after`. Cómo se corta el trozo depende del
timetable, y hay dos clases:

- **Un string cron o un `timedelta`** — `"0 3 * * *"`, el schedule que le darás
  a `dataops` en la §9, o `timedelta(days=1)` — dispara en un momento, y el
  trozo *es* ese momento: `logical_date`, `data_interval_start` y
  `data_interval_end` son el mismo instante, las 03:00 en el caso del string
  cron.
- **Un timetable de data interval** — `CronDataIntervalTimetable("0 0 * * *",
  timezone="UTC")`, importado de `airflow.sdk` — corta lapsos de verdad. El run
  *para* 2026-09-10 se crea una vez que ese día ha terminado, a las 2026-09-11
  00:00; su data interval va de 2026-09-10 00:00 a 2026-09-11 00:00, y su
  `logical_date` es 2026-09-10 00:00: el principio del lapso, un día antes de
  que ocurra el run. Usa esta clase cuando una task tenga que procesar "los
  datos de ayer", porque el intervalo le dice exactamente qué día es ese.

`start_date` es donde empiezan los trozos: nunca se programa un run antes de él.
Es opcional — sin él, un Dag con schedule empieza a partir del momento en que le
quitas la pausa — y solo rige los runs que crea el scheduler; a un trigger
manual nunca lo frena.

Ten presente esa imagen: el resto de esta sección se deduce de ella. Tres cosas
sobre scheduling que te ahorrarán una tarde:

**`catchup` es `False` por defecto** (`[scheduler] catchup_by_default`), y
conviene saber exactamente qué significa eso al quitar la pausa a un Dag. Quitar la pausa a un Dag con cron y un
`start_date` de 2025 no crea un run por cada trozo desde entonces. Crea **uno**,
para el trozo más reciente que el cron ya ha dejado atrás — las 03:00 de hoy si
ya han pasado, las de ayer si no — y lo crea inmediatamente, porque ese trozo
está vencido. A partir de ahí, uno por trozo según van tocando. Lo verás
ocurrir en la §10: quitar la pausa a `dataops` basta para arrancarlo. Si
*quieres* el histórico, pídelo con `catchup=True` — o haz un backfill solo del
rango que necesitas (§9).

**Un string cron dispara a la hora que dice.** `schedule="0 3 * * *"` corre a
las 03:00 y la `logical_date` del run es las 03:00. Un timetable de data
interval pone la `logical_date` un lapso entero antes del run, y por eso merece
la pena mirar la pestaña **Calendar** del Dag y la columna **Next Run** de la
lista de Dags después de escribir uno.

**`logical_date` puede estar vacía.** Etiqueta el trozo para el que es un run
programado. Un run disparado por asset no es para ningún trozo y no tiene
ninguna, y tampoco la tiene un run disparado desde la CLI o la REST API sin
indicarla. El formulario de Trigger de la UI es la excepción: rellena la logical
date con la hora actual, así que un run disparado desde el formulario tiene una
salvo que vacíes el campo. Así se verá la lista de runs de `mlops` en la §10, un
run de cada clase — el manual, disparado desde el formulario — recortada a las
columnas que importan aquí:

```bash
$ docker compose exec airflow-scheduler airflow dags list-runs mlops -o plain
dag_id  run_id                                      state    run_after             logical_date
mlops   asset_triggered__2026-09-09T08:23:16+00:00  success  2026-09-09T08:23:16              ← ninguna
mlops   manual__2026-09-09T08:22:31+00:00           success  2026-09-09T08:22:32   2026-09-09T08:22:31
```

Usa `run_after` cuando quieras decir "cuándo arrancó esto de verdad". Claves del
contexto disponibles en toda task: `ds`, `ts`, `logical_date`,
`data_interval_start`, `data_interval_end`, `run_id`, `dag_run`, `ti`, `params`,
`var`, `conn`, `macros`, `inlets`/`outlets`, `triggering_asset_events`.

> **La trampa que conviene conocer antes de disparar nada:** un run disparado
> desde la CLI o la REST API **no tiene logical date** salvo que se la des, así que `logical_date` falta en el task context y `ds`,
> `ts` y compañía están *undefined* en las plantillas. Ambas fallan en tiempo de
> ejecución, y ninguna falla bajo `airflow dags test`, que siempre aporta una
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
> que vas a disparar tienen éxito. Las dos fueron bugs reales en los Dags
> de este mismo tutorial hasta que un trigger manual los encontró. Los runs
> disparados por asset tampoco tienen logical date, así que `mlops` está expuesto
> a lo mismo.

### Runs manuales

Un schedule no es la única forma de arrancar un Dag. Dos comandos cubren casi
todo, y `hello_airflow3` es el Dag con el que probarlos:

```bash
docker compose exec airflow-scheduler airflow dags unpause hello_airflow3
docker compose exec airflow-scheduler airflow dags trigger hello_airflow3 -c '{"greeting": "hola"}' -r manual-demo-1
```

- **`-c/--conf`** es un objeto JSON. Acaba en `dag_run.conf`, y cualquier
  clave que coincida con un `Param` declarado lo sobrescribe para ese run.
- **`-r/--run-id`** nombra el run. Sin él obtienes `manual__<timestamp>`.
- **`-l/--logical-date`** fija una. Sin ella el run no tiene ninguna — la
  trampa bajo *`logical_date` puede estar vacía*.
- En la UI lo mismo es el botón **Trigger**, con el formulario generado a partir
  de tus `params` — lo usaste en la §5.

Un Dag nuevo llega **en pausa** (`[core] dags_are_paused_at_creation`), así que
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

**Comprobación.** Ejecuta esos cuatro comandos tú mismo contra `hello_airflow3`.
Mientras el Dag está en pausa el run está `queued` y ahí se queda por mucho que
esperes — el scheduler lo ve en cada bucle y lo salta deliberadamente. Ejecuta
`list-runs` otra vez poco después del `unpause`: muestra el mismo `run_id` como
`success`, y su
`start_date` — la del run, el momento en que arrancó de verdad, no la
`start_date` del Dag de antes en la §7 — es posterior a su `run_after`: arrancó
cuando quitaste la pausa, no cuando disparaste. No se perdió nada y nada te
avisó; simplemente estaba retenido.

El formulario de trigger de la UI tiene una casilla *"Unpause … on trigger"*
precisamente para esto.

Dos formas más de arrancar un run: `airflow dags test <dag_id>` ejecuta el Dag entero
in-process (§14), y la REST API lo hace por HTTP —
`POST /api/v2/dags/{dag_id}/dagRuns`, que es lo que llama el botón de la UI.

---

← [6. Operators, XComs y relaciones](06-operators-xcoms-y-relaciones.md) · [Índice](README.md) · [8. Assets: programar por datos, no por tiempo](08-assets-programar-por-datos-no-por-tiempo.md) →
