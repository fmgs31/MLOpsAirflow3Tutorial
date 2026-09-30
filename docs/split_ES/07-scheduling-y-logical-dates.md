## 7. Scheduling y logical dates

Los dos Dags que llevas tienen `schedule=None`. Los otros valores:

```python
schedule=None                       # manual / API only
schedule="0 3 * * *"                # cron
schedule=timedelta(days=1)          # interval
schedule=[some_asset]               # data-aware (§8)
schedule=CronTriggerTimetable(...)  # a Timetable, named directly
```

Cada una de las cuatro primeras es un atajo para un **Timetable**, el objeto que
posee las dos decisiones que toma un schedule; la última línea nombra uno
directamente, y escribir tu propia clase Timetable es como consigues una
cadencia que cron no sabe expresar.

Las dos decisiones: *cuándo* crear un run, y para qué trozo de tiempo es ese run
— su **data interval**, expuesto a todas las tasks como `data_interval_start` y
`data_interval_end`, y etiquetado por la `logical_date` del run. Ninguno de esos
es "cuándo corrió"; eso es `run_after`. Cómo se corta el trozo depende de la
forma de `schedule=`, y las dos formas comunes difieren:

- **Una cadena cron** — `"0 3 * * *"`, que es el schedule que le darás a
  `dataops` en la §9 — dispara en el momento que nombra, y el trozo *es* ese
  momento: `logical_date`, `data_interval_start` y `data_interval_end` son las
  tres las 03:00.
- **Un `timedelta`** — `timedelta(days=1)` — es un lapso. El run *para*
  2026-09-10 se crea una vez que ese día ha terminado, a las 2026-09-11 00:00, y
  su `logical_date` es 2026-09-10 00:00: el principio del lapso, un día antes de
  que ocurra el run. Usa esta forma cuando una task tenga que procesar "los datos
  de ayer", porque el intervalo le dice exactamente qué día es ese.

`start_date` es donde empiezan los trozos: nunca se programa un run antes de él.
Un archivo de Dag sin él parsea igual sin error, así que esto es algo que
recuerdas tú, no algo de lo que Airflow te avise.

Retén esa imagen y el resto de esta sección son consecuencias de ella. Tres cosas
sobre scheduling que te ahorrarán una tarde:

**`catchup` es `False` por defecto** (`[scheduler] catchup_by_default`), y lo
que hace al quitar la pausa es preciso. Quitar la pausa a un Dag con cron y un
`start_date` de 2025 no crea un run por cada trozo desde entonces. Crea **uno**,
para el trozo más reciente que el cron ya ha dejado atrás — las 03:00 de hoy si
ya han pasado, las de ayer si no — y lo crea inmediatamente, porque ese trozo
está vencido. A partir de ahí, uno por trozo según van tocando. Lo verás
ocurrir en la §10: quitar la pausa a `dataops` basta para arrancarlo. Si
*quieres* el histórico, pídelo con `catchup=True` — o backfillea solo el rango
que necesitas (§9).

**Una cadena cron dispara a la hora que dice.** `schedule="0 3 * * *"` corre a
las 03:00 y la `logical_date` del run es las 03:00. Los timetables son libres de
definir eso de otra forma, y por eso merece la pena mirar la pestaña **Calendar**
del Dag y la columna *"Next run"* después de escribir uno.

**`logical_date` es nullable.** Etiqueta el trozo para el que es un run
programado — y los runs manuales y disparados por asset, que no son para ningún
trozo, simplemente no tienen ninguna. Así se verá la lista de runs de `mlops` en
la §10, un run de cada clase:

```bash
$ airflow dags list-runs mlops -o plain
dag_id  run_id                                      state    run_after             logical_date
mlops   asset_triggered__2026-09-09T08:23:16+00:00  success  2026-09-09T08:23:16              ← ninguna
mlops   manual__2026-09-09T08:22:31+00:00           success  2026-09-09T08:22:32   2026-09-09T08:22:31
```

Usa `run_after` cuando quieras decir "cuándo arrancó esto de verdad". Claves del
contexto disponibles en toda task: `ds`, `ts`, `logical_date`,
`data_interval_start`, `data_interval_end`, `run_id`, `dag_run`, `ti`, `params`,
`var`, `conn`, `macros`, `inlets`/`outlets`, `triggering_asset_events`.

### Runs manuales

Un schedule no es la única forma de arrancar un Dag. Dos comandos cubren casi
todo, y `hello_airflow3` es el Dag con el que probarlos:

```bash
docker compose exec airflow-scheduler airflow dags unpause hello_airflow3
docker compose exec airflow-scheduler airflow dags trigger hello_airflow3 -c '{"greeting": "hola"}' -r manual-demo-1
```

- **`-c/--conf`** es un objeto JSON. Aterriza en `dag_run.conf`, y cualquier
  clave que coincida con un `Param` declarado lo sobrescribe para ese run.
- **`-r/--run-id`** nombra el run. Sin él obtienes `manual__<timestamp>`.
- **`-l/--logical-date`** fija una. Sin ella el run no tiene ninguna — ver la
  trampa de abajo.
- En la UI lo mismo es el botón **Trigger**, con el formulario generado a partir
  de tus `params` (§5) — lo pulsaste en la §5.

Un Dag nuevo llega **en pausa** (`[core] dags_are_paused_at_creation`), así que
`unpause` no es opcional — y es el paso que todo el mundo olvida:

**Un Dag en pausa acepta un trigger igualmente — el run se crea y luego se queda
en `queued` hasta que le quitas la pausa.** No falla nada, no avisa nada:

```console
$ airflow dags pause hello_airflow3 && airflow dags trigger hello_airflow3 -r demo
$ airflow dags list-runs hello_airflow3 -o plain
dag_id          run_id  state   run_after                         logical_date
hello_airflow3  demo    queued  2026-09-09T12:19:29.483234+00:00             ← atascado
$ airflow dags unpause hello_airflow3     # ahora arranca
```

**Comprobación.** Ejecuta esos cuatro comandos tú mismo contra `hello_airflow3`.
Mientras el Dag está en pausa el run está `queued` y ahí se queda por mucho que
esperes — el scheduler lo ve en cada bucle y lo salta deliberadamente. Poco
después del `unpause`, `list-runs` muestra el mismo `run_id` como `success`, y su
`start_date` es posterior a su `run_after`: arrancó cuando quitaste la pausa, no
cuando disparaste. No se perdió nada y nada te avisó; simplemente estaba
retenido.

El formulario de trigger de la UI tiene una casilla *"Unpause … on trigger"*
precisamente para esto.

> **La trampa que conviene conocer antes de disparar nada:** un run manual **no
> tiene logical date**, así que `logical_date` falta en el task context y `ds`,
> `ts` y compañía están *undefined* en las plantillas. Ambas fallan en tiempo de
> ejecución, y ninguna falla bajo `airflow dags test`, que siempre aporta una
> logical date:
>
> ```python
> context["logical_date"]          # KeyError on a manual run
> context.get("logical_date")      # None — fine
> "{{ ds }}"                       # UndefinedError: 'ds' is undefined
> "{{ ds | default('none') }}"     # fine
> "{{ dag_run.run_after }}"        # fine — every run has one
> ```
>
> Escribiste la forma segura de ambas en las lecciones 1 y 2, y por eso los runs
> que acabas de disparar tuvieron éxito. Las dos fueron bugs reales en los Dags
> de este mismo tutorial hasta que un trigger manual los encontró. Los runs
> disparados por asset tampoco tienen logical date, así que `mlops` está expuesto
> a lo mismo.

Dos vías más de entrada: `airflow dags test <dag_id>` ejecuta el Dag entero
in-process (§14), y la REST API lo hace por HTTP —
`POST /api/v2/dags/{dag_id}/dagRuns`, que es lo que llama el botón de la UI.

---

← [6. Operators, XComs y relaciones](06-operators-xcoms-y-relaciones.md) · [Índice](README.md) · [8. Assets: programar por datos, no por tiempo](08-assets-programar-por-datos-no-por-tiempo.md) →
