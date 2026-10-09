## 8. Assets: programar por datos, no por tiempo

Un **Asset** es un trozo de datos con nombre. Una task declara que lo produce con
`outlets=`, y cada vez que esa task tiene éxito Airflow registra un **asset
event**; un Dag declara que lo consume poniéndolo en `schedule=`, y cada evento
arranca un run suyo. Ese es todo
el mecanismo, y sustituye a cada cron del tipo "corre 30 minutos después del otro
Dag, y reza" que hayas visto en tu vida.

Los assets que usa este tutorial ya están definidos, en
[`src/tagifai/assets.py`](../../src/tagifai/assets.py), para que `dataops` y `mlops`
importen los mismos objetos:

```python
from airflow.sdk import Asset

LABELED_PROJECTS = Asset(
    name="labeled_projects",
    group="dataset",
    extra={"layer": "warehouse", "owner": "data-engineering"},
)
```

El lado productor es lo que escribirás en la §9 — fíjate en que el outlet va en
la task de **validación**, no en la de transform:

```python
@task(outlets=[LABELED_PROJECTS])
def validate_transformed(fp: str, *, outlet_events) -> dict:
    df = data.load_labeled(fp)
    result = validate.run_suite(df, "labeled_projects")
    if not result.passed:
        raise AirflowFailException(...)
    outlet_events[LABELED_PROJECTS].extra = {"rows": len(df), "path": fp}
    return {...}
```

`outlet_events` se inyecta por nombre, como `ti` en la §6, y permite a la task
adjuntar un dict `extra` al evento que está a punto de emitir.

Y el lado consumidor, §10, donde `triggering_asset_events` — también inyectado
por nombre — contiene los eventos que arrancaron el run:

```python
@dag(schedule=[LABELED_PROJECTS], ...)
def mlops():
    @task
    def prepare_data(*, triggering_asset_events):
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")
```

Esa es la idea en una línea: **los Dags aguas abajo se programan con "llegaron
datos buenos", nunca con "terminó una task" y nunca con una adivinanza de cron.**
El evento incluso lleva el recuento de filas que lo causó.

Más maquinaria de assets que conviene conocer:

| Funcionalidad | Qué hace |
|---|---|
| `AssetAny(a, b)` / `AssetAll(a, b)` | condiciones OR / AND en un `schedule` |
| `AssetAlias` | outlets cuya URI solo se conoce en runtime |
| `@asset` / `@asset.multi` | define asset + Dag + task en un solo decorador |
| `Metadata(asset, {...})` | adjunta metadatos haciendo yield desde la task |
| `AssetWatcher` + `BaseEventTrigger` | runs **event-driven** (§11) |
| `CronPartitionTimetable` | assets particionados |

Aquí los assets son **solo nombre**, lo que mantiene su identidad igual en Docker
y en runs nativos. En producción dales la URI real de los datos (`s3://…`,
`bq://…`) para que el lineage en la UI apunte a algún sitio.

Un asset existe a ojos de Airflow solo cuando un Dag parseado se refiere a él —
como outlet o como schedule. Ahora mismo `airflow assets list` imprime `No data
found`, y la página Assets está vacía; la §9 cambia eso en cuanto guardes un
archivo.

### Qué es en realidad un asset event

Es una fila. Cuando una task con `outlets=[…]` **tiene éxito**, Airflow registra
un asset event — qué asset, qué Dag run lo produjo, cuándo, y lo que hayas puesto
en `outlet_events[…].extra`. Eso es todo lo que es un "trigger". Una task que
falla no escribe evento, y en eso consiste toda la garantía de seguridad: a `mlops` no lo
puede arrancar un dato que no pasó la validación, porque la task que lo habría
anunciado nunca terminó.

El scheduler hace entonces la segunda mitad. En su siguiente bucle lee los asset
events nuevos y encola un Dag run por **cada Dag sin pausa** programado con ese
asset.

Los eventos que llegan más rápido de lo que el consumidor puede correr no se
pierden ni se ejecutan de uno en uno. `mlops` permite un único run activo
(`max_active_runs=1`), así que si terminan tres runs de `dataops` mientras está
ocupado, el *siguiente* run de `mlops` se lleva los tres eventos, y
`triggering_asset_events` en `prepare_data` los lista todos. Un consumidor se
pone al día en un run, no en tres.

> **Un consumidor en pausa no encola, y no se pone al día.** Las palabras *sin
> pausa* de arriba importan. Si `mlops` está en pausa cuando
> `dataops` emite, el asset event se escribe igualmente — lo verás en la página
> de Assets — pero no se encola ningún run para `mlops`, y **quitar
> la pausa después no lo reproduce**. Airflow filtra los Dags en pausa en el
> momento en que decide qué encolar, así que después ya no queda nada que
> encontrar. Lo mismo vale para un consumidor que todavía no existía cuando se
> escribió el evento: el run de `dataops` que harás en la §9 emitirá un evento
> que `mlops`, escrito en la §10, nunca verá. (Comprobado de las tres formas en
> este stack — ver [qué se verificó de verdad](../verified_ES.md).)
>
> Esto es lo contrario del comportamiento con Dags en pausa de la §7, donde un
> trigger *manual* crea el run primero y la pausa solo lo retiene. Un asset event
> no crea ningún run para un Dag en pausa, así que no hay nada que retener ni
> nada que arranque cuando quitas la pausa.

---

← [7. Scheduling y logical dates](07-scheduling-y-logical-dates.md) · [Índice](README.md) · [9. El Dag de DataOps](09-el-dag-de-dataops.md) →
