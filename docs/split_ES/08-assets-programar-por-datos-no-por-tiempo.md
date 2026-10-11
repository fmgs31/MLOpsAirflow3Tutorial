## 8. Assets: programar por datos, no por tiempo

Un **Asset** es un conjunto de datos con nombre. Una task declara que lo
produce poniéndolo en `outlets=` — los assets de esa lista son los **outlets**
de la task, y la task es su **productor** —, y cada vez que esa task tiene
éxito Airflow registra un **asset event**. Un Dag declara que lo consume
poniéndolo en `schedule=` — el Dag es el **consumidor** del asset —, y cada
evento nuevo arranca un run de ese Dag. Ese es todo el mecanismo, y sustituye a
cualquier cron del tipo "corre 30 minutos después del otro Dag, y cruza los
dedos" que hayas visto en tu vida.

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

`name` es lo que identifica al asset. `group` y `extra` son descripciones que
la UI enseña en la página del asset; este `extra` describe el asset en sí, no
un evento concreto.

En este tutorial, el productor de `labeled_projects` es `validate_transformed`,
la última de las cuatro tasks de `dataops` del diagrama de la §1, que escribirás
en la §9. El outlet va en esa task de **validación**, no en `transform`, la task
anterior, que es la que escribe los datos:

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
adjuntar un dict `extra` al evento que está a punto de emitir — un `extra`
distinto en cada evento, a diferencia del `extra` fijo del asset.
`AirflowFailException` hace fallar la task sin reintentos; la §9 lo explica.

Y el consumidor, `mlops`, que escribirás en la §10. Ahí
`triggering_asset_events` — también inyectado por nombre — contiene los eventos
que arrancaron el run:

```python
@dag(schedule=[LABELED_PROJECTS], ...)
def mlops():
    @task
    def prepare_data(*, triggering_asset_events):
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(f"triggered by {asset} extra={event.extra}")
```

Esa es la idea en una línea: **el consumidor se programa con "llegaron datos
buenos", no con un cron que adivina cuándo habrá terminado el Dag del
productor.**
El evento lo emite la task que validó los datos, así que "terminó con éxito"
significa justo eso.
El evento incluso lleva el número de filas que validó la task.

Más maquinaria de assets que conviene conocer:

| Funcionalidad | Qué hace |
|---|---|
| `AssetAny(a, b)` / `AssetAll(a, b)` | condiciones OR / AND en un `schedule` |
| `AssetAlias` | un outlet cuyo asset concreto decide la task mientras corre (por ejemplo, uno por fecha) |
| `@asset` / `@asset.multi` | define asset + Dag + task en un solo decorador |
| `Metadata(asset, {...})` | otra forma de poner el `extra` de un evento: la task hace `yield Metadata(...)` en vez de usar `outlet_events` |
| `AssetWatcher` + `BaseEventTrigger` | runs **event-driven** (§11) |
| `CronPartitionTimetable` | un cron que crea runs por partición: cada run lleva una clave (por ejemplo, una fecha) que indica qué parte de los datos le corresponde |

Aquí los assets **solo tienen nombre**. Un asset también puede tener una **URI**,
la dirección de sus datos reales (`s3://bucket/labeled_projects.csv`, por
ejemplo). Este tutorial no le pone ninguna porque la ruta de `data/` no es la
misma dentro del contenedor que en la instalación nativa de la §2. En
producción dale a cada asset la URI de sus datos, para que la UI diga dónde
están.

Para Airflow, un asset solo existe cuando un Dag parseado se refiere a él —
como outlet o como schedule. Ahora mismo `airflow assets list` imprime `No data
found`, y la página Assets está vacía; la §9 cambia eso en cuanto el Dag
processor parsee el primer archivo que guardes allí.

### Qué es en realidad un asset event

Es una fila. Cuando una task con `outlets=[…]` **tiene éxito**, Airflow registra
un asset event — qué asset, qué Dag run lo produjo, cuándo, y lo que hayas puesto
en `outlet_events[…].extra`. Esa fila es todo lo que hace falta para arrancar
al consumidor. Una task que
falla no escribe evento, y en eso consiste toda la garantía de seguridad: a `mlops` no lo
puede arrancar un dato que no pasó la validación, porque la task que lo habría
anunciado nunca terminó.

En ese mismo paso, Airflow añade una entrada a una cola por **cada Dag sin
pausa** programado con ese asset. El scheduler hace entonces la segunda mitad:
en su siguiente bucle convierte esas entradas en Dag runs.

Los eventos que llegan más rápido de lo que el consumidor puede procesarlos no
se pierden ni se procesan de uno en uno. `mlops` permite un único run activo
(`max_active_runs=1`), así que si terminan tres runs de `dataops` mientras está
ocupado, el *siguiente* run de `mlops` se lleva los tres eventos, y
`triggering_asset_events` en `prepare_data` los lista todos. Un consumidor se
pone al día en un run, no en tres.

> **Un consumidor en pausa no encola, y no se pone al día.** Las palabras *sin
> pausa* de "cada Dag sin pausa" importan. Si `mlops` está en pausa cuando
> `dataops` emite, el asset event se escribe igualmente — lo verás en la página
> de Assets — pero no se encola ningún run para `mlops`, y **quitar
> la pausa después no lo recupera**. Airflow filtra los Dags en pausa en el
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
