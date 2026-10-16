## 14. Cuando los datos fallan

Hasta ahora todos los runs pasaron, porque todos leyeron los mismos archivos
limpios. Los datos reales llegan por partes, tarde, repetidos y con otro
formato, y entonces el trabajo ya no es escribir el pipeline sino responderle
cuando un control falla. Esta sección carga un segundo lote de proyectos en
tres entregas, cada una con un problema que tienen las cargas reales, y
resuelve cada una: dónde aparece, cómo encontrar las filas que lo causan,
dónde va el arreglo y cuánto hay que volver a ejecutar. Después, el mismo lote
dispara el monitor de drift de la §12.

| Entrega | Qué llega | Qué está mal | Dónde aparece |
|---|---|---|---|
| Día 1 | 191 proyectos nuevos | sus tags todavía no llegaron | `transform`: falla un test de dbt |
| Día 2 | los tags, y otra vez los proyectos | cuatro proyectos viejos reenviados con ids nuevos | `validate_projects`: falla una suite de GX |
| Día 3 | los proyectos, corregidos | las fechas de los proyectos nuevos vienen en otro formato | `transform`: el model no se puede construir, y ningún control lo vio venir |
| — | (los mismos datos) | desde 2021 los tags usan otro vocabulario | `monitoring`: drift |

### El lote

Los archivos están en [`datasets/batch_2/`](../../datasets/batch_2/). Los 191
proyectos son reales: el holdout set de Made With ML, proyectos del mismo
sitio que no están en `datasets/projects.csv`. Sus fechas se movieron un año
hacia adelante, para que sean las filas más nuevas del warehouse, y los tres
primeros problemas de la tabla se añadieron encima. El cuarto no se añadió:
Made With ML publicó estos tags con los tres más raros ya fusionados en
`other`. [`scripts/make_batch_2.py`](../../scripts/make_batch_2.py) genera los
archivos, y [`datasets/README.md`](../../datasets/README.md) detalla qué trae
cada uno.

Cada sync de la §9 es **Full Refresh Overwrite**, así que cada archivo es la
tabla entera tal como la exportaría el origen ese día: las 764 filas
originales y después las nuevas. Una entrega es un cambio de URL en una source
de Airbyte: en Airbyte, abre **Sources**, elige la source, reemplaza su
**URL** y guarda.

### Antes de empezar

Pausa `mlops`:

```bash
docker compose exec airflow-scheduler airflow dags pause mlops
```

Un run de `dataops` que pasa emite el asset event que arranca `mlops` (§8).
Aquí eso entrenaría un modelo con los proyectos nuevos antes de que
`monitoring` los compare con lo que conoce el modelo en producción, y esa
comparación es de lo que trata "La alarma de drift". `dataops` sigue sin
pausa, como lo dejó la §10.

### Día 1: proyectos sin tags

Cambia la URL de la source `projects` a:

```
https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/batch_2/day1_projects.csv
```

Deja `tags` como está y arranca un run:

```bash
docker compose exec airflow-scheduler airflow dags trigger dataops
```

**Comprobación.** En la UI, el run falla en `transform` después de unos
minutos. Los dos syncs y las dos tasks `validate_*` están en verde,
`transform` en rojo y `validate_transformed` en `upstream_failed`.

**Lee el fallo.** Abre el log de `transform`. Cerca del final:

```
FAIL 191 not_null_labeled_projects_tag
...
Got 191 results, configured to fail if != 0
Done. PASS=5 WARN=0 ERROR=1 SKIP=0 NO-OP=0 REUSED=0 TOTAL=6
```

Un test de dbt es una consulta que devuelve las filas que rompen la regla, y
falla si vuelve alguna. dbt escribe cada una, compilada, en
`dbt/dbt_transforms/target/compiled/dbt_transforms/models/labeled_projects/schema.yml/not_null_labeled_projects_tag.sql`;
esta selecciona `tag` donde es nulo. Para ver *qué* proyectos son, pide más
columnas. En la consola de BigQuery (§9):

```sql
SELECT id, created_on, title
FROM `<project id>.mlops_course.labeled_projects`
WHERE tag IS NULL
ORDER BY created_on
```

191 filas, todas de 2021: el lote nuevo. Y `tags` no creció:

```sql
SELECT COUNT(*) AS n FROM `<project id>.mlops_course.tags`
```

`n` es 764. Llegaron los proyectos y no sus etiquetas: el etiquetado viene
después de que se envía un proyecto, y va atrasado. No hay nada roto.

**Dónde va el arreglo: en ningún lado, todavía.** Volver a ejecutar da el
mismo fallo, porque los datos son los mismos. Relajar el test dejaría llegar
proyectos sin etiqueta al entrenamiento, donde `preprocess` los descarta sin
avisar. Hay que esperar las etiquetas.

El fallo muestra dos cosas más. Primero, `labeled_projects` ya tiene las 191
filas sin tag: `dbt build` escribe la tabla y después la testea. Lo que las
mantuvo lejos del entrenamiento es el gate que viene después:
`validate_transformed` no corrió, así que no se emitió ningún asset event.
Segundo, la espera: `transform` corrió tres veces, con un minuto entre cada
una, porque `dataops` da `retries=2` a todas sus tasks. Los reintentos sirven
cuando la causa se va sola, como un error de red o una API ocupada; un
problema de datos falla igual cada vez. Por eso los gates de GX lanzan
`AirflowFailException`, que se salta los reintentos (§9). `transform` es un
`BashOperator` que ejecuta dbt, así que no distingue un test fallido de una
conexión perdida, y reintenta los dos.

### Día 2: los mismos proyectos dos veces

Llegan las etiquetas. Cambia las dos URLs, `projects` a:

```
https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/batch_2/day2_projects.csv
```

y `tags` a:

```
https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/batch_2/day2_tags.csv
```

Es una entrega nueva, así que le toca un run nuevo:

```bash
docker compose exec airflow-scheduler airflow dags trigger dataops
```

**Comprobación.** Esta vez el run falla en menos de un minuto, en
`validate_projects`, sin reintentos. `validate_tags` está en verde, y
`transform` y `validate_transformed` en `upstream_failed`.

**Lee el fallo.** El log de `validate_projects` termina con la expectation que
falló y algunas de las filas que la rompieron:

```
projects failed validation: [{'expectation': 'expect_compound_columns_to_be_unique:title,description', ...
'details': "[{'title': 'Coloring Greyscale Images', ...
```

Dos proyectos con el mismo título y la misma descripción. Encuéntralos todos:

```sql
SELECT title, ARRAY_AGG(id ORDER BY id) AS ids, ARRAY_AGG(created_on ORDER BY id) AS created
FROM `<project id>.mlops_course.projects`
GROUP BY title, description
HAVING COUNT(*) > 1
```

Cuatro filas. Cada una junta un proyecto de 2020 con una copia bajo un id
nuevo, del 2438 al 2441, las cuatro con fecha `2021-10-06 09:00:00`. La
exportación volvió a mandar cuatro proyectos viejos como si fueran nuevos.

**Dónde va el arreglo: en el origen.** El bug está en el origen, y quien lo
administra es quien puede decir cuál de los ids es el real. Repórtalo, con la
consulta. Borrar las copias en BigQuery no sirve: el siguiente sync vuelve a
reemplazar la tabla entera con la exportación, copias incluidas. Hasta que
llegue una exportación corregida, el gate detiene el pipeline.

Hay una línea más en el log de `validate_tags`, un aviso, no un fallo:
`expect_column_values_to_be_in_set:tag` con `'other'` en sus detalles. Los
tags nuevos traen un valor que el origen nunca había usado. La expectation no
es crítica (§9), así que el run siguió. "La alarma de drift" vuelve sobre él.

### Día 3: un formato de fecha nuevo

Llega la exportación corregida. Cambia la URL de la source `projects` a:

```
https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/batch_2/day3_projects.csv
```

```bash
docker compose exec airflow-scheduler airflow dags trigger dataops
```

**Comprobación.** Las dos tasks `validate_*` pasan, con el aviso sobre
`other` incluido, y `transform` vuelve a fallar después de sus reintentos. Su
log es distinto:

```
ERROR creating sql table model mlops_course.labeled_projects
...
Invalid timestamp: '06/30/2021 03:58:46'
Done. PASS=0 WARN=0 ERROR=1 SKIP=5 NO-OP=0 REUSED=0 TOTAL=6
```

Un `ERROR` no es un `FAIL`. Un FAIL es un test que se ejecutó y encontró
filas; un ERROR es SQL que no se pudo ejecutar. Aquí
`CAST(p.created_on AS TIMESTAMP)` se encontró con un valor que no sabe leer,
el model no se construyó y sus cinco tests se saltaron.

```sql
SELECT created_on
FROM `<project id>.mlops_course.projects`
WHERE SAFE_CAST(created_on AS TIMESTAMP) IS NULL
```

191 filas, todas escritas `MM/DD/YYYY HH:MM:SS`: los proyectos nuevos. La
exportación ahora escribe las fechas de otra forma. Ningún control lo vio: la
suite de `projects` revisa las columnas, los ids y los títulos, y nada sobre
cómo se escribe una fecha.

**Dónde va el arreglo: en el transform.** La tabla sin procesar guarda lo que
manda el origen, y todas las filas tienen una fecha válida, escrita de una de
dos formas. Leer las dos es trabajo del transform. Reemplaza
`dbt/dbt_transforms/models/labeled_projects/labeled_projects.sql` por:

```sql
SELECT
    p.id,
    COALESCE(
        SAFE_CAST(p.created_on AS TIMESTAMP),
        SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', p.created_on)
    ) AS created_on,
    p.title,
    p.description,
    t.tag
FROM {{ source('mlops_course', 'projects') }} AS p
LEFT JOIN {{ source('mlops_course', 'tags') }} AS t
    ON p.id = t.id
```

`SAFE_CAST` devuelve `NULL` donde `CAST` detendría la consulta, y `COALESCE`
se queda con el primer valor que no es `NULL`: primero el formato viejo,
después el nuevo. Un tercer formato daría `NULL`, sin avisar. Así que añade
el control que le faltó a este incidente, en
`dbt/dbt_transforms/models/labeled_projects/schema.yml`, antes de `title`:

```yaml
      - name: created_on
        data_tests:
          - not_null
```

Un problema que ningún control detectó debería dejar un control detrás. La
próxima vez será un FAIL con una cantidad de filas, no un error en medio del
SQL.

**Vuelve a ejecutar solo lo que cambió.** Las tablas sin procesar están bien, así
que no hay nada que volver a extraer: vuelve a ejecutar `transform` y lo que
viene después. Eso es lo que da ELT (§9). En la UI, abre este run fallido,
haz clic en `transform` y elige **Clear Task Instance**. **Downstream** ya
está seleccionado, así que `validate_transformed` también se limpia. Elige
**Confirm**.

**Comprobación.** `transform` vuelve a correr en el mismo run y su log termina
con `Done. PASS=7 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=7`: ahora un
model y seis tests. `validate_transformed` pasa, con el mismo aviso sobre
`other`, y el run queda en `success`. `labeled_projects` tiene 955 filas.

Los tres días hicieron tres preguntas distintas: si había que arreglar algo
(día 1, no), de quién era el arreglo (día 2, del origen) y dónde iba (día 3,
en el transform). Cuánto volver a ejecutar sale de la respuesta: una entrega
nueva es un run nuevo; un arreglo de tu lado se vuelve a ejecutar desde la
task que cambió.

### La alarma de drift

Los datos están cargados y revisados. Ahora pregunta qué opina de ellos el
monitor de la §12:

```bash
docker compose exec airflow-scheduler airflow dags test monitoring
```

**Comprobación.** `decide` registra un label drift con p-value cerca de
`1e-17`, un length drift cerca de `0.91` y `new_labels=['other']`, y no toma
ni `continue_serving` ni la rama de reentrenamiento: `Branch into inspect`.
Después:

```
MANUAL INSPECTION NEEDED
  new labels: ['other']
  current distribution: {'mlops': 0.092..., 'other': 0.108..., 'time-series': 0.016..., ...}
```

El run termina ahí, en segundos, sin reentrenar nada. Es la regla con la que
se escribió `decide` en la §12: un label que el modelo nunca vio significa que
el espacio de labels se movió, y reentrenar con datos que nadie entiende
todavía es como sale a producción un modelo peor. Una alarma de drift es una
pregunta; la inspección te toca a ti.

La ventana actual es el último cuarto: los 191 proyectos nuevos y 48 de los
viejos más recientes. En su distribución, `other` está cerca del 11%, cuando
antes no aparecía, y `reinforcement-learning`, `graph-learning` y
`time-series` bajan al 1–2%. Mira esos cuatro tags por año:

```sql
SELECT EXTRACT(YEAR FROM created_on) AS year, tag, COUNT(*) AS n
FROM `<project id>.mlops_course.labeled_projects`
WHERE tag IN ('other', 'reinforcement-learning', 'graph-learning', 'time-series')
GROUP BY year, tag
ORDER BY year, tag
```

```
2020  graph-learning           33
2020  reinforcement-learning   45
2020  time-series              28
2021  other                    26
```

Los proyectos no cambiaron; cambiaron las etiquetas. Desde 2021, el origen
escribe esos tres tags como `other`, que es la misma fusión que ya hace el
modelo: aprende cuatro clases, y todo otro tag pasa a `other` (§10). La línea
`f1 at promotion=… now=…`, registrada antes de `decide`, coincide: no hay
caída, así que el modelo funciona igual de bien con los proyectos nuevos que
con los viejos. Reentrenar no arreglaría nada. El arreglo es darles a las
filas viejas el vocabulario nuevo, en el transform, para que el historial y
los datos nuevos usen las mismas etiquetas. En `labeled_projects.sql`,
reemplaza la última columna, `t.tag`, para que el model quede así:

```sql
SELECT
    p.id,
    COALESCE(
        SAFE_CAST(p.created_on AS TIMESTAMP),
        SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', p.created_on)
    ) AS created_on,
    p.title,
    p.description,
    CASE
        WHEN t.tag IN ('reinforcement-learning', 'graph-learning', 'time-series') THEN 'other'
        ELSE t.tag
    END AS tag
FROM {{ source('mlops_course', 'projects') }} AS p
LEFT JOIN {{ source('mlops_course', 'tags') }} AS t
    ON p.id = t.id
```

Un tag que falta sigue siendo `NULL` (un `CASE` sin coincidencia toma su
`ELSE`), así que el test del día 1 sigue protegiendo contra eso. Vuelve a
limpiar `transform`, en el mismo run, con **Downstream**, y pregúntale otra
vez al monitor:

```bash
docker compose exec airflow-scheduler airflow dags test monitoring
```

**Comprobación.** Label drift cerca de `0.46`, length drift cerca de `0.91`,
`new_labels=[]`, y `decide` toma `continue_serving`. El run termina en
segundos.

Queda un cabo suelto: cada run de `dataops` sigue avisando sobre `other`, en
`validate_tags` y en `validate_transformed`. Un aviso que siempre está deja de
leerse, y el siguiente se pierde. El origen ahora emite `other` a propósito,
así que va en la lista de tags que se sabe que emite. Añádelo a
`KNOWN_RAW_TAGS` en [`src/project_tagger/config.py`](../../src/project_tagger/config.py):

```python
KNOWN_RAW_TAGS = [
    "natural-language-processing",
    "computer-vision",
    "mlops",
    "reinforcement-learning",
    "graph-learning",
    "time-series",
    "other",
]
```

Las tasks lo importan al ejecutarse, así que el próximo run lee la lista
nueva. Limpia `validate_tags` en el mismo run, con **Downstream**.

**Comprobación.** `validate_tags`, `transform` y `validate_transformed` pasan,
y ninguno de los dos logs de validación tiene ya el aviso.

### De vuelta a la normalidad

```bash
docker compose exec airflow-scheduler airflow dags unpause mlops
```

Los proyectos nuevos están cargados, revisados y etiquetados igual que los
viejos, y el próximo modelo va a entrenar con ellos. Las sources se quedan con
sus URLs del día 2 y del día 3: ahora esos son tus datos.

---

← [13. Deadline alerts](13-deadline-alerts.md) · [Índice](README.md) · [15. Testear Dags](15-testear-dags.md) →
