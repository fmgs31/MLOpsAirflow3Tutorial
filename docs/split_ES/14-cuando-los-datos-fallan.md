## 14. Cuando los datos fallan

Hasta ahora todos los runs salieron bien, porque todos leyeron los mismos archivos
limpios. Los datos reales llegan por partes, tarde, repetidos y con otro
formato, y el trabajo pasa a ser reaccionar cuando un control falla. Esta
sección carga un segundo lote de proyectos en tres entregas, cada una con un
problema que tienen las cargas reales. En las dos primeras muestra dónde
aparece el problema, cómo encontrar las filas que lo causan, dónde va el
arreglo y cuánto volver a ejecutar; la tercera la resuelves tú. Después, el
mismo lote dispara el monitor de drift de la §12.

| Entrega | Qué llega | Qué está mal | Dónde aparece |
|---|---|---|---|
| Día 1 | 191 proyectos nuevos | sus tags todavía no llegaron | `transform`: falla un test de dbt |
| Día 2 | los tags, y otra vez los proyectos | cuatro proyectos viejos reenviados con ids nuevos | `validate_projects`: falla una suite de GX |
| Día 3 | los proyectos, corregidos | te toca encontrarlo | te toca encontrarlo |
| — | (los mismos datos) | desde el 11 de septiembre de 2026 los tags usan otro vocabulario | `monitoring`: drift |

### El lote

Los archivos están en [`datasets/batch_2/`](../../datasets/batch_2/). Los 191
proyectos son reales: el holdout set de Made With ML, proyectos del mismo
sitio que no están en `datasets/projects.csv`. Sus fechas se movieron a las
cinco semanas siguientes al paper de arXiv más nuevo de la §12, para que sean
las filas más nuevas del warehouse, y encima se añadieron los tres primeros
problemas de la tabla. El cuarto no se añadió: Made With ML publicó estos tags
con los tres más raros ya fusionados en `other`.
[`datasets/README.md`](../../datasets/README.md) explica cómo se generan los
archivos y qué trae cada uno.

Cada sync de la §9 es **Full Refresh Overwrite**, así que cada archivo es la
tabla entera tal como la exportaría el origen ese día: las 1564 filas que ya
había enviado y después las nuevas. Una entrega es un cambio de URL en una
source de Airbyte: abre **Sources**, elige la source, reemplaza su **URL** y
guarda.

### Antes de empezar

Pausa `mlops`:

```bash
docker compose exec airflow-scheduler airflow dags pause mlops
```

Un run de `dataops` que tiene éxito emite el asset event que arranca `mlops`
(§8), que aquí entrenaría un modelo con los proyectos nuevos antes de que
`monitoring` los compare con lo que conoce el modelo en producción. Esa
comparación es "La alarma de drift". `dataops` sigue sin pausa, como lo dejó
la §10.

### Día 1: proyectos sin tags

Cambia la URL de la source `projects` a:

```
https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/batch_2/day1_projects.csv
```

Deja `tags` como está y arranca un run:

```bash
docker compose exec airflow-scheduler airflow dags trigger dataops
```

**Comprobación.** En la UI, tras unos minutos, el run falla en `transform`:
los dos syncs y las dos tasks `validate_*` están en verde, y
`validate_transformed` y `quality_report.summarize` en `upstream_failed`.

**Lee el fallo.** Abre el log de `transform`. Cerca del final:

```
FAIL 191 not_null_labeled_projects_tag
...
Got 191 results, configured to fail if != 0
Done. PASS=5 WARN=0 ERROR=1 SKIP=0 NO-OP=0 REUSED=0 TOTAL=6
```

Un test de dbt es una consulta que devuelve las filas que rompen la regla;
falla si vuelve alguna. dbt escribe cada test, compilado, en
`dbt/dbt_transforms/target/compiled/dbt_transforms/models/labeled_projects/schema.yml/not_null_labeled_projects_tag.sql`;
este selecciona `tag` donde es nulo. Para ver *qué* proyectos son, pide más
columnas en la consola de BigQuery (§9):

```sql
SELECT id, created_on, title
FROM `<project id>.mlops_course.labeled_projects`
WHERE tag IS NULL
ORDER BY created_on
```

191 filas, todas con fecha desde el 11 de septiembre de 2026: el lote nuevo.
Y `tags` no creció:

```sql
SELECT COUNT(*) AS n FROM `<project id>.mlops_course.tags`
```

`n` es 1564. Los proyectos llegaron antes que sus etiquetas: el etiquetado
viene después del envío, y va atrasado. No hay nada roto.

**Dónde va el arreglo: en ningún lado, todavía.** Volver a ejecutar da el
mismo fallo con los mismos datos. Compruébalo con una herramienta que todavía
no usaste: volver a ejecutar un run desde cualquiera de sus tasks. Abre el run
en la UI, haz clic en `transform` y haz clic en el botón de flecha circular
arriba a la derecha del panel de la task, a la izquierda de **✓ / ✗** (o pulsa
**Shift+C**). Se abre el diálogo **Clear Task Instance**. **Downstream** viene
seleccionado, así que también se limpian las tasks que vienen después. Elige
**Confirm**: `transform` vuelve a correr en el mismo run, y falla igual, tras
sus reintentos.

Relajar el test dejaría llegar proyectos sin etiqueta al entrenamiento, donde
`preprocess` los descarta sin avisar. Hay que esperar las etiquetas.

El fallo muestra dos cosas más. Primero, `labeled_projects` ya tiene las 191
filas sin tag: `dbt build` escribe la tabla y después la testea. Lo que las
mantuvo lejos del entrenamiento es el gate siguiente: `validate_transformed`
no corrió, así que no se emitió ningún asset event. Ese gate solo protege a los
Dags programados con el asset: corren cuando se emite el evento, y no se
emitió. Pero la tabla ya tiene las filas sin tag, y cualquier Dag que la lea
sin esperar el evento las recibe. Hay dos casos: `monitoring` (§12), que corre
con su cron y lee la tabla igual, y `mlops` ejecutado a mano, porque un trigger
manual no espera el asset. Ahí lo único que lo frena es su propia comprobación,
`validate_training_data` (§10). La §16 muestra el patrón que evita que una
tabla que no pasa sus tests llegue a publicarse. Segundo, la espera: cada vez,
`transform` corrió tres veces, con un minuto entre cada una, porque `dataops`
da `retries=2` a todas sus tasks. Los reintentos sirven cuando la causa se va
sola (un error de red, una API ocupada); un problema de datos falla igual cada
vez. Por eso los gates de GX lanzan `AirflowFailException`, que se salta los
reintentos (§9). `transform`, un `BashOperator` que ejecuta dbt, no distingue
un test fallido de una conexión perdida, así que reintenta los dos.

### Día 2: los mismos proyectos dos veces

Llegan las etiquetas. Cambia `projects` a:

```
https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/batch_2/day2_projects.csv
```

y `tags` a:

```
https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/batch_2/day2_tags.csv
```

Una entrega nueva lleva un run nuevo:

```bash
docker compose exec airflow-scheduler airflow dags trigger dataops
```

**Comprobación.** Esta vez el run falla en menos de un minuto en
`validate_projects`, sin reintentos. Los dos syncs están en verde, y también
`validate_tags`, aunque esta vez valida tags nuevos; `transform`,
`validate_transformed` y `quality_report.summarize` en `upstream_failed`.

**Lee el fallo.** El log de `validate_projects` termina con la expectation que
falló y algunas de las filas que la rompieron:

```
projects failed validation: [{'expectation': 'expect_compound_columns_to_be_unique:title,description', ...
'details': "[{'title': '…', 'description': '…'}, ...
```

Dos proyectos con el mismo título y la misma descripción. Encuéntralos todos:

```sql
SELECT title, ARRAY_AGG(id ORDER BY id) AS ids, ARRAY_AGG(created_on ORDER BY id) AS created
FROM `<project id>.mlops_course.projects`
GROUP BY title, description
HAVING COUNT(*) > 1
```

Cuatro filas, cada una un proyecto de 2020 junto a una copia con un id nuevo,
del 3800 al 3803, las cuatro con fecha `2026-10-16 09:00:00`. La exportación
reenvió cuatro proyectos viejos como nuevos.

**Dónde va el arreglo: en el origen.** El bug está en el origen, y solo quien
lo administra puede decir cuál de los ids es el real. Repórtalo, con la
consulta. Borrar las copias en BigQuery no sirve: el siguiente sync reemplaza
la tabla entera con la exportación, copias incluidas. Hasta que llegue una
exportación corregida, el gate detiene el pipeline.

El log de `validate_tags` tiene una línea más, un aviso, no un fallo:
`expect_column_values_to_be_in_set:tag` con `'other'` en sus detalles. Los
tags nuevos traen un valor que el origen nunca había usado. La expectation no
es crítica (§9), así que el run siguió. "La alarma de drift" vuelve sobre él.

### Día 3: la exportación corregida

En los días 1 y 2 el tutorial respondió cada pregunta por ti. El día 3 es
tuyo: abajo están la entrega y el objetivo; el diagnóstico y el arreglo te
tocan a ti.

Llega la exportación corregida. Cambia la URL de la source `projects` a:

```
https://raw.githubusercontent.com/fmgs31/MLOpsAirflow3Tutorial/main/datasets/batch_2/day3_projects.csv
```

```bash
docker compose exec airflow-scheduler airflow dags trigger dataops
```

**Tu tarea.** El run falla. Quien administra el origen confirma que esta
exportación está bien: así manda sus datos a partir de ahora. Consigue que
este run termine en `success`, y deja un control para que la próxima vez algo
así sea un `FAIL` de dbt con una cantidad de filas. Vuelve a ejecutarlo
limpiando una task, como en el día 1; qué task limpiar es parte del ejercicio.

**Terminaste cuando:**

- el run está en `success`, y `validate_transformed` pasa con el mismo aviso
  sobre `other` que `validate_tags`;
- el log de `transform` termina con
  `Done. PASS=7 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=7`, un test más
  que en el día 1;
- `labeled_projects` tiene 1755 filas.

Abre las pistas de una en una, solo cuando te atasques.

<details>
<summary>Pista 1: ¿qué tipo de fallo es?</summary>

Compara la última línea del log de `transform` con la del día 1,
`PASS=5 WARN=0 ERROR=1 SKIP=0`. ¿Qué significa un skip, y por qué se
saltarían cinco cosas? Después busca la línea que nombra un valor.

</details>

<details>
<summary>Pista 2: ¿qué filas?</summary>

El model lee la tabla sin procesar `projects` (§9). `SAFE_CAST`, en BigQuery,
devuelve `NULL` donde `CAST` detendría la consulta: úsalo para encontrar las
filas que `CAST` no sabe leer.

</details>

<details>
<summary>Pista 3: ¿dónde va el arreglo?</summary>

El día 2 mostró por qué cambiar una tabla sin procesar no dura. El arreglo va
en `dbt/dbt_transforms/models/labeled_projects/labeled_projects.sql`.
`SAFE.PARSE_TIMESTAMP`, en BigQuery, lee un timestamp escrito en el formato
que le indiques, y `COALESCE` devuelve el primero de sus argumentos que no es
`NULL`.

</details>

<details>
<summary>Pista 4: ¿qué control?</summary>

Después de tu arreglo, ¿en qué se convierte en `labeled_projects` una fecha
escrita en un tercer formato? ¿Qué test, añadido a
`dbt/dbt_transforms/models/labeled_projects/schema.yml` (§9), lo detectaría?

</details>

<details>
<summary>Pista 5: ¿qué task limpiar?</summary>

Las tablas sin procesar están bien. ¿Cuál es la primera task cuyo resultado
estuvo mal?

</details>

<details>
<summary>Solución</summary>

**Lee el fallo.** Las dos tasks `validate_*` pasan, con el aviso sobre
`other`, y `transform` vuelve a fallar tras sus reintentos, con un log
distinto del del día 1:

```
ERROR creating sql table model mlops_course.labeled_projects
...
Invalid timestamp: '09/11/2026 17:19:07'
Done. PASS=0 WARN=0 ERROR=1 SKIP=5 NO-OP=0 REUSED=0 TOTAL=6
```

Un `ERROR` no es un `FAIL`: un FAIL es un test que se ejecutó y encontró
filas; un ERROR es SQL que no se pudo ejecutar. Aquí
`CAST(p.created_on AS TIMESTAMP)` se encontró con un valor que no sabe leer,
así que el model no se construyó y sus cinco tests se saltaron.

```sql
SELECT created_on
FROM `<project id>.mlops_course.projects`
WHERE SAFE_CAST(created_on AS TIMESTAMP) IS NULL
```

191 filas, todas escritas `MM/DD/YYYY HH:MM:SS`: los proyectos nuevos. La
exportación ahora escribe las fechas de otra forma, y ningún control lo vio:
la suite de `projects` revisa las columnas, los ids y los títulos, no cómo se
escribe una fecha.

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
se queda con el primer valor no `NULL`: primero el formato viejo, después el
nuevo. Un tercer formato daría `NULL` sin avisar. Así que añade el control que
le faltó a este incidente, en
`dbt/dbt_transforms/models/labeled_projects/schema.yml`, antes de `title`:

```yaml
      - name: created_on
        data_tests:
          - not_null
```

Un problema que ningún control detectó debería dejar un control nuevo: la
próxima vez será un FAIL con una cantidad de filas, no un error en medio del
SQL.

**Vuelve a ejecutar solo lo que cambió.** Las tablas sin procesar están bien,
así que no hay nada que volver a extraer: vuelve a ejecutar `transform` y lo
que viene después. Eso es lo que da ELT (§9). Limpia `transform` en este run
fallido; `validate_transformed` y `quality_report.summarize` se limpian con
ella. `transform` vuelve a correr en el mismo run, y el `TOTAL=7` de su log
es ahora un model y seis tests.

</details>

Los tres días hicieron tres preguntas: si había que arreglar algo (día 1, no),
de quién era el arreglo (día 2, del origen) y dónde iba (día 3, en el
transform). Cuánto volver a ejecutar sale de la respuesta: una entrega nueva
es un run nuevo; un arreglo de tu lado se vuelve a ejecutar desde la task que
cambió.

### La alarma de drift

Los datos están cargados y revisados. Ahora pregunta al monitor de la §12 qué
opina de ellos:

```bash
docker compose exec airflow-scheduler airflow dags test monitoring
```

Busca en la salida como en la §12; el traceback de `pendulum` es la misma
comprobación inofensiva.

**Comprobación.** `prepare_windows` registra `'num_current': 191`. `decide`
registra un target drift con p-value cerca de `6e-41`, un length drift cerca de
`5e-19` y `new_labels=['other']`, y no toma ni `continue_serving` ni la rama de
reentrenamiento: `Branch into inspect`. Después:

```
MANUAL INSPECTION NEEDED
  new labels: ['other']
  current distribution: {'natural-language-processing': 0.408..., 'computer-vision': 0.371..., 'other': 0.136..., 'mlops': 0.083...}
  f1 baseline=… cumulative=…
  computer-vision: 71 rows, f1 …
  mlops: 16 rows, f1 …
  natural-language-processing: 78 rows, f1 …
  other: 26 rows, f1 …
  short_text: 11 rows, f1 …
  mentions_transformer: 28 rows, f1 …
```

e `inspect` falla con `manual inspection needed - nothing was retrained or
rolled back`. El run termina ahí, en segundos: necesita a una persona, y no se
reentrenó nada. Es la regla con la que se escribió `decide` en la §12: un
label que el modelo nunca vio significa que el espacio de labels se movió, y
reentrenar con datos que nadie entiende todavía es como sale a producción un
modelo peor. Una alarma de drift es una pregunta; la inspección te toca a ti.

La ventana actual son exactamente los 191 proyectos nuevos: ninguno de sus ids
está en el `split_ids.json` del modelo en producción. En ella, `other` ronda
el 14%, aunque las filas de las que aprendió el modelo nunca lo tienen, y
`reinforcement-learning`, `graph-learning` y `time-series` no aparecen. Mira
cuándo se usó cada uno de esos cuatro tags:

```sql
SELECT tag, COUNT(*) AS n, DATE(MIN(created_on)) AS first, DATE(MAX(created_on)) AS last
FROM `<project id>.mlops_course.labeled_projects`
WHERE tag IN ('other', 'reinforcement-learning', 'graph-learning', 'time-series')
GROUP BY tag
ORDER BY first
```

```
graph-learning          41  2020-02-24  2026-02-09
reinforcement-learning  73  2020-02-28  2026-09-10
time-series             58  2020-04-06  2026-08-11
other                   26  2026-09-11  2026-10-13
```

Los tres tags se acaban donde empieza `other`: desde el 11 de septiembre de
2026, el origen los escribe como `other`. Es la misma fusión que ya hace el
modelo: aprende cuatro clases, y todo otro tag pasa a `other` (§10). La línea
`f1 baseline=… cumulative=…` coincide: no hay caída, así que el modelo funciona
igual de bien con los proyectos nuevos que con su split de test. Reentrenar no
arreglaría esto. El arreglo es dar a las filas viejas el vocabulario nuevo, en
el transform, para que el historial y los datos nuevos usen las mismas
etiquetas. En `labeled_projects.sql`, reemplaza la última columna, `t.tag`,
para que el model quede así:

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
limpiar `transform`, en el mismo run, con **Downstream**, y pregunta otra vez
al monitor:

```bash
docker compose exec airflow-scheduler airflow dags test monitoring
```

**Comprobación.** `new_labels=[]`, así que `decide` ya no se para en
`inspect`. Los dos tests de drift tienen un p-value por debajo de 0.05 y un
effect size por encima de `min_effect_size`:

| Test | p-value | Effect size | Filas de las que aprendió el modelo | Filas nuevas |
|---|---|---|---|---|
| Target drift | cerca de `0.006` | 0.10 | `mlops` 4 % | `mlops` 8 % |
| Data drift | cerca de `5e-19` | 0.35 | mediana de 22 palabras por texto | mediana de 17 palabras por texto |

`decide` toma `trigger_retraining`, como en la §12, y deja en cola un run de
`mlops` que espera mientras `mlops` esté en pausa.

Los dos tests siguen informando de drift, y los dos tienen razón. Estos 191
son proyectos de Made With ML, como los 764 de la §9, pero el modelo aprendió
de esos más los papers de arXiv de la §12: frente a esa mezcla, los proyectos
nuevos son más cortos y tienen el doble de `mlops`. Y el f1 no bajó. Drift es una diferencia,
no un daño: un test de drift dice que los datos se movieron; solo la
comprobación de rendimiento dice si eso hizo daño. La política de la §12
reentrena en los dos casos, a costa de un entrenamiento, y el gate de `mlops`
decide si el resultado sale a producción; una política que solo reentrenara
ante una caída del f1 tomaría aquí `continue_serving`.

Queda un cabo suelto: cada run de `dataops` sigue avisando sobre `other`, en
`validate_tags` y en `validate_transformed`. Un aviso que siempre está deja de
leerse, y el siguiente se pierde. El origen ahora emite `other` a propósito,
así que añádelo a los tags que se sabe que emite, `KNOWN_RAW_TAGS` en
[`src/project_tagger/config.py`](../../src/project_tagger/config.py):

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

El run de `mlops` que dejó en cola el monitor arranca y entrena con los 1755
proyectos. Es bien posible que su gate se quede con el modelo actual: el f1 no
bajó, así que hay poco que ganar en las 49 filas de test nuevas, y `below the
champion's … on the 49 new test rows - not promoting` es un desenlace normal;
si el gate lo deja pasar, `approve_promotion` te espera, como en la §10. Los
proyectos nuevos están cargados, revisados y etiquetados igual que los
viejos, y desde ahora cada modelo entrena con ellos. Las sources se quedan con
sus URLs del día 2 y del día 3: ahora esos son tus datos.

---

← [13. Deadline alerts](13-deadline-alerts.md) · [Índice](README.md) · [15. Testear Dags](15-testear-dags.md) →
