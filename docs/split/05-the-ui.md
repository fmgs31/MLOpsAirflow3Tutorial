## 5. The UI

The UI is a React application on top of the api-server's API — the public
REST API plus some endpoints of its own under `/ui/` —, served
at <http://localhost:8080>. Half of what this tutorial teaches is easier to see
than to read about, so take the tour now that there is a Dag to look at.

The nav is **Home · Dags · Assets · Browse · Admin** at the top and **Docs**, a
clock and **User** at the bottom.

**Home** — health of the scheduler, triggerer and Dag processor, import errors,
failed runs and tasks, recent asset events, deadlines and pool slots. Open it
first when something is not running.

**Dags** — the list, as cards or a table, filtered by tag, owner or state, with
the pause toggle and a **Trigger** button on each row. Yours shows
`hello_airflow3`, its owner, its two tags, and the one run `airflow dags test`
made. A Dag scheduled on assets shows its asset condition where a cron Dag
shows its schedule; yours, with `schedule=None`, shows neither. A Dag that never
appears here usually failed to import: a red badge with the count of failures
sits next to the number of Dags — click it for the tracebacks.

**A Dag's page** is split in two: the Dag on the left, detail tabs on the right.
The left panel has two views, picked with the icons at its top left or toggled
with the `g` key. **Grid** is the run history — one column per run, one row per
task, each cell coloured by its state. **Graph** is the tasks and the edges
between them. At the other end of that row are the controls worth knowing:

- **Options** (the gear). In the Graph: the Dag version and the run to show,
  the graph direction, and **Dependencies** — whether the graph shows only the
  Dag's own tasks (the default) or also the assets and Dags connected to it.
  `hello_airflow3` is connected to nothing, so all three options look the
  same; §10 shows the difference on `mlops`. In the Grid: how many runs to show, and version
  indicators that mark where the code changed between runs.
- **Filter** (a funnel): pick a task and show only what is upstream or
  downstream of it.
- **Task Filters** (a magnifying glass, Graph only). It appears only when there
  is something to filter: a run is selected, or the Dag has task groups or more
  than one kind of operator — so on `hello_airflow3` you see it once you open a
  run. It filters by state, task group,
  duration, and **Min. map index** — "shows mapped tasks expanded to at least
  this index". A mapped task like `run_trials` (§10) is one collapsed node
  until you raise it; raise it and each mapped instance gets its own.
- **Download graph image**, the arrow at the bottom right of the Graph.

The tabs on the right follow what you select:

- **Nothing** — the Dag's own tabs: Overview, Runs, Tasks, Calendar, Audit Log,
  Code and Details, plus **Backfills** on a Dag with a schedule.
- **A task**, clicked in the Graph with no run selected — that task across all
  runs: Overview, Task Instances, Audit Log.
- **A run**, clicked in the **Runs** tab or as a bar in the Grid — Task
  Instances, Asset Events, Audit Log, Code, Details. The left panel also gets a
  third view icon, between Grid and Graph, whose tooltip says *Show Gantt*: the
  Grid with a timeline of that run's tasks next to it.
- **A task inside a run** — that task instance: **Logs**, **Rendered
  Templates**, **Storage** (which holds its **XCom**), Asset Events, Audit Log,
  Code, Details. A mapped task adds **Task Instances**, one row per mapped copy,
  and a task waiting on a human adds **Required Actions**.

**Do it now.** Open `hello_airflow3` and look at the Graph: `make_greeting`
fanning out to `shout` and `count_words`, and both fanning back into `report`.
That shape comes from the last lines of §4 — passing `message` to two tasks,
and both of their results to `report`.

**The Dag version selector.** Every Dag run records the version of the code it
ran with: each time you save a change to a Dag file, Airflow creates a new
version — `v1`, `v2`, … The picker, in the Graph's **Options** and on the
**Code** tab, lists each version with the time it was created, and lets you
look at a run through the code that actually produced it: last week's run still
shows last week's graph. **Code** also shows when the file was parsed and, once
there are two versions, a button to compare them.

Those versions come from the *bundle* the Dag was loaded from — the place the
Dag processor reads files from. This stack has one, the `dags/` folder, named
`dags-folder`: the `bundle_name` column of `airflow dags list` in §4. A local
folder has no version of its own; a Git bundle does (the commit), and then
**Code** shows that too (§16).

**Do it now.** Press **Trigger**, at the top right of `hello_airflow3`'s page.
A form opens with one field, `greeting`, pre-filled with `hello`: the `Param`
you declared in §4.

**The Trigger form is generated from your `params`.** This is why params are
worth declaring with types:

```python
params={"greeting": Param("hello", type="string")}              # dags/01
POLICY_PARAMS = {"drift_p_val": Param(0.05, type="number")}     # dags/06
```

A string param becomes a text field, a number a numeric one, an enum a dropdown,
and the values are validated against each `Param`'s type and limits before
the run is created. Under **Advanced
Options** the same thing is editable as raw **Configuration JSON**, and the form
also offers a run id, a logical date and a manual data interval. If the Dag is
paused there is also an *"Unpause … on trigger"* checkbox, ticked by default,
which un-pauses it as the run is created. Untick it and the run is still
created, but stays **queued** until you un-pause the Dag (§7).

Type another single word in `greeting` — leave *Unpause* ticked if it is
there — and submit. That is the same `Param` reaching the same task by a
third route: a default in the file, `--conf` on the command
line, a form in the browser.

Now read what it did. Open the **Runs** tab, click the new run, click `report`
in the graph, and open **Logs**: your word in capitals and `(7 words)`, the line
`airflow dags test` printed in your terminal in §4. Click `make_greeting`, open
**Storage**, then **XCom**: the greeting string, stored under the key
`return_value` — the value `shout` and `count_words` received.

> The run `airflow dags test` made has no logs here. That command prints task
> output to your terminal and writes no log file, so its **Logs** tab only says
> the log file was not found — and suggests remote logging. Nothing is broken;
> there is simply no file.

The Trigger form has a second mode, **Backfill** — *"Run this Dag for a range
of dates"* — and on `hello_airflow3` it is disabled; hover over it and it says
*"This Dag's schedule does not support backfills"*. A backfill asks the
schedule which runs should have happened between two dates and creates them: a
daily schedule over four days answers "four runs, one per day".
`schedule=None` has no answer — the Dag runs only when someone triggers it, so
no date range holds a run it should have had. Backfill becomes usable in §9, on
`dataops`, which runs daily: a date range, a **Reprocess Behavior** (missing
runs / missing and errored runs / all runs), max active runs, "run backwards",
and a dry run that tells you *"N runs will be triggered"* before you commit.
`mlops` (§10) is disabled for the same reason: it runs when an asset is
updated, not on dates.

**Assets** — the page this tutorial was built for, and empty until §9. Once the
Dags that use them exist, it lists every asset with its producing tasks,
consuming Dags and event history, and draws the cross-Dag graph: `dataops` →
`labeled_projects` → `mlops` → `production_model`. The Dags list cannot show
it, because the connection is not a dependency between Dags, it is an asset
between them; besides this page, only a Dag's Graph with **Options →
Dependencies → All Dag Dependencies** draws it (§10). **Create Event** on
an asset either materializes it (runs the Dag upstream, which produces the data
for real) or records a manual event, which triggers the downstream Dags without
running the producer at all.

**Browse** — Audit Log (who triggered, paused, cleared what — your UI trigger
is in it now), **Deadlines** (pending and missed deadline alerts, §13), Jobs
(the scheduler, triggerer and Dag processor and their heartbeats), XComs, and
**Required Actions** (every task waiting on a human, across all Dags).

**Admin** — Variables, Pools, Providers, Plugins, Connections and the rendered
Airflow **Config**. Connections and Variables are the two you will use: never
put a credential in a Dag file.

**Docs** — the Airflow documentation, the GitHub repo, the REST API reference
served by this api-server, and the Airflow version you are running, linked to
its own docs.

**The clock** — labelled with the UTC offset of the timezone every date in the
UI is shown in. Hover it for the current time there; click it to pick another
timezone.

**User** — language, light/dark mode, whether a Dag opens on Grid or Graph, an
API token generator, and logout.

> Two menus can appear that this stack does not show. **Security** comes with an
> auth manager that has users and roles to manage; this tutorial's stack runs the
> Simple auth manager with everyone an admin. **Plugins** appears when an installed plugin
> adds pages to the nav; the only plugin installed in the stack, OpenLineage, adds none.

---

← [4. Your first Dag](04-your-first-dag.md) · [Index](README.md) · [6. Operators, XComs and relationships](06-operators-xcoms-and-relationships.md) →
