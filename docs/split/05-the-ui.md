## 5. The UI

The UI is a React application on top of the REST API, served by the api-server
at <http://localhost:8080>. Half of what this tutorial teaches is easier to see
than to read about, so take the tour now that there is a Dag to look at.

The nav is **Home · Dags · Assets · Browse · Admin · Plugins · Security · Docs**.

**Home** — health of the scheduler, triggerer and Dag processor, import errors,
failed runs and tasks, recent asset events, deadlines and pool slots. Open it
first when something is not running: a Dag that never appears in the list is
usually an import error sitting on this page.

**Dags** — the list, as cards or a table, filtered by tag, owner or state, with
the pause toggle and a **Trigger** button on each row. Yours shows
`hello_airflow3`, its owner, its two tags, and the one run `airflow dags test`
made. A Dag scheduled on assets shows its asset condition where a cron Dag
shows its schedule; yours, with `schedule=None`, shows neither.

**A Dag's page** puts the run history on the left and detail tabs on the right.
The left panel holds **Grid** and **Graph** — `g` toggles between them — plus
**Show Gantt**, a graph direction control, and filters worth knowing:

- **Dependencies: All Dag Dependencies / External conditions / Only tasks.**
  Switch it away from "Only tasks" on `mlops` (§10) and the graph draws the
  `labeled_projects` asset that triggered the run, not just the tasks inside it.
- **Min. map index** — "shows mapped tasks expanded to at least this index". A
  mapped task like `train_candidate` (§10) is one collapsed node until you raise
  this; raise it and each hyperparameter set gets its own.
- Version indicators, and **Download graph image**.

The tabs are Overview, Runs, Tasks, Backfills, Asset Events, Audit Log,
Calendar, Code, Details and — when a task is waiting on a human — **Required
Actions**. Click a run, then a task, and the same strip becomes that task
instance's: **Logs**, **XCom**, **Rendered Templates**, and **Task Instances**
for a mapped fan-out.

**Do it now.** Open `hello_airflow3`, look at the Graph — the fan-out and
fan-in you drew with two function calls — then click the run, click `report`,
and open **Logs**: the `(7 words)` line, in the browser this time. Click
`make_greeting` and open **XCom**: the greeting string, stored under the key
`return_value`. That is the value `shout` and `count_words` received.

**The Dag version selector.** Every Dag run records the version of the code it
ran with, so the picker (`v1`, `v2`, … or **All Versions**) lets you look at a
run through the code that actually produced it, with the bundle name, bundle
version and parse time on the **Code** tab. A *bundle* is a place the Dag
processor loads Dags from; this stack has one, the `dags/` folder, and it is
named `dags-folder` — the `bundle_name` column you saw in `airflow dags list`
(§15 has the other kinds). Every time you save a change to a Dag file its
version count goes up, and last week's run still shows last week's graph.

**The Trigger form is generated from your `params`.** This is why params are
worth declaring with types:

```python
params={"greeting": Param("hello", type="string")}              # dags/01
POLICY_PARAMS = {"max_label_tvd": Param(0.15, type="number")}   # dags/06
```

A string param becomes a text field, a number a numeric one, an enum a dropdown,
and required fields are validated before the run is created. Under **Advanced
Options** the same thing is editable as raw **Configuration JSON**, and the form
also offers a run id, a logical date, a manual data interval, and an *"Unpause …
on trigger"* checkbox — which exists because triggering a paused Dag leaves the
run **queued** forever (§7).

**Do it now.** Press **Trigger** on `hello_airflow3`. The form has one field,
`greeting`, pre-filled with `hello` — type something else, tick *Unpause
hello_airflow3 on trigger*, and submit. A second run appears in the Grid; its
`report` log has your word in capitals. That is the same `Param` reaching the
same task by a third route: a default in the file, `--conf` on the command
line, a form in the browser.

The same dialog has a second mode, **Backfill**: a date range, a **Reprocess
Behavior** (missing runs / missing and errored runs / all runs), max active runs,
"run backwards", and a dry run that tells you *"N runs will be triggered"*
before you commit. It needs a Dag with a schedule, so it waits for §9.
Asset-scheduled Dags say *"This Dag's schedule does not support backfills"* —
`mlops` (§10) cannot be backfilled, because there is no date range to replay,
only asset events.

**Assets** — the tab this tutorial was built for, and empty until §9. Once the
Dags that use them exist, it lists every asset with its producing tasks,
consuming Dags and event history, and draws the cross-Dag graph: `dataops` →
`labeled_projects` → `mlops` → `production_model`. That graph is the one picture
of the whole pipeline; the Dags list cannot show it, because the connection is
not a dependency between Dags, it is an asset between them. **Create Event** on
an asset either materializes it (runs the Dag upstream, which produces the data
for real) or records a manual event, which triggers the downstream Dags without
running the producer at all.

**Browse** — Audit Log (who triggered, paused, cleared what — your UI trigger
is in it now), **Deadlines** (pending and missed deadline alerts, §13), and
XComs.

**Admin** — Connections, Variables, Pools, Providers, Plugins, Jobs and the
rendered Airflow **Config**. Connections and Variables are the two you will use:
never put a credential in a Dag file.

> **Security and Plugins** are near-empty here. This stack runs the Simple auth
> manager with everyone an admin, so there are no users or roles to manage.

---

← [4. Your first Dag](04-your-first-dag.md) · [Index](README.md) · [6. Operators, XComs and relationships](06-operators-xcoms-and-relationships.md) →
