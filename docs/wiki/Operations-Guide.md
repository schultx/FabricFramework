# Operations Guide

This page is field notes, not theory. Almost everything below — every error message, every
root cause — was hit for real while building and live-testing this exact framework against the
Columbus IUR tenant. If you're standing up Monza for a new client and something breaks, check
here before you start guessing.

Two things worth internalizing before you touch a live run: Fabric's own error messages are
frequently useless (see playbook item 1), and this framework's audit trail has a real blind spot
for `%run`-chained notebooks (see below) — don't trust either one at face value.

## How `PL_RUN_ALL` actually runs

`PL_RUN_ALL` (in the Code workspace) is the single entry point for a full run. Its committed
definition is at [src/PL_RUN_ALL.DataPipeline/pipeline-content.json](../../src/PL_RUN_ALL.DataPipeline/pipeline-content.json).
The shape:

```
EP_INGEST_SQL          ─┐
EP_INGEST_FILE          │
EP_INGEST_SQLMI         │
EP_INGEST_ORACLE        ├─  all 8 run in PARALLEL, dependsOn: []  ──►  EP_LOAD_BRONZE  ──►  [EP_LOAD_SILVER]  ──►  EP_LOAD_GOLD
EP_INGEST_SFTP          │                                              (waits on ALL 8)      (only if
EP_INGEST_FTP           │                                                                     include_silver)
EP_INGEST_ONELAKETABLE  │
EP_INGEST_ONELAKEFILE  ─┘
```

Facts worth knowing, straight from the JSON:

- All 8 `EP_INGEST_<TYPE>` activities have `"dependsOn": []` — they fire simultaneously, not in
  sequence. Each has `"policy": {"retry": 2, "retryIntervalInSeconds": 30, "timeout": "0.12:00:00"}`
  — up to 2 automatic retries, 30 seconds apart, before the activity is actually considered failed.
- `EP_LOAD_BRONZE` depends on **all 8** ingest activities with `dependencyConditions: ["Succeeded"]`
  — one ingest type failing after its retries are exhausted blocks Bronze (and therefore Gold)
  entirely for that run.
- `EP_LOAD_GOLD` depends only on `EP_LOAD_BRONZE` succeeding, in the committed JSON. There's no
  `EP_LOAD_SILVER` activity in the file you'll find in git — it isn't always part of the graph.
- `Custom`-type sources (the escape-hatch connector for REST APIs, SharePoint, Dataverse, etc. —
  see [Connector-Types](Connector-Types.md)) are **not** wired into `PL_RUN_ALL`'s fan-out. If a
  client has a `Custom` source, its notebook needs its own trigger; it won't run just because
  `PL_RUN_ALL` did.

**Where `EP_LOAD_SILVER` actually comes from:** `NB_DEPLOY` splices it into `PL_RUN_ALL`'s JSON
*in memory, at deploy time*, only when that environment's `config/environments.yaml` sets
`include_silver: true` — inserting the activity between Bronze and Gold and repointing Gold's
`dependsOn` accordingly. So the file in `src/` never shows the Silver stage even on an environment
that runs it; check the live item's definition (or just watch a run) if you need to confirm
whether Silver actually executed. See [Deployment-Guide](Deployment-Guide.md) for how the splice
works.

### Why `NB_RUN_REMOTE_PIPELINE` exists

Every `EP_INGEST_<TYPE>` activity in `PL_RUN_ALL` is a `TridentNotebook` activity, not an
`ExecutePipeline` activity, and they all point at the **same** notebook:
`NB_RUN_REMOTE_PIPELINE`. That's deliberate, not an oversight — Fabric's legacy `ExecutePipeline`
activity [only supports a pipeline in the same workspace as the caller](https://learn.microsoft.com/en-us/fabric/data-factory/invoke-pipeline-activity).
`PL_RUN_ALL` lives in **Code**; the real `PL_INGEST_SQL`, `PL_INGEST_FILE`, etc. pipelines live in
**Ingestion**. Cross-workspace triggering needs either a newer preview activity (Connection object,
Workspace-Identity/service-principal auth, a tenant setting, explicit cross-workspace grants) or
triggering the target item's job directly via the Job Scheduler REST API — this framework does the
latter.

`NB_RUN_REMOTE_PIPELINE` ([src/NB_RUN_REMOTE_PIPELINE.Notebook/notebook-content.py](../../src/NB_RUN_REMOTE_PIPELINE.Notebook/notebook-content.py))
takes two parameters, `target_workspace_id` and `target_item_id` — `PL_RUN_ALL` passes the
Ingestion workspace ID and the specific `PL_INGEST_*` pipeline's item ID for each of the 8
activities. The notebook then:

1. `POST {FABRIC_API}/workspaces/{target_workspace_id}/items/{target_item_id}/jobs/instances?jobType=Pipeline`
2. Reads the `Location` header from the response and polls it (every `Retry-After` seconds, or 15s
   if that header is missing) until status is `Completed`, `Failed`, `Cancelled`, or `Deduped`
3. Raises if the terminal status isn't `Completed` — which fails the `TridentNotebook` activity in
   `PL_RUN_ALL`, which fails the dependent chain, exactly as `waitOnCompletion: true` on a normal
   `ExecutePipeline` activity would have

There's a 6-hour hard timeout (`TIMEOUT_SECONDS = 60 * 60 * 6`) so a stuck remote pipeline doesn't
poll forever. Because the outer `TridentNotebook` activity is itself synchronous, this preserves
"wait for the child pipeline before continuing" semantics even though the actual `ExecutePipeline`
mechanism can't be used cross-workspace.

## Audit trail — and its real blind spot

Every notebook run gets exactly one row in `audit.NotebookRun`, written by
`start_notebook_run()` / `end_notebook_run()` (defined in `NB_MONZA_FUNCTIONS`, `%run` into every
loader notebook). `start_notebook_run(notebook_name)` inserts a row with a fresh `RunGuid` and
`Status = 'Running'`; `end_notebook_run(run_guid, status, error_message=None)` updates that same
row with a terminal `Status` and `EndTimeUtc`. See [Metadata-Model](Metadata-Model.md) for the
full schema.

**`NB_LOAD_BRONZE` isolates failures per table.** It opens one `audit.NotebookRun` row for the
whole run, then loops over every active `ingestion.Table` row inside its own `try`/`except`:

```python
for entity in entities:
    try:
        ...  # load this one entity into Bronze
    except Exception as exc:
        print(f"   ERROR loading '{entity['BronzeName']}': {exc!r} -- continuing with remaining tables")
        failed_entities.append(entity["BronzeName"])

if failed_entities:
    end_notebook_run(run_guid, "Failed", f"{len(failed_entities)} of {len(entities)} table(s) failed: {failed_entities}")
    raise RuntimeError(...)
else:
    end_notebook_run(run_guid, "Succeeded")
```

One malformed source table doesn't take every other active table down with it — every table gets
its turn, and only *then* does the run get marked `Failed` overall (with the list of which tables
failed in `ErrorMessage`) so the pipeline activity, and this framework's native failure
notifications, still see it.

**`NB_LOAD_SILVER` and `NB_LOAD_GOLD` do not have this protection.** Both orchestrate their
per-table notebooks via a straight `%run` chain (`%run dim_customer` → `%run fact_signup`, etc. —
see [src/NB_LOAD_GOLD.Notebook/notebook-content.py](../../src/NB_LOAD_GOLD.Notebook/notebook-content.py)
and [src/NB_LOAD_SILVER.Notebook/notebook-content.py](../../src/NB_LOAD_SILVER.Notebook/notebook-content.py)),
with `start_notebook_run()` in an early cell and `end_notebook_run(run_guid, "Succeeded")` in the
last one. A `%run` failure aborts the whole notebook immediately — the cells after it, including
that closing `end_notebook_run` call, never execute.

**Practical consequence: a `NB_LOAD_SILVER`/`NB_LOAD_GOLD` run that fails mid-chain leaves its
`audit.NotebookRun` row stuck at `Status = 'Running'` forever, never `'Failed'`.** The job itself
still reports failed to Fabric (so `PL_LOAD_GOLD`/`PL_LOAD_SILVER` and `PL_RUN_ALL` see the
failure correctly), but if you're triaging from the audit table alone, **don't just check for
`Failed` rows** — a run that silently never completed will hide as `Running`. Check for rows stuck
in `Running` well past that stage's normal duration too:

```sql
SELECT [NotebookName], [RunGuid], [Status], [StartTimeUtc], [EndTimeUtc], [ErrorMessage]
FROM [audit].[NotebookRun]
WHERE [Status] = 'Running'
  AND [StartTimeUtc] < DATEADD(MINUTE, -30, SYSUTCDATETIME())
ORDER BY [StartTimeUtc] DESC;
```

(30 minutes is a starting point, not a rule — size it to how long Gold/Silver normally takes for
that client's data volume.) A shared fix for this — a `try`/`finally` wrapper or a single helper
notebooks call instead of raw `%run` — is tracked but not built yet; see
[Improvement-Roadmap](Improvement-Roadmap.md).

## Troubleshooting playbook

Each of these was a real incident, not a hypothetical. Symptom → real cause → fix.

### 1. Generic wrapped error: "System cancelled the Spark session due to statement execution failures" / `System_Cancelled_Session_Statements_Failed`

**Symptom:** A deploy or a pipeline run fails with this message (or the same thing spelled out as
an error code) and nothing more useful.

**Real cause:** This is a Fabric-level wrapper. It fires for almost *any* underlying Python
exception in a notebook cell — a stale capacity name, a cold SQL database, a genuine code bug —
and they all look identical from the outside. It is never itself the diagnosis, only the starting
point.

**Fix — two ways to actually find the real cause, both used repeatedly while building this
framework:**
- Pull the notebook's Livy session list and read `cancellationReason`:
  `GET https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/notebooks/{notebookId}/livySessions`
  (or `POST` to start one interactively if you need a live session to attach to).
- **Most reliable:** reproduce the failing notebook's logic locally in plain Python against the
  real Fabric REST API, authenticated with an `az` CLI token:
  ```powershell
  az login
  $token = az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv
  ```
  Several real bugs in this exact framework (the malformed dual-`META` parameter cells, the
  missing-`id`-in-create-response item-creation flakiness) were only root-caused this way — the
  headless `RunNotebook` job path hides the real exception, but a plain `requests` call against the
  same endpoint from your own machine doesn't.

### 2. `InvalidExternalReferenceConnection: Invalid datasourceObjectId: <blank>` on a `PL_INGEST_*` Lookup activity

**Symptom:** An ingest pipeline's `Lookup` activity (querying `ingestion.vw_ActiveIngestTables`)
fails with this error, `datasourceObjectId` empty.

**Real cause:** `config/environments.yaml`'s `metadata_connection_guid` is empty for that
environment. This is the one genuine chicken-and-egg case in the whole metadata model — the
Connection used to *reach* the metadata database can't itself be a row *inside* that database, so
it has to be registered manually and pasted into git config.

**Fix:** see [Deployment-Guide](Deployment-Guide.md) for the one-time manual Connection bootstrap
(register a Connection to `SQL_METADATA_DATABASE` in the Ingestion workspace, paste its GUID into
`metadata_connection_guid`, redeploy). Until that's done, `NB_DEPLOY` deploys every `PL_INGEST_*`
pipeline anyway (with a warning), but every one of their Lookups fails at runtime exactly this way.

### 3. Lookup against a `WorkspaceIdentity`-credentialed Connection to a `FabricSqlDatabase` source fails with `InvalidToken`/`Unauthorized`

**Symptom:** A `Lookup` activity against a Fabric Connection to a `FabricSqlDatabase` source
(including the metadata catalog's own Connection) fails with an unauthorized/invalid-token error,
even though the identity demonstrably has DB access (verified directly via `pyodbc` in this
framework's own testing).

**Real cause:** This is a confirmed, unfixable Fabric platform bug in how `WorkspaceIdentity`
credential resolution works for `Lookup` activities against `FabricSqlDatabase` sources — not a
permissions gap, not a config mistake. Don't spend time debugging role assignments here.

**Fix:** use a **service-principal**-credentialed Connection instead of Workspace Identity. That
credential path sidesteps the bug entirely and is what this framework actually ships — the
metadata Connection bootstrap in [Deployment-Guide](Deployment-Guide.md) explicitly calls this
out. Don't try WorkspaceIdentity here even though it looks like the more "native" choice.

### 4. Capacity contention running all 8 `EP_INGEST_*` in parallel

**Symptom:** Either pipelines sit queued with zero job instances for 35+ minutes with nothing
visibly happening, or you get an explicit `TooManyRequestsForCapacity` / HTTP 430 error:
`"Spark job can't be run because you've hit spark overall capacity compute limit"`.

**Real cause:** `PL_RUN_ALL` fires all 8 `EP_INGEST_<TYPE>` activities at once (see above) — on a
small capacity (an F2 SKU was enough to trigger this live), that's genuine contention, not a bug.
A client on a comparably small SKU will hit this on day one if they have real data behind more
than a couple of the 8 connector types.

**Fix:** this is a capacity-sizing problem, not a framework defect — size the target capacity to
the client's actual parallel ingest workload. If a bigger SKU isn't an option, reduce fan-out
(there's no config knob for this yet; see [Improvement-Roadmap](Improvement-Roadmap.md) for the
proposed wave-batching fix). Don't burn time debugging the pipeline JSON itself for this one.

### 5. A deploy that worked before suddenly fails on the very first capacity-assignment step

**Symptom:** `NB_DEPLOY` fails immediately, early in the workspace-provisioning phase, on a client
environment that has deployed cleanly before.

**Real cause:** `NB_DEPLOY` resolves the target capacity **by name** from
`config/environments.yaml` and calls `assignToCapacity` for **every** workspace on **every** run —
even ones that already exist and are already correctly assigned. If that capacity was paused,
renamed, or replaced in the Fabric Admin portal (trial capacities expire; clients rename SKUs),
every subsequent deploy breaks the same way until config catches up.

**Fix:** `GET https://api.fabric.microsoft.com/v1/capacities` and confirm the capacity named in
`config/environments.yaml` still exists under that exact name and is `Active`. Update
`environments.yaml`'s `capacity` field to match reality and redeploy. See
[Deployment-Guide](Deployment-Guide.md) for the full deploy-failure diagnosis path — this is the
single most common way a previously-working deploy starts failing.

### 6. Azure SQL error 40613 ("database not currently available, retry") on first connection

**Symptom:** The very first connection attempt to a freshly created (or long-idle)
`SQL_METADATA_DATABASE` fails with SQL error 40613.

**Real cause:** Standard Azure SQL warm-up/resume behavior — transient, not a config or code
issue. Hit this on the first deploy attempt in *every single environment* tested while building
this framework.

**Fix:** just retry the deploy. No investigation needed. (A built-in retry loop for this specific
case is on the roadmap — see [Improvement-Roadmap](Improvement-Roadmap.md) — but isn't built yet,
so expect to retry by hand today.)

### 7. Data looks stale or missing right after a notebook reports "Completed"

**Symptom:** `NB_LOAD_BRONZE`/`NB_LOAD_GOLD`/`NB_LOAD_SILVER` reports success, but querying the
table through a lakehouse's **SQL analytics endpoint** shows old row counts or nothing at all.

**Real cause:** The SQL analytics endpoint syncs from the underlying Delta table asynchronously,
and the lag is real — anywhere from a few seconds to (for a freshly created lakehouse
specifically) several minutes. This isn't a silent pipeline failure.

**Fix:** don't conclude the run failed from the SQL endpoint alone. Either re-query after a short
wait, or check the Delta table directly (row count via Spark, or the table's own history) instead
of going through the SQL endpoint. For a brand-new lakehouse, prefer reading the underlying OneLake
parquet/Delta files directly if you need a fast, reliable confirmation.

### 8. `NB_LIST_LANDING_ENTITIES` can't be deleted via the Fabric API

**Symptom:** A `DELETE` call against this specific item consistently returns
`400 {"errorCode":"UnknownError","isRetriable":false}`, no matter how many times you retry.

**Real cause:** A reproducible platform quirk on this specific item — not a permissions problem,
not something wrong with your deploy. This notebook was an earlier ingestion-bridge design that
has since been replaced (ingestion now uses a native pipeline `Lookup` against
`ingestion.vw_ActiveIngestTables` — see [Architecture](Architecture.md)) and is no longer part of
`src/` or `config/items.yaml` in this repo. If you're operating an environment that still has it
as a leftover orphan item from an earlier deploy, it's dead and disconnected from every pipeline —
safe to ignore. See [Improvement-Roadmap](Improvement-Roadmap.md) for the recommendation to keep
it out of source entirely going forward.

### 9. `[REQUIRES_SINGLE_PART_NAMESPACE] spark_catalog requires a single-part namespace`

**Symptom:** A notebook reads its **own** default/attached lakehouse via a 3-part reference
(e.g. `dim_customer`, whose own default lakehouse is Bronze, doing `FROM Bronze.dbo.customer`) and
fails with this Spark parser error.

**Real cause:** A genuine Spark/Fabric quirk, not a bug in this repo's code. A notebook's own
default lakehouse behaves as Spark's built-in `spark_catalog` (single-part namespace only) — you
*can* write to it with `saveAsTable("Bronze.dbo.customer")` using the 3-part name, but reading it
back the same way trips the parser. **Sibling lakehouses — any lakehouse other than the notebook's
own default — work fine with full 3-part names**; it's specifically self-reference that breaks.
(`load_dimension()`/`load_fact()` writing to Gold from `dim_customer`/`fact_signup`, a sibling
lakehouse in each case, needed no workaround at all.)

**Fix (current workaround):** read the notebook's own lakehouse table via a direct OneLake path
into a session-local temp view instead of a catalog reference:

```python
_data_ws_id = resolve_workspace_id(data_workspace_name())
_bronze_lh_id = resolve_lakehouse_id(_data_ws_id, "Bronze")
spark.read.format("delta").load(
    onelake_path(_data_ws_id, _bronze_lh_id, "Tables", "dbo/customer")
).createOrReplaceTempView("bronze_customer")
```

This pattern is currently copy-pasted per notebook rather than centralized into one helper — see
[Improvement-Roadmap](Improvement-Roadmap.md) for the plan to fold it into `NB_MONZA_FUNCTIONS`.

## Running one stage in isolation

You don't have to run the whole `PL_RUN_ALL` chain to debug one stage. Every notebook and pipeline
in Code/Ingestion is a normal Fabric item — trigger it directly through the Job Scheduler REST API
and poll the same way `NB_RUN_REMOTE_PIPELINE` does internally.

Get a token and find the workspace/item IDs (both are in the Fabric portal URL when the item is
open, or via `GET {FABRIC_API}/workspaces/{workspaceId}/items`):

```powershell
az login
$token = az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv
```

**Run a notebook directly** (e.g. `NB_LOAD_BRONZE`, skipping ingestion entirely to iterate on a
Bronze bug):

```powershell
curl -X POST `
  "https://api.fabric.microsoft.com/v1/workspaces/<CODE_WORKSPACE_ID>/items/<NB_LOAD_BRONZE_ITEM_ID>/jobs/instances?jobType=RunNotebook" `
  -H "Authorization: Bearer $token" -H "Content-Type: application/json" -d "{}"
```

The response body is empty; the job's status URL comes back in the response's `Location` header.
Poll that URL (`GET`, same bearer token) until `status` is `Completed`/`Failed`/`Cancelled` —
exactly the loop `NB_RUN_REMOTE_PIPELINE` runs for you inside `PL_RUN_ALL`.

**Run a pipeline directly** (e.g. `PL_INGEST_SQL` in the Ingestion workspace, bypassing the
`NB_RUN_REMOTE_PIPELINE` bridge and `PL_RUN_ALL` entirely) — same call, `jobType=Pipeline` instead:

```powershell
curl -X POST `
  "https://api.fabric.microsoft.com/v1/workspaces/<INGESTION_WORKSPACE_ID>/items/<PL_INGEST_SQL_ITEM_ID>/jobs/instances?jobType=Pipeline" `
  -H "Authorization: Bearer $token" -H "Content-Type: application/json" -d "{}"
```

**One gotcha if you try to pass a parameter this way** — e.g. targeting `NB_LOAD_BRONZE`'s
`bronze_entity_name` at a single table for a fast rerun: the Job Scheduler API's `parameters`
array is confirmed to be **silently ignored** for `RunNotebook` jobs on this tenant (found live
while building this framework — the notebook runs with its cell's hardcoded parameter default no
matter what you pass in the request body). `deploy/run_notebook.py` works around this by editing
the notebook's parameter-cell *definition* (`getDefinition` → decode → modify default → encode →
`updateDefinition`) immediately before triggering it, rather than trusting the API's `parameters`
field. If you need a targeted single-table rerun of `NB_LOAD_BRONZE`, either do the same
definition-edit dance, or temporarily open the notebook in the portal, set
`bronze_entity_name` by hand, run it interactively, then revert.
