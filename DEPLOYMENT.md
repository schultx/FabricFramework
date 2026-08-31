# Deployment: one notebook, triggered from Azure DevOps

This repo deploys itself. There is exactly one notebook to run —
[`setup/NB_DEPLOY.ipynb`](setup/NB_DEPLOY.ipynb) — and exactly one pipeline that
runs it — [`azure-pipelines.yml`](azure-pipelines.yml). Everything else
(workspaces, lakehouses, the metadata catalog SQL Database, ingestion
pipelines, loader notebooks, the Gold function library) is created or updated
by that notebook, from whatever is currently on the `main` branch of this
repo.

`NB_DEPLOY` uses plain `requests` + `notebookutils` throughout — no
`ms-fabric-cli`, no `sempy`. (`%pip install ms-fabric-cli` was found, during
this framework's own live testing, to silently break `sempy.fabric`'s context
provider for the rest of that Spark session. Avoiding both dependencies
sidesteps the bug at the root instead of working around it.)

## What it does, per environment

1. Resolves the target Fabric capacity
2. Creates/updates the three workspaces: `Keystone Data (X)`,
   `Keystone Ingestion (X)`, `Keystone Code (X)`
3. Creates `Landing` / `Bronze` / `Gold` lakehouses in Data (+ `Silver` if
   `config/environments.yaml` sets `include_silver: true` for that environment)
4. Creates the `SQL_METADATA_DATABASE` SQL Database in Ingestion and applies
   `config/metadata_schema.sql` (idempotent — safe to re-run)
5. Creates a small, fixed folder structure in Code (`Notebooks/`,
   `Pipelines/`) and Ingestion (`Pipelines/`) — see
   [Workspace folders](#workspace-folders) below. Uses Fabric's Folder REST
   API, which is **Preview** as of this writing; best-effort by design (see
   that section)
6. Deploys every item in `config/items.yaml` into its target workspace and
   folder (notebooks, pipelines, one Variable Library) — every `PL_INGEST_*`
   pipeline goes into Ingestion, alongside the metadata catalog and source
   Connections it reads; everything else (including `sil_customer`, the
   hand-written, per-table Silver notebook, for `include_silver`
   environments) goes into Code — patching cross-item ID placeholders (e.g.
   a pipeline's `__NB_LOAD_BRONZE_ID__`) with the real IDs once they're known
7. Seeds `demodata/customer.csv` into `Landing/Files/customer/customer.csv`
   and registers a demo `ingestion.Connection` / `ingestion.Database` /
   `ingestion.Table` row (a File-type, full-load ingestion), so `PL_RUN_ALL`
   has something to load on a first run

### Workspace folders

The Code workspace gets `Notebooks/` (every loader + Gold/Silver notebook,
`NB_KEYSTONE_FUNCTIONS`, `NB_RUN_REMOTE_PIPELINE`) and `Pipelines/`
(`PL_LOAD_*`, `PL_RUN_ALL`); the Ingestion workspace gets `Pipelines/` (every
`PL_INGEST_*`). `VAR_KEYSTONE`, the lakehouses, and `SQL_METADATA_DATABASE`
stay at workspace root — not everything needs a folder.

Fabric's Folder REST API (`POST .../folders`, and the `folderId` field on
item creation) is **Preview** — Microsoft's own docs mark it "provided for
evaluation and development purposes only ... not recommended for production
use," and the shape may still change. `NB_DEPLOY` only relies on it for the
CREATE path: a brand-new item gets `folderId` set at creation time, but an
item created before this feature existed (or before this repo added it) is
never moved — no migration logic. If the API shape changes upstream, the
worst case is items landing at workspace root again, not a broken deploy.

`PL_RUN_ALL` (Code) triggers every `PL_INGEST_*` pipeline (Ingestion) through
a bridge notebook, `NB_RUN_REMOTE_PIPELINE`, rather than a direct pipeline
reference: Fabric's legacy `ExecutePipeline` activity only supports pipelines
in the *same* workspace as the caller, so `PL_RUN_ALL` calls
`NB_RUN_REMOTE_PIPELINE` once per `PL_INGEST_*` pipeline (same workspace,
trivial, and all of these `EP_INGEST_*` stages run in parallel) and that
notebook triggers the real cross-workspace pipeline run itself via the Job
Scheduler REST API, blocking until it finishes. `EP_LOAD_BRONZE` then depends
on every `EP_INGEST_*` stage succeeding before it runs.

Re-running it after a push to `main` is how you redeploy — there's no
separate "sync" step, and every step is safe to run again (create-if-missing,
overwrite-content-if-exists).

## One-time manual bootstrap

The very first run has to happen by hand, because Azure DevOps needs
something already inside Fabric to call:

1. Create a workspace (e.g. `Keystone Deploy`) — any workspace, Contributor
   role is enough for you personally at this point.
2. Download `setup/NB_DEPLOY.ipynb` from this repo and import it into that
   workspace (Fabric UI → **Import notebook**).
3. **Run all**, leaving the `target_environments_csv` parameter blank. This
   provisions all three environments (`development`, `test`, `production`) —
   their workspaces, lakehouses, the metadata catalog, and every item in
   `config/items.yaml` (Code and Ingestion workspaces alike).
4. Note the workspace ID and this notebook's item ID (Fabric UI → notebook →
   **Settings**, or `GET /v1/workspaces/{workspaceId}/items`) — these become
   `FABRIC_WORKSPACE_ID` and `FABRIC_NOTEBOOK_ID` below.

Everything after this step is driven from Azure DevOps.

## Wiring up Azure DevOps

`azure-pipelines.yml`'s only job, per environment, is to start a run of the
notebook you just imported and wait for it to finish. The actual deployment
logic stays entirely inside the notebook (and therefore entirely inside this
repo, versioned like everything else).

1. **Variable group** `fabric-framework-secrets` (Pipelines → Library), holding:
   - `FABRIC_CLIENT_ID`, `FABRIC_CLIENT_SECRET`, `FABRIC_TENANT_ID` — a service
     principal with Contributor on the target capacity
   - `FABRIC_WORKSPACE_ID`, `FABRIC_NOTEBOOK_ID` — from the bootstrap step above

2. **ADO Environments** (Pipelines → Environments): `fabricframework-dev`,
   `fabricframework-test`, `fabricframework-prod`. Add a required reviewer on
   `-test` and `-prod` — that's the approval gate; `-dev` needs none.

3. Push to `main`. The pipeline runs three stages in order — Dev, Test, Prod —
   each calling [`deploy/run_notebook.py`](deploy/run_notebook.py) with
   `--environment development|test|production`. That script starts the
   notebook via the Fabric
   [Job Scheduler API](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler)
   (`POST .../jobs/instances?jobType=RunNotebook`), then polls until the run
   completes or fails.

`target_environments_csv` is why one notebook can still support staged
promotion: the parameters cell near the top of `NB_DEPLOY.ipynb` filters
`environments.yaml` down to just the environment named in that parameter
before anything gets created. The Job Scheduler's own `parameters` array is
silently ignored for `RunNotebook` jobs (confirmed live against a real
tenant) — so `run_notebook.py` bakes the value directly into that cell via
`getDefinition` → decode → modify → encode → `updateDefinition` immediately
before each run. Run the notebook with no parameter (as the manual bootstrap
does) and it deploys all three environments at once; run it from the
pipeline with one environment at a time and each stage only touches its own
workspaces.

## Redeploying

Change anything under `src/` or `config/`, push to `main`, and the pipeline
does the rest — Dev automatically, Test and Prod after their approvals. No
re-import, no second notebook.

## One-time metadata Connection bootstrap (per environment)

Every `PL_INGEST_*` pipeline's `Lookup` activity queries the metadata catalog
(`ingestion.vw_ActiveIngestTables`) directly through a native `FabricSqlDatabaseSource`
— no notebook bridge, no `TridentNotebook` cold start. That Lookup needs its
own Fabric Connection to `SQL_METADATA_DATABASE`, and this is the one genuine
chicken-and-egg case in the whole metadata model: every *other* source
Connection is a row inside `ingestion.Connection`, queryable once you're
already connected to the metadata database — but the Connection used to reach
the metadata database in the first place can't live inside the database it's
reaching. It has to live in git config instead
(`config/environments.yaml`'s `metadata_connection_guid`).

This is a one-time manual step per environment, done after that environment's
first `NB_DEPLOY` run (so `SQL_METADATA_DATABASE` and the Ingestion workspace
already exist):

1. In the Fabric portal, open the `Keystone Ingestion (X)` workspace and
   register a new Connection to `SQL_METADATA_DATABASE` (**New item →
   Connection**, or via **Manage connections and gateways**)
2. Credential it with a **service principal**, not Workspace Identity. An
   earlier attempt at this exact pattern used a Workspace Identity-credentialed
   Connection and hit a confirmed, unfixable Fabric platform bug —
   `InvalidToken`/`Unauthorized` errors specifically on the combination of a
   `Lookup` activity and a `FabricSqlDatabaseSource` dataset. A
   service-principal credential authenticates through a different path and
   sidesteps that bug entirely; it needs the same Contributor-or-higher role
   on the Ingestion workspace that the rest of this framework's deployment
   identity already has
3. Copy the new Connection's GUID and paste it into
   `metadata_connection_guid` for that environment in
   `config/environments.yaml`, then push to `main` and let the pipeline
   redeploy (or re-run `NB_DEPLOY` by hand)

Until this is done, `metadata_connection_guid` stays `""`, `NB_DEPLOY` prints
a warning during Phase 6 (pipelines) but still deploys every `PL_INGEST_*`
pipeline — their `Lookup` activities just won't resolve a Connection and will
fail at runtime until the GUID is filled in.

## Adding a real data source

`ingestion.Connection` / `ingestion.Database` / `ingestion.Table` are the only
rows every `PL_INGEST_*` pipeline and `NB_LOAD_BRONZE` need to pick up a new
table — no pipeline or notebook changes required for a source of a type the
framework already supports. One active `ingestion.Table` row drives that
table's entire Source -> Landing -> Bronze flow.

**Supported `ConnectionType` values** (each has its own `PL_INGEST_<TYPE>`
pipeline in the Ingestion workspace, all following the same native-Lookup
pattern):

| ConnectionType | Source | Notes |
|---|---|---|
| `Sql` | Azure SQL Database | |
| `File` | ADLS Gen2 (or any Blob-FS-compatible store) | |
| `SqlMI` | Azure SQL Managed Instance | |
| `Oracle` | Oracle | Routed through an on-premises Data Gateway — gateway config lives on the Fabric Connection object itself, not in this catalog |
| `Sftp` | SFTP server | |
| `Ftp` | FTP server | |
| `OneLakeTable` | A Delta table in another Fabric workspace/lakehouse | No Fabric Connection object involved — see the `ConnectionType` comment in `config/metadata_schema.sql` for how `ConnectionGuid`/`Database.Name` are repurposed as the source workspace/lakehouse GUID |
| `OneLakeFile` | A file in another Fabric workspace/lakehouse's Files section | Same repurposing as `OneLakeTable` |
| `Custom` | Anything with no dedicated connector (REST APIs, SharePoint, Dataverse, Salesforce, etc.) | Escape hatch — see **Custom sources** below, not part of the generic Lookup-driven model |

`ADF` (FMD Framework's pass-through metadata tracking for an
externally-orchestrated ADF pipeline) was deliberately not ported — it isn't
a real data connector, and doesn't fit this framework's self-contained model
where every ingestion runs from inside Keystone's own pipelines.

To add a table for any non-`Custom` type above:

1. Register the actual Fabric Connection for your source in the Ingestion
   workspace — the same workspace every `PL_INGEST_*` pipeline itself deploys
   into — and insert its GUID into `ingestion.Connection.ConnectionGuid` with
   the matching `ConnectionType` (see the table above and the DDL comment in
   `config/metadata_schema.sql` for what `ConnectionGuid`/`Database.Name` mean
   for each type — `OneLakeTable`/`OneLakeFile` don't use a Connection object
   at all)
2. Insert an `ingestion.Database` row referencing that connection
3. Insert an `ingestion.Table` row describing the object to copy, where it
   lands, its Bronze schema/name, its `PrimaryKeys`, `LoadType`
   (`'Full'` or `'Delta'`, with `IncrementalColumn` for Delta), and
   `DeleteHandling` (`'None'` / `'SoftDelete'` / `'Reconcile'`, Delta only) —
   `PL_LOAD_BRONZE` picks it up on its next run
4. Only if the cleansed shape will be reused by 2+ Gold objects: hand-write a
   `sil_<name>.Notebook` (mirroring `dim_customer.Notebook`'s shape — read
   Bronze via direct OneLake path into a temp view, `%%sql` transform, write
   to Silver), add it to `config/items.yaml` with `requires_silver: true`,
   `%run` it from `NB_LOAD_SILVER.Notebook`, and set `include_silver: true`
   for that environment in `config/environments.yaml`

### Custom sources

`ConnectionType = 'Custom'` is the escape hatch for a source with no
dedicated connector — a REST API, SharePoint, Dataverse, Salesforce, or
anything else a generic `Copy` activity can't reach. It deliberately doesn't
fit the Lookup-driven generic-pipeline model above, so it isn't forced into
one: `ingestion.vw_ActiveIngestTables` excludes `ConnectionType = 'Custom'`
rows entirely (no `PL_INGEST_*` Lookup will ever pick one up), and instead you
hand-write a per-table notebook, the same way Silver and Gold already work:

1. Write a `custom_<name>.Notebook` mirroring `dim_customer.Notebook` /
   `sil_customer.Notebook`'s shape (`%run NB_KEYSTONE_FUNCTIONS`, then
   whatever the source needs — call a REST API, page through results, land
   the result as parquet in `Landing/Files/<path>` for `NB_LOAD_BRONZE` to
   pick up normally, or write straight to Bronze yourself if there's no
   cleansing step worth sharing with the generic path)
2. Add it to `config/items.yaml` (`workspace: Code`, same as every other
   hand-written notebook)
3. Insert an `ingestion.Table` row for it anyway, with
   `Connection.ConnectionType = 'Custom'` and `Table.CustomNotebookName` set
   to the notebook's name — this keeps the table documented in the catalog
   (for `ai.FeatureSet`, audits, etc.) even though no pipeline reads it
   automatically
4. Wire it into `PL_RUN_ALL` by hand: add a new `EP_INGEST_CUSTOM`
   `TridentNotebook` activity (`dependsOn: []`, same retry policy as the
   other `EP_INGEST_*` stages) that calls your notebook directly — *not*
   through `NB_RUN_REMOTE_PIPELINE`, since a Custom notebook deploys into
   Code, the same workspace `PL_RUN_ALL` itself runs in, so the
   cross-workspace bridge isn't needed. A dedicated stage (one per Custom
   source) was chosen over folding it into an existing stage so each source
   keeps its own independent retry/failure behavior, matching how every other
   `EP_INGEST_*` stage already runs in parallel and fails independently. If
   the notebook lands to `Landing/Files/...`, add its new stage to
   `EP_LOAD_BRONZE`'s `dependsOn` too, so `NB_LOAD_BRONZE` doesn't run ahead
   of it; if it writes straight to Bronze, that dependency isn't required,
   but it must still complete before `EP_LOAD_GOLD`
