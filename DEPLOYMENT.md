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
   `Keystone Integration (X)`, `Keystone Code (X)`
3. Creates `Landing` / `Bronze` / `Gold` lakehouses in Data (+ `Silver` if
   `config/environments.yaml` sets `include_silver: true` for that environment)
4. Creates the `SQL_METADATA_DATABASE` SQL Database in Integration and applies
   `config/metadata_schema.sql` (idempotent — safe to re-run)
5. Deploys every item in `config/items.yaml` into Code (notebooks, pipelines,
   one Variable Library) — including `sil_customer`, the hand-written,
   per-table Silver notebook, for `include_silver` environments — patching
   cross-item ID placeholders (e.g. a pipeline's `__NB_LOAD_BRONZE_ID__`) with
   the real IDs once they're known
6. Seeds `demodata/customer.csv` into `Landing/Files/customer/customer.csv`
   and registers a demo `ingestion.Connection` / `ingestion.Database` /
   `ingestion.Table` row (a File-type, full-load ingestion), so `PL_RUN_ALL`
   has something to load on a first run

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
   their workspaces, lakehouses, the metadata catalog, and every Code
   workspace item.
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

## Adding a real data source

`ingestion.Connection` / `ingestion.Database` / `ingestion.Table` are the only
rows `PL_INGEST_SQL`, `PL_INGEST_FILE`, and `NB_LOAD_BRONZE` need to pick up a
new table — no pipeline or notebook changes required for a source of a type
the framework already supports. One active `ingestion.Table` row drives that
table's entire Source -> Landing -> Bronze flow:

1. Register the actual Fabric Connection for your source (SQL Server, ADLS
   Gen2, etc.) in the Integration workspace, and insert its GUID into
   `ingestion.Connection.ConnectionGuid` with `ConnectionType = 'Sql'` or
   `'File'`
2. Insert an `ingestion.Database` row referencing that connection (the source
   database name for `Sql`, or the container/filesystem name for `File`)
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
