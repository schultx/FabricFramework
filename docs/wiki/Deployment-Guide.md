# Deployment Guide

This page is about the deploy mechanism itself — how `NB_DEPLOY` works phase by phase, how a run actually gets triggered, and the one-time manual steps a consultant has to do by hand for each new environment. For the full ordered checklist to stand up a brand-new client from a repo fork, see [New-Client-Onboarding](New-Client-Onboarding.md). For triaging a failed run, see [Operations-Guide](Operations-Guide.md).

## The deploy model: one notebook, git-synced

Monza deploys itself. There is exactly one notebook — `setup/NB_DEPLOY.ipynb` — and exactly one pipeline that runs it — `azure-pipelines.yml`. Every environment (workspaces, lakehouses, the metadata catalog SQL Database, ingestion pipelines, loader notebooks, the Gold function library) is created or updated by that one notebook, from whatever is currently on `main` in `schultx/FabricFramework`.

The first thing `NB_DEPLOY` does on every run is download a fresh copy of the repo:

```python
zip_url = f"https://github.com/{GITHUB_REPO}/archive/refs/heads/{GITHUB_BRANCH}.zip"
zip_bytes = requests.get(zip_url, timeout=REQUEST_TIMEOUT).content
```

`GITHUB_REPO = "schultx/FabricFramework"`, `GITHUB_BRANCH = "main"`. It unzips that archive in memory and reads `config/environments.yaml`, `config/lakehouses.yaml`, `config/items.yaml`, and `config/metadata_schema.sql` straight out of it — not from any local copy on disk, and not from whatever's checked out in the ADO agent. Every environment deploys from the same ref, so there's no local drift between dev/test/prod. This also means **a plain `git push` to `main` does nothing by itself** — nothing observes the push. Something still has to run `NB_DEPLOY`.

`NB_DEPLOY` uses plain `requests` + `notebookutils` throughout — deliberately **no `ms-fabric-cli`, no `sempy`**. `%pip install ms-fabric-cli` was found, during this framework's own live testing, to silently break `sempy.fabric`'s context provider for the rest of that Spark session. Avoiding both dependencies sidesteps that bug at the root instead of working around it — don't reintroduce either package into `NB_DEPLOY`.

## `NB_DEPLOY.ipynb` is generated — never hand-edit it

`setup/NB_DEPLOY.ipynb` is **generated output**. The source of truth is [`setup/build_nb_deploy.py`](../../setup/build_nb_deploy.py), which builds every cell in Python and writes the `.ipynb` JSON. If you need to change what a deploy does — add a phase, change a substitution token, fix a bug — edit `build_nb_deploy.py`, then regenerate:

```
python setup/build_nb_deploy.py
```

Run this from the repo root; it writes `setup/NB_DEPLOY.ipynb` in place. Commit the regenerated `.ipynb` alongside your `build_nb_deploy.py` change. Editing the `.ipynb` directly will work until the next regeneration silently overwrites it — don't do it.

## What `NB_DEPLOY` actually does, phase by phase

After the git download, deploy work is deliberately split across several small cells rather than one big cell. Headless `RunNotebook` jobs were found to enforce a per-statement execution ceiling well under a minute, and one cell doing every phase for every environment tripped it partway through, killing the whole session before any exception could even be caught. Splitting by phase keeps each cell's own work short regardless of how many environments or items are involved; state each later phase needs (workspace/lakehouse/notebook/pipeline IDs) is carried in an `env_state` dict keyed by environment name.

**Phase 1 — workspaces + capacity + roles.** For each target environment: resolve the Fabric capacity by name (`get_capacity_id`, `GET /capacities`, matched on `displayName`), then create-or-find the three workspaces `Monza Data (X)`, `Monza Ingestion (X)`, `Monza Code (X)` (`X` = that environment's `short` code), call `assignToCapacity` on **every** one of them, and apply `workspace_roles` per tier from `environments.yaml`.

**Phase 2 — lakehouses.** Create `Landing`, `Bronze`, `Gold` in Data (`always: true` in `config/lakehouses.yaml`), plus `Silver` if that environment's `include_silver: true`. Lakehouses are created via the dedicated `/lakehouses` endpoint with `creationPayload: {"enableSchemas": true}` — a plain `POST /items` with no creation payload produces a classic lakehouse where `CREATE SCHEMA lakehouse.dbo` fails outright, confirmed live.

**Phase 3 — metadata catalog SQL Database + schema.** Create-or-find `SQL_METADATA_DATABASE` in the Ingestion workspace, then apply `config/metadata_schema.sql` against it over ODBC (`pyodbc`, `ODBC Driver 18 for SQL Server`, token-based auth via `notebookutils.credentials.getToken`). The SQL is split into batches on any line whose stripped content is exactly `GO`. Idempotent — safe to re-run every deploy.

**Phase 4 — workspace folders.** Create the fixed folder structure: `Notebooks/` and `Pipelines/` in Code, `Pipelines/` in Ingestion, via Fabric's Folder REST API (`POST .../folders`). This API is **Preview** as of this writing — Microsoft's own docs call it "not recommended for production use." `NB_DEPLOY` only uses it on the CREATE path: a brand-new item gets `folderId` set at creation; an item created before this repo added folders is never migrated. `VAR_MONZA`, the lakehouses, and `SQL_METADATA_DATABASE` deliberately stay at workspace root.

**Phase 5 — notebooks.** Every `Notebook`-type row in `config/items.yaml` deploys into its target workspace (`workspace: Code` by default) via `get_or_create_item`, which round-trips `notebook-content.py` as base64 `definitionParts` — `POST /items` on first create, `updateDefinition` on redeploy. Two placeholder tokens get substituted into every notebook's source: `__BRONZE_LAKEHOUSE_ID__` and `__DATA_WORKSPACE_ID__` (this is what binds a loader notebook's default lakehouse to Bronze, which turns on OneLake Spark Catalog for the whole Data workspace so three-part names like `Landing.x.y` resolve without a separate lakehouse binding per notebook).

**Phase 6 — pipelines.** Every `DataPipeline` row (except `PL_RUN_ALL`, handled separately at the end of this phase) deploys the same way, with a larger substitution set — `__NB_LOAD_BRONZE_ID__`, `__NB_LOAD_GOLD_ID__`, `__METADATA_CONNECTION_GUID__`, `__DATA_WORKSPACE_ID__`, `__LANDING_LAKEHOUSE_ID__`, `__CODE_WORKSPACE_ID__`, plus `__NB_LOAD_SILVER_ID__` when `include_silver` is set. `__METADATA_CONNECTION_GUID__` comes straight from that environment's `metadata_connection_guid` in `environments.yaml` — see the walkthrough below; if it's blank, this phase prints a warning and deploys anyway rather than failing the run. `PL_RUN_ALL` is built last: its substitutions wire in every `PL_INGEST_*` pipeline ID plus `NB_RUN_REMOTE_PIPELINE`'s notebook ID (the cross-workspace bridge — Fabric's legacy `ExecutePipeline` activity can't reference a pipeline in a different workspace, so `PL_RUN_ALL` calls a same-workspace notebook that itself triggers the Ingestion-workspace pipeline via the Job Scheduler API and blocks until it finishes). For `include_silver` environments, an `EP_LOAD_SILVER` activity is spliced into `PL_RUN_ALL`'s activity list in Python before upload, between `EP_LOAD_BRONZE` and `EP_LOAD_GOLD`, rather than maintaining a second committed pipeline file.

**Phase 7 — variable library + demo data seed.** Deploy `VAR_MONZA` (the one Variable Library), upload `demodata/customer.csv` to `Landing/Files/customer/customer.csv` via direct OneLake DFS calls (`PUT ?resource=file`, `PATCH ?action=append`, `PATCH ?action=flush`), then insert one demo row each into `ingestion.Connection` / `ingestion.Database` / `ingestion.Table` (File type, `LoadType = 'Full'`) so `PL_RUN_ALL` has something to load on a first run. The demo `Connection` row is inserted with `IsActive = 0` deliberately — there's no real Fabric Connection behind its `NEWID()` GUID, so `ingestion.vw_ActiveIngestTables` (which filters `IsActive = 1`) correctly skips it for `PL_INGEST_FILE`'s Lookup, while `NB_LOAD_BRONZE` (which doesn't filter on `Connection.IsActive`) still picks the row up fine. See [Metadata-Model](Metadata-Model.md) for the full `ingestion` schema.

Every step above is create-if-missing / overwrite-content-if-exists. There's no separate "sync" step — re-running `NB_DEPLOY` after a push to `main` **is** how you redeploy.

## How a deploy actually gets triggered, day to day

`azure-pipelines.yml` does not build, package, or push anything itself. Its only job per environment is: start a run of the `NB_DEPLOY` notebook that's already living in Fabric, and wait for it to finish. All three stages (`Dev`, `Test`, `Prod`) run the same script, [`deploy/run_notebook.py`](../../deploy/run_notebook.py), differing only in the `--environment` flag and which ADO Environment gates the stage:

```
python deploy/run_notebook.py --environment development
python deploy/run_notebook.py --environment test
python deploy/run_notebook.py --environment production
```

`run_notebook.py` does two things, in order, every time it runs:

1. **Bakes the target environment into the live notebook's parameters cell.** This is the real gotcha to know about: the Fabric Job Scheduler API's own `parameters` payload on a `RunNotebook` job is **silently ignored** — confirmed live against a real tenant, no error returned, the notebook just runs with whatever its cell's hardcoded default already was. So `run_notebook.py` never uses that mechanism. Instead it does `getDefinition?format=ipynb` on the live notebook item, decodes the returned `.ipynb`, finds the code cell tagged `parameters`, regex-replaces `target_environments_csv = "..."` with the environment it was asked to deploy, re-encodes, and calls `updateDefinition` — **before every single run**. This is a genuinely different operation from a `git push`: pushing to `main` changes what's in the repo, which `NB_DEPLOY` will download on its *next* run, but it does **not** touch the live notebook item's own stored content or its parameter default. Only `updateDefinition` does that.
2. **Starts the run and polls it.** `POST /workspaces/{id}/items/{notebookId}/jobs/instances?jobType=RunNotebook`, then polls the returned `Location` every 15 seconds until it reaches `Completed`, `Failed`, `Cancelled`, or `Deduped` (up to a 60-minute timeout — a from-scratch run provisions whole workspaces).

If a run ends in anything other than `Completed`, `run_notebook.py` retries the *entire* trigger-and-poll cycle, up to `MAX_DEPLOY_ATTEMPTS = 5` times. This is intentional, not a bug being papered over — see the two known-failure sections below for why a `Failed` status is often transient rather than a real bug.

## One-time manual bootstrap

The very first run has to happen by hand, because Azure DevOps needs something already inside Fabric to call. Short version (full ordered checklist for a new client is in [New-Client-Onboarding](New-Client-Onboarding.md)):

1. Create a workspace by hand (e.g. `Monza Deploy`) — Contributor role for yourself is enough.
2. Download `setup/NB_DEPLOY.ipynb` from the repo and **Import notebook** into that workspace via the Fabric UI.
3. **Run all**, leaving `target_environments_csv` blank — this provisions all three environments (`development`, `test`, `production`) in one go.
4. Note the workspace ID and the notebook's item ID (notebook → **Settings**, or `GET /v1/workspaces/{workspaceId}/items`) — these become `FABRIC_WORKSPACE_ID` and `FABRIC_NOTEBOOK_ID`.
5. In ADO, create the variable group `fabric-framework-secrets` (Pipelines → Library) holding `FABRIC_CLIENT_ID`, `FABRIC_CLIENT_SECRET`, `FABRIC_TENANT_ID` (a service principal with Contributor on the target capacity), plus `FABRIC_WORKSPACE_ID` and `FABRIC_NOTEBOOK_ID` from step 4.
6. Create ADO Environments `fabricframework-dev`, `fabricframework-test`, `fabricframework-prod` (Pipelines → Environments). Add a required reviewer on `-test` and `-prod` — that's the approval gate. `-dev` needs none.

Note that today's `azure-pipelines.yml` points all three stages at this **one** shared variable group, i.e. one shared SPN and one shared `NB_DEPLOY` item/workspace drive every environment's deploy. The WIF section below is a recommended change to that, for blast-radius isolation.

After this, push to `main` and the pipeline does the rest — Dev automatically, Test and Prod after their approvals.

## `config/environments.yaml`, field by field

Every environment `NB_DEPLOY` can target is one entry under `environments:`:

| Field | Controls |
|---|---|
| `name` | The value matched against `target_environments_csv` (`development` / `test` / `production`). This is what scopes a deploy run to one environment. |
| `short` | The single letter baked into workspace display names: `Monza Data (D)`, `Monza Ingestion (T)`, `Monza Code (P)`, etc. |
| `capacity` | The Fabric capacity's `displayName`, resolved via `GET /capacities` and assigned to every workspace in this environment on **every** deploy run. See the capacity section below — this is a real trap. |
| `include_silver` | `true`/`false`. Turns the `Silver` lakehouse, `PL_LOAD_SILVER`, `NB_LOAD_SILVER`, and any `requires_silver: true` item in `config/items.yaml` on or off for this environment. Leave `false` for a fresh clone unless you already have a Bronze entity genuinely reused by 2+ Gold objects — the `true` currently set on all three environments in this repo is deliberately kept live end-to-end as a working example (`silver.customer`), not leftover test config. |
| `metadata_connection_guid` | The GUID of the Fabric Connection every `PL_INGEST_*` pipeline's native `Lookup` activity uses to reach `SQL_METADATA_DATABASE`. Blank (`""`) until the one-time manual step below is done for that environment. |

`environments.yaml` also holds `workspace_roles` (per-tier Entra principals granted access to Data/Ingestion/Code) as a sibling top-level key, not per-environment — out of scope for this page.

## One-time manual step: `metadata_connection_guid`

This is the one genuine chicken-and-egg case in the whole metadata model. Every *other* source Connection a `PL_INGEST_*` pipeline reads is a row inside `ingestion.Connection` — queryable once you're already connected to the metadata database. But the Connection used to *reach* the metadata database in the first place can't be a row inside the database it's reaching. It has to live outside it, in git config, which is why `metadata_connection_guid` exists as a plain string in `environments.yaml` rather than another catalog row.

**Today, this field is blank (`""`) in all three environments in this repo.** Pipelines deploy fine with it blank — Phase 6 just prints a warning — but every `PL_INGEST_*` pipeline's `Lookup` activity against `ingestion.vw_ActiveIngestTables` will fail at runtime until it's set. Do this once per environment, after that environment's first `NB_DEPLOY` run (so `SQL_METADATA_DATABASE` and the `Monza Ingestion (X)` workspace already exist):

1. Open the Fabric portal, go to the `Monza Ingestion (X)` workspace for the environment you're setting up.
2. **New item → Connection** (or **Manage connections and gateways** → New). Point it at the `SQL_METADATA_DATABASE` SQL Database that's already in that workspace.
3. Credential it with a **service principal** — do **not** use Workspace Identity. This is a confirmed, unfixable Fabric platform bug hit during this project: a Workspace Identity-credentialed Connection produces `InvalidToken`/`Unauthorized` errors specifically on the combination of a native `Lookup` activity and a `FabricSqlDatabaseSource` dataset. A service-principal credential authenticates through a different path and sidesteps the bug entirely. It needs the same Contributor-or-higher role on the Ingestion workspace that the deploying identity already has.
4. Copy the new Connection's GUID.
5. Paste it into `metadata_connection_guid` for that environment in `config/environments.yaml`, commit, and push to `main`. Let the pipeline redeploy (or re-run `NB_DEPLOY` by hand) — Phase 6 will pick it up and stop warning.

Repeat once per environment — the GUID is different in each `Monza Ingestion (X)` workspace.

## Setting up Workload Identity Federation (WIF) for CI/CD

The service principal(s) behind `FABRIC_CLIENT_ID`/`FABRIC_CLIENT_SECRET`/`FABRIC_TENANT_ID` are what actually run every deploy. The recommended pattern for a new client engagement is **three separate ADO service connections, one per environment**, rather than the single shared SPN the bootstrap steps above default to — this keeps a mistake or a leaked credential in Dev from having any reach into Prod.

1. In ADO: **Project Settings → Service connections → New service connection → Azure Resource Manager**.
2. Choose **Workload Identity federation (automatic)** as the authentication method — ADO creates the Entra app registration and its federated credential for you, so no client secret has to be generated or stored for this SPN's ADO-facing identity.
3. Scope it to the subscription/resource group your Fabric capacity lives in.
4. Name it per environment, e.g. `fabricframework-dev-wif`, `fabricframework-test-wif`, `fabricframework-prod-wif`. Repeat for all three.
5. Grant each resulting SPN Contributor on that environment's target Fabric capacity (matching what the bootstrap variable-group SPN needed), and once that environment's workspaces exist, the matching workspace roles.
6. Note each service connection's Application (client) ID and Directory (tenant) ID — these become that environment's `FABRIC_CLIENT_ID` / `FABRIC_TENANT_ID`.

One nuance: Fabric Connection objects (like the `metadata_connection_guid` Connection above) don't support WIF-style federated auth themselves — they need an actual client secret. WIF and a stored secret aren't mutually exclusive on the same app registration, though: the same per-environment SPN created in step 2 can *also* have a plain secret added under its Entra app registration (**Certificates & secrets → New client secret**), and that secret is what you hand to the `SQL_METADATA_DATABASE` Connection's service-principal credential in the walkthrough above. One SPN, two auth paths, used for two different things.

## Capacity assignment and the stale-capacity trap

`NB_DEPLOY` resolves the target capacity **by name**, every run, for every environment:

```python
def get_capacity_id(capacity_name: str) -> str:
    resp = api("GET", "/capacities")
    matches = [c for c in resp.json()["value"] if c["displayName"] == capacity_name]
    if len(matches) != 1:
        raise ValueError(f"Expected exactly one capacity named '{capacity_name}', found {len(matches)}")
    return matches[0]["id"]
```

and then calls `assignToCapacity` on every one of the three workspaces (Data/Ingestion/Code) for that environment — **including workspaces that already exist** — in Phase 1, on every single deploy run, not just on first creation.

**Flag this clearly for the client:** if the capacity named in `environments.yaml`'s `capacity` field is later paused, renamed, or swapped for a different one (for example, moved to a fresh trial capacity, as happened live during this project — see the `586f616` commit, "Point environments at the new trial capacity") without `environments.yaml` being updated to match, every subsequent deploy run fails. The failure you actually see is **not** a helpful "capacity not found" message — it's a generic, unhelpful `System cancelled the Spark session due to statement execution failures` error that gives no hint the real cause is a stale capacity name. Current value in this repo for all three environments is `Trial-20260916T102156Z-SXyTKCCwy06fHGjMeQxDSA` — if the client's trial or paid capacity ever gets renamed or replaced, update this field in the same commit, in all three environment blocks.

## Known transient failure: SQL error 40613

On the very first connection to a freshly-created (or long-idle) `SQL_METADATA_DATABASE`, Azure SQL sometimes returns `database not currently available, retry` (error 40613) from `run_metadata_schema`'s `pyodbc.connect` call in Phase 3. This is not a code bug — it resolves on a simple retry of the same deploy run. `run_notebook.py`'s built-in retry loop (`MAX_DEPLOY_ATTEMPTS = 5`) already covers this automatically; if you're running `NB_DEPLOY` manually from the Fabric UI instead, just **Run all** again.

## Where to go next

- [New-Client-Onboarding](New-Client-Onboarding.md) — the full ordered checklist for standing up a brand-new client from this repo, including this bootstrap and the WIF/`metadata_connection_guid` steps in sequence.
- [Operations-Guide](Operations-Guide.md) — troubleshooting a failed or stuck deploy run in more depth.
- [Metadata-Model](Metadata-Model.md) — the `ingestion` schema Phase 3 and Phase 7 populate.
- [Connector-Types](Connector-Types.md) — the `ConnectionType` values Phase 6's `PL_INGEST_*` pipelines are built around.
