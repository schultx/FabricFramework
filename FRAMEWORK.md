# Monza — framework overview (current state)

A lean, metadata-driven Microsoft Fabric data platform. Original implementation —
not built on FMD Framework or twoday's AquaVilla, though both informed the design.
Three workspaces per environment, one active metadata row drives each table's
whole ingestion flow, Silver/Gold are hand-written per-table notebooks.

## Workspaces (per environment: development / test / production)

| Workspace | Holds |
|---|---|
| `Monza Data (D/T/P)` | Lakehouses only: `Landing`, `Bronze`, `Gold`, `Silver` (only if `include_silver: true`) |
| `Monza Ingestion (D/T/P)` | `SQL_METADATA_DATABASE`, source Connections, every `PL_INGEST_*` pipeline |
| `Monza Code (D/T/P)` | Everything else: loader/orchestrator notebooks, `PL_LOAD_*`, `PL_RUN_ALL`, one Variable Library |

Code and Ingestion items are organized into `Notebooks/`/`Pipelines/` folders
(Fabric's Folder API — still Preview, so this is best-effort, not load-bearing).

## Medallion layers

- **Landing** — raw, unmodified, straight from source
- **Bronze** — generic, metadata-driven cleansing (dedupe, not-null, custom rules)
- **Silver** *(optional)* — hand-written per-table notebook, only when a Bronze
  entity is genuinely reused by 2+ Gold objects
- **Gold** — dimensions (SCD1/SCD2), facts, bridges — hand-written per-table
  notebooks, `%run`-chained by `NB_LOAD_GOLD`

## Metadata model — `ingestion` schema, three tables

`Connection` (source system) → `Database` (database/container within it) →
`Table` — **one active `Table` row drives that table's entire Source → Landing →
Bronze flow.** Not one row per pipeline stage.

Key `Table` columns: `LoadType` (`Full`/`Delta`), `IncrementalColumn`,
`DeleteHandling` (`None`/`SoftDelete`/`Reconcile`), `PrimaryKeys`,
`CleansingRules` (JSON), `CustomNotebookName` (for `Custom`-type sources).

Other schemas: `runtime.LoadWatermark` (Delta incremental state — now with a
CI/CD identity per environment, see below), `audit.PipelineRun`/`NotebookRun`
(now actually written to — see "Recently changed"), `ai.FeatureSet`.

## Ingestion — native Lookup, not notebooks

Each `PL_INGEST_*` pipeline's `Lookup` activity queries a SQL view,
`ingestion.vw_ActiveIngestTables`, directly — no notebook bridge. The view
resolves each active table's ready-to-run query in T-SQL (Full = `SELECT *`
or a custom override; Delta = the same, watermark-bounded). The `Lookup`
authenticates through a **service-principal**-credentialed Fabric Connection
(not Workspace Identity — that combination hit a confirmed Fabric platform
bug on `FabricSqlDatabase` sources).

**9 connector types**, each its own `PL_INGEST_<TYPE>` pipeline, all wired in
parallel under `PL_RUN_ALL`: `Sql`, `File`, `SqlMI`, `Oracle`, `Sftp`, `Ftp`,
`OneLakeTable`, `OneLakeFile` (cross-workspace Fabric-native), and `Custom`
(an escape hatch — hand-written notebook for sources with no dedicated
connector, e.g. REST APIs, SharePoint, Dataverse — explicitly excluded from
the generic Lookup view).

`PL_RUN_ALL` (Code) reaches every `PL_INGEST_*` (Ingestion) via a small
bridge notebook, `NB_RUN_REMOTE_PIPELINE` — Fabric's legacy pipeline-invoke
activity can't cross workspaces, so the bridge triggers the real
cross-workspace run via the Job Scheduler REST API and polls it to
completion.

## Orchestration (`PL_RUN_ALL`)

```
8x EP_INGEST_<TYPE>  (parallel)
        │
        ▼
   EP_LOAD_BRONZE
        │
        ▼
 [EP_LOAD_SILVER]     (only if include_silver)
        │
        ▼
   EP_LOAD_GOLD
```

## Deployment

One notebook, `setup/NB_DEPLOY.ipynb`, triggered by `azure-pipelines.yml`.
Re-downloads `src/`+`config/` from `main` on every run; every step is
create-if-missing/update-if-exists. Three ADO stages (dev auto, test/prod
behind required-reviewer approval gates).

## Recently changed this session

- Renamed **Stratum → Keystone → Monza** (two renames, both mechanical —
  workspaces, item names, docs); `Integration` workspace renamed **Ingestion**
  along the way
- Metadata schema rebuilt from an FMD-style one-table-per-stage design to the
  lean `Connection`/`Database`/`Table` hierarchy above, with real Full/Delta/
  delete-handling logic (not just schema)
- Silver converted from a generic metadata-loop notebook to per-table
  notebooks, matching Gold's existing pattern
- `audit.NotebookRun` is now actually written to; one bad Bronze table no
  longer aborts the whole run
- Ingestion pipelines moved from a notebook-bridge Lookup to native Lookup;
  6 new connector types added; workspace folders added
- Pre-deploy review (below) found and fixed two SQL robustness gaps in
  `vw_ActiveIngestTables`: a no-op `CONVERT` on an already-string watermark
  column, and unquoted source schema/table/column names spliced into
  generated SQL (now `QUOTENAME`d, so a reserved-word column name won't break
  a Lookup at runtime)

## Before you deploy again

A 5-agent cross-check of workspaces/lakehouses/folders, the metadata schema,
all 8 ingestion pipelines + `PL_RUN_ALL`'s wiring, the full deploy script, and
`items.yaml` vs. `src/` found **no deploy-breaking bugs** — every
`__TOKEN__` placeholder, every `EP_INGEST_*` dependency, every item-to-folder
mapping checked out. Two real (now fixed) issues and one pending item:

- ✅ **Fixed**: the two SQL robustness gaps above (`config/metadata_schema.sql`)
- ✅ **Fixed**: a small dead-code leftover in the deploy script (`ITEM_PAYLOAD_PATH`, unused)
- ⏳ **Still pending, your action**: `metadata_connection_guid` in
  `config/environments.yaml` is empty for all three environments — the deploy
  will succeed and print a warning, but every `PL_INGEST_*` Lookup won't
  resolve a Connection until you register one (portal, service-principal
  auth — see `DEPLOYMENT.md`) and I populate the GUID
- ⏳ **Still pending, your action**: Workload Identity Federation — waiting to
  hear whether the three service connections (`fabricframework-dev/test/prod-wif`)
  are set up yet
- ⚠️ **Not yet live-tested**: the original Full/Delta Bronze logic, Gold SCD
  facade, and demo path (File/Full) were live-verified across all three
  environments earlier this session. Everything from the Lookup redesign
  onward — the 6 new connectors, folders, `audit.NotebookRun` wiring — has
  only been checked for internal consistency, **not run against the live
  tenant yet**. Worth treating the next deploy as a real test, not a formality,
  especially for `SqlMI`/`Oracle`/`Sftp`/`Ftp`/`OneLakeTable`/`OneLakeFile`,
  whose exact Fabric type names were sourced from FMD's own working pipelines
  but never exercised here live.
- ℹ️ Harmless, no action needed: an untracked, pre-existing leftover worktree
  at `.claude/worktrees/agent-ad838412a41a5ee67/` still carries old `Stratum`
  branding — it's outside git, isn't part of any deploy, just filesystem
  clutter from an earlier session. Say the word if you'd like it deleted.
