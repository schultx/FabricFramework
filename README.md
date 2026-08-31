# Keystone

A lean, metadata-driven Fabric framework for Data Platform + AI teams. Three
workspaces per environment, three lakehouses (four if you need it), one
notebook that deploys everything from git.

This is an original implementation — no code from FMD Framework or any other
third-party framework. It follows the same general medallion-lakehouse
principles common across the industry, but the metadata schema, deployment
notebook, pipelines, and workspace topology here are all written for this
repo specifically.

## Architecture

**Three workspaces per environment** (`development` / `test` / `production`):

| Workspace | Holds |
|---|---|
| `Keystone Data (D/T/P)` | Lakehouses only, at workspace root: `Landing`, `Bronze`, `Gold`, and `Silver` **only if** that environment sets `include_silver: true` |
| `Keystone Ingestion (D/T/P)` | `SQL_METADATA_DATABASE` (root) + any registered source Connections, and every `PL_INGEST_*` ingestion pipeline that reads them, under a `Pipelines/` folder |
| `Keystone Code (D/T/P)` | Everything else executable, under `Notebooks/` (every loader + Gold/Silver notebook, `NB_KEYSTONE_FUNCTIONS`, `NB_RUN_REMOTE_PIPELINE`) and `Pipelines/` (`PL_RUN_ALL` and the other orchestration/load pipelines) — `VAR_KEYSTONE` (one Variable Library) stays at workspace root |

Folders are created via Fabric's Folder REST API, which is **Preview** as of
this writing — see [DEPLOYMENT.md](DEPLOYMENT.md#workspace-folders).

**Layers**, deliberately not one-size-fits-all:

- **Landing** — raw, as close to the source as possible, no transformation
- **Bronze** — cleansed, deduped, quality-checked Delta tables (one per source entity)
- **Silver** — *optional*. Only exists for a Bronze entity whose cleansed shape is
  reused by two or more Gold objects. Most Bronze entities never get one — by
  default, **Gold reads directly from Bronze**.
- **Gold** — business logic: dimensions (SCD1/SCD2), facts, bridges, ready for
  a semantic model

**Metadata catalog** (`SQL_METADATA_DATABASE`, in the Ingestion workspace)
drives ingestion with a lean 3-level hierarchy, not one table per pipeline
stage, in the `ingestion` schema: `ingestion.Connection` (one row per source
system) -> `ingestion.Database` (one row per database/container/lakehouse
within a Connection) -> `ingestion.Table` (one row per table drives its
entire Source -> Landing -> Bronze flow when active — Full or Delta load,
optional delete handling).

**Ingestion is Lookup-driven, not notebook-driven.** Each `PL_INGEST_*`
pipeline's first activity is a native `Lookup` (`FabricSqlDatabaseSource`)
querying `ingestion.vw_ActiveIngestTables` — a view that resolves each active
row's ready-to-run `ResolvedSourceQuery` in T-SQL — directly, filtered to its
own `ConnectionType`. No notebook sits in the loop and no `TridentNotebook`
cold-start tax is paid per run; a `ForEach` then `Copy`s each row straight
into Landing. Supported types: `Sql`, `File`, `SqlMI`, `Oracle` (via an
on-premises Data Gateway), `Sftp`, `Ftp`, `OneLakeTable`/`OneLakeFile` (from
another Fabric workspace/lakehouse), and `Custom` — an escape hatch for
sources with no dedicated connector (REST APIs, SharePoint, Dataverse,
Salesforce, etc.), hand-written as a per-table notebook and deliberately
excluded from the Lookup-driven model. See
[DEPLOYMENT.md](DEPLOYMENT.md#adding-a-real-data-source) for the full list
and how to add a source of each type.

Silver and Gold are **not** metadata-loop-driven — each is a hand-written,
per-table notebook, %run-chained together, so neither layer has a catalog
table of its own. `ai.FeatureSet` registers Silver/Gold entities for AI-team
consumption; `runtime.LoadWatermark` and `audit.PipelineRun`/
`audit.NotebookRun` track incremental state and execution history. Full DDL in
[`config/metadata_schema.sql`](config/metadata_schema.sql).

## Deployment

**Start with [DEPLOYMENT.md](DEPLOYMENT.md)** — one notebook
([`setup/NB_DEPLOY.ipynb`](setup/NB_DEPLOY.ipynb)) deploys everything, and
[`azure-pipelines.yml`](azure-pipelines.yml) is the only thing that triggers it,
per environment, with approval gates before Test and Production.

## Repo layout

```
config/     environments.yaml, lakehouses.yaml, items.yaml, metadata_schema.sql
setup/      NB_DEPLOY.ipynb -- the one notebook that deploys everything
src/        every item deployed into the Code or Ingestion workspace
            (config/items.yaml's `workspace` field picks which)
demodata/   a small CSV fixture (customer.csv) so dim_customer/fact_signup have
            something to load on a first run
deploy/     run_notebook.py -- triggers NB_DEPLOY from Azure DevOps
```
