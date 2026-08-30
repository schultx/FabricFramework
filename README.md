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
| `Keystone Data (D/T/P)` | Lakehouses only: `Landing`, `Bronze`, `Gold`, and `Silver` **only if** that environment sets `include_silver: true` |
| `Keystone Integration (D/T/P)` | The metadata catalog SQL Database + any registered source Connections |
| `Keystone Code (D/T/P)` | Everything executable: ingestion pipelines, loader notebooks, the Gold function library, one Variable Library |

**Layers**, deliberately not one-size-fits-all:

- **Landing** — raw, as close to the source as possible, no transformation
- **Bronze** — cleansed, deduped, quality-checked Delta tables (one per source entity)
- **Silver** — *optional*. Only exists for a Bronze entity whose cleansed shape is
  reused by two or more Gold objects. Most Bronze entities never get one — by
  default, **Gold reads directly from Bronze**.
- **Gold** — business logic: dimensions (SCD1/SCD2), facts, bridges, ready for
  a semantic model

**Metadata catalog** (`SQL_STRATUM_CATALOG`, in the Integration workspace) drives
everything: `catalog.Connection` / `catalog.Source` / `catalog.LandingEntity` /
`catalog.BronzeEntity` / `catalog.SilverEntity` describe what to ingest and how
to cleanse it; `gold.GoldEntity` describes each Gold object; `ai.FeatureSet`
registers Silver/Gold entities for AI-team consumption; `runtime.LoadWatermark`
and `audit.PipelineRun`/`audit.NotebookRun` track incremental state and
execution history. Full DDL in [`config/metadata_schema.sql`](config/metadata_schema.sql).

## Deployment

**Start with [DEPLOYMENT.md](DEPLOYMENT.md)** — one notebook
([`setup/NB_DEPLOY.ipynb`](setup/NB_DEPLOY.ipynb)) deploys everything, and
[`azure-pipelines.yml`](azure-pipelines.yml) is the only thing that triggers it,
per environment, with approval gates before Test and Production.

## Repo layout

```
config/     environments.yaml, lakehouses.yaml, items.yaml, metadata_schema.sql
setup/      NB_DEPLOY.ipynb -- the one notebook that deploys everything
src/        every item deployed into the Code workspace
demodata/   a small CSV fixture (customer.csv) so dim_customer/fact_signup have
            something to load on a first run
deploy/     run_notebook.py -- triggers NB_DEPLOY from Azure DevOps
```
