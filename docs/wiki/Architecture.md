# Architecture

Monza is a metadata-driven Microsoft Fabric data platform, structured as **three workspaces per environment** feeding a **medallion lakehouse** (Landing → Bronze → optional Silver → Gold). This page covers the physical topology: which workspaces exist, what item types live in each, how the lakehouses relate, and how everything is named and organized on disk and in Fabric. For the metadata schema that drives ingestion, see [Metadata-Model](Metadata-Model.md). For the connector inventory behind `PL_INGEST_*`, see [Connector-Types](Connector-Types.md). For how items actually get deployed, see [Deployment-Guide](Deployment-Guide.md).

## The 3-workspaces-per-environment model

Every environment (development / test / production) gets exactly three Fabric workspaces, defined in [`config/environments.yaml`](../../config/environments.yaml) and created by `get_or_create_workspace()` in `setup/build_nb_deploy.py` (Phase 1):

| Workspace | Holds | Item types |
|---|---|---|
| **Data** | Lakehouses only | `Landing`, `Bronze`, `Gold`, and `Silver` (conditional — see below) |
| **Ingestion** | Source-facing plumbing | `SQL_METADATA_DATABASE` (Fabric SQL Database), source Connections (registered in the Fabric portal, referenced by GUID from `ingestion.Connection` — see [Metadata-Model](Metadata-Model.md)), and every `PL_INGEST_<TYPE>` pipeline |
| **Code** | Everything else | Loader/orchestrator notebooks (`NB_MONZA_FUNCTIONS`, `NB_RUN_REMOTE_PIPELINE`, `NB_LOAD_BRONZE`, `NB_LOAD_SILVER`, `NB_LOAD_GOLD`), per-table Silver/Gold notebooks (e.g. `sil_customer`, `dim_customer`, `dim_customer_history`, `fact_signup` in the seeded demo — a real client build adds one per table here, following the same naming pattern), `PL_LOAD_BRONZE`/`PL_LOAD_SILVER`/`PL_LOAD_GOLD`/`PL_RUN_ALL` pipelines, and one Variable Library (`VAR_MONZA`) |

`config/items.yaml` is the source of truth for this split: each item entry has a `workspace` field (`Code` or `Ingestion`) that defaults to `Code` when omitted — which is why most `items.yaml` entries don't bother stating it. Only the eight `PL_INGEST_*` pipeline entries set `workspace: Ingestion` explicitly.

This isn't an arbitrary split. `config/environments.yaml` grants Entra access **per tier** (`workspace_roles.data` / `.ingestion` / `.code`), not as one flat list applied everywhere, specifically because Ingestion holds source Connections and the raw metadata catalog — access a report-building principal in Code has no business having. If you're standing this up for a client, decide those three role lists (or leave them `[]` to scope access to the deploying identity only) before your first real deploy.

Each environment's three workspaces all sit on a single Fabric capacity (`capacity` field per environment in `environments.yaml` — resolved to a capacity ID by `get_capacity_id()`). Nothing here requires the three workspaces to share a capacity across environments; it's just how the checked-in config currently points all three (dev/test/prod) at the same trial capacity.

## Medallion lakehouses

Four lakehouses are defined in `config/lakehouses.yaml`, all created inside the **Data** workspace by `get_or_create_lakehouse()` in Phase 2:

```yaml
lakehouses:
  - name: Landing
    always: true
  - name: Bronze
    always: true
  - name: Gold
    always: true
  - name: Silver
    always: false
```

- **Landing** — raw, unmodified data straight from source, one file/table per source object.
- **Bronze** — generic, metadata-driven cleansing (dedupe, not-null enforcement, custom rules per `ingestion.Table.CleansingRules`). Populated by `NB_LOAD_BRONZE`, itself driven off `ingestion.vw_ActiveIngestTables` — see [Metadata-Model](Metadata-Model.md).
- **Silver** *(conditional)* — hand-written per-table notebooks (`NB_LOAD_SILVER` + one notebook per reused entity, e.g. `sil_customer`), only deployed when needed.
- **Gold** — dimensions (SCD1/SCD2), facts, and bridges, hand-written per-table notebooks, `%run`-chained by `NB_LOAD_GOLD`.

### The `include_silver` switch is deliberately conditional

Whether the Silver lakehouse (and `NB_LOAD_SILVER`, `sil_customer`, `PL_LOAD_SILVER`) gets deployed at all is controlled by `include_silver` in `config/environments.yaml`, one flag per environment:

```yaml
environments:
  - name: development
    short: D
    include_silver: true
    ...
```

Mechanically: `lakehouses.yaml` marks `Silver` with `always: false`, so Phase 2 only creates it `if lh["always"] or st["include_silver"]`. In `config/items.yaml`, `NB_LOAD_SILVER`, `sil_customer`, and `PL_LOAD_SILVER` are all marked `requires_silver: true`, so the same flag gates whether those items deploy at all — they're skipped entirely, not deployed-and-unused.

**This flag is not meant to be permanently `true`.** The checked-in repo currently ships all three environments with `include_silver: true` on purpose, as a working, live-verified example (`silver.customer` is a demo 1:1 reshape of the Bronze `customer` entity — see the seed step in `setup/NB_DEPLOY.ipynb`), but the comment block directly above `environments:` in `environments.yaml` is explicit: *"for a fresh clone of this repo, that means false, not the true set below."*

The rule for a real client engagement:

- **Gold reads Bronze directly by default.** There is no default Silver hop — Gold notebooks are written against Bronze tables unless you have a specific reason not to be.
- **Flip `include_silver: true` only when a Bronze entity is genuinely reused by two or more Gold objects** (e.g. a `customer` Bronze table feeding both `dim_customer` and a bridge table, where you don't want the same cleansing/reshape logic duplicated in two Gold notebooks). Silver exists to hold that one shared reshape once.
- Otherwise, leave it `false` and drop straight from Bronze to Gold. Flipping it on "just in case" means shipping an empty conceptual layer and an unused lakehouse per environment for no reason.
- If you do turn it off for an environment that previously had it on, `environments.yaml`'s comment is explicit that you also need to **drop the Silver lakehouse/`NB_LOAD_SILVER`/`PL_LOAD_SILVER` from that environment by hand** — there's no automated teardown path.

## Workspace folder organization

Code and Ingestion each get a small, fixed folder structure, created once per environment in Phase 4 (`get_or_create_folder()`), before any item deploys:

- **Code** workspace: `Notebooks/` and `Pipelines/` folders.
- **Ingestion** workspace: `Pipelines/` folder only (Ingestion holds no notebooks, so there's no `Notebooks/` folder there).
- **Data** workspace: no folders at all — with only 3–4 lakehouses, it isn't worth it.

`folder_id_for_item()` maps an item's type + target workspace to the right folder ID: `Notebook` items in Code → `Notebooks`, `DataPipeline` items in Code → `Pipelines`, `DataPipeline` items in Ingestion → `Pipelines`. A few items intentionally stay at workspace root regardless of type: `VAR_MONZA` (the Variable Library), the lakehouses, and `SQL_METADATA_DATABASE`.

**The one real gotcha:** folder placement only happens on item **create**, never on update. `get_or_create_item()` only sets `folderId` in the `create_body` used for the initial `POST`; the `updateDefinition` call used on every subsequent redeploy has no `folderId` concept at all. In practice: if an item already exists in a workspace — created by hand, created before folders existed, or created by an older version of the generator — a later redeploy will **not** retroactively move it into `Notebooks/`/`Pipelines/`. It stays wherever it was created. If you need it foldered, you have to delete and let the next deploy recreate it (or move it manually in the portal).

Also worth knowing: Fabric's Folder API is still Preview, so this organization is best-effort tidiness, not load-bearing — nothing downstream depends on an item being in a particular folder.

## Naming convention — and a known limitation

Workspace display names follow a fixed pattern, built in Phase 1 of `setup/build_nb_deploy.py`:

```python
st["data_ws_name"]      = f"Monza Data ({env['short']})"
st["ingestion_ws_name"] = f"Monza Ingestion ({env['short']})"
st["code_ws_name"]      = f"Monza Code ({env['short']})"
```

`env['short']` comes from `config/environments.yaml` (`D` / `T` / `P`), so the six workspaces across dev/test/prod are: `Monza Data (D)`, `Monza Ingestion (D)`, `Monza Code (D)`, `Monza Data (T)`, `Monza Ingestion (T)`, `Monza Code (T)`, `Monza Data (P)`, `Monza Ingestion (P)`, `Monza Code (P)`.

**Known limitation:** `"Monza"` is a **hardcoded literal string** directly in `setup/build_nb_deploy.py` (the generator that produces `setup/NB_DEPLOY.ipynb`) — it is not read from any config field, environment variable, or anywhere in `config/environments.yaml`. There is no `platform_name` or equivalent knob. This means reusing this repo for a new client today requires **editing the generator source code**, not just filling in config — every `f"Monza ..."` occurrence in `build_nb_deploy.py` (workspace names, and any other place the literal appears) has to be changed by hand and the notebook regenerated. This is a real gap for repeatable delivery and is called out on [New-Client-Onboarding](New-Client-Onboarding.md) and [Improvement-Roadmap](Improvement-Roadmap.md) as something to fix, not something this page attempts to solve.

## Deployment model, at a glance

All of the above — workspaces, lakehouses, folders, and every notebook/pipeline/variable library in `config/items.yaml` — is created or updated by a single notebook, `setup/NB_DEPLOY.ipynb`, itself generated from `setup/build_nb_deploy.py`. It's triggered by `azure-pipelines.yml`, re-downloads `src/` and `config/` fresh from `main` on every run, and every step is create-if-missing/update-if-exists (`get_or_create_workspace`, `get_or_create_lakehouse`, `get_or_create_folder`, `get_or_create_item`), so re-running it is safe. Three ADO stages gate the rollout: dev deploys automatically, test and prod sit behind required-reviewer approval gates. The full mechanics — how `NB_DEPLOY.ipynb` is generated, what each phase does end to end, how `__TOKEN__` substitution and the pipeline gates work — belong on [Deployment-Guide](Deployment-Guide.md).
