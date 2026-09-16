# Metadata Model

Monza has no per-table code. One row in `ingestion.Table` — joined up through `ingestion.Database` and `ingestion.Connection` — is the entire specification for how a source table moves from Source through Landing into Bronze. `NB_LOAD_BRONZE` and the `PL_INGEST_<TYPE>` pipelines are generic engines that read this metadata and do exactly what it says; they contain no table names, no column lists, no business logic. If you need a new table ingested, or an existing one behaving differently, the fix is almost always a row (or an `UPDATE`) in this schema, not a code change.

This page is the reference for that schema: the full DDL is [config/metadata_schema.sql](../../config/metadata_schema.sql), deployed (idempotently, re-run-safe) into `SQL_METADATA_DATABASE` by `setup/NB_DEPLOY.ipynb`. For how the schema fits into the overall pipeline topology, see [Architecture](Architecture.md); for the list of supported `ConnectionType` values and what each one expects, see [Connector-Types](Connector-Types.md).

## The four schemas at a glance

| Schema | Purpose |
|---|---|
| `ingestion` | The Connection → Database → Table hierarchy that drives Source → Landing → Bronze. |
| `runtime` | Incremental-load state (`LoadWatermark`) — one row per Delta table. |
| `audit` | Execution history (`PipelineRun`, `NotebookRun`). |
| `ai` | Forward-looking registration table for Silver/Gold feature sets (`FeatureSet`) — not consumed by anything yet. |

## The ingestion hierarchy: Connection → Database → Table

This is deliberately a lean 3-level hierarchy, not one table per pipeline stage. There's no separate "Landing entity" or "Bronze entity" table — a single active `ingestion.Table` row drives that table's entire flow. Silver and Gold are hand-written, per-table notebooks (`%run`-chained by `NB_LOAD_SILVER` / `NB_LOAD_GOLD`), not metadata-loop-driven, so neither layer gets a table of its own here.

### `ingestion.Connection`

One row per external system.

| Column | Type | Meaning |
|---|---|---|
| `ConnectionId` | `INT IDENTITY` | PK. |
| `Name` | `VARCHAR(200)` | Display name. |
| `ConnectionType` | `VARCHAR(20)` | `Sql`, `File`, `SqlMI`, `Oracle`, `Sftp`, `Ftp`, `OneLakeTable`, `OneLakeFile`, `Custom`. Drives which `PL_INGEST_<TYPE>` pipeline picks the row up (see below). |
| `ConnectionGuid` | `UNIQUEIDENTIFIER` | The Fabric Connection item's GUID — **except** for `OneLakeTable`/`OneLakeFile`, where it's repurposed to hold the **source workspace GUID** (no Fabric Connection object exists for same-tenant cross-workspace OneLake access), and for `Custom`, where it's unused (set a placeholder). |
| `IsActive` | `BIT` | Soft-disable an entire connection. |

Full detail on what `ConnectionGuid`/`Database.Name` mean per type lives in the comment block above `CREATE TABLE [ingestion].[Connection]` in `metadata_schema.sql`, and in [Connector-Types](Connector-Types.md). Note FMD's `ADF` type was deliberately **not** ported — it's pass-through tracking for an externally-orchestrated ADF pipeline, which doesn't fit Monza's self-contained model where every ingestion runs from inside Monza's own pipelines.

### `ingestion.Database`

One row per database/container/lakehouse within a Connection.

| Column | Type | Meaning |
|---|---|---|
| `DatabaseId` | `INT IDENTITY` | PK. |
| `ConnectionId` | `INT` | FK → `Connection`. |
| `Name` | `VARCHAR(200)` | Source database name (`Sql`/`SqlMI`/`Oracle`), container/filesystem name (`File`), or source lakehouse item GUID as text (`OneLakeTable`/`OneLakeFile`). Unused for `Sftp`/`Ftp` (use `''`). |
| `IsActive` | `BIT` | Soft-disable everything under this database. |

### `ingestion.Table`

One row per table. **This is the row you edit for almost every ingestion task.**

| Column | Type | Meaning |
|---|---|---|
| `TableId` | `BIGINT IDENTITY` | PK, and the `EntityId` used in `runtime.LoadWatermark`. |
| `DatabaseId` | `INT` | FK → `Database`. |
| `SourceSchema` | `NVARCHAR(100)` NULL | Source schema, if the source has one. |
| `SourceObject` | `NVARCHAR(200)` | Source table/view/query name. |
| `SourceQuery` | `NVARCHAR(MAX)` NULL | Optional custom `SELECT` override. `NULL` = `SELECT *` from `SourceSchema.SourceObject`. |
| `FilePath` | `NVARCHAR(500)` | Target folder under `Landing/Files/` that the Copy activity writes to and `NB_LOAD_BRONZE` reads back from. |
| `FileType` | `VARCHAR(20)` | Default `'parquet'`. Also drives the Spark reader format in `NB_LOAD_BRONZE` (`.format(entity["FileType"].lower())`; `csv` gets `header=true, inferSchema=true`). |
| `BronzeSchema` | `NVARCHAR(100)` | Target schema in the Bronze lakehouse. |
| `BronzeName` | `NVARCHAR(200)` | Target table name in Bronze. Together: `Bronze.<BronzeSchema>.<BronzeName>`. |
| `PrimaryKeys` | `NVARCHAR(200)` | Comma-separated column list. Used for dedupe on **every** load, and as the `MERGE` key when `LoadType='Delta'`. |
| `LoadType` | `VARCHAR(10)` | `Full` (default) or `Delta`. |
| `IncrementalColumn` | `NVARCHAR(100)` NULL | Required when `LoadType='Delta'`. Drives both the watermark filter in `vw_ActiveIngestTables` and the watermark advance in `NB_LOAD_BRONZE`. |
| `DeleteHandling` | `VARCHAR(20)` | `None` (default), `SoftDelete`, or `Reconcile`. Only meaningful for `Delta` entities. |
| `IsDeletedColumn` | `NVARCHAR(100)` NULL | Required when `DeleteHandling='SoftDelete'`; also used by `Reconcile` if present (tombstone instead of hard-delete). |
| `CleansingRules` | `NVARCHAR(MAX)` NULL | JSON array of cleansing rules beyond the built-in PK dedupe/not-null. Shape below. |
| `CustomNotebookName` | `NVARCHAR(200)` NULL | Only populated when the row's `Connection.ConnectionType = 'Custom'` — names the hand-written notebook (registered in `config/items.yaml`) that lands this table. See the "Custom sources" section of [Deployment-Guide](Deployment-Guide.md). |
| `IsActive` | `BIT` | Set to `0` to pull a table out of ingestion without deleting its metadata or history. |

## `ingestion.vw_ActiveIngestTables` — the query a pipeline Lookup runs directly

This view is the seam between the metadata catalog and the pipeline layer, and it's pure T-SQL — there is **no notebook in this hop**. Every `PL_INGEST_<TYPE>` pipeline's `Lookup` activity (`LK_ACTIVE_<TYPE>_TABLES`) queries it directly, filtered to its own `ConnectionType`. From `src/PL_INGEST_SQL.DataPipeline/pipeline-content.json`, the Lookup's `sqlReaderQuery` is literally:

```sql
SELECT * FROM [ingestion].[vw_ActiveIngestTables] WHERE [ConnectionType] = 'Sql'
```

`PL_INGEST_FILE`, `PL_INGEST_SQLMI`, `PL_INGEST_ORACLE`, `PL_INGEST_SFTP`, `PL_INGEST_FTP`, `PL_INGEST_ONELAKETABLE` and `PL_INGEST_ONELAKEFILE` each run the same query with their own type substituted. `Custom` rows are excluded entirely (`WHERE ... c.[ConnectionType] <> 'Custom'`) — they're picked up by hand-written per-table notebooks instead, never by a generic Lookup-driven pipeline. The pipeline needs no connection of its own beyond the single bootstrap `metadata_connection_guid` in `config/environments.yaml`.

The view resolves, per active table, a ready-to-run `ResolvedSourceQuery`:

- **`SourceQuery IS NOT NULL`** (custom override): `SELECT * FROM (<SourceQuery>) AS src_query`
- **`SourceQuery IS NULL`** (default): `SELECT * FROM [SourceSchema].[SourceObject]` — built via `QUOTENAME()` on both parts
- **`LoadType = 'Delta'`**: the base query above gets wrapped again — `SELECT * FROM (<base>) AS w WHERE [IncrementalColumn] > '<watermark>'`, where the watermark comes from a `LEFT JOIN` to `runtime.LoadWatermark` (`ISNULL(w.LastValue, '1900-01-01')` on first run, so a Delta table with no watermark row yet effectively does a full pull)

Every identifier substituted in — `SourceSchema`, `SourceObject`, `IncrementalColumn` — goes through `QUOTENAME()`. This matters in practice: a source column or table named with a space, a reserved word, or odd casing will otherwise break the generated SQL that the pipeline actually executes. If you're troubleshooting a `PL_INGEST_*` failure that looks like a SQL syntax error, check the `ResolvedSourceQuery` this view produces for the offending `TableId` first — `SELECT * FROM [ingestion].[vw_ActiveIngestTables] WHERE TableId = <id>` — before touching anything else.

## Full vs Delta: what actually happens on each run

Both load types run inside `NB_LOAD_BRONZE` ([src/NB_LOAD_BRONZE.Notebook/notebook-content.py](../../src/NB_LOAD_BRONZE.Notebook/notebook-content.py)), a single Spark session that loops every active `ingestion.Table` row (or just one, if `bronze_entity_name` is passed for a targeted rerun). `PL_LOAD_BRONZE` is one `TridentNotebook` activity — no per-entity pipeline fan-out.

**`LoadType = 'Full'`**, on every run:
```python
clean_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
```
Bronze is completely overwritten from the latest Landing extract every time. Source-side deletes are handled for free, since Bronze exactly mirrors whatever the latest extract contains — there's nothing to reconcile. (If `DeleteHandling='Reconcile'` is set on a `Full` table by mistake, it's silently ignored — treated as redundant, not an error.)

**`LoadType = 'Delta'`**, on every run after the first:
```python
delta_table.alias("target").merge(clean_df.alias("source"), merge_conditions) \
    .whenMatchedUpdate(set=update_dict) \
    .whenNotMatchedInsertAll() \
    .execute()
```
where `merge_conditions` is built from `PrimaryKeys` (`target.<pk> = source.<pk>` for each key). (First run for a Delta table that doesn't exist yet in Bronze: plain `overwrite`, same as Full, to seed it.) After the merge, if `IncrementalColumn` is set and present in the batch:
```python
max_value = clean_df.agg(F.max(entity["IncrementalColumn"])).collect()[0][0]
_update_watermark(entity["TableId"], str(max_value))
```
which upserts `runtime.LoadWatermark` — see below. Everything downstream (the next run's `vw_ActiveIngestTables` Lookup) picks that new watermark up automatically.

### `DeleteHandling` (Delta entities only)

| Value | Behavior |
|---|---|
| `None` (default) | No special handling. Accepted limitation: a row deleted at the source stays in Bronze until a full reload. |
| `SoftDelete` | `IsDeletedColumn` rides along as an ordinary column through the merge — no special code runs. Downstream (Silver) consumers are expected to filter on it. |
| `Reconcile` | After the merge, fetches **just** the `PrimaryKeys` column(s)** from the FULL source table (`SELECT <pk_cols> FROM <SourceSchema>.<SourceObject>`, a lightweight JDBC read authenticated with the notebook's own Entra identity) and does a `left_anti` join against Bronze's current keys. Any Bronze row whose key is no longer at the source gets either flagged (`whenMatchedUpdate(set={IsDeletedColumn: "true"})`, if `IsDeletedColumn` is set) or hard-deleted (`whenMatchedDelete()`, if not). |

`Reconcile` is **only implemented for `Sql` connections today** — for any other `ConnectionType` it logs a warning and skips (`_reconcile_deletes` checks `entity["ConnectionType"] != "Sql"` up front). A `File` source has no separate PK-only fetch to make since the whole file is already copied to Landing in full each run, so there's no gap to close there. If you set `DeleteHandling='Reconcile'` on a non-`Sql` Delta table, it will run without error but silently do nothing — worth knowing before you assume it's working.

## Cleansing: metadata-driven, not per-table code

Every table gets the same two built-in cleansing steps before anything else, unconditionally:
```python
clean_df = raw_df.dropna(subset=primary_keys)     # drop rows missing a PK
clean_df = clean_df.dropDuplicates(primary_keys)   # dedupe on PK
```
On top of that, `CleansingRules` is a JSON array in the `ingestion.Table` row, applied in order. Two rule types exist today:

```json
[
  { "type": "not_null", "column": "Email" },
  { "type": "dedupe_keep_latest", "orderBy": "ModifiedDateUtc" }
]
```

- `not_null` → `clean_df.dropna(subset=[rule["column"]])`
- `dedupe_keep_latest` → windows over `PrimaryKeys` ordered by `rule["orderBy"]` descending, keeps `_rn = 1` (i.e., picks the most recent row per key when the source can hand back duplicate keys, e.g. a CDC-style extract with multiple versions of the same row in one batch)

An unrecognized `type` is logged (`Warning: unknown cleansing rule type '<type>', skipping`) and otherwise ignored — it won't fail the load. `CleansingRules` can be `NULL` (equivalent to `[]`).

Finally, every row gets a `bronze_loaded_datetime` column stamped with `F.current_timestamp()` before it's written.

## `audit.NotebookRun` and `runtime.LoadWatermark`

**`audit.NotebookRun`** — one row per `NB_LOAD_BRONZE` execution (not per table):

| Column | Written when |
|---|---|
| `RunId`, `NotebookName`, `RunGuid`, `Status='Running'`, `StartTimeUtc` | `start_notebook_run("NB_LOAD_BRONZE")` — INSERT, called once near the top of the notebook, before any table is processed. |
| `Status` (`Succeeded`/`Failed`), `EndTimeUtc`, `ErrorMessage` | `end_notebook_run(run_guid, status, error_message)` — UPDATE by `RunGuid`, called once at the end. |

Per-entity loads are wrapped individually in `try/except` — one malformed source table doesn't abort the run for every other table. Failures are collected in `failed_entities`, and only **after every table has had its turn** does the run close out `Failed` overall if anything failed, with `ErrorMessage` listing which `BronzeName`s failed and the total count (`f"{len(failed_entities)} of {len(entities)} table(s) failed: {failed_entities}"`). This is what the pipeline activity — and Monza's native failure notifications — actually see, so a `Failed` `NotebookRun` row means "look at the error message for which tables," not "the whole run produced nothing."

There's also `audit.PipelineRun`, structurally similar (`PipelineName`, `RunGuid`, `Status`, timestamps, plus `RowsRead`/`RowsWritten`) for pipeline-level (not notebook-level) execution tracking.

**`runtime.LoadWatermark`** — one row per Delta `ingestion.Table`, keyed by `(EntityType='Table', EntityId=TableId)`:

| Column | Meaning |
|---|---|
| `WatermarkId` | PK. |
| `EntityType` | Always `'Table'` today — kept as a column (not hardcoded/dropped) so a future watermark consumer outside `ingestion.Table` doesn't force a schema change. |
| `EntityId` | = `ingestion.Table.TableId`. |
| `LastValue` | `MAX(IncrementalColumn)` seen as of `LastRunUtc`, stored as text. |
| `LastRunUtc` | When that value was captured. |

Written by `_update_watermark()` in `NB_LOAD_BRONZE`, an upsert `MERGE` on `(EntityType, EntityId)` — insert on first watermark, update thereafter. It only fires if the Delta merge found a non-null `MAX(IncrementalColumn)` in the batch (an empty incremental batch leaves the watermark untouched, rather than advancing it to `NULL`).

## `ai.FeatureSet` (brief — forward-looking)

Registration table for future ML/feature work, not consumed by any pipeline today: `SourceEntityId` + `SourceLayer` (`silver`/`gold`) point at a Silver or Gold entity, plus `Name`, `RefreshCadence`, and an optional `VectorIndexRef` for an embedding index. Nothing in this repo writes to or reads from it yet — it exists so the shape is already there when that work starts.

## Worked example: registering a new Full-load table

Say the new client has a `dbo.Customer` table in an Azure SQL database you've already registered a Fabric Connection for, and you want a straight full-refresh copy into Bronze with no custom cleansing beyond the default PK dedupe.

**1. Confirm the `Connection` and `Database` rows exist** (created once per source system/database during onboarding — see [New-Client-Onboarding](New-Client-Onboarding.md)):

```sql
SELECT ConnectionId FROM ingestion.Connection WHERE Name = 'ClientSqlSource' AND ConnectionType = 'Sql';
SELECT DatabaseId FROM ingestion.Database WHERE ConnectionId = <ConnectionId> AND Name = 'ClientDb';
```

**2. Insert the `Table` row:**

```sql
INSERT INTO ingestion.Table (
    DatabaseId, SourceSchema, SourceObject, SourceQuery,
    FilePath, FileType, BronzeSchema, BronzeName,
    PrimaryKeys, LoadType, IncrementalColumn,
    DeleteHandling, IsDeletedColumn, CleansingRules,
    CustomNotebookName, IsActive
) VALUES (
    <DatabaseId>, 'dbo', 'Customer', NULL,
    'dbo/Customer', 'parquet', 'dbo', 'Customer',
    'CustomerId', 'Full', NULL,
    'None', NULL, NULL,
    NULL, 1
);
```

**3. Verify the Lookup will pick it up** — this is exactly the query `PL_INGEST_SQL`'s Lookup activity runs, so run it yourself first to sanity-check the resolved query before triggering the pipeline:

```sql
SELECT * FROM ingestion.vw_ActiveIngestTables WHERE ConnectionType = 'Sql' AND SourceObject = 'Customer';
```

Confirm `ResolvedSourceQuery` reads `SELECT * FROM [dbo].[Customer]`.

**4. Run it.** Trigger `PL_INGEST_SQL` (lands `dbo/Customer` under `Landing/Files/`), then `PL_LOAD_BRONZE` (or run `NB_LOAD_BRONZE` directly with `bronze_entity_name = "Customer"` for a targeted rerun). Check `audit.NotebookRun` for the run's status and `Bronze.dbo.Customer` for the row count.

If this were a Delta table instead, you'd additionally set `LoadType='Delta'`, `IncrementalColumn` to the source's change-tracking column (e.g. `ModifiedDateUtc`), and — if you need deletes reconciled — `DeleteHandling='Reconcile'` (Sql-only) or `'SoftDelete'` with `IsDeletedColumn` pointing at the source's soft-delete flag.

For the pipeline-level mechanics this example glosses over — how `PL_INGEST_SQL`'s Copy activity turns a Lookup row into an actual Landing file — see [Architecture](Architecture.md). For the full list of `ConnectionType` values and their onboarding checklists, see [Connector-Types](Connector-Types.md).
