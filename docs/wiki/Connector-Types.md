# Connector Types

Every source a `PL_INGEST_*` pipeline can read from is one of 9 `ConnectionType` values defined in [`config/metadata_schema.sql`](../../config/metadata_schema.sql) (`ingestion.Connection.ConnectionType`). Each non-`Custom` type has its own generic, metadata-driven pipeline in the `Monza Ingestion (X)` workspace — `PL_INGEST_SQL`, `PL_INGEST_FILE`, `PL_INGEST_SQLMI`, `PL_INGEST_ORACLE`, `PL_INGEST_SFTP`, `PL_INGEST_FTP`, `PL_INGEST_ONELAKETABLE`, `PL_INGEST_ONELAKEFILE` — and all 8 follow the identical shape:

1. A `Lookup` activity (`LK_ACTIVE_<TYPE>_TABLES`) runs `SELECT * FROM [ingestion].[vw_ActiveIngestTables] WHERE [ConnectionType] = '<Type>'` against the metadata catalog via `FabricSqlDatabaseSource`.
2. A `ForEach` (`FE_<TYPE>_ENTITY`) iterates `@activity('LK_ACTIVE_<TYPE>_TABLES').output.value`.
3. A single `Copy` activity (`CP_<TYPE>_TO_LANDING`) inside the loop reads one row's `@item()` fields and copies that table/file into the `LandingLakehouse` linked service (`workspaceId: __DATA_WORKSPACE_ID__`, `artifactId: __LANDING_LAKEHOUSE_ID__` — placeholders `NB_DEPLOY` patches with real IDs at deploy time; see [Deployment-Guide](Deployment-Guide.md)).

Adding a table of a type the framework already supports means inserting `ingestion.Connection` / `ingestion.Database` / `ingestion.Table` rows — **no pipeline JSON changes required**. This page covers what those rows mean for each of the 9 types and walks through the most common case end to end. For the full column-by-column schema reference see [Metadata-Model](Metadata-Model.md); for overall Landing → Bronze → Silver → Gold flow see [Architecture](Architecture.md).

## The 9 types at a glance

| ConnectionType | Pipeline | Source system | Copy `source.type` | `datasetSettings.type` | Sink |
|---|---|---|---|---|---|
| `Sql` | `PL_INGEST_SQL` | Azure SQL Database | `AzureSqlSource` | `AzureSqlTable` | `ParquetSink` |
| `File` | `PL_INGEST_FILE` | ADLS Gen2 / Blob-FS-compatible store | `BinarySource` (`AzureBlobFSReadSettings`) | `Binary` | `BinarySink` |
| `SqlMI` | `PL_INGEST_SQLMI` | Azure SQL Managed Instance | `SqlMISource` | `AzureSqlMITable` | `ParquetSink` |
| `Oracle` | `PL_INGEST_ORACLE` | Oracle, via on-prem Data Gateway | `OracleSource` | `OracleTable` | `ParquetSink` |
| `Sftp` | `PL_INGEST_SFTP` | SFTP server | `BinarySource` (`SftpReadSettings`) | `Binary` | `BinarySink` |
| `Ftp` | `PL_INGEST_FTP` | FTP server | `BinarySource` (`FtpReadSettings`) | `Binary` | `BinarySink` |
| `OneLakeTable` | `PL_INGEST_ONELAKETABLE` | Delta table in another Fabric workspace/lakehouse | `LakehouseTableSource` | `LakehouseTable` | `ParquetSink` |
| `OneLakeFile` | `PL_INGEST_ONELAKEFILE` | File under another Fabric workspace/lakehouse's `Files/` | `BinarySource` (`AzureBlobStorageReadSettings`) | `Binary`, Lakehouse-linked | `BinarySink` |
| `Custom` | *none* | Anything with no dedicated connector | — hand-written notebook, no generic Copy | — | — |

Notice the split: the four relational/tabular types (`Sql`, `SqlMI`, `Oracle`, `OneLakeTable`) all land as **Parquet**, with `TabularTranslator` type conversion (`typeConversion: true`, `allowDataTruncation: true`) and a `.parquet` extension appended (`@concat(item().SourceObject, '.parquet')`) because `SourceObject` is a bare table name. The four binary/file-based types (`File`, `Sftp`, `Ftp`, `OneLakeFile`) copy bytes through untouched via `BinarySink`, and the landed filename is `@item().SourceObject` as-is — for these types `SourceObject` already carries its real extension (e.g. `customer.csv`), and `ingestion.Table.FileType` should match it.

## Sql

Source: **Azure SQL Database**. `ingestion.Connection.ConnectionGuid` is the Fabric Connection GUID to that database; `ingestion.Database.Name` is the source database name.

`CP_SQL_TO_LANDING` (`src/PL_INGEST_SQL.DataPipeline/pipeline-content.json`):

```json
"source": {
  "type": "AzureSqlSource",
  "sqlReaderQuery": { "value": "@item().ResolvedSourceQuery", "type": "Expression" },
  "datasetSettings": {
    "type": "AzureSqlTable",
    "typeProperties": {
      "schema": { "value": "@item().SourceSchema", "type": "Expression" },
      "table":  { "value": "@item().SourceObject", "type": "Expression" }
    },
    "externalReferences": { "connection": "@item().ConnectionGuid" }
  }
}
```

`sqlReaderQuery` runs `ingestion.vw_ActiveIngestTables.ResolvedSourceQuery` verbatim — a full `SELECT *` or watermark-bounded query the view already built (see the view definition for how `LoadType = 'Delta'` wraps it). Sink is `ParquetSink` into `Landing/Files/<FilePath>/<SourceObject>.parquet`.

## File

Source: **ADLS Gen2** (or anything Blob-FS-compatible). `ConnectionGuid` = Fabric Connection GUID to the storage account; `Database.Name` = **container/filesystem name** (not a database).

`CP_FILE_TO_LANDING` (`src/PL_INGEST_FILE.DataPipeline/pipeline-content.json`):

```json
"source": {
  "type": "BinarySource",
  "storeSettings": { "type": "AzureBlobFSReadSettings", "recursive": true },
  "datasetSettings": {
    "type": "Binary",
    "typeProperties": {
      "location": {
        "type": "AzureBlobFSLocation",
        "fileSystem": { "value": "@item().DatabaseName", "type": "Expression" },
        "folderPath": { "value": "@item().SourceSchema", "type": "Expression" },
        "fileName":   { "value": "@item().SourceObject", "type": "Expression" }
      }
    },
    "externalReferences": { "connection": "@item().ConnectionGuid" }
  }
}
```

Note `fileSystem` pulls straight from `@item().DatabaseName` — the concrete proof that `ingestion.Database.Name` means "container name" for this type, not a database. `SourceSchema` doubles as the folder path inside the container.

## SqlMI

Source: **Azure SQL Managed Instance**. Same `ConnectionGuid`/`Database.Name` semantics as `Sql` (Connection GUID + source database name). The only real difference from `Sql` is the Copy activity's `source.type`/`datasetSettings.type`:

```json
"source": {
  "type": "SqlMISource",
  "sqlReaderQuery": { "value": "@item().ResolvedSourceQuery", "type": "Expression" },
  "datasetSettings": { "type": "AzureSqlMITable", "...": "same schema/table/externalReferences shape as Sql" }
}
```

(`src/PL_INGEST_SQLMI.DataPipeline/pipeline-content.json`). Sink, translator settings and the rest of the shape are byte-for-byte identical to `PL_INGEST_SQL`.

## Oracle

Source: **Oracle**, reached through an **on-premises Data Gateway**. The gateway itself is configured on the Fabric Connection object, not tracked in this catalog — `ConnectionGuid` is that Connection's GUID, `Database.Name` is the source database/service name.

`CP_ORACLE_TO_LANDING` (`src/PL_INGEST_ORACLE.DataPipeline/pipeline-content.json`):

```json
"source": {
  "type": "OracleSource",
  "oracleReaderQuery": { "value": "@item().ResolvedSourceQuery", "type": "Expression" },
  "numberPrecision": 38,
  "numberScale": 18,
  "datasetSettings": {
    "type": "OracleTable",
    "typeProperties": {
      "schema": { "value": "@item().SourceSchema", "type": "Expression" },
      "table":  { "value": "@item().SourceObject", "type": "Expression" }
    },
    "externalReferences": { "connection": "@item().ConnectionGuid" }
  }
}
```

> **Gotcha:** the query field is `oracleReaderQuery`, **not** `sqlReaderQuery` like `Sql`/`SqlMI` use. If you ever hand-copy one of the SQL pipelines to prototype a change, this is the field that silently breaks (Fabric won't validation-error on the wrong key sitting unused — the Copy activity just runs with an empty query). `numberPrecision: 38` / `numberScale: 18` are Oracle-specific: Oracle's arbitrary-precision `NUMBER` type needs an explicit target precision/scale or the Copy activity guesses badly.

## Sftp

Source: **SFTP server**. `ConnectionGuid` = Fabric Connection GUID (SFTP); `Database.Name` is unused for this type (set to `''`).

```json
"source": {
  "type": "BinarySource",
  "storeSettings": { "type": "SftpReadSettings", "recursive": true, "disableChunking": false },
  "formatSettings": { "type": "BinaryReadSettings" },
  "datasetSettings": {
    "type": "Binary",
    "typeProperties": {
      "location": {
        "type": "SftpLocation",
        "fileName":   { "value": "@item().SourceObject", "type": "Expression" },
        "folderPath": { "value": "@item().SourceSchema", "type": "Expression" }
      }
    },
    "externalReferences": { "connection": "@item().ConnectionGuid" }
  }
}
```

(`src/PL_INGEST_SFTP.DataPipeline/pipeline-content.json`.) `SourceSchema` is repurposed as the remote folder path, `SourceObject` as the remote filename — same convention as `File`.

## Ftp

Source: **FTP server**. Same `ConnectionGuid`/unused-`Database.Name` pattern as `Sftp`. Only the store settings and location type differ:

```json
"source": {
  "type": "BinarySource",
  "storeSettings": { "type": "FtpReadSettings", "recursive": true, "useBinaryTransfer": true, "disableChunking": false },
  "formatSettings": { "type": "BinaryReadSettings" },
  "datasetSettings": {
    "type": "Binary",
    "typeProperties": { "location": { "type": "FtpServerLocation", "fileName": "@item().SourceObject", "folderPath": "@item().SourceSchema" } },
    "externalReferences": { "connection": "@item().ConnectionGuid" }
  }
}
```

(`src/PL_INGEST_FTP.DataPipeline/pipeline-content.json`.) `useBinaryTransfer: true` matters — leaving it off risks ASCII-mode transfer corrupting binary files (parquet, zip, etc.) over plain FTP.

## OneLakeTable and OneLakeFile — the repurposed-columns exception

**Read this before configuring either of these two types.** `OneLakeTable` and `OneLakeFile` read from a Delta table or a file in a *different* Fabric workspace/lakehouse, same-tenant. There is no Fabric **Connection** object for same-tenant cross-workspace OneLake access — Fabric addresses that directly by workspace GUID + item GUID (the same mechanism this framework's own Landing/Bronze/Gold lakehouse references already use internally). So instead of adding new columns for it, these two types **repurpose the existing ones**:

- `ingestion.Connection.ConnectionGuid` = the **source workspace GUID** (not a Connection item's GUID — there is no Connection item)
- `ingestion.Database.Name` = the **source lakehouse's item GUID, as text** (not a display name)

This is a real deviation from every other connector's pattern (where `ConnectionGuid` genuinely points at a Fabric Connection object) — don't register a Connection for these two types, and don't put a lakehouse *name* in `Database.Name`.

You can see this directly in `CP_ONELAKETABLE_TO_LANDING` (`src/PL_INGEST_ONELAKETABLE.DataPipeline/pipeline-content.json`) — there's no `externalReferences.connection` at all; instead the linked service is built inline from `@item()`:

```json
"source": {
  "type": "LakehouseTableSource",
  "datasetSettings": {
    "type": "LakehouseTable",
    "linkedService": {
      "name": "SourceLakehouse",
      "properties": {
        "type": "Lakehouse",
        "typeProperties": {
          "workspaceId": "@item().ConnectionGuid",
          "artifactId":  "@item().DatabaseName",
          "rootFolder": "Tables"
        }
      }
    },
    "typeProperties": {
      "schema": { "value": "@item().SourceSchema", "type": "Expression" },
      "table":  { "value": "@item().SourceObject", "type": "Expression" }
    }
  }
}
```

`OneLakeFile` (`src/PL_INGEST_ONELAKEFILE.DataPipeline/pipeline-content.json`) does the identical repurposing for the `SourceLakehouse` linked service, but targets `rootFolder: "Files"` instead of `"Tables"`, and reads through `BinarySource` with `storeSettings.type: "AzureBlobStorageReadSettings"` (yes — `AzureBlobStorageReadSettings`, not `AzureBlobFSReadSettings` like plain `File` uses; it's still a Lakehouse-linked service under the hood, this is just the store-settings type Fabric expects for that combination):

```json
"source": {
  "type": "BinarySource",
  "storeSettings": { "type": "AzureBlobStorageReadSettings", "recursive": true },
  "formatSettings": { "type": "BinaryReadSettings" },
  "datasetSettings": {
    "type": "Binary",
    "linkedService": {
      "name": "SourceLakehouse",
      "properties": { "type": "Lakehouse", "typeProperties": { "workspaceId": "@item().ConnectionGuid", "artifactId": "@item().DatabaseName", "rootFolder": "Files" } }
    },
    "typeProperties": { "location": { "type": "LakehouseLocation", "fileName": "@item().SourceObject", "folderPath": "@item().SourceSchema" } }
  }
}
```

Both sink to `LandingLakehouse` exactly like their non-OneLake counterparts (`ParquetSink` for `OneLakeTable`, `BinarySink` for `OneLakeFile`).

## Custom — the escape hatch

`Custom` covers anything with no dedicated connector — REST APIs, SharePoint, Dataverse, Salesforce, etc. There is **no `PL_INGEST_CUSTOM` pipeline** and no generic `Copy` activity for it. Instead:

- `ingestion.Table.CustomNotebookName` is set to a hand-written notebook (mirroring `dim_customer.Notebook` / `sil_customer.Notebook`'s shape: `%run NB_MONZA_FUNCTIONS`, then whatever the source needs).
- `ingestion.Connection.ConnectionGuid` and `ingestion.Database.Name` aren't read by any pipeline for this type — populate them with placeholder values, they just need to satisfy the `NOT NULL` constraints.
- **`ingestion.vw_ActiveIngestTables` explicitly excludes `ConnectionType = 'Custom'` rows** — the view's `WHERE` clause ends `AND c.[ConnectionType] <> 'Custom'` (`config/metadata_schema.sql`). This is deliberate: no `PL_INGEST_*` Lookup will ever pick a Custom row up, by construction. The row still gets inserted into `ingestion.Table` (with `CustomNotebookName` set) purely so the table stays documented in the catalog — for `ai.FeatureSet`, audits, etc.
- You wire it in by hand instead: add the notebook to `config/items.yaml` (`workspace: Code`), then add a new `EP_INGEST_CUSTOM` `TridentNotebook` activity to `PL_RUN_ALL` (`dependsOn: []`, same retry policy as the other `EP_INGEST_*` stages) calling the notebook directly — not through `NB_RUN_REMOTE_PIPELINE`, since a Custom notebook deploys into Code (the same workspace `PL_RUN_ALL` itself runs in), so the cross-workspace bridge isn't needed. If the notebook lands to `Landing/Files/...`, add the new stage to `EP_LOAD_BRONZE`'s `dependsOn` too.

Full step-by-step for this path is in [Deployment-Guide](Deployment-Guide.md)'s "Custom sources" section.

## Why there's no 10th type: `ADF`

FMD Framework's own `ADF` connector type — pass-through metadata tracking for a table actually moved by an externally-orchestrated ADF pipeline — was **deliberately not ported**, not an oversight. It isn't a real data connector (it doesn't describe a source Copy activity at all, just records that some other system handled the move), and it doesn't fit Monza's self-contained model where every ingestion runs from inside Monza's own pipelines. If a client engagement has a legacy ADF pipeline you can't yet retire, model it as `Custom` instead (a notebook that triggers/monitors it) rather than trying to recreate `ADF`'s tracking-only semantics.

---

## Adding a new source table (Sql walkthrough)

This is the common case: a plain Azure SQL source, in a client's Fabric tenant where `Monza Ingestion (X)` and `SQL_METADATA_DATABASE` already exist (i.e. `NB_DEPLOY` has run at least once for this environment — see [Deployment-Guide](Deployment-Guide.md)).

**1. Register the Fabric Connection.** In the Fabric portal, open the `Monza Ingestion (X)` workspace (the same workspace every `PL_INGEST_*` pipeline lives in) → **New item → Connection** → Azure SQL Database. Point it at the client's server/database and credential it. Note the Connection's GUID (Connection → **Settings**, or from its URL).

**2. Insert the `ingestion.Connection` row.** Connect to `SQL_METADATA_DATABASE` and run:

```sql
INSERT INTO [ingestion].[Connection] ([Name], [ConnectionType], [ConnectionGuid], [IsActive])
VALUES ('contoso_salesdb', 'Sql', 'a1b2c3d4-e5f6-47a8-9b0c-1d2e3f4a5b6c', 1)
```

Use the Connection GUID you just noted in step 1. `ConnectionType` must exactly match one of the values in the table above (`'Sql'`, case as shown — the pipelines' Lookup queries filter on this string literally).

**3. Insert the `ingestion.Database` row**, naming the actual source database:

```sql
INSERT INTO [ingestion].[Database] ([ConnectionId], [Name], [IsActive])
SELECT [ConnectionId], 'SalesDB', 1
FROM [ingestion].[Connection] WHERE [Name] = 'contoso_salesdb'
```

**4. Insert the `ingestion.Table` row.** This one row drives the table's entire Source → Landing → Bronze flow:

```sql
INSERT INTO [ingestion].[Table] (
    [DatabaseId], [SourceSchema], [SourceObject], [FilePath], [FileType],
    [BronzeSchema], [BronzeName], [PrimaryKeys],
    [LoadType], [IncrementalColumn], [DeleteHandling], [IsActive]
)
SELECT
    [DatabaseId], 'dbo', 'Customer', 'contoso/customer', 'parquet',
    'dbo', 'customer', 'CustomerId',
    'Delta', 'ModifiedDateUtc', 'None', 1
FROM [ingestion].[Database] WHERE [Name] = 'SalesDB'
```

**Picking `LoadType`:**
- `'Full'` — simplest option, `SELECT *` every run. Pick this for small/reference tables where a full reload is cheap, or when there's no reliable "last changed" column to watermark on.
- `'Delta'` — requires `IncrementalColumn` (here, `ModifiedDateUtc`) to be set and to actually be maintained on every insert/update in the source. The view (`ingestion.vw_ActiveIngestTables`) wraps the base query in `WHERE [IncrementalColumn] > <last watermark>`, reading the watermark from `runtime.LoadWatermark`. `PrimaryKeys` (comma-separated; `CustomerId` here) is both the dedupe key and, for `Delta`, the MERGE key Bronze uses. Pick `Delta` for anything large or frequently updated — it's the common case for transactional tables like `Customer` or `SalesOrder`.

**Picking `DeleteHandling`** (only meaningful for `Delta` — `Full` reloads implicitly "see" deletes every run):
- `'None'` — default, and the right choice unless the source actually surfaces deletes and downstream consumers care about them. Most tables start here.
- `'SoftDelete'` — the source exposes a flag column (e.g. `IsDeleted`); set `IsDeletedColumn` to that column's name and it's carried through as a marker instead of the row being dropped.
- `'Reconcile'` — for sources that hard-delete rows with no flag at all; conceptually this needs a full comparison against the current source extract to detect rows that disappeared, which costs a full source read. Confirm the exact mechanics in `NB_LOAD_BRONZE.Notebook` before relying on it for a client that's sensitive to deleted-row accuracy.

`FilePath` (`'contoso/customer'` above) is the target folder under `Landing/Files/` — keep it collision-free across sources, e.g. `<connection-or-client-shorthand>/<table>`.

**5. Verify it shows up in the Lookup view** before waiting on a pipeline run:

```sql
SELECT * FROM [ingestion].[vw_ActiveIngestTables]
WHERE [ConnectionType] = 'Sql' AND [SourceObject] = 'Customer'
```

You should get exactly one row back, with `ResolvedSourceQuery` showing either `SELECT * FROM [dbo].[Customer]` (first run, no watermark yet — defaults to `> '1900-01-01'`) or a watermark-bounded `WHERE [ModifiedDateUtc] > '...'` on subsequent runs. If the row doesn't appear, check `IsActive = 1` on all three of `Connection`, `Database` and `Table` — the view's `WHERE` clause requires all three, plus `ConnectionType <> 'Custom'`.

**6. Run it.** Trigger `PL_INGEST_SQL` directly (it'll pick up every active `Sql` row, including this one), or run `PL_RUN_ALL` from Code to exercise the full chain through `NB_LOAD_BRONZE`. Confirm the parquet file landed at `Landing/Files/contoso/customer/Customer.parquet` and that `[dbo].[customer]` exists in the Bronze lakehouse with the expected row count.

For the full connection-bootstrap prerequisites (the metadata-database Connection, `metadata_connection_guid`, workspace/lakehouse creation) see [Deployment-Guide](Deployment-Guide.md). For the complete `ingestion.Connection`/`Database`/`Table` column reference see [Metadata-Model](Metadata-Model.md).
