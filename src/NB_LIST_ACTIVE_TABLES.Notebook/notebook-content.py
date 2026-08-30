# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # NB_LIST_ACTIVE_TABLES
# Bridge between the metadata catalog and the ingestion pipelines. Pipelines
# can't hold their own Connection to the metadata SQL Database without an
# environment-specific GUID baked into the pipeline JSON -- so instead this
# notebook does the lookup (using the same token-based `catalog_query()` every
# other notebook uses) and hands the result back as a `TridentNotebook`
# activity's exit value, which `PL_INGEST_SQL` / `PL_INGEST_FILE` then
# `ForEach` over via `activity('LK_...').output.result.exitValue`.
#
# A Data Factory-style Copy activity can't easily construct per-row
# conditional SQL, so this notebook does the smart part and hands the
# pipeline an already-resolved, ready-to-run `SourceQuery` per active
# `ingestion.Table` row: `SELECT *` (or the row's custom override) for
# `LoadType = 'Full'`, or the same wrapped in a watermark-bounded `WHERE`
# clause -- read from `runtime.LoadWatermark` -- for `LoadType = 'Delta'`.
# The pipeline's `Copy` activity then stays a thin shell that just executes
# `@item().SourceQuery`.

# CELL ********************

%run NB_KEYSTONE_FUNCTIONS

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Parameters
connector_type = ""   # ingestion.Connection.ConnectionType -- "Sql" or "File" -- required

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

if not connector_type:
    raise ValueError("connector_type parameter is required")

# Default watermark for a Delta table that has never completed a load yet --
# assumes a date/datetime-like IncrementalColumn (the common case). A table
# incrementing on a numeric/identity column would need a different sentinel;
# not needed by anything in this framework today.
DEFAULT_DELTA_WATERMARK = "1900-01-01"

rows = catalog_query(
    """
    SELECT
        t.[TableId], t.[SourceSchema], t.[SourceObject], t.[SourceQuery],
        t.[FilePath], t.[FileType], t.[LoadType], t.[IncrementalColumn],
        d.[Name] AS DatabaseName, c.[ConnectionGuid], c.[ConnectionType]
    FROM [ingestion].[Table] t
    JOIN [ingestion].[Database] d ON t.[DatabaseId] = d.[DatabaseId]
    JOIN [ingestion].[Connection] c ON d.[ConnectionId] = c.[ConnectionId]
    WHERE t.[IsActive] = 1 AND d.[IsActive] = 1 AND c.[IsActive] = 1 AND c.[ConnectionType] = ?
    """,
    (connector_type,)
)

watermark_rows = catalog_query(
    "SELECT [EntityId], [LastValue] FROM [runtime].[LoadWatermark] WHERE [EntityType] = 'Table'"
)
watermark_by_table_id = {r["EntityId"]: r["LastValue"] for r in watermark_rows if r["LastValue"] is not None}


def build_source_query(row: dict) -> str:
    """Resolve the exact ready-to-run SELECT for one active ingestion.Table row."""
    if row["SourceQuery"]:
        base = f"SELECT * FROM ({row['SourceQuery']}) AS src_query"
    else:
        qualified = f"{row['SourceSchema']}.{row['SourceObject']}" if row["SourceSchema"] else row["SourceObject"]
        base = f"SELECT * FROM {qualified}"

    if row["LoadType"] != "Delta":
        return base

    watermark = watermark_by_table_id.get(row["TableId"], DEFAULT_DELTA_WATERMARK)
    return f"SELECT * FROM ({base}) AS w WHERE {row['IncrementalColumn']} > '{watermark}'"


for row in rows:
    row["SourceQuery"] = build_source_query(row)
    # UNIQUEIDENTIFIER columns come back as uuid.UUID -- make them JSON-serializable strings
    if row.get("ConnectionGuid") is not None:
        row["ConnectionGuid"] = str(row["ConnectionGuid"])

print(f"Found {len(rows)} active Table row(s) for connector type '{connector_type}'")
notebookutils.notebook.exit(json.dumps(rows))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
