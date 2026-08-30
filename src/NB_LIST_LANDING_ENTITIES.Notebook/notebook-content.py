# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # NB_LIST_LANDING_ENTITIES
# Bridge between the metadata catalog and the ingestion pipelines. Pipelines
# can't hold their own Connection to the metadata SQL Database without an
# environment-specific GUID baked into the pipeline JSON -- so instead this
# notebook does the lookup (using the same token-based `catalog_query()` every
# other notebook uses) and hands the result back as a `TridentNotebook`
# activity's exit value, which `PL_INGEST_SQL` / `PL_INGEST_FILE` then
# `ForEach` over via `activity('LK_...').output.result.exitValue`.

# CELL ********************

%run NB_STRATUM_FUNCTIONS

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Parameters
connector_type = ""   # catalog.Connection.Type -- e.g. "SQL" or "FILE" -- required

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

if not connector_type:
    raise ValueError("connector_type parameter is required")

rows = catalog_query(
    """
    SELECT
        le.[LandingEntityId], le.[SourceSchema], le.[SourceObject], le.[SourceQuery],
        le.[FilePath], le.[FileType], le.[IsIncremental], le.[IncrementalColumn],
        s.[Namespace], c.[ConnectionGuid], c.[Type] AS ConnectionType
    FROM [catalog].[LandingEntity] le
    JOIN [catalog].[Source] s ON le.[SourceId] = s.[SourceId]
    JOIN [catalog].[Connection] c ON s.[ConnectionId] = c.[ConnectionId]
    WHERE le.[IsActive] = 1 AND s.[IsActive] = 1 AND c.[IsActive] = 1 AND c.[Type] = ?
    """,
    (connector_type,)
)

# UNIQUEIDENTIFIER columns come back as uuid.UUID -- make them JSON-serializable strings
for row in rows:
    if row.get("ConnectionGuid") is not None:
        row["ConnectionGuid"] = str(row["ConnectionGuid"])

print(f"Found {len(rows)} active LandingEntity row(s) for connector type '{connector_type}'")
notebookutils.notebook.exit(json.dumps(rows))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
