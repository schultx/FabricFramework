# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "__BRONZE_LAKEHOUSE_ID__",
# META       "default_lakehouse_name": "Bronze",
# META       "default_lakehouse_workspace_id": "__DATA_WORKSPACE_ID__",
# META       "known_lakehouses": [
# META         {
# META           "id": "__BRONZE_LAKEHOUSE_ID__"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # NB_LOAD_BRONZE
# Generic Landing -> Bronze loader. One Spark session, loops over every active
# `ingestion.Table` row (or just `bronze_entity_name`, if set, for a targeted
# rerun) -- `PL_LOAD_BRONZE` is a single `TridentNotebook` activity, no
# per-entity pipeline fan-out needed.
#
# `ingestion.Table.LoadType` decides how each entity is written to Bronze:
# - **Full** -- overwrite Bronze completely every run. Deletes are handled for
#   free since Bronze exactly mirrors the latest full extract.
# - **Delta** -- `MERGE` (upsert) the batch into Bronze keyed by `PrimaryKeys`
#   (same `DeltaTable` MERGE idiom as `NB_KEYSTONE_FUNCTIONS`'s Gold facade),
#   then advance `runtime.LoadWatermark` to `MAX(IncrementalColumn)` seen.
#
# `ingestion.Table.DeleteHandling` is only meaningful for Delta entities:
# - **None** (default) -- no special handling. Accepted limitation: a row
#   deleted at the source stays in Bronze until a full reload.
# - **SoftDelete** -- `IsDeletedColumn` rides along as a normal column through
#   the merge; no special code needed. Downstream consumers should filter it.
# - **Reconcile** -- after the merge, fetch just the `PrimaryKeys` column(s)
#   from the FULL source table and remove/tombstone any Bronze row whose key
#   is no longer present at the source.
#
# Cleansing is intentionally generic and metadata-driven, not per-table code:
# dedupe on `PrimaryKeys`, drop rows missing a primary key, and any additional
# rule in `CleansingRules` (JSON array of `{"type": "not_null", "column": "..."}`
# / `{"type": "dedupe_keep_latest", "orderBy": "..."}`).

# CELL ********************

%run NB_KEYSTONE_FUNCTIONS

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Parameters
bronze_entity_name = ""   # optional ingestion.Table.BronzeName filter -- empty = every active row

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

filter_sql = "AND t.[BronzeName] = ?" if bronze_entity_name else ""
params = (bronze_entity_name,) if bronze_entity_name else ()

# Deliberately doesn't filter on Database.IsActive / Connection.IsActive -- Bronze
# loads process whatever's already landed, independent of whether the source
# Connection is still active (matches the demo Connection row, which is seeded
# IsActive = 0 since demodata/customer.csv is uploaded to Landing directly).
entities = catalog_query(
    f"""
    SELECT
        t.[TableId], t.[BronzeSchema], t.[BronzeName], t.[PrimaryKeys], t.[CleansingRules],
        t.[FilePath], t.[FileType], t.[LoadType], t.[IncrementalColumn],
        t.[DeleteHandling], t.[IsDeletedColumn],
        t.[SourceSchema], t.[SourceObject],
        c.[ConnectionGuid], c.[ConnectionType]
    FROM [ingestion].[Table] t
    JOIN [ingestion].[Database] d ON t.[DatabaseId] = d.[DatabaseId]
    JOIN [ingestion].[Connection] c ON d.[ConnectionId] = c.[ConnectionId]
    WHERE t.[IsActive] = 1 {filter_sql}
    """,
    params
)

if bronze_entity_name and len(entities) != 1:
    raise ValueError(f"Expected exactly one active Table named '{bronze_entity_name}', found {len(entities)}")

print(f"Loading {len(entities)} active Bronze table(s)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# HELPERS -- Delta watermark + Reconcile delete handling
# ============================================================

def _update_watermark(table_id: int, new_value: str) -> None:
    """Upsert runtime.LoadWatermark for this ingestion.Table (EntityType is always 'Table')."""
    catalog_execute(
        """
        MERGE [runtime].[LoadWatermark] AS target
        USING (SELECT 'Table' AS EntityType, ? AS EntityId) AS src
        ON target.[EntityType] = src.[EntityType] AND target.[EntityId] = src.[EntityId]
        WHEN MATCHED THEN UPDATE SET [LastValue] = ?, [LastRunUtc] = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT ([EntityType], [EntityId], [LastValue], [LastRunUtc])
            VALUES ('Table', src.[EntityId], ?, SYSUTCDATETIME());
        """,
        (table_id, new_value, new_value)
    )


def _resolve_connection_endpoint(connection_guid: str) -> dict:
    """
    Look up a source Fabric Connection item's server/database via the
    Connections API. Used only by Reconcile's lightweight PK-only fetch --
    the main Copy activities move data through the pipeline's own Connection
    resolution, not through Spark JDBC.
    """
    resp = requests.get(f"{FABRIC_API}/connections/{connection_guid}", headers=_fabric_headers())
    resp.raise_for_status()
    parameters = resp.json()["connectionDetails"]["parameters"]
    values = {p["name"]: p["value"] for p in parameters}
    return {"server": values.get("server"), "database": values.get("database")}


def _read_source_sql(connection_guid: str, query: str) -> DataFrame:
    """
    Lightweight JDBC read of a source SQL query, authenticated with this
    notebook's own Entra identity (same AAD-token idiom as catalog_connection(),
    pointed at the source Connection instead of the metadata catalog).
    """
    endpoint = _resolve_connection_endpoint(connection_guid)
    token = notebookutils.credentials.getToken("https://database.windows.net/.default")
    return (
        spark.read.format("jdbc")
        .option("url", f"jdbc:sqlserver://{endpoint['server']}:1433;database={endpoint['database']};encrypt=true")
        .option("query", query)
        .option("accessToken", token)
        .load()
    )


def _reconcile_deletes(entity: dict, primary_keys: list, target_table: str) -> None:
    """
    DeleteHandling='Reconcile': fetch just the PrimaryKeys column(s) from the
    FULL source table and remove/tombstone any Bronze row whose key is no
    longer present at the source. Only implemented for Sql connections -- a
    File connection's "source" is a single file already copied to Landing in
    full each run, so there's no separate lightweight PK-only fetch to make.
    """
    if entity["ConnectionType"] != "Sql":
        print(f"   WARNING: DeleteHandling='Reconcile' is only implemented for Sql connections; "
              f"skipping reconcile for '{entity['BronzeName']}' (ConnectionType='{entity['ConnectionType']}')")
        return

    pk_cols = ", ".join(primary_keys)
    qualified = f"{entity['SourceSchema']}.{entity['SourceObject']}" if entity["SourceSchema"] else entity["SourceObject"]
    source_keys_df = _read_source_sql(entity["ConnectionGuid"], f"SELECT {pk_cols} FROM {qualified}")

    delta_table = DeltaTable.forName(spark, target_table)
    bronze_keys_df = delta_table.toDF().select(*primary_keys)
    missing_keys_df = bronze_keys_df.join(source_keys_df, on=primary_keys, how="left_anti")
    missing_count = missing_keys_df.count()

    if missing_count == 0:
        print(f"   Reconcile: no missing keys for {target_table}")
        return

    condition = " AND ".join(f"target.{pk} = source.{pk}" for pk in primary_keys)
    merge_builder = delta_table.alias("target").merge(missing_keys_df.alias("source"), condition)

    if entity["IsDeletedColumn"]:
        merge_builder.whenMatchedUpdate(set={entity["IsDeletedColumn"]: "true"}).execute()
        print(f"   Reconcile: flagged {missing_count} row(s) as deleted in {target_table}")
    else:
        merge_builder.whenMatchedDelete().execute()
        print(f"   Reconcile: hard-deleted {missing_count} row(s) from {target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data_ws_id = resolve_workspace_id(data_workspace_name())
landing_id = resolve_lakehouse_id(data_ws_id, "Landing")

for entity in entities:
    primary_keys = [k.strip() for k in entity["PrimaryKeys"].split(",") if k.strip()]
    cleansing_rules = json.loads(entity["CleansingRules"]) if entity["CleansingRules"] else []
    load_type = entity["LoadType"]
    delete_handling = entity["DeleteHandling"]

    print(f"-- Loading Bronze table '{entity['BronzeName']}' from Landing/{entity['FilePath']} "
          f"({entity['FileType']}, LoadType={load_type})")

    source_path = onelake_path(data_ws_id, landing_id, "Files", entity["FilePath"])
    reader = spark.read
    if entity["FileType"].lower() == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")
    raw_df = reader.format(entity["FileType"].lower()).load(source_path)

    # ---- generic cleansing: drop rows missing a primary key, dedupe on it,
    # then apply whatever extra rules the metadata specifies. IsDeletedColumn
    # (DeleteHandling='SoftDelete') needs no special handling here -- it just
    # rides along as an ordinary column already present in raw_df. ----
    clean_df = raw_df.dropna(subset=primary_keys)
    clean_df = clean_df.dropDuplicates(primary_keys)

    for rule in cleansing_rules:
        rule_type = rule.get("type")
        if rule_type == "not_null":
            clean_df = clean_df.dropna(subset=[rule["column"]])
        elif rule_type == "dedupe_keep_latest":
            order_col = rule["orderBy"]
            clean_df = (
                clean_df.withColumn("_rn", F.row_number().over(
                    Window.partitionBy(*primary_keys).orderBy(F.col(order_col).desc())
                ))
                .filter("_rn = 1")
                .drop("_rn")
            )
        else:
            print(f"Warning: unknown cleansing rule type '{rule_type}', skipping")

    clean_df = clean_df.withColumn("bronze_loaded_datetime", F.current_timestamp())

    _ensure_schema("Bronze", entity["BronzeSchema"])
    target_table = f"Bronze.{entity['BronzeSchema']}.{entity['BronzeName']}"
    table_exists = spark.catalog.tableExists(target_table)

    if load_type == "Full":
        # Bronze exactly mirrors the latest full extract every run, so source
        # deletes are handled for free -- Reconcile would be redundant
        # busywork here, not a bug to "fix", so it's silently skipped even if
        # DeleteHandling='Reconcile' is misconfigured on a Full table.
        clean_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
        print(f"   wrote {clean_df.count()} rows to {target_table} (full overwrite)")

    elif load_type == "Delta":
        if not table_exists:
            clean_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
            print(f"   wrote {clean_df.count()} rows to {target_table} (first load)")
        else:
            delta_table = DeltaTable.forName(spark, target_table)
            merge_conditions = " AND ".join(f"target.{pk} = source.{pk}" for pk in primary_keys)
            update_dict = {col: f"source.{col}" for col in clean_df.columns}
            delta_table.alias("target").merge(clean_df.alias("source"), merge_conditions) \
                .whenMatchedUpdate(set=update_dict) \
                .whenNotMatchedInsertAll() \
                .execute()
            print(f"   merged {clean_df.count()} rows into {target_table}")

        if entity["IncrementalColumn"] and entity["IncrementalColumn"] in clean_df.columns:
            max_value = clean_df.agg(F.max(entity["IncrementalColumn"])).collect()[0][0]
            if max_value is not None:
                _update_watermark(entity["TableId"], str(max_value))
                print(f"   watermark advanced to {max_value}")

        if delete_handling == "Reconcile":
            _reconcile_deletes(entity, primary_keys, target_table)

    else:
        raise ValueError(f"Invalid LoadType '{load_type}' for table '{entity['BronzeName']}'. Must be 'Full' or 'Delta'.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
