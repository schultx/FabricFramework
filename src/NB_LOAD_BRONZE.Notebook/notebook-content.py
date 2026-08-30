# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # NB_LOAD_BRONZE
# Generic Landing -> Bronze loader. One Spark session, loops over every active
# `catalog.BronzeEntity` row (or just `bronze_entity_name`, if set, for a
# targeted rerun) -- `PL_LOAD_BRONZE` is a single `TridentNotebook` activity,
# no per-entity pipeline fan-out needed.
#
# Cleansing is intentionally generic and metadata-driven, not per-table code:
# dedupe on `PrimaryKeys`, drop rows missing a primary key, and any additional
# rule in `CleansingRules` (JSON array of `{"type": "not_null", "column": "..."}`
# / `{"type": "dedupe_keep_latest", "orderBy": "..."}`).

# CELL ********************

%run NB_STRATUM_FUNCTIONS

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parameters
bronze_entity_name = ""   # optional catalog.BronzeEntity.Name filter -- empty = every active row

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
# META {
# META   "tags": [
# META     "parameters"
# META   ]
# META }

# CELL ********************

import json

filter_sql = "AND be.[Name] = ?" if bronze_entity_name else ""
params = (bronze_entity_name,) if bronze_entity_name else ()

entities = catalog_query(
    f"""
    SELECT be.[BronzeEntityId], be.[Schema], be.[Name], be.[PrimaryKeys], be.[CleansingRules],
           le.[FilePath], le.[FileType]
    FROM [catalog].[BronzeEntity] be
    JOIN [catalog].[LandingEntity] le ON be.[LandingEntityId] = le.[LandingEntityId]
    WHERE be.[IsActive] = 1 {filter_sql}
    """,
    params
)

if bronze_entity_name and len(entities) != 1:
    raise ValueError(f"Expected exactly one active BronzeEntity named '{bronze_entity_name}', found {len(entities)}")

print(f"Loading {len(entities)} active Bronze entity(ies)")

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

    print(f"-- Loading Bronze entity '{entity['Name']}' from Landing/{entity['FilePath']} ({entity['FileType']})")

    source_path = onelake_path(data_ws_id, landing_id, "Files", entity["FilePath"])
    reader = spark.read
    if entity["FileType"].lower() == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")
    raw_df = reader.format(entity["FileType"].lower()).load(source_path)

    # ---- generic cleansing: drop rows missing a primary key, dedupe on it,
    # then apply whatever extra rules the metadata specifies ----
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

    _ensure_schema("Bronze", entity["Schema"])
    target_table = f"Bronze.{entity['Schema']}.{entity['Name']}"
    clean_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
    print(f"   wrote {clean_df.count()} rows to {target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
