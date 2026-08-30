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

# # NB_LOAD_SILVER
# Only deployed when an environment sets `include_silver: true` in
# `config/environments.yaml` -- meaning a Bronze entity's cleansed logic is
# reused by two or more Gold objects and shouldn't be recomputed per Gold load.
#
# One Spark session, loops over every active `catalog.SilverEntity` row (or
# just `silver_entity_name`, if set) -- `PL_LOAD_SILVER` is a single
# `TridentNotebook` activity, same shape as `PL_LOAD_BRONZE`.
#
# Each Silver entity is defined by a plain 1:1 reshape of its source Bronze
# table (`catalog.SilverEntity` has no separate query column by design -- a
# Silver entity mirrors exactly one Bronze entity; anything more than that
# belongs in Gold's own transform).

# CELL ********************

%run NB_KEYSTONE_FUNCTIONS

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Parameters
silver_entity_name = ""   # optional catalog.SilverEntity.Name filter -- empty = every active row

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filter_sql = "AND se.[Name] = ?" if silver_entity_name else ""
params = (silver_entity_name,) if silver_entity_name else ()

entities = catalog_query(
    f"""
    SELECT se.[SilverEntityId], se.[Schema] AS SilverSchema, se.[Name] AS SilverName,
           be.[Schema] AS BronzeSchema, be.[Name] AS BronzeName
    FROM [catalog].[SilverEntity] se
    JOIN [catalog].[BronzeEntity] be ON se.[BronzeEntityId] = be.[BronzeEntityId]
    WHERE se.[IsActive] = 1 {filter_sql}
    """,
    params
)

if silver_entity_name and len(entities) != 1:
    raise ValueError(f"Expected exactly one active SilverEntity named '{silver_entity_name}', found {len(entities)}")

print(f"Loading {len(entities)} active Silver entity(ies)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

_silver_data_ws_id = resolve_workspace_id(data_workspace_name())
_silver_bronze_lh_id = resolve_lakehouse_id(_silver_data_ws_id, "Bronze")

for entity in entities:
    print(f"-- Loading Silver entity '{entity['SilverName']}' from Bronze.{entity['BronzeSchema']}.{entity['BronzeName']}")

    # Read by direct OneLake path, not a Spark-catalog table name -- Bronze is this
    # notebook's own default lakehouse, and reading it back via spark.table() with even
    # an unqualified name trips Spark's SQL parser ("spark_catalog requires a
    # single-part namespace") once a lakehouse is schema-enabled.
    bronze_path = onelake_path(
        _silver_data_ws_id, _silver_bronze_lh_id, "Tables", f"{entity['BronzeSchema']}/{entity['BronzeName']}"
    )
    silver_df = spark.read.format("delta").load(bronze_path).withColumn("silver_loaded_datetime", F.current_timestamp())

    _ensure_schema("Silver", entity["SilverSchema"])
    target_table = f"Silver.{entity['SilverSchema']}.{entity['SilverName']}"
    silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
    print(f"   wrote {silver_df.count()} rows to {target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
