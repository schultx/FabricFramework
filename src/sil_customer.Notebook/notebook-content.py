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

# # sil_customer (demo)
# Hand-written per-table Silver notebook -- a plain 1:1 reshape of the Bronze
# `customer` entity, kept purely to exercise the `NB_LOAD_SILVER`/
# `PL_LOAD_SILVER` mechanism end to end. Gold still reads Bronze directly for
# this demo (nothing here is actually reused by 2+ Gold objects, which is the
# real rule for when a table earns a Silver notebook).
#
# ## Data flow
# 1. Read `dbo.customer` (loaded by `NB_LOAD_BRONZE` from
#    `demodata/customer.csv` via the `PL_INGEST_FILE` pipeline) -- unqualified
#    by lakehouse name since Bronze is this notebook's own default lakehouse;
#    only *sibling* lakehouses need the 3-part `lakehouse.schema.table` form
# 2. Build `temp_silver_customer`, dropping the Bronze-only audit column
# 3. Overwrite `Silver.silver.customer`

# CELL ********************

%run NB_KEYSTONE_FUNCTIONS

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Notebook parameters
silver_schema = 'silver'
silver_name = 'customer'         # -> Silver.silver.customer

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read Bronze.dbo.customer by direct OneLake path rather than a Spark-catalog table
# name -- a notebook's own default lakehouse can be written via saveAsTable() using its
# own name in the path, but reading it back the same way trips Spark's SQL parser
# ("spark_catalog requires a single-part namespace"). A path read sidesteps catalog
# resolution entirely and works the same way regardless of which lakehouse is default.
_data_ws_id = resolve_workspace_id(data_workspace_name())
_bronze_lh_id = resolve_lakehouse_id(_data_ws_id, "Bronze")
spark.read.format("delta").load(
    onelake_path(_data_ws_id, _bronze_lh_id, "Tables", "dbo/customer")
).createOrReplaceTempView("bronze_customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_silver_customer AS
# MAGIC SELECT
# MAGIC     CustomerId,
# MAGIC     FirstName,
# MAGIC     LastName,
# MAGIC     Company,
# MAGIC     City,
# MAGIC     Country,
# MAGIC     Email,
# MAGIC     Website,
# MAGIC     SubscriptionDate
# MAGIC FROM bronze_customer

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df = spark.table('temp_silver_customer').withColumn("silver_loaded_datetime", F.current_timestamp())

_ensure_schema("Silver", silver_schema)
target_table = f"Silver.{silver_schema}.{silver_name}"
silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
print(f"wrote {silver_df.count()} rows to {target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
