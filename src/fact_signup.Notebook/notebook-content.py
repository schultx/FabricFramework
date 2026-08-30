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

# # fact_signup (demo)
# Gold fact built with `load_fact()` from `NB_STRATUM_FUNCTIONS`, sourced
# directly from Bronze -- one row per customer signup event, grained on
# `SubscriptionDate`.
#
# **Run `dim_customer` before this notebook** -- `load_fact()` auto-maps
# `customer_key` to `customer_sk` by looking up `gold.dim_customer`, which
# must already exist.
#
# ## Data flow
# 1. Read `Bronze.dbo.customer`
# 2. Build `temp_fact_signup` with a `customer_key` business key and the
#    signup date as the fact's only measure-adjacent attribute
# 3. Load to `gold.fact_signup` via `load_fact()` (full overwrite, auto FK mapping on)

# CELL ********************

%run NB_STRATUM_FUNCTIONS

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Notebook parameters
fact_name = 'signup'             # -> gold.fact_signup
destination_lakehouse = 'Gold'
write_mode = 'overwrite'
recreate_table = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_fact_signup AS
# MAGIC SELECT
# MAGIC     -- Business key -> auto-mapped to dim_customer.customer_sk by load_fact()
# MAGIC     CAST(CustomerId AS STRING) AS customer_key,
# MAGIC
# MAGIC     -- Degenerate dimension / event grain
# MAGIC     CAST(SubscriptionDate AS DATE) AS SignupDate,
# MAGIC     Country
# MAGIC FROM Bronze.dbo.customer

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_df = spark.table('temp_fact_signup')

output_df = load_fact(
    df=fact_df,
    lakehouse_name=destination_lakehouse,
    table_name=fact_name,
    write_mode=write_mode,
    recreate_table=recreate_table,
    auto_map_foreign_keys=True
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
