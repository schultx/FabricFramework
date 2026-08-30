# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # dim_customer (demo)
# Gold dimension built with `load_dimension()` from `NB_STRATUM_FUNCTIONS`,
# sourced **directly from Bronze** -- this shape isn't reused anywhere else, so
# there's no Silver entity for it.
#
# ## Data flow
# 1. Read `Bronze.dbo.customer` (loaded by `NB_LOAD_BRONZE` from
#    `demodata/customer.csv` via the `PL_INGEST_FILE` pipeline)
# 2. Build `temp_dim_customer` with a `customer_key` business key
# 3. Load to `gold.dim_customer` via `load_dimension()` (SCD1)

# CELL ********************

%run NB_STRATUM_FUNCTIONS

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Notebook parameters
dimension_name = 'customer'      # -> gold.dim_customer
destination_lakehouse = 'Gold'
dimension_type = 'scd1'
full_refresh = False
recreate_table = False
valid_from_column = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_dim_customer AS
# MAGIC SELECT
# MAGIC     -- Business key (auto-mapped by fact tables via '_key' -> '_sk' convention)
# MAGIC     CAST(CustomerId AS STRING) AS customer_key,
# MAGIC
# MAGIC     FirstName,
# MAGIC     LastName,
# MAGIC     Company,
# MAGIC     City,
# MAGIC     Country,
# MAGIC     Email,
# MAGIC     Website,
# MAGIC     SubscriptionDate
# MAGIC FROM Bronze.dbo.customer

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_df = spark.table('temp_dim_customer')

output_df = load_dimension(
    df=dim_df,
    lakehouse_name=destination_lakehouse,
    table_name=dimension_name,
    dimension_type=dimension_type,
    valid_from_column=valid_from_column,
    full_refresh=full_refresh,
    recreate_table=recreate_table
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
