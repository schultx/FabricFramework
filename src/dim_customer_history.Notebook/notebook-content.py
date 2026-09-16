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

# # dim_customer_history (demo -- SCD2 reference example)
# Gold dimension built with `load_dimension()` from `NB_MONZA_FUNCTIONS`, same
# source as `dim_customer` (`Bronze.dbo.customer`) but modeled as **SCD Type 2**
# instead of Type 1: a change to a tracked attribute (`Company`/`City`/`Country`)
# closes out the old row (`is_current = false`, `valid_to` set) and inserts a new
# current row with a fresh surrogate key, instead of overwriting in place.
#
# **Why this notebook exists**: `dim_customer` is hardcoded `dimension_type =
# 'scd1'`, so `write_dimension_type2()` -- half of the Gold facade -- was
# previously never exercised by anything runnable in this framework. Two real
# bugs were found and fixed in that code path by reading it, not by running it
# (see Improvement-Roadmap.md, Silver & Gold #1 and #2). This notebook is the
# regression check going forward: run it, change a tracked attribute at the
# source, run it again, and confirm history forms correctly.
#
# ## Data flow
# 1. Read `dbo.customer` -- unqualified by lakehouse name since Bronze is this
#    notebook's own default lakehouse; only *sibling* lakehouses need the
#    3-part `lakehouse.schema.table` form
# 2. Build `temp_dim_customer_history` with a `customer_history_key` business key
# 3. Load to `gold.dim_customer_history` via `load_dimension()` (SCD2)
#
# ## Exercising the history path live
# 1. Run this notebook once -- every customer gets exactly one current row.
# 2. Change one customer's `Company` (or `City`/`Country`) in the source and
#    re-run `NB_LOAD_BRONZE` so the change lands in `Bronze.dbo.customer`.
# 3. Run this notebook again. Expect: the changed customer's old row is now
#    `is_current = false` with `valid_to` set, a new row exists for them with a
#    **new** surrogate key, and every unchanged customer's row (surrogate key
#    included) is untouched.
# 4. `fact_signup` maps a `customer_history_key` column against this dimension
#    alongside its existing `customer_key` -> `dim_customer` mapping -- confirm
#    it resolves against the *current* row only (Improvement-Roadmap.md, Silver
#    & Gold #1: a fact must never fan out against SCD2 history).

# CELL ********************

%run NB_MONZA_FUNCTIONS

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Notebook parameters
dimension_name = 'customer_history'      # -> gold.dim_customer_history
destination_lakehouse = 'Gold'
dimension_type = 'scd2'
full_refresh = False
recreate_table = False
valid_from_column = None                 # no natural "changed on" source column -- defaults to current_date()

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_dim_customer_history AS
# MAGIC SELECT
# MAGIC     -- Business key -- named to match this dimension's OWN table name
# MAGIC     -- (customer_history), not dim_customer's. _discover_and_map_foreign_keys
# MAGIC     -- requires the fact's '<name>_key' column and the dimension's own
# MAGIC     -- business-key column to be named identically for the join to resolve --
# MAGIC     -- see fact_signup's customer_history_key.
# MAGIC     CAST(CustomerId AS STRING) AS customer_history_key,
# MAGIC
# MAGIC     -- Tracked attributes -- a change to any of these forms a new history row
# MAGIC     FirstName,
# MAGIC     LastName,
# MAGIC     Company,
# MAGIC     City,
# MAGIC     Country,
# MAGIC     Email,
# MAGIC     Website
# MAGIC FROM bronze_customer

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_df = spark.table('temp_dim_customer_history')

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
