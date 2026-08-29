# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b6e09e2a-99d3-4a4a-9d6e-8d978cd44956",
# META       "default_lakehouse_name": "LH_GOLD_LAYER",
# META       "default_lakehouse_workspace_id": "c90c9850-99fe-4050-899c-417cc30d7f70",
# META       "known_lakehouses": [
# META         {
# META           "id": "b6e09e2a-99d3-4a4a-9d6e-8d978cd44956"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # dim_customer (temp / demo)
# Notebook-pattern equivalent of the `gold.DimCustomer` materialized lake view in
# `NB_MLV_DEMO_GOLD`, built instead with `load_dimension()` from `NB_GOLD_LOADER_FUNCTIONS`.
# Same demo source shortcuts (`Sales_vCustomers`, `Sales_BuyingGroups`), so it can run
# side by side with the MLV version to compare the two Gold-layer approaches.
#
# ## Data flow
# 1. Read `LH_GOLD_LAYER.dbo.Sales_vCustomers` + `Sales_BuyingGroups` (FMD demo shortcuts)
# 2. Build `temp_dim_customer` with a `customer_key` business key
# 3. Load to `gold.dim_customer` via `load_dimension()` (SCD1)

# CELL ********************

%run NB_GOLD_LOADER_FUNCTIONS

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Notebook parameters
dimension_name = 'customer'          # -> gold.dim_customer
destination_lakehouse = 'LH_GOLD_LAYER'
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
# MAGIC     CAST(C.CustomerID AS STRING) AS customer_key,
# MAGIC
# MAGIC     C.CustomerName,
# MAGIC     BG.BuyingGroupName,
# MAGIC     C.CreditLimit,
# MAGIC     C.AccountOpenedDate,
# MAGIC     C.StandardDiscountPercentage,
# MAGIC     C.IsStatementSent,
# MAGIC     C.IsOnCreditHold,
# MAGIC     C.PaymentDays,
# MAGIC     C.PhoneNumber,
# MAGIC     C.DeliveryRun
# MAGIC FROM LH_GOLD_LAYER.dbo.Sales_vCustomers C
# MAGIC LEFT JOIN LH_GOLD_LAYER.dbo.Sales_BuyingGroups BG
# MAGIC     ON C.BuyingGroupID = BG.BuyingGroupID

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
