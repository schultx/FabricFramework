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

# # fact_orderlines (temp / demo)
# Notebook-pattern equivalent of the `gold.FactOrderLines` materialized lake view in
# `NB_MLV_DEMO_GOLD`, built instead with `load_fact()` from `NB_GOLD_LOADER_FUNCTIONS`.
#
# **Run `dim_customer` before this notebook** — `load_fact()` auto-maps `customer_key`
# to `customer_sk` by looking up `gold.dim_customer`, which must already exist.
#
# ## Data flow
# 1. Read `LH_GOLD_LAYER.dbo.Sales_OrderLines` + `Sales_Orders` (FMD demo shortcuts)
# 2. Build `temp_fact_orderlines` with a `customer_key` business key and a computed `LineTotal` measure
# 3. Load to `gold.fact_orderlines` via `load_fact()` (full overwrite, auto FK mapping on)

# CELL ********************

%run NB_GOLD_LOADER_FUNCTIONS

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Notebook parameters
fact_name = 'orderlines'             # -> gold.fact_orderlines
destination_lakehouse = 'LH_GOLD_LAYER'
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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_fact_orderlines AS
# MAGIC SELECT
# MAGIC     -- Business key -> auto-mapped to dim_customer.customer_sk by load_fact()
# MAGIC     CAST(SO.CustomerID AS STRING) AS customer_key,
# MAGIC
# MAGIC     -- Degenerate dimensions
# MAGIC     SOL.OrderLineID,
# MAGIC     SOL.OrderID,
# MAGIC     SOL.StockItemID,
# MAGIC     SOL.Description,
# MAGIC     SOL.PackageTypeID,
# MAGIC     SO.OrderDate,
# MAGIC     SO.ExpectedDeliveryDate,
# MAGIC
# MAGIC     -- Measures
# MAGIC     SOL.Quantity,
# MAGIC     SOL.UnitPrice,
# MAGIC     SOL.TaxRate,
# MAGIC     SOL.PickedQuantity,
# MAGIC     ROUND(SOL.Quantity * SOL.UnitPrice, 2) AS LineTotal,
# MAGIC     ROUND(SOL.Quantity * SOL.UnitPrice * (SOL.TaxRate / 100), 2) AS LineTax
# MAGIC FROM LH_GOLD_LAYER.dbo.Sales_OrderLines SOL
# MAGIC INNER JOIN LH_GOLD_LAYER.dbo.Sales_Orders SO
# MAGIC     ON SOL.OrderID = SO.OrderID

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_df = spark.table('temp_fact_orderlines')

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
