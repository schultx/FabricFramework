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

# # NB_LOAD_GOLD
# Orchestrates the Gold-layer dim/fact notebooks. Dimensions must run before the facts
# that reference them (`load_fact()` looks up each `_key` column against an already-built
# `gold.dim_*` table).
#
# Currently wired to the temp/demo pair built on `NB_GOLD_LOADER_FUNCTIONS`:
# `dim_customer` → `fact_orderlines`. Add each new dimension notebook above its
# dependent fact notebook as the business domain grows.

# CELL ********************

%run dim_customer

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run fact_orderlines

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
