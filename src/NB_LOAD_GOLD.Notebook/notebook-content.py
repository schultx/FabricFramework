# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # NB_LOAD_GOLD
# Orchestrates the Gold-layer dim/fact notebooks. Dimensions must run before the
# facts that reference them (`load_fact()` looks up each `_key` column against
# an already-built `gold.dim_*` table).
#
# Currently wired to the demo pair: `dim_customer` -> `fact_signup`. Add each
# new dimension notebook above its dependent fact notebook as the business
# domain grows, or drive this from `gold.GoldEntity` metadata once there are
# enough Gold objects to warrant a fully generic loop.

# CELL ********************

%run dim_customer

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run fact_signup

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
