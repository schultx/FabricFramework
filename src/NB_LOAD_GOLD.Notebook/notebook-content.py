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

# # NB_LOAD_GOLD
# Orchestrates the Gold-layer dim/fact notebooks. Dimensions must run before the
# facts that reference them (`load_fact()` looks up each `_key` column against
# an already-built `gold.dim_*` table).
#
# Currently wired to the demo pair: `dim_customer` -> `fact_signup`. Add each
# new dimension notebook above its dependent fact notebook as the business
# domain grows -- Gold is deliberately NOT metadata-loop-driven: every object
# gets its own hand-written notebook, %run-chained here.
#
# One `audit.NotebookRun` row per run, opened here and closed 'Succeeded' at
# the bottom. Limitation worth knowing: `%run` failures abort the notebook
# outright (Fabric surfaces that as a failed job either way), so a failed run
# leaves its row stuck at 'Running' rather than closed out 'Failed' -- there's
# no multi-cell try/finally across `%run` magics. Still a real, if incomplete,
# signal: a 'Running' row well past this pipeline's usual duration means
# something broke.

# CELL ********************

%run NB_KEYSTONE_FUNCTIONS

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

run_guid = start_notebook_run("NB_LOAD_GOLD")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# CELL ********************

end_notebook_run(run_guid, "Succeeded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
