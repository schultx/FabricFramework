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
# `config/environments.yaml`.
#
# Orchestrates the Silver-layer per-table notebooks, same shape as
# `NB_LOAD_GOLD`: Silver (if needed for custom logic) gets a unique
# hand-written notebook per table, %run-chained here rather than driven by a
# generic metadata loop.
#
# Currently wired to the demo table: `sil_customer`. Add each new Silver
# notebook here as the business domain grows.
#
# One `audit.NotebookRun` row per run, opened here and closed 'Succeeded' at
# the bottom -- same limitation as `NB_LOAD_GOLD`: a `%run` failure aborts the
# notebook before the closing cell runs, so a failed run leaves its row stuck
# at 'Running' rather than closed out 'Failed', though the job itself still
# reports failed to Fabric either way.

# CELL ********************

%run NB_KEYSTONE_FUNCTIONS

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

run_guid = start_notebook_run("NB_LOAD_SILVER")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run sil_customer

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
