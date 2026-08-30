# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # NB_RUN_REMOTE_PIPELINE
# Bridge so `PL_RUN_ALL` (Code) can trigger and wait on a Data Pipeline that
# lives in a *different* workspace -- `PL_INGEST_SQL` / `PL_INGEST_FILE`, both
# in Ingestion. The legacy `ExecutePipeline` activity only supports a pipeline
# in the SAME workspace as the caller
# (https://learn.microsoft.com/en-us/fabric/data-factory/invoke-pipeline-activity).
# Cross-workspace needs either a newer preview activity (a Connection object,
# Workspace-Identity/service-principal auth, a tenant setting not enabled
# everywhere, and explicit cross-workspace permission grants) or triggering
# the target item's job directly via the Job Scheduler REST API -- this
# notebook does the latter, using its own AAD token, exactly like every other
# cross-workspace REST call in this repo already does (no Connection object,
# no WorkspaceIdentity -- this framework deliberately avoids that whole
# mechanism after finding it unreliable on Lookup activities earlier).
#
# `PL_RUN_ALL` calls this via a same-workspace `TridentNotebook` activity
# (trivial, since this notebook lives in Code alongside it), passing the
# target pipeline's workspace/item id as parameters. This notebook then does
# the actual cross-workspace trigger-and-poll -- `POST .../jobs/instances?
# jobType=Pipeline`, then poll the returned `Location` until the job reaches
# a terminal status -- which preserves the same "wait for the child pipeline
# to finish before continuing" semantics the old `ExecutePipeline` /
# `waitOnCompletion: true` gave, since the outer `TridentNotebook` activity
# (like all pipeline activities) is itself synchronous.

# CELL ********************

%run NB_KEYSTONE_FUNCTIONS

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Parameters
target_workspace_id = ""   # workspace id of the pipeline to trigger -- required
target_item_id = ""        # item id of the pipeline to trigger -- required

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time

if not target_workspace_id:
    raise ValueError("target_workspace_id parameter is required")
if not target_item_id:
    raise ValueError("target_item_id parameter is required")

POLL_SECONDS = 15  # fallback if the job start response carries no Retry-After header
# A real ingestion pipeline copy could legitimately run long -- generous but
# finite rather than looping forever.
TIMEOUT_SECONDS = 60 * 60 * 6

resp = requests.post(
    f"{FABRIC_API}/workspaces/{target_workspace_id}/items/{target_item_id}/jobs/instances?jobType=Pipeline",
    headers=_fabric_headers(),
)
if resp.status_code not in (200, 202):
    raise RuntimeError(f"Failed to start pipeline job: {resp.status_code} {resp.text}")

status_url = resp.headers.get("Location")
if not status_url:
    raise RuntimeError(f"No Location header returned to poll job status: {dict(resp.headers)}")
retry_after = int(resp.headers.get("Retry-After", str(POLL_SECONDS)))

print(f"Job started. Polling {status_url}")
start = time.time()
status, body = None, None
while True:
    if time.time() - start > TIMEOUT_SECONDS:
        raise RuntimeError(
            f"Timed out after {TIMEOUT_SECONDS} seconds waiting for pipeline job "
            f"(item {target_item_id} in workspace {target_workspace_id}) to reach a terminal status"
        )
    time.sleep(retry_after)
    poll = requests.get(status_url, headers=_fabric_headers())
    poll.raise_for_status()
    body = poll.json()
    status = body.get("status")
    print(f"  status: {status}")
    if status in ("Completed", "Failed", "Cancelled", "Deduped"):
        break

if status != "Completed":
    raise RuntimeError(f"Remote pipeline job ended with status '{status}': {body}")

print("Remote pipeline job completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
