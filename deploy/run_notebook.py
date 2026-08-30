#!/usr/bin/env python3
"""
Triggers a run of NB_DEPLOY in Fabric via the Job Scheduler REST API and polls
until it completes. This is the ONLY thing azure-pipelines.yml does per environment --
NB_DEPLOY itself re-downloads the latest src/ and config/ from this repo and does
the actual deployment/update work.

Environment scoping does NOT use the Job Scheduler's `parameters` array -- live testing
against a real tenant proved Fabric accepts that payload (202, no error) but silently
never applies it to a RunNotebook job; the notebook always ran with its cell's hardcoded
default. Instead, this script bakes the target environment into the notebook's
"parameters"-tagged cell via getDefinition -> decode -> modify -> encode -> updateDefinition
before every run -- proven to work by the same live test.

Required env vars:
    FABRIC_CLIENT_ID, FABRIC_CLIENT_SECRET, FABRIC_TENANT_ID  -- service principal
    FABRIC_WORKSPACE_ID, FABRIC_NOTEBOOK_ID                   -- where NB_DEPLOY
                                                                  was imported (one-time
                                                                  manual bootstrap, see
                                                                  DEPLOYMENT.md)

Usage:
    python run_notebook.py --environment development
    python run_notebook.py --environment test
    python run_notebook.py --environment production
    python run_notebook.py                              # deploys all three environments
"""
import argparse
import base64
import json
import os
import re
import sys
import time

import requests
import msal

FABRIC_RESOURCE = "https://api.fabric.microsoft.com/.default"
API_BASE = "https://api.fabric.microsoft.com/v1"
POLL_SECONDS = 15
LRO_POLL_SECONDS = 5
TIMEOUT_SECONDS = 60 * 60  # first-ever run provisions whole workspaces; give it room
PARAM_CELL_TAG = "parameters"
PARAM_NAME = "target_environments_csv"


def get_token() -> str:
    app = msal.ConfidentialClientApplication(
        client_id=os.environ["FABRIC_CLIENT_ID"],
        client_credential=os.environ["FABRIC_CLIENT_SECRET"],
        authority=f"https://login.microsoftonline.com/{os.environ['FABRIC_TENANT_ID']}",
    )
    result = app.acquire_token_for_client(scopes=[FABRIC_RESOURCE])
    if "access_token" not in result:
        sys.exit(f"Failed to acquire Fabric token: {result.get('error_description', result)}")
    return result["access_token"]


def poll_lro(location: str, headers: dict, poll_seconds: int) -> dict:
    while True:
        time.sleep(poll_seconds)
        resp = requests.get(location, headers=headers)
        resp.raise_for_status()
        body = resp.json()
        status = body.get("status")
        if status == "Succeeded":
            return body
        if status == "Failed":
            sys.exit(f"Operation failed: {body}")


def set_environment_parameter(workspace_id: str, notebook_id: str, headers: dict, environment: str) -> None:
    """Bake `environment` into the notebook's parameters-tagged cell via getDefinition -> updateDefinition."""
    get_resp = requests.post(
        f"{API_BASE}/workspaces/{workspace_id}/notebooks/{notebook_id}/getDefinition?format=ipynb",
        headers=headers, json={},
    )
    if get_resp.status_code != 202:
        sys.exit(f"getDefinition failed: {get_resp.status_code} {get_resp.text}")
    lro = poll_lro(get_resp.headers["Location"], headers, LRO_POLL_SECONDS)
    result = requests.get(f"{get_resp.headers['Location']}/result", headers=headers)
    result.raise_for_status()
    parts = result.json()["definition"]["parts"]
    ipynb_part = next(p for p in parts if p["path"].endswith(".ipynb"))
    nb = json.loads(base64.b64decode(ipynb_part["payload"]))

    param_cell = next(
        c for c in nb["cells"]
        if c["cell_type"] == "code" and PARAM_CELL_TAG in c.get("metadata", {}).get("tags", [])
    )
    src = "".join(param_cell["source"])
    new_src = re.sub(
        rf'{PARAM_NAME}\s*=\s*"[^"]*"',
        f'{PARAM_NAME} = "{environment}"',
        src,
        count=1,
    )
    if new_src == src:
        sys.exit(f"Could not find `{PARAM_NAME} = \"...\"` in the parameters-tagged cell -- notebook structure changed?")
    param_cell["source"] = new_src.splitlines(keepends=True)

    new_payload = base64.b64encode(json.dumps(nb, indent=1).encode()).decode()
    update_resp = requests.post(
        f"{API_BASE}/workspaces/{workspace_id}/notebooks/{notebook_id}/updateDefinition",
        headers=headers,
        json={"definition": {"format": "ipynb", "parts": [
            {"path": "notebook-content.ipynb", "payload": new_payload, "payloadType": "InlineBase64"}
        ]}},
    )
    if update_resp.status_code != 202:
        sys.exit(f"updateDefinition failed: {update_resp.status_code} {update_resp.text}")
    poll_lro(update_resp.headers["Location"], headers, LRO_POLL_SECONDS)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--environment", default="",
        help="development | test | production (omit to deploy all three in one run)",
    )
    args = parser.parse_args()

    workspace_id = os.environ["FABRIC_WORKSPACE_ID"]
    notebook_id = os.environ["FABRIC_NOTEBOOK_ID"]
    headers = {"Authorization": f"Bearer {get_token()}", "Content-Type": "application/json"}

    print(f"Setting target_environments_csv = '{args.environment or 'ALL'}' on NB_DEPLOY")
    set_environment_parameter(workspace_id, notebook_id, headers, args.environment)

    print(f"Starting NB_DEPLOY for environment(s): '{args.environment or 'ALL'}'")
    resp = requests.post(
        f"{API_BASE}/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook",
        headers=headers,
    )
    if resp.status_code not in (200, 202):
        sys.exit(f"Failed to start notebook job: {resp.status_code} {resp.text}")

    status_url = resp.headers.get("Location")
    if not status_url:
        sys.exit(f"No Location header returned to poll job status: {dict(resp.headers)}")

    print(f"Job started. Polling {status_url}")
    start = time.time()
    while True:
        if time.time() - start > TIMEOUT_SECONDS:
            sys.exit("Timed out waiting for NB_DEPLOY to complete")
        time.sleep(POLL_SECONDS)
        poll = requests.get(status_url, headers=headers)
        poll.raise_for_status()
        status = poll.json().get("status")
        print(f"  status: {status}")
        if status == "Completed":
            print("NB_DEPLOY run completed successfully.")
            return
        if status in ("Failed", "Cancelled", "Deduped"):
            sys.exit(f"NB_DEPLOY run ended with status '{status}': {poll.text}")


if __name__ == "__main__":
    main()
