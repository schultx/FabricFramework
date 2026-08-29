#!/usr/bin/env python3
"""
Triggers a run of NB_DEPLOY_ALL in Fabric via the Job Scheduler REST API and polls
until it completes. This is the ONLY thing azure-pipelines.yml does per environment --
NB_DEPLOY_ALL itself re-downloads the latest src/ and config/ from this repo and does
the actual deployment/update work.

Required env vars:
    FABRIC_CLIENT_ID, FABRIC_CLIENT_SECRET, FABRIC_TENANT_ID  -- service principal
    FABRIC_WORKSPACE_ID, FABRIC_NOTEBOOK_ID                   -- where NB_DEPLOY_ALL
                                                                  was imported (one-time
                                                                  manual bootstrap, see
                                                                  FMD_FRAMEWORK_DEPLOYMENT.md)

Usage:
    python run_notebook.py --environment development
    python run_notebook.py --environment test
    python run_notebook.py --environment production
    python run_notebook.py                              # deploys all three environments
"""
import argparse
import os
import sys
import time

import requests
import msal

FABRIC_RESOURCE = "https://api.fabric.microsoft.com/.default"
POLL_SECONDS = 15
TIMEOUT_SECONDS = 60 * 60  # first-ever run provisions whole workspaces; give it room


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

    body = {
        "executionData": {
            "parameters": {
                "target_environments_csv": {"value": args.environment, "type": "string"}
            }
        }
    }

    print(f"Starting NB_DEPLOY_ALL for environment(s): '{args.environment or 'ALL'}'")
    resp = requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}"
        f"/jobs/instances?jobType=RunNotebook",
        headers=headers,
        json=body,
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
            sys.exit("Timed out waiting for NB_DEPLOY_ALL to complete")
        time.sleep(POLL_SECONDS)
        poll = requests.get(status_url, headers=headers)
        poll.raise_for_status()
        status = poll.json().get("status")
        print(f"  status: {status}")
        if status == "Completed":
            print("NB_DEPLOY_ALL run completed successfully.")
            return
        if status in ("Failed", "Cancelled", "Deduped"):
            sys.exit(f"NB_DEPLOY_ALL run ended with status '{status}': {poll.text}")


if __name__ == "__main__":
    main()
