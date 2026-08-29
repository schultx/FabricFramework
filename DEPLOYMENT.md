# Deployment: one notebook, triggered from Azure DevOps

This repo deploys itself. There is exactly one notebook to run —
[`setup/NB_DEPLOY_ALL.ipynb`](setup/NB_DEPLOY_ALL.ipynb) — and exactly one pipeline that
runs it — [`azure-pipelines.yml`](azure-pipelines.yml). Everything else (workspaces,
lakehouses, the metadata SQL database, ingestion pipelines, Gold-layer notebooks) is
created or updated by that notebook, from whatever is currently on the `main` branch of
this repo.

## Why one notebook

Upstream FMD ships this as two separate notebooks — one for the core framework, one for
business domains — each manually imported and run in sequence. `NB_DEPLOY_ALL.ipynb`
merges both into one: same logic, same deployment functions, copied over cell-for-cell
(not retyped, to avoid transcription errors in production automation), so there is only
one thing to import and only one thing to trigger.

It works by pulling this repo's `src/` and `config/` folders from GitHub
(`repo_owner`/`repo_name`/`branch` near the top of the notebook — currently
`schultx/FabricFramework` / `main`) and walking through them with the Fabric CLI and
REST API to create or update every workspace, lakehouse, SQL database object, notebook,
and pipeline they describe. Re-running it after a push to `main` is how you redeploy —
there's no separate "sync" step.

## One-time manual bootstrap

The very first run has to happen by hand, because Azure DevOps needs something already
inside Fabric to call:

1. Create a workspace (e.g. `FMD_FRAMEWORK_CONFIGURATION`) — any workspace, contributor
   role is enough for you personally at this point.
2. Download `setup/NB_DEPLOY_ALL.ipynb` from this repo and import it into that workspace
   (Fabric UI → **Import notebook**).
3. Open it and fill in the configuration cells — capacity names, workspace roles
   (Entra object IDs), and anything under **KeyVault settings** if you're using one. The
   repo pointer cell under **Repo Configuration** should already read
   `repo_owner = "schultx"`, `repo_name = "FabricFramework"` — leave it unless you're
   deploying from a fork or a different branch.
4. **Run all.** This provisions every environment's workspaces (`development`, `test`,
   `production`, per `environments` / `business_domain_deployment`), the metadata SQL
   database, the core ingestion pipelines, and the Gold-layer business-domain items —
   including `gold.GoldEntity` and `ai.FeatureSet`, the two tables the SQL deployment
   step adds on top of upstream FMD's own schema.
5. Note the workspace ID and this notebook's item ID (Fabric UI → notebook →
   **Settings**, or `GET /v1/workspaces/{workspaceId}/items`) — these become
   `FABRIC_WORKSPACE_ID` and `FABRIC_NOTEBOOK_ID` below.

Everything after this step is driven from Azure DevOps.

## Wiring up Azure DevOps

`azure-pipelines.yml` doesn't call `fabric-cicd` or touch item definitions directly — its
only job, per environment, is to start a run of the notebook you just imported and wait
for it to finish. The actual deployment logic stays entirely inside the notebook (and
therefore entirely inside this repo, versioned like everything else).

1. **Variable group** `fabric-framework-secrets` (Pipelines → Library), holding:
   - `FABRIC_CLIENT_ID`, `FABRIC_CLIENT_SECRET`, `FABRIC_TENANT_ID` — a service principal
     with Contributor on the target workspace(s)
   - `FABRIC_WORKSPACE_ID`, `FABRIC_NOTEBOOK_ID` — from the bootstrap step above

2. **ADO Environments** (Pipelines → Environments): `fabricframework-dev`,
   `fabricframework-test`, `fabricframework-prod`. Add a required reviewer on `-test`
   and `-prod` — that's the approval gate; `-dev` needs none.

3. Push to `main`. The pipeline runs three stages in order — Dev, Test, Prod — each
   calling [`deploy/run_notebook.py`](deploy/run_notebook.py) with
   `--environment development|test|production`. That script starts the notebook via the
   Fabric [Job Scheduler API](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler)
   (`POST .../jobs/instances?jobType=RunNotebook`) passing `target_environments_csv` as a
   notebook parameter, then polls until the run completes or fails.

`target_environments_csv` is why one notebook can still support staged promotion: the
parameters cell near the top of `NB_DEPLOY_ALL.ipynb` filters `environments` and
`business_domain_deployment` down to just the environment named in that parameter before
anything gets created. Run it with no parameter (as the manual bootstrap does) and it
deploys all three at once; run it from the pipeline with one environment at a time and
each stage only touches its own workspaces.

## Redeploying

Change anything under `src/` or `config/`, push to `main`, and the pipeline does the
rest — Dev automatically, Test and Prod after their approvals. No re-import, no second
notebook.
