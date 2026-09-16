# New Client Onboarding

This is the runbook: the actual order a Columbus consultant follows to take this repo from "clone" to "three verified, client-specific environments," start to finish. It assumes no prior familiarity with the repo, but does assume you (or someone you're paired with) has Azure DevOps admin rights and Fabric tenant/capacity access for the client. Where a step depends on the client's own Azure admin, that's called out — those are the steps most likely to stall your timeline, not the Fabric work itself.

Every numbered phase below is meant to be followed in order. Don't skip ahead to step 9 (source Connections) before step 8 (the metadata Connection) — ingestion simply won't work yet.

## 1. Fork and clone the repo for this client

This repo's own `origin` is a public GitHub repo (`git remote -v` on a clone of this project shows `https://github.com/schultx/FabricFramework.git`), and the Azure DevOps pipeline (`azure-pipelines.yml`) is wired to trigger off pushes to that GitHub repo's `main` branch — not an Azure Repos git.

1. Fork `schultx/FabricFramework` on GitHub into wherever this client's copy will actually live (a Columbus-owned org, or the client's own, as agreed with them). The repo name doesn't matter — step 2 is where the framework's *display* name gets changed.
2. Clone your fork locally: `git clone https://github.com/<your-org>/<client-repo>.git`
3. Do your setup work on `main` (or a short branch merged to `main` quickly) — everything downstream deploys from `main` specifically.

> **Read this before you go further.** `NB_DEPLOY`'s "Download `src/` and `config/` from git" step does a plain, **unauthenticated** `requests.get()` against `https://github.com/{GITHUB_REPO}/archive/refs/heads/{GITHUB_BRANCH}.zip` (`setup/build_nb_deploy.py`, line 131-132) — not an authenticated API call, not an Azure Repos clone. That means this client's fork has to stay a **public** GitHub repo, or someone adds auth to that one cell before you go further. If this client can't have their pipeline/table/entity names sitting in a public repo, treat that as a blocker to resolve before step 6, not something to discover mid-bootstrap.

## 2. Rename the framework for this client

Be upfront with whoever's supervising this engagement: there's no config-driven client-name field today. Renaming means editing generator *code*, in `setup/build_nb_deploy.py`:

1. **Workspace-naming f-strings** (lines 407-409):
   ```python
   st["data_ws_name"] = f"Monza Data ({env['short']})"
   st["ingestion_ws_name"] = f"Monza Ingestion ({env['short']})"
   st["code_ws_name"] = f"Monza Code ({env['short']})"
   ```
   Replace `"Monza` with the client's chosen framework/engagement name. Search the file for `"Monza` to catch all three plus the docstring on line 29 (`Deploys the Monza framework: 3 workspaces per environment...`).
2. **The git source the deployed notebook actually downloads from** (lines 65-66):
   ```python
   GITHUB_REPO = "schultx/FabricFramework"
   GITHUB_BRANCH = "main"
   ```
   Point `GITHUB_REPO` at your fork from step 1 (`"<your-org>/<client-repo>"`). This is not optional — skip it and `NB_DEPLOY` will keep pulling this repo's original content instead of your client's.
3. **Regenerate the actual deployed notebook.** Editing `build_nb_deploy.py` alone does nothing — `setup/NB_DEPLOY.ipynb` is a generated artifact:
   ```
   python setup/build_nb_deploy.py
   ```
   Commit both files together.
4. Update client-facing branding in `README.md` and this wiki's own `Home.md` so nothing a client-facing person reads still says "Monza."

Note what this rename step *doesn't* touch: item names like `VAR_MONZA` and `NB_MONZA_FUNCTIONS` inside `config/items.yaml`/`src/` keep the literal `MONZA` in their names for every client — renaming those would mean touching every notebook's `%run NB_MONZA_FUNCTIONS` reference too, and isn't part of this checklist.

This whole step is flagged as the **#1 item on [Improvement-Roadmap](Improvement-Roadmap.md)** — a `framework_name`/`client_name` field in `config/environments.yaml`, threaded through every naming f-string, would turn this into a one-line config edit instead of a code edit. Until that's built, this is the real process.

## 3. Decide topology for this client

- **Environments.** `config/environments.yaml`'s `environments:` list is what `NB_DEPLOY` iterates — one entry becomes 3 workspaces (`<Name> Data/Ingestion/Code (<short>)`). The repo ships `development`/`test`/`production`, but nothing requires all three. For a smaller engagement with no formal test gate, drop the `test` entry — and its `Test` stage in `azure-pipelines.yml`, and its `fabricframework-test` ADO Environment. Every environment needs a unique `short` and its own `capacity`.
- **Silver.** Leave `include_silver: false` for a fresh client. The `environments.yaml` header comment says this explicitly — the `true` set on all three environments in this committed repo is *this project's own* deliberate, permanent demo of the mechanism, not a default to copy. Only flip it on for a real environment once an actual Bronze entity is genuinely reused by 2+ Gold objects. See [Architecture](Architecture.md) and [Metadata-Model](Metadata-Model.md) for what that buys you and what it costs (a 4th lakehouse, a hand-written Silver notebook per reused entity).

Write these two decisions down before step 4 — they determine how many workspaces, capacities, and service connections the rest of this checklist creates.

## 4. Azure/Fabric prerequisites

- **Capacity, sized to this client's expected *parallel* ingest workload**, not just data volume. `PL_RUN_ALL` fans all `EP_INGEST_<TYPE>` stages out in parallel (8 of them, one per connector type) — on a small F2-class capacity, that many concurrent ingest pipelines starved and, in this project's own testing, failed outright with `TooManyRequestsForCapacity` (HTTP 430). See [Operations-Guide](Operations-Guide.md) for the full story; today's only mitigation is fewer simultaneous source connections or a bigger SKU — there's no fan-out throttle config yet.
- Get the capacity's **exact display name** from the client's Fabric Admin portal and put it in `config/environments.yaml`'s `capacity:` field for each environment, e.g.:
  ```yaml
  capacity: Trial-20260916T102156Z-SXyTKCCwy06fHGjMeQxDSA
  ```
  This has to match exactly — `NB_DEPLOY` resolves capacity by name with zero tolerance for it later being renamed, paused, or replaced, and that failure mode surfaces as an opaque generic error (see [Operations-Guide](Operations-Guide.md)).
- Confirm with the client's Fabric admin that these tenant settings are on: **Users can create Fabric items**, **Create workspaces**, and — since the deploy runs as a service principal — **Service principals can create workspaces, connections, and deployment pipelines**, **Service principals can call Fabric public APIs**, plus the matching Developer/Admin API settings.
- `workspace_roles` in the same file (`data`/`ingestion`/`code` lists) is where you'll grant the client's Entra users/groups access per workspace tier once you know who they are — leave it empty for now and come back to it before handoff.

## 5. Set up per-environment Azure DevOps service connections

Today's committed pipeline shares **one** identity across every stage: a single variable group, `fabric-framework-secrets` (holding `FABRIC_CLIENT_ID`/`FABRIC_CLIENT_SECRET`/`FABRIC_TENANT_ID`), referenced by all three stages in `azure-pipelines.yml`. For a real client engagement, don't carry that shape forward as-is — set up one service principal per environment, each scoped to Contributor on only that environment's capacity/workspaces, and wire each as its own workload-identity-federated (WIF) Azure DevOps service connection. `fabricframework-<env>-wif` is the naming convention already used in this project's own working notes. One per environment is what actually gives you blast-radius isolation: a compromised or misconfigured dev credential can't touch production.

Exact click-by-click steps — app registration, federated credential, ADO service connection, variable group wiring — are in [Deployment-Guide](Deployment-Guide.md). Follow that once per environment. You'll still need two more values per environment before the variable group is complete: `FABRIC_WORKSPACE_ID` and `FABRIC_NOTEBOOK_ID`, which come out of step 6, not this step.

## 6. Bootstrap: the one manual run

Azure DevOps needs something already inside Fabric to call, so the very first run happens by hand:

1. Create a workspace to stage the deploy notebook itself (e.g. `<Client> Deploy`) — any Fabric workspace; Contributor role is enough for you personally at this point.
2. Download the regenerated `setup/NB_DEPLOY.ipynb` from your fork (step 2) and import it into that workspace (Fabric UI → **Import notebook**).
3. Open the notebook's parameters-tagged cell and set `target_environments_csv = "development"` before running. This deliberately differs from a from-scratch, all-three-environments bootstrap: for a new client, scope the very first live run to development only, so any surprise — a bad capacity name, a missed tenant setting, a slip in your rename edits — surfaces before it can touch test or production. Leaving the parameter blank provisions all three environments in one run.
4. **Run all.**
5. Note the workspace ID and the notebook's own item ID (Fabric UI → notebook → **Settings**, or `GET /v1/workspaces/{workspaceId}/items`) — these are `FABRIC_WORKSPACE_ID` and `FABRIC_NOTEBOOK_ID` for the development variable group in step 5.
6. Push your renamed repo to `main` and finish wiring `azure-pipelines.yml` (variable group, ADO Environments, pipeline creation) per [Deployment-Guide](Deployment-Guide.md), so every deploy from here on is `git push` → pipeline, not another manual notebook import.

## 7. Verify the development deploy

Don't trust job status alone — check the actual artifacts:

- Three workspaces exist: `<Client> Data (D)`, `<Client> Ingestion (D)`, `<Client> Code (D)`.
- Data has `Landing`, `Bronze`, `Gold` lakehouses (plus `Silver` only if you set `include_silver: true` for development).
- Ingestion has the `SQL_METADATA_DATABASE` SQL Database, and its schema is applied — connect to it and confirm all **4 schemas** exist: `ingestion`, `ai`, `runtime`, `audit`.
- Code has the expected notebook/pipeline set from `config/items.yaml`: `NB_MONZA_FUNCTIONS`, `NB_RUN_REMOTE_PIPELINE`, `NB_LOAD_BRONZE`, `NB_LOAD_GOLD`, `dim_customer`, `fact_signup`, `PL_LOAD_BRONZE`, `PL_LOAD_GOLD`, `PL_RUN_ALL` — plus `NB_LOAD_SILVER`/`sil_customer`/`PL_LOAD_SILVER` if `include_silver: true`.
- Ingestion also has all 8 `PL_INGEST_<TYPE>` pipelines (`SQL`, `FILE`, `SQLMI`, `ORACLE`, `SFTP`, `FTP`, `ONELAKETABLE`, `ONELAKEFILE`) under its `Pipelines/` folder. See [Architecture](Architecture.md) for the full item-to-workspace map.
- If anything's missing or half-created, just re-run `NB_DEPLOY` — every step is create-if-missing/update-if-exists, so a retry picks up wherever the last attempt left off (this is also the standard fix for a transient Azure SQL "database warming up" failure on a brand-new `SQL_METADATA_DATABASE`).

## 8. Register the metadata Connection

Ingestion Lookups won't resolve without this. It's a one-time manual step per environment, done after step 6/7 because it needs `SQL_METADATA_DATABASE` and the Ingestion workspace to already exist.

1. In the Fabric portal, open `<Client> Ingestion (D)` and register a new Connection to `SQL_METADATA_DATABASE` (**New item → Connection**, or **Manage connections and gateways**).
2. Credential it with a **service principal**, not Workspace Identity — Workspace Identity hits a confirmed, unfixable Fabric platform bug (`InvalidToken`/`Unauthorized`) on this exact `Lookup` + `FabricSqlDatabaseSource` combination.
3. Copy the new Connection's GUID into `metadata_connection_guid` for `development` in `config/environments.yaml`, push to `main`, and let the pipeline redeploy (or re-run `NB_DEPLOY` by hand).

Full walkthrough: [Deployment-Guide](Deployment-Guide.md). Until this is filled in, `NB_DEPLOY` still deploys every `PL_INGEST_*` pipeline and only prints a warning — but every ingestion `Lookup` fails at runtime until the GUID is real.

## 9. Register this client's actual source Connections

Everything from here is metadata rows, not code: `ingestion.Connection` → `ingestion.Database` → `ingestion.Table`, one active `Table` row per source table, drives that table's entire Source → Landing → Bronze flow. See [Metadata-Model](Metadata-Model.md) for the full hierarchy and what each column means.

For each of this client's real source systems, pick the matching `ConnectionType` (`Sql`, `File`, `SqlMI`, `Oracle`, `Sftp`, `Ftp`, `OneLakeTable`, `OneLakeFile`, or `Custom` for anything with no dedicated connector — REST APIs, SharePoint, Dataverse, Salesforce) and follow [Connector-Types](Connector-Types.md) for the exact Connection/Database/Table rows that type needs. Do this for at least one real table before step 10, so the first end-to-end run proves something the client actually cares about, not just the seeded demo fixture.

## 10. Run `PL_RUN_ALL` end to end for development

Trigger `PL_RUN_ALL` in `<Client> Code (D)` and watch it clear all 8 `EP_INGEST_*` stages, then `EP_LOAD_BRONZE`, then (if `include_silver`) `EP_LOAD_SILVER`, then `EP_LOAD_GOLD`.

Verify real data actually landed — see [Operations-Guide](Operations-Guide.md) for the full verification playbook, but the one thing worth knowing before you start: **a freshly created lakehouse's SQL analytics endpoint has a real sync lag after its first write.** In this project's own testing, development's endpoint normalized within about 30 seconds; test's and production's hadn't caught up even several minutes later. If a SQL-endpoint query comes back empty or short right after a run, that's very likely lag, not a failure — re-query after a minute, or read the OneLake Delta files directly for a trustworthy answer sooner.

## 11. Repeat for test, then production

Same cycle, once development is verified: step 5 (that environment's service connection) → step 6 (redeploy scoped to `target_environments_csv = "test"`, this time through the pipeline's approval-gated stage, not a manual notebook run) → step 7 (verify) → step 8 (metadata Connection + GUID) → step 9 (source Connections, if they differ per environment) → step 10 (`PL_RUN_ALL`, watching for the same sync-lag caveat).

`azure-pipelines.yml`'s `Test` and `Prod` stages sit behind the `fabricframework-test`/`fabricframework-prod` ADO Environments' required-reviewer approval gates — get sign-off from whoever owns that gate before promoting. From here on, every future change to this client's platform is `git push` to `main`; there's no second manual bootstrap.

## 12. Handoff to ongoing operations

Once all three environments are live and verified, this checklist is done. Day-to-day running, monitoring, and troubleshooting live in [Operations-Guide](Operations-Guide.md) — walk the client (or whoever inherits this) through that page before you close out the engagement: how to re-run `PL_RUN_ALL`, read `audit.PipelineRun`/`audit.NotebookRun`, and the known failure modes already catalogued there.

## Time budget

Qualitative, not precise — grounded in how long the equivalent work actually took while this framework was being built and live-tested, but a real client engagement will vary with how much of the client's own environment (Azure AD, network, gateways) is already in place.

| Phase | Rough budget |
|---|---|
| 1. Fork/clone | A few minutes |
| 2. Rename | 15-30 minutes the first time you do it carefully; faster once you've done it once |
| 3. Topology decision | A short internal/client conversation — could be 15 minutes, could stall for days waiting on an answer |
| 4. Azure/Fabric prerequisites | Capacity sizing is your call in minutes; tenant-setting confirmation depends on the client's Azure/Fabric admin — can be same-day or take a day or more to get someone to flip the settings |
| 5. WIF service connections | 30-60 minutes for three if you already have ADO admin and have done WIF federated credentials before; meaningfully longer the first time you work out the federated-credential subject/issuer values in a new tenant |
| 6. Bootstrap | The manual clicking itself is minutes; the run underneath it took, in this project's own testing, several minutes to tens of minutes for a single from-scratch environment, and a cold `SQL_METADATA_DATABASE` warming up (a known, repeating Azure SQL "retry" condition) is normal, not a bad sign — budget up to ~30 minutes including one or two automatic retries |
| 7. Verify dev | 10-15 minutes of portal clicking plus one SQL query |
| 8. Metadata Connection | 10-15 minutes in the portal, assuming you already have Contributor there |
| 9. Source Connections | Entirely dependent on how many real source systems this client has — minutes for a handful of `Sql`/`File` tables you already have credentials for, real days for anything involving an on-premises gateway (`Oracle`) or a hand-written `Custom` notebook |
| 10. `PL_RUN_ALL` dev + verify | Minutes to run; give the sync-lag window a minute before treating an empty SQL-endpoint result as a failure |
| 11. Test + production | Each environment's steps 5-10 should go faster than dev did, the second and third time through. As a rough ceiling: deploying test and production together in one heavier run (extra items, not the steady state) took on the order of 20 minutes of automated retries alone in this project's own build-out — plus whatever calendar time the approval gates actually sit waiting on a human reviewer |
| 12. Handoff | As long as the conversation needs — this is a people step, not a technical one |
