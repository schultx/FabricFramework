# Setting up a live test

What's already verified offline (no tenant access needed): every code cell in
`setup/NB_DEPLOY.ipynb` parses as valid Python, every pipeline/notebook JSON
under `src/` is well-formed, and every item referenced in `config/items.yaml`
has a matching folder under `src/`. That's the ceiling of what's checkable
without your tenant.

Everything past this point needs something only you can provide.

## 1. Fabric tenant prerequisites

- A Fabric capacity to assign workspaces to — a trial capacity is fine for a
  first test
- In the **Fabric Admin portal**, under Tenant settings:
  - "Users can create Fabric items"
  - "Create Workspaces"
  - If testing via service principal: "Service principals can create
    workspaces, connections, and deployment pipelines", "Service principals
    can call Fabric public APIs", and the two admin-API settings under
    Developer/Admin API settings

## 2. Give me a way to call the Fabric REST API

Pick one:

- **Recommended — no secret changes hands.** Run `az login` yourself,
  interactively, in a terminal on this machine, signed in with an account
  that has rights on the target capacity. Once you're logged in, I can
  request short-lived tokens via
  `az account get-access-token --resource https://api.fabric.microsoft.com`
  without ever seeing a password or long-lived secret.
- **Alternative — service principal.** Create one
  (`az ad sp create-for-rbac --name stratum-test`) with Contributor on the
  capacity, and share the client id / tenant id / client secret with me. I'd
  only hold it in this session's environment variables for the calls — never
  write it into the repo or any file.

## 3. Tell me the specifics

`config/environments.yaml` needs real values before a run means anything:

| Setting | Default | What I need from you |
|---|---|---|
| `capacity` (per environment) | `fabfabricmsa` | Your actual capacity name — can point all three environments at the same trial capacity for a first test |
| `workspace_roles` | `[]` (only the deploying principal gets access) | Entra object IDs to grant access to, or leave empty for a first test |
| `include_silver` | `false` for all three | Leave false unless you already have a Bronze entity that two or more Gold objects need to share |

If you'd rather not decide all of this up front: say "use the defaults for a
first test" and I'll run it exactly as committed, scoped to `development`
only.

## 4. What I'll actually do once I have a token

1. Call the Fabric REST API to create one bootstrap workspace and import
   `NB_DEPLOY.ipynb` into it as a Notebook item — directly via the API, no
   manual UI import needed.
2. Trigger a run scoped to `target_environments_csv=development` only, so a
   first test can't touch test/prod.
3. Poll the run, pull back logs, and — this is the actual point of doing
   this live — fix whatever breaks. The offline checks above catch syntax
   and reference errors, not Fabric API behavior, capacity throttling, or
   permission gaps, which only show up on a real run.
4. Verify directly rather than trusting job status alone: list the 3 new
   workspaces and their lakehouses, query the SQL Database for all 5
   metadata schemas, confirm the Code workspace has the expected
   notebook/pipeline set, and run `PL_RUN_ALL` to confirm `gold.dim_customer`
   / `gold.fact_signup` land with rows from the demo fixture.
5. Report back exactly what got created and what, if anything, failed.

## What I can't do without you

- Run `az login`'s interactive browser flow — that step is yours
- Invent capacity names or Entra object IDs that mean something to you
- Set up Azure DevOps (org/project/variable group/environments) without
  either ADO access from you or you following `DEPLOYMENT.md` yourself and
  telling me what broke
