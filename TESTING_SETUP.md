# Setting up a live test

What's already verified offline (no tenant access needed): every code cell in
`setup/NB_DEPLOY_ALL.ipynb` parses as valid Python, `azure-pipelines.yml` is valid YAML,
every item referenced in `config/item_deployment*.json` has a matching folder under
`src/`, there are no ID collisions between the core and business-domain deployment
configs, and this machine can reach `login.microsoftonline.com`, `api.fabric.microsoft.com`,
and `dev.azure.com`. That's the ceiling of what's checkable without your tenant.

Everything past this point needs something only you can provide.

## 1. Fabric tenant prerequisites

Same as upstream FMD's own requirements (from `FMD_BUSINESS_DOMAIN_DEPLOYMENT.md`):

- A Fabric capacity to assign workspaces to — a trial capacity is fine for a first test
- In the **Fabric Admin portal**, under Tenant settings:
  - "Users can create Fabric items"
  - "Create Workspaces"
  - If testing via service principal: "Service principals can create workspaces,
    connections, and deployment pipelines", "Service principals can call Fabric public
    APIs", and the two admin-API settings under Developer/Admin API settings

## 2. Give me a way to call the Fabric REST API

Pick one:

- **Recommended — no secret changes hands.** Run `az login` yourself, interactively, in
  a terminal on this machine, signed in with an account that has rights on the target
  capacity. Once you're logged in, I can request short-lived tokens via
  `az account get-access-token --resource https://api.fabric.microsoft.com` without ever
  seeing a password or long-lived secret.
- **Alternative — service principal.** Create one
  (`az ad sp create-for-rbac --name fabricframework-test`) with Contributor on the
  capacity, and share the client id / tenant id / client secret with me. I'd only hold it
  in this session's environment variables for the Bash calls — never write it into the
  repo or any file.

## 3. Tell me the specifics for the config cells

`NB_DEPLOY_ALL.ipynb` needs real values before it can run — these are placeholders right
now:

| Setting | Default in the notebook | What I need from you |
|---|---|---|
| `capacity_name_dvlm` / `_prod` / `_test` / `_config` | `'Trial-Erwin'` | Your actual capacity name(s) — can all point at the same trial capacity for a first test |
| `workspace_roles_*` (Entra object IDs) | a placeholder group ID | Your own user object ID, or say "just me" and I'll resolve it from the signed-in principal |
| `domain_name` | `'INTEGRATION'` | Keep it, or tell me what you want |
| `business_domain_names` | `['FINANCE','SALES']` | Keep the FMD demo defaults, or your own domain names |
| `key_vault_uri_name` | blank | Leave blank for a first test unless you're already using a Key Vault |

If you'd rather not decide all of this up front: say "use the defaults for a first test"
and I'll run it exactly as the FMD demo ships, scoped to `development` only.

## 4. What I'll actually do once I have a token

1. Call the Fabric REST API to create one bootstrap workspace and import
   `NB_DEPLOY_ALL.ipynb` into it as a Notebook item — directly via the API, no manual UI
   import needed.
2. Trigger a run via the Job Scheduler API with `target_environments_csv=development`
   only, so a first test can't touch test/prod.
3. Poll the run, pull back logs, and — this is the actual point of doing this live —
   fix whatever breaks. The offline checks above catch syntax and reference errors, not
   Fabric API behavior, capacity throttling, or permission gaps, which only show up on
   a real run.
4. Report back exactly what got created (workspace names, item counts, the SQL Database)
   and what, if anything, failed.

## What I can't do without you

- Run `az login`'s interactive browser flow — that step is yours
- Invent capacity names, Entra object IDs, or a business domain that means something to you
- Set up Azure DevOps (org/project/variable group/environments) without either ADO access
  from you or you following `DEPLOYMENT.md` yourself and telling me what broke
