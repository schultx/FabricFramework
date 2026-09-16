import hashlib, itertools, json

# Cell ids are derived deterministically (call order + source text) rather than uuid4() --
# CI (Improvement-Roadmap.md, CI/CD #2) regenerates this notebook and diffs it against what's
# committed to catch drift between build_nb_deploy.py and NB_DEPLOY.ipynb. A random id would
# make every regeneration differ from the committed file even with zero logical change, so
# that check would fail on every single run regardless of drift.
_cell_seq = itertools.count()


def _deterministic_id(src: str) -> str:
    return hashlib.sha1(f"{next(_cell_seq)}:{src}".encode()).hexdigest()[:8]


def code(src, tags=None):
    return {
        "cell_type": "code",
        "id": _deterministic_id(src),
        "execution_count": None,
        "outputs": [],
        "metadata": {
            "microsoft": {"language": "python", "language_group": "jupyter_python"},
            **({"tags": tags} if tags else {}),
        },
        "source": src.splitlines(keepends=True),
    }

def md(src):
    return {
        "cell_type": "markdown",
        "id": _deterministic_id(src),
        "metadata": {},
        "source": src.splitlines(keepends=True),
    }

cells = []

cells.append(md(
"""# NB_DEPLOY

Deploys the Monza framework: 3 workspaces per environment (**Data** /
**Ingestion** / **Code**), the Landing/Bronze/Gold (+Silver if configured)
lakehouses, the metadata catalog SQL Database, the two ingestion pipelines
(in Ingestion) and every other item (in Code) -- all downloaded fresh from
`schultx/FabricFramework@main` on every run.

Pure `requests` + `notebookutils` throughout -- **no `ms-fabric-cli`, no
`sempy`**. (`%pip install ms-fabric-cli` was found to silently break
`sempy.fabric`'s context provider for the rest of the Spark session; avoiding
both dependencies sidesteps the bug at the root instead of working around it.)

Safe to re-run: every step is idempotent (create-if-missing / overwrite
content on existing items)."""
))

cells.append(code(
"""# Parameters
# deploy/run_notebook.py sets this by editing this cell's default via
# updateDefinition before every run (Fabric's Job Scheduler silently ignores
# job-level `parameters` for RunNotebook jobs -- confirmed live, no error).
target_environments_csv = ""   # "" deploys all three (development, test, production)
target_environments = [e.strip() for e in target_environments_csv.split(",") if e.strip()]
""", tags=["parameters"]))

cells.append(code(
"""import base64
import io
import json
import struct
import time
import traceback
import zipfile

import requests
import yaml

FABRIC_API = "https://api.fabric.microsoft.com/v1"
GITHUB_REPO = "schultx/FabricFramework"
GITHUB_BRANCH = "main"


def _token(resource: str = "pbi") -> str:
    return notebookutils.credentials.getToken(resource)


def fabric_headers() -> dict:
    return {"Authorization": f"Bearer {_token('pbi')}", "Content-Type": "application/json"}


def storage_headers() -> dict:
    return {"Authorization": f"Bearer {_token('storage')}"}


REQUEST_TIMEOUT = 60  # seconds -- a hung request with no timeout blocks the whole
                      # notebook session indefinitely; Fabric then kills the entire
                      # session ("System cancelled ... statement execution failures")
                      # rather than surfacing a catchable error. Fail fast and retry instead.
MAX_RETRIES = 3


def api(method: str, path: str, **kwargs) -> requests.Response:
    kwargs.setdefault("timeout", REQUEST_TIMEOUT)
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.request(method, f"{FABRIC_API}{path}", headers=fabric_headers(), **kwargs)
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            print(f"    {method} {path} attempt {attempt}/{MAX_RETRIES} raised {exc!r}, retrying")
            time.sleep(5 * attempt)
            continue
        if (resp.status_code >= 500 or resp.status_code == 429) and attempt < MAX_RETRIES:
            retry_after = resp.headers.get("Retry-After")
            if retry_after is not None:
                try:
                    delay = float(retry_after)
                except ValueError:
                    delay = 5 * attempt
            else:
                delay = 5 * attempt
            print(f"    {method} {path} attempt {attempt}/{MAX_RETRIES} -> {resp.status_code}, retrying in {delay}s")
            time.sleep(delay)
            continue
        if resp.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {resp.status_code}: {resp.text[:1000]}")
        return resp
    raise RuntimeError(f"{method} {path} failed after {MAX_RETRIES} attempts: {last_exc!r}")


POLL_LRO_MAX_SECONDS = 2700  # 45 min -- comfortably under the outer CI job's ~1-hour timeout, so a
                             # genuinely stuck operation raises a clear, named error here instead of
                             # being indistinguishable from a live one until that outer timeout kills
                             # the whole job with no signal.


def poll_lro(resp: requests.Response) -> requests.Response:
    \"\"\"Poll a long-running Fabric operation (202 + Location header) to completion.\"\"\"
    if resp.status_code != 202:
        return resp
    location = resp.headers["Location"]
    retry_after = int(resp.headers.get("Retry-After", "5"))
    started = time.monotonic()
    poll_count = 0
    while True:
        elapsed = time.monotonic() - started
        if elapsed > POLL_LRO_MAX_SECONDS:
            raise TimeoutError(
                f"poll_lro: operation at {location} did not reach a terminal status within "
                f"{POLL_LRO_MAX_SECONDS}s ({poll_count} polls) -- aborting instead of looping forever."
            )
        time.sleep(retry_after)
        poll_count += 1
        poll = requests.get(location, headers=fabric_headers(), timeout=REQUEST_TIMEOUT)
        if poll.status_code == 200:
            body = poll.json()
            print(f"    poll_lro: attempt {poll_count} ({elapsed:.0f}s elapsed) -> {body.get('status')}")
            if body.get("status") in ("Succeeded", "Completed"):
                return poll
            if body.get("status") == "Failed":
                raise RuntimeError(f"Long-running operation failed: {body}")
        elif poll.status_code not in (200, 202):
            poll.raise_for_status()
        else:
            print(f"    poll_lro: attempt {poll_count} ({elapsed:.0f}s elapsed) -> HTTP {poll.status_code} (pending)")
""".rstrip() + "\n"))

cells.append(md("## Download `src/` and `config/` from git\n\nSame branch/ref every environment deploys from -- no local drift between dev/test/prod."))

cells.append(code(
"""zip_url = f"https://github.com/{GITHUB_REPO}/archive/refs/heads/{GITHUB_BRANCH}.zip"
zip_bytes = requests.get(zip_url, timeout=REQUEST_TIMEOUT).content
archive = zipfile.ZipFile(io.BytesIO(zip_bytes))
root_prefix = archive.namelist()[0]  # "FabricFramework-main/"


def read_repo_file(relative_path: str) -> bytes:
    return archive.read(f"{root_prefix}{relative_path}")


def read_repo_text(relative_path: str) -> str:
    return read_repo_file(relative_path).decode("utf-8")


environments_cfg = yaml.safe_load(read_repo_text("config/environments.yaml"))
lakehouses_cfg = yaml.safe_load(read_repo_text("config/lakehouses.yaml"))
items_cfg = yaml.safe_load(read_repo_text("config/items.yaml"))
metadata_sql = read_repo_text("config/metadata_schema.sql")

# Known item.yaml `type` values -- one deploy branch exists for each (Phase 5/6/7 below).
# An unrecognized value (a typo like "Notebok", wrong casing) would otherwise match none
# of those branches and be silently dropped from the deploy with no error -- catch it here,
# before any Fabric API call, instead of discovering it after "Deployment complete." prints.
KNOWN_ITEM_TYPES = {"Notebook", "DataPipeline", "VariableLibrary"}


def _validate_config(environments_cfg: dict, lakehouses_cfg: dict, items_cfg: dict) -> None:
    \"\"\"Fail fast on a malformed config/*.yaml with a clear, config-path-specific error --
    before any Fabric API call runs, so a missing/mistyped key can't throw a bare KeyError
    mid-deploy after earlier environments in the same run have already been mutated.\"\"\"
    errors = []

    envs = environments_cfg.get("environments") or []
    if not envs:
        errors.append("config/environments.yaml: missing or empty top-level 'environments' list")
    for i, env in enumerate(envs):
        for key in ("name", "short", "capacity", "include_silver"):
            if key not in env:
                errors.append(f"config/environments.yaml: environments[{i}] (name={env.get('name', '?')!r}) missing required key '{key}'")

    lhs = lakehouses_cfg.get("lakehouses") or []
    if not lhs:
        errors.append("config/lakehouses.yaml: missing or empty top-level 'lakehouses' list")
    for i, lh in enumerate(lhs):
        for key in ("name", "always"):
            if key not in lh:
                errors.append(f"config/lakehouses.yaml: lakehouses[{i}] missing required key '{key}'")

    its = items_cfg.get("items") or []
    if not its:
        errors.append("config/items.yaml: missing or empty top-level 'items' list")
    for i, item in enumerate(its):
        for key in ("name", "type", "path"):
            if key not in item:
                errors.append(f"config/items.yaml: items[{i}] (name={item.get('name', '?')!r}) missing required key '{key}'")
        if "type" in item and item["type"] not in KNOWN_ITEM_TYPES:
            errors.append(
                f"config/items.yaml: items[{i}] (name={item.get('name', '?')!r}) has unrecognized "
                f"type {item['type']!r} -- must be one of {sorted(KNOWN_ITEM_TYPES)}"
            )

    if errors:
        raise ValueError("Config validation failed before any Fabric API call:\\n" + "\\n".join(f"  - {e}" for e in errors))


_validate_config(environments_cfg, lakehouses_cfg, items_cfg)

environments = environments_cfg["environments"]
if target_environments:
    environments = [e for e in environments if e["name"] in target_environments]
workspace_roles = environments_cfg.get("workspace_roles", {})

print(f"Deploying environment(s): {[e['name'] for e in environments]}")
"""))

cells.append(md("## Workspace + capacity + role helpers"))

cells.append(code(
"""def get_capacity_id(capacity_name: str) -> str:
    resp = api("GET", "/capacities")
    matches = [c for c in resp.json()["value"] if c["displayName"] == capacity_name]
    if len(matches) != 1:
        raise ValueError(f"Expected exactly one capacity named '{capacity_name}', found {len(matches)}")
    return matches[0]["id"]


def get_or_create_workspace(display_name: str, capacity_id: str, tier: str) -> str:
    resp = api("GET", "/workspaces")
    matches = [w for w in resp.json()["value"] if w["displayName"] == display_name]
    if matches:
        workspace_id = matches[0]["id"]
        current_capacity_id = matches[0].get("capacityId")
        print(f"  workspace exists: {display_name}")
    else:
        resp = api("POST", "/workspaces", json={"displayName": display_name})
        workspace_id = resp.json()["id"]
        current_capacity_id = None
        print(f"  created workspace: {display_name}")

    # Skip the reassignment call entirely once the workspace is already on the target
    # capacity -- assignToCapacity has no reason to run unconditionally on every deploy,
    # and this also sidesteps it entirely on a stale/renamed/paused capacity id once the
    # workspace is already correctly placed.
    if current_capacity_id != capacity_id:
        api("POST", f"/workspaces/{workspace_id}/assignToCapacity", json={"capacityId": capacity_id})
    else:
        print(f"    already on target capacity: {display_name}")

    # Per-tier, not one flat list applied everywhere -- Ingestion holds source
    # Connections and the raw metadata catalog, so a principal only needed in Code
    # shouldn't automatically also reach it.
    #
    # roleAssignments is additive, not an upsert -- re-POSTing a (principal, role) pair
    # the workspace already has is very likely to 4xx, so list what's already there first
    # and only create what's missing, same check-then-create shape as every other
    # get_or_create_* helper in this notebook.
    resp = api("GET", f"/workspaces/{workspace_id}/roleAssignments")
    existing_roles = {(ra["principal"]["id"], ra["role"]) for ra in resp.json()["value"]}
    for role in workspace_roles.get(tier, []):
        if (role["principal_id"], role["role"]) in existing_roles:
            continue
        api("POST", f"/workspaces/{workspace_id}/roleAssignments", json={
            "principal": {"id": role["principal_id"], "type": role["principal_type"]},
            "role": role["role"],
        })

    return workspace_id


def get_or_create_lakehouse(workspace_id: str, display_name: str) -> str:
    resp = api("GET", f"/workspaces/{workspace_id}/items")
    matches = [i for i in resp.json()["value"] if i["type"] == "Lakehouse" and i["displayName"] == display_name]
    if matches:
        print(f"    lakehouse exists: {display_name}")
        return matches[0]["id"]
    # Schema support (3-part lakehouse.schema.table names, used throughout this repo's
    # SQL/notebooks) is creation-time-only and opt-in -- confirmed live: a plain
    # POST .../items with no creationPayload produces a classic lakehouse where even
    # `CREATE SCHEMA lakehouse.dbo` fails ("database name is not valid"). Must use the
    # dedicated /lakehouses endpoint with enableSchemas: true (the generic /items create
    # doesn't accept creationPayload for this item type).
    resp = api("POST", f"/workspaces/{workspace_id}/lakehouses", json={
        "displayName": display_name,
        "creationPayload": {"enableSchemas": True},
    })
    body = poll_lro(resp).json() if resp.status_code == 202 else resp.json()
    print(f"    created lakehouse: {display_name}")
    return body["id"]
"""))

cells.append(md("## Workspace folders (Preview API)\n\n`POST .../folders` and the `folderId` field on item creation are both part\nof Fabric's Folder REST API, which is **Preview** as of this writing\n(\"provided for evaluation and development purposes only ... not\nrecommended for production use\" per Microsoft's own docs) -- so this stays\nadditive and best-effort: if the API shape changes, only folder placement\nis affected, not item creation itself (a create call without a valid\n`folderId` still succeeds, just lands at the workspace root).\n\nOnly the CREATE path needs to set `folderId` -- this is a new capability\nadded to an already-idempotent create-if-missing deploy flow, so an item\ncreated before folders existed simply stays wherever it already is\n(no move/migration logic here; out of scope)."))

cells.append(code(
"""def get_or_create_folder(workspace_id: str, display_name: str, parent_folder_id: str = None) -> str:
    # List this parent's DIRECT children only (recursive=False) and match both name and
    # parent -- a top-level folder and a same-named subfolder elsewhere in the workspace
    # are different folders. A folder with no parent (workspace root as parent) omits
    # parentFolderId entirely in the API response, so .get(...) -> None lines up with the
    # parent_folder_id=None default correctly.
    params = {"recursive": "False"}
    if parent_folder_id:
        params["rootFolderId"] = parent_folder_id
    resp = api("GET", f"/workspaces/{workspace_id}/folders", params=params)
    matches = [f for f in resp.json().get("value", []) if f["displayName"] == display_name and f.get("parentFolderId") == parent_folder_id]
    if matches:
        print(f"    folder exists: {display_name}")
        return matches[0]["id"]

    body = {"displayName": display_name}
    if parent_folder_id:
        body["parentFolderId"] = parent_folder_id
    resp = api("POST", f"/workspaces/{workspace_id}/folders", json=body)
    print(f"    created folder: {display_name}")
    return resp.json()["id"]
"""))

cells.append(md("## Generic item deploy (Notebook / DataPipeline / VariableLibrary)\n\nEvery item type in the Code workspace round-trips through the same shape: a\nfolder of files becomes a base64 `definitionParts` list, POSTed on first\ncreate or PATCHed via `updateDefinition` on every redeploy."))

cells.append(code(
"""ITEM_PART_FILENAMES = {
    "Notebook": ["notebook-content.py"],
    "DataPipeline": ["pipeline-content.json"],
    "VariableLibrary": ["settings.json", "variables.json"],  # valueSets/*.json appended dynamically
}


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def build_definition_parts(item_type: str, folder: str, substitutions: dict, content_override: dict = None) -> list:
    content_override = content_override or {}
    parts = []

    def add_part(payload_path: str, raw: bytes, is_text: bool = True):
        if is_text:
            text = raw.decode("utf-8")
            for token, value in substitutions.items():
                text = text.replace(token, value)
            raw = text.encode("utf-8")
        parts.append({
            "path": payload_path,
            "payload": _b64(raw),
            "payloadType": "InlineBase64",
        })

    add_part(".platform", read_repo_file(f"{folder}/.platform"))

    if item_type == "VariableLibrary":
        add_part("settings.json", read_repo_file(f"{folder}/settings.json"))
        add_part("variables.json", read_repo_file(f"{folder}/variables.json"))
        value_set_names = [n for n in archive.namelist() if n.startswith(f"{root_prefix}{folder}/valueSets/") and n.endswith(".json")]
        for name in value_set_names:
            rel = name[len(root_prefix):]
            add_part(rel[len(f"{folder}/"):], archive.read(name))
    else:
        for filename in ITEM_PART_FILENAMES[item_type]:
            raw = content_override.get(filename) or read_repo_file(f"{folder}/{filename}")
            add_part(filename, raw)

    return parts


def get_or_create_item(workspace_id: str, item_type: str, display_name: str, folder: str, substitutions: dict, content_override: dict = None, folder_id: str = None) -> str:
    resp = api("GET", f"/workspaces/{workspace_id}/items")
    matches = [i for i in resp.json()["value"] if i["type"] == item_type and i["displayName"] == display_name]
    definition = {"parts": build_definition_parts(item_type, folder, substitutions, content_override)}

    if matches:
        item_id = matches[0]["id"]
        resp = api("POST", f"/workspaces/{workspace_id}/items/{item_id}/updateDefinition", json={"definition": definition})
        poll_lro(resp)
        print(f"    updated {item_type}: {display_name}")
    else:
        # folderId is only meaningful on create -- this flow only handles the CREATE path for
        # folder placement (see the "Workspace folders" note above); an item that already
        # exists elsewhere isn't moved by updateDefinition, which has no folderId concept.
        create_body = {"displayName": display_name, "type": item_type, "definition": definition}
        if folder_id:
            create_body["folderId"] = folder_id
        resp = api("POST", f"/workspaces/{workspace_id}/items", json=create_body)
        body = poll_lro(resp).json() if resp.status_code == 202 else resp.json()
        item_id = body.get("id")
        if not item_id:
            # The generic /items create LRO's terminal poll body doesn't reliably inline
            # the created item (unlike the dedicated /lakehouses, /sqldatabases endpoints) --
            # confirmed live: the item exists right after "Succeeded" even when this body
            # has no "id". Re-fetch and match by name instead of trusting the poll body.
            resp = api("GET", f"/workspaces/{workspace_id}/items")
            found = [i for i in resp.json()["value"] if i["type"] == item_type and i["displayName"] == display_name]
            if not found:
                raise RuntimeError(f"Created {item_type} '{display_name}' but can't find it by name afterward: {body}")
            item_id = found[0]["id"]
        print(f"    created {item_type}: {display_name}")

    return item_id
"""))

cells.append(md("## Metadata catalog SQL Database"))

cells.append(code(
"""def get_or_create_sql_database(workspace_id: str, display_name: str) -> tuple:
    resp = api("GET", f"/workspaces/{workspace_id}/items")
    matches = [i for i in resp.json()["value"] if i["type"] == "SQLDatabase" and i["displayName"] == display_name]
    if matches:
        item_id = matches[0]["id"]
        print(f"    SQL database exists: {display_name}")
    else:
        resp = api("POST", f"/workspaces/{workspace_id}/sqldatabases", json={"displayName": display_name})
        body = poll_lro(resp).json() if resp.status_code == 202 else resp.json()
        item_id = body["id"]
        print(f"    created SQL database: {display_name}")

    detail = api("GET", f"/workspaces/{workspace_id}/sqldatabases/{item_id}").json()
    props = detail["properties"]
    return props["serverFqdn"], props["databaseName"]


def run_metadata_schema(server: str, database: str, sql_text: str) -> None:
    import pyodbc

    token_bytes = _token("https://database.windows.net/.default").encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = 1256

    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server},1433;DATABASE={database};Encrypt=yes"

    # Error 40613 ("database ... is not currently available, please retry") is the classic
    # Azure SQL cold-start/warm-up condition -- hit on the first deploy attempt against a
    # freshly created or long-idle SQL_METADATA_DATABASE in every environment tested. Always
    # resolved on a plain retry, so self-heal here instead of needing a human to notice and
    # re-run the whole job.
    CONNECT_RETRIES = 3
    conn = None
    for attempt in range(1, CONNECT_RETRIES + 1):
        try:
            conn = pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct}, autocommit=True)
            break
        except pyodbc.Error as exc:
            if attempt < CONNECT_RETRIES:
                print(f"    connect to {database} attempt {attempt}/{CONNECT_RETRIES} raised {exc!r}, retrying in 10s")
                time.sleep(10)
            else:
                raise
    try:
        cursor = conn.cursor()
        # split on a line whose only content (once stripped) is "GO" -- regardless of
        # leading indentation, so callers can pass an indented triple-quoted string too
        batches, current = [], []
        for line in sql_text.splitlines():
            if line.strip() == "GO":
                batches.append("\\n".join(current))
                current = []
            else:
                current.append(line)
        if current:
            batches.append("\\n".join(current))

        for batch in batches:
            batch = batch.strip()
            if batch:
                cursor.execute(batch)
        print("    metadata schema applied")
    finally:
        conn.close()
"""))

cells.append(md("## OneLake file upload (demo data seed)"))

cells.append(code(
"""def upload_file_to_onelake(workspace_id: str, lakehouse_id: str, dest_path: str, data: bytes) -> None:
    base = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}/Files/{dest_path}"
    headers = storage_headers()

    resp = requests.put(f"{base}?resource=file", headers=headers, timeout=REQUEST_TIMEOUT)
    if resp.status_code not in (201, 200):
        resp.raise_for_status()

    resp = requests.patch(f"{base}?action=append&position=0", headers=headers, data=data, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    resp = requests.patch(f"{base}?action=flush&position={len(data)}", headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
"""))

cells.append(md("## Deploy -- split across several small cells\n\nEach phase loops over every target environment, but stays in its own cell:\nheadless `RunNotebook` jobs on this runtime appear to enforce a per-statement\nexecution ceiling well under a minute, and a single cell that did every phase\nfor every environment (workspaces -> lakehouses -> SQL Database + schema ->\nnotebooks -> pipelines -> variable library -> demo data) tripped it partway\nthrough, killing the whole session before any exception could even be\ncaught. Splitting by phase keeps each cell's own work short regardless of how\nmany environments or items are involved. State that later phases need\n(workspace/lakehouse/notebook/pipeline ids) is carried in `env_state`, keyed\nby environment name."))

cells.append(code(
"""env_state = {env["name"]: {"processed_item_names": set()} for env in environments}
"""))

cells.append(md("### Phase 1 -- workspaces"))

cells.append(code(
"""for env in environments:
    print(f"\\n==================== {env['name']} ====================")
    st = env_state[env["name"]]
    try:
        st["include_silver"] = env["include_silver"]
        # Defaults to "Monza" (see config/environments.yaml) so a config predating this
        # field still deploys under the original name -- same backward-compat shape as
        # metadata_connection_guid's env.get(...) below.
        st["framework_name"] = env.get("framework_name", "Monza")
        st["capacity_id"] = get_capacity_id(env["capacity"])
        st["data_ws_name"] = f"{st['framework_name']} Data ({env['short']})"
        st["ingestion_ws_name"] = f"{st['framework_name']} Ingestion ({env['short']})"
        st["code_ws_name"] = f"{st['framework_name']} Code ({env['short']})"

        st["data_ws_id"] = get_or_create_workspace(st["data_ws_name"], st["capacity_id"], "data")
        st["ingestion_ws_id"] = get_or_create_workspace(st["ingestion_ws_name"], st["capacity_id"], "ingestion")
        st["code_ws_id"] = get_or_create_workspace(st["code_ws_name"], st["capacity_id"], "code")
    except Exception:
        print(f"    ERROR: Phase 1 (workspaces) failed for environment '{env['name']}':")
        traceback.print_exc()
        raise
"""))

cells.append(md("### Phase 2 -- lakehouses"))

cells.append(code(
"""for env in environments:
    st = env_state[env["name"]]
    print(f"-- lakehouses ({st['data_ws_name']})")
    try:
        st["lakehouse_ids"] = {}
        for lh in lakehouses_cfg["lakehouses"]:
            if lh["always"] or st["include_silver"]:
                st["lakehouse_ids"][lh["name"]] = get_or_create_lakehouse(st["data_ws_id"], lh["name"])
    except Exception:
        print(f"    ERROR: Phase 2 (lakehouses) failed for environment '{env['name']}':")
        traceback.print_exc()
        raise
"""))

cells.append(md("### Phase 3 -- metadata catalog SQL Database + schema"))

cells.append(code(
"""for env in environments:
    st = env_state[env["name"]]
    print(f"-- metadata catalog ({st['ingestion_ws_name']})")
    try:
        server, database = get_or_create_sql_database(st["ingestion_ws_id"], "SQL_METADATA_DATABASE")
        run_metadata_schema(server, database, metadata_sql)
        st["sql_server"] = server
        st["sql_database"] = database
    except Exception:
        print(f"    ERROR: Phase 3 (metadata catalog) failed for environment '{env['name']}':")
        traceback.print_exc()
        raise
"""))

cells.append(md("### Phase 4 -- workspace folders\n\nA small, fixed structure per workspace -- not everything needs a folder\n(`VAR_MONZA`, the lakehouses, and `SQL_METADATA_DATABASE` all stay at\nworkspace root). Created once per environment and reused by every\n`get_or_create_item` call in the phases below."))

cells.append(code(
"""for env in environments:
    st = env_state[env["name"]]
    print(f"-- folders ({st['code_ws_name']} / {st['ingestion_ws_name']})")
    try:
        st["code_folder_ids"] = {
            "Notebooks": get_or_create_folder(st["code_ws_id"], "Notebooks"),
            "Pipelines": get_or_create_folder(st["code_ws_id"], "Pipelines"),
        }
        st["ingestion_folder_ids"] = {
            "Pipelines": get_or_create_folder(st["ingestion_ws_id"], "Pipelines"),
        }
    except Exception:
        print(f"    ERROR: Phase 4 (folders) failed for environment '{env['name']}':")
        traceback.print_exc()
        raise


def target_workspace_id(st: dict, item: dict) -> str:
    \"\"\"Resolve an item's deploy target from its (optional) `workspace` field --
    'Code' (default) or 'Ingestion'.\"\"\"
    workspace = item.get("workspace", "Code")
    if workspace == "Code":
        return st["code_ws_id"]
    if workspace == "Ingestion":
        return st["ingestion_ws_id"]
    raise ValueError(f"Unknown workspace '{workspace}' for item '{item['name']}'")


def folder_id_for_item(st: dict, item_type: str, workspace: str) -> str:
    \"\"\"Resolve which folder (if any) an item's deploy target workspace should place it in --
    Code gets Notebooks/Pipelines by item type, Ingestion gets Pipelines; anything else (the
    Variable Library today) stays at workspace root by returning None.\"\"\"
    if workspace == "Code" and item_type == "Notebook":
        return st["code_folder_ids"]["Notebooks"]
    if workspace == "Code" and item_type == "DataPipeline":
        return st["code_folder_ids"]["Pipelines"]
    if workspace == "Ingestion" and item_type == "DataPipeline":
        return st["ingestion_folder_ids"]["Pipelines"]
    return None
"""))

cells.append(md("### Phase 5 -- notebooks"))

cells.append(code(
"""for env in environments:
    st = env_state[env["name"]]
    print(f"-- notebooks ({st['code_ws_name']})")
    try:
        # Binds each loader notebook's *default* lakehouse to Bronze -- that's what turns on
        # OneLake Spark Catalog for the whole Data workspace, so 3-part names (Landing.x.y,
        # Gold.x.y, etc.) resolve inside these notebooks without a separate binding per lakehouse.
        # A no-op substitution for notebooks that don't reference these placeholders at all.
        notebook_substitutions = {
            "__BRONZE_LAKEHOUSE_ID__": st["lakehouse_ids"]["Bronze"],
            "__DATA_WORKSPACE_ID__": st["data_ws_id"],
        }

        st["notebook_ids"] = {}
        for item in items_cfg["items"]:
            if item.get("requires_silver") and not st["include_silver"]:
                continue
            if item["type"] != "Notebook":
                continue
            workspace = item.get("workspace", "Code")
            st["notebook_ids"][item["name"]] = get_or_create_item(
                target_workspace_id(st, item), "Notebook", item["name"], f"src/{item['path']}", notebook_substitutions,
                folder_id=folder_id_for_item(st, "Notebook", workspace),
            )
            st["processed_item_names"].add(item["name"])
    except Exception:
        print(f"    ERROR: Phase 5 (notebooks) failed for environment '{env['name']}':")
        traceback.print_exc()
        raise
"""))

cells.append(md("### Phase 6 -- pipelines"))

cells.append(code(
"""for env in environments:
    st = env_state[env["name"]]
    print(f"-- pipelines ({st['code_ws_name']} / {st['ingestion_ws_name']})")
    try:
        notebook_ids = st["notebook_ids"]

        # This substitutions dict is used for every DataPipeline's own content. Every PL_INGEST_*
        # pipeline's Lookup activity needs __METADATA_CONNECTION_GUID__ -- the one Connection that
        # can't live in ingestion.Connection itself (see config/environments.yaml's comment on
        # metadata_connection_guid). Left "" until the one-time manual bootstrap (DEPLOYMENT.md) is
        # done for this environment -- deploy anyway (tolerating incomplete-but-recoverable state,
        # same as elsewhere in this notebook) rather than failing the whole run over it.
        metadata_connection_guid = env.get("metadata_connection_guid", "")
        if not metadata_connection_guid:
            print(f"    WARNING: metadata_connection_guid is not set for '{env['name']}' -- "
                  f"PL_INGEST_* pipelines will deploy, but their Lookup activities won't resolve "
                  f"a Connection until config/environments.yaml is updated and this is redeployed.")

        pipeline_substitutions = {
            "__NB_LOAD_BRONZE_ID__": notebook_ids["NB_LOAD_BRONZE"],
            "__NB_LOAD_GOLD_ID__": notebook_ids["NB_LOAD_GOLD"],
            "__METADATA_CONNECTION_GUID__": metadata_connection_guid,
            "__DATA_WORKSPACE_ID__": st["data_ws_id"],
            "__LANDING_LAKEHOUSE_ID__": st["lakehouse_ids"]["Landing"],
            "__CODE_WORKSPACE_ID__": st["code_ws_id"],
        }
        if st["include_silver"]:
            pipeline_substitutions["__NB_LOAD_SILVER_ID__"] = notebook_ids["NB_LOAD_SILVER"]

        st["pipeline_ids"] = {}
        for item in items_cfg["items"]:
            if item.get("requires_silver") and not st["include_silver"]:
                continue
            if item["type"] != "DataPipeline" or item["name"] == "PL_RUN_ALL":
                continue
            workspace = item.get("workspace", "Code")
            st["pipeline_ids"][item["name"]] = get_or_create_item(
                target_workspace_id(st, item), "DataPipeline", item["name"], f"src/{item['path']}", pipeline_substitutions,
                folder_id=folder_id_for_item(st, "DataPipeline", workspace),
            )
            st["processed_item_names"].add(item["name"])

        # PL_RUN_ALL itself always deploys into Code, and reaches every PL_INGEST_* pipeline (in
        # Ingestion) via NB_RUN_REMOTE_PIPELINE's cross-workspace TridentNotebook bridge instead of
        # a same-workspace ExecutePipeline reference -- see the EP_INGEST_* activities in
        # PL_RUN_ALL.DataPipeline/pipeline-content.json.
        run_all_substitutions = {
            "__PL_INGEST_SQL_ID__": st["pipeline_ids"]["PL_INGEST_SQL"],
            "__PL_INGEST_FILE_ID__": st["pipeline_ids"]["PL_INGEST_FILE"],
            "__PL_INGEST_SQLMI_ID__": st["pipeline_ids"]["PL_INGEST_SQLMI"],
            "__PL_INGEST_ORACLE_ID__": st["pipeline_ids"]["PL_INGEST_ORACLE"],
            "__PL_INGEST_SFTP_ID__": st["pipeline_ids"]["PL_INGEST_SFTP"],
            "__PL_INGEST_FTP_ID__": st["pipeline_ids"]["PL_INGEST_FTP"],
            "__PL_INGEST_ONELAKETABLE_ID__": st["pipeline_ids"]["PL_INGEST_ONELAKETABLE"],
            "__PL_INGEST_ONELAKEFILE_ID__": st["pipeline_ids"]["PL_INGEST_ONELAKEFILE"],
            "__PL_LOAD_BRONZE_ID__": st["pipeline_ids"]["PL_LOAD_BRONZE"],
            "__PL_LOAD_GOLD_ID__": st["pipeline_ids"]["PL_LOAD_GOLD"],
            "__NB_RUN_REMOTE_PIPELINE_ID__": notebook_ids["NB_RUN_REMOTE_PIPELINE"],
            "__INGESTION_WORKSPACE_ID__": st["ingestion_ws_id"],
            "__CODE_WORKSPACE_ID__": st["code_ws_id"],
        }

        # config/items.yaml's committed PL_RUN_ALL.DataPipeline is the 4-stage form (no
        # Silver). For an include_silver environment, splice in a 5th ExecutePipeline
        # activity here rather than maintaining a second committed pipeline file --
        # insert EP_LOAD_SILVER between Bronze and Gold and repoint Gold's dependsOn.
        run_all_override = None
        if st["include_silver"]:
            pl_run_all = json.loads(read_repo_text("src/PL_RUN_ALL.DataPipeline/pipeline-content.json"))
            activities = pl_run_all["properties"]["activities"]
            gold_idx = next(i for i, a in enumerate(activities) if a["name"] == "EP_LOAD_GOLD")
            activities.insert(gold_idx, {
                "name": "EP_LOAD_SILVER",
                "type": "ExecutePipeline",
                "dependsOn": [{"activity": "EP_LOAD_BRONZE", "dependencyConditions": ["Succeeded"]}],
                "policy": {"retry": 2, "retryIntervalInSeconds": 30, "secureInput": False, "secureOutput": False},
                "typeProperties": {
                    "pipeline": {"referenceName": st["pipeline_ids"]["PL_LOAD_SILVER"], "type": "PipelineReference"},
                    "waitOnCompletion": True,
                    "parameters": {},
                },
            })
            activities[gold_idx + 1]["dependsOn"] = [{"activity": "EP_LOAD_SILVER", "dependencyConditions": ["Succeeded"]}]
            run_all_override = {"pipeline-content.json": json.dumps(pl_run_all, indent=2).encode("utf-8")}

        get_or_create_item(
            st["code_ws_id"], "DataPipeline", "PL_RUN_ALL", "src/PL_RUN_ALL.DataPipeline",
            run_all_substitutions, content_override=run_all_override,
            folder_id=st["code_folder_ids"]["Pipelines"],
        )
        st["processed_item_names"].add("PL_RUN_ALL")
    except Exception:
        print(f"    ERROR: Phase 6 (pipelines) failed for environment '{env['name']}':")
        traceback.print_exc()
        raise
"""))

cells.append(md("### Phase 7 -- variable library + demo data seed"))

cells.append(code(
"""for env in environments:
    st = env_state[env["name"]]
    try:
        for item in items_cfg["items"]:
            if item["type"] == "VariableLibrary":
                get_or_create_item(st["code_ws_id"], "VariableLibrary", item["name"], f"src/{item['path']}", substitutions={})
                st["processed_item_names"].add(item["name"])

        print(f"-- seeding demo data ({st['data_ws_name']}/Landing)")
        upload_file_to_onelake(
            st["data_ws_id"], st["lakehouse_ids"]["Landing"], "customer/customer.csv", read_repo_file("demodata/customer.csv")
        )
        print(f"Done: {env['name']}")
    except Exception:
        print(f"    ERROR: Phase 7 (variable library / demo data) failed for environment '{env['name']}':")
        traceback.print_exc()
        raise
"""))

cells.append(md("## Register demo Connection/Database/Table + summary\n\nOne-time metadata rows so `NB_LOAD_BRONZE` / `dim_customer` / `fact_signup`\nhave something to load on first run (the `ingestion.*` DDL creates empty\ntables only). A single active `ingestion.Table` row drives a table's entire\nSource -> Landing -> Bronze flow now, so one `Connection` + one `Database` +\none `Table` row (File-type, `LoadType = 'Full'`) is all it takes.\n\nThe demo `Connection` row is deliberately `IsActive = 0` --\n`demodata/customer.csv` is uploaded directly to Landing above (there's no\nreal Connection object behind that `NEWID()` GUID), so `PL_INGEST_FILE`'s own\nLookup (`ingestion.vw_ActiveIngestTables`, which filters on\n`c.[IsActive] = 1` in its own `WHERE` clause -- see `config/metadata_schema.sql`)\ncorrectly skips it rather than failing trying to Copy through a Connection\nthat doesn't exist. `NB_LOAD_BRONZE` doesn't filter on `Connection.IsActive`\nat all, so it still picks up the row fine.\n\nSilver no longer has a metadata table of its own -- for an `include_silver`\nenvironment, `sil_customer.Notebook` (a hand-written, per-table notebook,\nsame shape as `dim_customer`) is deployed automatically via `config/items.yaml`\nin Phase 5 above and %run-chained from `NB_LOAD_SILVER`. No seed row needed\nto exercise `NB_LOAD_SILVER`/`PL_LOAD_SILVER` end to end -- Gold still reads\nBronze directly here, since nothing in this demo actually reuses the Silver\nshape (the real rule this framework follows throughout)."))

cells.append(code(
"""for env in environments:
    st = env_state[env["name"]]
    try:
        seed_sql = \"\"\"
        IF NOT EXISTS (SELECT 1 FROM [ingestion].[Connection] WHERE [Name] = 'demo_customer_source')
        INSERT INTO [ingestion].[Connection] ([Name], [ConnectionType], [ConnectionGuid], [IsActive]) VALUES ('demo_customer_source', 'File', NEWID(), 0)
        GO
        IF NOT EXISTS (SELECT 1 FROM [ingestion].[Database] WHERE [Name] = 'demo')
        INSERT INTO [ingestion].[Database] ([ConnectionId], [Name])
        SELECT [ConnectionId], 'demo' FROM [ingestion].[Connection] WHERE [Name] = 'demo_customer_source'
        GO
        IF NOT EXISTS (SELECT 1 FROM [ingestion].[Table] WHERE [BronzeName] = 'customer' AND [SourceObject] = 'customer.csv')
        INSERT INTO [ingestion].[Table] ([DatabaseId], [SourceObject], [FilePath], [FileType], [BronzeSchema], [BronzeName], [PrimaryKeys], [LoadType])
        SELECT [DatabaseId], 'customer.csv', 'customer', 'csv', 'dbo', 'customer', 'CustomerId', 'Full' FROM [ingestion].[Database] WHERE [Name] = 'demo'
        GO
        \"\"\"
        run_metadata_schema(st["sql_server"], st["sql_database"], seed_sql)
        print(f"Seeded demo Connection/Database/Table for {env['name']}")

        # Confirm every applicable items.yaml entry was actually claimed by Phase 5/6/7 above --
        # an unrecognized/mistyped 'type' would already have been caught by _validate_config, but
        # this catches any other way an entry could fall through every phase's filter unnoticed
        # (e.g. a future phase filter bug) before this reports success.
        expected_item_names = {
            i["name"] for i in items_cfg["items"] if not i.get("requires_silver") or st["include_silver"]
        }
        missing_item_names = expected_item_names - st["processed_item_names"]
        if missing_item_names:
            raise RuntimeError(
                f"Deploy for '{env['name']}' never processed these items.yaml entries: "
                f"{sorted(missing_item_names)}"
            )
    except Exception:
        print(f"    ERROR: final phase (demo seed / item-coverage check) failed for environment '{env['name']}':")
        traceback.print_exc()
        raise

print("\\nDeployment complete for:", [e["name"] for e in environments])
"""))

nb = {
    "nbformat": 4,
    "nbformat_minor": 5,
    "metadata": {
        "dependencies": {"lakehouse": {}},
        "kernel_info": {"jupyter_kernel_name": "python3.11", "name": "jupyter"},
        "kernelspec": {"display_name": "Jupyter", "language": "Jupyter", "name": "jupyter"},
        "language_info": {"name": "python"},
        "microsoft": {"language": "python", "language_group": "jupyter_python", "ms_spell_check": {"ms_spell_check_language": "en"}},
        "nteract": {"version": "nteract-front-end@1.0.0"},
        "spark_compute": {"compute_id": "/trident/default", "session_options": {"conf": {"spark.synapse.nbs.session.timeout": "1200000"}}},
    },
    "cells": cells,
}

import os
out_path = os.path.join(os.getcwd(), "setup", "NB_DEPLOY.ipynb")
with open(out_path, "w", encoding="utf-8", newline="\n") as f:
    json.dump(nb, f, indent=1)
    f.write("\n")

print("wrote", out_path, "cells:", len(cells))
