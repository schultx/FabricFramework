# Improvement Roadmap

Findings from a hands-on review of the framework (live-tested against development, test, and
production in the `1fdf3400-c721-4fc4-84ca-7163d6ba4399` tenant), read through one lens: **what
makes this framework harder to stand up for a new client, faster.** Ordered by priority.

## 1. The framework name is hardcoded, not configurable

`"Monza Data ({env['short']})"` and its siblings are literal f-strings in
[setup/build_nb_deploy.py](../../setup/build_nb_deploy.py) (search for `"Monza`). There is no
config field for a client/engagement name. Today, onboarding a new client means editing generator
*code*, not config.

**Fix:** add a `framework_name` (or `client_name`) field to
[config/environments.yaml](../../config/environments.yaml) and thread it through every naming
f-string. This is probably the single highest-leverage change for turning this repo from
"our internal build" into an actual reusable template — see
[New-Client-Onboarding](New-Client-Onboarding.md) step 2, which currently has to tell the reader
to hand-edit code because this doesn't exist yet.

## 2. Deploy failures surface as an opaque, generic error

`assignToCapacity` runs unconditionally on *every* deploy — even for workspaces that already
exist and are already correctly assigned — and it's resolved by capacity **name** with zero
tolerance for that capacity being paused, renamed, or replaced. When it fails, Fabric wraps
literally any statement failure in the same generic message: `"System cancelled the Spark
session due to statement execution failures"`. This gave zero signal about the real cause on
every one of the failure modes actually hit while building this: a stale capacity name, a cold
SQL database, and (before this review) a real code bug — all three looked identical from the
outside.

**Fix, two parts:**
- Wrap each deploy phase in `try`/`except` that prints the real Python exception to the
  notebook's own cell output before re-raising. Costs nothing, turns a diagnosis that took
  hours of live reproduction into a 30-second read of the notebook's own log.
- Skip the `assignToCapacity` call when the workspace is already on the target capacity, so a
  stale/paused capacity name doesn't break an otherwise-healthy redeploy.

## 3. `metadata_connection_guid` is a manual, easy-to-forget onboarding step

Every environment needs a human to register a Fabric Connection in the portal and paste its GUID
into git before ingestion Lookups work at all. It's currently blank in all three environments of
this very project. See [Deployment-Guide](Deployment-Guide.md) for why it exists (a genuine
chicken-and-egg case) and the manual steps.

**Fix candidate (unverified):** Fabric's REST API supports creating Connections programmatically
(`POST /v1/connections`). Worth investigating whether `NB_DEPLOY` can create this Connection
itself using the same service-principal credentials used elsewhere, removing the manual step
entirely.

## 4. Transient SQL warm-up failures require a human to notice and retry

The classic Azure SQL "database not currently available, retry" condition (error 40613) hit on
the *first* deploy attempt against a freshly created or long-idle `SQL_METADATA_DATABASE` in
**every single environment tested this session** — development, test, and production. It always
resolved on a plain retry. This is a well-understood, repeating pattern at this point, not bad
luck.

**Fix:** build a retry loop directly into `run_metadata_schema()` in
[setup/build_nb_deploy.py](../../setup/build_nb_deploy.py) (3 attempts, ~10s apart) so a cold
database self-heals without a human watching the job.

## 5. Capacity contention is a real sizing risk, not just a dev/test fluke

Running all 8 `EP_INGEST_*` stages of `PL_RUN_ALL` in parallel starved and, in one run, outright
failed with `TooManyRequestsForCapacity` (HTTP 430) on a small (F2-class) capacity. A client on a
comparably small SKU will hit this on day one.

**Fix:** expose ingest fan-out concurrency as a config knob (e.g. batch the 8 `EP_INGEST_*`
activities into 2-3 sequential waves instead of one flat parallel fan-out) so smaller clients
don't need to over-provision capacity just to get a clean first run.

## 6. `NB_LIST_LANDING_ENTITIES` is dead weight

Unused, disconnected from every pipeline, and cannot even be deleted via the Fabric API — it
fails consistently with `400 {"errorCode":"UnknownError","isRetriable":false}` in every
environment tested. It will accumulate as a permanent orphan in every future client environment.

**Fix:** remove it from `src/` and [config/items.yaml](../../config/items.yaml) entirely rather
than continuing to deploy it.

## 7. The Bronze self-reference workaround needs one shared, well-tested helper

A notebook's own default lakehouse can be *written* via `saveAsTable()` using its own name in the
table path, but *reading* it back the same way trips Spark's
`[REQUIRES_SINGLE_PART_NAMESPACE]` parser error. The current fix — a direct OneLake path read into
a session-local temp view — is copy-pasted across `dim_customer`, `fact_signup`, and
`sil_customer` individually. One of those copies had already drifted back to the broken form in
the live deployed environment before this review caught it (see
[TODO.md](../../TODO.md)) — proof this pattern is fragile when duplicated by hand.

**Fix:** a single shared helper in `NB_MONZA_FUNCTIONS` (e.g. `read_own_lakehouse_table()`) that
every Gold/Silver notebook calls, so the workaround exists exactly once instead of once per
notebook. Tracked in [TODO.md](../../TODO.md) — explicitly deferred, not started.

## 8. The deploy notebook's generator only just started living in git

`setup/build_nb_deploy.py` — the entire source of truth for what `NB_DEPLOY.ipynb` actually
does — existed only in a local scratchpad outside version control for most of this project's
build-out. From a fresh clone there was no way to regenerate or modify the deploy notebook's own
logic at all. Fixed during this review (now committed at
[setup/build_nb_deploy.py](../../setup/build_nb_deploy.py)) — flagged here so it isn't
reintroduced. Regenerate with `python setup/build_nb_deploy.py` from the repo root after any
change to it, and commit the regenerated `setup/NB_DEPLOY.ipynb` alongside.

---

**If prioritizing for the next round of work:** #1 and #2 first. Today, standing up production
specifically cost several hours *directly* because of these two — there's no clean per-client
naming story yet, and a routine capacity change (unrelated to the framework itself) produced an
undiagnosable error instead of a clear one.
