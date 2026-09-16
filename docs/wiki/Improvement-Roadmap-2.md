# Improvement Roadmap 2

Round 2. [Improvement-Roadmap.md](Improvement-Roadmap.md) is the completed record of the first
full code review (33/33 findings actioned) — this page is everything found since: a deeper
code-level pass, live Fabric platform research (what's shipped since ~January 2026), and
governance/security/admin research specifically against
[learn.microsoft.com/fabric/admin](https://learn.microsoft.com/en-us/fabric/admin/),
[learn.microsoft.com/fabric/data-engineering](https://learn.microsoft.com/en-us/fabric/data-engineering/),
and the [Fabric roadmap](https://roadmap.fabric.microsoft.com/?product=administration%2Cgovernanceandsecurity)
filtered to Administration/Governance/Security. Nothing here duplicates round 1.

None of this has been implemented yet — this is the list, not the fix.

## Priority order

1. [§Data Quality 1 — Gold's overwrite paths never got the empty-result guard Bronze got](#1-golds-overwrite-paths-never-got-the-empty-result-guard-bronze-got)
2. [§Docs 1 — Incremental-Loading.md shows the pre-fix, injection-shaped SQL as current](#1-incremental-loadingmd-shows-the-pre-fix-injection-shaped-sql-as-current)
3. [§Governance & Security 1 — no PII/sensitivity classification posture, and it's now a one-line native fix](#1-no-piisensitivity-classification-posture--fabric-has-a-native-answer)
4. [§Governance & Security 2 — the public-GitHub-repo requirement may be a hard blocker, not just a confidentiality risk](#2-the-public-github-repo-requirement-may-be-a-hard-blocker-not-just-a-confidentiality-risk)
5. [§Governance & Security 3 — SPN is over-privileged with no documented scope-down step](#3-spn-is-over-privileged-with-no-documented-scope-down-step)
6. [§Platform Features 1 — Variable Library's new Connection Reference type could close the metadata_connection_guid gap](#1-variable-librarys-new-connection-reference-type-could-close-the-metadata_connection_guid-gap)
7. [§Testing 1 — zero automated tests exist anywhere in the framework](#1-zero-automated-tests-exist)

---

## Governance & Security

*New section — a deep-dive pass plus targeted research against Microsoft's own admin/governance/
security docs and roadmap, since neither prior round looked at this framework through a security/
compliance lens at all.*

### 1. No PII/sensitivity classification posture — Fabric has a native answer

*Severity: high*

Repo-wide search for PII/GDPR/classification/sensitivity/masking across every wiki page and root
doc returns nothing. The shipped demo (`demodata/customer.csv`) carries Email/FirstName/LastName/
Phone flowing unmasked Bronze→Gold with no guidance for when real client PII replaces it. Separately:
`_discover_and_map_foreign_keys()` (`NB_MONZA_FUNCTIONS.Notebook`) prints up to 5 sample **unmatched
business-key values** on every FK-mapping warning — if a client's natural key is itself sensitive
(email, account number), real values land unredacted in Fabric run logs on every run with unmatched
rows.

Microsoft Purview Information Protection sensitivity labels are fully GA in Fabric (confirmed via
the roadmap: GA since Q1 2024, with **"Data protection: sensitivity labels in Public APIs"** reaching
GA in Q1 2026 — meaning labels can now be read/set programmatically, not just through the portal) —
this isn't a "wait and see" platform feature, it's mature and already scriptable.

**Fix:** add a PII/classification section to [New-Client-Onboarding.md](New-Client-Onboarding.md) —
at minimum a checklist prompt ("does this client's data include PII? apply a sensitivity label to
the Gold lakehouse / affected tables before go-live"), pointing at Purview Information Protection
rather than inventing a custom mechanism. Separately, redact or truncate the sample values in
`_discover_and_map_foreign_keys()`'s unmatched-key log line (print a count and a hash/prefix instead
of the raw value).

### 2. The public-GitHub-repo requirement may be a hard blocker, not just a confidentiality risk

*Severity: high*

`setup/build_nb_deploy.py` does a plain unauthenticated `requests.get()` against
`https://github.com/{GITHUB_REPO}/archive/.../main.zip` — `New-Client-Onboarding.md` step 1 already
flags that this means every client fork must stay a **public** repo (schema, business logic,
Connection GUIDs, and once filled in, `workspace_roles` Entra principal IDs, all world-readable) —
but treats it only as a one-line confidentiality aside.

Research against Fabric's own roadmap surfaces a second, harder problem: **Outbound Access
Protection** shipped GA for Spark (Q3 2025) and is expanding to more workloads (EventHouse, Mirrored
DBs, Data Integration, Variable Library — all Q1–Q2 2026), and a **Workspace Public IP Firewall**
also reached GA in Q1 2026. Any client whose security team enables outbound network restrictions on
their Fabric tenant — an increasingly available, increasingly likely lever — can **block the
Spark session's own outbound call to github.com entirely**, unless it's explicitly allowlisted. This
isn't hypothetical: it's the exact class of control these recently-GA'd features exist to let a
security-conscious client turn on. The current design has no fallback if that happens.

**Fix:** (a) add a pre-flight check to [New-Client-Onboarding.md](New-Client-Onboarding.md) —
confirm outbound access to `github.com` is allowed (or get it allowlisted) before the first deploy
attempt in a new client tenant; (b) investigate a private-repo-compatible fetch path (a PAT or
deploy key passed as a pipeline secret instead of an anonymous `requests.get`) as a real alternative
to "the fork must be public," not just a documented constraint.

### 3. SPN is over-privileged with no documented scope-down step

*Severity: high*

Every documented path (bootstrap SPN, each per-environment WIF SPN, even the throwaway SPN in
`TESTING_SETUP.md`) is granted **capacity-level** Contributor, and via the Fabric API the SPN also
becomes implicit **Workspace Admin** (not Contributor) on every workspace it creates — with no
documented step anywhere to scope it down after bootstrap. `New-Client-Onboarding.md` step 5 sells
per-environment WIF SPNs as "blast-radius isolation," but `config/environments.yaml` points all
three environments at the *same* `capacity:` value, undercutting that claim as currently configured.

Fabric has a native, GA mechanism for exactly this handoff: **"Take ownership of Fabric items"**
(GA Q1 2025) — items created by a service principal can have ownership transferred to a named
user/group, after which the SPN's standing access can be reduced.

**Fix:** add a post-bootstrap step to [New-Client-Onboarding.md](New-Client-Onboarding.md) — after
the first successful deploy, use "Take ownership" to transfer created items to a named admin/group,
then reduce the deploying SPN back to the minimum role it actually needs for redeploys (Contributor,
not the implicit Admin it started with). Separately, either give each environment its own capacity
in `environments.yaml` or update the "blast-radius isolation" claim in
[Deployment-Guide.md](Deployment-Guide.md) to be honest that it doesn't hold today.

### 4. Nothing watches Fabric's own native audit/monitoring surface

*Severity: medium*

[Operations-Guide.md](Operations-Guide.md) already documents that `audit.NotebookRun`/`PipelineRun`
data is written but nothing watches it (Round 2's own finding). Separate from that: Fabric has a
native **admin monitoring workspace**, **Monitoring hub**, and tenant-level **audit log** that Monza
never references at all — these cover platform-level events (who deployed what, when, from where)
that Monza's own business-logic-level audit schema doesn't and shouldn't try to duplicate.

**Fix:** add a short pointer in [Operations-Guide.md](Operations-Guide.md) to the admin Monitoring
hub / audit log as the platform-level complement to `audit.NotebookRun` — "who ran a deploy" is a
different question from "did the deploy's own business logic succeed," and a consultant
troubleshooting an incident needs both.

### 5. Customer Managed Keys and Private Link are available but never mentioned

*Severity: low*

Customer Managed Keys (GA for Fabric broadly Q4 2025, GA for SQL DB specifically Q1 2026) and
Private Link at both tenant and workspace level (GA) are mature, adoptable hardening options for a
security-conscious client engagement. Neither is mentioned anywhere in the framework's docs as an
available lever.

**Fix:** a short "hardening options" callout in [Deployment-Guide.md](Deployment-Guide.md) or
[New-Client-Onboarding.md](New-Client-Onboarding.md) naming both as things to raise with a client's
security team when relevant, not something to implement by default.

---

## Cost & Capacity Observability

### 1. Capacity sizing and cost visibility are still just prose

*Severity: medium*

Round 1's Operations-Guide already documents the capacity-contention failure mode live-hit this
session (`TooManyRequestsForCapacity`), and Round 2 already flagged that sizing guidance is prose,
not a table. Since then: Microsoft's own **Capacity Metrics app** (GA), **Fabric Chargeback app**
(GA Q2 2026), and the upcoming **Capacity Insights & Actions** (preview, Q4 2026) are native,
zero-build tools that directly answer "how much capacity does this client need" and "what is this
costing" — exactly the gap Monza currently has no good answer for beyond hand-written prose.

**Fix:** recommend installing the Capacity Metrics app as a standard step in
[New-Client-Onboarding.md](New-Client-Onboarding.md), and reference it directly from
[Operations-Guide.md](Operations-Guide.md)'s capacity-sizing section instead of (or alongside) prose
guidance.

### 2. Fabric Capacity Overage — a platform-level alternative to hard throttling failures

*Severity: low*

A new capability (public preview since Q1 2026, GA planned Q4 2026) — worth understanding as a
possible alternative to the hard `TooManyRequestsForCapacity` failures hit live this session, once
it reaches GA. Not actionable yet (still preview) — flagged for a future pass.

---

## Disaster Recovery

### 1. Fabric has native DR/recovery capabilities Operations-Guide.md is silent about

*Severity: medium*

Round 2 already flagged "no documented rollback/DR procedure." Since then, confirmed: Fabric has had
native **Disaster Recovery support** and **Workspace recovery** (both GA since Q1 2024) — this isn't
a gap Monza needs to build custom tooling for, it's a gap in *documenting what already exists*.

**Fix:** add a short "if something goes badly wrong" section to
[Operations-Guide.md](Operations-Guide.md) pointing at native workspace recovery and DR support,
plus (already known, separately) that the Fabric SQL Database backing `SQL_METADATA_DATABASE` has
its own point-in-time restore via `earliestRestorePoint`/`latestRestorePoint`.

### 2. Item Soft-Delete and Recovery API — worth checking against the persistent NB_LIST_LANDING_ENTITIES bug

*Severity: low, worth investigating*

A new REST API (public preview, Q1 2026) for soft-delete/recovery of Fabric items. Worth a live check
against the still-unresolved `NB_LIST_LANDING_ENTITIES` item in dev/test, which has consistently
failed to delete via the normal item-delete API all session
(`400 {"errorCode":"UnknownError","isRetriable":false}`) — this new API might explain why (soft-delete
state?) or offer a different path to actually clean it up. Not confirmed; flagged for investigation,
not assumed to work.

---

## Platform Features Monza Doesn't Use Yet

### 1. Variable Library's new Connection Reference type could close the metadata_connection_guid gap

*Severity: medium*

Fabric Variable Library gained two new types since Monza's `VAR_MONZA` library was built: **Item
Reference** (GA, points at a Fabric item by workspace+item ID) and **Connection Reference** (GA
March 2026, points at a Connection by ID). `VAR_MONZA` currently uses only plain `String`/`Boolean`
types.

Connection Reference maps directly onto Improvement-Roadmap.md's still-open Deployment #6: the
manually-pasted, easy-to-forget `metadata_connection_guid` field in `environments.yaml`. If Variable
Library can hold a Connection Reference that's promoted per-environment through deployment (the same
way it already promotes plain variables), this could turn a manual git-config step into a proper,
git-tracked, deployment-aware variable.

**Fix:** investigate replacing `metadata_connection_guid` with a Connection Reference variable in
`VAR_MONZA`, referenced by `PL_INGEST_*`'s Lookup activities instead of the `__METADATA_CONNECTION_GUID__`
substitution token. Item Reference is separately worth evaluating as a replacement for the
`__BRONZE_LAKEHOUSE_ID__`/`__DATA_WORKSPACE_ID__` substitution tokens threaded through every notebook.

### 2. `fab` CLI is now GA and pre-installed in notebooks — worth a live retest

*Severity: low, worth investigating*

`ms-fabric-cli` reached GA (v1.5) and is now **pre-installed and pre-authenticated** in PySpark
notebooks — `!fab` works with no `%pip install` at all. [Deployment-Guide.md](Deployment-Guide.md)'s
documented reason for avoiding it entirely was that `%pip install ms-fabric-cli` broke `sempy.fabric`'s
context provider mid-session — since that installation step is no longer necessary to use the CLI,
the exact trigger condition may no longer apply. No confirmation the underlying bug was fixed, only
that the trigger changed.

**Fix:** a live retest — `!fab` in a scratch notebook in dev, confirm `sempy.fabric` still works
afterward in the same session. If clean, `fab deploy` (which wraps the `fabric-cicd` Python library)
is separately worth evaluating against `build_nb_deploy.py`'s hand-rolled REST calls for future
simplification — not a small change, a real re-architecture candidate, not to be done casually.

### 3. Fabric "Environments" — unused, but the right vehicle if a client ever needs custom libraries

*Severity: low*

Fabric has a first-class "Environment" item (pin Spark compute + library configuration, natively
Git-integrated and deployment-pipeline-aware) that Monza's notebooks never reference — every notebook
runs on default compute with no custom libraries. Fine today (nothing in the demo needs one), but if
a client engagement ever needs a custom Python package, this is the native, deployable vehicle for
it — not an ad hoc `%pip install` cell (which, per Monza's own hard-won lesson about `ms-fabric-cli`,
is exactly the kind of thing that can break notebook context providers mid-session).

**Fix:** no code change needed now — add a note to [Architecture.md](Architecture.md) or
[Deployment-Guide.md](Deployment-Guide.md) that custom library needs should go through a Fabric
Environment item, not inline `%pip install`, so this doesn't get rediscovered the hard way later.

---

## Data Quality

### 1. Gold's overwrite paths never got the empty-result guard Bronze got

*Severity: high*

Improvement-Roadmap.md's Bronze #2 fixed `NB_LOAD_BRONZE` so an empty Full-load result no longer
silently truncates an existing table. That fix was never ported one layer up: `load_fact()`'s default
`write_mode='overwrite'` and `write_dimension_type1`/`write_dimension_type2`'s full-refresh/
first-create overwrite branches all call `.mode("overwrite")...saveAsTable(...)` with zero row-count
check. A transient empty upstream read silently blanks a Gold dimension or fact — one hop from a
client Power BI semantic model — and the run still reports success.

**Fix:** apply the same guard used in `NB_LOAD_BRONZE` (Improvement-Roadmap Bronze #2) to `load_fact`'s
overwrite path and to `write_dimension_type1`/`write_dimension_type2`'s first-create/full-refresh
overwrite branches: refuse an empty result against an already-existing, already-populated table.

### 2. No schema/contract validation anywhere in the Gold or Silver facade

*Severity: medium*

The only validation anywhere in the facade is raising when zero `_key`-suffixed columns are found
(already fixed this round — Improvement-Roadmap Silver & Gold #6). No type checks, no null-rate
checks on business attributes, no uniqueness assertions, no row-count bounds anywhere else.

**Fix:** not a full data-quality framework — but consider a minimal, opt-in row-count/null-rate
sanity check in `write_silver_table`/`load_dimension`/`load_fact` (e.g. warn if row count drops by
more than X% from the previous run), logged to `audit.NotebookRun` rather than failing the run
outright.

---

## Documentation Accuracy

### 1. `Incremental-Loading.md` shows the pre-fix, injection-shaped SQL as current

*Severity: high*

Its Hop 1 walkthrough shows `ISNULL(w.[LastValue], '1900-01-01')` spliced with no escaping — exactly
the shape Improvement-Roadmap Ingestion #1 (high severity, SQL-injection-shaped) fixed. The actual
current `config/metadata_schema.sql` wraps it in `REPLACE(...,'''','''''')` and branches on
`IncrementalColumnType` for numeric watermarks (Ingestion #6's fix) — neither appears on the page.
Improvement-Roadmap.md's own closing cross-reference points readers to this exact page for "how
incremental actually works" — a consultant learning from the page the roadmap itself recommends sees
stale, unescaped example SQL presented as ground truth.

**Fix:** update the Hop 1 SQL example to match the current, escaped, type-branching view definition.

### 2. Root `DEPLOYMENT.md` and `TESTING_SETUP.md` have drifted from the wiki and from reality

*Severity: medium*

`README.md` tells every reader to "Start with DEPLOYMENT.md" — but that file's CI/CD section
describes a plain 3-stage pipeline with no mention of the `Validate` stage, `pr: none`,
`templates/deploy-stage.yml`, `--commit` traceability, or WIF status, all added since and correctly
documented in [Deployment-Guide.md](Deployment-Guide.md). `TESTING_SETUP.md` is a first-person
AI-assistant scratch note sitting in the repo root as if it were project documentation, citing
defaults that no longer match the repo (`capacity: fabfabricmsa` vs. the actual trial capacity;
`include_silver: false` vs. the actual, deliberate `true`).

**Fix:** reduce `DEPLOYMENT.md` to a short pointer at [Deployment-Guide.md](Deployment-Guide.md)
rather than maintaining two descriptions of the same pipeline that can drift independently. Delete or
relocate `TESTING_SETUP.md` — it reads as a leftover working note, not documentation a client should
see in their forked repo.

---

## Extensibility & Maintainability

### 1. No versioning story for the framework itself across parallel client forks

*Severity: medium*

No `CHANGELOG.md`, no `VERSION` file, no git tags. The delivery model is "fork the repo per client,"
and each fork immediately diverges. Nothing tracks which upstream commit a client's fork branched
from, and there's no process for pulling one of the (now 33+) fixed roadmap items into an
already-delivered fork. The `--commit` flag added this round only answers "which commit is live in
*this* fork's dev/test/prod," not "how far behind upstream is this fork" — a distinct problem once
multiple engagements exist side by side.

**Fix:** tag releases on `main` (even lightweight, e.g. `v2026.09.1`) so a delivered fork has a
concrete point to diff against; consider a `CHANGELOG.md` summarizing what changed per tag, written
for a consultant deciding whether to pull an upstream fix into a live client fork.

### 2. Residual "MONZA" branding survives the rename step even after a full client rebrand

*Severity: low*

`New-Client-Onboarding.md` step 2 already documents that `VAR_MONZA`/`NB_MONZA_FUNCTIONS` keep the
literal "MONZA" in item names for every client, even after `framework_name` is set — every delivered
client tenant permanently shows Columbus's internal codename inside its own Fabric workspace.

**Fix:** either accept this as a known, documented limitation (it already is) or extend the
`framework_name` substitution to these two item names as well — a larger change than the naming
fields it currently touches, since these are referenced by literal name (`%run NB_MONZA_FUNCTIONS`)
throughout every notebook.

### 3. `PL_RUN_ALL`'s 8-way ingestion fan-out has no per-client trim

*Severity: low*

Every `EP_INGEST_<TYPE>` stage fires unconditionally on every run, even for connector types a given
client never uses — a Sql+File-only client still pays `ForEach` cold-start overhead for the other 6
types on every run, forever, with no config knob to disable an unused stage.

**Fix:** a config flag per connector type (`environments.yaml` or a new `config/connectors.yaml`)
controlling whether that `EP_INGEST_<TYPE>` stage is included when `PL_RUN_ALL` is generated/deployed.

---

## Testing & Developer Experience

*Carried forward from the in-chat Round 2 list, not yet written anywhere else — included here so
it's not lost.*

### 1. Zero automated tests exist

*Severity: high*

No automated tests anywhere in the framework. The two highest-priority bugs fixed last round (SCD2
fact duplication, `full_refresh` dropping history) were only findable by reading code line-by-line.
**Partially addressed this round**: `dim_customer_history` now gives the SCD2 path a live, runnable
regression check via the Fabric UI — but there's still no *local*, fast, CI-runnable test suite.

**Fix:** a local pytest suite against the Gold facade functions using a local Spark session, runnable
in seconds without a live Fabric deploy.

### 2. No local dev loop

*Severity: medium*

Every notebook change needs a full Fabric redeploy + live run to test — confirmed the hard way
repeatedly this session (every fix needed a live round-trip to verify). If the Gold facade were a
plain importable Python module with no Fabric-specific calls, it could be tested locally — the
prerequisite for #1 above.

### 3. No single bootstrap/quickstart script

*Severity: medium*

Onboarding means reading ~5 wiki pages and running manual `az`/`curl` commands by hand. A
`bootstrap.py` walking a consultant through capacity check → WIF/SPN setup → first deploy would
substantially shorten this.

### 4. Inconsistent friendly-error handling outside the deploy path

*Severity: low*

`build_nb_deploy.py` now prints real tracebacks (Improvement-Roadmap Deployment #3); other
operational notebooks (`NB_RUN_REMOTE_PIPELINE`, etc.) likely still surface raw Fabric API errors.

---

*Still open from round 1, not superseded by anything here:* `metadata_connection_guid`
auto-creation (Deployment #6 — see §Platform Features 1 above for a possibly-better native
replacement), WIF wiring into the pipeline (CI/CD #1, deferred by explicit request — see also the
Key Vault-backed variable group preference noted separately), true per-item Fabric `ForEach` failure
isolation (Ingestion #3), Reconcile's unbounded full-table-scan cost (Bronze #5), and `%run`-chaining
scale past a handful of Gold tables (Silver & Gold #5).
