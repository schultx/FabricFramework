# Improvement Roadmap

A full-solution review — deployment, CI/CD, ingestion, and the notebooks through every medallion
layer — read through one lens: **what makes this framework harder to stand up for a new client,
faster, or more likely to produce wrong data once it's live.** Grounded in live testing against
development, test, and production in the real tenant, plus a line-by-line read of every area's
code. Severity reflects how much a given issue would actually bite a consultant or a client, not
abstract code quality.

## Priority order

If you can only do a handful of these next, do them in this order — each one either bit us
directly while building this framework, or is a silent-wrong-data bug waiting for the first real
client to trigger it:

1. [§Gold 2 — `full_refresh` silently drops history, the Unknown member, and reassigns every surrogate key](#2-full_refresh-on-an-existing-dimension-silently-reassigns-every-surrogate-key-and-for-scd2-drops-history)
2. [§Gold 1 — fact tables silently duplicate against any SCD2 dimension with history](#1-foreign-key-auto-mapping-joins-all-scd2-history-not-just-the-current-row)
3. [§Ingestion 5 — no CHECK constraints on the metadata schema's enum/conditional columns](#5-no-check-constraints-on-enumerated-or-conditionally-required-columns)
4. [§CI/CD 1 — WIF was set up but never wired into the pipeline; a plain secret is still live](#1-wif-service-connections-exist-but-the-pipeline-still-authenticates-with-a-stored-client-secret)
5. [§Deployment 1 — the framework name is hardcoded, not configurable](#1-the-framework-name-is-hardcoded-not-configurable)
6. [§Deployment 3 — deploy failures surface as an opaque, generic error](#3-deploy-failures-surface-as-an-opaque-generic-error)
7. [§Bronze 1 — the `dedupe_keep_latest` cleansing rule is silently neutralized](#1-dedupe_keep_latest-cleansing-rule-is-neutralized-by-an-earlier-blind-dedupe)

---

## Deployment

### 1. The framework name is hardcoded, not configurable

*Severity: high*

`"Monza Data ({env['short']})"` and its siblings are literal f-strings in
[setup/build_nb_deploy.py](../../setup/build_nb_deploy.py) (search for `"Monza`). There is no
config field for a client/engagement name. Today, onboarding a new client means editing generator
*code*, not config.

**Fix:** add a `framework_name` (or `client_name`) field to
[config/environments.yaml](../../config/environments.yaml) and thread it through every naming
f-string — see [New-Client-Onboarding](New-Client-Onboarding.md) step 2, which currently has to
tell the reader to hand-edit code because this doesn't exist yet.

### 2. `workspace_roles` role assignment has no idempotency guard

*Severity: high*

Every other mutating call in `build_nb_deploy.py` — `get_or_create_workspace`,
`get_or_create_lakehouse`, `get_or_create_folder`, `get_or_create_item`,
`get_or_create_sql_database` — lists existing objects first and only creates on a miss. The
`workspaceRoleAssignments` POST inside `get_or_create_workspace()` breaks that pattern: it fires
unconditionally for every entry in `workspace_roles[tier]`, every deploy, with no prior check for
whether the principal already holds that role. Fabric's role-assignment endpoint is additive, not
an upsert, so re-running it against an already-granted principal is very likely to 4xx.

Because `workspace_roles` ships empty in this repo's own committed `environments.yaml`, this path
has never actually been exercised by any of this session's live testing — it only bites the
moment a real client engagement does exactly what
[New-Client-Onboarding](New-Client-Onboarding.md) tells every consultant to do: fill in
`workspace_roles` to grant the client's own Entra users/groups access before handoff. The very
first redeploy after that config is filled in fails on Phase 1, for every workspace.

**Fix:** `GET /workspaces/{workspace_id}/roleAssignments` first, build a set of
`(principalId, role)` already present, and only POST entries not already satisfied — same
check-then-create shape as everything else in this file. Document in
[Deployment-Guide](Deployment-Guide.md) that `workspace_roles` is applied on every deploy.

### 3. Deploy failures surface as an opaque, generic error

*Severity: high*

`assignToCapacity` runs unconditionally on every deploy — even for workspaces already correctly
assigned — resolved by capacity **name** with zero tolerance for that capacity being paused,
renamed, or replaced. When anything fails, Fabric wraps it in the same generic message:
`"System cancelled the Spark session due to statement execution failures"`. This gave zero signal
about the real cause for every failure mode actually hit while building this: a stale capacity
name, a cold SQL database, and (per this review) real code bugs — all looked identical from the
outside, and diagnosing the capacity-name one cost hours of live reproduction.

**Fix, three parts:**
- Wrap each deploy phase in `try`/`except` that prints the real Python exception to the
  notebook's own cell output before re-raising.
- Skip `assignToCapacity` when the workspace is already on the target capacity.
- Add a validation pass right after loading `environments.yaml`/`items.yaml`/`lakehouses.yaml`
  that asserts every required key is present (`name`/`short`/`capacity`/`include_silver` per
  environment, `name`/`type`/`path` per item) and raises a clear, config-path-specific error
  *before* any Fabric API call runs — today a missing key throws a bare `KeyError` mid-loop,
  after earlier environments in the same run may already have been mutated.

### 4. `api()`'s retry wrapper retries 5xx but not 429

*Severity: high*

`build_nb_deploy.py`'s `api()` helper retries server errors and connection exceptions, but a 429
(Too Many Requests) — a realistic outcome given this function fires dozens of times back-to-back
across up to 3 environments in one `NB_DEPLOY` run — aborts immediately with no retry and no
honoring of `Retry-After`. This is a distinct rate limit from the already-known capacity
throttling (§Operations, `TooManyRequestsForCapacity` during a *pipeline run*) — this is Fabric's
management-plane API being rate-limited during *deploy itself*.

**Fix:** extend the retry condition to include 429, and sleep for the response's `Retry-After`
header when present instead of (or in addition to) the fixed backoff.

### 5. An `items.yaml` entry with a mistyped `type` is silently dropped

*Severity: medium*

Phases 5–7 each select items by exact string match (`if item["type"] != "Notebook": continue`,
etc.) with no step anywhere that verifies every entry in `items.yaml` was actually claimed by one
of the three phases. A typo (`Notebok`, wrong casing) means that item is quietly never deployed —
the run still prints "Deployment complete." This is exactly the failure mode a consultant hits
following [DEPLOYMENT.md](../../DEPLOYMENT.md)'s own "adding a real data source" step.

**Fix:** validate `item["type"]` against the known set up front and raise on an unrecognized
value; track which item names each phase actually processes and assert that set equals the full
`items.yaml` name list before the final success message.

### 6. `metadata_connection_guid` is a manual, easy-to-forget onboarding step

*Severity: medium*

Every environment needs a human to register a Fabric Connection in the portal and paste its GUID
into git before ingestion Lookups work at all. It's currently blank in all three environments of
this very project. See [Deployment-Guide](Deployment-Guide.md) for why it exists (a genuine
chicken-and-egg case).

**Fix candidate (unverified):** Fabric's REST API supports creating Connections programmatically
(`POST /v1/connections`). Worth investigating whether `NB_DEPLOY` can create this Connection
itself using the same service-principal credentials used elsewhere, removing the manual step
entirely.

### 7. Transient SQL warm-up failures require a human to notice and retry

*Severity: medium*

The classic Azure SQL "database not currently available, retry" condition (error 40613) hit on
the first deploy attempt against a freshly created or long-idle `SQL_METADATA_DATABASE` in
**every single environment tested this session**. Always resolved on a plain retry — a
well-understood, repeating pattern, not bad luck.

**Fix:** build a retry loop directly into `run_metadata_schema()` (3 attempts, ~10s apart) so a
cold database self-heals without a human watching the job.

### 8. `poll_lro()` polls forever with no progress output

*Severity: low*

No maximum iteration count and nothing printed per poll — a genuinely stuck long-running
operation is indistinguishable from a live one until the outer CI job's 1-hour timeout kills it.

**Fix:** print a short progress line each poll, and add a maximum elapsed-time bound that raises
a clear, named `TimeoutError` instead of relying on the outer job timeout as the only backstop.

### 9. `NB_LIST_LANDING_ENTITIES` is dead weight

*Severity: low*

Unused, disconnected from every pipeline, and cannot even be deleted via the Fabric API — fails
consistently with `400 UnknownError` in every environment tested. Accumulates as a permanent
orphan in every future client environment.

**Fix:** remove it from `src/` and [config/items.yaml](../../config/items.yaml) entirely.

### 10. The deploy notebook's generator only just started living in git

*Severity: resolved*

`setup/build_nb_deploy.py` — the entire source of truth for `NB_DEPLOY.ipynb` — existed only in a
local scratchpad outside version control for most of this project's build-out. Fixed during this
review, now committed at [setup/build_nb_deploy.py](../../setup/build_nb_deploy.py) — flagged
here so it isn't reintroduced. Regenerate with `python setup/build_nb_deploy.py` from the repo
root after any change, and commit the regenerated `.ipynb` alongside.

---

## CI/CD

### 1. WIF service connections exist but the pipeline still authenticates with a stored client secret

*Severity: high*

[Deployment-Guide](Deployment-Guide.md) documents three ADO service connections
(`fabricframework-dev/test/prod-wif`) using Workload Identity Federation specifically so no client
secret has to be stored — but `azure-pipelines.yml` never references any service connection at
all, and `deploy/run_notebook.py`'s token acquisition still does classic secret-based SPN auth
via `FABRIC_CLIENT_SECRET` from the shared `fabric-framework-secrets` variable group, across all
three stages. The migration was scoped and the connections were provisioned, but never wired into
the pipeline that actually runs deploys — exactly the single-shared-secret blast-radius problem
WIF was meant to retire is still live today.

**Fix:** give each stage's job an actual `serviceConnection: fabricframework-<env>-wif`, swap
`run_notebook.py`'s MSAL client-secret flow for a federated-token flow, then delete
`FABRIC_CLIENT_SECRET` from the variable group so the secret is actually retired, not just
unused.

### 2. No CI validation runs before the Dev stage performs a live Fabric deploy

*Severity: medium*

The pipeline goes straight from checkout to `pip install` to a live `run_notebook.py` call
against real infrastructure — no Python syntax check, no YAML schema check on
`config/*.yaml`, no confirmation `NB_DEPLOY.ipynb` actually matches what `build_nb_deploy.py`
would currently generate. A trivial mistake is only caught after burning real deploy time (each
attempt can run up to an hour, up to 5 attempts).

**Fix:** add a `Validate` stage before `Dev` requiring no Fabric credentials — syntax/lint checks
over `deploy/*.py` and `setup/*.py`, a YAML load/schema check of `config/*.yaml`, and ideally a
check that regenerating `NB_DEPLOY.ipynb` produces no diff against what's committed.

### 3. No `pr: none` guard — opening a PR also fires a live Dev deploy

*Severity: medium*

`azure-pipelines.yml` only sets `trigger: branches: [main]` with no `pr:` block. ADO's documented
default for an omitted `pr:` is an implicit trigger on *any* pull request. Since Dev's deployment
job has no required reviewer, opening a PR silently triggers a real, unattended `NB_DEPLOY` run
against live Dev infrastructure — surprising behavior for anyone who just wanted code review.

**Fix:** add `pr: none` (or a separate, lightweight PR-validation pipeline).

### 4. `run_notebook.py`'s retry logic doesn't cover transient errors during polling

*Severity: medium*

The `MAX_DEPLOY_ATTEMPTS = 5` retry loop wraps only the top-level trigger-and-poll cycle on an
explicit job-`Failed` status. But `poll_lro()` and the status-poll loop both call
`raise_for_status()` with no try/except of their own — a transient network blip or momentary
429/503 during the up-to-one-hour polling window crashes the whole script immediately, bypassing
the retry loop entirely. `set_environment_parameter()` is called once, *before* the retry loop
even starts, so a transient failure there gets zero retries at all.

**Fix:** wrap the HTTP calls in both poll loops and in `set_environment_parameter` with a small
retry/backoff for transient status codes, and/or move `set_environment_parameter` inside the
retry loop.

### 5. Nothing records which commit actually got deployed to each environment

*Severity: medium*

`run_notebook.py` never references `$(Build.SourceVersion)`, and `NB_DEPLOY` always re-downloads
`main` fresh on every run. Combined with Test/Prod approval gates that can sit for days, the code
that's actually live when an approval is finally granted can be a later commit than the one that
triggered the run being approved — with nothing anywhere recording which commit SHA is live in
which environment.

**Fix:** pass `$(Build.SourceVersion)` into `run_notebook.py` and log it (or write it into the
`NB_DEPLOY` parameters cell) so each Fabric job run is traceable back to a commit.

### 6. No failure notification or rollback mechanism, including on Prod

*Severity: medium*

Every failure path in `run_notebook.py` just calls `sys.exit(...)`. No notification step
(email/Teams/Slack), no `on: failure:` hook, no automated rollback. A failed Prod run after all 5
retry attempts just leaves the pipeline red in ADO with no alert to anyone.

**Fix:** add a failure-notification step (standard ADO Teams/Slack/email task) at least on
Test/Prod, and document in [Operations-Guide](Operations-Guide.md) what state a failed run leaves
an environment in.

### 7. Dev/Test/Prod stages are 100% duplicated YAML

*Severity: low*

The same four steps and five-variable `env:` block are copy-pasted verbatim three times, differing
only in the `--environment` value. Any pipeline-wide fix (including the WIF migration above) has
to be hand-applied and kept in sync across all three copies.

**Fix:** extract into a parameterized template (`templates/deploy-stage.yml`) referenced from all
three stages.

### 8. `deploy/requirements.txt` has no pinned versions

*Severity: low*

Open-ended lower bounds only (`requests>=2.31`, `msal>=1.28`), no lockfile. Since Test/Prod
approvals can sit for days, a newer release published between the Dev run and the Prod approval
installs for Prod without having been exercised in Dev at all.

**Fix:** pin exact versions or generate a lockfile.

---

## Ingestion

### 1. Watermark value is spliced into generated SQL unescaped

*Severity: high*

In `vw_ActiveIngestTables`'s Delta branch, `SourceSchema`/`SourceObject`/`IncrementalColumn` are
correctly `QUOTENAME()`'d as identifiers, but `runtime.LoadWatermark.LastValue` (plain
`NVARCHAR(100)`, no format constraint) is concatenated directly into a quoted string literal with
zero escaping of embedded quotes. Any watermark value containing an apostrophe — a manually
corrected watermark, a future non-idempotent writer, a source value that isn't a clean
date/number — terminates the literal early, and the remainder executes as raw SQL against the
live source system via the Copy activity's dynamic query.

**Fix:** escape embedded quotes before splicing (`REPLACE(w.[LastValue], '''', '''''')`), and add
a `CHECK`/format constraint on `LoadWatermark.LastValue` so only date/numeric-shaped text can ever
be written there.

### 2. No CHECK constraints on enumerated or conditionally-required columns

*Severity: high*

`ConnectionType`, `LoadType`, and `DeleteHandling` each have a fixed, documented value set in
*comments only* — no actual `CHECK` constraint. A typo during bulk metadata seeding (`SqlServer`
instead of `Sql`) inserts cleanly, then simply never matches any `PL_INGEST_*`'s filter — the
table silently never lands, with no error anywhere. Separately, `IncrementalColumn` ("required
when LoadType = Delta") and `IsDeletedColumn` ("required when DeleteHandling = SoftDelete") have
no enforcing constraint either — a Delta row saved with `IncrementalColumn NULL` collapses
`QUOTENAME(NULL)` to `NULL`, which collapses the entire `ResolvedSourceQuery` to `NULL`.

**Fix:** add `CHECK` constraints for all four enumerated columns (including `FileType`), plus
`CHECK (LoadType <> 'Delta' OR IncrementalColumn IS NOT NULL)` and the equivalent for
`SoftDelete`/`IsDeletedColumn` — reject bad onboarding data at `INSERT` time, not at pipeline
runtime.

### 3. `ForEach` has no per-item failure isolation

*Severity: high*

Every `PL_INGEST_*` pipeline's `ForEach` wraps a single Copy activity with no on-failure branch
and no explicit `isSequential`/`batchCount`. One table failing for a routine reason (a grant not
yet applied during onboarding, a locked table, a drifted column) fails the whole `ForEach` — and
because there's no isolation, the entire pipeline run reports Failed even if every other table's
Copy already succeeded, masking every other table's actual pass/fail status.

**Fix:** give each Copy an explicit failure path so one bad table degrades gracefully, and add a
post-run reconciliation (compare `vw_ActiveIngestTables` against what actually landed) so partial
failures are visible per-table.

### 4. `FileType` is configured on every table but never actually read by three of the four pipelines

*Severity: medium*

`ingestion.Table.FileType` reads as a per-table configurable landing format, but `PL_INGEST_SQL`,
`PL_INGEST_ORACLE`, and `PL_INGEST_SQLMI` all hardcode their Copy sink to `ParquetSink` with a
hardcoded `.parquet` extension — none reference `@item().FileType` anywhere. Setting `FileType` to
anything else on a Sql/Oracle/SqlMI table is silently ignored.

**Fix:** either wire the sink type/extension to `@item().FileType`, or drop the column's
"configurable" framing and document that non-File sources always land as parquet.

### 5. Copy/Lookup activity timeout is shorter than its own query timeout

*Severity: medium*

Every Lookup and Copy activity across all four pipelines sets `policy.timeout` to 1 hour while
`source.queryTimeout` is set to 2 hours — the outer activity timeout governs the whole activity,
so the 2-hour value is unreachable. This bites hardest on exactly the framework's core use case:
an initial `LoadType='Full'` historical copy of a large client table, which fails at the 1-hour
mark looking like an infra problem rather than a self-inflicted config mismatch.

**Fix:** make `policy.timeout >= source.queryTimeout` consistently across all four pipelines.

### 6. Hardcoded `'1900-01-01'` watermark seed assumes a datetime incremental column

*Severity: medium*

The first-run watermark seed (`ISNULL(w.[LastValue], '1900-01-01')`) assumes a datetime-shaped
`IncrementalColumn`. A client whose incremental column is numeric (a change-tracking sequence, a
version counter) generates `WHERE [ChangeSeq] > '1900-01-01'` on first run, which fails
source-side type conversion with no hint the real cause is this hardcoded default.

**Fix:** track the incremental column's data type on `ingestion.Table` and pick a type-appropriate
seed per type, or require a seed watermark row to be inserted as part of onboarding every Delta
table.

### 7. `runtime.LoadWatermark` has no uniqueness constraint on `(EntityType, EntityId)`

*Severity: medium*

Only a surrogate `WatermarkId IDENTITY` key exists — no `UNIQUE` on the pair
`vw_ActiveIngestTables` actually joins on. A stray duplicate row (a manual troubleshooting insert,
any future writer that inserts instead of upserts) makes the view's `LEFT JOIN` fan out, handing
the `ForEach` duplicate items for the same table — copied to Landing multiple times per run,
possibly with conflicting watermark filters.

**Fix:** add a `UNIQUE` constraint/index on `(EntityType, EntityId)`, and make the view defensive
regardless (`TOP 1 ... ORDER BY LastRunUtc DESC`) so a stray duplicate can't silently multiply
ingestion.

### 8. No audit columns on the ingestion control tables themselves

*Severity: low*

`ingestion.Connection`/`Database`/`Table` — the entire control surface for what gets ingested for
a client — have no `CreatedUtc`/`ModifiedUtc`/`ModifiedBy`, only `IsActive`. No way to answer "who
changed this table's `LoadType`, and when" from the metadata database itself.

**Fix:** add `CreatedUtc`/`ModifiedUtc DATETIME2 DEFAULT SYSUTCDATETIME()` (and ideally
`ModifiedBy`) to all three tables.

### 9. No indexes on FK/lookup columns joined every pipeline run

*Severity: low*

`Database.ConnectionId`, `Table.DatabaseId`, and `LoadWatermark`'s lookup pair have no supporting
nonclustered index. Low impact at today's likely table counts, but free to fix now.

**Fix:** add nonclustered indexes on both FK columns, plus the `UNIQUE` index on `LoadWatermark`
noted above.

---

## Bronze

### 1. `dedupe_keep_latest` cleansing rule is neutralized by an earlier blind dedupe

*Severity: high*

`NB_LOAD_BRONZE` runs `clean_df.dropDuplicates(primary_keys)` **before** the `CleansingRules` loop
that implements `dedupe_keep_latest` (an ordered `Window`-based rule choosing the row with the
latest `orderBy` value). Because the blind dedupe already collapses the batch to one
(arbitrary, order-undefined) row per key first, `dedupe_keep_latest` has nothing left to do — a
table configured with this rule for a source landing multiple versions of the same key per batch
(a common CDC/full-extract pattern) silently keeps whichever row Spark's hash-based dedupe
happened to pick, not the latest one, with no error. `dropDuplicates` without explicit ordering is
documented as non-deterministic across plans/retries, so the surviving row can even change between
runs of the identical batch.

**Fix:** only apply the blind `dropDuplicates(primary_keys)` when no `dedupe_keep_latest` rule is
configured for that entity; when one is present, let the ordered window-function rule be the sole
source of truth for which row survives.

### 2. An empty Full-load source file silently truncates the existing Bronze table

*Severity: high*

`LoadType='Full'` always does an unconditional `mode("overwrite")` with no row-count check. If a
day's landing file is empty (an upstream job that runs but produces zero rows, a truncated file,
a source-side connectivity blip) this overwrites a previously healthy Bronze table with 0 rows —
and since no exception is raised, the run reports `Succeeded` and this data-loss event is
completely invisible to the audit trail.

**Fix:** before the Full-mode overwrite, check for an empty (or suspiciously small) result while a
target table already exists with rows, and raise instead of silently truncating — caught by the
existing per-entity try/except so it shows up in the run's failure list. Make it configurable
per-table for the legitimate cases where an empty Full extract is expected.

### 3. Delta MERGE has no recency check against the incremental column

*Severity: medium*

The Delta upsert merges on primary-key equality only and unconditionally overwrites every matched
column with the incoming value — no comparison against `IncrementalColumn`. A resent/out-of-order
batch (a watermark reset, a backfill, a source-side retry re-emitting a stale snapshot) silently
overwrites Bronze's current, newer values with older incoming ones.

**Fix:** add a recency guard to the matched-update clause, e.g.
`whenMatchedUpdate(condition="source.<col> >= target.<col>", ...)` when `IncrementalColumn` is
set.

### 4. `clean_df` is never cached, so Delta entities are read and transformed 2–4x per run

*Severity: medium*

The same lazily-built `clean_df` is consumed by the merge, a `.count()` used only for a log line,
and the watermark's `.agg(...).collect()` — each re-executing the full lineage from the source
read (including CSV `inferSchema=true`, itself a full extra pass). Unnoticeable on demo-sized
data; multiplies I/O/compute 3–4x on every Bronze run for every Delta entity at real client scale.

**Fix:** `.cache()` `clean_df` after cleansing and before it's used by the merge/count/watermark
steps; `unpersist()` at the end of each entity iteration.

### 5. Reconcile does a full unbounded scan of both tables on every run

*Severity: medium*

`_reconcile_deletes` reads every primary key out of the *entire* Bronze table and issues an
unfiltered `SELECT` against the *entire* source table, every single run — O(target + source) work
that grows unbounded as Bronze accumulates history, on every scheduled run forever. Separately,
because the watermark commits *before* Reconcile runs, a Reconcile-only failure (a transient
source-connection hiccup) still marks the whole entity as failed even though the merge and
watermark already succeeded.

**Fix:** make Reconcile frequency a separate, coarser config knob from the regular merge cadence
(e.g. nightly, not every incremental run), or maintain a persisted key inventory instead of a live
full-table scan each time. Log Reconcile-step failures distinctly from load failures.

### 6. Schema-drift handling is inconsistent and undocumented between Full and Delta

*Severity: low*

Full-load tables silently accept any schema drift (`overwriteSchema=true` resets the table's
schema every run with no warning). Delta-load tables set no schema-merge option at all, so a new
source column hits a hard Delta MERGE schema-mismatch error that then fails identically on every
subsequent run until someone manually intervenes. The same class of change is silent on one load
type and a hard, repeating failure on the other.

**Fix:** pick one explicit policy per load type — enable Delta schema evolution for Delta merges
(or detect and log the mismatch clearly before failing), and log a schema diff before Full's
`overwriteSchema` write so drift is visible in the audit trail instead of silent.

---

## Silver & Gold

### 1. Foreign-key auto-mapping joins all SCD2 history, not just the current row

*Severity: high*

`_discover_and_map_foreign_keys` never filters on `IS_CURRENT_COL` when joining a fact's `_key`
column against a dimension table. Harmless for SCD1 (one row per business key) — but
`write_dimension_type2` deliberately keeps multiple physical rows per business key (an expired
row plus a current row, each with a *different* surrogate key). A fact joined against any
dimension with real history matches **both** rows and is silently duplicated in the output — one
copy per historical version. The shipped demo can't catch this: `dim_customer` hardcodes
`dimension_type = 'scd1'`, so the bug is invisible until a real client dimension that actually
needs history (the entire point of offering SCD2) is joined by a fact.

**Fix:** filter the dimension read to the current row before joining
(`.filter(F.col(IS_CURRENT_COL) != False)` when the column exists). Add a demo/test case with an
actual SCD2 dimension carrying 2+ versions of one member joined by a fact, since the current
structure structurally cannot catch this class of bug.

### 2. `full_refresh` on an existing dimension silently reassigns every surrogate key, and for SCD2 drops history

*Severity: high*

`full_refresh=True` on a table that already exists still routes through `_generate_surrogate_key`'s
"existing table" branch, which assigns **every** row — including members that already existed — a
brand-new surrogate key, discarding the old one. The subsequent overwrite write replaces the whole
table with these freshly-keyed rows, orphaning every fact table's existing foreign keys.
`_create_unknown_record` also only fires when the table doesn't already exist, so `full_refresh`
on an existing table never re-adds the `-1` Unknown row even though the overwrite just deleted it.
`write_dimension_type2` has the identical structure, so `full_refresh` there additionally discards
all `valid_from`/`valid_to`/`is_current` history — "track history" mode silently loses history the
moment `full_refresh` is used on a pre-existing table. Note `recreate_table=True` (a *different*
flag) does **not** have this bug — it correctly starts keys clean at 1 and recreates the Unknown
row — which makes `full_refresh`'s behavior look like an oversight, since its own docstring
describes it as simply "drop and rewrite all rows instead of merging."

**Fix:** either raise a clear error if `full_refresh=True` is requested against an existing table
without `recreate_table=True`, or fix the logic to preserve each existing row's surrogate key
(left-join back onto the existing table by business key first, only minting new keys for genuinely
new members) and always re-append the Unknown record / merge rather than blind-overwrite for SCD2
so history survives.

### 3. `_generate_surrogate_key` relies on `monotonically_increasing_id()`, which only looks clean on single-partition demo data

*Severity: medium*

Spark's documented semantics for this function: values pack the partition index into the upper
bits, unique and increasing but not contiguous. The demo's tiny single-file CSV likely lands in
one partition, rendering small consecutive-looking integers and masking the real behavior. At real
client volumes (multiple files/partitions), values from partition 1 alone start around 8.59
billion, compounding further on every incremental run. If any downstream layer (a Warehouse table,
a semantic-model key) later narrows this to `int32` — a common compact-key modeling choice — two
unrelated rows can silently collide onto the same key.

**Fix:** replace with a deterministic dense sequence
(`F.row_number().over(Window.orderBy(<stable key>)) + max_sk`) or a Delta
`GENERATED ALWAYS AS IDENTITY` column, and enforce `bigint` everywhere downstream.

### 4. Rows falling back to the Unknown member are never counted or logged

*Severity: medium*

When a dimension table doesn't exist at all, `_discover_and_map_foreign_keys` prints an explicit
warning. But when the table exists and a specific business-key *value* simply has no match — the
far more common real-world case — the code silently coalesces to the Unknown key with zero
logging of how many rows fell back or which keys were unmatched. A run closes its audit row as
`Succeeded` even if a large fraction of a client's fact rows silently landed on `customer_sk=-1`
because of a late-arriving dimension row or a data-quality gap.

**Fix:** log a count (and ideally a sample of offending keys) for every `_key` column that falls
back to Unknown, printed alongside the existing "no dimension table found" warning.

### 5. The `%run`-per-table chaining pattern has no parallelism, no ordering validation, and a shared global namespace

*Severity: medium*

`NB_LOAD_GOLD`'s own docstring is explicit that Gold is "deliberately NOT metadata-loop-driven" —
every object gets its own hand-written notebook, `%run`-chained together. For a real client's
15-table Gold layer, this becomes 15 fully serial `%run` cells sharing one global Python
namespace — `dim_customer` and `fact_signup` both declare identically-named top-level variables
(`destination_lakehouse`, `recreate_table`), so a future notebook copy-pasted from these templates
that forgets to redeclare one silently inherits whatever the previous `%run` left in scope. There's
also no automated check that a fact's dimensions ran first — getting the order wrong doesn't
error, it just means every row falls back to Unknown (compounding the logging gap above), and
independent dimensions can't run concurrently, so total runtime is additive across every table
with no parallelism.

**Fix:** past a handful of Gold tables, move to a Fabric pipeline with explicit activity-level
dependencies (parallel branches for independent dimensions) instead of one growing `%run` chain.
At minimum, add a lightweight per-fact-notebook assertion that its required `gold.dim_*` tables
already exist, raising instead of silently coalescing to Unknown.

### 6. `load_dimension()` has no way to specify primary keys explicitly

*Severity: medium*

Primary-key inference is 100% dependent on a `_key` column-name suffix convention, with no
`key_columns` parameter on `load_dimension()`'s signature at all (unlike `load_fact()`, which does
expose one). If a client's source keeps its native key column name instead of renaming it to fit
the convention, the inferred primary-key list comes back empty, and that empty list produces a raw
Spark parse exception when handed to Delta's `.merge()` — not an actionable framework-level error.

**Fix:** expose an explicit `key_columns` parameter on `load_dimension()`/`write_dimension_type1`/
`write_dimension_type2` mirroring `load_fact()`'s, and raise a clear error upfront when no primary
keys can be determined.

### 7. No shared Silver-layer write helper — every Silver notebook hand-rolls its own write and audit convention

*Severity: medium*

Gold has a rich facade (`write_dimension_type1/2`, `load_fact` with 5 write modes); Silver's only
shared helper is `_ensure_schema`. `sil_customer` writes with a bare `saveAsTable(...)` and invents
its own audit column (`silver_loaded_datetime`) instead of reusing `_append_audit_timestamps()` —
so Silver and Gold tables in the same lakehouse end up with divergent audit-column names and
semantics, and every new Silver table means hand-rolling incremental/merge logic from scratch.

**Fix:** add a small Silver facade helper (e.g. `write_silver_table(...)`) reusing `_ensure_schema`
and `_append_audit_timestamps` so audit-column naming stays consistent and future Silver notebooks
aren't fully bespoke.

### 8. Redundant `.count()` after `saveAsTable()` in the Silver template

*Severity: low*

`sil_customer`'s `print(f"wrote {silver_df.count()} rows...")` after an uncached write forces a
full redundant re-scan purely to print a row count. Harmless on demo data, but `sil_customer` is
explicitly the copy-paste template for every new client Silver table, so this pattern propagates
to every table in a client's Silver layer, including large ones.

**Fix:** drop the `.count()` print, or pull the row count from the write's own Delta operation
metrics instead of re-executing the DataFrame.

---

*See also: [Incremental-Loading](Incremental-Loading.md) for how the Bronze Delta-merge and
Gold SCD logic referenced above fit together end to end, and
[Materialized-Lake-Views](Materialized-Lake-Views.md) for a forward-looking alternative to the
hand-written Gold facade for simple SCD1 dimensions.*
