# Materialized Lake Views

> **This is a forward-looking design showcase, not current-state documentation.** Nothing on
> this page exists in the repo today — there is no MLV defined anywhere in `src/`,
> `config/items.yaml`, or `setup/NB_DEPLOY.ipynb`, and no lakehouse in this project has one.
> Every other page in this wiki describes Monza "as it stands after live end-to-end testing"
> (see [Home](Home.md)'s "Ground truth" section); this page is the opposite of that on purpose —
> it's a concrete, worked comparison of Fabric's declarative Materialized Lake View (MLV)
> feature against Monza's actual hand-written Gold facade, written to inform a real build-vs-buy
> decision on a future engagement, not to describe something already built.

## Today's Gold approach, for grounding

Before proposing an alternative, here's exactly what exists. Monza's Gold layer is driven by
three functions in `src/NB_MONZA_FUNCTIONS.Notebook/notebook-content.py`, `%run` into every
per-table Gold notebook:

- **`write_dimension_type1()`** — SCD1 upsert. Generates a surrogate key
  (`{table}_sk`, `F.monotonically_increasing_id() + max_sk`), appends audit timestamps, builds an
  "Unknown" member row (`sk = -1`) via `_create_unknown_record()` on first creation, then either
  full-overwrites or runs a `DeltaTable.merge()` keyed on every `_key` business-key column.
- **`load_fact()`** — writes a fact table in one of five modes (`overwrite` / `append` / `upsert`
  / `incremental` / `replace_partition`), and — when `auto_map_foreign_keys=True` (the default) —
  calls **`_discover_and_map_foreign_keys()`** first.
- **`_discover_and_map_foreign_keys()`** — a pure convention scan, no config table behind it: for
  every column in the incoming DataFrame whose name ends in `_key` (and not `_sk`), it strips the
  suffix to get `<base>`, looks for a table named `dim_<base>` in the target schema, and — if
  found — `LEFT JOIN`s on `<base>_key`, pulls back `<base>_sk`, `COALESCE`s unmatched rows to
  `UNKNOWN_KEY_VALUE` (`-1`), and drops the original `_key` column. If no matching `dim_<base>`
  exists it just prints a warning and leaves the column alone.

The demo objects that exercise this are `src/dim_customer.Notebook/notebook-content.py` (SCD1,
sourced straight from `Bronze.dbo.customer`, no Silver hop — "this shape isn't reused anywhere
else") and `src/fact_signup.Notebook/notebook-content.py` (one row per signup, `write_mode =
'overwrite'`, `customer_key` auto-mapped to `customer_sk` against `gold.dim_customer`). Both are
plain `%%sql` cells building a `CREATE OR REPLACE TEMPORARY VIEW`, then a call into
`load_dimension()` / `load_fact()`. See [Architecture](Architecture.md) for where Gold sits in
the medallion, and [Metadata-Model](Metadata-Model.md) for how Bronze itself gets populated.

## What an MLV is, and why it's worth a look here

A **Materialized Lake View** is Fabric's declarative alternative to writing a notebook cell that
reads a DataFrame and calls `.saveAsTable()` or `DeltaTable.merge()`: you author
`CREATE MATERIALIZED LAKE VIEW <schema>.<name> AS SELECT ...` (in a Lakehouse-attached SQL
notebook cell, or through the MLV designer canvas in the Lakehouse explorer), and Fabric owns
turning that `SELECT` into a physical Delta table, tracking which other MLVs it depends on,
and refreshing it — on a schedule or on demand — in dependency order. Three things make it
interesting for Monza specifically: it replaces hand-written merge/orchestration code with a
single `SELECT` per hop; it tracks the dependency graph between MLVs automatically, so a Bronze
→ Silver → Gold chain shows up as a visual lineage graph in the Lakehouse explorer instead of
being implicit in which notebook `%run`s which; and it has data-quality gates built into the
`CREATE` statement itself — a `CONSTRAINT <name> EXPECT (<condition>) ON VIOLATION DROP ROW`
clause — instead of cleansing rules living as separate logic (Monza's own generic Bronze
cleansing today is driven by `ingestion.Table.CleansingRules`, per
[Metadata-Model](Metadata-Model.md)).

## Worked example: `dim_customer` as an MLV chain

The same shape as the real `dim_customer` demo, split into a cleansing hop and a shaping hop.

### Bronze → Silver: `silver.stg_customer`

Written in a notebook whose default lakehouse is `Silver`, reading `Bronze.dbo.customer` by its
three-part name. Note this needs none of the OneLake-path workaround `dim_customer` and
`fact_signup` currently use to read their *own* default lakehouse back — that workaround exists
specifically because `saveAsTable()`/`spark.read` on a notebook's own default lakehouse trips
Spark's `[REQUIRES_SINGLE_PART_NAMESPACE]` parser error (see
[Improvement-Roadmap](Improvement-Roadmap.md) #7); reading a *sibling* lakehouse by its full
`lakehouse.schema.table` name already works today, which is exactly what Phase 5 of `NB_DEPLOY`
turns on for every Gold/Silver notebook by binding a default lakehouse and enabling OneLake Spark
Catalog for the whole Data workspace (see [Architecture](Architecture.md)). An MLV's `SELECT`
would use that same three-part name, unmodified:

```sql
CREATE MATERIALIZED LAKE VIEW silver.stg_customer
(
    CONSTRAINT ck_customer_id_present EXPECT (CustomerId IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT ck_email_present       EXPECT (Email IS NOT NULL)      ON VIOLATION DROP ROW
)
COMMENT 'Deduplicated, quality-gated reshape of Bronze.dbo.customer -- one row per CustomerId'
AS
SELECT
    CustomerId,
    FirstName,
    LastName,
    Company,
    City,
    Country,
    Email,
    Website,
    CAST(SubscriptionDate AS DATE) AS SubscriptionDate
FROM Bronze.dbo.customer
QUALIFY ROW_NUMBER() OVER (PARTITION BY CustomerId ORDER BY SubscriptionDate DESC) = 1
```

Two constraints replace what would otherwise be ad-hoc `.filter()` calls, and the `QUALIFY`
window function does the deduplication that a hand-written notebook would do with
`Window.partitionBy(...).orderBy(...)` and a `row_number() == 1` filter. This is a genuinely
different table than the existing `sil_customer.Notebook` demo (`Silver.silver.customer`, a
plain 1:1 reshape with no dedup or constraints) — in practice you'd replace that notebook with
this MLV, not run both.

### Silver → Gold: `gold.dim_customer`

```sql
CREATE MATERIALIZED LAKE VIEW gold.dim_customer
COMMENT 'SCD1 customer dimension -- always reflects the latest row in silver.stg_customer, no history'
AS
SELECT
    CAST(CustomerId AS STRING) AS customer_key,
    FirstName,
    LastName,
    Company,
    City,
    Country,
    Email,
    Website,
    SubscriptionDate
FROM Silver.silver.stg_customer

UNION ALL

SELECT '-1', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', DATE'1900-01-01'
```

Read that last `SELECT` closely — it's a literal, hand-written Unknown row, `UNION ALL`ed in by
hand. That single line is the whole point of the section below: it's exactly what
`_create_unknown_record()` generates automatically today, for any schema, with zero per-table
code.

## Worked example: `fact_signup` as an MLV — explicit JOIN vs. runtime convention

This is the sharpest contrast with `NB_MONZA_FUNCTIONS`. Today, `fact_signup.Notebook` hands
`load_fact()` a DataFrame with a plain `customer_key` column and `auto_map_foreign_keys=True`;
`_discover_and_map_foreign_keys()` figures out at **runtime**, by scanning both the DataFrame's
columns and `spark.catalog.listTables()`, that `customer_key` should resolve against
`gold.dim_customer`. Nothing in `fact_signup.Notebook` itself names `dim_customer` — the
connection is entirely convention (`<base>_key` ↔ `dim_<base>`), which is exactly what lets the
same `load_fact()` function serve every fact table in every client build without per-table FK
code.

An MLV has no equivalent runtime convention scanner — a JOIN in an MLV's `SELECT` is exactly as
explicit as a JOIN in any other SQL, written once, per fact, by hand:

```sql
CREATE MATERIALIZED LAKE VIEW gold.fact_signup
COMMENT 'One row per customer signup; FK to dim_customer resolved by an explicit JOIN'
AS
SELECT
    COALESCE(d.customer_key, '-1') AS customer_key,
    CAST(s.SubscriptionDate AS DATE) AS SignupDate,
    s.Country
FROM Silver.silver.stg_customer AS s
LEFT JOIN gold.dim_customer AS d
    ON CAST(s.CustomerId AS STRING) = d.customer_key
```

Two things worth being explicit about, because they're easy to gloss over:

1. **This trades a generic runtime helper for per-table SQL.** `_discover_and_map_foreign_keys()`
   is one function that works against *any* dimension named by convention — add a new fact table
   tomorrow with a `product_key` column and it's auto-mapped for free, no code change. The MLV
   version means every new fact needs its own hand-written `JOIN ... ON`, in every fact MLV that
   references it. That's a real, recurring cost across a client build with a dozen facts, not a
   one-time cost.
2. **This still isn't a surrogate-key join, because `gold.dim_customer` above has no surrogate
   key.** `d.customer_key` is the same string business key on both sides — the `COALESCE(...,
   '-1')` mimics the *shape* of `_discover_and_map_foreign_keys()`'s Unknown-row fallback, but
   there's no `_sk` being resolved because none was ever generated. To get an actual `customer_sk`
   out of an MLV dimension, you'd need to bake a deterministic key expression (e.g.
   `xxhash64(CustomerId)`, not `monotonically_increasing_id()` or `ROW_NUMBER()`, which aren't
   stable across refreshes and would silently reassign keys as rows come and go) into
   `gold.dim_customer`'s `SELECT`, and then repeat that *exact* expression in every fact MLV that
   joins to it — one more thing to write and keep in sync by hand, per relationship, that
   `_generate_surrogate_key()` currently does once, centrally.

## Where MLVs genuinely fit in Monza — and where they don't

| | MLV | `NB_MONZA_FUNCTIONS` today |
|---|---|---|
| SCD1 "latest wins" dimension | Strong fit — this is exactly what `write_dimension_type1()`'s merge does, expressed as a `SELECT` instead | Works, but every table repeats the same merge/audit-column boilerplate the facade exists to hide |
| SCD2 history tracking | **No native fit** — no `VALID_FROM`/`VALID_TO`/`IS_CURRENT` merge semantics, no built-in "close out the old row, insert the new one" pattern; you'd be hand-rolling `write_dimension_type2()`'s conditional-update-then-append logic yourself, in SQL, inside a construct that isn't designed for it | `write_dimension_type2()` already does exactly this, in one call |
| Unknown member row (`sk = -1`) | Possible, but 100% hand-written per dimension (see the `UNION ALL` above) — no equivalent of `_create_unknown_record()` | Automatic, generic, zero per-table code |
| Surrogate key generation | Not built in — needs a hand-picked deterministic expression, repeated everywhere it's joined | Automatic (`_generate_surrogate_key()`), one place |
| Fact-to-dimension FK resolution | Explicit `JOIN`, written once per fact, in SQL you can read directly | Convention-driven (`_discover_and_map_foreign_keys()`), zero per-table code, but implicit |
| Straightforward, always-overwrite fact loads | Strong fit — `fact_signup`'s own `write_mode='overwrite'` is already conceptually what an MLV gives you by default | `load_fact(write_mode='overwrite')` already does this |
| Data-quality gating | Built in (`CONSTRAINT ... EXPECT ... ON VIOLATION DROP ROW`) | Not present in the Gold facade at all today — cleansing rules only exist at the Bronze layer, driven by `ingestion.Table.CleansingRules` |
| Lineage/dependency visibility | Automatic dependency graph, visual, in the Lakehouse explorer | Implicit in which notebook `%run`s `NB_MONZA_FUNCTIONS` and which order `NB_LOAD_GOLD` chains them |

**Recommendation: hybrid, not a replacement.** MLVs are a strong fit for the Bronze → Silver
cleansing hop generally (the `stg_customer` example above is a plausible drop-in for the
`sil_customer` pattern) and for genuinely SCD1, "always reflects the source" Gold dimensions —
`dim_customer` in this repo's own demo is a good example of exactly that shape. Keep the
`NB_MONZA_FUNCTIONS` facade for anything that needs SCD2 history tracking, or that leans on the
Unknown-member/surrogate-key conventions it already standardizes across a client build — that's
real, working, generic infrastructure that an MLV chain would otherwise have you reinventing by
hand, per table.

## Adoption cost and risk: pilot first, don't commit broadly

MLVs are a genuinely different execution *and deployment* model from "run this notebook," and
that difference is bigger than the SQL syntax suggests:

- Every deployable item in Monza today is either a `Notebook` or `DataPipeline` row in
  `config/items.yaml`, round-tripped through `get_or_create_item()` — base64 `notebook-content.py`
  on `POST /items`, `updateDefinition` on redeploy (see [Deployment-Guide](Deployment-Guide.md)).
  An MLV definition is neither. It's SQL DDL that has to be applied against a Lakehouse's own
  SQL/Spark context, and nothing in `setup/build_nb_deploy.py` or `NB_DEPLOY.ipynb` today knows
  how to create, update, or idempotently reapply that DDL across dev/test/prod. The closest
  existing analog is Phase 3's `run_metadata_schema()`, which batch-splits and applies
  `config/metadata_schema.sql` over `pyodbc` — but that's against a SQL Database, not a
  Lakehouse, and it isn't a pattern that's been proven to extend to MLV DDL.
- That means adopting MLVs isn't "add a config row" the way a new dimension notebook is today —
  it's a new deployment mechanism that would need to be designed, built, and tested from scratch,
  with its own story for versioning and rollback that doesn't exist yet anywhere in this repo.
- MLVs are also a comparatively new Fabric capability. Treat it the way
  [Architecture](Architecture.md) treats the Folder API ("still Preview... best-effort, not
  load-bearing") until it's been exercised live: verify refresh behavior (which query shapes
  actually qualify for incremental refresh vs. silently fall back to a full recompute) and
  cross-lakehouse dependency tracking (whether the automatic lineage graph spans a Silver-lakehouse
  MLV feeding a Gold-lakehouse MLV, or only tracks dependencies within one lakehouse) against a
  real tenant before relying on either.

**Concretely:** pilot this on one non-critical dimension on a real client engagement — something
with `dim_customer`'s own shape (SCD1, no downstream SCD2 consumers, tolerant of a full
recompute) — before proposing it as a default part of the Monza template. Don't fold it into
`config/items.yaml` or the standard onboarding checklist until that pilot has actually exercised
the deployment and refresh story end to end.

## See also

- [Architecture](Architecture.md) — the medallion lakehouse layout and OneLake Spark Catalog
  binding this page's cross-lakehouse `SELECT`s depend on.
- [Metadata-Model](Metadata-Model.md) — how `Bronze.dbo.customer` itself gets populated and
  cleansed today.
- [Incremental-Loading](Incremental-Loading.md) — Monza's watermark-based incremental pattern at
  Bronze, for contrast with an MLV's own scheduled/triggered refresh model.
- [Deployment-Guide](Deployment-Guide.md) — why `NB_DEPLOY`'s item model doesn't cover MLVs today.
- [Improvement-Roadmap](Improvement-Roadmap.md) — where this kind of change would need to be
  weighed against the framework's other open gaps.
