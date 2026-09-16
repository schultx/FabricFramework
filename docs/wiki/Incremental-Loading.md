# Incremental Loading

[Metadata-Model](Metadata-Model.md) documents the `ingestion.Table` schema field by field, and [Architecture](Architecture.md) documents the workspace/lakehouse topology. This page is neither — it's the single story of one Delta-load table's journey from Source to Gold, told as one continuous run-through, so a consultant who's never touched this repo can answer "how does incremental actually work here" without stitching the answer together from three other pages.

## The table we're following

Say a client has an Azure SQL `dbo.Orders` table with an always-increasing `ModifiedDateUtc` column, and you've registered it like this in `ingestion.Table` (see [Metadata-Model](Metadata-Model.md) for the full column reference):

| Column | Value |
|---|---|
| `TableId` | `42` |
| `SourceSchema` / `SourceObject` | `dbo` / `Orders` |
| `FilePath` | `dbo/Orders` |
| `BronzeSchema` / `BronzeName` | `dbo` / `orders` |
| `PrimaryKeys` | `OrderId` |
| `LoadType` | `Delta` |
| `IncrementalColumn` | `ModifiedDateUtc` |
| `DeleteHandling` | `None` to start (we'll flip it later) |

Everything below follows this one row through `PL_INGEST_SQL`, `NB_LOAD_BRONZE`, and finally a Gold notebook.

## Hop 1: Source → Landing, via a watermark-bounded query

`PL_INGEST_SQL`'s first activity, `LK_ACTIVE_SQL_TABLES`, is a plain `Lookup` whose `sqlReaderQuery` is:

```sql
SELECT * FROM [ingestion].[vw_ActiveIngestTables] WHERE [ConnectionType] = 'Sql'
```

That view (`config/metadata_schema.sql`) is where the watermark actually gets applied — it's pure T-SQL, no notebook involved. For every active `Table` row it computes a base query (either the row's `SourceQuery` override, or `SELECT * FROM [SourceSchema].[SourceObject]`), then, only when `LoadType = 'Delta'`, wraps it again:

```sql
LEFT JOIN [runtime].[LoadWatermark] w ON w.[EntityType] = 'Table' AND w.[EntityId] = t.[TableId]
...
'SELECT * FROM (' + base.[BaseQuery] + ') AS w WHERE ' + QUOTENAME(t.[IncrementalColumn])
    + ' > ''' + ISNULL(w.[LastValue], '1900-01-01') + ''''
```

For `TableId = 42` on its very first run (no `runtime.LoadWatermark` row exists yet, so the `LEFT JOIN` produces `NULL` and `ISNULL(...)` falls back to `'1900-01-01'`), `ResolvedSourceQuery` comes back as:

```sql
SELECT * FROM (SELECT * FROM [dbo].[Orders]) AS w WHERE [ModifiedDateUtc] > '1900-01-01'
```

— effectively a full pull, because there's nothing to bound it against yet. On every run after that, once a watermark exists, the same row instead resolves to something like:

```sql
SELECT * FROM (SELECT * FROM [dbo].[Orders]) AS w WHERE [ModifiedDateUtc] > '2026-09-15 03:12:47.000'
```

`QUOTENAME()` wraps `[dbo]`, `[Orders]` and `[ModifiedDateUtc]` so a source identifier that happens to be a reserved word or has odd casing doesn't break the generated SQL.

The Lookup's output — one row per active table, including this `ResolvedSourceQuery` string — feeds straight into `FE_SQL_ENTITY`, a `ForEach` over `@activity('LK_ACTIVE_SQL_TABLES').output.value`. Inside it, the `CP_SQL_TO_LANDING` Copy activity (`src/PL_INGEST_SQL.DataPipeline/pipeline-content.json`) runs `@item().ResolvedSourceQuery` — literally the string the view built — as its `AzureSqlSource.sqlReaderQuery`, and writes the result as Parquet to `Landing/Files/@item().FilePath/@item().SourceObject.parquet`, i.e. `Landing/Files/dbo/Orders/Orders.parquet`.

This is the whole point of the hop: **what lands in `Landing/Files/dbo/Orders/` on an incremental run is only the rows where `ModifiedDateUtc` moved past the watermark** — new orders and any order that's been touched since the last run. Not a full extract, not even close, once the watermark is past its first run. Every other `ConnectionType`'s ingest pipeline (`PL_INGEST_FILE`, `PL_INGEST_SQLMI`, etc. — see [Connector-Types](Connector-Types.md)) is structurally identical: same `vw_ActiveIngestTables` Lookup filtered to its own type, same `ResolvedSourceQuery`-driven Copy.

## Hop 2: Landing → Bronze — MERGE, then advance the watermark

`NB_LOAD_BRONZE` (`src/NB_LOAD_BRONZE.Notebook/notebook-content.py`) is one Spark session that loops every active `ingestion.Table` row. For our `orders` entity it reads back exactly the file the Copy activity just wrote:

```python
source_path = onelake_path(data_ws_id, landing_id, "Files", entity["FilePath"])
raw_df = spark.read.format(entity["FileType"].lower()).load(source_path)
```

then runs the same generic cleansing every table gets — `dropna(subset=primary_keys)`, `dropDuplicates(primary_keys)`, plus whatever `CleansingRules` says — before hitting the `LoadType` branch. For `LoadType = 'Delta'`:

```python
if not table_exists:
    clean_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
else:
    delta_table = DeltaTable.forName(spark, target_table)
    merge_conditions = " AND ".join(f"target.{pk} = source.{pk}" for pk in primary_keys)
    update_dict = {col: f"source.{col}" for col in clean_df.columns}
    delta_table.alias("target").merge(clean_df.alias("source"), merge_conditions) \
        .whenMatchedUpdate(set=update_dict) \
        .whenNotMatchedInsertAll() \
        .execute()
```

For `orders`, `merge_conditions` becomes `target.OrderId = source.OrderId` (from `PrimaryKeys`), and every column in the batch is written unconditionally on a match — Bronze doesn't ask "did anything actually change," it just takes the incoming row as truth (that conditional check is a Gold-layer concern, covered below). The very first time `Bronze.dbo.orders` doesn't exist yet, it's seeded with a plain `overwrite` — same code path as a `Full` load's first run — so there's no special "day zero" logic to reason about.

Immediately after the merge, if `IncrementalColumn` is set and present in the batch, the watermark advances:

```python
max_value = clean_df.agg(F.max(entity["IncrementalColumn"])).collect()[0][0]
if max_value is not None:
    _update_watermark(entity["TableId"], str(max_value))
```

`_update_watermark()` is a `MERGE` into `runtime.LoadWatermark` keyed on `(EntityType='Table', EntityId=TableId)` — insert on the first watermark, update after that. Critically, the new value is `MAX(ModifiedDateUtc)` **seen in this batch**, not `SYSUTCDATETIME()` or anything else — the watermark always trails exactly as far as the data Bronze actually ingested. That's what `vw_ActiveIngestTables` reads back on the next `PL_INGEST_SQL` run, closing the loop.

### What "incremental" actually means at this hop

This is the part that trips people up: **Bronze is not a partial table.** After this merge, `Bronze.dbo.orders` holds the full current state of every order the source has ever produced — old orders untouched since before Delta was even turned on, plus every order this run's watermark-bounded batch touched, all merged in place by primary key. "Incremental" describes what moved through Landing and what the MERGE had to process (a small batch), not what Bronze contains (always the complete, current picture). You get to a full table by repeatedly applying deltas, the same way a bank balance is the full current total built from a stream of individual transactions, not a partial record of just the most recent one.

## Delete handling at the Bronze hop

A watermark-bounded `WHERE ModifiedDateUtc > ...` query has a structural blind spot: **it can only return rows that still exist and were touched.** A row that was hard-deleted at the source produces no row at all in any future extract — there's nothing for the `WHERE` clause to catch, no matter how the watermark is tuned. That's not a bug in this framework; it's what "incremental" means by definition. `ingestion.Table.DeleteHandling` gives you three ways to deal with it, in increasing cost:

- **`None`** (the default) — no special handling. A row deleted at the source simply stays in Bronze, stale, until a full reload. Accepted limitation, not silently swallowed — call it out to the client if `orders` deletions matter downstream.
- **`SoftDelete`** — requires `IsDeletedColumn` to be set, and requires the *source* to already implement soft deletes (a flag column that flips and, critically, bumps `ModifiedDateUtc` when it flips). If it does, no special Bronze code runs at all — `IsDeletedColumn` just rides through the same `MERGE`/`update_dict` as every other column, because a soft-delete is, from Bronze's perspective, just another attribute change on a row that's still selectable. Downstream (Silver/Gold) consumers are expected to filter on it themselves.
- **`Reconcile`** — for sources with genuine hard deletes and no soft-delete flag to lean on. After the merge, `_reconcile_deletes()` does a second, deliberately narrow fetch:

  ```python
  source_keys_df = _read_source_sql(entity["ConnectionGuid"], f"SELECT {pk_cols} FROM {qualified}")
  bronze_keys_df = delta_table.toDF().select(*primary_keys)
  missing_keys_df = bronze_keys_df.join(source_keys_df, on=primary_keys, how="left_anti")
  ```

  — i.e. `SELECT OrderId FROM dbo.Orders`, a **full** primary-key-only extract, read directly over JDBC (`_read_source_sql`, authenticated with the notebook's own Entra identity via `notebookutils.credentials.getToken`) rather than through the pipeline's Copy activity. Any `OrderId` present in Bronze but missing from that full key set gets either flagged (`whenMatchedUpdate(set={IsDeletedColumn: "true"})`, if `IsDeletedColumn` is set) or hard-deleted (`whenMatchedDelete()`, if not).

This is the necessary trade-off, not a design flaw: since an incremental extract by construction cannot observe absence, the only way to detect a delete is to compare against the full set of keys at the source — but `Reconcile` keeps that comparison as cheap as it can be, pulling *only* the primary key column(s) instead of every column, so you get delete-detection without paying for a full-row full reload every run. It's also why `Reconcile` is `Sql`-only today (`_reconcile_deletes` checks `entity["ConnectionType"] != "Sql"` and warns-and-skips otherwise): a `File` connection's "source" is already a full copy sitting in Landing each run, so there's no separate lightweight fetch to make there — see [Connector-Types](Connector-Types.md) for the full connector matrix.

## Bronze/Silver → Gold: the batch is already gone, only current state remains

By the time a Gold notebook runs, it has no idea whether Bronze got to its current state via a `Full` overwrite or a `Delta` merge — and it doesn't need to. Bronze (or Silver, if the table earns one — see the `include_silver` rule on [Architecture](Architecture.md)) is, either way, a normal Delta table holding full current state. Gold's SCD1/SCD2 logic in `NB_MONZA_FUNCTIONS` (`src/NB_MONZA_FUNCTIONS.Notebook/notebook-content.py`) reads it exactly the same way regardless of how it got there.

`dim_customer.Notebook` (`src/dim_customer.Notebook/notebook-content.py`) is the concrete, shipped example of this shape — even though the demo's `customer` ingestion is itself `Full` (it's a CSV dropped straight into Landing; there's no watermark to speak of). It reads `Bronze.dbo.customer` by direct OneLake path, builds `temp_dim_customer` with `customer_key = CAST(CustomerId AS STRING)`, and calls:

```python
output_df = load_dimension(
    df=dim_df, lakehouse_name='Gold', table_name='customer',
    dimension_type='scd1', ...
)
```

— which routes to `write_dimension_type1()`. Its merge is a plain upsert: match on `customer_key`, `whenMatchedUpdate` overwrites every non-surrogate-key column with the incoming value, `whenNotMatchedInsertAll` for new customers. No history kept — that's what "Type 1" means.

**Now walk the same table through `write_dimension_type2()` instead** — the function this repo already ships for exactly the case where you want history, e.g. if `dim_customer` were configured `dimension_type='scd2'`. Say `CustomerId = 7`'s `Company` changes from `'Acme'` to `'Acme Corp'` between two Gold runs. The merge condition isn't just the business key:

```python
merge_conditions = " AND ".join(f"target.{pk} = source.{pk}" for pk in column_info["primary_keys"])
merge_conditions += f" AND target.{IS_CURRENT_COL} = true"
```

so it only ever matches against the one row per customer that's currently marked live (`target.customer_key = source.customer_key AND target.is_current = true`). Whether that match actually **fires an update** depends on a second condition built from every non-key, non-system column — `FirstName`, `LastName`, `Company`, `City`, `Country`, `Email`, `Website`, `SubscriptionDate`:

```python
change_conditions = " OR ".join(
    f"target.{attr} != source.{attr} OR (target.{attr} IS NULL AND source.{attr} IS NOT NULL) "
    f"OR (target.{attr} IS NOT NULL AND source.{attr} IS NULL)"
    for attr in column_info["attributes"]
)
```

Since `Company` differs (`'Acme'` vs `'Acme Corp'`), `change_conditions` evaluates true, and `whenMatchedUpdate` closes out the *old* row — it does not touch its attribute values, only:

```python
set={
    VALID_TO_COL: f"source.{valid_from_column}" if valid_from_column else "current_date()",
    IS_CURRENT_COL: "false",
    MODIFIED_COL: "current_timestamp()"
}
```

`dim_customer`'s call passes `valid_from_column=None`, so `VALID_TO_COL` falls back to `current_date()` — the boundary is dated to when the Gold notebook happened to run, not to whatever timestamp the source recorded the change at. (If that distinction matters for a client, carry a source change-timestamp column through to Gold and pass it as `valid_from_column` instead.)

The function then re-reads the table's current rows (`spark.table(full_table_name).filter(is_current == True)`) — which, because the update above already ran, no longer includes an active row for customer 7 — and anti-joins the full incoming batch against it on `primary_keys + attributes` together:

```python
new_and_changed = df.join(
    current_target.select(column_info["primary_keys"] + column_info["attributes"]),
    on=column_info["primary_keys"], how="left_anti"
)
```

Customer 7's incoming row (with `Company='Acme Corp'`) now matches nothing in `current_target`, so it lands in `new_and_changed` alongside any genuinely brand-new customers, gets the next surrogate key (`max_sk + 1`), and is appended (`mode("append")`) with a fresh `valid_from_date` and `is_current=true`. The net result in `gold.dim_customer`: two rows for `customer_key='7'` — an old `customer_sk` with `Company='Acme'`, `is_current=false`, `valid_to_date=<today>`, and a new `customer_sk` with `Company='Acme Corp'`, `is_current=true`, `valid_to_date=9999-12-31`. Any fact row loaded before today's run keeps pointing at the old surrogate key, so historical facts still report against `'Acme'` — that's the entire point of SCD2.

None of this cared whether Bronze got to its current state through `orders`-style Delta merging or `customer`-style Full overwrite. A hypothetical `fact_orders` notebook would call `load_fact(df, ..., write_mode='upsert', key_columns=['OrderId'])`, which resolves `order_key`-style business keys against `gold.dim_*` tables via `_discover_and_map_foreign_keys()` and then does the same `DeltaTable.merge()`/`whenMatchedUpdate`/`whenNotMatchedInsertAll` upsert idiom Bronze itself uses — reading Bronze's already-complete `orders` table, not a delta batch.

## Checklist: flipping an existing table from `Full` to `Delta`

Turning on incremental loading for a table that's currently `Full` in `ingestion.Table` (a worked `Full`-table registration example is on [Metadata-Model](Metadata-Model.md)):

1. **Set `IncrementalColumn`** to a column that's monotonically non-decreasing on every update at the source (a `ModifiedDateUtc`/`rowversion`-style column) — it drives both `vw_ActiveIngestTables`' `WHERE` filter and the post-merge watermark advance. If the source never actually bumps it on update, rows will silently stop showing up in future batches.
2. **Confirm `PrimaryKeys`** genuinely identifies one row. Under `Full` it was only a dedupe key; under `Delta` it's also the `MERGE` key — a `PrimaryKeys` value that isn't actually unique will silently collapse rows on `whenMatchedUpdate` instead of just deduping the extract.
3. **Decide `DeleteHandling`** up front: `None` if source deletes don't matter downstream, `SoftDelete` only if the source already flags-and-timestamps deletes, `Reconcile` (and set `IsDeletedColumn` if you want tombstoning instead of hard-delete) only for `ConnectionType = 'Sql'` sources.
4. **Run it once, then verify:**
   - `SELECT * FROM ingestion.vw_ActiveIngestTables WHERE TableId = 42` — confirm `ResolvedSourceQuery` now has the `WHERE [IncrementalColumn] > '1900-01-01'` shape (first run) rather than a bare `SELECT *`.
   - `SELECT * FROM runtime.LoadWatermark WHERE EntityType='Table' AND EntityId=42` — confirm a row exists with a non-null `LastValue`/`LastRunUtc` after the run.
   - Re-run `vw_ActiveIngestTables` for `TableId = 42` a second time — `ResolvedSourceQuery`'s literal date/value should now match the watermark you just saw, not `1900-01-01`.
   - Check `audit.NotebookRun` for the `NB_LOAD_BRONZE` run's status — see [Operations-Guide](Operations-Guide.md) for the general troubleshooting playbook if it came back `Failed`.
5. **The first Delta run is still effectively a full pull** (no watermark row exists yet, so `ISNULL(w.LastValue, '1900-01-01')` bounds nothing meaningful) — nothing is lost switching an existing `Full` table over; only the *second* run onward is genuinely incremental.
