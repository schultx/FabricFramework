# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Monza Functions
# Shared library for the Code workspace: `%run` this from any loader notebook.
#
# - **Runtime helpers** — resolve the Data workspace and its lakehouses by name
#   (never hardcode IDs), and build OneLake paths for cross-lakehouse file access.
# - **Gold facade** — `load_dimension()` / `load_fact()`, supporting SCD1/SCD2,
#   surrogate keys, automatic `_key` -> `_sk` business-key-to-surrogate-key mapping,
#   and an "Unknown" member row (sk = -1) on every dimension.
#
# All three loader notebooks (`NB_LOAD_BRONZE`, `NB_LOAD_SILVER`, `NB_LOAD_GOLD`) bind
# their own default lakehouse to one lakehouse in the Data workspace (set by
# `NB_DEPLOY` after both workspaces exist) — that's what turns on OneLake Spark
# Catalog for the whole Data workspace, so every function below can reference any
# sibling lakehouse there by name (`Landing`, `Bronze`, `Silver`, `Gold`) without
# needing a separate binding per lakehouse.

# CELL ********************

import requests
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
from typing import List, Literal, Optional

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# RUNTIME HELPERS -- resolve workspaces/lakehouses by name, never hardcode IDs
# ============================================================

FABRIC_API = "https://api.fabric.microsoft.com/v1"


def _fabric_headers() -> dict:
    token = notebookutils.credentials.getToken("pbi")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def resolve_workspace_id(name: str) -> str:
    """Look up a workspace's id by its exact display name."""
    resp = requests.get(f"{FABRIC_API}/workspaces", headers=_fabric_headers())
    resp.raise_for_status()
    matches = [w for w in resp.json()["value"] if w["displayName"] == name]
    if len(matches) != 1:
        raise ValueError(f"Expected exactly one workspace named '{name}', found {len(matches)}")
    return matches[0]["id"]


def resolve_lakehouse_id(workspace_id: str, name: str) -> str:
    """Look up a Lakehouse item's id by its exact display name within a workspace."""
    resp = requests.get(f"{FABRIC_API}/workspaces/{workspace_id}/items", headers=_fabric_headers())
    resp.raise_for_status()
    matches = [i for i in resp.json()["value"] if i["type"] == "Lakehouse" and i["displayName"] == name]
    if len(matches) != 1:
        raise ValueError(f"Expected exactly one Lakehouse named '{name}' in workspace {workspace_id}, found {len(matches)}")
    return matches[0]["id"]


def data_workspace_name() -> str:
    """Derive '<Project> Data (X)' from this notebook's own '<Project> Code (X)' workspace name."""
    code_name = notebookutils.runtime.context["currentWorkspaceName"]
    marker = " Code ("
    if marker not in code_name:
        raise ValueError(f"Expected this notebook's workspace name to contain '{marker}', got '{code_name}'")
    return code_name.replace(marker, " Data (")


def ingestion_workspace_name() -> str:
    """Derive '<Project> Ingestion (X)' from this notebook's own '<Project> Code (X)' workspace name."""
    code_name = notebookutils.runtime.context["currentWorkspaceName"]
    marker = " Code ("
    if marker not in code_name:
        raise ValueError(f"Expected this notebook's workspace name to contain '{marker}', got '{code_name}'")
    return code_name.replace(marker, " Ingestion (")


def onelake_path(workspace_id: str, lakehouse_id: str, section: Literal["Files", "Tables"] = "Files", subpath: str = "") -> str:
    """Build an abfss:// path into another lakehouse's Files or Tables section."""
    subpath = subpath.strip("/")
    base = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/{section}"
    return f"{base}/{subpath}" if subpath else base

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# METADATA CATALOG -- read entity config from SQL_METADATA_DATABASE (Ingestion workspace)
# ============================================================

import struct
import uuid
import pyodbc


def _resolve_sql_endpoint(workspace_id: str, database_name: str = "SQL_METADATA_DATABASE") -> str:
    """Look up the SQL connection string (server) for the metadata SQL Database item."""
    resp = requests.get(f"{FABRIC_API}/workspaces/{workspace_id}/items", headers=_fabric_headers())
    resp.raise_for_status()
    matches = [i for i in resp.json()["value"] if i["type"] == "SQLDatabase" and i["displayName"] == database_name]
    if len(matches) != 1:
        raise ValueError(f"Expected exactly one SQLDatabase named '{database_name}' in workspace {workspace_id}, found {len(matches)}")
    item_id = matches[0]["id"]
    detail = requests.get(f"{FABRIC_API}/workspaces/{workspace_id}/sqldatabases/{item_id}", headers=_fabric_headers())
    detail.raise_for_status()
    props = detail.json()["properties"]
    return props["serverFqdn"], props["databaseName"]


def catalog_connection():
    """
    Open a pyodbc connection to the metadata catalog SQL Database, authenticating
    with this notebook's own Entra identity (no stored password/secret).
    """
    workspace_id = resolve_workspace_id(ingestion_workspace_name())
    server, database = _resolve_sql_endpoint(workspace_id)

    token = notebookutils.credentials.getToken("https://database.windows.net/.default")
    token_bytes = token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = 1256

    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server},1433;DATABASE={database};Encrypt=yes"
    return pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})


def catalog_query(sql: str, params: tuple = ()) -> "list[dict]":
    """Run a SELECT against the metadata catalog and return rows as a list of dicts."""
    conn = catalog_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        conn.close()


def catalog_execute(sql: str, params: tuple = ()) -> None:
    """
    Run an INSERT/UPDATE/DELETE/MERGE against the metadata catalog (no result
    rows expected). Used by NB_LOAD_BRONZE to advance runtime.LoadWatermark
    after a Delta-type load.
    """
    conn = catalog_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def start_notebook_run(notebook_name: str) -> str:
    """
    Insert an audit.NotebookRun row for this run, status 'Running'. Returns a
    RunGuid -- pass it to end_notebook_run() when the notebook finishes (or
    fails) to close the row out. Call once, near the top of the notebook,
    before any real work starts.
    """
    run_guid = str(uuid.uuid4())
    catalog_execute(
        """
        INSERT INTO [audit].[NotebookRun] ([NotebookName], [RunGuid], [Status], [StartTimeUtc])
        VALUES (?, ?, 'Running', SYSUTCDATETIME())
        """,
        (notebook_name, run_guid)
    )
    return run_guid


def end_notebook_run(run_guid: str, status: str, error_message: str = None) -> None:
    """Close out the audit.NotebookRun row opened by start_notebook_run()."""
    catalog_execute(
        """
        UPDATE [audit].[NotebookRun]
        SET [Status] = ?, [EndTimeUtc] = SYSUTCDATETIME(), [ErrorMessage] = ?
        WHERE [RunGuid] = ?
        """,
        (status, error_message, run_guid)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# GOLD FACADE -- CONFIGURATION CONSTANTS
# ============================================================

DIM_TABLE_PREFIX = "dim_"
FACT_TABLE_PREFIX = "fact_"
SK_SUFFIX = "_sk"
BK_SUFFIX = "_key"
UNKNOWN_KEY_VALUE = -1
CREATED_COL = "lakehouse_created_datetime"
MODIFIED_COL = "lakehouse_modified_datetime"
VALID_FROM_COL = "valid_from_date"
VALID_TO_COL = "valid_to_date"
IS_CURRENT_COL = "is_current"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# GOLD FACADE -- HELPERS
# ============================================================

def _full_table_name(lakehouse_name: str, schema_name: str, table_prefix: str, table_name: str) -> str:
    return f"{lakehouse_name}.{schema_name}.{table_prefix}{table_name.lower()}"


def _ensure_schema(lakehouse_name: str, schema_name: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {lakehouse_name}.{schema_name}")


def _append_audit_timestamps(df: DataFrame) -> DataFrame:
    """Add audit timestamp columns if they don't already exist."""
    current_time = F.current_timestamp()
    if CREATED_COL not in df.columns:
        df = df.withColumn(CREATED_COL, current_time)
    if MODIFIED_COL not in df.columns:
        df = df.withColumn(MODIFIED_COL, current_time)
    return df


def _append_scd_type2_columns(df: DataFrame) -> DataFrame:
    """Add SCD Type 2 tracking columns if they don't already exist."""
    max_date = F.lit("9999-12-31").cast("date")
    if VALID_FROM_COL not in df.columns:
        df = df.withColumn(VALID_FROM_COL, F.current_timestamp().cast("date"))
    if VALID_TO_COL not in df.columns:
        df = df.withColumn(VALID_TO_COL, max_date)
    if IS_CURRENT_COL not in df.columns:
        df = df.withColumn(IS_CURRENT_COL, F.lit(True))
    return df


def _generate_surrogate_key(
    df: DataFrame,
    lakehouse_name: str,
    schema_name: str,
    table_name: str,
    table_prefix: str,
    new_table: bool
) -> DataFrame:
    """
    Generate and prepend a surrogate key column to the DataFrame.

    Uses a deterministic dense row_number() sequence rather than
    monotonically_increasing_id() -- that function only produces small,
    contiguous-looking values on single-partition demo data; at real client
    volumes (multiple files/partitions) its values are large, non-contiguous,
    and partition-dependent, risking a silent collision if a downstream layer
    narrows the key to int32. Ordered by this DataFrame's own '_key'-suffixed
    business-key column(s) when present (the same convention
    _identify_column_types() falls back to), since this function isn't told
    the caller's resolved primary_keys list; falls back to ordering by the
    full row when no such column exists.
    """
    sk_column_name = f"{table_name}{SK_SUFFIX}"

    if sk_column_name in df.columns:
        return df

    order_columns = [
        col for col in df.columns
        if col.endswith(BK_SUFFIX) and col not in [CREATED_COL, MODIFIED_COL]
    ]
    if not order_columns:
        order_columns = df.columns
    row_order = Window.orderBy(*[F.col(col) for col in order_columns])

    if new_table:
        df = df.withColumn(sk_column_name, F.row_number().over(row_order))
    else:
        full_table_name = _full_table_name(lakehouse_name, schema_name, table_prefix, table_name)
        existing_df = spark.table(full_table_name)
        max_sk = existing_df.agg(F.max(sk_column_name)).collect()[0][0] or 0
        df = df.withColumn(sk_column_name, F.row_number().over(row_order) + max_sk)

    other_columns = [col for col in df.columns if col != sk_column_name]
    return df.select([sk_column_name] + other_columns)


def _preserve_surrogate_keys_scd1(
    df: DataFrame,
    full_table_name: str,
    primary_keys: List[str],
    sk_column_name: str,
    lakehouse_name: str,
    schema_name: str,
    table_name: str,
    table_prefix: str
) -> DataFrame:
    """
    For write_dimension_type1(full_refresh=True) against an already-existing
    table: left-join df onto the existing table by business key so each
    previously-existing member keeps its surrogate key, and mint a new key
    (via _generate_surrogate_key's existing-table path) only for rows with no
    match in the existing table -- genuinely new members. Any previously
    carried-over Unknown row (surrogate key == UNKNOWN_KEY_VALUE) is excluded
    from the existing lookup so the caller's own fresh _create_unknown_record()
    call doesn't end up duplicating it.
    """
    existing_lookup = (
        spark.table(full_table_name)
        .filter(F.col(sk_column_name) != UNKNOWN_KEY_VALUE)
        .select(primary_keys + [sk_column_name])
    )

    df = df.join(existing_lookup, on=primary_keys, how="left")
    existing_members = df.filter(F.col(sk_column_name).isNotNull())
    new_members = df.filter(F.col(sk_column_name).isNull()).drop(sk_column_name)

    if new_members.take(1):
        new_members = _generate_surrogate_key(
            new_members, lakehouse_name, schema_name, table_name, table_prefix, new_table=False
        )
        result = existing_members.select(new_members.columns).unionByName(new_members)
    else:
        result = existing_members

    other_columns = [col for col in result.columns if col != sk_column_name]
    return result.select([sk_column_name] + other_columns)


def _full_refresh_scd2_with_history(
    df: DataFrame,
    full_table_name: str,
    primary_keys: List[str],
    attribute_columns: List[str],
    sk_column_name: str,
    valid_from_column: Optional[str],
    lakehouse_name: str,
    schema_name: str,
    table_name: str,
    table_prefix: str
) -> DataFrame:
    """
    For write_dimension_type2(full_refresh=True) against an already-existing
    table: rebuild the dimension's full row set in memory, preserving history
    and existing surrogate keys instead of discarding them the way a blind
    overwrite would.

    Mirrors the ordinary (non-full_refresh) merge branch row-for-row: a current
    row whose business key reappears in df with unchanged attributes is left
    exactly as-is; one whose attributes changed is closed out (valid_to /
    is_current / modified) the same way the live MERGE does; a business key not
    present in df at all is left untouched; and only genuinely new-or-changed
    members get a freshly minted surrogate key via _generate_surrogate_key,
    reading the same existing table the live merge path reads for max_sk.
    Already-historical rows (is_current = false) always pass through unchanged.
    Returns the full, ready-to-overwrite DataFrame -- the caller writes it.
    """
    existing_df = spark.table(full_table_name)
    # Drop any previously-added Unknown row (sk == UNKNOWN_KEY_VALUE) -- the
    # caller re-adds a fresh one via _create_unknown_record() when requested,
    # and it must never be matched against or duplicated by the logic below.
    existing_df = existing_df.filter(F.col(sk_column_name) != UNKNOWN_KEY_VALUE)

    existing_noncurrent = existing_df.filter(F.col(IS_CURRENT_COL) == False)
    existing_current = existing_df.filter(F.col(IS_CURRENT_COL) == True)

    src_cols = [F.col(attr).alias(f"__src_{attr}") for attr in attribute_columns]
    has_valid_from_source = bool(valid_from_column) and valid_from_column in df.columns
    if has_valid_from_source:
        src_cols.append(F.col(valid_from_column).alias("__src_valid_to"))
    incoming = df.select(*(primary_keys + src_cols + [F.lit(True).alias("__matched")]))

    joined = existing_current.join(incoming, on=primary_keys, how="left")

    change_expr = F.lit(False)
    for attr in attribute_columns:
        src_attr = F.col(f"__src_{attr}")
        change_expr = change_expr | (
            (F.col(attr) != src_attr)
            | (F.col(attr).isNull() & src_attr.isNotNull())
            | (F.col(attr).isNotNull() & src_attr.isNull())
        )
    should_close = F.col("__matched").isNotNull() & change_expr
    new_valid_to = F.col("__src_valid_to") if has_valid_from_source else F.current_date()

    updated_current = (
        joined
        .withColumn(VALID_TO_COL, F.when(should_close, new_valid_to).otherwise(F.col(VALID_TO_COL)))
        .withColumn(IS_CURRENT_COL, F.when(should_close, F.lit(False)).otherwise(F.col(IS_CURRENT_COL)))
        .withColumn(MODIFIED_COL, F.when(should_close, F.current_timestamp()).otherwise(F.col(MODIFIED_COL)))
        .select(existing_current.columns)
    )

    current_target_after = updated_current.filter(F.col(IS_CURRENT_COL) == True)
    new_and_changed = df.join(
        current_target_after.select(primary_keys + attribute_columns),
        on=primary_keys,
        how="left_anti"
    )

    if new_and_changed.take(1):
        new_and_changed = new_and_changed.drop(sk_column_name) if sk_column_name in new_and_changed.columns else new_and_changed
        new_and_changed = _generate_surrogate_key(
            new_and_changed, lakehouse_name, schema_name, table_name, table_prefix, new_table=False
        )
        new_and_changed = new_and_changed.select(existing_current.columns)
        return existing_noncurrent.unionByName(updated_current).unionByName(new_and_changed)

    return existing_noncurrent.unionByName(updated_current)


def _create_unknown_record(df: DataFrame) -> DataFrame:
    """Create an 'Unknown' dimension record with -1 as the surrogate key."""
    unknown_values = []
    for field in df.schema.fields:
        col_name = field.name
        if col_name.endswith(SK_SUFFIX):
            unknown_values.append(F.lit(UNKNOWN_KEY_VALUE).cast(field.dataType))
        elif col_name in [CREATED_COL, MODIFIED_COL]:
            unknown_values.append(F.current_timestamp())
        elif col_name == VALID_FROM_COL:
            unknown_values.append(F.lit("1900-01-01").cast("date"))
        elif col_name == VALID_TO_COL:
            unknown_values.append(F.lit("9999-12-31").cast("date"))
        elif col_name == IS_CURRENT_COL:
            unknown_values.append(F.lit(True))
        elif isinstance(field.dataType, (StringType, VarcharType)):
            unknown_values.append(F.lit("Unknown"))
        elif isinstance(field.dataType, (IntegerType, LongType, ShortType, ByteType)):
            unknown_values.append(F.lit(-1).cast(field.dataType))
        elif isinstance(field.dataType, (DoubleType, FloatType, DecimalType)):
            unknown_values.append(F.lit(-1.0).cast(field.dataType))
        elif isinstance(field.dataType, BooleanType):
            unknown_values.append(F.lit(False))
        elif isinstance(field.dataType, DateType):
            unknown_values.append(F.lit("1900-01-01").cast("date"))
        elif isinstance(field.dataType, TimestampType):
            unknown_values.append(F.lit("1900-01-01 00:00:00").cast("timestamp"))
        else:
            unknown_values.append(F.lit(None).cast(field.dataType))

    unknown_df = spark.range(1).select(*unknown_values).toDF(*df.columns)
    return df.union(unknown_df)


def _discover_and_map_foreign_keys(df: DataFrame, lakehouse_name: str, schema_name: str) -> DataFrame:
    """
    Automatically map business keys to surrogate keys from dimension tables in the Gold schema.
    Convention: a column named '<name>_key' maps to '<schema>.dim_<name>', joining on '<name>_key'
    and pulling back '<name>_sk'.
    """
    dimension_tables = [
        t.name for t in spark.catalog.listTables(f"{lakehouse_name}.{schema_name}")
        if t.name.startswith(DIM_TABLE_PREFIX)
    ]
    print(f"Found {len(dimension_tables)} dimension table(s) in {lakehouse_name}.{schema_name} for key mapping")

    for col_name in df.columns:
        if not col_name.endswith(BK_SUFFIX) or col_name.endswith(SK_SUFFIX):
            continue

        base_name = col_name[: -len(BK_SUFFIX)].lower()
        target_dim = f"{DIM_TABLE_PREFIX}{base_name}"

        if target_dim not in [t.lower() for t in dimension_tables]:
            print(f"Warning: no dimension table found for '{col_name}' (looking for {target_dim})")
            continue

        sk_col = f"{base_name}{SK_SUFFIX}"
        dim_path = f"{lakehouse_name}.{schema_name}.{target_dim}"
        print(f"Mapping {col_name} -> {sk_col} (using {dim_path})")

        dim_table_df = spark.table(dim_path)
        if IS_CURRENT_COL in dim_table_df.columns:
            # SCD2 dimensions keep multiple physical rows per business key (an
            # expired row plus a current row, each with a different surrogate
            # key) -- join against the current row only, or a fact matches both
            # and is silently duplicated, one copy per historical version.
            dim_table_df = dim_table_df.filter(F.col(IS_CURRENT_COL) != False)
        dim_df = dim_table_df.select(col_name, sk_col)

        df = df.join(dim_df, on=col_name, how="left")

        unmatched_df = df.filter(F.col(sk_col).isNull())
        unmatched_count = unmatched_df.count()
        if unmatched_count > 0:
            sample_keys = [
                row[col_name] for row in
                unmatched_df.select(col_name).distinct().limit(5).collect()
            ]
            print(
                f"Warning: {unmatched_count} row(s) for '{col_name}' had no match in {dim_path} "
                f"and fell back to Unknown ({UNKNOWN_KEY_VALUE}). Sample unmatched value(s): {sample_keys}"
            )
        else:
            print(f"All rows for '{col_name}' matched successfully against {dim_path}")

        df = df.withColumn(sk_col, F.coalesce(F.col(sk_col), F.lit(UNKNOWN_KEY_VALUE)))
        df = df.drop(col_name)

    return df


def _identify_column_types(df: DataFrame, table_name: str, key_columns: Optional[List[str]] = None) -> dict:
    """Classify columns into primary keys, surrogate key, and attributes."""
    sk_col = f"{table_name}{SK_SUFFIX}"

    if key_columns is None:
        key_columns = [
            col for col in df.columns
            if col.endswith(BK_SUFFIX) and col not in [CREATED_COL, MODIFIED_COL]
        ]

    system_columns = {sk_col, CREATED_COL, MODIFIED_COL, VALID_FROM_COL, VALID_TO_COL, IS_CURRENT_COL}
    attribute_columns = [col for col in df.columns if col not in key_columns and col not in system_columns]

    return {"primary_keys": key_columns, "surrogate_key": sk_col, "attributes": attribute_columns}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# DIMENSION LOADING -- SCD TYPE 1 / TYPE 2
# ============================================================

def write_dimension_type1(
    df: DataFrame,
    lakehouse_name: str,
    table_name: str,
    schema_name: str = "gold",
    table_prefix: str = DIM_TABLE_PREFIX,
    full_refresh: bool = False,
    recreate_table: bool = False,
    create_unknown_record: bool = True,
    key_columns: Optional[List[str]] = None
) -> DataFrame:
    """Load a Slowly Changing Dimension Type 1 (overwrite changes)."""
    table_name = table_name.lower()
    _ensure_schema(lakehouse_name, schema_name)
    full_table_name = _full_table_name(lakehouse_name, schema_name, table_prefix, table_name)
    table_exists = not recreate_table and spark.catalog.tableExists(full_table_name)

    column_info = _identify_column_types(df, table_name, key_columns)
    if not column_info["primary_keys"]:
        raise ValueError(
            f"write_dimension_type1('{full_table_name}'): could not determine primary key column(s). "
            f"Pass key_columns explicitly, or name the business key column(s) with a '{BK_SUFFIX}' suffix."
        )
    primary_keys = column_info["primary_keys"]
    sk_column_name = column_info["surrogate_key"]

    full_refresh_with_history = full_refresh and table_exists

    if full_refresh_with_history:
        # Carry forward each existing member's surrogate key instead of
        # reassigning every key on the blind-overwrite that follows.
        df = _preserve_surrogate_keys_scd1(
            df, full_table_name, primary_keys, sk_column_name,
            lakehouse_name, schema_name, table_name, table_prefix
        )
    else:
        df = _generate_surrogate_key(df, lakehouse_name, schema_name, table_name, table_prefix, new_table=not table_exists)

    df = _append_audit_timestamps(df)

    # The overwrite below (whether from a brand-new table or full_refresh on an
    # existing one) replaces every row, so the Unknown record must be re-added
    # in both cases, not only on first creation.
    if create_unknown_record and (not table_exists or full_refresh):
        print(f"{'Creating new dimension table' if not table_exists else 'Full refresh'} with unknown record: {full_table_name}")
        df = _create_unknown_record(df)

    if not table_exists or full_refresh:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)
        print(f"Full refresh completed for {full_table_name}")
    else:
        delta_table = DeltaTable.forName(spark, full_table_name)
        merge_conditions = " AND ".join(f"target.{pk} = source.{pk}" for pk in primary_keys)
        update_dict = {col: f"source.{col}" for col in df.columns if col != sk_column_name}
        update_dict[MODIFIED_COL] = "current_timestamp()"

        delta_table.alias("target").merge(df.alias("source"), merge_conditions) \
            .whenMatchedUpdate(set=update_dict) \
            .whenNotMatchedInsertAll() \
            .execute()
        print(f"Upsert completed for {full_table_name}")

    return df


def write_dimension_type2(
    df: DataFrame,
    lakehouse_name: str,
    table_name: str,
    schema_name: str = "gold",
    table_prefix: str = DIM_TABLE_PREFIX,
    valid_from_column: Optional[str] = None,
    full_refresh: bool = False,
    recreate_table: bool = False,
    create_unknown_record: bool = True,
    key_columns: Optional[List[str]] = None
) -> DataFrame:
    """Load a Slowly Changing Dimension Type 2 (track history)."""
    table_name = table_name.lower()
    _ensure_schema(lakehouse_name, schema_name)
    full_table_name = _full_table_name(lakehouse_name, schema_name, table_prefix, table_name)
    table_exists = not recreate_table and spark.catalog.tableExists(full_table_name)

    # Resolved once, up front, on the raw incoming columns -- system columns
    # (sk / audit / SCD2 tracking) aren't appended yet at this point, so
    # column_info["attributes"] already comes back as exactly the business
    # attribute columns, same as if this were computed after they're appended.
    column_info = _identify_column_types(df, table_name, key_columns)
    if not column_info["primary_keys"]:
        raise ValueError(
            f"write_dimension_type2('{full_table_name}'): could not determine primary key column(s). "
            f"Pass key_columns explicitly, or name the business key column(s) with a '{BK_SUFFIX}' suffix."
        )
    primary_keys = column_info["primary_keys"]
    attribute_columns = column_info["attributes"]
    sk_column_name = column_info["surrogate_key"]

    full_refresh_with_history = full_refresh and table_exists

    if not full_refresh_with_history:
        df = _generate_surrogate_key(df, lakehouse_name, schema_name, table_name, table_prefix, new_table=not table_exists)

    df = _append_audit_timestamps(df)
    df = _append_scd_type2_columns(df)

    if valid_from_column and valid_from_column in df.columns:
        df = df.withColumn(valid_from_column, F.col(valid_from_column).cast("date"))

    # The overwrite below (whether from a brand-new table or full_refresh on an
    # existing one) replaces every row, so the Unknown record must be re-added
    # in both cases, not only on first creation.
    add_unknown_record = create_unknown_record and (not table_exists or full_refresh)

    if full_refresh_with_history:
        # Preserve history (valid_from/valid_to/is_current) and existing
        # surrogate keys instead of dropping them on the blind overwrite that
        # full_refresh would otherwise perform -- consistent with what the
        # ordinary merge branch below would itself have produced.
        df = _full_refresh_scd2_with_history(
            df, full_table_name, primary_keys, attribute_columns, sk_column_name,
            valid_from_column, lakehouse_name, schema_name, table_name, table_prefix
        )
        if add_unknown_record:
            print(f"Full refresh (history preserved) with unknown record: {full_table_name}")
            df = _create_unknown_record(df)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)
        print(f"Full refresh (history preserved) completed for {full_table_name}")
        return df

    if not table_exists:
        if add_unknown_record:
            print(f"Creating new SCD2 dimension table with unknown record: {full_table_name}")
            df = _create_unknown_record(df)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)
        print(f"Full refresh completed for {full_table_name}")
        return df

    delta_table = DeltaTable.forName(spark, full_table_name)
    merge_conditions = " AND ".join(f"target.{pk} = source.{pk}" for pk in primary_keys)
    merge_conditions += f" AND target.{IS_CURRENT_COL} = true"
    change_conditions = " OR ".join(
        f"target.{attr} != source.{attr} OR (target.{attr} IS NULL AND source.{attr} IS NOT NULL) "
        f"OR (target.{attr} IS NOT NULL AND source.{attr} IS NULL)"
        for attr in attribute_columns
    )

    delta_table.alias("target").merge(df.alias("source"), merge_conditions).whenMatchedUpdate(
        condition=change_conditions,
        set={
            VALID_TO_COL: f"source.{valid_from_column}" if valid_from_column else "current_date()",
            IS_CURRENT_COL: "false",
            MODIFIED_COL: "current_timestamp()"
        }
    ).execute()

    current_target = spark.table(full_table_name).filter(F.col(IS_CURRENT_COL) == True)
    new_and_changed = df.join(
        current_target.select(primary_keys + attribute_columns),
        on=primary_keys,
        how="left_anti"
    )

    if new_and_changed.count() > 0:
        new_and_changed = new_and_changed.drop(sk_column_name)
        new_and_changed = _generate_surrogate_key(
            new_and_changed, lakehouse_name, schema_name, table_name, table_prefix, new_table=False
        )
        new_and_changed.write.format("delta").mode("append").saveAsTable(full_table_name)

    print(f"SCD2 merge completed for {full_table_name}")
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def load_dimension(
    df: DataFrame,
    lakehouse_name: str,
    table_name: str,
    schema_name: str = "gold",
    table_prefix: str = DIM_TABLE_PREFIX,
    dimension_type: Literal["scd1", "scd2"] = "scd1",
    valid_from_column: Optional[str] = None,
    full_refresh: bool = False,
    recreate_table: bool = False,
    create_unknown_record: bool = True,
    key_columns: Optional[List[str]] = None
) -> DataFrame:
    """
    Load a dimension table into Gold. Facade over SCD Type 1 / Type 2.

    Args:
        df: Source DataFrame. Business key column(s) should end in '_key'
            unless key_columns is passed explicitly.
        lakehouse_name: Target lakehouse (e.g. 'Gold').
        table_name: Table name without prefix (e.g. 'customer' -> gold.dim_customer).
        schema_name: Target schema (default 'gold').
        dimension_type: 'scd1' (overwrite changes) or 'scd2' (track history).
        valid_from_column: For SCD2, column to use for effective dating.
        full_refresh: Drop and rewrite all rows instead of merging. On an
            already-existing table, previously-existing members keep their
            surrogate key (and, for SCD2, their history) instead of every key
            being reassigned.
        recreate_table: Drop and recreate the table from scratch.
        create_unknown_record: Add a -1 "Unknown" member row on first creation
            (and on every full_refresh, since that replaces the whole table).
        key_columns: Explicit business/primary key column(s), overriding the
            default '_key'-suffix inference. Required when the source doesn't
            follow that naming convention.
    """
    if dimension_type.lower() == "scd1":
        print(f"Loading dimension as SCD Type 1: {table_name}")
        return write_dimension_type1(
            df, lakehouse_name, table_name, schema_name, table_prefix,
            full_refresh, recreate_table, create_unknown_record, key_columns
        )
    elif dimension_type.lower() == "scd2":
        print(f"Loading dimension as SCD Type 2: {table_name}")
        return write_dimension_type2(
            df, lakehouse_name, table_name, schema_name, table_prefix,
            valid_from_column, full_refresh, recreate_table, create_unknown_record, key_columns
        )
    else:
        raise ValueError(f"Invalid dimension_type: '{dimension_type}'. Must be 'scd1' or 'scd2'.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def load_fact(
    df: DataFrame,
    lakehouse_name: str,
    table_name: str,
    schema_name: str = "gold",
    table_prefix: str = FACT_TABLE_PREFIX,
    write_mode: Literal["overwrite", "append", "upsert", "incremental", "replace_partition"] = "overwrite",
    recreate_table: bool = False,
    key_columns: Optional[List[str]] = None,
    include_surrogate_key: bool = False,
    auto_map_foreign_keys: bool = True,
    partition_column: Optional[str] = None
) -> DataFrame:
    """
    Load a fact table into Gold.

    Args:
        df: Source DataFrame. Any '<name>_key' column is auto-mapped to gold.dim_<name>.<name>_sk
            when auto_map_foreign_keys is True.
        lakehouse_name: Target lakehouse (e.g. 'Gold').
        table_name: Table name without prefix (e.g. 'signup' -> gold.fact_signup).
        schema_name: Target schema (default 'gold').
        write_mode: 'overwrite' | 'append' | 'upsert' | 'incremental' | 'replace_partition'.
        key_columns: Required for 'upsert' - the natural key of the fact grain.
        include_surrogate_key: Most facts don't need their own sk; leave False unless something references it.
        auto_map_foreign_keys: Resolve '_key' business-key columns to '_sk' surrogate keys.
        partition_column: Required for 'incremental' / 'replace_partition'.
    """
    table_name = table_name.lower()
    _ensure_schema(lakehouse_name, schema_name)
    full_table_name = _full_table_name(lakehouse_name, schema_name, table_prefix, table_name)
    table_exists = not recreate_table and spark.catalog.tableExists(full_table_name)

    if auto_map_foreign_keys:
        df = _discover_and_map_foreign_keys(df, lakehouse_name, schema_name)

    df = _append_audit_timestamps(df)

    if include_surrogate_key:
        df = _generate_surrogate_key(df, lakehouse_name, schema_name, table_name, table_prefix, new_table=not table_exists)

    write_mode = write_mode.lower()

    if write_mode == "overwrite":
        print(f"Overwriting fact table: {full_table_name}")
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)

    elif write_mode == "append":
        print(f"Appending to fact table: {full_table_name}")
        mode = "overwrite" if not table_exists else "append"
        df.write.format("delta").mode(mode).saveAsTable(full_table_name)

    elif write_mode == "upsert":
        if not key_columns:
            raise ValueError("key_columns must be provided for upsert mode")
        print(f"Upserting fact table: {full_table_name}")
        if not table_exists:
            df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
        else:
            delta_table = DeltaTable.forName(spark, full_table_name)
            merge_conditions = " AND ".join(f"target.{key} = source.{key}" for key in key_columns)
            update_dict = {col: f"source.{col}" for col in df.columns}
            update_dict[MODIFIED_COL] = "current_timestamp()"
            delta_table.alias("target").merge(df.alias("source"), merge_conditions) \
                .whenMatchedUpdate(set=update_dict) \
                .whenNotMatchedInsertAll() \
                .execute()

    elif write_mode == "incremental":
        if not partition_column:
            raise ValueError("partition_column must be provided for incremental mode")
        print(f"Incremental load to fact table: {full_table_name}")
        if not table_exists:
            df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
        else:
            partition_values = [row[partition_column] for row in df.select(partition_column).distinct().collect()]
            delta_table = DeltaTable.forName(spark, full_table_name)
            delta_table.delete(F.col(partition_column).isin(partition_values))
            df.write.format("delta").mode("append").saveAsTable(full_table_name)

    elif write_mode == "replace_partition":
        if not partition_column:
            raise ValueError("partition_column must be provided for replace_partition mode")
        print(f"Replacing partition range in fact table: {full_table_name}")
        if not table_exists:
            df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
        else:
            stats = df.agg(F.min(partition_column).alias("lo"), F.max(partition_column).alias("hi")).collect()[0]
            delta_table = DeltaTable.forName(spark, full_table_name)
            delta_table.delete((F.col(partition_column) >= F.lit(stats["lo"])) & (F.col(partition_column) <= F.lit(stats["hi"])))
            df.write.format("delta").mode("append").saveAsTable(full_table_name)

    else:
        raise ValueError(
            f"Invalid write_mode: '{write_mode}'. Must be one of: overwrite, append, upsert, incremental, replace_partition"
        )

    print(f"Fact load completed: {full_table_name}")
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# SILVER FACADE -- shared write helper for Silver-layer notebooks
# ============================================================

def write_silver_table(
    df: DataFrame,
    lakehouse_name: str,
    table_name: str,
    schema_name: str = "silver",
    mode: str = "overwrite"
) -> DataFrame:
    """
    Write a Silver table, reusing the same schema-creation and audit-timestamp
    conventions as the Gold facade (_ensure_schema / _append_audit_timestamps)
    instead of every Silver notebook hand-rolling its own write and inventing
    its own audit-column name.
    """
    _ensure_schema(lakehouse_name, schema_name)
    full_table_name = f"{lakehouse_name}.{schema_name}.{table_name.lower()}"
    df = _append_audit_timestamps(df)

    writer = df.write.format("delta").mode(mode)
    if mode.lower() == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(full_table_name)

    print(f"Wrote Silver table: {full_table_name}")
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
