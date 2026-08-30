# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Keystone Functions
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
    """Generate and prepend a surrogate key column to the DataFrame."""
    sk_column_name = f"{table_name}{SK_SUFFIX}"

    if sk_column_name in df.columns:
        return df

    if new_table:
        df = df.withColumn(sk_column_name, F.monotonically_increasing_id() + 1)
    else:
        full_table_name = _full_table_name(lakehouse_name, schema_name, table_prefix, table_name)
        existing_df = spark.table(full_table_name)
        max_sk = existing_df.agg(F.max(sk_column_name)).collect()[0][0] or 0
        df = df.withColumn(sk_column_name, F.monotonically_increasing_id() + max_sk + 1)

    other_columns = [col for col in df.columns if col != sk_column_name]
    return df.select([sk_column_name] + other_columns)


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

        dim_df = spark.table(dim_path).select(col_name, sk_col)
        df = df.join(dim_df, on=col_name, how="left")
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
    create_unknown_record: bool = True
) -> DataFrame:
    """Load a Slowly Changing Dimension Type 1 (overwrite changes)."""
    table_name = table_name.lower()
    _ensure_schema(lakehouse_name, schema_name)
    full_table_name = _full_table_name(lakehouse_name, schema_name, table_prefix, table_name)
    table_exists = not recreate_table and spark.catalog.tableExists(full_table_name)

    df = _generate_surrogate_key(df, lakehouse_name, schema_name, table_name, table_prefix, new_table=not table_exists)
    df = _append_audit_timestamps(df)

    if not table_exists and create_unknown_record:
        print(f"Creating new dimension table with unknown record: {full_table_name}")
        df = _create_unknown_record(df)

    column_info = _identify_column_types(df, table_name)

    if not table_exists or full_refresh:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)
        print(f"Full refresh completed for {full_table_name}")
    else:
        delta_table = DeltaTable.forName(spark, full_table_name)
        merge_conditions = " AND ".join(f"target.{pk} = source.{pk}" for pk in column_info["primary_keys"])
        update_dict = {col: f"source.{col}" for col in df.columns if col != column_info["surrogate_key"]}
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
    create_unknown_record: bool = True
) -> DataFrame:
    """Load a Slowly Changing Dimension Type 2 (track history)."""
    table_name = table_name.lower()
    _ensure_schema(lakehouse_name, schema_name)
    full_table_name = _full_table_name(lakehouse_name, schema_name, table_prefix, table_name)
    table_exists = not recreate_table and spark.catalog.tableExists(full_table_name)

    df = _generate_surrogate_key(df, lakehouse_name, schema_name, table_name, table_prefix, new_table=not table_exists)
    df = _append_audit_timestamps(df)
    df = _append_scd_type2_columns(df)

    if valid_from_column and valid_from_column in df.columns:
        df = df.withColumn(valid_from_column, F.col(valid_from_column).cast("date"))

    if not table_exists and create_unknown_record:
        print(f"Creating new SCD2 dimension table with unknown record: {full_table_name}")
        df = _create_unknown_record(df)

    column_info = _identify_column_types(df, table_name)

    if not table_exists or full_refresh:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)
        print(f"Full refresh completed for {full_table_name}")
        return df

    delta_table = DeltaTable.forName(spark, full_table_name)
    merge_conditions = " AND ".join(f"target.{pk} = source.{pk}" for pk in column_info["primary_keys"])
    merge_conditions += f" AND target.{IS_CURRENT_COL} = true"
    change_conditions = " OR ".join(
        f"target.{attr} != source.{attr} OR (target.{attr} IS NULL AND source.{attr} IS NOT NULL) "
        f"OR (target.{attr} IS NOT NULL AND source.{attr} IS NULL)"
        for attr in column_info["attributes"]
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
        current_target.select(column_info["primary_keys"] + column_info["attributes"]),
        on=column_info["primary_keys"],
        how="left_anti"
    )

    if new_and_changed.count() > 0:
        max_sk = delta_table.toDF().agg(F.max(column_info["surrogate_key"])).collect()[0][0] or 0
        new_and_changed = new_and_changed.drop(column_info["surrogate_key"])
        new_and_changed = new_and_changed.withColumn(
            column_info["surrogate_key"], F.monotonically_increasing_id() + max_sk + 1
        )
        cols = new_and_changed.columns
        cols.remove(column_info["surrogate_key"])
        new_and_changed = new_and_changed.select([column_info["surrogate_key"]] + cols)
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
    create_unknown_record: bool = True
) -> DataFrame:
    """
    Load a dimension table into Gold. Facade over SCD Type 1 / Type 2.

    Args:
        df: Source DataFrame. Business key column(s) should end in '_key'.
        lakehouse_name: Target lakehouse (e.g. 'Gold').
        table_name: Table name without prefix (e.g. 'customer' -> gold.dim_customer).
        schema_name: Target schema (default 'gold').
        dimension_type: 'scd1' (overwrite changes) or 'scd2' (track history).
        valid_from_column: For SCD2, column to use for effective dating.
        full_refresh: Drop and rewrite all rows instead of merging.
        recreate_table: Drop and recreate the table from scratch.
        create_unknown_record: Add a -1 "Unknown" member row on first creation.
    """
    if dimension_type.lower() == "scd1":
        print(f"Loading dimension as SCD Type 1: {table_name}")
        return write_dimension_type1(
            df, lakehouse_name, table_name, schema_name, table_prefix,
            full_refresh, recreate_table, create_unknown_record
        )
    elif dimension_type.lower() == "scd2":
        print(f"Loading dimension as SCD Type 2: {table_name}")
        return write_dimension_type2(
            df, lakehouse_name, table_name, schema_name, table_prefix,
            valid_from_column, full_refresh, recreate_table, create_unknown_record
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
