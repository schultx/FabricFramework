# TODO

## Replace the OneLake-path Bronze read workaround with direct schema.table references

Several notebooks read Bronze via a direct OneLake path instead of a plain
Spark-catalog table reference, to work around `spark_catalog requires a
single-part namespace` when reading back a table under a *different*
lakehouse than the notebook's own default:

```python
_data_ws_id = resolve_workspace_id(data_workspace_name())
_bronze_lh_id = resolve_lakehouse_id(_data_ws_id, "Bronze")
spark.read.format("delta").load(
    onelake_path(_data_ws_id, _bronze_lh_id, "Tables", "dbo/customer")
).createOrReplaceTempView("bronze_customer")
```

Replace with a direct lakehouse schema/table reference instead (e.g.
`Bronze.dbo.customer`) in every notebook that uses this pattern:

- [src/dim_customer.Notebook/notebook-content.py](src/dim_customer.Notebook/notebook-content.py)
- [src/fact_signup.Notebook/notebook-content.py](src/fact_signup.Notebook/notebook-content.py)
- [src/sil_customer.Notebook/notebook-content.py](src/sil_customer.Notebook/notebook-content.py)
- [src/NB_LOAD_BRONZE.Notebook/notebook-content.py](src/NB_LOAD_BRONZE.Notebook/notebook-content.py)
- [src/NB_MONZA_FUNCTIONS.Notebook/notebook-content.py](src/NB_MONZA_FUNCTIONS.Notebook/notebook-content.py) — defines the `onelake_path()` helper being worked around; check whether it's still needed elsewhere before removing.

Not started yet — flagged by the user 2026-09-16, explicitly deferred.
