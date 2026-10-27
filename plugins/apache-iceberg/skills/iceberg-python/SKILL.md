---
name: iceberg-python
description: "Python-native Apache Iceberg without Spark - PyIceberg (load_catalog, create_table, append, overwrite with overwrite_filter, delete, upsert, transactions, update_schema/update_spec, scans with row_filter and plan_files, time travel, manage_snapshots), Polars (scan_iceberg, write_iceberg), DuckDB (iceberg_scan, ATTACH a REST catalog, PyIceberg to_duckdb), Daft (read_iceberg, write_iceberg), and platform SDKs (Dremio over Arrow Flight, Bauplan branches, Spice AI spicepod.yaml), with an engine capability matrix and cross-engine consistency checks. Use when writing or debugging Python jobs on Iceberg tables, choosing a Python engine, fixing errors like 'Mismatch in fields', 'Cannot visit non-type', 'does not yet support equality deletes', Polars calling amazonaws.com, DuckDB snapshot_id BinderException, or Dremio/Spice showing stale data."
---

# Python for Apache Iceberg

A complete Iceberg lakehouse runs from Python with no JVM on the client: **PyIceberg** is the control
plane (catalog, commits, schema and partition evolution, scan planning), and **Polars, DuckDB and Daft**
are in-process engines that read through it or next to it. **Dremio** and **Spice AI** are query servers
reached over Arrow Flight; **Bauplan** is a managed branching service. All of them meet at Apache Arrow
and at the catalog. Read support is broad; **write support differs a lot** between libraries, and many
widely published snippets do not run unchanged on current versions.

## When to Use This Skill

- Creating, appending to, overwriting, deleting from or upserting into Iceberg tables from Python scripts,
  CI or orchestrators (PyIceberg).
- Evolving schemas or partition specs, reading old snapshots, tagging or branching snapshots.
- Choosing between Polars, DuckDB and Daft for analytics or writes on Iceberg, or wiring their S3 config.
- Querying Iceberg through Dremio (Arrow Flight), Spice AI (federation via `spicepod.yaml`) or Bauplan.
- Debugging stale results, unreadable tables (equality deletes), type mismatches on write, or API errors
  after a library upgrade.
- Proving that dashboards built with different Python tools return the same numbers.

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| DDL, commits, evolution, time travel from Python | PyIceberg catalog + table API | [pyiceberg_api.md](references/pyiceberg_api.md) |
| Replace one partition atomically | `table.overwrite(df, overwrite_filter=EqualTo(...))` | [pyiceberg_api.md](references/pyiceberg_api.md) |
| Merge changes by key | `table.upsert(df, join_cols=[...])` | [pyiceberg_api.md](references/pyiceberg_api.md) |
| Data + metadata change visible together | `with table.transaction() as tx:` | [pyiceberg_api.md](references/pyiceberg_api.md) |
| Know the I/O cost of a filter before reading | `table.scan(row_filter=...).plan_files()` | [pyiceberg_api.md](references/pyiceberg_api.md) |
| Fast lazy analytics with pruning | `pl.scan_iceberg(t, storage_options=...)` | [dataframe_engines_polars_duckdb_daft.md](references/dataframe_engines_polars_duckdb_daft.md) |
| SQL ETL into the lakehouse (CTAS, DELETE, UPDATE) | DuckDB `ATTACH ... (TYPE iceberg)` | [dataframe_engines_polars_duckdb_daft.md](references/dataframe_engines_polars_duckdb_daft.md) |
| SQL with no extension available | `scan().to_arrow()` / `to_duckdb()` + DuckDB | [dataframe_engines_polars_duckdb_daft.md](references/dataframe_engines_polars_duckdb_daft.md) |
| Large scans, transformed writes | `daft.read_iceberg` / `df.write_iceberg` | [dataframe_engines_polars_duckdb_daft.md](references/dataframe_engines_polars_duckdb_daft.md) |
| Shared governed SQL for many users | Dremio over Arrow Flight | [platform_sdks_and_best_practices.md](references/platform_sdks_and_best_practices.md) |
| Join Iceberg with CSV/DB sources in one SQL | Spice AI Spicepod datasets | [platform_sdks_and_best_practices.md](references/platform_sdks_and_best_practices.md) |
| Write-audit-publish as a service | Bauplan branches | [platform_sdks_and_best_practices.md](references/platform_sdks_and_best_practices.md) |
| Which library can do which operation | Measured capability matrix | [platform_sdks_and_best_practices.md](references/platform_sdks_and_best_practices.md) |

## Topics

### PyIceberg: the control plane
`load_catalog(name, type="rest", uri=..., **fileio_props)`, `create_table(ident, schema, partition_spec)`,
`append`, `overwrite(df, overwrite_filter=...)`, `delete(delete_filter=...)`, `upsert(df, join_cols=[...])`,
`update_schema()` / `update_spec()` context managers, `scan(row_filter, selected_fields, snapshot_id)`,
`manage_snapshots()` for tags and branches.
- **Mechanic**: PyArrow writes one Parquet file per partition value per call; PyIceberg writes manifests and
  a new `metadata.json`; the catalog swaps the pointer. A partition-aligned overwrite is `delete` + `append`;
  a filter that cuts through files is `overwrite` + `append`; `delete()` is copy-on-write (0 delete files).
- **Gotchas**: `overwrite(where=...)` does not exist (`TypeError`); `add_column("c", "string")` fails, pass
  `StringType()`; extra `pyiceberg[sql]` does not exist (`sql-sqlite`); bucket writes need
  `pyiceberg[pyiceberg-core]`; `PYICEBERG_CATALOG__*` env vars are read at import time; 0.9.x cannot read
  equality deletes.
- Ref: [references/pyiceberg_api.md](references/pyiceberg_api.md) · Code: [examples/python_lakehouse/pyiceberg_lifecycle/](examples/python_lakehouse/pyiceberg_lifecycle/)

### Polars
Lazy `pl.scan_iceberg` with predicate pushdown; `DataFrame.write_iceberg(t, mode)` (marked `@unstable`).
- **Mechanic**: filters in the lazy plan reach Iceberg planning: 5 files -> 1 for one `pickup_date`, 0 for
  a filter above every file's max.
- **Gotchas**: endpoint is **not** inherited from the PyIceberg table (requests go to amazonaws.com) -> pass
  `storage_options`; cast `Int64` to the table's `int` before writing; `mode="overwrite"` replaces the whole
  table; writing by name needs a `default` catalog.
- Ref: [references/dataframe_engines_polars_duckdb_daft.md](references/dataframe_engines_polars_duckdb_daft.md) · Code: [examples/python_lakehouse/polars_engine/](examples/python_lakehouse/polars_engine/)

### DuckDB
Three routes: Arrow (`con.register` / `scan().to_duckdb()`), `iceberg_scan(metadata.json)`, `ATTACH` a catalog.
- **Mechanic**: with `ATTACH` DuckDB is a full writer; DELETE/UPDATE write position delete files that
  PyIceberg applies; it also reads equality deletes.
- **Gotchas**: time travel is `snapshot_from_id=` or `AT (VERSION => id)` (not `snapshot_id :=`);
  unauthenticated REST needs `AUTHORIZATION_TYPE 'none'`; `iceberg_scan` on a fixed metadata file is stale
  by design; `total-records` counts physical rows under MOR deletes.
- Ref: [references/dataframe_engines_polars_duckdb_daft.md](references/dataframe_engines_polars_duckdb_daft.md) · Code: [examples/python_lakehouse/duckdb_engine/](examples/python_lakehouse/duckdb_engine/)

### Daft
`daft.read_iceberg(table)` and `df.write_iceberg(table, mode, overwrite_filter=...)`.
- **Mechanic**: S3 config comes from the table's FileIO; one scan task per data file, pruned by partition and
  column stats; writes commit snapshots directly.
- **Gotchas**: `agg()` takes expressions, not a dict; `to_arrow()` and `schema().to_pyarrow_schema()`;
  `write_parquet` + `create_table(location=...)` registers nothing and `add_files` refuses Daft's files
  (field IDs) -> use `write_iceberg`.
- Ref: [references/dataframe_engines_polars_duckdb_daft.md](references/dataframe_engines_polars_duckdb_daft.md) · Code: [examples/python_lakehouse/daft_engine/](examples/python_lakehouse/daft_engine/)

### Dremio, Spice AI, Bauplan
- **Dremio**: `pyarrow.flight` `authenticate_basic_token` -> `get_flight_info` -> `do_get().read_all()`.
  OSS 26 has no REST catalog source; its S3 source follows `version-hint.text`, so REST commits stay
  invisible even after `REFRESH METADATA`. CTAS with decimal literals silently zeroes wider inserts.
- **Spice AI**: datasets in `spicepod.yaml` (`from: iceberg:<rest>/v1/namespaces/<ns>/tables/<t>`,
  `from: file:...`); query over Flight. Read-only by default; `access: read_write` DELETE writes **equality
  deletes** that PyIceberg, Polars and Daft cannot read. PyPI `spicepy` is a circuit simulator.
- **Bauplan**: `create_branch`, `create_table(search_uri=...)`, `import_data`, `scan`, `merge_branch`,
  `delete_branch`; there is no `Client.update_schema`.
- Ref: [references/platform_sdks_and_best_practices.md](references/platform_sdks_and_best_practices.md) · Code: [examples/python_lakehouse/platform_sdks/](examples/python_lakehouse/platform_sdks/)

### Capability matrix and consistency
| Library | Partition overwrite | Row delete | Schema evolution | Reads equality deletes |
|---|---|---|---|---|
| PyIceberg 0.9.1 | yes | yes (COW) | yes | no |
| Polars 2.0.0 | no (whole table) | no | no | no |
| DuckDB 1.5.5 | yes (txn DELETE + INSERT) | yes (MOR) | yes | yes |
| Daft 0.7.26 | yes | no | no | no |

All four append and time-travel. Same KPIs at the same snapshot id from PyIceberg, Polars, DuckDB (3 routes)
and Daft; Spice matches at the current snapshot.
- Ref: [references/platform_sdks_and_best_practices.md](references/platform_sdks_and_best_practices.md) · Code: [examples/python_lakehouse/engine_consistency/](examples/python_lakehouse/engine_consistency/)

## Best Practices

1. **One catalog for every engine.** Stale or divergent results came only from catalog bypasses (Dremio
   file-system source, `iceberg_scan` on a fixed metadata file).
2. **Pin snapshot ids** when comparing engines or auditing; compare counts exactly and float sums with a
   relative tolerance (1e-9).
3. **PyIceberg for control, engines for compute**: DDL, evolution and partition-scoped overwrites in
   PyIceberg; joins and aggregations in Polars/DuckDB/Daft.
4. **Cast to the table schema before writing** (`table.schema().as_arrow()`); include partition source columns.
5. **Prefer pushdown scans** and measure pruning with `plan_files()` / `explain()` instead of timings.
6. **One writer style per table**: equality-delete writers (Spice, Flink) make tables unreadable for
   PyIceberg-based readers until compacted.
7. **Pin versions and keep snippets as tests**: assert the failure of the legacy form and the fix, so an
   upgrade that changes either side fails loudly.
