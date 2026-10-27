# Pure-Python Iceberg lakehouse (PyIceberg, Polars, DuckDB, Daft, Dremio, Spice, Bauplan)

One Docker network: Iceberg REST catalog + SeaweedFS (S3 API), a `python:3.11-slim` runner hosting every
in-process engine, Dremio OSS and the Spice OSS runtime. No JVM on the client side. 55 tests.

| File | Shows |
|---|---|
| `common/lakehouse.py` | REST catalog via `load_catalog` with explicit S3 FileIO props; namespace reset |
| `common/trips.py` | Trips schema, identity spec on `pickup_date`, synthetic data (tz-aware UTC timestamps) |
| `common/seed_spice.py` | Creates the Spicepod tables before Spice starts (`seed` service) |
| `pyiceberg_lifecycle/lifecycle.py` | `overwrite(overwrite_filter=)`, `delete(delete_filter=)`, `update_schema()`, `update_spec()`, `plan_files()` |
| `pyiceberg_lifecycle/test_pyiceberg.py` | Files per partition, snapshot operations, COW delete, schema/partition evolution, time travel, pruning |
| `pyiceberg_lifecycle/test_snippets.py` | Widely published snippets on a SQLite catalog: failure asserted, then the fix |
| `polars_engine/` | `scan_iceberg` with `storage_options`, plan-based pruning count, `write_iceberg` pitfalls |
| `duckdb_engine/` | Arrow route vs `iceberg_scan` vs `ATTACH` (CTAS, INSERT, DELETE, UPDATE, `AT (VERSION => ...)`) |
| `daft_engine/` | `read_iceberg`, scan-task pruning, `write_iceberg` with `overwrite_filter`, why Parquet + `create_table(location=)` fails |
| `platform_sdks/dremio_client.py`, `test_dremio.py` | Dremio REST bootstrap, Arrow Flight queries, Hadoop pointer, CTAS/INSERT interop, decimal pitfall |
| `platform_sdks/spice_client.py`, `test_spice.py` | Spice over Flight: aggregate, federated join with CSV, freshness, pushdown, read-only vs `read_write` |
| `platform_sdks/test_bauplan.py` | Offline signature check of the Bauplan client calls |
| `engine_consistency/consistency.py` | Same KPI query through 7 read paths at one snapshot id |
| `engine_consistency/capabilities.py` | Probes that run each write/read capability for real |
| `config/spice/spicepod.yaml` | Iceberg datasets (read-only and `read_write`) + CSV dataset |

## Prerequisites

Docker with Compose v2, about 6 GB free RAM (Dremio alone has a 4 GB limit). Pinned versions: PyIceberg
0.9.1 (+ pyiceberg-core 0.4.0), PyArrow 19.0.1, Polars 2.0.0, DuckDB 1.5.5 (+ extension wheels), Daft
0.7.26, Dremio OSS 26.0.5, Spice 2.3.2, bauplan 0.3.4, `apache/iceberg-rest-fixture:1.8.1`, SeaweedFS.

`config/s3.json`, the compose defaults (`${AWS_ACCESS_KEY_ID:-admin}`), the Spicepod S3 keys and the
Dremio defaults (`DREMIO_USER` / `DREMIO_PASSWORD`) are **local-only dev credentials**: change them
consistently (and use secret stores) anywhere outside a throwaway sandbox.

## How to run

```bash
docker compose -p iceberg-python-lakehouse up -d --build
docker compose -p iceberg-python-lakehouse exec runner python -m pytest -v
docker compose -p iceberg-python-lakehouse exec runner python -m pytest -v pyiceberg_lifecycle   # one suite
docker compose -p iceberg-python-lakehouse down -v
```

On Git Bash for Windows prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`. No ports are published.
Dremio needs 1-2 minutes to start; its tests wait for it.

## Fixes

- Renamed the suite folders to topic names (`pyiceberg_lifecycle`, `polars_engine`, `duckdb_engine`,
  `daft_engine`, `platform_sdks`, `engine_consistency`) and updated every import and `pytest.ini` testpaths.
- Snippet test module renamed to `test_snippets.py`; test functions and helpers renamed (`_sql_catalog`,
  `test_overwrite_snippet_fails_and_fix`, `test_sales_lifecycle_needs_the_same_overwrite_fix`,
  `test_flight_query`, `DOCUMENTED_CALLS`, table `cleaned_by_location`, ...).
- Spice namespace renamed to `spice_demo` (in `seed_spice.py`, `spicepod.yaml`); Spicepod name
  `python_lakehouse`; compose project `iceberg-python-lakehouse`.
- `docker-compose.yml`: S3 credentials as `${AWS_ACCESS_KEY_ID:-admin}` / `${AWS_SECRET_ACCESS_KEY:-password}`.
- `common/lakehouse.py`: credential defaults marked `# local dev default`.
- `platform_sdks/dremio_client.py`: hardcoded Dremio user/password -> `DREMIO_USER` / `DREMIO_PASSWORD`
  env vars with marked local dev defaults.
- `config/spice/spicepod.yaml`: comment marking the S3 keys as local-only dev credentials (values kept so
  the stack stays runnable; validated as YAML).
- PyIceberg / Polars / DuckDB / Daft calls checked: the code already uses the current forms
  (`overwrite(df, overwrite_filter=)`, `delete(delete_filter=)`, `update_schema()`/`update_spec()` context
  managers, `pl.scan_iceberg(..., storage_options=)`, `DataFrame.write_iceberg`, `daft.read_iceberg`,
  `df.write_iceberg`, `iceberg_scan(..., snapshot_from_id=)`); no API changes were needed. Legacy forms
  appear only inside `pytest.raises` blocks that document their failure.
