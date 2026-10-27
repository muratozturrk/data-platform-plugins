# Consumption layer examples (Spark + PyIceberg + Trino + Arrow Flight + MCP)

One Iceberg REST catalog on SeaweedFS (S3 API) serves Spark, PyIceberg (Arrow, pandas, Polars,
DuckDB) and Trino, which stands in for the federation/SQL endpoint BI tools connect to. Five test
modules share one environment.

| File | Shows |
|---|---|
| `store_once_read_everywhere/readers.py`, `test_*.py` | Six consumers (Spark SQL, Trino, PyIceberg/Arrow, pandas, Polars, DuckDB) compute identical KPIs, also pinned to one snapshot id; a Parquet extract goes stale; two team "revenue" definitions drift 25.3% |
| `consumption_requirements/requirements.py`, `test_*.py` | Partition pruning, projection, rollup tables, freshness, concurrency measured with `cursor.stats` and PyIceberg `plan_files()`; audit limits turned into PASS/FAIL |
| `open_interfaces/flight_service.py` | Arrow Flight server streaming PyIceberg scans, bearer-token middleware, server-side tenant filter |
| `open_interfaces/mcp_service.py` | FastMCP server with `list_datasets`, `describe_metrics`, `query_metrics`; no free-form SQL; runs on Trino as the caller |
| `open_interfaces/test_open_interfaces.py` | DB-API vs Flight identical rows and transfer speed, Flight auth and tenant isolation, MCP tools and caller permissions |
| `bi_and_ml_tools/bi.py`, `ml.py`, `test_*.py` | One semantic model on Trino/Spark/DuckDB; Superset-style SQLAlchemy and DB-API dashboards; per-merchant row filters, PII masks; engine rules not following the data; snapshot-pinned, hashed training set |
| `tool_selection/selector.py`, `test_tool_selection.py` | Rule-based consumption component shortlist for ten scenarios, each reason labeled `guideline` or `lab assumption` |
| `common/lakehouse.py`, `sales.py`, `semantic.py` | Spark/PyIceberg/Trino clients, deterministic gold `sales` table, metrics + dimensions compiled to ANSI SQL, governed `SECURITY DEFINER` view |
| `config/trino/` | `catalog/iceberg.properties`, `rules.json` (row filters, column masks), `access-control.properties`, `jvm.config` |

## Prerequisites

- Docker with Compose v2, about 6 GB free memory.
- Images: `tabulario/spark-iceberg:latest` (Spark 3.5.5 + Iceberg 1.8.1 when last verified),
  `apache/iceberg-rest-fixture:1.8.1`, `trinodb/trino:476`, `chrislusf/seaweedfs`.
- Extra Python packages in the Spark image: pytest 8.3.4, pyiceberg 0.9.1, polars 1.31.0,
  trino[sqlalchemy] 0.335.0, mcp 1.9.4 (the base image ships pyarrow 17.0.0, duckdb 1.2.1, pandas 2.2.3).

## How to run

```bash
cd examples/consumption_layer
docker compose -p iceberg-consumption-layer up -d --build
docker compose -p iceberg-consumption-layer exec -w /home/iceberg/labs spark-iceberg python -m pytest -v
docker compose -p iceberg-consumption-layer down -v
```

On Git Bash for Windows prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`. No ports are published.

Notes:
- Trino users have no passwords (local only): `admin` (owner), `analyst` (semantic schemas, emails
  masked), `m01`..`m05` (semantic schemas, own merchant rows only).
- Views created by Trino are Iceberg views whose SQL names Trino's catalog (`iceberg`); Spark resolves
  them only with a catalog of that name, and Trino's access rules do not apply there.
- Trino reads Parquet files below `parquet_small_file_threshold` (default 3MB) in one request, so
  projection saves no I/O on small files.
- `config/s3.json` holds local-only SeaweedFS dev credentials (`admin`/`password`); change them together
  with `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`.
- `tabulario/spark-iceberg:latest` is not pinned upstream; pin a digest if a newer image changes Spark or
  Iceberg versions.

## Fixes

- Folders and imports renamed to topic names (`store_once_read_everywhere`, `consumption_requirements`,
  `open_interfaces`, `bi_and_ml_tools`, `tool_selection`); `pytest.ini` testpaths updated.
- Compose project renamed to `iceberg-consumption-layer`; Spark app names are `lakehouse-tests` and
  `lakehouse-consumption`.
- Hardcoded S3 secrets removed from `config/trino/catalog/iceberg.properties`: they use
  `${ENV:AWS_ACCESS_KEY_ID}` / `${ENV:AWS_SECRET_ACCESS_KEY}`, and the `trino` service now receives
  these variables from compose.
- `docker-compose.yml` credentials are `${VAR:-default}` local dev defaults; Python `os.getenv(...)`
  credential defaults are marked `# local dev default`.
- Tool selection: reason labels changed from citation tags to `[guideline]` / `[lab assumption]`
  (the label test was updated), scenario ids are plain names, the expected-picks table is now `REFERENCE_PICKS`
  and the two tests using it were renamed.
- Trino Iceberg connector properties checked and current for Trino 476 (`iceberg.catalog.type=rest`,
  `iceberg.rest-catalog.uri`, `iceberg.rest-catalog.warehouse`, `fs.native-s3.enabled`, `s3.*`).
  PyIceberg 0.9 APIs (`scan(row_filter=..., selected_fields=..., snapshot_id=...)`, `plan_files()`,
  `to_arrow_batch_reader()`, `schema_to_pyarrow`) and the Spark `rewrite_data_files` call
  (`strategy => 'sort'`, `sort_order`, `options => map('rewrite-all','true')`) are current.
