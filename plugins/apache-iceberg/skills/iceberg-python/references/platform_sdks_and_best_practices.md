# Platform SDKs (Dremio, Bauplan, Spice AI) and Cross-Engine Best Practices

## Problem
Beyond in-process engines, Python teams reach Iceberg through query servers and managed platforms:
Dremio (governed SQL server), Spice AI (federated SQL runtime) and Bauplan (Git-style branches as a
service). They return Arrow over Arrow Flight, but each sees the lakehouse through its own catalog
integration and writes its own flavor of Iceberg. Without a shared catalog and a common snapshot id,
results from different tools cannot be compared or trusted.

## Dremio over Arrow Flight
```python
from pyarrow import flight

client = flight.FlightClient("grpc+tcp://dremio:32010")
token = client.authenticate_basic_token(USER, PASSWORD)     # OSS: basic auth; Cloud: PAT bearer header
opts = flight.FlightCallOptions(headers=[token])
info = client.get_flight_info(flight.FlightDescriptor.for_command(
    "SELECT trip_distance, total_amount FROM lake.warehouse.taxi.trips WHERE total_amount > 100"), opts)
result = client.do_get(info.endpoints[0].ticket, opts).read_all()   # pyarrow.Table
```

- Flight streams columnar batches: large results skip row-by-row conversion. ODBC (`pyodbc` + vendor
  driver) also works but needs the driver installed.
- Setup over REST: `PUT /apiv2/bootstrap/firstuser`, `POST /apiv2/login` (header `_dremio<token>`),
  `POST /api/v3/catalog` for sources, promotion with `format: {"type": "Iceberg"}`.
- **Dremio OSS 26 has no Iceberg REST catalog source**: type `RESTCATALOG` returns 400. Its S3 source
  finds Iceberg tables only in the Hadoop layout (`metadata/vN.metadata.json` + `version-hint.text`).
  Promotion of a REST-catalog table folder fails with 400 until such a pointer exists.
- After a commit through the REST catalog, `ALTER TABLE ... REFRESH METADATA` keeps returning the old
  count (5,000 after a 10-row commit); Dremio follows `version-hint.text`. Only after the pointer moves
  does it return 5,010. In production, use a catalog source that all engines share.
- Dremio CTAS/INSERT write a Hadoop-layout table (`v1`, `v2` + hint). PyIceberg reads it with
  `StaticTable.from_metadata(...)`; `catalog.register_table(ident, metadata_location)` makes it a
  catalog table (6 rows readable).
- A CTAS typed by decimal literals (`VALUES (1, 2.5)` -> `DECIMAL(2,1)`) silently stores a later
  `INSERT ... 32.75` as `0.0`. Cast literals explicitly (`CAST(2.5 AS DOUBLE)`).
- Dremio caches dataset metadata by path: a table re-created at the same location fails with a missing
  snapshot file. Use new names or forget/refresh the dataset.

## Bauplan (managed branching)
| Call | Purpose |
|---|---|
| `bauplan.Client(api_key=...)` | Connect (no network call until an operation runs) |
| `create_branch(branch="dev", from_ref="main")` | Isolated branch |
| `create_namespace(namespace=..., branch=...)` | Namespace on the branch |
| `create_table(table=..., search_uri="s3://.../*.parquet", namespace=..., branch=..., replace=True)` | Iceberg table from Parquet schema |
| `import_data(table=..., search_uri=..., namespace=..., branch=...)` | Load the files |
| `scan(table=..., ref="dev", namespace=..., columns=[...])` | Read as Arrow |
| `merge_branch(source_ref="dev", into_branch="main")`, `delete_branch(branch="dev")` | Publish, clean up |

- Write-audit-publish without copying data: import and validate on a branch, merge atomically.
- All seven calls above bind to the `bauplan` 0.3.4 SDK signatures (`inspect.signature(...).bind`).
  `Client.update_schema(add_columns=...)` does **not** exist in the SDK; schema changes go through
  `create_table`/import on a branch.
- Execution needs an API key and the cloud service. Self-hosted alternative: Nessie or Iceberg branches
  and tags (`manage_snapshots().create_branch`).

## Spice AI (federated SQL)
```yaml
version: v1
kind: Spicepod
name: lakehouse
datasets:
  - from: iceberg:http://rest:8181/v1/namespaces/taxi/tables/trips
    name: trips
    params:
      iceberg_s3_endpoint: http://seaweedfs:8333
      iceberg_s3_region: us-east-1
      iceberg_s3_access_key_id: ${secrets:AWS_ACCESS_KEY_ID}
      iceberg_s3_secret_access_key: ${secrets:AWS_SECRET_ACCESS_KEY}
      iceberg_s3_path_style_access: true
  - from: iceberg:http://rest:8181/v1/namespaces/taxi/tables/trips_rw
    name: trips_rw
    access: read_write            # default is read-only
  - from: file:/app/drivers.csv
    name: driver_ratings
    params: {file_format: csv}
```

```python
client = flight.FlightClient("grpc://spice:50051")          # spicepy's client.query wraps this
info = client.get_flight_info(flight.FlightDescriptor.for_command(
    "SELECT i.trip_id, p.driver_rating FROM trips i JOIN driver_ratings p ON i.driver_id = p.driver_id"))
table = client.do_get(info.endpoints[0].ticket).read_all()
```

- Address datasets by their dataset `name` (`trips`), not `iceberg.default.trips` (that form needs a
  catalog connector).
- `pip install spicepy` from PyPI (1.0.5) installs an unrelated circuit simulator: no `Client`. The Spice
  SDK is distributed from GitHub; `pyarrow.flight` works directly.
- New PyIceberg snapshots become visible without a refresh after about 1 s (1 s results-cache TTL).
- Filters are pushed into the scan: physical plan `IcebergTableScan ... predicate:[total_amount > 100]`.
- Default datasets reject writes (`ArrowInvalid: ... read-only dataset`). With `access: read_write`,
  INSERT and DELETE commit to Iceberg, and **DELETE writes equality deletes** (`added-equality-deletes` = 1).
  PyIceberg 0.9.x then raises `ValueError ... equality deletes`, Polars and Daft fail too; DuckDB reads
  ids 2-10 and 99 correctly.
- A dataset whose table does not exist yet is retried with a growing backoff (17-26 s between attempts):
  create tables before the runtime starts.

## Capability matrix (measured with the pinned versions)
| Library | Time travel | Append | Partition overwrite | Row delete | Schema evolution | Reads equality deletes |
|---|---|---|---|---|---|---|
| PyIceberg 0.9.1 | yes | yes | yes (`overwrite_filter`) | yes (COW) | yes | no |
| Polars 2.0.0 | yes | yes | no (whole table only) | no | no | no |
| DuckDB 1.5.5 | yes | yes | yes (DELETE + INSERT in one transaction) | yes (position deletes) | yes | yes |
| Daft 0.7.26 | yes | yes | yes (`overwrite_filter`) | no | no | no |
| Dremio OSS 26.0.5 | - | yes (CTAS, INSERT) | - | - | - | - |
| Spice 2.3.2 | - | yes (`read_write`) | - | yes (equality deletes) | - | - |

`-` = not measured. Measure capabilities with probes that run each operation on a scratch table and
assert an expected matrix; the test fails when an upgrade adds or removes a feature.

## Consistency: one snapshot, same answers
```python
KPI_SQL = """SELECT passenger_count, count(*) AS trips, sum(total_amount) AS revenue,
                    avg(trip_distance) AS avg_distance FROM {source} GROUP BY 1 ORDER BY 1"""
sources = {
    "duckdb_scan": f"iceberg_scan('{t.metadata_location}', snapshot_from_id={snap})",
    "duckdb_catalog": f"iceberg_catalog.taxi.trips AT (VERSION => {snap})",
}
# PyIceberg: t.scan(snapshot_id=snap); Polars: pl.scan_iceberg(t, snapshot_id=snap, ...);
# Daft: daft.read_iceberg(t, snapshot_id=snap)
```

- Pin the **snapshot id**, not a timestamp. PyIceberg, Polars, DuckDB (Arrow, `iceberg_scan`, ATTACH) and
  Daft returned identical KPIs at two snapshots (5,000 then 4,300 trips). Spice has no snapshot selector
  over Flight; compare it at the current snapshot.
- Compare keys and counts exactly; float sums with a relative tolerance (1e-9), since summation order
  differs per engine.

## Best practices
1. **Use one catalog consistently.** The only stale or divergent results came from catalog bypasses:
   Dremio's file-system source and `iceberg_scan` on a fixed metadata file.
2. **Evolve schemas carefully**: additive changes via `update_schema()`, on a branch first when possible.
3. **Optimize writes up front**: partition on the filter column, batch appends (one file per partition
   value per call), avoid many tiny commits.
4. **Prefer pushdown scans**: filters above the column maximum skipped every file in PyIceberg, Polars and
   Daft; a one-day filter cut 5 files to 1.
5. **One writer style per table**, or verify every reader supports what writers produce: a single Spice
   DELETE made the table unreadable for PyIceberg, Polars and Daft.
6. **Pin versions and keep snippets as tests**: in every engine at least one widely published snippet
   needed a fix with current versions.

## Troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| Dremio count unchanged after commit + REFRESH | Follows `version-hint.text` | Shared catalog source; or republish pointer |
| Dremio `RESTCATALOG` 400 | Not in OSS 26 | S3 source + Hadoop pointer, or a supported catalog source |
| Dremio INSERT stores `0.0` | Decimal type from CTAS literals | Cast literals |
| `from spicepy import Client` fails | Wrong PyPI package | GitHub SDK or `pyarrow.flight` |
| Spice `read-only dataset` | Default access | `access: read_write` (expect equality deletes) |
| PyIceberg `equality deletes` error after Spice write | MOR equality delete | Read with DuckDB; compact; keep Spice read-only |
| Spice slow to start, datasets pending | Missing tables, backoff | Create tables before Spice starts |
