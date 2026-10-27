# In-Process Engines: Polars, DuckDB and Daft on Iceberg

## Problem

PyIceberg plans scans and commits, but has no query engine. Polars, DuckDB and Daft add vectorized
execution in the same Python process, with no server and no JVM. They meet PyIceberg at Apache Arrow and
at the catalog, but each implements a different subset of Iceberg, and each takes object-store settings
differently. Pick by job, not by habit.

| Engine | Reads | Writes | Gets S3 settings from |
|---|---|---|---|
| Polars 2.0 | `pl.scan_iceberg` (lazy, pushdown) | `DataFrame.write_iceberg` append / whole-table overwrite (`@unstable`) | `storage_options` **only** |
| DuckDB 1.5 | Arrow, `iceberg_scan`, `ATTACH` catalog | CTAS, INSERT, DELETE, UPDATE, ALTER via `ATTACH` | DuckDB `SECRET` |
| Daft 0.7 | `daft.read_iceberg` | `df.write_iceberg` append / overwrite with `overwrite_filter` | The PyIceberg table's FileIO |

## Polars

```python
import polars as pl
S3_PROPS = {"s3.endpoint": "http://seaweedfs:8333", "s3.access-key-id": KEY,
            "s3.secret-access-key": SECRET, "s3.region": "us-east-1", "s3.path-style-access": "true"}

lf = pl.scan_iceberg(table, snapshot_id=None, storage_options=S3_PROPS)   # or a metadata.json path
out = (lf.filter(pl.col("passenger_count") > 1)
         .group_by("passenger_count").agg(pl.col("trip_distance").mean().alias("avg_distance"))
         .sort("passenger_count").collect())
print(lf.filter(pl.col("pickup_date") == date(2023, 1, 2)).explain())   # files left after pruning
```

- **Endpoint is not inherited.** `pl.scan_iceberg(table)` ignores the table's `s3.endpoint` and calls
  `s3.us-east-1.amazonaws.com` (`OSError` naming `amazonaws.com`). Pass `storage_options`; PyIceberg-style
  keys are accepted.
- Lazy filters reach Iceberg planning: 5 files unfiltered, 1 for a single `pickup_date`, 0 for
  `total_amount > 10000` (above every file's max). Read `explain()` (`SCAN [...]`, `... N other sources`)
  rather than timing queries.
- `write_iceberg(target, mode)` converts to Arrow and calls `table.append` / `table.overwrite`:
  - Polars infers `Int64` for Python ints; appending to an Iceberg `int` fails with `Mismatch in fields`.
    Cast to the table schema first:
    ```python
    target = table.schema().as_arrow()
    df.cast({c: pl.from_arrow(target.empty_table()).schema[c] for c in df.columns}) \
      .write_iceberg(table, mode="append")
    ```
  - Missing non-partition columns are written as null; a missing **partition source** column raises `KeyError`.
  - `mode="overwrite"` has no filter: 5,000 rows across 5 days became 2. Time travel still returns 5,000.
    Use PyIceberg `overwrite_filter` for partition-scoped replacement.
  - Passing a table **name** calls `load_catalog()` with no name: configure the `default` catalog
    (`PYICEBERG_CATALOG__DEFAULT__*` or `~/.pyiceberg.yaml`).

## DuckDB: three routes

| Route | How | Extension | Sees latest commit |
|---|---|---|---|
| Arrow | `con.register("t", table.scan().to_arrow())` or `scan.to_duckdb("t", connection=con)` | No | Yes (PyIceberg loads via catalog) |
| File | `iceberg_scan('s3://.../metadata.json')` | Yes | **No**: fixed metadata file |
| Catalog | `ATTACH 'warehouse' AS c (TYPE iceberg, ...)`, then `c.ns.table` | Yes | Yes, and can write |

```sql
LOAD iceberg; LOAD httpfs;
CREATE SECRET lake_s3 (TYPE s3, KEY_ID '...', SECRET '...', REGION 'us-east-1',
                       ENDPOINT 'seaweedfs:8333', URL_STYLE 'path', USE_SSL false);
SELECT count(*) FROM iceberg_scan('s3://warehouse/.../00002-....metadata.json', snapshot_from_id=123);
ATTACH 'warehouse' AS iceberg_catalog (TYPE iceberg, ENDPOINT 'http://rest:8181', AUTHORIZATION_TYPE 'none');
SHOW TABLES FROM iceberg_catalog.taxi;
CREATE TABLE iceberg_catalog.taxi.daily_trips AS SELECT ... FROM iceberg_catalog.taxi.trips WHERE ...;
INSERT INTO iceberg_catalog.taxi.daily_trips VALUES (1001, 3.2, 15.75);
DELETE FROM iceberg_catalog.taxi.dml WHERE total_amount < 20;
UPDATE iceberg_catalog.taxi.dml SET passenger_count = 9 WHERE trip_id % 100 = 3;
SELECT count(*) FROM iceberg_catalog.taxi.dml AT (VERSION => 123);   -- time travel
BEGIN; DELETE FROM ... WHERE pickup_date = DATE '2023-01-01'; INSERT INTO ... ; COMMIT;  -- partition replace
```

- Time travel in `iceberg_scan` is `snapshot_from_id=<id>`; `snapshot_id := '...'` raises
  `BinderException: Invalid named parameter "snapshot_id"`. On attached tables use `AT (VERSION => id)`.
- `ATTACH` to an unauthenticated REST catalog needs `AUTHORIZATION_TYPE 'none'`; the first argument is
  the warehouse name. Authenticated catalogs use an `iceberg` secret (OAuth2 client id/secret).
- DELETE and UPDATE write **position delete files** (merge-on-read): snapshots `append, delete, overwrite`,
  2 position delete files, no equality deletes; PyIceberg reads the result correctly. `total-records`
  counts physical rows, so it exceeds the row count readers return.
- CTAS + INSERT produce ordinary Iceberg tables: 1,200 + 2 rows read back by PyIceberg, snapshots
  `append, append`. Always verify a writer's commits with a different reader.
- The Arrow route and `iceberg_scan` returned identical top-10 rows, also at an old snapshot.
- DuckDB reads equality deletes; PyIceberg 0.9.x, Polars and Daft do not.
- Offline / locked-down installs: the `duckdb-extension-iceberg`, `-httpfs`, `-avro` PyPI wheels are
  version-locked to DuckDB; install them with `duckdb_extensions.import_extension(...)`, then only `LOAD`.
- `con.execute(...).arrow()` returns a `RecordBatchReader` in 1.5; use `fetch_arrow_table()` for a table.

## Daft

```python
import daft
from daft import col

df = daft.read_iceberg(table)                       # snapshot_id=... for time travel
res = (df.filter(col("total_amount") > 100).groupby("passenger_count")
         .agg(col("trip_distance").mean()).sort("passenger_count").to_pydict())
df.explain(show_all=True)                           # "Num Scan Tasks = N"
log = df.where(col("total_amount") > 50).write_iceberg(target, mode="append").to_pydict()
day1 = col("pickup_date") == date(2023, 1, 1)
src.where(day1).write_iceberg(target, mode="overwrite", overwrite_filter=day1)
arrow = df.to_arrow(); arrow_schema = df.schema().to_pyarrow_schema()
```

- Daft takes the S3 endpoint from the PyIceberg table's FileIO: no extra config.
- One scan task per data file: 5 unfiltered, 1 for a `pickup_date` filter, 0 for `total_amount > 10000`.
  Without a partition filter Daft logs a full-table-scan warning.
- `write_iceberg` commits snapshots directly and returns a log (`operation`, `file_name`): 5 `ADD` files
  for a 5-day append. A day-1 overwrite with `overwrite_filter` leaves exactly the 3 new rows on that day.
- `agg({"trip_distance": ["mean"]})` raises `ValueError: ... only accepts expression type`: use expressions.
- `to_arrow_table()` and `schema().to_arrow()` do not exist: `to_arrow()`, `schema().to_pyarrow_schema()`.
- **Parquet + `create_table(location=...)` registers nothing**: the table at that location has 0 rows.
  `add_files` then refuses the files because Daft kept the source table's Iceberg field IDs. Use
  `write_iceberg`. `write_parquet` returns `bucket/key` paths without the `s3://` scheme.
- Row deletes and schema changes: no Daft API, use PyIceberg.

## Choosing an engine

| Condition | Choice | Reason |
|---|---|---|
| Interactive exploration, feature engineering | Polars `scan_iceberg` + lazy API | Pruning + fast single-node engine |
| Ad hoc SQL, SQL ETL into the lakehouse | DuckDB with `ATTACH` | Full writer through the catalog |
| Extension unavailable (air-gapped) | DuckDB Arrow route | Only PyIceberg needed; PyIceberg prunes |
| One-off read without catalog access | `iceberg_scan(metadata.json)` | Must know the current metadata file |
| Tables with equality deletes | DuckDB | Only Python engine here that applies them |
| Scans that outgrow one process | Daft (optionally on Ray) | Distributed execution, same API |
| Write transformed results, partition-scoped | Daft `overwrite_filter` or PyIceberg | Polars overwrite is whole-table |

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Polars `OSError ... amazonaws.com` | Endpoint not inherited | `storage_options=S3_PROPS` |
| Polars `Mismatch in fields` | `Int64` vs Iceberg `int` | Cast to table schema |
| Polars `KeyError: pickup_date` on write | Partition source column missing | Include it |
| DuckDB `Invalid named parameter "snapshot_id"` | Wrong parameter name | `snapshot_from_id=` or `AT (VERSION => ...)` |
| DuckDB result is stale | `iceberg_scan` on an old metadata file | `ATTACH` the catalog |
| Daft `only accepts expression type` | Dict aggregation | `col(...).mean()` |
| Daft-written Parquet invisible | Location is not a registration | `write_iceberg` |
