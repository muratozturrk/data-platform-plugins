# PyIceberg: Catalog, Commits, Evolution and Scan Planning

## Problem
Managing Iceberg tables usually means a Spark or Flink cluster. PyIceberg is the official pure-Python
client: it talks to a catalog, writes Parquet through PyArrow, commits snapshots and plans scans with
partition values and column statistics. It is the **control plane** of a Python lakehouse; Polars, Daft
and the Arrow route of DuckDB rely on it for catalog access and planning, so anything PyIceberg cannot
read (equality deletes in 0.9.x) they cannot read either.

## Install
| Need | Extra |
|---|---|
| Arrow I/O (S3 via PyArrow's native filesystem) | `pyiceberg[pyarrow]` |
| SQLite SQL catalog | `pyiceberg[sql-sqlite]` (`pyiceberg[sql]` does **not** exist; pip only warns) |
| Postgres SQL catalog | `pyiceberg[sql-postgres]` |
| Write bucket / truncate partitioned tables (0.9.x) | `pyiceberg[pyiceberg-core]` (else `No module named 'pyiceberg_core'`) |

`pyiceberg[s3fs]` backtracks for a long time during resolution; `[pyarrow]` with pinned versions installs
fast. PyIceberg 0.9.1 requires `pyarrow<20` (19.0.1 is the newest accepted).

## Catalog loading
```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("lake", **{
    "type": "rest", "uri": "http://rest:8181",
    # FileIO properties: required for MinIO/SeaweedFS-style endpoints
    "s3.endpoint": "http://seaweedfs:8333", "s3.access-key-id": KEY,
    "s3.secret-access-key": SECRET, "s3.region": "us-east-1", "s3.path-style-access": "true",
})
local = load_catalog("default", type="sql", uri="sqlite:///demo.db", warehouse="file:///tmp/wh")
```

- Config can also come from `~/.pyiceberg.yaml` or `PYICEBERG_CATALOG__<NAME>__<PROP>` env vars
  (`__` becomes `.` / `-` per segment, e.g. `PYICEBERG_CATALOG__DEFAULT__S3__ENDPOINT`). They are read
  **once at import time**: set them before `import pyiceberg`.
- `load_catalog()` without a name resolves the `default` catalog; Polars `write_iceberg("ns.table")`
  depends on that.
- Other types: `glue`, `hive`, `dynamodb`; REST covers Polaris, Nessie, Unity, Glue REST.

## Create, append, overwrite, delete, upsert
```python
from pyiceberg.expressions import EqualTo, LessThan, GreaterThan, And, GreaterThanOrEqual
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform

spec = PartitionSpec(PartitionField(source_id=5, field_id=1000,
                                    transform=IdentityTransform(), name="pickup_date"))
t = catalog.create_table("taxi.trips", schema=TRIPS_SCHEMA, partition_spec=spec)
t.append(arrow_table)                                           # one snapshot per call
t.overwrite(day_rows, overwrite_filter=EqualTo("pickup_date", "2023-01-02"))  # replace one day
t.delete(delete_filter=LessThan("total_amount", 10.0))          # copy-on-write in 0.9.x
res = t.upsert(changes, join_cols=["trip_id"])                  # 0.9+: res.rows_updated / rows_inserted
```

- `create_table` also accepts a `pyarrow.Schema` (field IDs assigned), e.g. `pq.read_schema(path)`.
- `overwrite()` takes the data **and** the filter and commits in one call. The legacy form
  `table.overwrite(where=...)` + `.add_file(...).commit()` raises `TypeError`.
- `overwrite(df)` with no filter replaces the whole table.
- `upsert` matches on `join_cols` (or the table's identifier fields), updates changed rows and inserts
  new ones in one commit; source keys must be unique. Not part of the measured stack: test on your version.
- `t.add_files(file_paths=[...])` registers existing Parquet files without rewriting them, but refuses
  files whose Parquet schema already carries Iceberg field IDs (`NotImplementedError: ... field IDs`).

### Snapshot operations produced (identity-partitioned on `pickup_date`)

| Call | Snapshot operations |
|---|---|
| `append` | `append` |
| Partition-aligned `overwrite` | `delete` (whole files dropped) + `append` |
| `overwrite` whose filter cuts through files (e.g. unpartitioned) | `overwrite` (files rewritten) + `append` |
| `delete(filter)` | `overwrite`, `total-delete-files` = 0 (copy-on-write) |

5,000 rows over 5 days appended in one call give 5 data files (one per partition value) and 1 snapshot;
a second 500-row append adds 5 more files (10) and never rewrites. Overwriting day 2 with 10 trips leaves
the other 4 days byte-identical (same count and sum) and the log reads `append, delete, append`.

## Transactions
```python
with t.transaction() as tx:                 # one atomic catalog commit on exit
    tx.overwrite(day_rows, overwrite_filter=EqualTo("pickup_date", "2023-01-02"))
    tx.set_properties({"owner": "etl"})
    with tx.update_schema() as u:
        u.add_column("pickup_borough", StringType())
```

Use a transaction when a data change and a metadata change must become visible together; readers see
either none or all of it. Concurrent commits are retried against the new base metadata by the catalog's
optimistic concurrency; a conflicting change raises `CommitFailedException`.

## Schema and partition evolution
```python
from pyiceberg.types import StringType
from pyiceberg.transforms import BucketTransform

with t.update_schema() as u:                # commits on exit
    u.add_column("pickup_borough", StringType(), doc="Added by schema evolution")
    # u.rename_column("a", "b"); u.update_column("x", LongType()); u.delete_column("y")
with t.update_spec() as u:
    u.add_field("vendor_id", BucketTransform(4), "vendor_bucket")
```

- Pass a type **object**: `add_column("c", "string")` raises `NotImplementedError: Cannot visit non-type: string`.
- No data is rewritten. 5,000 old rows read `null` in the new column; the old files keep their paths and
  the table holds 2 schemas.
- Partition evolution applies to new files only: the 5 old files keep spec 0 (1 field), new files get
  spec 1 (2 fields), one file per day x bucket. Filters on `vendor_id` still return correct counts across
  both specs.
- `union_by_name(arrow_schema)` adds every missing column from an incoming schema.

## Scans, row filters and planning
```python
scan = t.scan(row_filter=And(GreaterThanOrEqual("pickup_date", "2023-01-02"),
                             LessThan("pickup_date", "2023-01-04")),
              selected_fields=("trip_id", "pickup_date", "total_amount"),
              snapshot_id=None, limit=None)
n_files = len(list(scan.plan_files()))      # deterministic I/O cost before reading a row
arrow = scan.to_arrow()                     # also to_pandas(), to_duckdb(table_name, connection),
                                            # to_arrow_batch_reader() for streaming
```

- Row filters are expression objects or strings (`"total_amount < 10"`). PyIceberg has no query engine:
  hand the Arrow result to Polars/DuckDB for joins and aggregations; use Spark for large compactions.
- Planning is two-level: the manifest list skips manifests by partition summary, manifests skip files
  by partition value and column min/max. On a 5-file, 5-day table: a 2-day range plans 2 files, a filter
  above the column maximum plans 0, `total_amount > 100` plans all 5 (every file holds such rows); the
  residual filter is then applied to rows by PyArrow.
- `to_arrow()` returns strings as `large_string` in 0.9.x.
- `timestamptz` columns need tz-aware UTC Arrow values (`pa.timestamp("us", tz="UTC")`).

## Time travel and snapshot management
```python
first = t.metadata.snapshots[0].snapshot_id
t.scan(snapshot_id=first).to_arrow()                    # read any retained snapshot
[(s.snapshot_id, s.summary.operation.value, s.summary["total-records"]) for s in t.metadata.snapshots]
with t.manage_snapshots() as ms:
    ms.create_tag(snapshot_id=first, tag_name="audit-2023-01")
    ms.create_branch(snapshot_id=first, branch_name="backfill")
t.snapshot_by_name("audit-2023-01")
t.inspect.snapshots(); t.inspect.files(); t.inspect.partitions()   # metadata tables as Arrow
```

- After append(100) and a 1-row day overwrite, `total-records` goes 5,000, 5,100, 4,080, 4,081; the first
  snapshot still reads 5,000 rows from 5 files. Rolling back is pointing readers at an older id; nothing
  is copied.
- Newer releases add `rollback_to_snapshot` / `set_current_snapshot` on `manage_snapshots()` and snapshot
  expiration (`t.maintenance.expire_snapshots()`); check the API of your pinned version.
- `StaticTable.from_metadata("s3://.../vN.metadata.json", properties=S3_PROPS)` reads a table without a
  catalog; `catalog.register_table(ident, metadata_location)` makes it a catalog table.

## Troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| `TypeError` on `overwrite(where=...)` | Legacy API | `overwrite(df, overwrite_filter=...)` |
| `NotImplementedError: Cannot visit non-type: string` | Type given as string | `StringType()` |
| `No module named 'pyiceberg_core'` on write | Bucket/truncate transform | Install `pyiceberg[pyiceberg-core]` |
| `ValueError: PyIceberg does not yet support equality deletes` | Table written by Spice/Flink with equality deletes | Read with DuckDB; compact with an engine that applies them |
| SQLAlchemy missing after `pip install pyiceberg[sql]` | Extra does not exist | `pyiceberg[sql-sqlite]` |
| Env catalog config ignored | Vars set after import | Set `PYICEBERG_CATALOG__*` before importing |
| `Mismatch in fields` on append | Arrow `int64` into Iceberg `int` | Cast to the table schema (`t.schema().as_arrow()`) |
