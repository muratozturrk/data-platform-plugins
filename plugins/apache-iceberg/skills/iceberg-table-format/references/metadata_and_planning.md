# Metadata Tree and Planning-Time Pruning

## Problem
A query on a lake table must decide which files to read. Directory-based layouts (Hive, plain
Parquet folders) find files by LISTing prefixes and opening footers, often at execution time.
On object storage every LIST and footer read is a paid, slow request, and a filter that is not
on the partition column cannot prune at all. Iceberg moves this work into planning: the engine
reads a few small metadata files and gets every data file with its partition values and column
bounds, without touching storage listings or data files.

## The metadata tree

```
catalog ── name -> metadata-location (the only thing the catalog stores)
  metadata.json        table level: schemas, partition specs, sort orders, snapshots, refs, logs
    manifest list      snapshot level (Avro): one row per manifest + partition summaries
      manifest         file level (Avro): data/delete file path, partition tuple, column stats
        data files     Parquet (or ORC/Avro); delete files; Puffin side-cars
```

| File | Written | Key fields | Pruning role |
|---|---|---|---|
| `metadata.json` | Once per commit, never edited | `current-snapshot-id`, `schemas`, `partition-specs`, `refs`, `metadata-log` | Picks the snapshot and the schema/spec to bind filters to |
| Manifest list | Once per snapshot | `manifest_path` (500), `partition_spec_id` (502), `content` (517), `partitions` (507: `contains_null`, `lower_bound`, `upper_bound`) | Step 1: skip whole manifests |
| Manifest | Only for changed files; reused otherwise | `status` (0 existing, 1 added, 2 deleted), `data_file.partition`, `lower_bounds` (125), `upper_bounds` (128), `null_value_counts`, `record_count` | Step 2: skip individual files |

- `content` in the manifest list: 0 = data manifest, 1 = delete manifest.
- Every commit writes a new metadata.json and a new manifest list; unchanged manifests are
  reused. A 5-commit append-only table has 1, 2, 3, 4, 5 manifests per snapshot and reuses
  1, 2, 3, 4 from the parent.
- Old metadata.json files stay on storage byte-identical: the catalog moves its pointer; it does
  not edit files. A table dropped without `PURGE` can be re-attached with
  `catalog.register_table(ident, metadata_location)`.

## Two-level pruning, measured
A 5-day table (one commit per day, `days(order_ts)`, `WRITE ORDERED BY order_id`, 4 files per day,
20 files total). Query: one day and a 50-id range of `order_id`.

| Step | Input | Decision source | Result |
|---|---|---|---|
| Manifest list | 5 manifests | partition summary `[lower, upper]` of `order_ts_day` | 4 manifests skipped, 1 scanned |
| Manifest | 4 data files | partition value, then `order_id` bounds | 3 files skipped, 1 file read |

Spark's Iceberg scan metrics show the same thing:
`totalDataManifest=5, scannedDataManifests=1, skippedDataManifests=4, resultDataFiles=1, skippedDataFiles=3`.
`skippedDataFiles` counts only files inside scanned manifests (3, not 19).

Planning a 3-day filter on a 30-day `days(order_ts)` table with PyIceberg opens exactly 2 metadata
files (manifest list + 1 manifest, both `.avro`), 0 data files, and returns the 3 matching files.
The same filter on `order_ts` against a Hive-style `order_date=YYYY-MM-DD/` layout needs 31 LIST
calls (1 for the partition directories + 1 per partition) and cannot prune; filtering on
`order_date` itself needs 4 LIST calls.

## Hidden partitioning
Partition values are derived by a transform and kept in metadata; no column to remember.

```sql
CREATE TABLE lake.sales.orders (order_id BIGINT, amount DOUBLE, order_ts TIMESTAMP)
USING iceberg PARTITIONED BY (days(order_ts));

-- the filter is on the real column; the planner projects it through days()
SELECT * FROM lake.sales.orders
WHERE order_ts >= '2025-01-10 00:00:00' AND order_ts < '2025-01-11 00:00:00';
```

With the same naive timestamp filter, a Hive-style Parquet layout reads 31 of 31 files
(`numFiles=31`; 1 file only when `order_date = '2025-01-10'` is added), while the Iceberg table
reads 1 file and skips 30.

| Transform | Result type | Typical use |
|---|---|---|
| `identity(col)` | source type | low-cardinality keys (country, tenant) |
| `years/months/days/hours(ts)` | int | time-series facts |
| `bucket(N, col)` | int | spread high-cardinality keys (ids) |
| `truncate(W, col)` | source type | string prefixes, numeric ranges |

## Binary bounds (single-value serialization)
Bounds and partition summaries are stored as bytes; decode them per type before comparing.

| Iceberg type | Encoding |
|---|---|
| `int`, `date` | 4-byte little-endian (`date` = days since 1970-01-01) |
| `long`, `time`, `timestamp`, `timestamptz` | 8-byte little-endian (micros) |
| `float` / `double` | IEEE 754 little-endian |
| `boolean` | 1 byte, `0x00` = false |
| `string` | UTF-8 bytes; may be truncated |
| `decimal(P,S)` | unscaled value, big-endian two's complement |

```python
import struct
from decimal import Decimal

def decode_value(iceberg_type: str, b: bytes):
    if iceberg_type in ("int", "date"):
        return struct.unpack("<i", b)[0]
    if iceberg_type in ("long", "time", "timestamp", "timestamptz"):
        return struct.unpack("<q", b)[0]
    if iceberg_type == "double":
        return struct.unpack("<d", b)[0]
    if iceberg_type == "string":
        return b.decode("utf-8")
    if iceberg_type.startswith("decimal"):
        scale = int(iceberg_type.split(",")[1].rstrip(")"))
        return Decimal(int.from_bytes(b, "big", signed=True)).scaleb(-scale)
    return b
```

- Strings longer than 16 characters get truncated bounds: lower = first 16 characters, upper =
  first 16 characters with the last one incremented (`customer-with-a-` / `customer-with-a.`).
  Numeric, decimal, date and timestamp bounds are exact.
- fastavro applies the Avro `date` logical type to a `days()` partition value, so a raw reader
  gets `datetime.date`, not the spec's integer. Normalize with `(d - date(1970, 1, 1)).days`
  before comparing, or a hand-written planner silently matches nothing.
- Iceberg copies Parquet footer min/max into the manifest: `readable_metrics.<col>.lower_bound`
  in the `files` metadata table equals the Parquet footer statistic.

## Walking the tree by hand
```python
import io, json, fastavro

loc = rest_get(f"/v1/namespaces/{ns}/tables/{name}")["metadata-location"]   # catalog pointer
md = json.loads(read_object(loc))
snap = next(s for s in md["snapshots"] if s["snapshot-id"] == md["current-snapshot-id"])
for m in fastavro.reader(io.BytesIO(read_object(snap["manifest-list"]))):
    if m["content"] != 0:                       # skip delete manifests
        continue
    for e in fastavro.reader(io.BytesIO(read_object(m["manifest_path"]))):
        if e["status"] == 2:                    # deleted entry
            continue
        print(e["data_file"]["file_path"], e["data_file"]["partition"])
```

Cross-check with metadata tables: `t.files`, `t.manifests`, `t.snapshots`, `t.entries`, `t.refs`.

## What helps pruning

| Situation | What helps | Why |
|---|---|---|
| Filters on a time column | Partition transform on that column | Manifest list summaries skip whole manifests |
| Selective filters on a non-partition column | Sort order / `WRITE ORDERED BY` on it | Tight per-file min/max bounds |
| One key's rows spread over all files | Nothing in metadata | Bounds overlap, every file matches |
| Many small commits, one manifest each | Rewrite manifests | Fewer manifests to read during planning |
| Debugging a slow plan | Metadata tables, scan metrics, raw files | Show exactly which files were kept |

## Gotchas
- Never point readers at a table's `data/` prefix. After one copy-on-write `DELETE`, the folder of a
  15,000-row table holds 35,000 rows, including all 5,000 deleted ones.
- Do not name tables `files`, `manifests`, `snapshots`, `refs`: `db.manifests` is parsed as the
  metadata table of `db`.
- Disable AQE (`spark.sql.adaptive.enabled=false`) when asserting on Iceberg scan metrics.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Every file planned for a timestamp filter | Table partitioned by a derived column, not a transform | Repartition with `days(ts)` (partition evolution) |
| `skippedDataFiles=0` with a selective id filter | Data not clustered by that column | `WRITE ORDERED BY col` or sort during compaction |
| Hand planner finds nothing | Bounds compared as bytes or `date` objects | Decode per type; normalize Avro `date` to int days |
| Planning slow on a long-lived table | Thousands of small manifests | `rewrite_manifests`, larger commits |
| `db.files` resolves to the wrong thing | Table named like a metadata table | Rename the table |
