# Structured Streaming and Maintenance on Polaris Tables

## Concept

A Polaris table can be the sink of one stream and the source of the next. Spark's Iceberg source
reads snapshots incrementally, and the Iceberg sink commits one snapshot per micro-batch through
Polaris. The cost is many small files and a long snapshot list. The fix is Iceberg's procedures,
called through the Polaris Spark catalog (`CALL <catalog>.system.<proc>`). Compaction and expiry
in depth are in the `iceberg-maintenance` skill of the apache-iceberg plugin.

## Option reference

| Option | Side | Effect |
|---|---|---|
| `stream-from-timestamp` (ms) | read | First snapshot to replay |
| `streaming-skip-overwrite-snapshots=true` | read | Ignore MERGE/UPDATE/overwrite commits (their rows are never emitted) |
| `streaming-skip-delete-snapshots=true` | read | Ignore DELETE commits (downstream never learns about the deletions) |
| `checkpointLocation` | write | Required. One per query |
| `fanout-enabled=true` | write | Keep one open file per partition, so the input need not be clustered |
| `trigger(processingTime=...)` / `trigger(availableNow=True)` | write | Commit cadence, and with it the file count |

## Worked example

```python
from pyspark.sql.functions import current_timestamp, lit     # lit must be imported too

src = (spark.readStream.format("rate").option("rowsPerSecond", 10).load()
       .withColumn("name", lit("Simulated Name"))
       .withColumn("event_date", current_timestamp()))

spark.sql("CREATE TABLE polaris.db.bucketed_events (id BIGINT, name STRING, event_date TIMESTAMP) "
          "USING iceberg PARTITIONED BY (bucket(8, id))")      # create first, partitioned
q = (src.selectExpr("value AS id", "name", "event_date")
     .writeStream.format("iceberg").outputMode("append")
     .trigger(processingTime="2 seconds")
     .option("checkpointLocation", "/tmp/checkpoints/bucketed_events")
     .toTable("polaris.db.bucketed_events"))
# bound it: q.processAllAvailable() / a timeout, then q.stop()

feed = (spark.readStream.format("iceberg")
        .option("stream-from-timestamp", "1640995200000")
        .option("streaming-skip-overwrite-snapshots", "true")
        .option("streaming-skip-delete-snapshots", "true")
        .load("polaris.db.events"))
```

Maintenance on a small-file streaming table (`db.stream_events`, written unpartitioned by `toTable`):

```python
spark.sql("CALL polaris.system.rewrite_data_files(table => 'db.stream_events', "
          "options => map('min-input-files', '2'))")
current = spark.sql("SELECT max(committed_at) FROM polaris.db.stream_events.snapshots").first()[0]
spark.sql(f"CALL polaris.system.expire_snapshots(table => 'db.stream_events', "
          f"older_than => TIMESTAMP '{current}', retain_last => 1)")
spark.sql("CALL polaris.system.rewrite_manifests('db.stream_events')")
```

## Measured behavior

| Case | Result |
|---|---|
| Incremental read of a table with history `append, overwrite, delete, append`, no skip options | `Cannot process overwrite snapshot: ..., to ignore overwrites, set streaming-skip-overwrite-snapshots` |
| Same with both skip options | Emitted ids 1, 2, 3, 5, 6. Id 4 (inserted by MERGE) was skipped. Id 1 (deleted later) was emitted |
| Rate-source snippet that imports only `current_timestamp` | `NameError: name 'lit' is not defined` |
| Streaming TIMESTAMP `event_date` into a table with STRING/DATE `event_date` | `Cannot write incompatible dataset to table with schema` |
| `toTable()` on a missing table with `fanout-enabled` | Table created unpartitioned (its `.partitions` has no `partition` column) |
| `bucket(8, id)` table, no fanout | Succeeded: 100 rows, 38 files, 6 snapshots |
| `bucket(8, id)` table, fanout on | Succeeded: 110 rows, 44 files, 7 snapshots |
| 25 s, 10 rows/s, 2 s trigger, 16 cores, unpartitioned | 240 rows, 192 files, 13 snapshots (1.2 rows per file) |
| `rewrite_data_files` | 192 files (197,539 bytes) into 1 |
| `expire_snapshots(retain_last => 1)` | Deleted 192 data files, 12 manifests and 13 manifest lists, leaving 1 snapshot |
| `rewrite_manifests` | 13 manifests into 1. Rows stayed 240 throughout |

On Iceberg 1.5.2 with Spark 3.5.5, partitioned streaming writes did not need sorted input or
fanout. Iceberg requests a hash distribution for micro-batches too. Treat this as
version-dependent.

## RBAC notes

- All three procedures ran with a principal whose catalog role had `CATALOG_MANAGE_CONTENT`.
- `expire_snapshots` deletes files. With vending, Polaris must issue write-capable keys for the
  table location, so a read-only principal cannot run maintenance.
- Give the streaming writer, the downstream readers and the maintenance job separate principals,
  so you can revoke each on its own.

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| Stream fails at `Cannot process overwrite snapshot` | Source table has MERGE/UPDATE commits | Skip options, or read changes another way (changelog view or merge-based design) |
| Downstream misses rows or keeps deleted rows | Skip options drop non-append commits | Don't use an Iceberg incremental read as CDC |
| Table created without partitions | `toTable()` created it | `CREATE TABLE ... PARTITIONED BY` before starting the query |
| Test or job never ends | `awaitTermination()` | `processAllAvailable()`, `availableNow`, or a timeout plus `stop()` |
| Thousands of tiny files | One file per task per micro-batch | Longer trigger, fewer shuffle partitions, scheduled `rewrite_data_files` |
| Stream cannot restart after maintenance | Snapshots expired past the checkpoint | Expire only behind the slowest consumer's checkpoint |

## Related

- [connection.md](connection.md), [dataframe-and-sql.md](dataframe-and-sql.md)
- The `iceberg-maintenance` and `iceberg-ingestion-cdc` skills of the apache-iceberg plugin
