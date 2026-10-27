# Streaming CDC: Kafka + Spark Structured Streaming into Iceberg

Scope: applying Debezium-style change events to an Iceberg table with `foreachBatch` + `MERGE INTO`:
tombstones, many changes per key per batch, ordering by source position, replays, late events,
merge-on-read table layout, and the Flink alternative.

## Problem
A CRM `customers` table is captured with Debezium into Kafka (PostgreSQL connector, JSON converter,
`value.converter.schemas.enable=false`, tombstones on delete). The Iceberg copy must equal the source
after random inserts, updates and deletes, survive replays, and ignore late, older events.

## Event shape
```json
key:   {"id": 7}
value: {"before": {...} | null, "after": {...} | null, "op": "c|u|d|r", "ts_ms": 1736500001015,
        "source": {"connector": "postgresql", "db": "crm", "schema": "public",
                   "table": "customers", "lsn": 1010, "ts_ms": 1736500001010}}
after a delete: same key, value = null   (tombstone, for log compaction)
```
- **Kafka key = primary key**: all changes of one row land in one partition, in order.
- Order across replays, backfills or repartitioning is still not guaranteed; the **LSN decides**.

## Pipeline
```
spark.readStream.format("kafka") -> foreachBatch(handle)
  handle(batch, epoch_id):
    1. drop tombstones (value IS NULL)
    2. parse envelope, id = coalesce(after.id, before.id)
    3. keep latest change per id (row_number over id ORDER BY lsn DESC)
    4. one MERGE INTO with an LSN guard
  -> Iceberg table (merge-on-read) ; checkpoint: offsets + commits
```

## Code
```python
ROW_STRUCT = "STRUCT<id: BIGINT, name: STRING, email: STRING, tier: STRING>"
ENVELOPE = (f"before {ROW_STRUCT}, after {ROW_STRUCT}, op STRING, ts_ms BIGINT, "
            "source STRUCT<db: STRING, table: STRING, lsn: BIGINT, ts_ms: BIGINT>")

def latest_changes(batch):
    changes = (batch.where("value IS NOT NULL")
               .select(F.from_json(F.col("value").cast("string"), ENVELOPE).alias("e"))
               .select(F.coalesce("e.after.id", "e.before.id").alias("id"), F.col("e.op").alias("op"),
                       "e.after.name", "e.after.email", "e.after.tier",
                       F.col("e.source.lsn").alias("_lsn"),
                       F.timestamp_millis("e.source.ts_ms").alias("_source_ts")))
    w = Window.partitionBy("id").orderBy(F.col("_lsn").desc())
    return changes.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")

def handle(batch, epoch_id):
    latest_changes(batch).createOrReplaceTempView("cdc_changes")
    batch.sparkSession.sql("""
      MERGE INTO lake.crm.customers t USING cdc_changes s ON t.id = s.id
      WHEN MATCHED AND s._lsn > t._lsn AND s.op = 'd' THEN DELETE
      WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET
           t.name = s.name, t.email = s.email, t.tier = s.tier,
           t._lsn = s._lsn, t._source_ts = s._source_ts
      WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT (id, name, email, tier, _lsn, _source_ts)
           VALUES (s.id, s.name, s.email, s.tier, s._lsn, s._source_ts)""")

(spark.readStream.format("kafka").option("kafka.bootstrap.servers", bootstrap)
   .option("subscribe", "crm.public.customers").option("startingOffsets", "earliest")
   .option("maxOffsetsPerTrigger", 300).load()
   .writeStream.foreachBatch(handle).option("checkpointLocation", cp)
   .trigger(availableNow=True).start())          # or processingTime="30 seconds"
```

Target table: store the source position and use merge-on-read.
```sql
CREATE TABLE lake.crm.customers (id BIGINT, name STRING, email STRING, tier STRING,
                                 _lsn BIGINT, _source_ts TIMESTAMP)
USING iceberg PARTITIONED BY (bucket(4, id))
TBLPROPERTIES ('format-version'='2', 'write.merge.mode'='merge-on-read',
               'write.update.mode'='merge-on-read', 'write.delete.mode'='merge-on-read');
```

## Why each piece exists
| Piece | Without it |
|---|---|
| Drop tombstones | Null rows break parsing; they carry no change |
| Latest per key | MERGE fails: several source rows match one target row |
| `s._lsn > t._lsn` guard | Replays and late older events overwrite newer state |
| `_lsn` column in table | No idempotency check; also serves as lineage for audits |
| `WHEN NOT MATCHED AND op <> 'd'` | A delete for an unknown key would insert a ghost row |
| MOR table | Every small MERGE rewrites whole data files |

## Measured behavior
- 1,737 Kafka records (237 tombstones) in 6 micro-batches converge to the source's 159 live rows.
- `c, u, d, c, u` on key 1 and `c, d` on key 2 in one batch: only key 1 remains, with final values.
- Forced replay of the last micro-batch (commit-log entry removed): state still equals the source.
- Late update with an older LSN is ignored (`tier` stays `enterprise`).
- 4 MERGE snapshots, none with `deleted-data-files`; live files: 16 data, 12 position delete.

## Limits and gotchas
- **Hard deletes defeat the LSN guard**: once a key is deleted there is no `_lsn` to compare, so an
  older `c`/`u` arriving after the delete re-inserts it. If old events can arrive after a delete, use
  soft deletes (`is_deleted`, keep `_lsn`) and filter in a view, or purge tombstoned keys later.
- `foreachBatch` is at-least-once; the guarded MERGE is what makes it effectively exactly-once.
- `foreachBatch` runs in a cloned session with its own catalog instance; set
  `spark.sql.catalog.<c>.cache-enabled=false` or the driver session reads a stale table.
- `op = 'r'` (snapshot reads) is handled as an upsert by the same MERGE.
- Schema changes in the source: the fixed envelope schema drops new columns silently; add drift
  detection (see [ingestion_models_and_reliability.md](ingestion_models_and_reliability.md)).
- MOR delete files accumulate: schedule `rewrite_position_delete_files` / `rewrite_data_files`,
  and `expire_snapshots`.
- Bucket on the primary key spreads MERGE work; `distribution-mode=hash` keeps one file per
  partition per commit.

## Flink alternative
Flink's Iceberg sink commits on checkpoints (two-phase commit, exactly-once) and supports upserts via
**equality deletes** (`'format-version'='2'`, `'write.upsert.enabled'='true'`, identifier fields =
primary key). Easier to write CDC with; harder and costlier to read and compact. Prefer it for
event-time logic, late data and CDC at scale; plan compaction from day one.

```sql
CREATE TABLE customers (id BIGINT, name STRING, email STRING, tier STRING,
                        PRIMARY KEY (id) NOT ENFORCED)
WITH ('format-version'='2', 'write.upsert.enabled'='true');
```

## Troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| `MERGE` error: multiple source rows matched | Several changes per key in one batch | `row_number()` latest per key |
| Deleted rows come back | Late event after hard delete | Soft deletes |
| Table drifts from source after restart | Unguarded MERGE / plain append | LSN guard, store `_lsn` |
| Parse errors on null values | Tombstones | `where("value IS NOT NULL")` |
| Reads slow down over days | Position/equality deletes pile up | Compaction schedule |
