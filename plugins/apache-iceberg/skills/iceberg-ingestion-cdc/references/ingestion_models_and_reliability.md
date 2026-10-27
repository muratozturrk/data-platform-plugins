# Ingestion Models and Reliability

Scope: delivery guarantees (at-least-once vs exactly-once), idempotent consumers, bad-record
handling (fail-fast, controlled casting, DLQ), schema drift, and batch vs micro-batch vs streaming.

## Problem
A payments topic feeds an Iceberg table. The job must survive crashes without duplicates, keep
running when a few records are malformed, keep rejects for inspection and pick up new source
fields. A daily batch job next to it must be rerunnable without double-loading a day.

## Requirements first
| Category | Key questions | Typical answers |
|---|---|---|
| Throughput and latency | Volume per period (peak and sustained)? How fresh? | Fraud/monitoring: seconds. Reports, close, regulatory exports: minutes to hours |
| Reliability | Delivery guarantee? Can the source replay? How does the job resume? | Exactly-once with replayable sources; at-least-once + idempotency otherwise |
| Schema management | How do new, renamed, retyped fields reach the table? | Detect, validate, evolve; fail-fast, cast or DLQ |
| Operations | Who runs, orchestrates, monitors it? | Airflow/Step Functions for batch; always-on deployments for streaming |

Iceberg is an analytical (OLAP) sink. Commits faster than a few seconds are discouraged and even
5-second commits are aggressive. Sub-second visibility belongs in an OLTP store or stream state,
with Iceberg as the analytical copy.

## Exactly-once = three parts together
1. **Replayable source** (Kafka offsets, CDC log, versioned files).
2. **Tracked progress**: Spark writes `offsets/N` before batch N and `commits/N` after it.
3. **Atomic, idempotent sink commit.**

```
offsets/N written -> data files written -> Iceberg commit (summary: queryId, epochId=N)
  -> commits/N written -> batch N+1
crash after the Iceberg commit, before commits/N  => restart re-runs batch N with the same offsets
  native Iceberg sink: sees epochId N already committed for this queryId -> skips it
  foreachBatch + append(): no check -> duplicates (at-least-once)
```

- The native sink stores `spark.sql.streaming.queryId` and `spark.sql.streaming.epochId` in each
  snapshot summary. After a forced replay of epoch 1: still 1,500 rows, 0 duplicates, 2 snapshots.
- Same replay through `foreachBatch` + `writeTo(t).append()`: 2,000 rows, 1,000 duplicates.
- Same replay through a `MERGE ... WHEN NOT MATCHED THEN INSERT *` handler: 1,000 rows, 0 duplicates.
- Source cannot replay (webhooks, some APIs) -> at best at-least-once; make the consumer idempotent
  (MERGE on a business key) or dedupe downstream.

```python
# Native sink: exactly-once
(parsed.writeStream.format("iceberg").outputMode("append")
   .option("checkpointLocation", cp).trigger(availableNow=True).toTable("lake.pay.payments"))

# foreachBatch: guarantee = whatever the function makes it. Make it idempotent:
def handle(batch, epoch_id):
    good = validate(batch, cols).where("error IS NULL").select(*names).dropDuplicates(["event_id"])
    good.createOrReplaceTempView("incoming")
    batch.sparkSession.sql("""MERGE INTO lake.pay.payments t USING incoming s
                              ON t.event_id = s.event_id WHEN NOT MATCHED THEN INSERT *""")
```

## Bad records
| Strategy | Behavior | Risk | Use for |
|---|---|---|---|
| Fail-fast | Reject batch / halt query | Downtime when quality varies | Keys, amounts, event time |
| Controlled casting | `try_cast` -> NULL | Silent errors reduce trust | Optional fields |
| Dead-letter queue | Route to side table | Needs inspect/replay process | External, semi-structured sources |
| Hybrid (recommended) | Fail-fast essentials, cast optionals, DLQ the rest | More logic | Most pipelines |

Pattern: parse every payload as `map<string,string>` and cast to the **current table schema**, so the
table is the validation contract and a schema change needs no code change.

```python
m = F.from_json(F.col("value").cast("string"), "map<string,string>")
value = F.expr(f"try_cast(m['{name}'] AS {dtype})")          # NULL instead of failure
checks += [F.when(m.isNotNull() & m[name].isNull(), F.lit(f"missing {name}")),
           F.when(m[name].isNotNull() & value.isNull(), F.concat(F.lit(f"invalid {name}: "), m[name]))]
```

- A DLQ table keyed by `(topic, kafka_partition, kafka_offset)` and written with MERGE stays
  duplicate-free on replay. 200 valid rows landed; 4 rejects: `malformed json`, `missing event_id`,
  `invalid amount: ten`, `invalid event_ts: yesterday`; a missing optional `currency` became NULL.
- Fail-fast: one bad amount fails the query with `StreamingQueryException`; 0 rows, 0 snapshots.

## Schema drift
- Iceberg tracks columns by field ID: add/rename/reorder/drop needs no data rewrite; history is in
  `metadata_log_entries` (schema IDs 0 and 1 after one evolution). v3 adds `variant`.
- A fixed `from_json` schema **silently drops** new source fields (100 rows landed, `device_os`
  missing, visible only in custom metrics). Detect drift explicitly: compare `map_keys(m)` with table columns.
- Add new fields as `STRING` (`ALTER TABLE t ADD COLUMNS (`device_os` STRING)`); widen deliberately
  later. Old rows read NULL. Validate names (`^[a-z_][a-z0-9_]{0,63}$`) before DDL.

## Ingestion models
| Model | Input boundary | Cadence | Spark mechanism |
|---|---|---|---|
| Batch | Defined period/dataset per run | Hourly to weekly | `spark.read` + `overwritePartitions()` |
| Micro-batch | New offsets since checkpoint | Seconds to minutes | `trigger(availableNow=True)` on a schedule |
| Streaming | Unbounded, stateful | Single-digit seconds commit interval | `trigger(processingTime="...")`, or Flink |

No engine writes per record into Iceberg: Spark and Flink both commit bounded units; they differ in
execution model, state and operations. Streaming is the place for stateful work (dedupe,
sessionization, CDC, watermarks). Frequent commits => small files => plan compaction.

### Bounded, idempotent batch
```python
reader = spark.read.option("snapshot-id", source_snapshot_id)   # pin the input, store it with the run
df = reader.table("lake.raw.orders").where(F.col("order_date") == F.lit(day))
df.writeTo("lake.cur.orders").overwritePartitions()              # one atomic `overwrite` snapshot
```
- Rerun with `append`: 1,600 rows for an 800-row day. With `overwritePartitions`: 800, other day untouched.
- After 50 late source rows: pinned rerun loads 800, unpinned rerun 850. "Yesterday" is not a boundary.

### Micro-batch sizing and triggers
- Restart on one checkpoint processes only new offsets (snapshots added 400 then 300).
- `maxOffsetsPerTrigger` is a soft cap split across Kafka partitions by lag: 2,000 records with cap
  1,000 took 3 batches; cap 100 took 21 (84 files, 23.8 rows/file vs 12 files, 166.7 rows/file).
- `processingTime="2 seconds"`: produce-to-visible 1.9-2.6 s; idle triggers commit nothing
  (snapshots = micro-batches with data).

## Troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| Duplicates after restart | `foreachBatch` + `append` replayed an epoch | Native sink, or MERGE on a key |
| Batch retry loads different data | Mutable, unbounded source read | Pin `snapshot-id`, write `overwritePartitions` |
| New fields never appear | Fixed `from_json` schema | Explicit drift check + `ADD COLUMNS` |
| Driver reads 0 rows / stale table after `foreachBatch` | Cloned session + catalog cache | `spark.sql.catalog.<c>.cache-enabled=false` |
| Query dies on one bad record | Fail-fast on optional fields | Hybrid: cast optionals, DLQ |
| Thousands of tiny files | Interval/cap too small | Larger interval/cap, compaction |
