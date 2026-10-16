# streaming x batch hybrid: stream joined with a static lookup (manual section 7.4)

Reference scenario, not a separate case in `complex_cases.py`: the module docstring lists a
`StreamBatchJoinCase`, but it was never implemented. The generated code for it comes from
`hybrid_checkpoint` in `cases/__init__.py`, triggered by `lineage.has_streaming_source`
(DSL `.streaming()`).

## Problem

A Structured Streaming query (Kafka, Delta, files) is enriched with a static dimension
table. Four components need four different persistence decisions, and the usual mistake
is to apply batch habits (checkpoint the DataFrame, cache everything) to a streaming plan.

## How Spark executes it

- The streaming query is re-planned every micro-batch: the static side is re-evaluated
  each trigger. Without cache that means a fresh scan of the lookup per batch; with a
  persisted lookup the cached relation is substituted into every batch plan.
- Stream-static joins are stateless (inner, and left outer with the stream on the left).
  State appears with aggregations, `dropDuplicates`, stream-stream joins, and is stored
  in the state store under `checkpointLocation`, together with source offsets and commits.
- `checkpointLocation` is what gives exactly-once with an idempotent/transactional sink
  (Delta, foreachBatch with idempotent writes). It is unrelated to `DataFrame.checkpoint()`.
- `localCheckpoint()` / `checkpoint()` / `count()` cannot be called on a streaming
  DataFrame (AnalysisException: queries with streaming sources must be executed with
  writeStream.start()). Inside `foreachBatch` the batch DataFrame is a normal DataFrame.

Plan signal: `query.lastProgress` -> `durationMs.addBatch` and `getBatch`; the batch
plan in the SQL tab: `BroadcastHashJoin` over `InMemoryTableScan` = cached lookup;
over `FileScan` = re-read every trigger. `stateOperators[].numRowsTotal` growing without
bound = missing watermark.

## Component decisions (manual table, corrected)

| Component | Manual says | Corrected | Why |
|---|---|---|---|
| Static lookup | cache `MEMORY_ONLY_SER` | `persist(MEMORY_AND_DISK)` + `count()` + `F.broadcast()` | `_SER` does not exist in PySpark; avoids a scan per trigger |
| Streaming state | writeStream checkpoint | `checkpointLocation` on HDFS/S3/ABFS, one per query | Offsets + state for exactly-once recovery |
| Intermediate join result | `localCheckpoint` | Nothing | Not allowed on streaming plans; one consumer anyway |
| Result | Delta/Parquet sink | Delta sink (transactional) | Durable, idempotent per batch id |

If the batch output feeds two sinks, use `foreachBatch`: `batch_df.persist()`, write
both, `batch_df.unpersist()`. That is the only place a hybrid job persists an
intermediate.

## sparkopt rule (hybrid_checkpoint)

| Item | Value |
|---|---|
| `name` | `hybrid_checkpoint` |
| `priority` | `Priority.HIGH` (90) |
| `matches()` | `lineage.has_streaming_source` |
| Registration | Built-in, plus DSL `.streaming()` |

Bugs in its generated code (fixed in `examples/streaming_batch_hybrid.py`):
`schema` undefined in `from_json`; the "localCheckpoint the join
result" advice. The Kafka/Delta/`checkpointLocation` part is correct. (Original sparkopt also
used `MEMORY_ONLY_SER`; the bundled copy emits `MEMORY_ONLY`.)

## Before / After

```python
# Before: lookup re-read every micro-batch, checkpoint on local disk
products = spark.read.parquet(lookup_path)
q = (orders.join(products, "product_id", "left").writeStream.format("delta")
     .option("checkpointLocation", "/tmp/chk").start(out))
```

```python
# After
products = (spark.read.parquet(lookup_path).select("product_id", "category", "unit_price")
            .persist(StorageLevel.MEMORY_AND_DISK))
products.count()
orders = (kafka_stream.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
          .select("e.*")
          .withWatermark("event_time", "10 minutes")
          .dropDuplicates(["order_id", "event_time"]))          # bounded state
q = (orders.join(F.broadcast(products), "product_id", "left")
     .writeStream.format("delta").outputMode("append")
     .option("checkpointLocation", "s3a://bucket/_chk/orders_enriched/")
     .trigger(processingTime="30 seconds").start(out))
```

Full runnable version: `examples/streaming_batch_hybrid.py`. Lookup that changes during
the stream: `examples/cached_broadcast.py` (versioned reload inside `foreachBatch`).

## Freshness trade-off

| Lookup handling | Freshness | Cost per trigger |
|---|---|---|
| Cached snapshot | Frozen at start | Broadcast from cache only |
| Uncached Delta table | Latest committed version every trigger | Scan + broadcast |
| foreachBatch + versioned cache | Latest version, reloaded only on change | Version check + broadcast |

## Configs

| Config | Why |
|---|---|
| `checkpointLocation` | Durable, unique per query; never shared, never deleted while the query lives |
| `spark.sql.streaming.stateStore.providerClass` = RocksDB provider (3.2+) | Large state off the JVM heap |
| `spark.sql.shuffle.partitions` | Fixed into the checkpoint at first start for stateful ops; changing it later is ignored |
| `trigger(processingTime=...)` / `availableNow=True` | Batch cadence; `availableNow` for scheduled catch-up |

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Queries with streaming sources must be executed with writeStream.start()` | `count()`/`localCheckpoint()` on the stream | Only in `foreachBatch` |
| Enrichment never sees new products | Cached snapshot | Versioned reload (cached_broadcast) |
| Duplicates after restart | Local `/tmp` checkpoint or non-idempotent sink | Durable checkpoint, Delta sink |
| State grows forever | `dropDuplicates`/agg without watermark | `withWatermark` |
| Restart fails after code change | Incompatible change to stateful operators | New checkpoint location (reprocess) |
| Each batch scans the lookup | Lookup not persisted | Persist + `count()` before `start()` |

## Related cases

- `cached_broadcast`: refreshable lookup.
- `broadcast_join`: broadcast thresholds.
- `staged_materialization`: batch-side rules for checkpoint vs cache.
