# hybrid_checkpoint: streaming x static persistence

## Problem

A Structured Streaming query enriches events with a static lookup table:

```
Kafka events (stream)  --join-->  products (static parquet/Delta)  -->  Delta sink
```

Three different things need "persistence", each with a different mechanism:

| Component | Mechanism | Why |
|---|---|---|
| Static lookup | `persist()` (optional) + `broadcast` | read once instead of every micro-batch |
| Stream progress/state | `writeStream.option("checkpointLocation", ...)` | offsets + state for exactly-once recovery |
| Per-micro-batch intermediates | `persist()` inside `foreachBatch` | reuse within one batch |
| Result | Delta/Parquet sink | durable output |

## How Spark executes it

- Each micro-batch re-plans the query. The static side is re-scanned every micro-batch unless it is
  cached; with Delta, re-scanning means each batch sees the latest table version.
- A stream-static join keeps no state for the static side. Stream-stream joins and aggregations keep
  state in the state store, persisted under `checkpointLocation/state`.
- `checkpointLocation` stores offsets, commits and state. It is not related to
  `SparkContext.setCheckpointDir` and is never cleaned by Spark.
- Streaming DataFrames cannot be `cache()`d, `persist()`ed, `checkpoint()`ed or
  `localCheckpoint()`ed: they raise `AnalysisException: Queries with streaming sources must be
  executed with writeStream.start()`.

Signals: `df.isStreaming == True`; `explain()` shows `StreamingRelation`/`MicroBatchScan`; with
cached static side `InMemoryTableScan` on the build side of `BroadcastHashJoin`; Streaming Query UI
shows per-batch durations.

## sparkopt rule

`HybridCheckpointCase`, priority `HIGH`.

```python
def matches(self, profile):
    return profile.lineage.has_streaming_source
```

The advisor returns `HYBRID` only when `has_streaming_source and estimated_reuse_count >= 2`
(storage_level reported as `MEMORY_ONLY_SER`). Otherwise the streaming flag falls through to the
remaining branches.

Generated code: static lookup `.persist(StorageLevel.MEMORY_ONLY_SER)` + `count()`, Kafka
`readStream` with `from_json`, `stream_df.join(F.broadcast(static_lookup), "key", "left")`,
`writeStream.format("delta").option("checkpointLocation", "hdfs:///checkpoints/...")`,
`trigger(processingTime="30 seconds")`. The advisor variant adds `withWatermark("event_time", "5 minutes")`.

Corrections:

- `MEMORY_ONLY_SER` → `MEMORY_ONLY` (or `MEMORY_AND_DISK`; `_2` if executors are preemptible).
- The docstring's "join result: localCheckpoint" is impossible on a streaming DataFrame. Use
  `foreachBatch` and persist `batch_df` there if a batch is written to several sinks.
- Watermark on a stream-static join with append output does nothing unless there is a stateful
  operator (aggregation, dedup, stream-stream join). Add it where state exists.
- `F.broadcast()` requires the lookup to fit driver and executor memory (broadcast hard limit 8GB).
- Caching the static side freezes it. If the lookup table changes, either do not cache (Delta
  re-reads per batch), or restart / refresh the cache on a schedule.

## Before / After

Before:

```python
lookup = spark.read.parquet(lookup_path).persist(StorageLevel.MEMORY_ONLY_SER)  # AttributeError
events = spark.readStream.format("kafka")...load()
enriched = events.join(lookup, "product_id").localCheckpoint()                   # AnalysisException
enriched.writeStream.format("delta").start(out)                                   # no checkpointLocation
```

After:

```python
lookup = spark.read.parquet(lookup_path).persist(StorageLevel.MEMORY_AND_DISK)
lookup.count()
events = parse(spark.readStream.format("kafka").option(...).load())

def write_batch(batch_df, batch_id):
    batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        batch_df.write.format("delta").mode("append").save(out_main)
        batch_df.filter("amount > 1000").write.format("delta").mode("append").save(out_alerts)
    finally:
        batch_df.unpersist()

query = (events.join(F.broadcast(lookup), "product_id", "left")
         .writeStream.foreachBatch(write_batch)
         .option("checkpointLocation", ckpt_location)     # stable, per query
         .trigger(processingTime="1 minute")
         .start())
```

See `examples/hybrid_checkpoint.py` (uses the `rate` source so it runs without Kafka).

## Configs

| Config | Note |
|---|---|
| `checkpointLocation` option | required for production queries; one dir per query; never reuse across queries |
| `spark.sql.streaming.checkpointLocation` | session default root, query name appended |
| `spark.sql.streaming.minBatchesToRetain` | default 100; metadata retention in the checkpoint |
| `spark.sql.autoBroadcastJoinThreshold` | default 10MB; use `F.broadcast` for larger lookups |

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Queries with streaming sources must be executed with writeStream.start()` | `cache`/`count`/`localCheckpoint` on a streaming DF | move logic into `foreachBatch` |
| Duplicate or lost output after restart | `checkpointLocation` changed/deleted | keep one stable location per query |
| Lookup updates never visible | static side cached | do not cache, or periodic `unpersist()`+re-read in `foreachBatch` |
| Executor OOM on join | broadcast lookup too big | drop `F.broadcast`, let Spark choose SMJ |
| State grows unbounded | stateful op without watermark | `withWatermark` on the event-time column |
| Query fails after code change | incompatible change for existing checkpoint (e.g. new stateful op) | new checkpointLocation (reprocess) |

## Related cases

- `fanout_cache`: static lookup reuse.
- `storage_level`: level for the lookup.
- `lineage_break`: batch reliable checkpoint, a different mechanism.
