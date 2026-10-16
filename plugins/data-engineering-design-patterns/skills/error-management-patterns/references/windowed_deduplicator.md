# Windowed Deduplicator

> Category: data error, delivery semantics. Batch and streaming.

## Problem

A batch job processes visit events synchronized from the streaming layer to an object store and
exposes them directly to business users. It must guarantee **exactly-once processing for each
distinct record**, but the streaming layer contains duplicates caused by producers' automatic
retries. Distributed systems rarely deliver exactly once. At-least-once is the
normal case.

The key idea: you can only deduplicate data that is **limited**. In batch the limit
is the processed dataset. In streaming it is a time-based window.

## Solution / Mechanics

1. **Pick the deduplication attributes** that make a record unique, e.g. `visit_id` plus
   `visit_time`, or `type, full_name, version`.
2. **Define the scope**:
   - batch: the currently processed dataset. Including datasets processed in the past is
     possible, but costs more compute and time;
   - streaming: a **time-based window** during which already-seen keys are kept in a **state
     store**.
3. **Remove duplicates**:
   - batch: `DISTINCT`, `dropDuplicates`, or `ROW_NUMBER() OVER (PARTITION BY keys) = 1`
    . There is no explicit window. The implicit global window is the
     whole dataset;
   - streaming: for each record, look the key up in the state store. Seen means drop. Unseen
     means emit and remember. Expire keys when the window passes them.

```
 streaming                                         batch
 ---------                                         -----
 record(k, t) --> state store lookup               partition D
                   |  seen?                           |
           yes <---+---> no                           v
           drop          emit + remember k         GROUP BY / ROW_NUMBER() over keys
                         |                            |
   watermark = max(t) - window                        v
   keys with t < watermark are evicted             one row per key --> overwrite D
   records with t < watermark are late (ignored)
```

State store types, each a performance vs. consistency trade-off:

| Store | Where state lives | Pros | Cons |
| --- | --- | --- | --- |
| Local | memory only | fastest | lost on failure |
| Local with fault tolerance | memory/disk + periodic remote snapshot | fast reads | the snapshot costs latency; snapshot frequency trades consistency for speed |
| Remote | external store (Redis, DB) | fault tolerant by design | per-record latency and cost |

## Consequences & trade-offs

- **Space versus time** (streaming): you never see all the data, so you only look for
  duplicates inside the window. A short window misses late duplicates but needs little state. A
  long window catches more and needs more memory and storage for unique keys.
- **Idempotent producer**: correct deduplication still does not give exactly-once
  *delivery*. Retries after transient errors can re-emit already-deduplicated records. Combine it
  with an idempotency pattern to get exactly-once output.
- **Automatic retries**: a restarted job can reprocess records it already
  emitted, despite the dedup logic. This is usually accepted in exchange for automatic recovery.
- **Watermark doubles as the late-data boundary**: records older than the watermark are
  dropped as late, not only deduplicated.

## Key design decisions

- **Attributes**: in Spark streaming the event-time column **must** be among the keys of
  `dropDuplicates`. Otherwise the state never expires (e.g.
  `["visit_id", "visit_time"]`). If retries change the event time, use
  `dropDuplicatesWithinWatermark(["visit_id"])` (Spark 3.5+).
- **Window length**: the longest delay between two copies you still want to catch. That is the
  producers' retry horizon, not the business lateness.
- **Which copy wins** (batch): `dropDuplicates` keeps an arbitrary row, which is fine for
  identical copies. Use `ROW_NUMBER()` with an `ORDER BY` (e.g. `ingested_at`) when copies differ
  and the result must be deterministic across reruns.
- **Batch scope**: a single partition (cheap). For a lookback over past outputs, anti-join keys
  already published (more correct, more expensive).
- **State store**: RocksDB in Spark (local with fault tolerance) for big key sets. For a
  multi-instance consumer you need per-partition state or a remote store.

## When to use / when not

| Situation | Use? | Alternative |
| --- | --- | --- |
| At-least-once source, consumers need distinct records | Yes | none |
| Duplicates spread over days in a stream | Partly (window cost) | Batch dedup over the day + lookback |
| Need exactly-once **output** after job retries | Not alone | + Keyed Idempotency / Transactional Writer / Merger |
| Updates to the same entity (latest state wanted) | No: that is not duplication | Merger / Stateful Merger |
| Source already exactly-once (e.g. Kafka transactions end to end) | No | none |

## Gotchas & troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Streaming state grows forever | No watermark, or the event-time column is not in the dedup keys | `withWatermark(t, w)` + `dropDuplicates([id, t])` |
| Duplicates still reach output | Copies arrive more than one window apart, or have different event times | Longer window or `dropDuplicatesWithinWatermark([id])`; batch post-dedup |
| Rows silently missing | Records older than the watermark are dropped as late | Size the window for lateness too; capture late data (Late Data Detector) |
| Different row kept on each rerun | `dropDuplicates` on non-identical copies | `ROW_NUMBER()` with a deterministic `ORDER BY` |
| Duplicates appear after a job restart | Output re-emitted by the replayed micro-batch / poll | Idempotent sink (Delta streaming sink, MERGE, keyed writes) |
| Kafka dedup forgets keys after restart | Local state without snapshot | Snapshot state before committing offsets, or use a remote store |
| `SELECT * EXCEPT` fails in Spark SQL | Not supported in OSS Spark | List columns explicitly |

## Implementation notes per stack

**Spark batch**: `examples/windowed_deduplicator/spark/spark_windowed_deduplicator_batch.py`:

```python
deduplicated = dataset.dropDuplicates(["type", "full_name", "version"])
window = Window.partitionBy(*keys).orderBy(F.expr("ingested_at"))
deduplicated = df.withColumn("position", F.row_number().over(window)).filter("position = 1")
(result.write.format("delta").mode("overwrite")
   .option("replaceWhere", f"partition_date = '{ds}'").save(output_path))         # rerunnable
```
`--lookback-days N` anti-joins (null-safe) the keys found in the N previous output partitions.

**Spark streaming**: `examples/windowed_deduplicator/spark/spark_streaming_deduplicator.py`
:

```python
deduplicated = (prepared.withWatermark("visit_time", "10 minutes")
                .dropDuplicates(["visit_id", "visit_time"])
                .drop("visit_time", "visit_id"))
# --mode within_watermark: .dropDuplicatesWithinWatermark(["visit_id"])  (Spark 3.5+)
```

**Kafka**: `examples/windowed_deduplicator/kafka/kafka_streams_deduplicator.py`. A
`WindowedDeduplicationState` keeps a `seen` dict plus a heap ordered by event time. The watermark
is `max_event_time - window`. Eviction is O(log n). The per-poll order is produce, `flush()`,
`snapshot()`, `commit()`.

**Airflow**: `examples/windowed_deduplicator/airflow/windowed_deduplicator_dag.py`. FileSensor,
then `SparkSubmitOperator` running the batch job, then a check of the `duplicate_ratio` from the
metrics JSON against a Param threshold. A jump in that ratio signals a change in producer
retries.

## Related patterns

- **Late Data Detector**: the same watermark bounds the dedup state and the late-data boundary.
- **Dead-Letter**: replays re-emit records, so deduplicate downstream of a replay.
- **Checkpointer**: persists the dedup state store and the offsets together (Spark).
- **Keyed Idempotency / Transactional Writer**: turn exactly-once processing into
  exactly-once delivery.
- **Merger**: for "keep the latest version of an entity" rather than "drop copies".
