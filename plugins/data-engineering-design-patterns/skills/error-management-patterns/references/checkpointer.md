# Checkpointer

> Category: hardware/runtime errors. Continuous (streaming) processing.

## Problem

A streaming job counts unique visits in 10-minute windows. Any fatal failure would stop it and
make it **reprocess the data from the beginning**. Unlike batch, a streaming source
is an append-only log with no partitions that tell you what to process next. The job needs a
mechanism that persists its progress, and its computed state, as the query moves on.

## Solution / Mechanics

The job records the **last position in the consumed source** and the **computed state** in
storage that outlives the job's environment.

Where the checkpoint lives:

| Flavour | How | Examples |
| --- | --- | --- |
| Data processing framework based | The framework stores progress metadata in a resilient object store and manages it fully | Spark Structured Streaming, Flink |
| Data store based | The store's SDK keeps the checkpoint | Kafka consumer offsets in `__consumer_offsets`; Kinesis KCL in DynamoDB |

How checkpointing is triggered:

| Implementation | Who checkpoints | Examples |
| --- | --- | --- |
| Configuration driven | You set the frequency/location; the library does it | Spark (`checkpointLocation`, every micro-batch), Flink (`enable_checkpointing(interval)`) |
| Intentional action | Your code confirms after reading and processing | Kafka consumer `commit()` |

```
 Spark micro-batch N                          Kafka consumer poll
 -------------------                          -------------------
 plan offsets ──► offsets/N  (WAL)            poll() ─► process ─► write sink
      │                                                         │
 read + update state ──► state/<ver>          at-least-once:  commit() AFTER write
      │                                       at-most-once:   commit() BEFORE process
 write sink ──► commits/N                     transactional:  offsets in the sink txn
      │
 crash before commits/N?  restart replays N with the same offsets and state
```

Delivery modes affected by checkpoint placement:

| Mode | Checkpoint | Failure effect |
| --- | --- | --- |
| At least once | after processing/writing | retry re-writes, so duplicates are possible |
| At most once | before processing | data lost on processing failure |
| Exactly once | not from checkpointing alone | needs idempotency patterns |

## Consequences & trade-offs

- **Delivery guarantee vs. latency**: tracking positions is cheap, a few numbers per
  partition. Stateful jobs, such as sessions in the Stateful Sessionizer, must also checkpoint
  **state**, which can be far bigger. More frequent checkpoints mean more overhead and less to
  reprocess after a failure. Less frequent checkpoints mean the opposite.
- **Exactly-once feeling**: checkpointing gives only an impression of exactly-once. In a
  distributed job, parallel tasks may finish work and then fail before the checkpoint, and the
  restart reprocesses records that were already written. Exactly-once delivery needs an
  idempotency pattern.
- **Spark vs. Flink cadence**: Spark writes offsets at each iteration, which gives a
  stronger guarantee and fewer duplicates on restart at the cost of per-batch overhead. Flink
  checkpoints on a time interval (`enable_checkpointing(30000, EXACTLY_ONCE)`), and
  `RETAIN_ON_CANCELLATION` keeps checkpoints after cancellation/failure.
- **Asynchronous progress tracking**: Spark 3.4 added async checkpoints that are
  not synchronized with micro-batches. As of 2024 they were experimental and
  had no state-store support.

## Key design decisions

- **Location**: durable, job-independent storage (object store, HDFS). One checkpoint directory
  per query. Never share one between queries.
- **Frequency**: the trigger interval in Spark, `interval` in Flink, commit cadence (poll size)
  in Kafka. Set it from the acceptable replay volume and the latency budget.
- **What is covered**: offsets only (stateless) or offsets plus state (stateful). Choose the state
  backend: RocksDB, and in Spark 3.4+ changelog checkpointing to upload deltas instead of full
  snapshots.
- **Commit placement** (intentional checkpoints): after the write for at-least-once, before the
  write for at-most-once, or inside the sink transaction for the transactional variant.
- **Sink idempotency**: MERGE/upsert by key or the Delta streaming sink. This is what turns
  at-least-once into effectively exactly-once output.
- **Retention**: `spark.sql.streaming.minBatchesToRetain`; Flink retained checkpoints.

## When to use / when not

| Situation | Use? | Alternative |
| --- | --- | --- |
| Any long-running streaming job | Yes, always | none |
| Stateful streaming (windows, dedup, sessions) | Yes, with a state backend | none |
| Batch on partitioned data | Usually not needed | Partition-level reruns (idempotent overwrite) |
| Need exactly-once output | Necessary, not sufficient | + Keyed Idempotency / Transactional Writer |
| Topology/schema change incompatible with old state | Start a new checkpoint | Migrate state, or rebuild from source |

## Gotchas & troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Job restarts from the beginning | No `checkpointLocation`, or a new path on each deploy | Fixed path per query, kept across deploys |
| `startingOffsets` change ignored | The checkpoint takes precedence after the first start | Expected; new checkpoint dir to re-seed |
| Duplicates after restart | At-least-once replay of the last batch/poll | Idempotent sink (MERGE, upsert, Delta `txnVersion`) |
| Data lost after a crash | Commit before processing (at-most-once) or auto-commit | `enable_auto_commit=False`, commit after the write |
| Restart fails: incompatible state | Changed aggregation keys/schema of a stateful query | New checkpoint + backfill, or keep the operator compatible |
| Batches slow, checkpoint upload dominates | Large state snapshots each batch | RocksDB + changelog checkpointing, longer trigger |
| Flink checkpoints gone after cancel | Default cleanup | `RETAIN_ON_CANCELLATION` |
| Consumer re-reads after rebalance (transactional variant) | Not seeking to the DB-stored offsets | Seek in `on_partitions_assigned` |

## Implementation notes per stack

**Spark Structured Streaming**: `examples/checkpointer/spark/spark_streaming_checkpointer.py`
:

```python
query = (unique_visits.writeStream.outputMode("update")
         .option("checkpointLocation", args.checkpoint_dir)
         .trigger(processingTime="1 minute")          # = checkpoint frequency
         .foreachBatch(upsert_windows(output_path))   # MERGE on window: replay-safe
         .start())
```
```
$ cat checkpoint/offsets/18
v1
{"batchWatermarkMs":...,"batchTimestampMs":...,"conf":{...}}
{"visits":{"1":1276,"0":1224}}
```
`inspect` compares `offsets/N` and `commits/N` to show whether a restart replays a batch.

**Kafka**: `examples/checkpointer/kafka/kafka_offset_checkpointer.py`:

```python
if args.mode == "at-most-once":
    consumer.commit(); write_batch(...)
elif args.mode == "at-least-once":
    write_batch(...); consumer.commit()
else:  # transactional: offsets UPSERTed in the same DB transaction, seek on assignment
    write_batch(conn, group, records, store_offsets=True)
```

**Airflow**: `examples/checkpointer/airflow/checkpointer_dag.py`. Every 15 minutes it checks the
age of the last `commits/N`, the planned-vs-committed gap, and the Kafka lag computed from the
checkpointed offsets. It fails, which pages someone, when the query is stuck or down.

Flink equivalent:
`env.enable_checkpointing(30000, mode=EXACTLY_ONCE)` plus
`enable_externalized_checkpoints(RETAIN_ON_CANCELLATION)`.

## Related patterns

- **Windowed Deduplicator / Late Data Detector**: their state and watermark live in the checkpoint.
- **Dead-Letter**: keeps the job alive on poison pills. The Checkpointer makes stopping on real
  failures cheap.
- **Stateful Sessionizer**: an example of large state that drives checkpoint
  cost.
- **Keyed Idempotency / Transactional Writer**: required for exactly-once delivery on top
  of checkpoints.
