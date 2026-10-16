# Hybrid Source

Category: Data Value.

## Problem
A streaming job computing user sessions passes all tests and is ready for release, except for
one detail: it must first be backfilled with historical data from the lakehouse, and then
continue processing from the real-time layer. You need a way to initialize the
job and later switch to real-time processing.

## Solution / Mechanics
1. **Define all historical sources** the job needs before switching to real time.
2. **Define dataset boundaries**: first and last processed positions in each historical
   source, and the start position of the real-time source. This avoids:
   - *getting stuck in the historical dataset* when a historical source is still active
     (continuously receiving data);
   - *interleaved records*: with a Zero-ETL synced table as history and its Kafka topic as real
     time, the same records exist in both, so the job processes them twice unless the
     boundaries are clearly separated.
3. **Choose the form**:
   - *single job*: chain several readers in the processing logic; some frameworks offer a
     native API (Flink `HybridSource`);
   - *multiple jobs*: historical jobs precede the real-time job, orchestrated externally.
4. **Make retries cheap**: retries must not re-read already processed historical data. Use
   checkpoints, or the orchestrator's state that relaunches only the failed task.

Historical sources can be of any type, including other Kafka topics, as long as you can set
**closed boundaries**: `endingOffsets` in Spark, `setBounded` in Flink.

```
            boundary: end offsets / timestamp T captured up front
 historical:  [archive files] ----> |                                   (bounded)
 historical:  [topic A, off 0..9000] -> |                               (bounded)
 real time :                         [topic B from T] -----------------> (unbounded)
                    single job (HybridSource)  or  job1 >> job2 (orchestrated)
 output    :  -------- continuous result, no gap, no overlap -------->
```

## Consequences & trade-offs
Five issues:
- **Schema consistency**: historical and real-time data may have slightly different schemas.
  Add a conversion step into a common representation, separate from the transformation logic,
  instead of per-schema code.
- **Data overlap**: if you cannot guarantee non-overlapping boundaries, design the processing
  for idempotency so duplicates still produce consistent output.
- **Doing it all at once**: processing all history at once risks OOM on an undersized compute
  layer and I/O errors in external systems not prepared for the load. Treat the historical
  dataset as a stream and process it incrementally.
- **Data filters**: business filters are not boundaries. A historical topic filtered on
  `user_id BETWEEN 1 AND 1000` still receives new records for those users, so the reader never
  finishes. For unbounded historical sources, use technical information (offsets, ingestion
  time) as boundaries.
- **Being stuck in the past**: at first release there may be much more historical than
  real-time data. With 10,000 real-time records per minute and 100,000 historical records to
  process per minute, the job never catches up. Size the compute (CPU, RAM, autoscaling), or
  split the pipeline: a big-hardware part initializes the state, a standard part runs real
  time.

**Alternative**: run the Batch Reprocessor for the backfill in parallel with the streaming
job, separated by time boundaries. Without them, both jobs build partial state for the same
window (the 10:00-10:30 window is created by both jobs and neither has the
full view).

## Key design decisions

| Decision | Options | Notes |
|----------|---------|-------|
| Form | single job (Flink HybridSource, chained queries), multiple orchestrated jobs | single job keeps state across the switch; multi-job allows different compute |
| Boundary type | offsets, table versions, ingestion runs, timestamps | never business filters |
| Switch point | timestamp T used as end of history and start of real time | capture it once, store it with the run |
| Historical read mode | batch (all at once), streaming with `availableNow` + rate limits | streaming mode resumes from checkpoints on retry |
| Real-time start | fire-and-forget submission, wait-for-completion | fire-and-forget frees orchestrator slots |

**When to use / when not to use**

| Use it when | Do not use it when | Alternative |
|-------------|--------------------|-------------|
| A new streaming job needs state from history before going live | The topic retention still covers all the needed history | start the job from the earliest offsets |
| History and real time live in different stores (lakehouse, archive topic) | Backfill and live processing can run independently by time range | Batch Reprocessor in parallel, separated by time boundaries |
| Stateful logic (sessions) must see a continuous stream | You need to fix past output after a bug | Streaming Reprocessor / Batch Reprocessor |

## Gotchas & best practices
- In Spark, state is not carried between two separate queries. For stateful logic spanning the
  switch, use Flink's HybridSource or make the sink idempotent and accept rebuilt sessions at
  the boundary.
- `startingTimestamp` (Spark) and `offsets_for_times` (Kafka clients) return the first offset
  whose timestamp is >= T; records with older timestamps written later are skipped. Use
  `LogAppendTime` topics or offsets when that matters.
- Capture end offsets of an active historical topic at start-up; never wait for "end of topic".
- Rate-limit history (`maxFilesPerTrigger`, `maxOffsetsPerTrigger`) to protect sinks.
- Tag records with their origin (`historical` / `real_time`) for debugging.

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Historical phase never ends | active historical source or business-filter boundary | capture end offsets / versions first |
| Duplicated sessions around the switch | overlapping boundaries | single switch timestamp; idempotent sink |
| OOM during backfill | all history in one batch | incremental reads with limits per trigger |
| Real-time lag keeps growing after the switch | job sized for real time only | bigger compute for the backfill phase, or split jobs |
| Retry reloads all history | batch-mode historical job | `availableNow` streaming with a checkpoint |

## Implementation notes per stack
**Flink**, single job with a native API:

```python
switch_timestamp = KafkaOffsetsInitializer.timestamp(1762144261000)
kafka_source = (KafkaSource.builder().set_bootstrap_servers("localhost:9094")
                .set_topics("visits").set_starting_offsets(switch_timestamp).build())
hybrid_source = HybridSource.builder(file_source).add_source(kafka_source).build()
```

**Spark** - `examples/hybrid_source/spark/hybrid_source_job.py`: two chained queries in one
driver. History is a Delta stream over explicit ingestion runs with `availableNow` and
`maxFilesPerTrigger`; real time reads Kafka from `startingTimestamp`:

```python
historical = (spark.readStream.format("delta").option("maxFilesPerTrigger", 100).table("visits")
              .filter(F.col("ingestion_run").isin(*HISTORICAL_RUNS)))
# ... writeStream.trigger(availableNow=True).start().awaitTermination(), then:
kafka = (spark.readStream.format("kafka").option("subscribe", "visits")
         .option("startingTimestamp", SWITCH_TIMESTAMP_MS).load())
```

**Kafka** - `examples/hybrid_source/kafka/hybrid_consumer.py`: bounded read of an active
archive topic with end offsets captured at start, switch with `offsets_for_times`, schema
harmonization in `to_common_schema`, state kept in-process across the switch.

**Airflow** - `examples/hybrid_source/airflow/hybrid_source_dag.py`:
`process_historical_data >> start_real_time_pipeline`. The historical job runs on big compute
with retries; the real-time job is submitted fire-and-forget
(`spark.kubernetes.submission.waitAppCompletion=false`).

## Related patterns
- [Zero-ETL Synchronizer](zero_etl_synchronizer.md): synced tables are typical historical sources and the classic interleaving risk.
- [Batch Reprocessor](batch_reprocessor.md): the parallel alternative for backfills.
- [Stateful Sessionizer](../../data-value-patterns/references/stateful_sessionizer.md): the stateful job the backfill initializes.
- [Checkpointer](../../error-management-patterns/references/checkpointer.md): makes historical retries resume instead of restart.
- [Local Sequencer](../../data-flow-patterns/references/local_sequencer.md): the multi-job form is a sequence of historical then real-time tasks.
- [Keyed Idempotency](../../idempotency-patterns/references/keyed_idempotency.md): absorbs overlaps at the boundary.
