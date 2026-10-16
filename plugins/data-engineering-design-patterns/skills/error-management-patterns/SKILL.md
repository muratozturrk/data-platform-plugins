---
name: error-management-patterns
description: "Error management design patterns for batch and streaming pipelines: Dead-Letter, Windowed Deduplicator, Late Data Detector, Static Late Data Integrator, Dynamic Late Data Integrator, Filter Interceptor, Checkpointer, with Spark, Kafka and Airflow code. Use when handling poison pills / unprocessable records, dead-letter queues, duplicates from producer retries, watermarks and late events, lookback backfills of late partitions, finding which filter dropped rows, or restarting streaming jobs from checkpoints."
---

# Error Management Patterns

Data engineers consume data and infrastructure built by others and inherit their problems:
records that cannot be processed, duplicates caused by retries, events delivered late, filters
that silently remove too much, and long-running jobs that crash. These seven patterns each give one of these failure modes an
explicit path, so it does not end in a crash, a silent loss, or a full reprocess. The patterns
improve correctness but do not make delivery exactly-once. Pair them with the idempotency
patterns.

## When to Use This Skill

- A streaming or batch job fails on a few malformed records ("poison pills")
- Error-safe SQL functions (`CONCAT`, `TRY_CAST`) silently return NULL for broken input
- Producers' automatic retries create duplicates that business users must not see
- Events buffered on devices arrive late; windows or state need a watermark
- Daily statistics must include late data for the last N days, or for any past day
- Filtered volume jumps after a release (15% to 90%) and you need to know which filter did it
- A streaming job must restart where it stopped instead of reprocessing the topic

## Pattern Selection Guide

| Symptom / need | Pattern | Reference |
| --- | --- | --- |
| Bad records stop the job; keep the job running and keep the records | Dead-Letter | [dead_letter](references/dead_letter.md) |
| At-least-once delivery, consumers need each distinct record once | Windowed Deduplicator | [windowed_deduplicator](references/windowed_deduplicator.md) |
| Need to know whether an event is late (watermark, allowed lateness) | Late Data Detector | [late_data_detector](references/late_data_detector.md) |
| Late data has a bounded delay ("final after N days") | Static Late Data Integrator | [static_late_data_integrator](references/static_late_data_integrator.md) |
| Late data has any delay; backfill only the impacted partitions | Dynamic Late Data Integrator | [dynamic_late_data_integrator](references/dynamic_late_data_integrator.md) |
| Which filter condition removed the rows? | Filter Interceptor | [filter_interceptor](references/filter_interceptor.md) |
| Streaming job must resume from its last position and state | Checkpointer | [checkpointer](references/checkpointer.md) |

Typical composition: a **Checkpointer** under every streaming job. The **Late Data Detector**
watermark bounds the **Windowed Deduplicator** state. Late output goes to the **Static** or
**Dynamic Late Data Integrator**. **Dead-Letter** catches records that cannot be processed, and
the **Filter Interceptor** explains records removed on purpose.

## Patterns

### Dead-Letter
Keep processing when a record is unprocessable: wrap the likely fail spots with a safety control,
decorate the failed record with its error, and write it to separate dead-letter storage.
Monitor that storage and optionally replay it.
- Mechanic: `try/except` in mapping functions; for error-safe functions an if-else
  (`input NOT NULL AND output NULL`); a resilient, monitorable store (Kafka topic, object store,
  Delta table).
- Gotchas: replay causes **snowball backfilling** and breaks ordering/sessions; flag replayed
  rows `was_dead_lettered`; stop the job above an error ratio; retry
  transient errors and do not dead-letter them.
- [Reference](references/dead_letter.md) · [Examples](examples/dead_letter/)

### Windowed Deduplicator
Remove duplicates inside a bounded scope: the processed dataset in batch, a time window kept in
a state store in streaming.
- Mechanic: batch `dropDuplicates` / `ROW_NUMBER() ... = 1`; streaming `withWatermark` +
  `dropDuplicates([id, event_time])` (or `dropDuplicatesWithinWatermark`, Spark 3.5+).
- Gotchas: the event-time column must be in the streaming keys or the state never expires; the
  window is a space-vs-time trade-off; dedup gives exactly-once processing, not delivery.
- [Reference](references/windowed_deduplicator.md) · [Examples](examples/windowed_deduplicator/)

### Late Data Detector
Classify records as late or on time with a watermark derived from event time.
- Mechanic: MAX event time per partition (monotonic), then MIN or MAX across partitions, then
  `watermark = that - allowed lateness`, never decreasing.
- Gotchas: Spark drops late rows but cannot capture them natively (use `foreachBatch` capture);
  MIN per partition leads to open-close-open loops and getting stuck in the past; MAX plus event
  skew drops slow sources; persist the watermark.
- [Reference](references/late_data_detector.md) · [Examples](examples/late_data_detector/)

### Static Late Data Integrator
Every run reprocesses a fixed lookback window of past partitions in addition to the current one.
- Mechanic: Airflow Dynamic Task Mapping (`generate_backfilling_runs` + `.expand()`), with
  late-first placement for stateful jobs and current-first or parallel for stateless ones;
  idempotent partition overwrite.
- Gotchas: overlapping executions mean you backfill only the latest run; integration
  stays inside the main pipeline; reprocessing unchanged partitions wastes resources; needs time
  partitions.
- [Reference](references/static_late_data_integrator.md) · [Examples](examples/static_late_data_integrator/)

### Dynamic Late Data Integrator
Backfill only partitions updated after their last processing, tracked in a state table.
- Mechanic: `last_update > last_processed AND partition < current AND NOT is_processed`; Delta
  versions from `_delta_log`; `depends_on_past` on the marker and planner tasks; triggered
  backfill runs.
- Gotchas: without `is_processed` plus sequential planners, partitions run twice; a failed
  planner blocks later runs; very late data on stateful jobs needs a max lookback; record the
  version that was read.
- [Reference](references/dynamic_late_data_integrator.md) · [Examples](examples/dynamic_late_data_integrator/)

### Filter Interceptor
Wrap each filter condition with a counter, because optimizers merge filters and plans hide
per-condition stats.
- Mechanic: PySpark `FilterWithAccumulator` + `mapInPandas`; SQL `status_flag` CASE subquery
  + `GROUP BY`; streaming counters in time windows.
- Gotchas: accumulators in transformations can double count on retries or speculation; read them
  after the action; NULL-safe conditions; compare against the history of past runs.
- [Reference](references/filter_interceptor.md) · [Examples](examples/filter_interceptor/)

### Checkpointer
Persist the source position and the state outside the job so a restart resumes instead of
reprocessing.
- Mechanic: framework-based and configuration-driven (`checkpointLocation`, Flink
  `enable_checkpointing`) or data-store-based and intentional (Kafka `commit()`).
- Gotchas: commit after the write is at-least-once, commit before is at-most-once; the
  exactly-once feeling needs an idempotent sink; checkpoint frequency trades latency for replay
  volume; state checkpoints cost more than offsets.
- [Reference](references/checkpointer.md) · [Examples](examples/checkpointer/)

## Cross-cutting Best Practices

- Separate transient errors (retry, then stop and resume from the checkpoint) from non-transient
  ones (dead-letter).
- Every side channel (dead letters, late records, filter stats) needs monitoring and a threshold
  that stops the pipeline.
- Writes that replays, retries or backfills can repeat must be idempotent: `replaceWhere`,
  MERGE, Delta `txnVersion`, keyed upserts.
- Tell downstream consumers about every backfill or replay (snowball backfilling); flag replayed
  rows.
- Watermarks and state are data: persist them together with the offsets.
- Keep windows, lookbacks and thresholds as configuration/Params, sized from observed delays.
