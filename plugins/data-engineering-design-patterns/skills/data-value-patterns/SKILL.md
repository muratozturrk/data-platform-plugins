---
name: data-value-patterns
description: Data value design patterns for enrichment, decoration, aggregation, sessionization and ordering (Static Joiner, Dynamic Joiner, Wrapper, Metadata Decorator, Distributed Aggregator, Local Aggregator, Incremental Sessionizer, Stateful Sessionizer, Bin Pack Orderer, FIFO Orderer) for Spark, Kafka and Airflow pipelines. Use when enriching events with reference or dimension data, broadcast or SCD type 2 joins, stream-to-stream joins with watermarks, wrapping raw records with computed fields, adding job version or lineage metadata (Kafka headers, hidden columns), GROUP BY / shuffle / data skew / salting, avoiding shuffle with pre-partitioned data or groupByKey, building user sessions in batch or streaming (session windows, applyInPandasWithState), or delivering records in order through bulk APIs with partial commits (Kinesis PutRecords, Elasticsearch bulk) or FIFO producers (max.in.flight, idempotent producer).
---

# Data Value Patterns

Data sitting in storage is not yet an asset. Raw events are poor in context, scattered across partitions and often delivered out of order. This skill is about **augmenting datasets so they become useful**. It groups ten patterns into five families:
- **Enrichment** combines a dataset with another one, at rest or in motion.
- **Decoration** adds computed or technical attributes, either visible or hidden.
- **Aggregation** reduces data with or without a shuffle.
- **Sessionization** correlates events of one activity, in batch or streaming.
- **Ordering** preserves chronology on delivery, with bulk or per-record writes.

The first two families add information. The others remove or reorganize information to make it understandable.

## When to Use This Skill

- Events lack context (user profile, device, geo), and the context lives in a table, an API or another stream.
- A stream-to-stream join loses matches because one source lags behind the other, or its state keeps growing.
- Records from many providers need one consistent shape while the original payload must stay available for debugging.
- You must trace which job version or run produced a record without exposing that detail to business users.
- A GROUP BY or join is slow because of shuffle or a skewed key, or you want to remove the shuffle entirely.
- Analysts need sessions (first visit to inactivity timeout) from hourly partitions, or in near real time from a broker.
- A sink with partial-commit bulk writes (Kinesis, DynamoDB, Elasticsearch) or a strict FIFO requirement must keep per-entity order.

## Pattern Selection Guide

| Problem / symptom | Pattern | Reference |
|---|---|---|
| Enrich events with a slowly changing reference table or API (incl. SCD2 "as of" joins) | Static Joiner | [references/static_joiner.md](references/static_joiner.md) |
| Both datasets are streams with different latencies; joins come back empty | Dynamic Joiner | [references/dynamic_joiner.md](references/dynamic_joiner.md) |
| Need computed fields next to the untouched raw record, clearly separated | Wrapper | [references/wrapper.md](references/wrapper.md) |
| Need technical context (job version, run, attempt) per record but hidden from users | Metadata Decorator | [references/metadata_decorator.md](references/metadata_decorator.md) |
| Aggregate a dataset too big for one node; cubes; skewed keys | Distributed Aggregator | [references/distributed_aggregator.md](references/distributed_aggregator.md) |
| Data already partitioned by the grouping key; want to drop the shuffle | Local Aggregator | [references/local_aggregator.md](references/local_aggregator.md) |
| Sessions spanning several batch partitions | Incremental Sessionizer | [references/incremental_sessionizer.md](references/incremental_sessionizer.md) |
| Sessions with low latency from a stream | Stateful Sessionizer | [references/stateful_sessionizer.md](references/stateful_sessionizer.md) |
| Ordered delivery through a bulk API with partial commits | Bin Pack Orderer | [references/bin_pack_orderer.md](references/bin_pack_orderer.md) |
| Ordered, as-soon-as-possible delivery with moderate volume | FIFO Orderer | [references/fifo_orderer.md](references/fifo_orderer.md) |

## Patterns

### Static Joiner
- **What:** joins the primary dataset with an at-rest enrichment dataset on a key. Works for batch and for stream-to-static joins. The time-sensitive variant joins SCD type 2/4 tables `BETWEEN start_date AND end_date`.
- **When:** the reference changes slowly (dimensions, lookup tables, a materialized API).
- **Mechanics:**
  - Broadcast join for small references, plain `stream.join(static_df)` for stream-to-batch joins.
  - Bulk API lookups (buffer N rows, distinct uncached keys, one request) when the reference sits behind an API.
  - Materialize the API into an SCD table when replays must be idempotent.
- **Gotchas:**
  - The reference can lag the events (late data). In batch, wait with the Readiness Marker.
  - Use the orchestrator's execution date or event time, not `NOW()`, in SCD joins. Inclusive `BETWEEN` on touching intervals matches two rows.
  - Duplicate reference keys multiply rows, so assert `output_rows == primary_rows`.
  - A stream can read a non-atomic reference while it is half rewritten. Use Delta or Iceberg.
- **Files:** [references/static_joiner.md](references/static_joiner.md), [examples/static_joiner/](examples/static_joiner/)

### Dynamic Joiner
- **What:** a stream-to-stream join with time boundaries. Each side keeps a time-bounded buffer of unmatched rows so the faster stream waits for the slower one. A GC watermark evicts rows that can no longer match.
- **When:** the enrichment side is itself in motion (CDC user changes, ads vs visits) and the two sources have different latencies.
- **Mechanics:**
  - `withWatermark` on both sides plus a range condition (`display_time BETWEEN event_time AND event_time + INTERVAL 2 minutes`), with `left_outer`.
  - Write to an idempotent sink (Delta `txnAppId`/`txnVersion`).
  - The Flink temporal table join gives "version as of event time".
- **Gotchas:**
  - Space vs exactness: a bigger buffer gives more matches and more state.
  - Rows later than the watermark lose their join, so monitor `numRowsDroppedByWatermark`.
  - Outer-join NULL rows appear only after the watermark passes.
  - The slowest input drives the watermark.
- **Files:** [references/dynamic_joiner.md](references/dynamic_joiner.md), [examples/dynamic_joiner/](examples/dynamic_joiner/)

### Wrapper
- **What:** wraps the untouched raw record in an envelope that also carries computed attributes, business (`is_connected`) or technical (`processing_context`: job version, batch).
- **When:** heterogeneous sources need one consistent shape, and the original values must stay available for debugging.
- **Mechanics:** four layouts:
  1. Raw flat with computed nested (`decorated` struct).
  2. Computed flat with raw nested (`raw` struct).
  3. Everything flat with `raw_*` / `computed_*` prefixes.
  4. Two tables joined by a deterministic id.

  For streams, use the envelope `to_json(struct(raw_data, processing_context))`.
- **Gotchas:**
  - Domain split: consumers must look in two places, so keep the split in Silver and flatten for Gold.
  - Decorations add size, which projection pushdown helps with.
  - Never transform the raw part; check its count and checksum after each run.
- **Files:** [references/wrapper.md](references/wrapper.md), [examples/wrapper/](examples/wrapper/)

### Metadata Decorator
- **What:** stores technical context (job version, batch or run, attempt, source position) in the data store's metadata layer. The business payload stays unchanged.
- **When:** operators need to trace which release or run produced a record, and end users must not see that context.
- **Mechanics:**
  - Kafka headers, with `includeHeaders=true` on read and write. Header values are binary. Keep upstream headers and replace only the keys you own.
  - Object tags per file.
  - For tables: a context table (`dp.visits_context`: execution date, loading time, code version, `try_number`) referenced by an FK column, or a hidden column behind a view and grants.
- **Gotchas:**
  - Kinesis has no headers, so use a context table there.
  - Never put business attributes in metadata: consumers never look there.
  - Take ids from immutable context (batch id, execution date, `try_number`). When rerunning, delete the data rows before the context row.
- **Files:** [references/metadata_decorator.md](references/metadata_decorator.md), [examples/metadata_decorator/](examples/metadata_decorator/)

### Distributed Aggregator
- **What:** a cluster-wide group-by and reduce. A shuffle (`Exchange hashpartitioning`) brings rows with the same key to one reducer (MapReduce).
- **When:** the data doesn't fit on one node, or related rows live on different nodes or in different stores (JDBC + files, external tables). Typical case: an OLAP cube of visits in daily and weekly views.
- **Mechanics:** decomposable measures are partially aggregated map-side before the shuffle. Use `cube()`/`rollup()` with `grouping_id`. Roll the weekly view up from the daily cubes (sum of sums). In Kafka the shuffle is a repartition topic. Check it with `explain()`.
- **Gotchas:**
  - The shuffle is the main latency cost.
  - For data skew, salt (two-stage groupBy) or use AQE.
  - Executors that hold shuffle data can't be released, so use an external shuffle service.
  - Never merge averages, medians or distinct counts.
- **Files:** [references/distributed_aggregator.md](references/distributed_aggregator.md), [examples/distributed_aggregator/](examples/distributed_aggregator/)

### Local Aggregator
- **What:** aggregates inside each input partition with no shuffle. The only network exchange is reading the input.
- **When:** producers write each key to a fixed partition, the partition count never changes, and consumers group by that same key. Also when the dataset fits on one node.
- **Mechanics:**
  - Kafka Streams `groupByKey()`.
  - Spark `sortWithinPartitions(key, time)` + `foreachPartition` fold, failing the job if the plan contains `Exchange`.
  - Tables bucketed on the key with the same bucket count.
  - Redshift `DISTKEY` / `DISTSTYLE ALL`.
- **Gotchas:**
  - Changing the partition count or the partitioner splits aggregates silently. Check the count at startup and in the orchestrator, and treat rescaling as a stop-the-world reorganization.
  - Only one grouping key is served; other keys need the Distributed Aggregator.
  - `repartition` or `minPartitions` destroys key locality.
- **Files:** [references/local_aggregator.md](references/local_aggregator.md), [examples/local_aggregator/](examples/local_aggregator/)

### Incremental Sessionizer
- **What:** batch sessionization across consecutive partitions. It uses three storages: input, private pending sessions and public completed sessions.
- **When:** sessions span several hourly or daily partitions and batch latency is acceptable.
- **Mechanics:**
  1. Delete what this and later runs wrote (`execution_time_id >= current`).
  2. FULL OUTER JOIN the new input with the pending sessions of exactly the previous run (new / restored with data / restored without data).
  3. Initialize, accumulate and finalize each session (closing event, or `expiration_batch_id = run + N`).
  4. Write expired sessions to public storage and the rest to pending.
- **Gotchas:**
  - Sessions are forward dependent: a backfill of one run forces rerunning all later runs (`depends_on_past`, `max_active_runs=1`).
  - Empty partitions must still run.
  - Flag partial sessions with `is_completed=false`.
  - The inactivity gap trades late-data coverage against cost.
- **Files:** [references/incremental_sessionizer.md](references/incremental_sessionizer.md), [examples/incremental_sessionizer/](examples/incremental_sessionizer/)

### Stateful Sessionizer
- **What:** streaming sessionization where the state store holds in-flight sessions (in memory, checkpointed to durable storage). It is the streaming counterpart of pending sessions.
- **When:** sessions are needed in near real time.
- **Mechanics:**
  - Session windows: gap duration, Flink `EventTimeSessionWindows`, Spark `session_window`.
  - Arbitrary stateful processing: PySpark `applyInPandasWithState` keyed by `visit_id`, with an EventTimeTimeout set to the watermark plus the gap (event time in the first batch).
  - Without a framework: an in-memory state plus a compacted changelog topic.
- **Gotchas:**
  - Checkpointing gives at-least-once processing, so session ids must be deterministic and never use wall-clock time; dedupe replays.
  - Rescaling rebalances state.
  - Prefer event-time expiry over processing-time expiry.
  - Sessions don't close while the watermark is stalled.
  - `flatMapGroupsWithState` is Scala-only.
- **Files:** [references/stateful_sessionizer.md](references/stateful_sessionizer.md), [examples/stateful_sessionizer/](examples/stateful_sessionizer/)

### Bin Pack Orderer
- **What:** ordered delivery through partial-commit bulk APIs: Kinesis `PutRecords`, DynamoDB `BatchWriteItem`, Elasticsearch `_bulk`.
- **When:** you need bulk throughput and per-entity event-time order.
- **Mechanics:**
  1. Sort by (entity, time).
  2. Put the n-th record of each entity into bin n, so a bin holds at most one record per entity.
  3. Send bins sequentially, retrying only the failed items of the current bin.
- **Gotchas:**
  - Ordering holds within one execution only: a pipeline rerun re-emits bins.
  - Never parallelize bins.
  - Check the per-item status, and chunk bins to the API limits.
- **Files:** [references/bin_pack_orderer.md](references/bin_pack_orderer.md), [examples/bin_pack_orderer/](examples/bin_pack_orderer/)

### FIFO Orderer
- **What:** delivers each record, oldest first, and waits for its acknowledgment before sending the next.
- **When:** low volume or relaxed latency, with no buffering allowed.
- **Mechanics:**
  - Kafka `produce` + `flush`, or `max.in.flight.requests.per.connection=1`, or the idempotent producer (5 in flight, still ordered).
  - Kinesis `PutRecord` + `SequenceNumberForOrdering`.
  - Pub/Sub `ordering_key`.
- **Gotchas:**
  - One request per record means I/O overhead and latency.
  - To parallelize, use per-entity ordering scopes.
  - FIFO is not exactly-once: an ack can fail after a successful send. Add idempotency.
- **Files:** [references/fifo_orderer.md](references/fifo_orderer.md), [examples/fifo_orderer/](examples/fifo_orderer/)

## Cross-cutting Best Practices

- **Use immutable time for idempotency.** Use the orchestrator's logical date (`ds`, `execution_date`) in SCD joins, session cleanup and run ids. Never use `NOW()` or wall-clock time in keys.
- **Prefer event time with watermarks** for joins and session expiry. Size watermarks from the measured lag between sources.
- **Bound all state:** watermarks for joins, timeouts for sessions, TTLs for Redis or Kafka state. Monitor state size and checkpoint age.
- **Keep the shuffle in view:** run `explain()` before and after a change. Use broadcast for small references, local aggregation only when partitioning is guaranteed, and salting or AQE for skew.
- **Keep raw values:** wrap rather than overwrite. Put purely technical context in metadata, and expose clean flat views in Gold.
- **Ordering and delivery guarantees are separate.** Ordering patterns guarantee order within a run. Pair them with deterministic ids or idempotent writers for replays, and commit source offsets only after the sink acks.
- **Serialize dependent runs** (`max_active_runs=1`, `depends_on_past=True`) for sessionization and ordered delivery.
- **Validate in the orchestrator:**
  - row counts after left joins;
  - miss and enrichment rates;
  - the distinct-key drop and reduction ratio after aggregations;
  - session counts and durations.
