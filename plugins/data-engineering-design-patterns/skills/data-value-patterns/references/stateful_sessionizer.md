# Stateful Sessionizer

Category: Sessionization (streaming).

## Problem
Stakeholders like the batch sessions but now need them with **lower latency**. Hourly partitions cap the Incremental Sessionizer at one hour. The visits are also available in the streaming broker within seconds, so the batch pipeline should become near-real-time sessionization. Default streaming jobs are **stateless** and can't correlate events across iterations.

## Solution / Mechanics
A **stateful** streaming job adds a **state store** (first seen with the Windowed Deduplicator). It plays the role of the Incremental Sessionizer's pending sessions: it persists every in-flight session and keeps it available across iterations. The workflow is the same as the incremental one:
1. **Create** the session, or **resume** it if it's present in the state store.
2. **Combine** it with the new records according to the business logic (e.g. store visited pages in order).
3. If the session is **completed**, or partial sessions must be visible, transform it and **write it to the output**. If it isn't completed, also **write the new state** to the store.

State store layout: a **fast in-memory** store (volatile), synchronized regularly (**checkpointing**) to a resilient **fault-tolerance storage**. Spark Structured Streaming and Flink provide it out of the box. A hand-rolled job must code these interactions itself.

```
 Kafka visits ──> micro-batch ──groupBy(visit_id)──> state function per key
                                                     │  resume / create     <── in-memory state store
                                                     │  accumulate pages         (RocksDB / HashMap)
                                                     │  timeout = base wm + gap      │ checkpoint
                                                     │                               v (every batch / interval)
                                                     │                          fault-tolerant storage
                                                     ├── timed out  -> emit completed session, remove state
                                                     └── still open -> update state (+ partial row if wanted)
```

Two processing abstractions:
| Abstraction | You define | Notes |
|---|---|---|
| **Session window** | Key + **gap duration** (max inactivity between two events of a key). A larger gap opens a new window (e.g. a 20-min gap gives two windows). | Low code: Flink `EventTimeSessionWindows`, Spark `session_window()`. |
| **Arbitrary stateful processing** | State schema, function and timeout. The gap can be static or **dynamic per key**. | More effort, more flexibility: Spark `applyInPandasWithState`, Flink `KeyedProcessFunction`, Dataflow stateful DoFn. |

## Reference implementation (PySpark)
Declarative part. The expiration column, grouping key, function, output and state schemas, `outputMode="update"` and `timeoutConf="EventTimeTimeout"`:
```python
(visits_from_kafka.withWatermark("event_time", "1 minute").groupBy(F.col("visit_id"))
   .applyInPandasWithState(func=map_visits_to_session, outputStructType=output_schema,
                           stateStructType=state_schema, outputMode="update",
                           timeoutConf="EventTimeTimeout"))
```
Logic: if `current_state.hasTimedOut`, build the final session from `current_state.get` and call `remove()`. Otherwise:
```python
base_watermark = current_state.getCurrentWatermarkMs()
if base_watermark == 0:                       # first micro-batch: no watermark yet
    base_watermark = max_event_time_ms_of_batch
current_state.update((visits_so_far + new_visits, user_id))
current_state.setTimeoutTimestamp(base_watermark + 10 * 60 * 1000)
```
**Watermark vs event-time base** (1-min watermark, 10-min expiry; one row per micro-batch, and `getCurrentWatermarkMs()` returns the watermark computed by the previous batch):

| Key | Event time | Expiry, watermark base | Expiry, event-time base | New watermark |
|---|---|---|---|---|
| A | 10:00 | 10:10 (first batch: event time) | 10:10 | 09:59 |
| A | 10:01 | 10:09 | 10:11 | 10:00 |
| A | 10:08 | 10:10 | 10:18 | 10:07 |
| B | 10:15 | 10:17 | 10:25 | 10:14 |

With the watermark base, A can be emitted right after B's first element (10:10 < 10:14). With the event-time base, it stays open (10:18 > 10:14). The watermark follows job progress (`withWatermark`), so it is the preferred base.

Flink session window: `key_by(VisitIdSelector()).window(EventTimeSessionWindows.with_gap(Time.minutes(10))).allowed_lateness(Time.minutes(15).to_milliseconds()).process(VisitToSessionConverter(), ...)`. `allowed_lateness` sets how late events may still update an already emitted window.

## Consequences & trade-offs
| Consequence | Detail | Handling |
|---|---|---|
| At-least-once processing | Checkpoints happen at intervals, not on every state update. A restart resumes from the last checkpoint and replays events. | Keep the logic deterministic: never build session keys from attributes that change between runs, such as real (wall-clock) time. Deduplicate replayed events. |
| Scaling | Changing the compute capacity rebalances state. Keys can't be processed until they are reassigned. | Scale less often, and size the state partitions upfront (Spark `shuffle.partitions` is fixed by the checkpoint). |
| Inactivity period length | Longer means more late events included, but more hardware pressure and staler output. | Agree on the trade-off with consumers. |
| Inactivity period time base | **Event time** expiry is reliable. **Processing time** expiry uses the current clock, so any latency (write retries) can expire sessions too early. | Prefer event time (`EventTimeTimeout`, watermark). |

## When to use / when not
| Situation | Use | Alternative |
|---|---|---|
| Sessions needed within seconds or minutes, events in a broker | Stateful Sessionizer | - |
| Fixed inactivity gap, standard output | Session window flavor | Arbitrary processing only if needed |
| Per-key or dynamic gaps, custom close events, partial emissions | Arbitrary stateful processing | - |
| Hourly or daily freshness is enough | No | **Incremental Sessionizer** (cheaper, easier backfills) |
| Huge per-session payloads (MBs) | Carefully | Keep only aggregates in state, and store raw events elsewhere |
| Frequent autoscaling | Costly | A stateless pre-aggregation plus a batch sessionizer |

## Key design decisions
| Decision | Guidance |
|---|---|
| Session key | `visit_id` or `user_id`. Session id = key + first event time: deterministic and replay-safe. |
| Watermark delay | Lateness tolerance versus latency. Rows behind the watermark are dropped by the stateful operator. |
| Gap | 10 min in the reference implementation. It can be dynamic per key with arbitrary processing. |
| Output mode | `update`: emit on close, plus optional partial rows (`is_completed=false`). Session windows use `append`: finished only. |
| State schema | Compact (pages + timestamps). Changing it usually needs a new checkpoint, so version it. |
| State backend | RocksDB for large state (off-heap), HDFS-backed in-memory for small state. |
| Sink | Idempotent: MERGE on `session_id`, and never downgrade a completed row to a partial one. |

## Implementation notes per stack
**Spark**: `examples/stateful_sessionizer/spark/spark_streaming_sessionizer.py`. It is the reference function, extended with replay dedupe, a deterministic `session_id`, optional partial emission, a RocksDB state store and a Delta MERGE sink. `--mode session-window` is the low-code variant:
```python
visits.withWatermark("event_time", cfg.watermark_delay) \
      .groupBy(F.session_window("event_time", "10 minutes"), "visit_id") \
      .agg(F.min("event_time").alias("start_time"), F.max("event_time").alias("end_time"), ...)
```
`flatMapGroupsWithState` / `mapGroupsWithState` exist only in Scala and Java. PySpark uses `applyInPandasWithState` (3.4+), and its state is a tuple matching `stateStructType`.

**Kafka (hand-rolled)**: `examples/stateful_sessionizer/kafka/kafka_stateful_sessionizer.py`. It holds a dict per input partition (fast store) and a co-partitioned compacted changelog topic (fault-tolerant store). Each checkpoint emits sessions, writes the changelog, flushes, then commits offsets. State is restored on `on_partitions_assigned`, which implements state rebalancing.
```python
ps.apply(event)                    # resume/create + accumulate, timeout = (watermark or event) + gap
for session in ps.expire():        # watermark = max event time - allowed lateness
    producer.send(cfg.sessions_topic, key=session["session_id"], value=session)
```
Kafka Streams offers `SessionWindows.ofInactivityGapAndGrace(...)` with RocksDB + changelog built in.

**Airflow**: no role in the design. Airflow can only deploy or monitor the streaming job (checkpoint age, state size, consumer lag).

## Gotchas & best practices
1. `EventTimeTimeout` fires only when the watermark advances. If input stops entirely, open sessions never close. Monitor for this, or inject heartbeat events.
2. `setTimeoutTimestamp` must be greater than the current watermark, or Spark raises an error.
3. Iterator order inside the state function isn't guaranteed. Sort by event time before accumulating.
4. Bound the state: remove it on close, cap page arrays for bot traffic, and set a timeout on every update.
5. Use a deterministic `session_id` and dedupe within the state. Replays after a checkpoint restore are normal.
6. `spark.sql.shuffle.partitions` is frozen into the checkpoint. Changing it later needs a new checkpoint.
7. The watermark base is the previous batch's watermark. If event time jumps by more than the gap between two batches, a session created in the jump can expire after one batch. Use `max(watermark, batch_max_event - watermark_delay)` as the base if that matters (the Kafka sample does this per record).
8. Partial sessions downstream: consumers must take the latest row per session_id or filter on `is_completed`.

## Troubleshooting
| Symptom | Likely cause | Fix |
|---|---|---|
| `AttributeError: ... flatMapGroupsWithState` | Scala API used from Python | `applyInPandasWithState` |
| Sessions never emitted | Watermark stuck (idle source or partition) | Heartbeats; check `withWatermark` is on the event-time column; set `spark.sql.streaming.noDataMicroBatches.enabled` |
| Sessions closed too early | Processing-time expiry, or a watermark delay smaller than the real lateness | Event-time timeout; a larger watermark delay |
| Duplicate sessions after a restart | Non-deterministic id (UUID, `now()`) or an append sink | Deterministic id, MERGE on session_id |
| State size grows without bound | Timeout not set or not moved on update; keys never close | Set the timeout every invocation; cap sessions |
| Long pause after scaling | State rebalancing / restore from the changelog | Scale less often; smaller state; RocksDB incremental checkpoints |
| Rows silently missing | Events behind the watermark are dropped | Monitor `numRowsDroppedByWatermark`; widen the watermark |

## Related patterns
- **Incremental Sessionizer**: the batch version. Its pending storage is the state store here.
- **Windowed Deduplicator**: introduces the state store and checkpointing used by this pattern.
- **Late data handling / watermarks**: the watermark drives both expiry and late-row dropping.
- **Dynamic Joiner**: the same buffered-state-plus-watermark garbage collection for stream-to-stream joins.
- **Distributed Aggregator**: `groupBy(visit_id)` before the state function is a shuffle by key.
