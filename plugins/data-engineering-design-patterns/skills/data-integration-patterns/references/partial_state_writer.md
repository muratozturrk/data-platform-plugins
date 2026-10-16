# Partial State Writer

Category: Data Value.

## Problem
Your near real-time sessionization pipeline works, but sessions last two to four hours. The data security team wants to build fraud detection on the sessions and cannot
wait hours. They want immediate access, even to partial data: whenever a new observation
arrives, update the session and send it to the output topic. You want to implement this in
the data processing framework itself.

## Solution / Mechanics
Modern stateful frameworks (Flink, Spark Structured Streaming) support incremental state
emission out of the box. You decide two things:
1. **When a state update triggers a partial emission.** The frequency is a business decision.
   Emitting each updated session can make sense; emitting a counter after each element is
   usually too much, so every 10 or 20 updates may be valid. Time-based emission (every N
   event-time minutes) is another option.
2. **How to distinguish unfinalized from final state**, usually a boolean such as `is_final`.

```
events for user u1:  e1   e2   e3   e4   e5   e6 ........ (30 min inactivity)
state            :  [1]  [2]  [3]  [4]  [5]  [6]           expire
emitted (N=5)    :  P(1)                     P(6)           F(6)
                    is_final=false           is_final=false is_final=true
consumer table   :  upsert by session_id -> latest version wins; final never overwritten
```

Where each framework emits:
- **Flink**: triggers decide when windows emit intermediary results: `CountTrigger` (number of
  elements), `DeltaTrigger` (condition between deliveries), or `ContinuousEventTimeTrigger`
  (every N event-time minutes inside the window; it advances only when the watermark does).
- **Spark**: emission is in your stateful function. In `transformWithStateInPandas` (Spark 4),
  output from `handleInputRows` is early (the group has new records, so the state can still
  change); output from `handleExpiredTimer` is likely complete. In Spark 3.4+,
  `applyInPandasWithState` gives the same split: output on new input is partial, output when
  `state.hasTimedOut` is final.

**Final result and late data**: streaming data is prone to producer issues. You
may close a state and treat it as final, then receive late data. You may have to correct the
"final" result.

## Consequences & trade-offs
Four drawbacks:
- **Partial is not final**: late data can still change a state. Consumers who treat partial
  state as final can, for example, wrongly label a user as a fraudster. Announce state
  completeness explicitly so consumers know the consequences.
- **Increased throughput**: many more records than final-only emission. If the lakehouse must
  keep one row per state, it runs frequent, costly MERGEs that reduce write throughput. As a
  consumer, you may refresh at less regular intervals.
- **Writing semantics**: final-only output can use append (INSERT). Partial output needs MERGE
  to replace each increment with a newer version. With retries and an exactly-one-row
  requirement you may need MERGE anyway.
- **Latency and ownership**: partial state still crosses a separate pipeline, with I/O and
  network latency. Very latency-sensitive consumers may embed the state logic in their own
  application. That also gives them control over the logic; asking the producing team to adapt
  it can conflict with other consumers' expectations.

## Key design decisions

| Decision | Options | Notes |
|----------|---------|-------|
| Emission frequency | every update, every N updates, every T (event or processing time) | business requirement; balance with sink cost |
| Completeness flag | `is_final` boolean, `state_version` + `is_final` | always explicit |
| Output key | stable session id | must not change between partial versions |
| Sink semantics | MERGE upsert, compacted topic keyed by session id, append + view of latest | append-only sinks need "latest per key" readers |
| Late data after final | drop, reopen a new session, emit a correction | define the contract with consumers |

**When to use / when not to use**

| Use it when | Do not use it when | Alternative |
|-------------|--------------------|-------------|
| Consumers need in-progress state of long windows or sessions | Consumers cannot handle non-final data | final-only emission |
| Early signals matter more than exactness (fraud, monitoring) | The consumer is extremely latency-sensitive | embed the stateful logic in the consumer |
| The sink supports upserts | The sink is append-only and readers cannot dedupe | emit less often, or final only |

## Gotchas & best practices
- Keep the output key stable: deriving the session id from a start time that late events can
  move earlier creates a new row per version. Store the first-seen key in the state.
- Never let a partial version overwrite a final one (MERGE condition `t.is_final = false`).
- Use compacted topics (`cleanup.policy=compact`) keyed by session id for Kafka outputs.
- Count-based emission (every N) misses sessions with fewer than N events until they close;
  emit the first version immediately if early visibility matters.
- Processing-time timeouts in Spark fire only when micro-batches run; with idle input, final
  emission is delayed (a heartbeat source helps, see Sidecar).

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Many rows per session in the table | append sink or unstable session id | MERGE on a stable id |
| Final session replaced by an older partial | out-of-order writes | MERGE only when target `is_final = false` |
| Sink throughput collapses | MERGE per partial update | emit every N updates, or batch merges |
| Sessions stay partial forever | timeouts not firing (no input) | event-time timeout with watermark, heartbeat source |
| Fraud alerts on partial sessions | consumer ignores `is_final` | document the contract; separate topics for final data |

## Implementation notes per stack
**Flink**: emit every 10 event-time minutes inside 1-hour windows:

```python
records = (visits_data_source.map(map_json_to_visit).key_by(lambda v: v.visit_id)
           .window(TumblingEventTimeWindows.of(Time.hours(1))).allowed_lateness(0)
           .trigger(ContinuousEventTimeTrigger.of(Time.minutes(10))))
```

**Spark** - `examples/partial_state_writer/spark/partial_sessions_job.py`: `applyInPandasWithState`
emits a partial version every `EMIT_EVERY` visits and the final version on timeout, then MERGEs:

```python
if state.hasTimedOut:
    session_key, start, end, visits, _ = state.get
    state.remove()
    yield session_row(user_id, session_key, start, end, visits, is_final=True)
    return
# ... update with new rows; yield is_final=False every EMIT_EVERY visits
state.setTimeoutDuration(INACTIVITY_MS)
```
The `foreachBatch` sink uses `merge(...).whenMatchedUpdateAll(condition="t.is_final = false")`.

**Kafka** - `examples/partial_state_writer/kafka/partial_state_consumer.py`: plain-consumer
sessions with partial emissions, watermark-based closing, and a compacted output topic keyed by
`session_id`; `--self-test` checks partial and final emission.

**Airflow** - `examples/partial_state_writer/airflow/README.md`: no DAG; describes the batch
equivalent (frequent incremental MERGE with `is_final = false`, last run sets `true`).

## Related patterns
- [Stateful Sessionizer](../../data-value-patterns/references/stateful_sessionizer.md): the state that this pattern exposes early.
- [Merger](../../idempotency-patterns/references/merger.md) / [Stateful Merger](../../idempotency-patterns/references/stateful_merger.md): the upsert semantics partial output requires.
- [Dynamic Late Data Integrator](../../error-management-patterns/references/dynamic_late_data_integrator.md): handles late data that changes "final" results.
- [Sidecar](sidecar.md): the heartbeat source keeps timeouts firing on idle inputs.
- [Metadata Decorator](../../data-value-patterns/references/metadata_decorator.md): an alternative place for the `is_final` flag (header).
