# Late Data Detector

> Category: data error, time. Mostly streaming; the concepts also apply to partitioned batch.

## Problem

Visitors of the blogging platform usually produce events that reach the system within 15
seconds. Sometimes a user loses connectivity, buffers visits locally and flushes them when the
connection returns. The pipelines must **detect** these late events so each use
case can apply its own strategy: ignore them, store them aside, or reintegrate them.

Detection also controls state in stateful jobs. It tells a windowed aggregation or a
deduplicator when a window is complete (56).

## Solution / Mechanics

Time concepts: **event time** is when the action happened. **Processing time**
is when the pipeline saw it, and is never late. Detection needs an event-time attribute.

1. **Per-partition tracker**: for each input partition keep the event time seen so far using a
   **monotonically increasing** aggregate, `MAX`. `MIN` would get stuck in the past.
2. **Global aggregation** across partitions:
   - `MIN`: follow the slowest partition. More data counts as on time, but you buffer more;
   - `MAX`: follow the fastest partition. Smaller buffers, but records from slow partitions are
     skipped;
   - combined: with several partitioned sources, MIN per source and MAX across sources, or the
     opposite.
3. **Allowed lateness**: `watermark = aggregated event time - allowed lateness`.
   The watermark is the minimum event time still considered on time.
4. **Monotonic watermark**: a candidate lower than the current watermark is ignored. The
   watermark never moves back.
5. **Classify**: `event_time < current watermark` means late. Then ignore the record, or send it
   to a separate output for integration.

```
 partition 0: 10:00 10:05 10:06 ──► MAX = 10:06 ─┐
 partition 1: 09:58 10:02       ──► MAX = 10:02 ─┼─► global MIN = 10:02 (or MAX = 10:06)
 partition 2: 10:04             ──► MAX = 10:04 ─┘            │
                                                              ▼  - allowed lateness (30')
                                          candidate 09:32; watermark = max(old, candidate)
 next record e: e < watermark ? ──yes──► late output / ignore
                                 └─no──► on-time output / stateful operators
```

Watermark progression (allowed lateness 30 min, one partition):

| Event times | Input watermark | Candidate | Output watermark | Ignored |
| --- | --- | --- | --- | --- |
| 10:00, 10:05, 10:06 | (none) | MAX(10:06) - 30' = 09:36 | 09:36 | (none) |
| 09:20, 09:31, 10:07 | 09:36 | MAX(10:07) - 30' = 09:37 | 09:37 | 09:20, 09:31 |

All records of a batch are judged against the **input** watermark. The new watermark applies
from the next batch on.

## Consequences & trade-offs

- **Late data capture**: some frameworks detect late data but do not expose it.
  Spark Structured Streaming drops late rows from stateful operators and has no API to capture
  them. Flink does, through `ctx.timer_service().current_watermark()` and side outputs.
- **MIN at partition level**: leads to an *open-close-open infinite loop*. Windows
  emitted at watermark 10:30 must reopen when late data drags the watermark back to 09:00. It
  also leads to *stuck in the past*: constant late data means the watermark never progresses and
  state grows without bound.
- **MAX strategy and event skew**: if 4 of 5 sources deliver 40 minutes late, a
  MAX-based watermark marks almost all their records as late. There is no silver bullet. Monitor
  late events and be able to reintegrate them (Static / Dynamic Late Data Integrator).
- The allowed lateness is a latency vs. completeness dial. A larger value means a later window
  emission and more state, but fewer late records.

## Key design decisions

- **Event-time attribute**: it must be produced at the source. Do not use broker or ingestion
  time.
- **Allowed lateness**: start from the observed delay distribution (e.g. p99 of
  `ingestion - event time`) and the latency the consumers tolerate.
- **Global strategy**: MIN for completeness (default), MAX for low-latency jobs with uniform
  sources.
- **What to do with late records**: ignore them (simple), store them in a late-data sink
  (capture), or reintegrate them (integrator patterns).
- **Watermark persistence**: the watermark is state. A restarted detector that forgets it
  accepts arbitrarily old records as on time.

## When to use / when not

| Situation | Use? | Alternative |
| --- | --- | --- |
| Stateful streaming (windows, dedup, sessions) | Yes, required to close state | none |
| Late records are valuable and must be kept | Yes, in capture mode | + Static / Dynamic Late Data Integrator |
| Late records are noise | Yes (ignore mode) | Native watermark only |
| Batch on event-time partitions | Detection = "this record belongs to a past partition" | Static / Dynamic Late Data Integrator |
| No event time in the data | Not possible | Processing-time partitions, which move the problem downstream |

## Gotchas & troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Windows never emitted, state grows | Watermark stuck: a silent partition under the MIN strategy, or no new data | Watch per-partition MAX; idle-partition handling; alert on a stuck watermark |
| Many records dropped after an incident | MAX strategy plus event skew between sources | Switch to MIN, raise the allowed lateness, reintegrate the late output |
| Old data accepted after a restart | Watermark not persisted | Persist tracker state with the checkpoint (before the commit) |
| Late rows "disappear" in Spark | Native watermark drops them silently | Capture mode in `foreachBatch`, or Flink side output |
| Emitted windows change later | Watermark moved backwards (MIN at partition level, non-monotonic code) | `watermark = max(watermark, candidate)` |
| Different split on micro-batch retry | Watermark computed from mutable state | Batch N reads the state saved by N-1 only |
| Spark output windows appear later than expected | Spark rounds the watermark to window bounds (footnote) | Expected; size `allowed lateness + window` |

## Implementation notes per stack

**Spark, native** (`--mode native`), in
`examples/late_data_detector/spark/spark_late_data_detector.py`:

```python
(visits.withWatermark("event_time", "1 hour")
       .groupBy(F.window(F.col("event_time"), "10 minutes")).count())
```

**Spark, capture** (`--mode capture`): `WatermarkTracker` folds the per-partition MAX into a
monotonic watermark. `LateDataSplitter` (`foreachBatch`) loads the state of batch N-1, flags rows,
writes the on-time and late Delta tables with `txnVersion`, then saves the new state:

```python
flagged = batch_df.withColumn("is_late", F.col("event_time") < F.lit(input_watermark))
...
tracker.advance((r["source_partition"], r["max_event_time"]) for r in stats)
self.store.save(batch_id, tracker.to_json())
```

**Kafka**: `examples/late_data_detector/kafka/kafka_late_data_detector.py`, the counterpart of
the Flink `VisitLateDataProcessor`. `PartitionWatermark.observe()` runs per
record and `advance()` once per poll. Late records go to `visits_late` with `is_late=true`. The
order is state saved, then offsets committed.

**Airflow**: `examples/late_data_detector/airflow/late_data_monitor_dag.py` (hourly). It computes
the late ratio, the skew in minutes (fastest minus slowest partition MAX) and whether the
watermark is stuck, and triggers the Dynamic Late Data Integrator above a late-record count.

## Related patterns

- **Windowed Deduplicator**: the same watermark expires its state.
- **Static Late Data Integrator**: reintegrates late data within a fixed lookback.
- **Dynamic Late Data Integrator**: reintegrates only the partitions that received late data.
- **Dead-Letter**: same side-output mechanics; late records are a time-based error output.
- **Stateful Sessionizer**: relies on the watermark to close sessions.
- **Checkpointer**: persists watermark and state so a restart does not reset them.
