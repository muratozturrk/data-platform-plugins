# Dynamic Joiner

> Category: data enrichment, streaming. Both joined datasets are in motion.

## Problem

The visits-to-users enrichment was built with the Static Joiner. Thousands of new users come
online each week and profile changes become frequent, so the enriched dataset goes stale and
causes problems for downstream consumers. Each user change is already published
to a streaming broker by the Change Data Capture pattern. You need to combine **two streams**.

The Static Joiner fails here because of data perception: it treats one side as slowly evolving
data at rest, while both sides now move continuously. The two streams also
have **different latencies**. The enrichment side can be late compared to the enriched side, or
the other way around, so a naive join comes back empty.

## Solution / Mechanics

The Dynamic Joiner shares the Static Joiner's basics (join keys, join method) and adds one
requirement: **time boundaries**.

1. **Join keys plus a time condition.** Example: `page = ad_page AND display_time BETWEEN
   event_time AND event_time + INTERVAL 2 MINUTES`. The condition is a business rule (an ad is
   displayed at most two minutes after the visit) and a technical bound.
2. **A time-bounded buffer on each side.** Unmatched rows wait in the buffer, so the faster
   source can align with the slower one. The buffer covers the allowed latency difference: if
   users are one hour behind visits, visits stay buffered one extra hour.
3. **A GC watermark.** Keeping everything forever needs unlimited hardware. The garbage
   collection watermark removes rows that are too old to match. Rows older than the
   slower stream's oldest event time minus the allowed lateness are evicted.
4. **Emission.** Matches are emitted as soon as both sides are present. In a left outer join,
   unmatched rows of the primary side are emitted with NULLs once the watermark has passed their
   time window.

You don't implement the buffering yourself: Spark, Flink and Kafka Streams provide it.

```
 Stream A (fast: visits)              Stream B (slow: ads)
 key=10 10:20 ┐                       ┌ key=1 10:00
 key=11 10:21 │                       │ key=2 10:01
 key=12 10:23 ┘                       └ key=3 10:02
      │ no match yet                       │
      v                                    v
 +-------------------------+     match  +-----------------+
 | buffer A                |<---------->| buffer B        |
 | key=1 09:56  key=2 09:57|            |                 |
 | key=3 09:58  key=10..12 |            |                 |
 +-------------------------+            +-----------------+
      │                                    │
      └──> joined: (1,09:56|1,10:00) (2,09:57|2,10:01) (3,09:58|3,10:02)

 GC watermark = min(max_time_A, max_time_B) - allowed lateness
 rows with time < watermark: evicted (left outer: emitted with NULL first)
 rows arriving with time < watermark: late, never joined
```

## Consequences & trade-offs

| Consequence | What happens | Mitigation |
| --- | --- | --- |
| Space vs exactness | The GC watermark and time boundaries mean some possible joins are missed. A bigger buffer gives more matches but costs memory and checkpoint size. A smaller buffer saves storage but loses matches when the latency gap is large | No universal formula. Measure the usual latency gap between sources, size the watermark from it, and monitor the match rate |
| Late data | Temporary connectivity issues delay a subset of events. The watermark moves on, the buffered state is invalidated and the late events are ignored | Neither enrichment pattern gives 100% join guarantees. Add late-data detection and integration |
| Outer-join latency | NULL rows for unmatched visits appear only after the watermark passes `event_time + range` | Accept the delay, or emit inner matches to a low-latency sink and unmatched rows separately |
| Stateful job | The buffer lives in the state store, checkpointed with the query | Plan checkpoint storage. Changing the watermark or condition usually requires a new checkpoint |

## Key design decisions

- **Watermark per side**: derive it from each source's delay distribution (for example, P99
  lag), not from a guess. Spark uses the minimum across inputs by default
  (`spark.sql.streaming.multipleWatermarkPolicy=min`), so the slowest source drives eviction.
- **Time-range condition**: required by Spark for state eviction in outer joins. Encode the real
  business rule; the range plus the watermark is the buffer duration.
- **Join type**: `left_outer` keeps visits that never get an ad, as NULLs. `inner` emits faster
  and drops unmatched rows.
- **Interval join or temporal (versioned) join**: correlate two event streams with an interval
  join. To get the reference version valid at event time (for example, the latest ad for a
  page with `update_time <= event_time`), use a Flink temporal table join.
- **Sink idempotency**: a replayed micro-batch must not duplicate output. Use Delta
  `txnAppId`/`txnVersion` or a keyed upsert.

## When to use / when not

| Situation | Use Dynamic Joiner? | Alternative |
| --- | --- | --- |
| Both sides are streams (CDC users + visits, ads + visits) | Yes | none |
| Enrichment side is at rest and changes slowly | No | Static Joiner (stream-to-static join) |
| Need the dimension value *as of* event time from a CDC stream | Yes, versioned form | Flink temporal table join, Kafka Streams KStream-KTable join |
| Latency gap between sources is hours or days | Rarely (state too big) | Land both in tables and join in batch with SCD (Static Joiner) |
| Every match is mandatory (billing) | Not alone | Add Late Data Detector + Dynamic Late Data Integrator, or reconcile in batch |

## Gotchas & troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| State grows until executors fail | One side has no watermark, or the join condition has no time range | `withWatermark` on both sides plus a `BETWEEN` range condition |
| Match rate drops after a source incident | The source lagged beyond the watermark, so its rows arrived late and were dropped | Check `numRowsDroppedByWatermark`; widen the watermark or integrate late data |
| No output at all for a while | One source silent: the minimum watermark does not advance | Alert on watermark lag; use the `max` policy only if you accept dropping the slow side |
| Unmatched visits appear minutes late | Expected for outer joins: emitted after the watermark passes | Document it for consumers; size the watermark down if latency matters more |
| Duplicates after a restart | Non-idempotent sink replaying a micro-batch | `txnAppId`/`txnVersion` on Delta, or upsert by `(visit_id, ad_id)` |
| `AnalysisException` on outer join | Missing watermark or range condition, or an output mode other than append | Watermarks on both sides, a time range, `outputMode("append")` |
| Wrong matches across time zones | Mixed local and UTC timestamps between producers | Normalize to UTC at ingestion |

## Implementation notes per stack

**Spark Structured Streaming**: `examples/dynamic_joiner/spark/spark_stream_stream_time_bounded_join.py`.

```python
visits = visits.withWatermark("event_time", cfg.watermark)
ads = ads.withWatermark("display_time", cfg.watermark)
joined = visits.join(ads, F.expr(f"""
    page = ad_page AND
    display_time BETWEEN event_time AND event_time + INTERVAL {cfg.max_display_delay}
"""), "left_outer")
# foreachBatch: Delta append with txnAppId/txnVersion=batch_id, matched/unmatched stats to JSONL
```
The driver loop appends `lastProgress` fields (`eventTime.watermark`, `stateOperators[].numRowsTotal`,
`numRowsDroppedByWatermark`) to the same stats file.

**Kafka (plain consumer)**: `examples/dynamic_joiner/kafka/kafka_stream_stream_joiner.py` writes
the buffers out:

```python
@property
def watermark(self):
    if self.max_visit_time is None or self.max_ad_time is None:
        return None
    return min(self.max_visit_time, self.max_ad_time) - self.lateness_s

def evict(self):  # visits whose window closed: emit unmatched with ad=None
    ...
```
Offsets are committed only up to the oldest still-buffered record, so the buffer is rebuilt after
a crash. For production, Kafka Streams `KStream.join(..., JoinWindows.ofTimeDifferenceAndGrace(...))`
gives the same with a changelog-backed state store.

**Flink** (Java only): declare watermarks on both tables, create a
`TemporalTableFunction` on `ads` keyed by `ad_page` and versioned by `update_time`, then
`visitsTable.joinLateral(call("adsLookupFunction", $("event_time")), $("ad_page").isEqual($("page")))`.

**Airflow**: `examples/dynamic_joiner/airflow/dynamic_joiner_dag.py` (DAG id
`dynamic_joiner_health`). Airflow does not run the join. It reads the hourly stats and
fails on match rate < 60%, watermark lag > 30 min, state growth > 2x per hour, or more than 1% of
rows dropped as late. Thresholds come from a JSON Variable.

## Related patterns

- **Static Joiner**: the at-rest counterpart; the Dynamic Joiner is its fix for frequent changes.
- **Change Data Capture**: produces the user-change stream used as the enrichment side.
- **Late Data Detector / Dynamic Late Data Integrator**: recover joins lost to the watermark.
- **Windowed Deduplicator**: same watermark-bounded state mechanics.
- **Stateful Sessionizer**: same state store, checkpoint and watermark trade-offs.
- **Idempotency patterns**: keep micro-batch replays from duplicating joined rows.
