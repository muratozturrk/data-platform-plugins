# Lag Detector

Category: Time Detectors.

## Problem
One week ago a streaming job started receiving 30% more data. You missed the announcement,
and a downstream consumer now complains about slower delivery. Before putting a
scaling strategy in place, you need to measure how fast the consumer processes its input
compared with what the producer writes. Lag is also an early indicator of freshness and
availability problems.

## Solution / Mechanics
1. **Define the lag unit**, which depends on the store:
   - Kafka topic: record position (offset) or record append time;
   - Delta Lake (or Iceberg) table: commit number (version);
   - time-partitioned store: partition timestamp.
2. **Define the comparison**: the most recent unit available in storage minus the unit
   currently processed:
   `lag = last_available_unit - last_processed_unit`.
3. **Aggregate partitioned results** (Kafka gives one lag per partition):
   - `MAX` for the worst case, which fires even when a single partition lags;
   - a percentile (P90, P95) for how most partitions perform. P90 = 10 s means 90% of the
     partitions are at most 10 s behind; some may be lower.
   - Combine both: percentile for overall latency, MAX for the worst case.
4. **Publish and alert**: push the metric (Prometheus Pushgateway, CloudWatch) and alert on
   thresholds or trends.

```
partition   latest offset   processed offset   lag
   p0            1200             1190          10
   p1             980              975           5
   p2            3100             3070          30   <- MAX = 30
   p3             400              398           2
   ...                                              P90 = 18, avg = 8 (misleading)
```

**The average trap**: for lags 10, 5, 30, 2, 3, 5, 3 seconds the average is
8 s but P90 is 18 s. 90% of the data is processed within 18 s; looking at the average would
suggest the job is fine. Use percentiles.

```
producer --> [topic / table]  latest unit  ----+
                    |                           |  lag = latest - processed
                    v                           |  per partition -> MAX, P90
              consumer job --> processed unit --+--> metrics --> alert
```

Two consumer modes:
- **Continuous Spark job on Kafka**: a `StreamingQueryListener` compares the end offsets of
  each micro-batch with the latest offsets per partition and pushes per-partition lag.
- **Scheduled Spark job on Delta** (`availableNow` trigger): the consumer reports the last
  read `reservoirVersion` from the query progress; the producer reports `MAX(version)` from
  `DESCRIBE HISTORY`. The alert is the difference between them.

## Consequences & trade-offs
- **Data skew**: with MAX, a poor result may not be the consumer's fault. If one
  partition receives more load, the consumer naturally processes it more slowly. Fix the data
  distribution at write time so consumers work on even partitions, instead of scaling the
  consumer.
- Offset lag is a count of records, not a duration. 10,000 records can be 1 second or 1 hour
  depending on throughput. Time-based lag (append time of the last processed record vs now)
  is easier to compare with SLAs but needs timestamps.
- Measuring from inside the job (listener) sees its real progress, including checkpoint-only
  state. Measuring from outside (consumer group) only works when the consumer commits offsets.

## Key design decisions

| Decision | Options | Notes |
|----------|---------|-------|
| Unit | offset, append time, table version, partition time | pick what both producer and consumer can report |
| Aggregation | MAX, P90/P95, both | never alert on the average |
| Measurement point | in-job listener, external checker, broker exporter | Spark keeps offsets in its checkpoint, not in the consumer group |
| Alert form | absolute threshold, growth over time | a continuously growing lag is worse than a stable high one |
| Reaction | scale out, rebalance partitions, throttle producers | depends on whether the lag is global or one partition |

**When to use / when not to use**

| Use it when | Do not use it when | Alternative |
|-------------|--------------------|-------------|
| Streaming consumers must keep up with producers | The job is a scheduled batch that starts after the data is complete | SLA Misses Detector on run duration |
| You plan autoscaling and need a signal | You need per-record end-to-end latency | SLA Misses Detector with event-time SLA |
| Several consumers share a topic and you need per-consumer health | The source is not ordered or versioned | Flow Interruption Detector on outputs |

## Gotchas & best practices
- `kafka-consumer-groups.sh` shows nothing useful for Spark Structured Streaming jobs because
  Spark does not commit offsets to the group by default. Measure from a listener.
- Partition keys in Spark progress JSON are strings (`"0"`, `"1"`). Normalize before comparing
  with integer partitions from admin APIs.
- Compute percentiles on per-partition lag, not on summed lag.
- For Delta, `reservoirVersion` in the end offset can point to the next version to read, so a
  lag of 1 may mean "caught up". Calibrate the threshold with a known caught-up state.
- Report lag per consumer group or job name label, otherwise two jobs overwrite each other's
  gauges in the Pushgateway.

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| MAX lag high, P90 low | one hot partition | fix the partitioning key or add salting |
| Lag grows steadily after a volume increase | consumer under-provisioned | scale out, raise `maxOffsetsPerTrigger` carefully |
| Lag metric frozen | listener not called (no new micro-batches) or push failing | alert on metric age as well |
| Consumer group shows huge lag for a Spark job | group offsets are never committed | ignore group lag, use the listener metric |
| Lag spikes at every deploy | restart replays from checkpoint | expected; add a short alert delay |

## Implementation notes per stack
**Spark** - `examples/lag_detector/spark/streaming_lag_listener.py` (Kafka listener,
plus a version lag for Delta). The Kafka source reports `latestOffset` in its progress, so no
extra admin call is needed:

```python
def onQueryProgress(self, event) -> None:
    source = event.progress.sources[0]
    processed = json.loads(source.endOffset)[TOPIC]
    latest = json.loads(source.latestOffset)[TOPIC]
    lags = {p: latest[p] - processed.get(p, 0) for p in latest}
    # per-partition gauge + visits_reader_lag_max + visits_reader_lag_p90 -> push_to_gateway
```
`delta_version_lag()` returns `MAX(version)` from `DESCRIBE HISTORY` minus
`query.lastProgress["sources"][0]["endOffset"]["reservoirVersion"]`.

**Kafka** - `examples/lag_detector/kafka/consumer_group_lag.py`: committed offset of a group vs
high watermark per partition, with MAX/P90 alerting. Its self-test asserts the average
trap numbers:

```python
s = summarize([10, 5, 30, 2, 3, 5, 3])
assert round(s["avg"]) == 8 and s["p90"] == 18 and s["max"] == 30
```

**Airflow** - `examples/lag_detector/airflow/partition_lag_dag.py`: lag on a time-partitioned
store, newest `hour=` folder minus the last processed hour stored by the consumer. The task
fails when the lag exceeds `MAX_LAG_HOURS`.

## Related patterns
- [SLA Misses Detector](sla_misses_detector.md): complementary. A throttled job meets its SLA while lag grows; a daily batch has no lag but can miss its SLA.
- [Skew Detector](skew_detector.md): explains MAX-lag outliers caused by unbalanced partitions.
- [Flow Interruption Detector](flow_interruption_detector.md): zero lag can also mean the producer stopped.
- [Checkpointer](../../error-management-patterns/references/checkpointer.md): the checkpoint is where Spark keeps the processed position the listener reads.
- [Horizontal Partitioner](../../data-storage-patterns/references/horizontal_partitioner.md): even partitions keep MAX lag meaningful.
