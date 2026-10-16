# SLA Misses Detector

Category: Time Detectors.

## Problem
A batch job scheduled at 06:00 must complete within 40 minutes, because critical downstream
pipelines must publish business statistics before 08:00. The job is optimized,
but unpredictable events will break the 40-minute SLA one day. You want to notify yourself and
the downstream consumers whenever the job runs longer than 40 minutes.

## Solution / Mechanics
Measure the processing time and compare it with the maximum allowed execution time. The
measurement depends on the processing mode:

- **Batch job**: `end_time - start_time`. If it exceeds the SLA, mark the run as an SLA miss
  and send the notification.
- **Streaming job, micro-batch or windowed mode**: the same subtraction per iteration (each
  execution processes a bunch of data).
- **Streaming job, per record (no windowing)**: the difference between reading and writing
  each record to the output store, aggregated with `MAX` (longest delay) and percentiles
  (overall delay). Collect the measures with the Online or Offline Observer patterns.

```
batch:        06:00 start ------------------------------ 06:47 end   -> 47 > 40 min: MISS
micro-batch:  [b1: 8 s] [b2: 9 s] [b3: 41 s] [b4: 7 s]               -> b3 > 30 s: MISS
per record:   read_ts --process--> write_ts   delay = write - read
              per 1-min window: P95(delay), MAX(delay) -> compare with SLA
```

**Two clocks**: processing time is the simplest and most common unit, but
you can also measure event time to get the end-to-end delay between data generation and
processing. Keep them separate:

```
event_time (generated) ---- network/buffer ---- read_time ---- processing ---- write_time
|<------------------ event-time SLA = write_time - event_time ------------------>|
                                                 |<- processing-time SLA ------->|
                                                    = write_time - read_time
```

A producer that loses connectivity and delivers buffered data minutes later breaks the
event-time SLA without any fault on your side, while the processing-time SLA, which depends on
how fast your job processes the input, is still respected.

**SLA vs lag**: the two time detectors are complementary, not interchangeable.
- A consumer with throughput limits always processes the same volume, so it meets its SLA,
  while lag keeps growing on a skewed partition.
- A daily batch has no lag when it starts (the data is complete), but it can still miss its
  SLA if it runs too long.

## Consequences & trade-offs
- **Late data and event time**: event-time SLAs depend on producers.
  Late-arriving data can break them for reasons outside your job, so you need the
  processing-time SLA to know whether your own processing is at fault. Monitor the
  processing-time SLA as `write - read` per record, plus the event-time SLA if relevant.
- Batch SLAs are straightforward; streaming SLAs are harder because of data arrival
  semantics and the need to aggregate millions of per-record measures.
- **Airflow 2.x SLA semantics**: the `sla` task parameter is computed from the
  DAG run's start (its scheduled time), not from the task start. With a 10-second SLA on a
  task of a run scheduled at 08:00, the task is already late if it starts after 08:00:10.
  The SLA feature was rewritten later (AIP-57); in Airflow 3.x `sla` is removed and Deadline
  Alerts replace it from 3.1.

## Key design decisions

| Decision | Options | Notes |
|----------|---------|-------|
| Clock | processing time, event time, both | always keep processing time; add event time for end-to-end contracts |
| Granularity | run, task, micro-batch, record | per-record needs aggregation (P95, MAX) |
| Where measured | orchestrator, in-job listener, separate monitoring job | a separate job keeps the main job simple |
| Aggregation window | 1 min tumbling, per batch | event-time windows need a watermark for late measures |
| Notification | team only, team + consumers | the problem explicitly includes consumers |

**When to use / when not to use**

| Use it when | Do not use it when | Alternative |
|-------------|--------------------|-------------|
| Consumers depend on a completion deadline | The real question is "is the consumer keeping up?" | Lag Detector |
| A streaming job has a latency contract | Data does not arrive at all | Flow Interruption Detector |
| You need early warning before a downstream deadline | The SLA can only be checked on the final dataset content | Audit-Write-Audit-Publish with freshness rules |

## Gotchas & best practices
- In Airflow 2.x, set the task SLA as "time since the scheduled run time", so it must include
  all upstream tasks. For a pure task duration, compare `ti.duration` in a callback.
- Airflow 2.x SLA checks need `[core] check_slas = True`, and SLAs are evaluated only for
  scheduled runs, not for manually triggered ones.
- Compute per-record delays from timestamps that are produced by the same clock source, or
  accept clock skew between hosts in the threshold.
- Never average delays. Report P95 and MAX like for lag.
- Notify consumers as soon as the SLA is at risk, not only after the job ends.

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| SLA miss on a task that ran 2 minutes | Airflow 2.x SLA measured from DAG run start | budget the SLA from the scheduled time, or check `ti.duration` |
| Event-time SLA misses at night only | producer buffering / connectivity | check the processing-time SLA; open a ticket with the producer |
| Negative delays | clock skew between producer and broker hosts | use broker append time for both sides, or NTP |
| No SLA alerts in Airflow 3 | `sla` parameter removed | use Deadline Alerts (3.1+) or explicit duration checks |
| Micro-batch SLA always met, data still late | throttling (`maxOffsetsPerTrigger`) hides growing lag | add the Lag Detector |

## Implementation notes per stack
**Airflow** - `examples/sla_misses_detector/airflow/sla_dag.py`: native `sla=` plus
`sla_miss_callback` and an explicit task-duration budget:

```python
@task(sla=datetime.timedelta(minutes=40), on_success_callback=check_task_duration)
def process_visits(path: str) -> None: ...

def check_task_duration(context) -> None:
    ti = context["ti"]
    if ti.duration > TASK_DURATION_SLA[ti.task_id].total_seconds():
        notify(...)
```

**Spark** - `examples/sla_misses_detector/spark/microbatch_sla_listener.py`: the listener
compares `event.progress.batchDuration` with the processing SLA per micro-batch; `foreachBatch`
computes the event-time P95/MAX separately with `percentile_approx`:

```python
def onQueryProgress(self, event) -> None:
    duration_ms = event.progress.batchDuration
    push("visits_sla", visits_batch_duration_ms=duration_ms,
         visits_batch_sla_missed=int(duration_ms > PROCESSING_SLA_MS))
```

**Kafka** - `examples/sla_misses_detector/kafka/record_sla_monitor.py` rewrites the Flink
per-record monitor without Flink. The processing job stamps `start_processing_time_ms` when it reads
the record; a monitor consumes the output topic, subtracts it from the append time, and
aggregates P95/MAX per 1-minute window:

```python
_, append_ms = msg.timestamp()
windows[append_ms // 60_000]["processing"].append(append_ms - int(headers["start_processing_time_ms"]))
```
In Flink, the same is a `TumblingEventTimeWindows.of(Time.minutes(1))` aggregate over a table
that exposes `append_time` with `METADATA FROM 'timestamp'`.

## Related patterns
- [Lag Detector](lag_detector.md): complementary time detector; neither replaces the other.
- [Online Observer](../../data-quality-patterns/references/online_observer.md) and [Offline Observer](../../data-quality-patterns/references/offline_observer.md): ways to collect per-record delay metrics.
- [Late Data Detector](../../error-management-patterns/references/late_data_detector.md): explains event-time SLA misses caused by producers.
- [Dataset Tracker](dataset_tracker.md): tells you whom to contact when the delay comes from upstream.
- [Metadata Decorator](../../data-value-patterns/references/metadata_decorator.md): how the processing start time travels with the record (header).
