# Flow Interruption Detector

Category: Data Detectors.

## Problem
A streaming job synchronizes records to an object store, and many batch jobs owned by other
teams read that dataset. After seven months of normal operation the job keeps consuming
input but stops writing output. It does not fail, so nobody notices until a consumer
complains that no new data arrived. Data unavailability spreads to every
downstream dependency. You want to detect it yourself instead of relying on consumer
feedback.

## Solution / Mechanics
Alert when no new data has been observed for longer than an accepted threshold. The
implementation depends on the processing mode and on the layer you observe.

**Streaming (data in motion)**
- *Continuous delivery*: you expect at least one record (or N records) every unit of time.
  Alert as soon as a single unit of time has no data, for example no record for 1 minute.
- *Irregular delivery*: gaps are normal, for example up to 5 empty minutes. Evaluate a
  window of consecutive units and alert only when the no-data period exceeds the accepted
  window. Using the continuous rule here produces false positives and alarm fatigue.

```
continuous (unit = 1 min)          irregular (accepted gap = 3 min)
 t:  1  2  3  4  5  6               t:  1  2  3  4  5  6  7
 n:  4  6  0  5  3  0               n:  4  0  0  6  0  0  0
           ^ALERT     ^ALERT                            ^ALERT (3 consecutive empty)
```

**Batch / data at rest**: check freshness on one of three layers.

| Layer | Signal | Cost | Caveat |
|-------|--------|------|--------|
| Metadata | table modification or commit time (`pg_xact_commit_timestamp`, Delta/Iceberg commit log) | cheapest, no data scan | may be missing; schema changes also update it |
| Data | `modified_at` column, or row count compared with the previous evaluation | needs a query, maybe extra storage for past counts | count only proves "something changed" |
| Storage | time of the last file written in the location | works for raw JSON too | compaction writes files without new data |

Step by step:
1. Pick the signal (record rate, commit time, max modification column, row count, last file time).
2. Pick the evaluation unit and the threshold (per unit for continuous, window for irregular,
   schedule interval for batch, for example 1 hour for an hourly table).
3. Collect the signal: broker metrics, a producer-side metric pushed after each successful
   write, or a scheduled query.
4. Evaluate in the alerting layer (Grafana/Prometheus rule, Airflow task failure) and notify.

```
producer job --write--> table/topic
     |                        |
     | push last_update_time  | metadata / data / storage signal
     v                        v
  Pushgateway / metrics  <-- scheduled check --> alert (no change > threshold)
```

## Consequences & trade-offs
Three traps:
- **Threshold**: "at least one record per minute" is easy but unrealistic when you normally
  receive hundreds per minute. Thresholds derived from past volume are common but give false
  positives during unusual activity such as a marketing operation.
- **Metadata**: cheap, but the layer may not exist for your database. When it exists, the
  modification time can also move on metadata operations such as schema evolution, which
  add no records.
- **False positives for storage**: housekeeping such as compaction creates new files without
  new data. Storage looks active while the dataset is unchanged, which hides a real interruption.

## Key design decisions

| Decision | Options | Choose by |
|----------|---------|-----------|
| Delivery model | continuous / irregular | real arrival pattern; measure it before choosing |
| Signal source | broker metric, producer metric, metadata, data, storage | what the store exposes; prefer metadata, then data |
| Threshold | fixed count, historical baseline, per-hour baseline | volume stability and seasonality |
| Who measures | producer (push) or external checker (pull) | producer push catches "runs but writes nothing" |
| Action | page, ticket, block downstream | criticality of consumers |

**When to use / when not to use**

| Use it when | Do not use it (alone) when | Alternative |
|-------------|----------------------------|-------------|
| A dataset feeds consumers that expect regular updates | Data arrival is truly event-driven with no expected rhythm | Readiness Marker or External Trigger to start consumers only when data exists |
| A job can succeed without writing (silent no-op) | You need to know whether the volume is complete | Skew Detector |
| Freshness is part of the contract | You need to know how far behind a consumer is | Lag Detector |

## Gotchas & best practices
- Emit the freshness metric only after a write that actually contained rows. Pushing it on
  every micro-batch, including empty ones, reproduces the silent failure.
- Exclude housekeeping commits (OPTIMIZE, VACUUM, `rewrite_data_files`, SET TBLPROPERTIES)
  when reading table history.
- In PostgreSQL, `track_commit_timestamp = on` must be set before the commits you want to
  observe; older rows return NULL. Treat NULL as "unknown", not "fresh".
- Use the same time zone (UTC) for "now" and for stored timestamps.
- Alert on "no data" separately from "job down". A missing metric is itself a signal: add an
  `absent()` rule in Prometheus.
- Per-hour or per-weekday thresholds reduce noise for traffic with strong daily patterns.

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Alert storms at night | continuous rule on irregular traffic | switch to a window-based rule |
| No alert although no new rows | compaction or schema change updated metadata or files | filter data-changing operations only |
| Freshness metric never updates | gauge pushed under a different job/label, or Pushgateway unreachable | alert on `absent(metric)` and check the push errors |
| Alert after every deploy | producer restart resets the gauge to 0 | `set_to_current_time()` at the first real write, not at start |
| Row-count check passes but data is stale | deletes and inserts cancel out | use commit time or a `modified_at` column |

## Implementation notes per stack
**Kafka + Prometheus**: record rate per minute from broker metrics, alert
when the last 5 values are 0 (Grafana rule). Without a metrics stack,
`examples/flow_interruption_detector/kafka/topic_flow_monitor.py` reads partition high
watermarks and implements both modes:

```python
def is_interrupted(new_records_history, mode, window, min_records=1):
    if mode == "continuous":
        return new_records_history[-1] < min_records
    recent = list(new_records_history)[-window:]
    return len(recent) == window and all(n < min_records for n in recent)
```

**Spark + Delta** - `examples/flow_interruption_detector/spark/delta_writer_with_freshness_metric.py`
pushes `visits_last_update_time` only after non-empty writes and offers a
metadata-layer check over `DESCRIBE HISTORY` that keeps data-changing operations only:

```python
if batch.count() > 0:
    batch.write.format("delta").mode("append").save(TABLE_PATH)
    push_last_update_time()          # Gauge.set_to_current_time() + push_to_gateway
```
Alert rule: `time() - visits_last_update_time > 3600`.

**Airflow + PostgreSQL** - `examples/flow_interruption_detector/airflow/postgres_flow_interruption_dag.py`
runs hourly. It checks the metadata layer with `pg_xact_commit_timestamp(xmin)` and the data layer with a row count stored in an Airflow Variable:

```sql
SELECT CAST(EXTRACT(EPOCH FROM NOW() - MAX(pg_xact_commit_timestamp(xmin))) AS INT)
FROM dp.visits_flattened
```
The task raises `AirflowFailException`, and `on_failure_callback` sends the alert.

## Related patterns
- [Skew Detector](skew_detector.md): the next step after "is there data?" is "is the volume plausible?".
- [Lag Detector](lag_detector.md): data may be flowing, but the consumer may be behind it.
- [SLA Misses Detector](sla_misses_detector.md): an interruption upstream often shows up first as an SLA miss.
- [Readiness Marker](../../data-ingestion-patterns/references/readiness_marker.md): prevents consumers from starting on missing data; the detector alerts on it.
- [Audit-Write-Audit-Publish](../../data-quality-patterns/references/audit_write_audit_publish.md): AWAP cannot catch a run that never happens; this pattern can.
- [Compactor](../../data-ingestion-patterns/references/compactor.md): the source of storage-layer false positives.
