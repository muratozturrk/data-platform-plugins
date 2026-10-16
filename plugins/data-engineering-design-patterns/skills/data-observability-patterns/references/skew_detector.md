# Skew Detector

Category: Data Detectors.

## Problem
After the Flow Interruption Detector is in place, consumers complain again: the batch job
processed an incomplete dataset. The job worked, but the input was half empty because the
data provider had generation issues. You want the pipeline to always process a
complete dataset. "Skew" covers two cases here: different volumes in two consecutive
executions, and unbalanced partitions in a storage system.

## Solution / Mechanics
Three steps:
1. **Comparison window**: decide which periods to compare, for example today's partition with
   yesterday's for a daily job, or two processing-time or event-time windows in streaming.
2. **Tolerance threshold**: how different the compared datasets can be. 50% means the job
   accepts 50% less or 50% more data than the previous window. Derive it from past variations
   or ask the business users.
3. **Skew calculation**:
   - *Window-to-window*: percentage difference between two values, `current / previous`.
     Works for batch and streaming windows.
   - *Ratio of standard deviation*: `STDDEV(x) / AVG(x)`, where `x` is the observed metric
     per partition (rows, bytes). Use it for partitioned stores such as Kafka topics or
     PostgreSQL partitioned tables. Most stores and PromQL ship `stddev` and `avg`.

```
window-to-window (tolerance 50%)          ratio of stddev (partitions)
 day      D-2    D-1    D                  p0   p1   p2   p3
 rows    1000   980    430                 250  260  240  900
 ratio          0.98   0.44 -> FAIL        stddev/avg*100 = 69% -> FAIL (threshold 40%)
```

Place it in the first **Audit** step of Audit-Write-Audit-Publish, where it acts as a guard
against processing partial datasets:

```
sensor (partition ready) --> skew audit --> transform --> audit --> publish
                                 | ratio outside [1-t, 1+t]
                                 v
                           fail + alert (nothing written)
```

## Consequences & trade-offs
Three issues:
- **Seasonality**: the biggest challenge. A ±50% rule holds only if the business is stable.
  Ad campaigns can add 50% more records; seasonal businesses have higher summer or winter
  volume. Use business knowledge to build the comparison formula and add exceptions or
  adapted thresholds for the most variable periods.
- **Communication**: even a well-tuned threshold gives false positives, for example after a
  very successful campaign. The mitigation is organizational: synchronize with other
  departments so alerts can be adapted for known periods.
- **Fatality loop**: with day-to-day comparison, a day three times smaller fails the check.
  If nothing is fixed by the next day, the next *valid* day is three times bigger than the
  failed one and fails too. Ask the producer to fix the data. If that is not possible before
  the next run, compare with the dataset of the **most recent successful run** (here, two
  days ago) instead of the previous day.

```
D-1 ok (1000) | D fails (330, ratio 0.33) | D+1 valid (1000)
  naive  : D+1 vs D   -> 3.0  -> FAIL again (loop)
  correct: D+1 vs D-1 -> 1.0  -> PASS (last successful baseline)
```

## Key design decisions

| Decision | Options | Notes |
|----------|---------|-------|
| Metric | row count, file size, bytes per partition | file size is cheapest; compression and format changes distort it |
| Baseline | previous window, last successful run, same weekday last week, rolling median | last successful run breaks the fatality loop |
| Tolerance | symmetric ±t, asymmetric (small drops are worse than growth) | agree with consumers |
| Exceptions | calendar of campaigns and holidays | owned with the business |
| Action | fail (blocking audit) or warn (non-blocking) | blocking when consumers cannot handle partial data |

**When to use / when not to use**

| Use it when | Do not use it when | Alternative |
|-------------|--------------------|-------------|
| Batch inputs have a homogeneous volume per run | Volume is naturally erratic (event-driven, few records) | Constraints Enforcer on content, or completeness flags from the producer |
| Partitions should be balanced (Kafka, partitioned tables) | You only need to know if any data arrived | Flow Interruption Detector |
| Partial datasets would produce wrong aggregates | The producer can publish an explicit completeness marker | Readiness Marker with row counts in a manifest |

## Gotchas & best practices
- Store a run's volume as a baseline only **after** the run passes. Failed runs must never
  become baselines.
- With STDDEV/AVG over very few partitions, one partition moves the ratio a lot. Set a
  minimum partition count before alerting.
- Population vs sample standard deviation: PostgreSQL `STDDEV` is the sample version; Python
  `statistics.pstdev` and Spark `stddev_pop` are population. Keep one definition per metric.
- High partition skew in Kafka usually means a hot partitioning key. It also inflates
  MAX-based lag (see Lag Detector).
- Put ratio, baseline value and baseline date in the error message so on-call can judge
  quickly.

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Every Monday fails | weekly seasonality vs day-to-day baseline | compare with the same weekday of the previous week |
| Two consecutive failures after one bad day | fatality loop | baseline = last successful run |
| Skew alert on a tiny topic | few partitions or near-zero averages | min partitions and min volume guards |
| Ratio division by zero | empty previous window | treat an empty baseline as "no comparison" and rely on the Flow Interruption Detector |
| False alert after a codec change | file-size metric | switch to row counts from metadata |

## Implementation notes per stack
**PostgreSQL** - skew of partitioned tables from catalog statistics
(in `examples/skew_detector/README.md`):

```sql
SELECT NOW() AS "time", (STDDEV(n_live_tup) / AVG(n_live_tup)) * 100 AS value
FROM pg_catalog.pg_stat_user_tables
WHERE relname != 'visits_all_range' AND relname LIKE 'visits_all_range_%';
```

**Kafka** - `examples/skew_detector/kafka/partition_skew_detector.py` computes records per
partition from watermarks (`high - low`) and the same ratio. The PromQL equivalent is
`stddev(sum(kafka_log_size{topic='visits'}) by (partition)) / avg(kafka_log_size{topic='visits'}) * 100`.

**Spark** - `examples/skew_detector/spark/volume_skew_guard.py`: window-to-window guard against
the last successful volume kept in a Delta stats table, a seasonal exception set, and key skew:

```python
baseline = last_successful_volume(spark)
if baseline and partition_date not in SEASONAL_EXCEPTIONS:
    ratio = current / baseline
    if ratio > 1 + TOLERANCE or ratio < 1 - TOLERANCE:
        raise SkewDetected(...)
# ... write/audit/publish ...; append (partition_date, current) to STATS_TABLE only now
```

**Airflow** - `examples/skew_detector/airflow/volume_skew_dag.py` follows the classic sensor flow
(`FileSensor >> compare_volumes >> transform >> load`) but takes the baseline from
`prev_data_interval_start_success`, so a failed day is never the reference:

```python
baseline_date = context["prev_data_interval_start_success"]
ratio = os.path.getsize(file_path(context["data_interval_start"])) / os.path.getsize(file_path(baseline_date))
if not MIN_RATIO <= ratio <= MAX_RATIO:
    raise AirflowFailException(...)
```

## Related patterns
- [Flow Interruption Detector](flow_interruption_detector.md): detects "no data"; the Skew Detector detects "wrong amount of data".
- [Audit-Write-Audit-Publish](../../data-quality-patterns/references/audit_write_audit_publish.md): the skew check is a natural first Audit step.
- [Readiness Marker](../../data-ingestion-patterns/references/readiness_marker.md): the sensor that starts the DAG before the skew check.
- [Lag Detector](lag_detector.md): partition skew distorts MAX lag.
- [Horizontal Partitioner](../../data-storage-patterns/references/horizontal_partitioner.md): bad partitioning keys are the root cause of storage skew.
