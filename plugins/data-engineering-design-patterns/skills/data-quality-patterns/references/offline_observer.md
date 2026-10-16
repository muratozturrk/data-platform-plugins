# Offline Observer

> Category: quality observation, detached from the pipeline. Batch and streaming.

## Problem

A new pipeline started this month with few data quality issues: the dataset is structured and
the enforcement patterns cover the business rules. From your previous project you know this will
not last, because the upstream dataset will evolve. You want to monitor properties of the dataset,
such as value distributions and NULLs per column. Since everything is fine for now, this
monitoring **must not block** the main pipeline.

## Solution / Mechanics

A separate **data observability job** analyzes the processed records and adds insight to the
monitoring layer. The insight depends on the business context: value
distributions, NULLs in nullable fields, new input fields that are not processed yet, and so on.
Stored over time, these parameters show quality issues as they appear.

```
 producer pipeline (daytime) --> dp.visits_output -------------------------> consumers
                                        |  (read only, own schedule, e.g. nightly)
                                        v
 observer:  wait_for_new_data --> record_new_observation_state --> insert_new_observations
                                  (first_row_id, last_row_id)      (counts, NULLs, distinct,
                                                                   lag, profile report)
                                                                          |
                                                    monitoring tables / dashboards / alerts
                                                                          |
                                                feedback: add / change / delete enforcement rules
```

1. **Run independently**, possibly on a different schedule; e.g. observe at night when the
   producers are idle, to avoid resource contention.
2. **Freeze the observation scope** before computing anything. Record the first and
   last row id of the run in a state table, so a rerun analyzes exactly the same rows even if the
   observed table changed.
3. **Compute observations** over that scope: row count, invalid/NULL counts per column, optionally a full profile (ydata-profiling HTML report).
4. **Observe the producer, not only the data**: in streaming, compare the offsets the producer
   last committed in its checkpoint with the latest offsets in the topic to get its lag.
5. **Feed findings back** into AWAP / Constraints Enforcer rules.

**Observability is not auditing**: an audit validates and blocks the
pipeline on issues; observability monitors, detects, and lets the pipeline move on.

## Consequences & trade-offs

| Consequence  | What happens | Mitigation |
| --- | --- | --- |
| Time accuracy | The observer may run much later than the producer; consumers may already have used bad data | Run more often, or switch to the Online Observer |
| Compute resources | A less frequent observer processes bigger batches (24 hourly loads at once) | Size the observer cluster for the batch, or observe more often |
| Sampling | Observing a sample saves compute but can miss interesting observations | Sample only expensive profiling; keep counts and NULL metrics exhaustive |

## Key design decisions

- **Cadence**: nightly vs hourly, a trade-off between latency of insight and cost per run.
- **Scope tracking**: row-id range, partition, or offset range. It must be deterministic for
  idempotent reruns.
- **Metrics**: start with volume, NULL/invalid counts per column, distinct counts, unknown
  fields and lag; add profiles when you need distributions.
- **Storage of observations**: a monitoring table (trendable, alertable), not only logs.
- **Isolation**: own consumer group / own checkpoint so the observer never moves the producer's
  offsets.

## When to use / when not

| Situation | Offline Observer? | Alternative |
| --- | --- | --- |
| Data is fine today; you want early warning of drift without risk to production | Yes (the problem above) | - |
| Insight must arrive before consumers read the data | No | Online Observer |
| Violations must stop publication | No, observers don't block | AWAP, Constraints Enforcer |
| Observation is too heavy for the producer's cluster | Yes, run it elsewhere | - |
| You need job health (CPU, lag) in near real time | Partially | Online Observer / Lag Detector (data observability patterns) |

## Gotchas & troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| Rerun produces different numbers | Scope computed at run time from a moving table | Persist first/last row id per execution, then compute over that range |
| Observer "steals" messages or moves offsets | Shares the producer's consumer group or checkpoint | Own group id / checkpoint (Kafka and Spark examples) |
| Lag reads a batch that never finished | Spark writes `offsets/<n>` before `commits/<n>` | Read the latest batch present in both, or accept a one-batch overestimate |
| Sensor never fires after a backfill | "Last observed id" taken from the wrong execution | Use `MAX(last_row_id) WHERE execution_time < logical_date`, as in the DAG |
| Profile report OOMs the driver | `toPandas()` on the full batch | Profile a sample (`fraction=0.1`) and keep exhaustive SQL metrics |
| Alerts but nobody acts | No feedback loop to enforcement | Review observations and turn recurring issues into AWAP / constraint rules |

## Implementation notes per stack

**Airflow** - `examples/offline_observer/airflow/offline_observer_dag.py`: nightly DAG with SqlSensor, idempotent state, then per-column invalid counts.
```python
RECORD_STATE = f"""
DELETE FROM dp.visits_monitoring_state WHERE execution_time = '{{{{ logical_date }}}}';
INSERT INTO dp.visits_monitoring_state (execution_time, first_row_id, last_row_id)
SELECT '{{{{ logical_date }}}}', MIN(id), MAX(id) FROM dp.visits_output WHERE id > {LAST_OBSERVED_ID};
"""
create >> wait_for_new_data >> record_state >> insert_obs
```

**Spark** - `examples/offline_observer/spark/spark_streaming_offline_observer.py`: Structured Streaming job with its own checkpoint and a 10-minute trigger.
NULL metrics go to Delta, an optional profile is built on a sample, and producer lag is computed
from the producer's checkpoint.
```python
def get_offsets_lag() -> dict:
    processed = read_last_processed_offsets()        # <producer checkpoint>/offsets/<n>
    available = read_last_available_offsets(processed.keys())   # high watermarks
    return {str(p): available[p] - processed[p] for p in processed}

if os.environ.get("PROFILE") == "1":
    sample = visits.sample(fraction=0.1, seed=batch_id).toPandas()
    ProfileReport(sample, minimal=True).to_file(f"{BASE_DIR}/profile_{batch_id}.html")
```

**Kafka** - `examples/offline_observer/kafka/kafka_offline_observer.py`: standalone consumer group
emitting per-window metrics (NULL/empty counts, unknown upstream fields) and the processing
group's lag (high watermark - committed offset), read through a consumer that never commits.
```python
unknown.update(set(r) - EXPECTED_FIELDS)   # new upstream fields we don't process yet
...
committed = observed.committed([TopicPartition(TOPIC, p) for p in partitions], timeout=10)
for tp in committed:
    high = consumer.get_watermark_offsets(tp, timeout=10)[1]
    lag[tp.partition] = high - tp.offset if tp.offset >= 0 else high
```

## Related patterns

- **Online Observer**: same logic inside the pipeline when latency of insight matters.
- **Audit-Write-Audit-Publish**: the blocking counterpart. Observations tell you which audits to add.
- **Constraints Enforcer**: target of the feedback loop for stable rules.
- **Lag Detector / Flow Interruption Detector** (data observability patterns): observe the job's timeliness, not only its data.
- **Incremental Loader** (data ingestion patterns): the row-id range state is the same mechanism.
