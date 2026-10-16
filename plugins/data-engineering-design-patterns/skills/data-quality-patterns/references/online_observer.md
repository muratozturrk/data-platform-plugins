# Online Observer

> Category: quality observation, inside the data generation pipeline. Batch and streaming.

## Problem

Last week data analysts complained about an unexpected zip code format. An upstream regression
got through your trust rules. Your Offline Observer did find it, but it runs once a week, so users
noticed before you did. You want to detect such issues, and fix them, before consumers find out,
instead of up to a week later.

## Solution / Mechanics

The Online Observer runs the same observation job as the offline pattern. The difference is
**when** it runs: it is an intrinsic part of the data generation pipeline, so insight is available
right after the data is generated.

**Batch placement**. The usual spot is after the Transform stage:

```
 Local Sequencer (observe what consumers see)       Parallel Split (observe concurrently)
 extract -> transform -> load -> observe            extract -> transform -+-> load
                                    |                                     +-> observe
                               pipeline_done (all_done)                   (sees transformed data,
                                                                           not the loaded data)
```

**Streaming placement**. Observation is part of the processing job,
not a separate pipeline:
```
 kafka visits --> micro-batch --> write visits_enriched
                        |
                        +--> one pass: accumulators (invalid fields, counts,
                             max offset per partition) --> observation record
                             (errors caught: observation must not stop delivery)
```

1. Reuse the offline observation logic.
2. **Batch**: append the observation tasks after the load. Add a final no-op task
   with `trigger_rule=all_done`, so an observation failure doesn't fail the run.
3. **Streaming**: read the `partition` and `offset` columns with each record so the lag detector
   can take the most recent processed offset from the micro-batch itself.
4. Use **accumulators** to compute invalid-field counts and max offsets in one pass, instead of
   separate queries over the batch. Executors keep accumulators
   locally and send them to the driver only when `.value` is read.
5. Optionally **sample** to limit the risk of observation memory errors, accepting missed
   insights.

Observability also covers technical metadata (CPU, memory, disk). Those are near-real-time
measurements and so fit this pattern naturally.

## Consequences & trade-offs

| Consequence  | What happens | Mitigation |
| --- | --- | --- |
| Extra delays | With Local Sequencer, observation is one more step and delays pipeline completion | Keep observation SQL light; move heavy profiling to an Offline Observer |
| Parallel splits | Observing in parallel with the load can miss load-time issues (a date format the DB rejects into NULLs is invisible to the observer) | Prefer Local Sequencer to observe the exposed dataset. Observing the transformed data only checks the transformation |
| Coupled failure in streaming | Observer bugs or memory issues can fail the whole job | try/except around observation; sampling |
| Pipeline failure from observation | An observation task failure marks the run failed | Final `all_done` task as the only leaf, deciding success from the data tasks |

## Key design decisions

- **Placement**: Local Sequencer (accurate, slower) vs Parallel Split (faster, may observe a
  different dataset than consumers see).
- **Failure isolation**: Airflow `all_done` sink, try/except in `foreachBatch` or the consume loop.
- **Single pass**: accumulators / inline counters instead of re-reading the batch.
- **Sampling fraction** for streaming observation, a trade-off between safety and insight.
- **Split with offline**: cheap metrics online, expensive profiles offline.

## When to use / when not

| Situation | Online Observer? | Alternative |
| --- | --- | --- |
| Issues must surface before consumers notice (the problem above) | Yes | - |
| Pipeline SLA can't absorb an extra step | Parallel Split or offline | Offline Observer |
| Observation is heavy (full profiling) | No, keep it out of the critical path | Offline Observer |
| Bad data must not be published | No, observers don't block | AWAP |
| You need lag/throughput of the job itself | Yes, inline offsets/accumulators | Lag Detector (data observability patterns) |

## Gotchas & troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| DAG failed although the data was copied | Observation task is a leaf; run state comes from the leaves | Make `pipeline_done` (`all_done`) the only leaf and check the copy task's state in it |
| DAG succeeded although the copy failed | `all_done` no-op without a check | Raise in `pipeline_done` when `copy_new_visits` is not `success` |
| Accumulator values doubled | Accumulators updated in a transformation that was recomputed | Update them in an action (`foreachPartition`) and `persist()` the batch |
| Accumulator reads 0 | `.value` read before an action ran | Read only after `foreachPartition` returns |
| Streaming job died on an observer bug | Observation not isolated | try/except around `observe()`; the data write happens before it |
| Lag looks negative / jumps | Mixing cached and live watermarks | Compute it consistently, e.g. `high - position` from one source |

## Implementation notes per stack

**Airflow** - `examples/online_observer/airflow/online_observer_dag.py`:
synchronization pipeline followed by the offline observer's SQL (Local Sequencer), with a sole
`all_done` leaf.
```python
@task(trigger_rule=TriggerRule.ALL_DONE)
def pipeline_done(ti=None):
    copy_state = ti.get_dagrun().get_task_instance("copy_new_visits").state
    if copy_state != "success":
        raise RuntimeError(f"Data generation failed (copy_new_visits={copy_state})")

(wait_for_new_data >> record_sync_state >> clean_previous >> copy_new_visits
 >> record_obs_state >> insert_obs >> pipeline_done())
```

**Spark** - `examples/online_observer/spark/spark_streaming_online_observer.py`: `foreachBatch` writes to Kafka first, then one `foreachPartition` pass fills
accumulators, including a custom `AccumulatorParam` for per-partition max offsets.
```python
def write_to_kafka_with_observer(visits, batch_number):
    visits.persist()
    (visits.selectExpr(...).write.format("kafka")...save())   # data delivery first
    try:
        observe(visits, batch_number)
    except Exception as error:      # observation must never break data delivery
        print(f"Observation failed for batch {batch_number}: {error}")
    finally:
        visits.unpersist()
```

**Kafka** - `examples/online_observer/kafka/kafka_online_observer.py`: consume-transform-produce
loop with an `InlineObserver`. Every `OBSERVE_EVERY` messages it emits an observation with its own
lag (high watermark - position), isolated from delivery. `--self-check` runs offline asserts.
```python
producer.produce("visits_enriched", key=msg.key(), value=json.dumps(transform(visit)).encode())
try:  # observation isolated from delivery
    observer.observe(visit)
    if processed % OBSERVE_EVERY == 0:
        producer.produce("visits_observations", json.dumps(observer.flush(current_lag(consumer))).encode())
except Exception as error:
    print(f"Observation error ignored: {error}")
```

## Related patterns

- **Offline Observer**: same logic, detached. Use both: cheap metrics online, heavy profiling offline.
- **Local Sequencer / Parallel Split** (data flow patterns): the two ways to place the observation step.
- **Audit-Write-Audit-Publish**: blocks on known rules; the observer finds unknown ones.
- **Lag Detector** (data observability patterns): formalizes the offset-lag measurement done inline here.
- **Distributed Aggregator** (data value patterns): accumulators are a driver-side aggregate of executor-local values.
