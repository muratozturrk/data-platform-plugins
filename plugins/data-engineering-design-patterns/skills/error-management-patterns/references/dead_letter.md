# Dead-Letter

> Category: data error, consumer side. Batch and streaming.

## Problem

A streaming job copies visit events from Kafka to an object store. Producers start emitting
unprocessable records and the job fails. For three days in a row someone relaunches it and edits
offsets in the checkpoint by hand. The fix has to keep the pipeline running through
occasional bad records and keep those records for later investigation.

Two error kinds are involved:

| Kind | Example | Recovers by itself? | Right tool |
| --- | --- | --- | --- |
| Transient | short DB outage, timeout, throttling | yes | automatic retries with backoff |
| Non-transient ("poison pill") | malformed JSON, missing field, bad type | never | **Dead-Letter** |

Fail-fast stays the right default for short batch jobs where a human looks at every failure. It
stops working for long-running streaming jobs.

## Solution / Mechanics

Four components: error handling logic, dead-letter storage,
monitoring layer, and an optional replay pipeline.

1. **Find the likely fail spots**: custom mapping functions, parsing, casts, lookups.
2. **Add a safety control at each spot**:
   - mapping function: `try/except` that emits the record to a side output instead of raising;
   - error-safe transformation (SQL `CONCAT`, `TRY_CAST`, `from_json`...): an **if-else on the
     output**. Input present but output NULL means an unprocessable record.
3. **Decorate the failed record** with the error message and context (job, version, time, source
   offset). This is the Metadata Decorator pattern.
4. **Configure a separate output** for failures. Pick a destination that is:
   resilient (you do not want a dead-letter strategy for the dead-letter store), easy to monitor,
   and fast to write. Object stores and streaming brokers fit.
5. **Monitor** how often records fail and how many failed recently. Occasional errors look
   different from a system going down.
6. **Optionally replay**: a pipeline that feeds fixed records back into the main flow.

```
                         +-------------------- monitoring ------------------+
                         |  dead-letter count / ratio / trend -> alert/stop |
                         +-----------------------^--------------------------+
                                                 |
 source --> [ processing with safety control ] --+--> valid output --> consumers
                    | try/except  or  if-else on NULL output
                    v
           failed record + error metadata
                    |
                    v
           dead-letter storage (topic / object store / table)
                    |
                    +--(optional, after the fix)--> replay pipeline --> main flow
                                                    (flag: was_dead_lettered)
```

Streaming handles **one record at a time** and writes each failure as it happens. Batch works on
a dataset and usually writes a **subset of failed rows at once**.

## Consequences & trade-offs

| Consequence | What happens | Mitigation |
| --- | --- | --- |
| Snowball backfilling | Replayed rows land in partitions consumers have already processed, so they must backfill, and so must *their* consumers | Skip replay and accept the loss, or replay and notify direct consumers. Either way you trade completeness against consistency |
| Dead-lettered records identification | Replayed rows look like normal rows | Add `was_dead_lettered` or richer replay metadata (job, version, replay time) so consumers can filter or trace |
| Ordering and consistency | Five minutes of dead-lettered IoT events close a five-minute-inactivity session too early. Replaying 10:01 after 10:00 and 10:02 breaks ordering | Don't use Dead-Letter for order-sensitive or session-sensitive consumers without a reordering step; consider fail-fast there |
| Error-safe functions | No exception to catch; semantics differ per function | Compare output against input (`input IS NOT NULL AND output IS NULL`), and learn each function's NULL rules |
| Error or failure? | The pattern hides errors and can hide a fatal failure | Alert, and **stop the job** when the dead-letter ratio exceeds a threshold |

Cost: an extra write path, which slows the job. It also adds a dataset that needs retention,
access control (it often holds PII) and an owner.

## Key design decisions

- **Which errors to dead-letter**: non-transient only. Retry transient errors a bounded number of
  times. If they persist, stop *without* committing so the record is re-read later; never park it
  in the DLQ.
- **Storage**: a Kafka topic (replayable, streaming-friendly) and/or a Delta/object-store table
  (SQL triage). Use the same type as the main sink, or a different one.
- **Envelope**: keep the original payload as a raw string, because typed columns fail for the same
  reason the record failed. Add the error type and message, the job name and version, and the
  source position (topic/partition/offset or file/partition).
- **Threshold**: the ratio, plus a minimum sample size, decides between "occasional error" and
  "failure". Raise to stop the pipeline.
- **Replay policy**: do you replay at all, which consumers you notify, and how you flag replayed
  rows.

## When to use / when not

| Situation | Use Dead-Letter? | Alternative |
| --- | --- | --- |
| Long-running stream, sporadic poison pills | Yes | none |
| Batch job, a few bad rows must not block the daily load | Yes (subset write) | Fail-fast with manual fix if volumes are tiny |
| Output must be complete and strictly ordered (sessions, ledgers) | Rarely | Fail-fast, then fix and replay in order |
| Errors are transient (DB down, timeouts) | No | Retries / backoff; stop and resume from checkpoint |
| Most records fail after a release | No (that is a failure) | Stop the job, roll back the code |
| Records rejected by business filters on purpose | No | Filter Interceptor (counts), not a DLQ |

## Gotchas & troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Records vanish: neither in output nor in DLQ | Offsets committed before DLQ writes were flushed | `producer.flush()` then `consumer.commit()` |
| DLQ rows duplicated after a micro-batch retry | Non-transactional DLQ sink (Kafka) in `foreachBatch` | Dedupe on replay, or use Delta `txnAppId`/`txnVersion` for table sinks |
| Parse failures not detected in Spark | `from_json` returns a struct of NULLs (Spark 3+), not a NULL struct | Use a `try/except` mapper, or check required fields with the if-else rule |
| A poison pill retried forever, partition blocked | Every exception treated as transient | Classify exceptions; dead-letter non-transient ones immediately |
| Job "healthy" but dataset half empty | No ratio guard | Fail above `max_error_ratio`; alert on the trend |
| Spark query computed twice | Two writes on an un-persisted flagged dataset | `persist()` before splitting |
| Consumers double count after replay | Replay appended rows that were already replayed | Replay with MERGE / `whenNotMatchedInsert`, flag `was_dead_lettered` |

## Implementation notes per stack

**Spark Structured Streaming**: `examples/dead_letter/spark/spark_streaming_dead_letter.py`.
PySpark has no Flink-style side output, so the try/except mapper returns an `error` column, and
`foreachBatch` splits on it:

```python
def map_visit(raw_value: str) -> dict:
    try:
        evt = json.loads(raw_value)
        return {"visit_id": str(evt["visit_id"]), "event_time": dt.datetime.fromisoformat(evt["event_time"]),
                "page": evt["page"], "error": None, "raw_value": None}
    except Exception as error:
        return {..., "error": f"{type(error).__name__}: {error}", "raw_value": raw_value}

mapped = source.mapInPandas(map_partition, out_schema)
# foreachBatch: persist -> valid.write(txnVersion=batch_id) / failed -> Kafka DLQ + Delta DLQ
```

**Spark batch, error-safe functions**: `examples/dead_letter/spark/spark_error_safe_dead_letter.py`
:

```sql
CASE WHEN (full_name IS NOT NULL OR version IS NOT NULL) AND name_with_version IS NULL
     THEN 'CONCAT(full_name, version) returned NULL' END AS failure_reason
```
```python
flagged.persist()
valid.write.format("delta").mode("overwrite").option("replaceWhere", partition_filter).save(...)
invalid.write.format("delta").mode("overwrite").option("replaceWhere", partition_filter).save(...)
```
`replay` re-applies the fixed logic and MERGEs with `was_dead_lettered = true`.

**Kafka**: `examples/dead_letter/kafka/kafka_dead_letter_consumer.py`. Transient errors are retried
and then stop the consumer. Non-transient errors go to the DLQ topic with an envelope. One
`flush()` and one `commit()` per poll, and an `ErrorRateGuard` that raises above the ratio.
Replay adds `was_dead_lettered` / `replay_id` headers.

**Airflow**: `examples/dead_letter/airflow/dead_letter_monitor_dag.py`. Runs the load job, then
`evaluate_dead_letters` reads the metrics JSON: it fails the run on a failure-level ratio and
warns on a trend above 3x the 7-run average. A branch runs the optional replay and then notifies
consumers about the backfill.

Flink equivalent: side outputs (`OutputTag` + `get_side_output`).

## Related patterns

- **Metadata Decorator**: how failed records carry their error context.
- **Late Data Detector**: its late output is a dead-letter-like side channel for time errors.
- **Static / Dynamic Late Data Integrator**: the same snowball backfilling trade-off as replay.
- **Windowed Deduplicator**: run it after replays that may re-emit records.
- **Checkpointer**: keeps the "stop on failure" option safe, since the job resumes where it stopped.
- **Filter Interceptor**: intentionally filtered rows are statistics, not dead letters.
- **Idempotency patterns**: make replay and retried DLQ writes safe.
