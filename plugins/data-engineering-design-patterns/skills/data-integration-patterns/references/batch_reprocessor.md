# Batch Reprocessor

Category: Data Reprocessing.

## Problem
Another bug is found in the streaming application, three weeks after it started.
The Kafka topic retention is two weeks, so the Streaming Reprocessor cannot reach the data.
Fortunately, the raw data from the topic is kept in the lakehouse for six months. You need a way
to use that copy for reprocessing. Keeping such a copy is good practice in general: it also
serves batch use cases such as BI reports, and it is subject to the same privacy regulations as
the streaming data.

## Solution / Mechanics
Reprocess from a different store than the real-time job uses, with the same business logic
.

1. **Decouple business logic from I/O**: filters, groupings and mappings live in their own
   module so other jobs can use them with different inputs and outputs:

```
 coupled:    readStream(kafka) -> select(from_protobuf(...)) -> writeStream(kafka)
 decoupled:  readStream(kafka) -> extract_columns(df)        -> writeStream(kafka)   real time
             read(delta)       -> extract_columns(df)        -> write(kafka)         reprocessing
```

2. **Mark reprocessed output** so consumers can distinguish it from real-time records:
   - Wrapper: `origin: reprocessing` inside the message;
   - Metadata Decorator: origin in the metadata layer (Kafka header), invisible unless fetched.
3. **Choose the execution mechanism**:
   - *CI/CD deployment pipeline*: IaC (Terraform) creates the reprocessing stack, runs the job,
     destroys it;
   - *data orchestration layer*: run the reprocessing job like other batch workloads;
   - *combined*: CI/CD provisions the infrastructure and triggers the job in the orchestrator
     (External Trigger); the infrastructure is destroyed afterwards but the run history stays
     in the orchestrator.
   No option is best; choose by the tools in daily use.

**Stateful pipelines**:
- *Isolated input*: event-time windows are self-contained. Reprocessing 10:00-11:00 with
  10-minute windows needs only that range; filter by window range instead of offsets.
- *Non-isolated input*: arbitrary state such as sessions may start at 09:50, 08:50 or the day
  before. Approaches:
  - **increase state retention** in the streaming job (or back up state regularly): easy, but
    you never know what will need backfilling; three months may not cover a six-month request;
  - **accept approximate results**: consider only the reprocessed range, or a fixed number of
    adjacent partitions;
  - **full state reconstruction from adjacent partitions**: (1) read all users of the
    reprocessed range; (2) fetch their earlier records as long as they do not break the session
    rule; (3) do the same for future partitions.

```
 partitions:   08:00   09:00   10:00   11:00   12:00
 user u1:        .  e e  e  |  e   e  e |  e  e   .        gap > 30 min ends a session
                  <-- extend past --  [ range ]  -- extend future -->
 materialize: u1 events 08:40..11:20 -> run shared session logic -> keep sessions touching range
```

Data preparation: query `MIN/MAX(event_time)` per user in the
range, then loop over past partitions (bounded by `max_iterations`), keep only records of the
same session (`LEAD` + inactivity gap), append them to a reprocessing table, and move the user
edge to the new minimum; stop when nothing new is found.

## Consequences & trade-offs
Two main issues:
- **Shared is not the same**: the code is designed for real time, and the application evolves.
  Supporting both Kafka and Delta inputs adds complexity. Software patterns help: Factory /
  Abstract Factory to build environment-specific classes (real time vs reprocessing), Decorator
  / Adapter to adapt a different reprocessing schema to the input expected by the shared logic,
  Template to share the processing and vary only steps such as output decoration.
- **State recovery**: arbitrary stateful logic needs retention, approximation or
  reconstruction. Reconstruction is correct but needs extra code and reads more data than the
  requested range.
- Reprocessed output duplicates earlier output by key; consumers must upsert or filter by origin.
- A second execution path (CI/CD or orchestrator) must be maintained and tested before the day
  you need it.

## Key design decisions

| Decision | Options | Notes |
|----------|---------|-------|
| Source | raw lakehouse copy with offsets/partitions/timestamps | without offsets you cannot target exact ranges |
| Range | offsets, event-time window range, users + adjacent partitions | depends on stateless / windowed / arbitrary state |
| Origin marking | Wrapper attribute, Metadata Decorator header | header keeps payloads identical |
| Execution | CI/CD + IaC, orchestrator, both | combined keeps history after the infra is destroyed |
| Output | same topic/table (overwrite by key), side table then swap | consumers must be ready |

**When to use / when not to use**

| Use it when | Do not use it when | Alternative |
|-------------|--------------------|-------------|
| Source retention expired but a lakehouse copy exists | The data is still in the topic and everything must be replayed | Streaming Reprocessor (simpler) |
| Only a subset (tenant, range) must be fixed | No raw copy was kept | nothing; start keeping one (Zero-ETL Synchronizer) |
| You want reprocessing isolated from real-time compute | Logic is so stateful that reconstruction is unaffordable | accept approximate results or longer state retention |
| Backfill can run in parallel with live processing | Both jobs would build state for the same windows | Hybrid Source |

## Gotchas & best practices
- Keep business logic in pure functions over DataFrames; the real-time and reprocessing jobs
  must import the same module (`--py-files`).
- Store offset, partition, append timestamp and the raw value in the lakehouse copy.
- Bound reconstruction loops (`max_iterations`) and prune partitions by time.
- Decorate output with `origin=reprocessing` and notify consumers before the run.
- Use stable keys so reprocessed results replace, not add to, earlier results.
- In joins between the visits and the edge DataFrame, qualify columns (`v.user_id`) so
  `select("v.*")` keeps the original columns.

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Reprocessed sessions shorter than real-time ones | range cut sessions at its edges | adjacent-partition reconstruction |
| Results differ from the real-time job | logic copied instead of shared | single shared module |
| Reconstruction never stops | no iteration bound, or gap condition wrong | `max_iterations` + strict gap check |
| Consumers double count | append of reprocessed rows | upsert by key, or filter by origin |
| Job scans the full table | no partition pruning on event time | filter on partition columns first |

## Implementation notes per stack
**Spark** - `examples/batch_reprocessor/spark/`:
- `visits_logic.py`: `extract_columns` (shared business logic) and `to_kafka_records(df, origin)`
  (Metadata Decorator header);
- `real_time_job.py`: Kafka -> Kafka with `origin=real_time`;
- `batch_reprocessor_job.py`: `stateless` mode and `sessions` mode with
  reconstruction from adjacent partitions:

```python
raw = spark.read.table(RAW_TABLE).filter(F.col("offset").between(start_offset, end_offset))
write_to_kafka(extract_columns(raw))          # to_kafka_records(..., origin="reprocessing")
# sessions: extend_sessions() hops past/future while
#   0 < delta(edge, event_time) <= SESSION_GAP, bounded by max_iterations
```

**Airflow** - `examples/batch_reprocessor/airflow/batch_reprocessing_dag.py`: `schedule=None`,
typed params (`mode`, `start`, `end`), triggered by CI/CD through the REST API (combined
execution mechanism); the run history stays in Airflow.

**Kafka** - `examples/batch_reprocessor/kafka/README.md`: Kafka is only the output here; covers
the raw copy, origin headers, idempotent consumers, and tiered storage as a way to extend the
Streaming Reprocessor window.

## Related patterns
- [Streaming Reprocessor](streaming_reprocessor.md): the simpler option while the data is still in the topic.
- [Zero-ETL Synchronizer](zero_etl_synchronizer.md) / [Passthrough Replicator](../../data-ingestion-patterns/references/passthrough_replicator.md): produce the raw copy this pattern depends on.
- [Wrapper](../../data-value-patterns/references/wrapper.md) / [Metadata Decorator](../../data-value-patterns/references/metadata_decorator.md): mark reprocessed output.
- [External Trigger](../../data-ingestion-patterns/references/external_trigger.md): CI/CD starts the orchestrated reprocessing run.
- [Incremental Sessionizer](../../data-value-patterns/references/incremental_sessionizer.md): batch sessionization logic reused when reconstructing sessions.
- [Hybrid Source](hybrid_source.md): a backfill alternative; parallel batch backfills need time boundaries.
