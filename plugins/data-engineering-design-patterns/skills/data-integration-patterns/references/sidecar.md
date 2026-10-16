# Sidecar

Category: Data Value.

## Problem
The sessionization project must combine data from two streaming sources and emit the results to
two different data stores. From Kubernetes work with DevOps you know the Sidecar
pattern, where two complementary services share a lifecycle. You want to apply the same
principle to the streaming job.

## Solution / Mechanics
The Sidecar groups the fan-in and fan-out data flow patterns in one streaming job, for
input and output data only.

**Input side (fan-in)**: transform several sources together instead of separately, with
`UNION ALL`, which groups inputs into one processing abstraction. Harmonize the schemas first.
Otherwise every column needs conditional logic per source:

```sql
SELECT CASE WHEN src = 'topic_1' THEN event_ts
            WHEN src = 'topic_2' THEN event_timestamp
            ELSE event END AS event_time   -- ... and the same for every column
```

**Output side (fan-out)**: apply a common transformation, then write to several destinations,
possibly in different versions of the same dataset (raw rows to an object store, aggregates to
a warehouse).

```
 topic_web    --harmonize--\                         /--> object store (raw rows)
                            UNION --> shared logic --+
 topic_mobile --harmonize--/   ^                     \--> warehouse (aggregates)
 heartbeat (0 rows) -----------+  keeps micro-batches running when topics are idle
```

How the Sidecar maps to the data flow patterns:

| Condition | Underlying pattern |
|-----------|--------------------|
| all inputs must be available, or all must have new data | Aligned Fan-In |
| inputs may be missing or idle | Unaligned Fan-In |
| the write must succeed for all stores | Parallel Split |
| writes may fail for some stores (outcome decides) | Exclusive Choice |

Unlike the fan-in/fan-out patterns, which also apply to other task types such as conditional
orchestration logic, the Sidecar is reserved for input and output data.

## Consequences & trade-offs
Three issues:
- **Schema consistency**: inputs with different schemas must be transformed into a common
  schema before working with them holistically, which adds complexity.
- **Consistency versus performance**: writing to several stores from the streaming job slows
  it down. A storage-based fan-out writes once to the fastest store (a broker), and dedicated
  jobs copy to the other stores (a key-value store, an object store that needs buffering for
  bigger files). This keeps the streaming job fast but adds two jobs to deploy, monitor and
  maintain.

```
 job-based fan-out:      job --> store A
                             \-> store B          (slowest sink sets the pace)
 storage-based fan-out:  job --> broker --> job_A --> store A
                                       \--> job_B --> store B
```

- **Data consistency**: outputs rarely share a technology, so transactions are not available.
  Alternatives: Change Data Capture (write once into a store with a change feed, such as Delta
  change data feed, and let consumers stream from it); multi-table transactions, which are
  ongoing work (Delta catalog-managed tables, Iceberg multi-table transaction API).

## Key design decisions

| Decision | Options | Notes |
|----------|---------|-------|
| Fan-in semantics | aligned (wait for all), unaligned (process what arrives) | streaming unions are unaligned by nature |
| Schema harmonization | per-source select/rename before the union | one place, before shared logic |
| Fan-out location | in-job (`foreachBatch`, side outputs), storage-based | performance vs number of jobs |
| Failure semantics | all sinks required (Parallel Split), optional sinks (Exclusive Choice) | decides when offsets/checkpoints advance |
| Idempotency | Delta `txnAppId`/`txnVersion`, keyed upserts, idempotent producer | required because retries rewrite all sinks |

**When to use / when not to use**

| Use it when | Do not use it when | Alternative |
|-------------|--------------------|-------------|
| Several streams share the same business logic | Sources need different logic | separate jobs |
| One result must land in several stores | One sink is much slower than the others | storage-based fan-out |
| A legacy and a new store must be fed during a migration | Sinks need atomic consistency together | CDC from one store, or multi-table transactions when available |
| An idle source must not stall micro-batches | Orchestrated (batch) tasks | Fan-in/fan-out data flow patterns in the orchestrator |

## Gotchas & best practices
- `persist()` the micro-batch in `foreachBatch`; otherwise Spark recomputes the input once per
  writer. `unpersist()` at the end.
- Use the same `txnAppId` for the life of a checkpoint; changing it with the same checkpoint
  disables Delta's duplicate detection.
- Spark does not run a micro-batch when there is no new data; a heartbeat source
  (`rate-micro-batch` filtered to zero rows) keeps time-based logic running.
- A union of streams with different watermarks uses the minimum watermark by default
  (`spark.sql.streaming.multipleWatermarkPolicy=min`).
- In plain consumers, commit offsets only after every required sink succeeded.

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Each micro-batch reads Kafka twice | no `persist()` in `foreachBatch` | persist / unpersist |
| Duplicates in one sink after a retry | non-idempotent writes | `txnAppId` + `txnVersion` (Delta), keyed upserts |
| Stream slows down to the slowest store | in-job fan-out to a slow sink | storage-based fan-out |
| `CASE WHEN src = ...` everywhere | union before harmonization | harmonize per source first |
| Timers never fire on idle topics | no micro-batches | heartbeat sidecar source |

## Implementation notes per stack
**Flink**: side outputs emit differently shaped results from the
same window state:

```python
pages_stats = OutputTag("pages_stats", Types.STRING())
# in ProcessWindowFunction.process: yield main_result; yield pages_stats, json.dumps({...})
window_output.get_side_output(pages_stats).sink_to(kafka_sink_aggregates)
```

**Spark** - `examples/sidecar/spark/sidecar_job.py`: harmonization + `unionByName`, heartbeat
source, and the multi-sink writer:

```python
def write_enriched_visits(visits: DataFrame, batch_number: int) -> None:
    visits.persist()
    options = {"txnVersion": str(batch_number), "txnAppId": APP_ID}
    visits.write.format("delta").mode("append").options(**options).save(f"{OUTPUT_DIR}/visits_raw")
    (visits.groupBy("source", "type").count().withColumn("batch_id", F.lit(batch_number))
     .write.format("delta").mode("append").options(**options).save(f"{OUTPUT_DIR}/devices_stats"))
    visits.unpersist()
```

**Kafka** - `examples/sidecar/kafka/sidecar_consumer.py`: one consumer on two topics,
`harmonize()` per topic, fan-out to a required sink (offsets rewound if it fails, Parallel
Split) and an optional sink (failures logged, Exclusive Choice).

**Airflow** - `examples/sidecar/airflow/README.md`: no DAG, the Sidecar is a streaming-job
pattern; it lists the orchestration equivalents (sensors, trigger rules, branching).

## Related patterns
- [Aligned Fan-In](../../data-flow-patterns/references/aligned_fan_in.md) / [Unaligned Fan-In](../../data-flow-patterns/references/unaligned_fan_in.md): the input-side semantics.
- [Parallel Split](../../data-flow-patterns/references/parallel_split.md) / [Exclusive Choice](../../data-flow-patterns/references/exclusive_choice.md): the output-side semantics.
- [Change Data Capture](../../data-ingestion-patterns/references/change_data_capture.md): consistency alternative to multi-store writes.
- [Transactional Writer](../../idempotency-patterns/references/transactional_writer.md): what you would use if all sinks shared one transactional store.
- [Hybrid Source](hybrid_source.md): also combines sources, but sequentially in time instead of concurrently.
