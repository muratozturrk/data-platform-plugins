# Incremental Loader

Category: data loading. One-liner: each run ingests only the **new part** of a physically or logically divided dataset.

## Problem
Most visit events arrive through a streaming broker, but legacy producers still write some of them into a
transactional database. These visits must reach the Bronze layer, and because the volume keeps growing, the
process should only integrate the visits **added since the last execution**. Each visit is immutable
. Reloading everything (Full Loader) would cost more every day.

## Solution / Mechanics
Two implementations, chosen by the input data structure:

1. **Delta column.** A column identifies rows added since the last run; for immutable event data it is
   usually the *ingestion time*. The loader remembers the last ingestion time it processed and asks for rows
   after it.
2. **Time-partitioned dataset.** The ingestion job picks the whole partition for its time slot. No state: the
   partition is resolved from the execution date (run at 11:00 → partition 10:00). Combine with a Readiness
   Marker to know the partition is complete.

```
Delta column                                       Partition-based
 event_id ingestion_time                            input/date=2024-01-01/   (complete)
   1       09:59  (done)                            input/date=2024-01-02/   (being written)
   2       10:01  ─┐                                        │
   3       10:02  ─┴► loader(last=10:00) ─► output          ▼
                     set last = 10:02               sensor: next partition exists?
                                                            │ yes
                                                    loader --ds 2024-01-01 ─► output/date=2024-01-01/
```

Step by step (delta column, bounded):
1. Resolve the window: `[data_interval_start, data_interval_end)` from the orchestrator, or
   `(watermark - look-back, min(watermark - look-back + max_window, now)]` from a state table.
2. Extract `WHERE delta_column >= start AND delta_column < end` (push the predicate to the source).
3. Write idempotently: overwrite the window/partition output, or MERGE on the key if rows can be updated.
4. Persist progress (if you keep a watermark) **after** the write commits.

Step by step (partition-based): wait for readiness of partition `ds` (marker or next partition) → read
`input/date=ds` → overwrite `output/date=ds`.

## Consequences & trade-offs
- **Hard deletes**. A deleted row disappears from the source and the delta column with it, so the
  loader can't see it. Mitigations: *soft deletes* (the producer `UPDATE`s a flag instead of `DELETE`), or
  *insert-only/append-only tables* where consumers reconstruct the state. Otherwise use CDC.
- **Mutable rows.** Updates are visible through the delta column (use `updated_at`, not `created_at`), but the
  writer must MERGE instead of append.
- **Backfilling**. With an unbounded delta column, a backfill after two months of processing turns the
  incremental job into a full load needing far more resources. Limit the ingestion window, e.g.
  `delta_column BETWEEN ingestion_time AND ingestion_time + INTERVAL '1' HOUR`. Gains: controlled data volume,
  and several backfill windows can run concurrently if the source supports it. The partition-based version
  does not have this problem when the job processes one partition at a time.
- **Late data**. An *event-time* delta column misses events that arrive after their time was
  processed. Prefer ingestion time; otherwise add a look-back buffer and idempotent writes (see Late Data).

## Key design decisions
| Decision | Options | Guidance |
|---|---|---|
| Implementation | delta column · time partitions | Partitions when the source is already time-partitioned (no state, simplest backfill) |
| Delta column | ingestion time · `updated_at` · auto-increment id | Ingestion time for immutable events; `updated_at` (+ id tie-breaker) for mutable rows |
| Window source | orchestrator data interval · persisted watermark | Data interval = reproducible; watermark = catches up after outages automatically |
| Window bound | unbounded `> watermark` · bounded `[start, end)` | Always bounded; cap with `max_window_hours`, never with a row `LIMIT` |
| Boundary semantics | `BETWEEN` · half-open `[start, end)` | Half-open avoids loading a boundary row twice across adjacent windows |
| Write | append · overwrite window/partition · MERGE | Overwrite the unit for immutable data; MERGE when rows can change |
| Watermark storage | Airflow Variable · state table · checkpoint | External, transactional; updated only after the data commit |

## When to use / when not
| Situation | Use Incremental Loader? | Alternative |
|---|---|---|
| Large, growing, append-mostly dataset with a reliable delta column | Yes | - |
| Time-partitioned input (date=/hour= folders, partitioned tables) | Yes (partition-based) | - |
| Source hard-deletes rows and deletes matter | No | CDC, or ask for soft deletes |
| Latency requirement in seconds | No (scheduling + query overhead) | Change Data Capture |
| No delta column and no partitions | No | Full Loader |

## Common mistakes / troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| Rows silently missing | Watermark saved before the write, or `LIMIT n` + `max(updated_at)` as new watermark | Save after commit; bound by time window, not row count |
| Rows missing around run boundaries | Event-time delta column + late events; `>=`/`>` mismatch | Ingestion-time column, look-back buffer, half-open windows + idempotent writes |
| Duplicates after retry | Append without dedup on a re-processed window | Overwrite the window/partition or MERGE on the key |
| Backfill job runs out of memory | Unbounded window from an old watermark | `max_window_hours` / per-interval runs (`catchup=True`, `max_active_runs`) |
| Loader never advances | Look-back ≥ window size, or empty capped window not advancing the watermark | Window > look-back; move the watermark to the window end on empty windows |
| Partition loaded half-written | Job started when the folder appeared, not when it was complete | Sensor on the next partition or on a `_SUCCESS`/marker file |
| Output layout flattened | `aws s3 sync` prefix omitted on the destination side | Keep `date=...` on both sides |
| Wrong hour loaded | Naive timestamps interpreted in the cluster's local timezone | `spark.sql.session.timeZone=UTC`, pass UTC bounds |

## Implementation notes per stack

### Spark
`examples/incremental_loader/spark/incremental_loader_jobs.py` - both implementations.
Delta column (half-open bounds):
```python
in_data = (spark.read.text(args.input)
           .select("value", F.from_json(F.col("value"), "ingestion_time TIMESTAMP").alias("j")))
input_to_write = in_data.where(
    (F.col("j.ingestion_time") >= F.to_timestamp(F.lit(args.date_from))) &
    (F.col("j.ingestion_time") < F.to_timestamp(F.lit(args.date_to)))).select("value")
input_to_write.write.mode("overwrite").text(target)      # target = output/window_start=...
```
Partition mode copies `input/date=<ds>` to `output/date=<ds>` with `mode("overwrite")`.

`examples/incremental_loader/spark/jdbc_watermark_incremental_load.py` - remembered watermark for JDBC
sources: `compute_window()` (look-back + `max_window_hours`), `MIN/MAX(id)` bounds inside the window for
parallel `partitionColumn` reads, dedup, Delta MERGE, `WatermarkStore.save()` last.

### Airflow
`examples/incremental_loader/airflow/incremental_loader_dags.py`:
```python
next_partition_sensor = FileSensor(
    task_id="input_partition_sensor",
    filepath=INPUT_BASE + "/date={{ data_interval_end | ds }}",
    mode="reschedule")
load_job = SparkSubmitOperator(..., application_args=[..., "--ds", "{{ ds }}"])
next_partition_sensor >> load_job
```
The delta-column DAG has no sensor and passes `data_interval_start/end` as `--date-from/--date-to`.
Immutable macros make `airflow dags backfill` reproducible. Cloud sensors with the same role:
`AwsGlueCatalogPartitionSensor`, `BigQueryTablePartitionExistenceSensor`, `DatabricksPartitionSensor`.

### Kafka
`examples/incremental_loader/kafka/`: Kafka Connect JDBC source with `"mode": "timestamp+incrementing"`
(delta column + id tie-breaker), `timestamp.delay.interval.ms` as the look-back for late commits; the
connector stores its offsets itself. `incremental_consumer.py` reads the resulting topic.

## Related patterns
- **Full Loader** - fallback when there is no delta column or partitioning.
- **Change Data Capture** - lower latency and hard-delete support when the Incremental Loader is not enough.
- **Readiness Marker** - guarantees a partition is complete before the partition-based loader reads it.
- **Late Data patterns** - handling events that arrive after their window was loaded.
- **Idempotency patterns** - overwrite/MERGE writers that make reruns and backfills safe.
