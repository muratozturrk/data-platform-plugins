# Dynamic Late Data Integrator

> Category: data error, time. Batch / orchestrated pipelines with partitioned storage.

## Problem

The 15-day Static Late Data Integrator is no longer enough. The product owner wants the
statistics to include **all** late data, even beyond 15 days. The pipeline must stop blindly
replaying the last two weeks and integrate exactly the partitions that received late data
.

## Solution / Mechanics

The lookback window becomes **dynamic**: every backfilled partition really contains late data.
This needs a **state table** with the last processing time and the last update time of each
partition:

| Partition | Last processed time | Last update time |
| --- | --- | --- |
| 2024-12-17 | 2024-12-17T10:20 | 2024-12-17T03:00 |
| 2024-12-18 | 2024-12-18T09:55 | 2024-12-20T10:12  (late data) |

Partitions to backfill:

```sql
SELECT partition FROM state_table
WHERE `Last update time` > `Last processed time` AND `Partition` < `Processed partition`
```

Pipeline integration: after processing a partition, update its *last
processed time*, then query the partitions to backfill and run them.

Where the *last update time* comes from:
- natively: BigQuery `INFORMATION_SCHEMA.PARTITIONS.last_modified_timestamp`, the Iceberg
  `partitions` metadata table (`last_updated_at`);
- Delta Lake: derive it from the transaction log. In Scala use `DeltaLog.getChanges`, and reason in **table versions** instead of timestamps;
- other stores: generate it yourself, e.g. from the ingestion stream.

Not whole partitions: if you can isolate the impacted entities, overwrite
only their rows. This saves resources but is harder to build.

### Concurrent pipelines

With concurrent runs, two runs can plan the same late partitions. Fixes:
1. add an **`is_processed`** column and filter on `Is processed = false`;
2. **first task** marks the current partition as being processed, so the next run does not plan
   it. It runs only if the same task of the previous run succeeded;
3. the **planner** marks every returned partition as processed, and also depends on its previous
   execution, so there is only one planner at a time;
4. the task that sets *last processed* also resets `is_processed = false`, so new late data can
   be replayed later.

Then the backfills can run as separate, complete pipelines.

```
 run D (concurrent with D+1, D+2, ...)
  [mark D processing]* ─► [process D] ─► [mark D processed, is_processed=false]
                                              │
                                              ▼
              [plan: last_update > last_processed AND partition < D AND NOT is_processed
                     -> set is_processed=true -> plan file]*
                                              │
                                              ▼
              generate_backfilling_arguments ─► TriggerDagRun(P1), TriggerDagRun(P2) ...
                                                   (backfill pipeline: process P, mark processed)
   * depends_on_past=True: sequential across runs, everything else runs in parallel
```

## Consequences & trade-offs

- **Concurrency**: without `is_processed` plus sequential marker and planner tasks,
  partitions are backfilled several times in parallel. With them, **a failed planner blocks all
  following planners**. The pipeline sits in progress until someone intervenes.
- **Stateful pipelines and very late data**: an Incremental Sessionizer that last ran on
  2024-10-20 and gets late data for 2024-09-21 must regenerate every run from 09-21 to 10-20.
  Even a dynamic window may need a **maximum accepted lookback**.
- **Scheduling complexity**: backfill pipelines are created dynamically, and getting
  per-partition modification info may mean digging into storage internals or building your own
  tracking table.
- The gain over the static pattern: no wasted reprocessing and no fixed cut-off.

## Key design decisions

- **Change signal**: timestamps (BigQuery/Iceberg/custom) or versions (Delta). Versions avoid
  clock skew and are simpler.
- **Who updates `last_update`**: the planner from storage metadata, or a tracker fed by
  ingestion. Keep it a running MAX so lost metadata (Delta log cleanup) never moves it back.
- **What "processed" means**: record the version or time the processing job **read**, not the
  time it finished. Otherwise late data that lands during processing is skipped.
- **Execution mode**: integration tasks inside the main DAG, or triggered backfill DAG runs
  (more parallelism, needs the concurrency controls).
- **Max lookback**: cap very late partitions for stateful consumers, and alert on what is skipped.
- **Granularity**: whole partitions (simple) or impacted entities only (cheaper, harder).

## When to use / when not

| Situation | Use? | Alternative |
| --- | --- | --- |
| Late data with unbounded delay must be included | Yes | none |
| Late data is rare; a static lookback wastes compute | Yes | Static + change-check control task |
| Storage exposes per-partition modification info | Yes, cheap | none |
| No partitioning by time | No | Full reload |
| Strongly stateful job with very late data | Yes, with max lookback | Accept approximations / periodic full rebuild |
| Simple, bounded delay, single sequential run | Overkill | Static Late Data Integrator |

## Gotchas & troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Same partition backfilled twice | No `is_processed` flag, or planner not sequential | `NOT is_processed` planner filter + `depends_on_past=True` on the marker and planner |
| Pipeline stuck "running" for days | A planner failed; `depends_on_past` blocks later runs | Fix, then clear the failed task; alert on stuck planners |
| Late data never integrated | `mark-processed` did not reset `is_processed=false` | Reset the flag in the same update as last processed |
| Late data written during processing skipped | `last_processed` set to "now" instead of the version read | Pass the read version (`versionAsOf`) to mark-processed |
| `DagRunAlreadyExists` on trigger | Backfill DAG already has a run for that date | `reset_dag_run=True` (re-run) or unique logical dates |
| Old changes missing from the Delta log | `_delta_log` JSON cleaned after `logRetentionDuration` | State table keeps `last_update_version` as a running MAX; run the planner frequently |
| Huge backfill after very late data | Stateful dependency chain | `--max-lookback-days`, log skipped partitions |

## Implementation notes per stack

**Spark (Delta)**: `examples/dynamic_late_data_integrator/spark/spark_dynamic_late_integrator.py`.
The PySpark port of `DeltaLog.getChanges` reads the commit JSON files directly:

```python
log_df = spark.read.json(f"{table_path}/_delta_log/*.json")
version = F.regexp_extract(F.input_file_name(), r"(\d{20})\.json$", 1).cast("long")
changes = log_df.filter(F.col("add.dataChange")).select(
    F.col("add.partitionValues.event_date").alias("partition"), version.alias("version"))
# + the same for "remove"; groupBy(partition).max(version)
```
`select_partitions_to_backfill()` is the pure version of the planner query (version >
`last_processed_version`, not `is_processed`, partition < current, optional max lookback).
Subcommands: `mark-processing`, `process` (records `versionAsOf` read), `mark-processed`, `plan`
(writes a per-run JSON plan file).

**Airflow**: `examples/dynamic_late_data_integrator/airflow/dynamic_late_integrator_dag.py`
:

```python
with DAG("visits_dynamic_late_data_integrator", max_active_runs=5,
         default_args={"depends_on_past": False, ...}):
    processing_marker = spark_step("mark_partition_as_being_processed", ..., depends_on_past=True)
    backfill_creation_job = spark_step("get_late_partitions_and_mark_them_as_being_processed",
                                       "plan", ..., depends_on_past=True)
    TriggerDagRunOperator.partial(trigger_dag_id=BACKFILL_DAG_ID, reset_dag_run=True) \
        .expand_kwargs(generate_backfilling_arguments())
```
The backfill DAG (`schedule=None`) runs `process` + `mark-processed` for the triggered date.

**Kafka + PostgreSQL**: `examples/dynamic_late_data_integrator/kafka/kafka_partition_update_tracker.py`.
For stores without partition metadata, it builds `last_update_time` from the ingestion stream
(`GREATEST` upsert per poll). Its `plan` mode replaces `depends_on_past` with row locks:
`UPDATE ... WHERE partition IN (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING partition`.

## Related patterns

- **Static Late Data Integrator**: the fixed-window predecessor; simpler when the delay is
  bounded.
- **Late Data Detector**: produces the late records whose arrival updates `last_update`.
- **Incremental Sessionizer**: an example of a stateful consumer with costly
  very-late backfills.
- **Readiness Marker / External Trigger**: how triggered backfill pipelines start.
- **Data Overwrite / Merger**: idempotent rewrite of each backfilled partition.
