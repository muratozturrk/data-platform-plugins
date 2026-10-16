# Data Overwrite

Family: **Overwriting**. Make reruns idempotent by physically replacing the dataset, or the partition a run owns, through the *data* layer. It is the fallback when the metadata operations of the Fast Metadata Cleaner (TRUNCATE/DROP) are not available.

## Problem
A daily batch job writes visits into event-time-partitioned locations of an object store. It has no idempotency strategy, so every backfill appends a second copy of the day. The Fast Metadata Cleaner is ruled out because an object store has no metadata layer with TRUNCATE or DROP.

## Solution / Mechanics
Use the native *replace* command of your stack, so each run deletes what any earlier attempt for the same scope wrote:
- **Data processing framework**: a writer option.
  - Spark save mode `overwrite`; Flink write mode properties.
  - Selective overwrite where the format supports conditions: Delta Lake `replaceWhere` replaces only the rows that match a predicate.
  - Spark `partitionOverwriteMode=dynamic` replaces only the partitions present in the output.
- **SQL**:
  - `DELETE FROM ... WHERE` + `INSERT INTO`. Well known, and the predicate can select any rows.
  - `INSERT OVERWRITE`. Shorter, but it replaces the whole table or partition. It cannot select rows.
  - A load command with replace semantics: BigQuery `bq load --replace=true` (= `WRITE_TRUNCATE`). Other stores: `TRUNCATE TABLE` first, then load.
- **`INSERT ... SELECT` needs no VALUES list**: `INSERT INTO visits (id, v_time) SELECT visit_id, visit_time FROM visits_raw` copies a whole table.
- **Overwrite does not erase bytes.** Stores with time travel (table formats, BigQuery, Snowflake) keep the replaced blocks until the retention period ends or VACUUM runs.

```
 run for D (attempt 1, fails half-way)        run for D (attempt 2 / backfill)
 ----------------------------------------     -----------------------------------
 read input WHERE part = D                    read input WHERE part = D
 transform                                    transform
 write mode=overwrite, replaceWhere part=D    write mode=overwrite, replaceWhere part=D
   |  table format: new files + 1 commit        |  commit N+1: remove ALL files of D,
   |  (never committed -> invisible)            |  add the new files of D
   v                                            v
 readers: still version N                     readers: version N+1, D exactly once
                    other partitions (D-1, D+1 ...) are never touched
```

Overwrite modes:

| Mode | Scope | Typical use |
|---|---|---|
| `replaceWhere` / `DELETE ... WHERE` | Explicit predicate | The partition of one scheduled run; corrections for known dates |
| Dynamic partition overwrite | Partitions present in the output | Backfills whose scope comes from the data |
| `mode=overwrite`, `INSERT OVERWRITE`, `--replace` | Whole table or one partition | Full reloads (Full Loader), dimension refreshes |

## Consequences & trade-offs
- **Data overhead**. The pattern is a data operation. On a big, unpartitioned dataset the overwrite gets slower every day as the volume grows. Partitioning (a storage pattern) cuts what each run has to rewrite.
- **Vacuum need**. `DELETE` in relational databases and table formats only hides the rows. The dead blocks stay on disk until VACUUM. Schedule it, and remember that VACUUM also ends time travel to the versions it cleans.
- **Atomicity depends on the target**. A Spark save mode on raw files is not transactional: for a while the old files are gone and the new ones are partial. Table formats commit the delete as a new log entry, so the data files stay untouched until the commit.
- **Wider support than the Fast Metadata Cleaner**, but more I/O. Compared to MERGE: overwrite wins when most rows of a partition change, MERGE when few do.

## Key design decisions

| Decision | Guidance |
|---|---|
| Overwrite scope | The **same column** the run reads its input by. Read `_ingestion_date = D` and overwrite `_ingestion_date = D`, never `DATE(updated_at) = D`. |
| Granularity | One partition per run: a retry or backfill of D touches only D, and runs for different dates can run in parallel. |
| Engine | Table format (`replaceWhere`, atomic) > SQL transaction (`DELETE` + `INSERT`) > raw files (`mode=overwrite`, not atomic). |
| `INSERT OVERWRITE` vs `DELETE` + `INSERT` | `INSERT OVERWRITE` for whole partitions, `DELETE` + `INSERT` in one transaction when a predicate is needed or the engine lacks `INSERT OVERWRITE` (PostgreSQL). |
| Schema | `overwriteSchema=false`: an accidental type change fails instead of rewriting the table schema. |
| Audit | Record before/after counts and the table version, so you can `RESTORE` or time travel back. |

## When to use / when not

| Situation | Use Data Overwrite? | Alternative |
|---|---|---|
| Object store or lakehouse, full partition recomputed per run | Yes | - |
| Warehouse/RDBMS with cheap TRUNCATE/DROP, full unit reloaded | Possible, but slower | Fast Metadata Cleaner |
| Only increments (CDC, changed rows) arrive | No: you would drop unchanged rows | Merger |
| Increments + consistent backfills | No | Stateful Merger |
| Every version must be kept (legal) | No: overwrite destroys the old version once vacuumed | Proxy |
| Streaming sink | Rarely | Keyed Idempotency, Transactional Writer |

## Gotchas & best practices
- **Empty output wipes data.** `replaceWhere` or a dynamic overwrite with zero rows commits an empty partition. Validate the input before writing.
- A bare `mode("overwrite")` on a partitioned table replaces **all** partitions.
- Delta rejects rows that do not match the `replaceWhere` predicate. Treat that error as a scope bug, not as something to disable.
- Do not combine `replaceWhere` and `partitionOverwriteMode=dynamic` in the same write.
- Retries are safe and should stay enabled. Turning them off hides the main benefit of the pattern.
- Check that the version you plan to roll back to is still inside the log and VACUUM retention.
- Kafka is append-only: "overwrite" means re-publishing corrected records with the same key to a compacted topic. Old and new versions coexist until compaction runs.

## Implementation notes per stack

### Spark
[`spark/spark_data_overwrite.py`](../examples/data_overwrite/spark/spark_data_overwrite.py): one commit per date, scoped by the input partition column:
```python
(df.write.format("delta")
   .mode("overwrite")
   .option("replaceWhere", f"{partition_col} = '{partition_date}'")
   .option("overwriteSchema", "false")   # fail if schema changed
   .partitionBy(partition_col)
   .save(silver_path))
```
[`spark/spark_dynamic_partition_overwrite.py`](../examples/data_overwrite/spark/spark_dynamic_partition_overwrite.py): the data decides the scope, so the partition column is derived from the input partition and an empty result aborts:
```python
.config("spark.sql.sources.partitionOverwriteMode", "dynamic")
...
if total_rows == 0:
    raise ValueError("... writing zero rows would wipe affected partitions.")
output_df.write.format("delta").mode("overwrite").partitionBy("_overwrite_date").save(silver_path)
```

### Kafka
[`kafka/kafka_topic_overwrite_replay.py`](../examples/data_overwrite/kafka/kafka_topic_overwrite_replay.py) adapts the pattern to a log. It checks `cleanup.policy=compact`, records the log-end offset, re-publishes corrected records under their original keys, then moves the stopped downstream group to that offset, so consumers read only the corrected versions:
```python
corrections_start = get_log_end_offset(source_topic, 0)
count = republish_corrected_records(source_topic=source_topic, target_topic=source_topic, ...)
if not dry_run and count > 0:
    reset_consumer_group_offset(consumer_group, source_topic, 0, corrections_start)
```

### Airflow
[`airflow/data_overwrite_dag.py`](../examples/data_overwrite/airflow/data_overwrite_dag.py): a daily, retry-enabled DAG. Each run owns `{{ ds }}`:
```python
SparkSubmitOperator(task_id="overwrite_lake_partition", application=SPARK_APP,
                    application_args=["--dates", "{{ ds }}", "--reason", "daily_run"], ...)
SQLExecuteQueryOperator(task_id="overwrite_pg_partition", conn_id=PG_CONN_ID,
    sql=["DELETE FROM dp.visits WHERE visit_date = '{{ ds }}'",
         "INSERT INTO dp.visits (...) SELECT ... FROM dp.visits_raw WHERE visit_date = '{{ ds }}'"],
    autocommit=False)   # one transaction
```
The DAG docstring has the BigQuery `WRITE_TRUNCATE` load on a `$YYYYMMDD` partition decorator, plus the `INSERT OVERWRITE` form for Databricks, Spark SQL and Snowflake.

## Common mistakes / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Backfill of D deletes rows of another day | Overwrite predicate on a different column than the input filter | Partition and `replaceWhere` on the input partition column |
| `Data written out does not match replaceWhere` | Rows outside the predicate in the DataFrame | Same as above, or filter the output to the predicate |
| Whole table emptied | `mode=overwrite` without `replaceWhere` / dynamic mode | Always scope the overwrite |
| Partition empty after a run | Upstream delivered nothing, overwrite committed zero rows | Empty-input guard, post-write count check |
| Storage keeps growing despite overwrites | Old versions kept for time travel | VACUUM with a retention you can live with |
| Readers saw half-written files | Raw Parquet/JSON target, non-transactional save mode | Write to a table format |
| PostgreSQL run left the day half-deleted | `DELETE` and `INSERT` committed separately | Run both in one transaction (`autocommit=False`) |

## Related patterns
- **Fast Metadata Cleaner**: same goal through TRUNCATE/DROP. Faster where metadata operations exist.
- **Full Loader**: a full-table overwrite on every run is Data Overwrite at table granularity.
- **Merger**: needed when the input holds only changes, so replacing would lose rows.
- **Transactional Writer**: explains why a `replaceWhere` commit is atomic; the overwrite adds cross-run idempotency.
- **Proxy**: when you must keep, not replace, every version.
