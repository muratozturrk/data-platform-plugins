# incremental_etl: upsert of an increment into a large table

## Problem

Each run brings an increment (CDC rows, a daily drop) that must be inserted, updated or
deleted in a large target table. The naive approach full-outer-joins the whole target
with the increment and rewrites the table, every run. Cost scales with the table, not
with the change, and overwriting the path you are reading is unsafe.

## How Spark executes it

- Plain parquet has no transactions: `mode("overwrite")` deletes the target, then writes.
  Reading and overwriting the same path in one job raises
  `AnalysisException: Cannot overwrite a path that is also being read from`; working
  around it with `checkpoint()` lets the job run but a failure mid-write loses the table.
- Delta MERGE: scans the source + target to find touched files (inner join on the merge
  condition, with file pruning from min/max stats and partition predicates), then rewrites
  only those files and commits atomically through `_delta_log`. It reads the source more
  than once, so the source must be deterministic; recent Delta versions can materialize
  the source themselves (`merge.materializeSource`), your persist still helps when you
  also reuse the source (counts, audit, quarantine writes).
- Multiple source rows matching one target row -> MERGE error. Deduplicate first.

Plan signal: Delta history `operationMetrics` (`numTargetFilesAdded`, `numTargetRowsUpdated`,
`numTargetFilesRemoved`): files rewritten close to the table's file count = no pruning.
In the SQL tab the MERGE job's target `FileScan` shows `number of files read`.

## sparkopt rule

| Item | Value |
|---|---|
| `name` | `incremental_etl` |
| `priority` | `Priority.HIGH` (90) |
| `matches()` | `lineage.estimated_reuse_count >= 2 and not lineage.has_iterative_loop` |
| Registration | DSL `.incremental_etl()` |

The trigger is very broad (any reused, non-iterative frame); treat a match as "consider
this", and confirm it really is an upsert workload.

Generated code: persist the full existing table at `MEMORY_AND_DISK`, full-outer-join
with the increment for change detection with `F.hash(*new_df.columns) != F.hash(*existing_df.columns)`,
persist changes at `MEMORY_ONLY`, full-outer-join again for the merge with
coalesce per column and `is_deleted` when the key is missing from the increment,
`setCheckpointDir("hdfs:///...")` + `checkpoint(eager=True)` "for atomic write",
`repartition(200, "partition_col")` and overwrite the same S3 path.

Bugs (fixed in `examples/incremental_etl.py`):

1. Original sparkopt used `*_SER` levels (AttributeError in PySpark); the bundled copy emits
   PySpark levels.
2. Persisting the whole target: a full scan + memory/disk for the entire table every run.
3. `F.hash(*new_df.columns)` after aliasing both sides with the same column names:
   ambiguous references (AnalysisException); hash also collides.
4. "Key missing from the increment = deleted": wrong for increments (they only contain
   changed keys); that logic is for full snapshots. Use an explicit op/delete flag.
5. `checkpoint()` does not make a write atomic. Overwrite of the input path is either
   rejected or, after a checkpoint, unrecoverable on failure.
6. `merged_df` has no `partition_col` column, so the `repartition`/`partitionBy` fails.

## Before / After

```python
# Before: whole table cached, joined twice, rewritten in place
existing_df = spark.read.parquet("s3a://bucket/main_table/").persist(StorageLevel.MEMORY_AND_DISK_SER)
merged_df = existing_df.join(new_df, "primary_key", "full").select(...)
merged_df = merged_df.checkpoint(eager=True)
merged_df.write.mode("overwrite").parquet("s3a://bucket/main_table/")
```

```python
# After: Delta MERGE touches only matching files; cache only the small source
updates = (raw.withColumn("_rn", F.row_number().over(Window.partitionBy(KEY).orderBy(F.desc("changed_at"))))
              .where("_rn = 1").drop("_rn"))
updates.persist(StorageLevel.MEMORY_AND_DISK); n = updates.count()
changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in VALUE_COLS)   # null-safe
(DeltaTable.forPath(spark, target).alias("t")
   .merge(updates.alias("s"), f"t.{KEY} = s.{KEY}")
   .whenMatchedDelete(condition="s.op = 'D'")
   .whenMatchedUpdate(condition=f"s.op != 'D' AND ({changed})", set=set_cols)
   .whenNotMatchedInsert(condition="s.op != 'D'", values=insert_cols)
   .execute())
updates.unpersist()
```

Full runnable version: `examples/incremental_etl.py`.

No Delta/Iceberg/Hudi available? Compute the changed partitions only, write them with
`spark.sql.sources.partitionOverwriteMode=dynamic` to the target, and read the unchanged
partitions from the table, not from a cached copy. Never overwrite the input path.

## Configs

| Config | Why |
|---|---|
| `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension` + Delta catalog | Required for Delta in OSS Spark (session builder / `--conf`) |
| Partition predicate in the ON clause | File pruning; the biggest MERGE speedup |
| `delta.autoOptimize` / `OPTIMIZE ... ZORDER BY (key)` | Keeps key ranges tight so MERGE rewrites fewer files |
| `spark.sql.sources.partitionOverwriteMode=dynamic` | Parquet fallback: overwrite only written partitions |

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `multiple source rows matched` | Duplicate keys in the increment | `row_number()` dedup by key, latest wins |
| Every file rewritten each run | No pruning predicate, keys spread over all files | Partition predicate in ON; Z-order by key |
| Updates that change nothing rewrite files | Unconditional `whenMatchedUpdateAll` | Null-safe change condition |
| `Cannot overwrite a path that is also being read from` | In-place parquet overwrite | Delta MERGE, or write elsewhere + swap |
| Wrong result after retry | Non-deterministic source (`rand`, current time) | Persist the source, avoid non-determinism |
| `ConcurrentAppendException` | Two writers on the same partitions | Partition-disjoint writers or retry |

## Related cases

- `fanout_cache`: why the increment is cached (two consumers).
- `skew_join`: hot keys in a large merge.
- `delta_zorder`, `coalesce_write` (I/O skill): file layout for cheap merges.
