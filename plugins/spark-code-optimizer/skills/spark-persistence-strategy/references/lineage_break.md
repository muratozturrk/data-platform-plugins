# lineage_break: reliable checkpoint for very deep plans

## Problem

Long pipelines (many joins, `withColumn` chains, unions in loops, generated SQL) produce plans with
dozens of operators. Effects:

- Planning and `explain()` get slow; the driver holds a huge plan tree.
- On executor loss, Spark recomputes lost partitions through the entire chain, often several
  shuffles deep.
- `cache()` does not shorten the plan; it only adds a cached node.

## How Spark executes it

`df.checkpoint(eager=True)` runs a job that writes each partition as a file under
`<checkpointDir>/<app-uuid>/rdd-<id>/`, then returns a DataFrame over those files. The new plan is
`LogicalRDD` / `Scan ExistingRDD`: depth effectively 0. The data survives executor and node loss
because it lives on HDFS/S3/ABFS. Internally Spark computes the DataFrame once more for the write
unless it was already persisted, so the common pattern for expensive inputs is
`persist()` → `checkpoint()` → `unpersist()`; with `eager=True` and a single use this is not needed.

Signals:

- `df.explain("extended")` output with very many lines; `df.rdd.toDebugString()` showing a deep
  indentation tree.
- Spark UI: stages being re-run after `FetchFailedException` / executor lost, going back far.
- After the checkpoint: `explain()` shows `Scan ExistingRDD`.

## sparkopt rule

`LineageBreakCase`, priority `HIGH`, `DEPTH_THRESHOLD = 20`.

```python
def matches(self, profile):
    return profile.lineage.depth > 20
```

The advisor returns `RELIABLE_CHECKPOINT` for `depth > 20` when there is no streaming+reuse and no
iterative loop; for `10 < depth <= 20` with `reuse >= 2` it returns `LOCAL_CHECKPOINT` instead.
`lineage.depth` is an estimate supplied in the profile (or by `LineageInspector`); 20 is a sparkopt
heuristic, not a Spark limit.

Generated code: `CKPT_DIR = os.environ.get("SPARK_CHECKPOINT_DIR", "hdfs:///checkpoints/job/")`,
`setCheckpointDir(CKPT_DIR)`, `checkpoint(eager=True)` after the expensive joins.

Corrections:

- The checkpoint dir must be on a filesystem all executors and the driver share (HDFS, S3,
  ABFS, GCS). A local path works only in `local[*]` mode.
- Cleanup: the library's snippet calls `FileSystem.get(hadoopConf)` (the default FS; wrong for
  `s3a://` paths) and deletes the whole root. Delete only this application's subdirectory returned
  by `spark.sparkContext.getCheckpointDir()`, via `Path.getFileSystem(conf)`; or set
  `spark.cleaner.referenceTracking.cleanCheckpoints=true` at launch.

## Before / After

Before:

```python
df = base
for t in transforms:          # 40 chained steps
    df = t(df)
result = df.groupBy("category").agg(F.sum("value"))
result.write.parquet(out)
```

After:

```python
import os
spark.sparkContext.setCheckpointDir(os.environ.get("SPARK_CHECKPOINT_DIR", "hdfs:///checkpoints/job"))

df = base
for t in transforms[:20]:
    df = t(df)
df = df.checkpoint(eager=True)          # plan truncated here, files on HDFS
for t in transforms[20:]:
    df = t(df)
df.groupBy("category").agg(F.sum("value")).write.mode("overwrite").parquet(out)
# end of job: delete spark.sparkContext.getCheckpointDir() (see example)
```

Put the checkpoint right after the most expensive shuffle-heavy part, not at an arbitrary depth.
Alternative with explicit, readable output: write to Parquet/Delta and read it back. That also
truncates lineage, is inspectable, and is easy to clean up.

See `examples/lineage_break.py`.

## Configs

| Config | Default | Note |
|---|---|---|
| `SparkContext.setCheckpointDir(path)` | unset | required for `checkpoint()` |
| `spark.cleaner.referenceTracking.cleanCheckpoints` | `false` | launch-time; delete files of unreferenced checkpoints |
| `spark.checkpoint.compress` | `false` | compress checkpoint files (launch-time) |

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Checkpoint directory has not been set in the SparkContext` | no `setCheckpointDir` | set it before `checkpoint()` |
| Warning "checkpoint directory must not be on the local filesystem" | local path on a cluster | use `hdfs://`, `s3a://`, `abfss://` |
| Checkpoint storage grows forever | files never deleted | delete app subdir at end or `cleanCheckpoints=true` |
| Checkpoint takes as long as the pipeline | DataFrame computed again for the write | `persist()` before, `unpersist()` after |
| Still slow planning afterwards | checkpoint placed too early; long chain after it | add a second checkpoint, or move it later |
| Can't checkpoint streaming DataFrame | not supported | `writeStream` with `checkpointLocation` |

## Related cases

- `iterative_loop`: loops use localCheckpoint every N.
- `persistence_strategy`: advisor ordering (depth > 20 vs depth > 10 + reuse).
- `hybrid_checkpoint`: streaming `checkpointLocation` (a different mechanism, never delete it).
