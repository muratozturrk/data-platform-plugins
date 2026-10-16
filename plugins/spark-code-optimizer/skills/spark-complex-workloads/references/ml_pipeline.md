# ml_pipeline: feature fan-out + cross-validated training

## Problem

A feature-engineering DataFrame (joins, UDF-free transforms, null filling) feeds
`randomSplit`, then a `CrossValidator` that fits `numFolds x len(paramGrid)` models.
Without persistence every fit re-reads the source and re-runs the joins. Worse, an
unpersisted parent of `randomSplit` is re-evaluated per split, so train and test can
overlap or lose rows when the parent is not deterministic (re-read files that changed,
shuffle output order).

## How Spark executes it

- `randomSplit(weights, seed)` = N `sample`-style filters over the same parent. Each
  branch re-evaluates the parent unless it is cached.
- PySpark `CrossValidator._fit` adds a random fold column, then for every fold builds
  `train`/`validation` with `filter`, calls `.cache()` on both, fits all param maps,
  and unpersists. Its input (your `train_df`) is the parent of every fold.
- Tree ensembles with `cacheNodeIds=True` and ALS keep an RDD that grows each iteration;
  `checkpointInterval` truncates it, but only when `sparkContext.setCheckpointDir()` is set
  (otherwise silently ignored, lineage grows, eventual StackOverflowError).

Plan signal: in the Spark UI SQL tab, each CV fit shows a fresh `FileScan` + `Exchange`
for the feature joins. After the fix, fits start from `InMemoryTableScan`.
`features_df.explain()` after persist shows `InMemoryRelation` once materialized.

## sparkopt rule

| Item | Value |
|---|---|
| `name` | `ml_pipeline` |
| `priority` | `Priority.HIGH` (90) |
| `matches()` | `lineage.has_iterative_loop or lineage.estimated_reuse_count >= 3` |
| Params | `has_cv=False`, `num_folds=5`, `feature_cols=["feature_1","feature_2"]` |
| Registration | Not in `ALL_BUILTIN_CASES`; added by DSL `.ml_pipeline(has_cv, num_folds)` (also forces iterative) |

The `>= 3` reuse threshold is a sparkopt heuristic, not a Spark rule.

Generated code (summarized): persist `features_df` at `memory.recommended_storage_level`,
`count()`, `randomSplit`, persist `train_df` at the same level and `test_df` at
`MEMORY_ONLY`, a `VectorAssembler -> StandardScaler -> RandomForestClassifier`
pipeline, optional `CrossValidator(parallelism=4)`, evaluate AUC, unpersist all three.

Bugs in the generated code (fixed in `examples/ml_pipeline.py`):

1. Original sparkopt emitted `MEMORY_ONLY_SER` / `MEMORY_AND_DISK_SER` (AttributeError in
   PySpark); the bundled copy emits `MEMORY_ONLY` / `MEMORY_AND_DISK`. DataFrame cache is already
   columnar + compressed.
2. `F.log1p` is used but `pyspark.sql.functions` is never imported; `lookup_df` is undefined.
3. "localCheckpoint every 5 training iterations" is not something you can do: `fit()` runs
   inside the estimator. The real knob is the estimator's `checkpointInterval` with a
   reliable checkpoint dir.
4. Persisting `test_df` is wasted memory when it is scored once.

## Before / After

```python
# Before: nothing persisted, every CV fit re-runs the join
features_df = raw_df.join(lookup_df, "customer_id", "left").withColumn("log_amount", F.log1p("amount"))
train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)
model = cv.fit(train_df)
```

```python
# After
features_df.persist(StorageLevel.MEMORY_AND_DISK); features_df.count()
train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)
train_df.persist(StorageLevel.MEMORY_AND_DISK); train_df.count()
spark.sparkContext.setCheckpointDir("s3a://bucket/_ckpt/ml/")      # for checkpointInterval
clf = RandomForestClassifier(cacheNodeIds=True, checkpointInterval=10, ...)
model = cv.fit(train_df)
auc = evaluator.evaluate(model.transform(test_df))                # test_df: no persist
train_df.unpersist(); features_df.unpersist()                      # after the last action
```

Full runnable version: `examples/ml_pipeline.py`.

## Configs

| Config / param | Why |
|---|---|
| `spark.serializer=org.apache.spark.serializer.KryoSerializer` | Builder or `--conf` only, not `spark.conf.set`. Helps RDD-based ML internals (trees, ALS), not DataFrame cache size |
| `CrossValidator(parallelism=k)` | k concurrent fits; each holds its own fold caches, so memory need grows with k |
| `checkpointInterval` (trees, ALS, LDA) | Needs reliable `setCheckpointDir` (HDFS/S3/ABFS) |
| `spark.sql.adaptive.enabled` | On by default since 3.2; coalesces small shuffle partitions in the feature joins |

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `AttributeError: MEMORY_ONLY_SER` | Scala-only level | `MEMORY_ONLY` / `MEMORY_AND_DISK` |
| Train + test counts do not add up to total | `randomSplit` parent recomputed non-deterministically | Persist + `count()` the parent first |
| StackOverflowError deep in a GBT/ALS fit | No checkpoint dir, so `checkpointInterval` ignored | `setCheckpointDir(<durable path>)` |
| Executors OOM during CV | `parallelism` x fold caches + your caches | Lower `parallelism`, drop the `test_df` persist, `MEMORY_AND_DISK` |
| CV slow, Storage tab shows `features_df` at 30% cached | Partial cache (`MEMORY_ONLY` evicted) | `MEMORY_AND_DISK`, or select only model columns before persisting |
| Scaler fitted on full data before CV | Leakage across folds | Keep scaler inside the `Pipeline` given to `CrossValidator` |

Persist only the columns the model reads (`select` before `persist`); wide raw frames
inflate the cache for nothing.

## Related cases

- `fanout_cache`, `storage_level` (persistence skill): the generic rule behind features_df.
- `iterative_loop`: hand-written training loops (not `fit()`) use localCheckpoint every N.
- `graph_algorithm`: same lineage-growth problem for DataFrame iterations.
- `broadcast_join`: the feature lookup join (`F.broadcast(lookup_df)`).
