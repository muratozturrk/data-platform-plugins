---
name: spark-persistence-strategy
description: "Choose between cache/persist, localCheckpoint and reliable checkpoint in PySpark, pick a StorageLevel that actually exists in PySpark, and apply sparkopt's PersistenceAdvisor decision tree. Use when a DataFrame is reused by several actions, lineage or plans grow in loops, jobs spill, a streaming job joins static data, or code uses MEMORY_ONLY_SER / MEMORY_AND_DISK_SER (which do not exist in Python)."
---

# Spark Persistence Strategy

Persistence is the part of Spark people get wrong most often. `cache()` is not the only tool, and it is
not a lineage break. This skill covers the seven sparkopt persistence cases and the `PersistenceAdvisor`
that settles conflicts between them. It also fixes the library's StorageLevel bug: sparkopt (and its
manual) suggests `StorageLevel.MEMORY_ONLY_SER` / `MEMORY_AND_DISK_SER`, and **those names raise
`AttributeError` in PySpark**.

## When to Use

- The same DataFrame feeds 2+ actions or branches (reports, writes, ML features).
- A Python `for` loop rebuilds a DataFrame from itself (ML, graph, convergence loops): plans and
  driver planning time grow with every iteration.
- Very deep plans (many chained joins/withColumn), slow `explain()`, slow job start or driver OOM.
- Stages show `Spill (Memory)` / `Spill (Disk)` in the Spark UI.
- A Structured Streaming query joins a static lookup table.
- Code reviews: `persist()` without `unpersist()`, `cache()` used to "break lineage",
  `setCheckpointDir("/tmp/...")` on a cluster, `*_SER` storage levels in Python.

## The three tools

| | `persist()` / `cache()` | `localCheckpoint()` | `checkpoint()` (reliable) |
|---|---|---|---|
| Breaks lineage/plan | No (plan kept, `InMemoryRelation`) | Yes | Yes |
| Where data lives | Executor block manager (level you pick) | Executor block manager, MEMORY_AND_DISK | Files under `setCheckpointDir` (HDFS/S3/ABFS) |
| Survives executor loss | Recomputed from lineage | **No** (job fails, cannot recompute) | Yes |
| Needs `setCheckpointDir` | No | **No** | Yes |
| Lazy by default | Yes (materialize with `count()`) | `eager=True` default | `eager=True` default |
| Release | `unpersist()` | Drop the reference (ContextCleaner) | Delete files / `spark.cleaner.referenceTracking.cleanCheckpoints` |

## PySpark StorageLevel table (correct)

| PySpark level | Memory | Disk | Replicas | Use |
|---|---|---|---|---|
| `MEMORY_AND_DISK_DESER` | yes | yes | 1 | What `df.cache()` / `df.persist()` use for DataFrames (Spark 3.x). Default choice. |
| `MEMORY_AND_DISK` | yes | yes | 1 | Same behaviour for DataFrames; spill to disk instead of recomputing. |
| `MEMORY_ONLY` | yes | no | 1 | Small, hot data; evicted partitions are recomputed. |
| `DISK_ONLY` | no | yes | 1 | Data far bigger than storage memory, recompute is very expensive. |
| `OFF_HEAP` | off-heap | no | 1 | Only with `spark.memory.offHeap.enabled=true` + `spark.memory.offHeap.size`. |
| `MEMORY_ONLY_2`, `MEMORY_AND_DISK_2`, `DISK_ONLY_2`, `DISK_ONLY_3` | | | 2/3 | Replicated: survives one executor loss without recompute, costs 2-3x space. |

Not in PySpark: `MEMORY_ONLY_SER`, `MEMORY_AND_DISK_SER`, `MEMORY_ONLY_SER_2`, `MEMORY_AND_DISK_SER_2`
(Scala/Java only). Map them: `MEMORY_ONLY_SER → MEMORY_ONLY`, `MEMORY_AND_DISK_SER → MEMORY_AND_DISK`,
`MEMORY_ONLY_SER_2 → MEMORY_ONLY_2`, `MEMORY_AND_DISK_SER_2 → MEMORY_AND_DISK_2`.
DataFrame cache is already columnar and compressed (`spark.sql.inMemoryColumnarStorage.compressed=true`),
so "serialized vs deserialized" is not a real choice for DataFrames. Kryo (`spark.serializer`) does not
change DataFrame cache size; it matters for RDD shuffles/caching and must be set in the SparkSession
builder or `spark-submit --conf`, never with `spark.conf.set` at runtime.

## PersistenceAdvisor decision tree (`core/persistence_advisor.py`)

First match wins. Thresholds are **sparkopt heuristics, not Spark rules**.

| # | Condition (from code) | Strategy | Output |
|---|---|---|---|
| 1 | `has_streaming_source and estimated_reuse_count >= 2` | `HYBRID` | cache static side, `checkpointLocation` for the stream |
| 2 | `has_iterative_loop` | `ITERATIVE_LOCAL_CHECKPOINT` | `localCheckpoint(eager=True)` every N iterations |
| 3 | `depth > 20` (`DEPTH_RELIABLE_CKPT`) | `RELIABLE_CHECKPOINT` | `setCheckpointDir(hdfs/s3)` + `checkpoint(eager=True)` |
| 4 | `depth > 10` (`DEPTH_LOCAL_CKPT`) `and reuse >= 2` (`MIN_REUSE`) | `LOCAL_CHECKPOINT` | `localCheckpoint(eager=True)` |
| 5 | `reuse >= 2` | `CACHE_*` | `persist(MemoryProfile.recommended_storage_level)` |
| 6 | otherwise | `NO_PERSIST` | nothing |

`MemoryProfile` (`core/analyzer.py`), defaults `executor_memory_mb=4096`, `executor_count=10`:

```
total_cluster_mb = executor_memory_mb * executor_count
usable_memory_mb = total_cluster_mb * 0.6 * 0.5      # memory.fraction x storageFraction
fits_in_memory   = df_size_mb       < usable_memory_mb * 0.8
fits_serialized  = df_size_mb / 3.0 < usable_memory_mb * 0.8
level = MEMORY_ONLY if fits_in_memory else MEMORY_AND_DISK if fits_serialized else DISK_ONLY
```

(The original library returns the `_SER` names; the vendored copy maps them as above.) Real Spark is
more generous: unified memory = `(heap - 300MB) * spark.memory.fraction`, and storage can borrow
unused execution memory; `storageFraction` is only the part protected from eviction. Treat
`usable_memory_mb` as a conservative floor and check the real cached size in the Spark UI Storage tab.

## Decision table (symptom → case → reference)

| Symptom / plan signal | Case | Reference |
|---|---|---|
| Several signals at once, need one verdict | `persistence_strategy` | [references/persistence_strategy.md](references/persistence_strategy.md) |
| Loop reassigns `df = f(df)`; plan and planning time grow per iteration | `iterative_loop` | [references/iterative_loop.md](references/iterative_loop.md) |
| Same expensive DataFrame scanned once per action (duplicate stages in UI) | `fanout_cache` | [references/fanout_cache.md](references/fanout_cache.md) |
| Plan depth > 20, huge `explain()`, recompute storms after executor loss | `lineage_break` | [references/lineage_break.md](references/lineage_break.md) |
| Picking a level; `AttributeError: MEMORY_ONLY_SER` | `storage_level` | [references/storage_level.md](references/storage_level.md) |
| `readStream` joined with a static table | `hybrid_checkpoint` | [references/hybrid_checkpoint.md](references/hybrid_checkpoint.md) |
| `Spill (Disk)` > 0, data > storage memory, > 5M rows | `spill_prevention` | [references/spill_prevention.md](references/spill_prevention.md) |

## Cases

### persistence_strategy (CRITICAL)
- Trigger: `reuse >= 2 or has_iterative_loop or depth > 10`.
- Generates one of: local checkpoint (iterative and not streaming), reliable checkpoint
  (`depth > 20` and not iterative), local checkpoint (`depth > 10` and reuse >= 2, like the advisor),
  cache with the recommended level (reuse >= 2), or "no persist".
- Gotcha: the case has no HYBRID branch (original sparkopt also lacked the depth > 10 + reuse
  branch; the bundled copy adds it). The depth branch reuses the iterative-loop template, so write
  `df = df.localCheckpoint(eager=True)` once instead. Use `--advice` for the final verdict.
  Example: [examples/persistence_strategy.py](examples/persistence_strategy.py).

### iterative_loop (CRITICAL)
- Trigger: `has_iterative_loop`. `checkpoint_interval=5` (constructor default).
- Generates a loop with `localCheckpoint(eager=True)` every 5 iterations.
- Corrected: no `setCheckpointDir` needed; `prev.unpersist()` on a non-persisted DataFrame is a no-op,
  so drop references instead. Use reliable `checkpoint()` every N if executor loss must not kill the job.
  Example: [examples/iterative_loop.py](examples/iterative_loop.py).

### fanout_cache (HIGH)
- Trigger: `reuse >= 2`. Generates `persist(level)` on the shared base, N consumers (N = the reuse
  count), `unpersist()`.
- Corrected: materialize with `count()` before branching; `unpersist()` only after the **last action**.
  Example: [examples/fanout_cache.py](examples/fanout_cache.py).

### lineage_break (HIGH)
- Trigger: `depth > 20`. Generates `setCheckpointDir(env or hdfs:///...)` + `checkpoint(eager=True)`.
- Corrected: checkpoint dir must be a shared FS on a cluster; clean up only this app's subdirectory.
  Example: [examples/lineage_break.py](examples/lineage_break.py).

### storage_level (HIGH)
- Trigger: `reuse >= 2`. Prints a level table banded at `usable*0.3 / 0.8 / 1.5` and persists with
  `recommended_storage_level` (the bands and the property disagree; the property wins).
- Corrected: PySpark table above; no `_SER`; no runtime `spark.serializer`.
  Example: [examples/storage_level.py](examples/storage_level.py).

### hybrid_checkpoint (HIGH)
- Trigger: `has_streaming_source` (case). Advisor only returns HYBRID if reuse >= 2 too.
- Generates: cached static lookup + `count()`, Kafka `readStream`, broadcast stream-static join,
  `writeStream` with `checkpointLocation`.
- Corrected: streaming DataFrames cannot be `cache()`d or `localCheckpoint()`ed; persist inside
  `foreachBatch` instead. Caching the static side freezes it. Example: [examples/hybrid_checkpoint.py](examples/hybrid_checkpoint.py).

### spill_prevention (HIGH)
- Trigger: `not memory.fits_in_memory and row_count > 5_000_000`.
- Generates AQE on (`spark.conf.set`), `spark.memory.fraction=0.75` / `spark.memory.storageFraction=0.4`
  as `--conf` comments (launch-time configs are never emitted as `spark.conf.set`; original sparkopt
  did, and Spark 3.x rejects that), `repartition(max(200, size_mb/128))`, `persist(MEMORY_AND_DISK)`
  for multi-aggregation.
- Corrected: prefer AQE partition coalescing / `spark.sql.shuffle.partitions` over a blind
  `repartition`.
  Example: [examples/spill_prevention.py](examples/spill_prevention.py).

## Discipline rules

1. **Eager materialization**: `persist()` is lazy. Call `df.count()` right after persisting when
   several branches follow; `show()`/`take()`/`first()` only compute some partitions.
2. **Unpersist after the last action**, not after the last transformation. Transformations are lazy;
   unpersisting before downstream actions run makes them recompute. `spark.catalog.clearCache()` for all.
3. **Checkpoint cleanup**: reliable checkpoint files are not deleted by default. Either set
   `spark.cleaner.referenceTracking.cleanCheckpoints=true` at launch or delete
   `spark.sparkContext.getCheckpointDir()` (this app's UUID subdir) at the end. Never delete a
   streaming `checkpointLocation` of a query you intend to restart.
4. Verify: `df.storageLevel`, `explain()` shows `InMemoryTableScan` (cache) or `Scan ExistingRDD`
   / `LogicalRDD` (checkpoint).

## Run sparkopt

```bash
# All persistence cases plus the advisor verdict
python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --profile profile.json \
  --case persistence_strategy --case iterative_loop --case fanout_cache --case lineage_break \
  --case storage_level --case hybrid_checkpoint --case spill_prevention --advice

# Only the advisor decision, machine-readable
python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --profile profile.json --advice --format json
```

`--case <builtin>` restricts the run to the named built-in cases (a complex case name adds an opt-in
case). With `--plan`, capture the plan with plain `df.explain()` (simple mode); it fills
`lineage.depth`, `has_streaming_source` and (from duplicate FileScans) the reuse count when the profile
leaves them out. The profile fields that drive these cases: `row_count`, `estimated_bytes`,
`lineage.depth`, `lineage.has_iterative_loop`, `lineage.estimated_reuse_count`,
`lineage.has_streaming_source`, `memory.executor_memory_mb`, `memory.executor_count` (schema:
[sparkopt-toolkit/references/profile_json.md](../sparkopt-toolkit/references/profile_json.md)). Each
matched case in the output has a `reviewed_example` path (relative to the plugin root, pointing at
`examples/<case>.py` here) next to the generated `code` sketch; prefer the reviewed example and
re-check any generated snippet against the corrections above before pasting it.

## Anti-patterns

- `cache()` to "break lineage": it does not; use `localCheckpoint()` / `checkpoint()`.
- `StorageLevel.MEMORY_ONLY_SER` in Python: `AttributeError`.
- `spark.conf.set("spark.serializer", ...)` or `spark.conf.set("spark.memory.fraction", ...)` at runtime.
- `setCheckpointDir("/tmp/...")` on a cluster (each executor writes to its own local disk; the driver
  cannot read it back). Calling `setCheckpointDir` just for `localCheckpoint()`.
- Persisting a single-use DataFrame, or persisting everything "just in case" (evicts useful blocks).
- `persist()` followed by `unpersist()` before any action ran.
- `localCheckpoint()` on critical long jobs where losing an executor must not fail the job.
- Caching a static Delta table in a stream-static join that is expected to see table updates.
