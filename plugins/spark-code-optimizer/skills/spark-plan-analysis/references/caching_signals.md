# Caching signals: InMemoryRelation, reuse, streaming

This reference covers the plan signals that feed sparkopt's **persistence** decisions: is the data
already cached, is the same data consumed more than once, and is this a streaming query.

## InMemoryRelation / InMemoryTableScan

After `df.cache()` / `df.persist(level)` the plan of `df`, and of every DataFrame derived from it, reads
from the cache:

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Filter (isnotnull(k#2L) AND (k#2L = 1))
   +- InMemoryTableScan [id#0L, k#2L, v#3], [isnotnull(k#2L), (k#2L = 1)]
         +- InMemoryRelation [id#0L, k#2L, v#3], StorageLevel(disk, memory, deserialized, 1 replicas)
               +- *(1) Project [id#0L, (id#0L % 10) AS k#2L, (cast(id#0L as double) / 7.0) AS v#3]
                  +- *(1) Range (0, 1000, step=1, splits=2)
```

How to read it:

- `InMemoryTableScan [cols], [filters]` is the operator that reads cached batches. The filters are
  evaluated with per-batch min/max stats, which is weaker than Parquet pushdown.
- `InMemoryRelation ... StorageLevel(disk, memory, deserialized, 1 replicas)` is
  `MEMORY_AND_DISK_DESER`, which is what `df.cache()` uses for DataFrames in Spark 3.x. `MEMORY_ONLY`
  prints `StorageLevel(memory, deserialized, 1 replicas)`, and `DISK_ONLY` prints
  `StorageLevel(disk, 1 replicas)`.
- The subtree **below** `InMemoryRelation` is the plan that builds the cache. It runs once, on the first
  action, and later actions skip it **if** the cache is still materialized (Storage tab).
- The relation appears as soon as `cache()` is called, not when it is materialized. `cache()` is lazy,
  and the plan text cannot tell whether the blocks exist or were evicted.

Storage-level reminder for PySpark: only `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_AND_DISK_DESER`,
`DISK_ONLY`, `OFF_HEAP` and the `_2`/`_3` replicas exist. The `*_SER` names are Scala/Java only and raise
`AttributeError` in Python. DataFrame caches are columnar and compressed anyway.

### What sparkopt extracts

```python
_IMR_RE = re.compile(r'\bInMemoryRelation\b|\bInMemoryTableScan\b', re.I)
features.has_in_memory_relation = bool(_IMR_RE.search(plan))   # physical section
```

- Both the scan operator and its `InMemoryRelation` child match. Original sparkopt looked for
  `InMemoryScan`, which never matches `InMemoryTableScan`; the bundled copy fixes it.
- **No case consumes it.** sparkopt will still emit `persistence_strategy` / `fanout_cache` /
  `storage_level` ("cache this") for a DataFrame that is already cached. Treat a `True` here as a reason
  to **suppress** new cache advice and to check for a missing `unpersist()`.
- The cached build plan is part of the physical section, so its depth and Exchanges are counted
  (measured: a cached `groupBy` gave `depth = 9` and `exchange_count = 1` for its 1 real shuffle; the
  2 that original sparkopt reported for full captures came from the logical plans, which the bundled
  copy skips). A cache that is materialized actually *shortens* the effective lineage; the heuristic
  sees it as longer.

## Reuse: `estimated_reuse`

```python
path_re = re.compile(r'FileScan\s+\w+\s+\[([^\]]+)\]')
paths = path_re.findall(plan)
features.estimated_reuse = max(Counter(paths).values()) if paths else 1
```

The manual says "same FileScan path repeated". The regex actually captures the bracketed **column
list** right after `FileScan <format>`, for example `id#0L,k#1L,v#2` from
`FileScan parquet [id#0L,k#1L,v#2] Batched: true, ... Location: InMemoryFileIndex[file:/data/t]`. The
`Location` path is never looked at. Consequences:

| Situation | Real reuse | `estimated_reuse` |
|---|---|---|
| `df.filter(a).union(df.filter(b))` on one `spark.read` | 2 scans of the same data | **2** (same attribute ids, same column list) |
| two separate `spark.read.parquet(path)` calls joined/unioned | 2 scans | **1** (each read gets new attribute ids such as `#0L` / `#12L`) |
| self-join of an aggregated DataFrame | 1 shuffle reused (`ReusedExchange`) | **1** (the second scan is not printed) |
| same DataFrame used by 3 separate actions / writes | 3x recompute (classic fan-out) | **1** (each action has its own plan; one plan never shows it) |
| `explain(mode="formatted")` capture | any | **1** (scans print as `Scan parquet`, no `FileScan`); use plain `df.explain()` for `--plan` |
| Iceberg / JDBC / Kafka / in-memory sources (`BatchScan`, `Scan JDBCRelation`, `LocalTableScan`) | any | **1** |
| two different tables that happen to select identical column names *and* ids | 0 | can be > 1 (rare) |

The CLI uses the value only when it is greater than 1 and the profile does not set
`lineage.estimated_reuse_count`.
**Set `estimated_reuse_count` in the profile yourself**: count how many actions, writes or downstream
branches consume the DataFrame. That is the signal behind the main cache cases.

Cases driven by reuse (profile `lineage.estimated_reuse_count`):

| Condition | Case |
|---|---|
| `>= 2` (or `depth > 10`, or iterative) | `persistence_strategy` (CRITICAL) |
| `>= 2` | `fanout_cache`, `storage_level` |
| `>= 2` and `row_count > 5M` | `multi_stage_agg` (opt-in) |
| `>= 2` and not iterative | `incremental_etl` (opt-in) |
| `>= 3` and `is_small` (<= 200 MB) | `cached_broadcast` (opt-in) |

## Streaming

```python
_STREAM_RE = re.compile(r'\bStreamingRelation(?:V2)?\b|\bMicroBatch\w*\b|\bStreamExec\b|\bStreamingDataSourceV2\w*\b', re.I)
features.has_streaming = bool(_STREAM_RE.search(full_text))   # whole text, not just the physical section
```

Measured with a `rate` source `groupBy().count()` on Spark 3.5.3:

| Capture | `has_streaming` | Why |
|---|---|---|
| `df.explain()` (simple; what `collect_profile.py` writes) | **True** | physical plan prints `StreamingRelation rate, [timestamp#145, value#146L]` |
| `explain(mode="formatted")` | True | same operator name |
| `queryExecution().toString()` | **True** | logical plans print `StreamingRelationV2 ...`; the optimized/physical sections contain only `AnalysisException: Queries with streaming sources must be executed with writeStream.start()`, so the other counts are useless |

`MicroBatchScan` (the operator in a running query's `query.explain()`) and `StreamingDataSourceV2*`
match too; `StreamExec` is not a Spark operator name and is harmless. Original sparkopt missed
`StreamingRelationV2` and `MicroBatchScan`, so `toString()` captures (then `collect_profile.py`'s
default) reported `False`; the bundled copy fixes it. Capture streaming plans with `df.explain()`
before `writeStream.start()`, or set `lineage.has_streaming_source: true` in the profile.

Streaming physical plans also show `StateStoreRestore` / `StateStoreSave` (stateful aggregation),
`StreamingSymmetricHashJoin` (stream-stream join) and `EventTimeWatermark`. None of them are extracted,
but they matter: state without a watermark grows forever.

Cases: `has_streaming_source` drives `hybrid_checkpoint` (writeStream `checkpointLocation` for the stream,
`localCheckpoint` for batch side intermediates) and turns off `needs_local_checkpoint`. AQE does not apply
to streaming queries.

## Signal to case

| Signal | Action | sparkopt case | Skill |
|---|---|---|---|
| `InMemoryRelation` / `InMemoryTableScan` present | don't add another cache; ensure `unpersist()` after the last consumer | suppress `persistence_strategy` / `fanout_cache` advice | spark-persistence-strategy |
| `InMemoryRelation` with `StorageLevel(memory, ...)` on data larger than storage memory | evictions / recompute | `storage_level`: `MEMORY_AND_DISK` | spark-persistence-strategy |
| same subtree twice in one plan (`estimated_reuse >= 2`) | cache that subtree once | `fanout_cache` | spark-persistence-strategy |
| one DataFrame feeding several actions (not visible in a plan) | set `estimated_reuse_count` | `fanout_cache`, `persistence_strategy` | spark-persistence-strategy |
| `StreamingRelation` / `StateStoreSave` | stream checkpoint + watermark | `hybrid_checkpoint` | spark-persistence-strategy |
| small dimension broadcast in several joins | cache + broadcast once | `cached_broadcast` (opt-in) | spark-complex-workloads |
