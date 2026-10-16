# cached_broadcast: small lookup, joined in many batches, changes now and then

## Problem

An exchange-rate, product-catalog or segment table is small and joined by many queries
or micro-batches. Without care every batch re-reads and re-parses it from object storage;
with a naive cache the joins keep using a stale snapshot forever.

## How Spark executes it

- `F.broadcast(dim)` / `dim.hint("broadcast")` plans a `BroadcastExchange`: the driver
  collects the small side from executors, builds a hash relation, and ships it to every
  executor. That happens once per query (per micro-batch in streaming). There is no
  broadcast reuse across separate queries, only `ReusedExchange` inside one plan.
- Caching the lookup does not avoid that collect + broadcast; it makes the collect cheap
  (read cached blocks instead of re-scanning + re-transforming the source).
- Once a cached relation is materialized, its statistics are the real in-memory size, so
  Spark may auto-broadcast it when it is below `spark.sql.autoBroadcastJoinThreshold`
  (10MB default). Larger tables need the explicit hint.
- A cache is a snapshot. New data in the source is invisible until you unpersist and
  reload.

Plan signal: `BroadcastHashJoin` with `BroadcastExchange` over `InMemoryTableScan` =
cached + broadcast. `BroadcastExchange` over `FileScan` in every batch = no cache.
`SortMergeJoin` = not broadcast (size estimate too large, or hint missing).

## sparkopt rule

| Item | Value |
|---|---|
| `name` | `cached_broadcast` |
| `priority` | `Priority.MEDIUM` (50) |
| `matches()` | `profile.is_small and lineage.estimated_reuse_count >= 3`, where `is_small` = `estimated_bytes <= 200 * 1024**2` |
| Registration | Not in `ALL_BUILTIN_CASES`, no DSL method; select it via the CLI `--case` |

The 200 MB `is_small` limit is a sparkopt heuristic: it is the ceiling for an *explicit*
broadcast hint, not Spark's automatic threshold (10MB). Spark's hard limit is 8GB per
broadcast; in practice driver memory, `spark.driver.maxResultSize` and executor memory
(every executor holds a full copy) bite well before that.

Generated code: a `LookupManager` that reads parquet, persists at `MEMORY_ONLY`,
`count()`s, and reloads after `ttl_batches` calls to `get()`; a `for batch_id, batch_df
in enumerate(batch_stream)` loop joining `F.broadcast(lookup)` and appending each batch to
its own directory.

Bugs (fixed in `examples/cached_broadcast.py`):

1. Original sparkopt used `MEMORY_ONLY_SER` (AttributeError in PySpark); the bundled copy emits
   `MEMORY_ONLY`. Prefer `MEMORY_AND_DISK` so an evicted lookup is not recomputed.
2. The "cache prevents re-fetch to the driver" comment is wrong: broadcast still collects
   to the driver each query; cache only saves the source scan.
3. `batch_stream` is undefined; Structured Streaming does this with `foreachBatch`.
4. Refresh by call count reloads unchanged data and serves stale data in between.
   Refresh on source version change (Delta `history(1)`), with a max-age fallback.
5. The old snapshot is unpersisted before the new one is loaded; a failed reload leaves
   nothing cached. Load + `count()` the new one, then drop the old one.
6. One output directory per batch -> small-files explosion; append to one table.

## Before / After

```python
# Before: re-read every batch (or cached once and never refreshed)
def enrich(batch_df, batch_id):
    dim = spark.read.parquet("s3a://bucket/products/")
    batch_df.join(dim, "product_id", "left").write.mode("append").parquet(out)
```

```python
# After: cached snapshot, reloaded only when the Delta version moves
class VersionedLookup:
    def get(self):
        v = DeltaTable.forPath(spark, path).history(1).select("version").first()[0]
        if self.df is None or v != self.version:
            fresh = (spark.read.format("delta").option("versionAsOf", v).load(path)
                     .select("product_id", "category", "list_price")
                     .persist(StorageLevel.MEMORY_AND_DISK))
            fresh.count()
            if self.df is not None:
                self.df.unpersist()
            self.df, self.version = fresh, v
        return self.df

def enrich(batch_df, batch_id):
    batch_df.join(F.broadcast(lookup.get()), "product_id", "left") \
            .write.format("delta").mode("append").save(out)
```

Full runnable version: `examples/cached_broadcast.py`.

## Configs

| Config | Why |
|---|---|
| `spark.sql.autoBroadcastJoinThreshold` (10MB) | Automatic broadcast ceiling; `-1` disables auto, hints still work |
| `spark.sql.broadcastTimeout` (300s) | Large broadcasts on busy drivers time out |
| `spark.driver.maxResultSize` (1g) | The broadcast collect is subject to it |
| `spark.driver.memory` | Driver builds the hash relation; size for 2-3x the lookup |

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Join results ignore lookup changes | Stale cached snapshot | Version-based refresh |
| `SortMergeJoin` despite small lookup | Size estimate above threshold before caching | `F.broadcast()` hint |
| Driver OOM / `maxResultSize` exceeded | Lookup bigger than it looks (wide strings) | Select only needed columns; drop the broadcast |
| `Could not execute broadcast in 300 secs` | Slow collect | Cache the lookup; raise `broadcastTimeout` |
| Executors near memory limit | Every executor holds a copy per concurrent query | Smaller lookup or sort-merge join |
| Storage tab keeps old versions | Old snapshot not unpersisted | `unpersist()` after the new one is materialized |

## Related cases

- `broadcast_join` (join skill): the general broadcast rule and thresholds.
- `streaming_batch_hybrid` (this skill): the same lookup in a plain stream-static join.
- `fanout_cache`: why the lookup is cached at all (3+ consumers).
