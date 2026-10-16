# sort_merge_join

## Problem

Both sides of the join are large (sparkopt: >= 10 GB). Broadcasting is impossible, so Spark must
shuffle both sides by key and merge sorted partitions. The usual pain points are a bad
partition count (too few means spill/OOM, too many means scheduler overhead and tiny files), key
skew, and redundant shuffles when the same frame is joined several times on one key.

## How Spark executes it

`SortMergeJoin` is the default physical join for equi-joins when neither side is below the broadcast
threshold (`spark.sql.join.preferSortMergeJoin=true`):

1. `Exchange hashpartitioning(key, spark.sql.shuffle.partitions)` on each side.
2. `Sort [key ASC]` within each partition.
3. Stream-merge the two sorted iterators. Memory per task stays bounded, and it spills when needed.

Plan signal:

```
SortMergeJoin [k], [k], Inner
:- Sort [k ASC NULLS FIRST]
:  +- Exchange hashpartitioning(k, 200)
+- Sort [k ASC NULLS FIRST]
   +- Exchange hashpartitioning(k, 200)
```

If one side already has the required partitioning (e.g. an earlier `repartition(n, k)` with the same
`n`, a bucketed table, or a previous aggregation on `k`), its `Exchange` disappears. Under AQE look
for `AQEShuffleRead coalesced` (partitions merged) and `isSkew=true` (skew split).

## sparkopt rule

| Field | Value |
|---|---|
| Class | `SortMergeJoinCase()` |
| Priority | `LOW` (10) |
| matches() | `profile.is_large`, i.e. `estimated_bytes >= large_table_threshold_bytes` (default `10 * 1024**3`, 10 GB) |
| Output | `ctx.add_hint("sort_merge_join", ...)` |

10 GB is a **sparkopt heuristic** (`DataProfile.large_table_threshold_bytes`). LOW priority means this
is advice, not a forced rewrite.

Library output (original):

```python
left_df  = left_df.repartition(200, "join_key").sortWithinPartitions("join_key")
right_df = right_df.repartition(200, "join_key").sortWithinPartitions("join_key")
result   = left_df.join(right_df, on="join_key", how="inner")
```

**Correction**: for a single join this changes nothing useful. Spark inserts the same exchange and sort
itself, so the snippet only hardcodes 200 partitions and overrides AQE's coalescing. Pre-partitioning
pays off only when the *same* partitioned frame feeds several joins/aggregations on the same key.
Then `repartition(n, key)` once lets later operators in the same plan skip their `Exchange`. If you
also `cache()` it for reuse across actions, check `explain()`. With AQE (verified on Spark 3.5) the
cached plan's partitioning can be hidden, and Spark adds a new `Exchange` above the
`InMemoryTableScan`. `sortWithinPartitions` is almost never needed by hand.

## Before / After

```python
# Before (library style): manual shuffle, fixed 200 partitions
l = left.repartition(200, "k").sortWithinPartitions("k")
r = right.repartition(200, "k").sortWithinPartitions("k")
out = l.join(r, "k")

# After, single join: let Spark plan SMJ, size partitions via AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "800")  # upper bound; AQE coalesces down
out = left.join(right, "k")

# After, frame reused by several operators on k: partition once
left_p = left.repartition(800, "k")
a = left_p.join(right, "k")            # no second Exchange on the left side
b = left_p.groupBy("k").count()        # only the repartition Exchange in this plan
# .cache() on left_p + AQE may re-add an Exchange above InMemoryTableScan: verify with explain()
```

Rule of thumb for `spark.sql.shuffle.partitions`: total shuffle bytes / 128-200 MB. With AQE, set it
high and let `coalescePartitions` merge small ones.

## Configs

| Config | Default | Notes |
|---|---|---|
| `spark.sql.shuffle.partitions` | `200` | Initial reducer count for joins/aggregations |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Merge small post-shuffle partitions |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `64MB` | Target post-shuffle partition size |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Split skewed SMJ partitions |
| `spark.sql.join.preferSortMergeJoin` | `true` | `false` lets Spark choose shuffled hash join when one side is much smaller |
| hint `shuffle_hash` / `merge` | n/a | `df.hint("shuffle_hash")` avoids the sort when the build side partitions fit in memory |

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Disk spill in the sort phase | Too few partitions / partitions too large | Raise `shuffle.partitions`, or set AQE advisory size |
| Thousands of tiny tasks | Partition count too high, AQE off | Enable AQE coalescing |
| Join still shuffles after `repartition(n, k)` | Different `n` than the join's expected partitioning, or a different key expression | Same column, and let the join reuse it |
| One task dominates | Key skew | See `skew_join` |
| Sort not needed (hash fits) | SMJ sort cost dominates | `hint("shuffle_hash")` on the smaller side |
| Exploding row count | Many-to-many key duplication | Deduplicate or pre-aggregate one side first |

## Related cases

- `broadcast_join`: try first if either side can be made small (filter, project).
- `bucket_join`: removes the exchange for repeated large x large joins.
- `skew_join`: AQE skew split works on SMJ.
- `repartition` / `adaptive_qe` (partition skill): partition sizing.
