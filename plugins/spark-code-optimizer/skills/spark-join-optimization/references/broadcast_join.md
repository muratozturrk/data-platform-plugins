# broadcast_join

## Problem

A join between a large fact table and a small dimension table runs as a sort-merge join. Both sides
are hash-partitioned across the network (two `Exchange` nodes) and then sorted, even though the small
side could fit in every executor's memory. A broadcast hash join (BHJ) ships the small side once to
each executor and streams the large side through a hash lookup. The large side is not shuffled and
not sorted.

## How Spark executes it

1. The driver collects the small side (`collect` of the build relation).
2. It builds a `HashedRelation` and broadcasts it as a torrent broadcast to all executors.
3. Each task of the large side probes the hash table locally.

Plan signals (`df.explain()`):

```
# Without broadcast (the problem)
SortMergeJoin [customer_id], [customer_id], Inner
:- Sort [customer_id ASC]
:  +- Exchange hashpartitioning(customer_id, 200)
+- Sort [customer_id ASC]
   +- Exchange hashpartitioning(customer_id, 200)

# With broadcast (the goal)
BroadcastHashJoin [customer_id], [customer_id], Inner, BuildRight
:- FileScan parquet orders
+- BroadcastExchange HashedRelationBroadcastMode(...)
```

With AQE enabled, the final plan is only visible after an action, or in the SQL tab of the Spark UI
(`AdaptiveSparkPlan isFinalPlan=true`). AQE can turn an SMJ into a BHJ at runtime when the
shuffle statistics show a small side (`spark.sql.adaptive.autoBroadcastJoinThreshold`).

## sparkopt rule

| Field | Value |
|---|---|
| Class | `BroadcastJoinCase(right_table_bytes=None)` |
| Priority | `HIGH` (90) |
| matches() | `(right_table_bytes or profile.estimated_bytes) <= 200 * 1024**2` |
| Output | `ctx.add_hint("broadcast_join", ...)` + import `functions as F` |

The 200 MB value is a **sparkopt heuristic** for an explicit hint, not a Spark limit. When
`right_table_bytes` is not passed, the rule falls back to the profiled DataFrame's own estimated size.
So profile the candidate small side, or pass its size explicitly. Otherwise a small *left* table can
trigger a hint that is applied to the wrong side. The CLI guards against this: without `join_cols` or
`right_table_bytes` in the profile it excludes `broadcast_join`.

Generated output (bundled copy):

```python
result = large_df.join(F.broadcast(small_df), on=join_keys, how="inner")
# spark.sql.autoBroadcastJoinThreshold değerini de kontrol edin (default: 10MB)
# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(200 * 1024 * 1024))
```

**Correction**: keep the explicit hint. Drop the commented global threshold bump. Raising
`autoBroadcastJoinThreshold` to 200 MB makes *every* join with a side estimated under 200 MB a
broadcast. Estimates are often wrong (filters, missing stats), and the result is driver OOM.
(Original sparkopt's `explain()` printed `~0 MB` without `right_table_bytes`; the bundled copy prints
the right size when it is given and a generic "≤200 MB" message otherwise.)

## Before / After

```python
from pyspark.sql import functions as F

# Before: Spark picks SortMergeJoin because dim_customer's estimated size > 10MB
result = orders.join(dim_customer, "customer_id", "inner")

# After: explicit broadcast of the small side
result = orders.join(F.broadcast(dim_customer), "customer_id", "inner")

# Equivalent SQL / hint forms
result = orders.join(dim_customer.hint("broadcast"), "customer_id")
spark.sql("SELECT /*+ BROADCAST(c) */ * FROM orders o JOIN dim_customer c USING (customer_id)")
```

Shrink the small side before broadcasting. Select only the needed columns and filter first.
Broadcast size is the in-memory `UnsafeRow` size, which can be 2-5x the compressed Parquet size.

## Configs

| Config | Default | Notes |
|---|---|---|
| `spark.sql.autoBroadcastJoinThreshold` | `10MB` | Auto BHJ when the estimated size is below this. `-1` disables auto broadcast. Hints still work. |
| `spark.sql.adaptive.autoBroadcastJoinThreshold` | same as above | AQE runtime conversion threshold (3.2+). |
| `spark.sql.broadcastTimeout` | `300` (s) | Increase if building the broadcast is slow. Usually the fix is a smaller table. |
| `spark.driver.memory` / `spark.driver.maxResultSize` | `1g` / `1g` | The driver collects the build side. It must fit within both. |
| executor memory | n/a | Each executor holds one copy of the hashed relation. |

Hard limit: Spark refuses to broadcast a relation larger than **8 GB** (or more than ~512M rows).

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Hint ignored, plan still SMJ | Join type can't build on that side (e.g. `left` join with a hint on the left table) | Broadcast the non-preserved side. For a full outer join BHJ is impossible. |
| `SparkException: Cannot broadcast the table that is larger than 8GB` | Build side too big | Don't broadcast. Use SMJ + AQE, or filter first. |
| Driver `OutOfMemoryError` | Collected relation exceeds driver heap | Raise `spark.driver.memory`, or prune columns/rows |
| `Could not execute broadcast in 300 secs` | Slow upstream for the build side | Cache/materialize the small side first, or raise `spark.sql.broadcastTimeout` |
| Executors GC-thrash after the join | Broadcast too large for executor heap | Lower the hinted size, or use SMJ |
| Auto broadcast of a huge table | Stale stats claim it is small | `ANALYZE TABLE ... COMPUTE STATISTICS`. Disable auto with `-1` and hint explicitly. |

## Related cases

- `skew_join`: broadcasting the small side removes join skew completely (no shuffle), so try it first.
- `sort_merge_join`: the fallback when neither side fits the broadcast heuristics.
- `bucket_join`: the shuffle-free alternative when both sides are large.
- `adaptive_qe` (partition skill): AQE can convert SMJ to BHJ at runtime.
