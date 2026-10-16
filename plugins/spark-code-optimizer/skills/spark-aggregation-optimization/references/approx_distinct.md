# approx_distinct

## Problem

`countDistinct(col)` / `COUNT(DISTINCT col)` is exact. To get it, Spark has to group by
`(group keys, col)` first, which shuffles one row per distinct value per map task. Then it groups
again to count. With several distinct aggregates in one `agg`, Spark adds an `Expand` that
duplicates every input row once per distinct aggregate. On billions of rows with high-cardinality
columns (user ids, session ids) this is often the most expensive part of a query.
`approx_count_distinct` uses a HyperLogLog++ sketch of fixed size per group and runs as a normal
partial/final aggregation.

## How Spark executes it

Exact, single distinct:

```
HashAggregate(keys=[country], functions=[count(distinct user_id)])
+- Exchange hashpartitioning(country, 200)
   +- HashAggregate(keys=[country], functions=[partial_count(distinct user_id)])
      +- HashAggregate(keys=[country, user_id], functions=[])
         +- Exchange hashpartitioning(country, user_id, 200)     <- big shuffle
            +- HashAggregate(keys=[country, user_id], functions=[])
```

Exact, several distinct columns: an `Expand` node appears under the aggregates (row count x number of
distinct groups).

Approximate:

```
HashAggregate(keys=[country], functions=[approx_count_distinct(user_id, 0.05, 0, 0)])
+- Exchange hashpartitioning(country, 200)
   +- HashAggregate(keys=[country], functions=[partial_approx_count_distinct(user_id, ...)])
```

That is one shuffle, carrying one sketch per group per map task.

## sparkopt rule

| Field | Value |
|---|---|
| Class | `ApproxDistinctCase()` |
| Priority | `MEDIUM` (50) |
| matches() | `profile.row_count > 5_000_000` |
| Output | `add_import(functions as F)`, `add_hint("approx_distinct", ...)` |

5M rows is a **sparkopt heuristic**. The profile does not know whether a distinct count is even
present, or whether approximation is acceptable.

Library output (original):

```python
# approx_count_distinct (HyperLogLog, ~2% error), 100x faster than countDistinct
result = df.groupBy("group_col").agg(
    F.approx_count_distinct("user_id", rsd=0.02).alias("approx_unique_users")
)
```

**Corrections**:
1. **Default rsd is 0.05, not 0.02.** `F.approx_count_distinct(col)` without `rsd` gives a 5% relative
   standard deviation. The library's "~2%" holds only because the snippet passes `rsd=0.02`
   explicitly. The manual (section 5.2) states "%2 error" as if it were the default.
2. `rsd` is the *relative standard deviation* of the estimate, not a maximum error. About 95% of
   estimates fall within +/-2 x rsd.
3. Lower rsd costs memory. HLL++ precision is `p = ceil(2 * log2(1.106 / rsd))`, with `2^p`
   registers per group. rsd 0.05 gives p=9 (512 registers). rsd 0.02 gives p=12 (4096 registers,
   8x larger). With millions of groups this matters. rsd must be <= ~0.39 (p >= 4).
4. "100x faster" is a library claim. The gain depends on cardinality and the number of distinct
   aggregates. Measure it.

## Before / After

```python
from pyspark.sql import functions as F

# Before: exact, two shuffles (plus Expand when there are several distinct aggregates)
daily = events.groupBy("event_date").agg(
    F.countDistinct("user_id").alias("dau"),
    F.countDistinct("session_id").alias("sessions"),
)

# After: approximate, one shuffle, fixed memory per group
daily = events.groupBy("event_date").agg(
    F.approx_count_distinct("user_id").alias("dau_approx"),                 # rsd=0.05 default
    F.approx_count_distinct("session_id", rsd=0.02).alias("sessions_approx"),
)

# Re-aggregatable (Spark 3.5+): store sketches, union them later (e.g. daily -> monthly)
sk = events.groupBy("event_date").agg(F.hll_sketch_agg("user_id").alias("sk"))
monthly = (sk.groupBy(F.date_trunc("month", "event_date").alias("m"))
             .agg(F.hll_sketch_estimate(F.hll_union_agg("sk")).alias("mau_approx")))
```

You can't sum approximate counts across groups (the same user appears in several days). Use sketches
for that.

## Configs

No dedicated configs. Accuracy is controlled per call via `rsd`. Related settings:

| Config | Default | Notes |
|---|---|---|
| `spark.sql.shuffle.partitions` | `200` | Final aggregation parallelism |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Fewer, fuller final partitions |

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Result differs from the exact count by a few % | Expected. rsd is the standard deviation. | Lower `rsd`, or keep exact for that metric |
| Memory pressure with many groups | Low `rsd` gives large sketches per group | Use the default 0.05 where acceptable |
| `IllegalArgumentException ... at most 39%` | rsd too large | Use rsd <= 0.39 |
| Monthly = sum of daily approx counts is wrong | Distinct counts are not additive | `hll_sketch_agg` + `hll_union_agg` (3.5+) |
| Exact counts still required (billing) | Approximation not allowed | Keep `countDistinct`. Pre-dedupe with `dropDuplicates([keys, col])` once and reuse. |
| Small cardinalities | HLL++ is near-exact for small sets anyway | Fine either way |

## Related cases

- `partial_agg`: approx distinct turns a distinct count into a normal two-phase aggregation.
- `repartition_before_agg`: for skewed group keys, the sketch merge still lands on one reducer per key.
- `window_opt`: `approx_count_distinct` also works over windows, while `countDistinct` over windows is not supported.
