# skew_join

## Problem

A few join keys (a null-ish placeholder such as `-1`, a big customer, a default country) hold a large
share of the rows. Hash partitioning sends all rows of one key to one reducer task. That task runs
for minutes or hours, spills to disk, or OOMs, while the other 199 tasks finish in seconds. The stage
is only as fast as its slowest task.

## How Spark executes it

A sort-merge (or shuffled hash) join puts both sides through
`Exchange hashpartitioning(key, N)`, so every row with the same key lands in the same partition.
Skew shows up at runtime, not in the static plan.

Signals:
- Spark UI > Stage > Summary Metrics: **Max** duration / shuffle read size far above the **75th
  percentile** (for example 2 GB vs 40 MB).
- Spill (memory/disk) present on only a few tasks.
- With AQE skew handling active, the final plan shows `SortMergeJoin ... isSkew=true` and
  `AQEShuffleRead skewed` in `explain()` after the action (Spark UI SQL tab).
- Key check:
  ```python
  df.groupBy("key").count().orderBy(F.desc("count")).show(20)
  ```

## sparkopt rule

| Field | Value |
|---|---|
| Class | `SkewJoinCase(skew_col=None, salt_buckets=10)` |
| Priority | `HIGH` (90) |
| matches() | `profile.partition.is_skewed`, i.e. `skew_ratio > 3.0` where `skew_ratio = max_rows / avg_rows` over partitions |
| Output | `ctx.add_transformation("skew_salt", ...)`. Key = `skew_col`, else `ctx.join_cols[0]`, else `"join_key"` |

The 3.0 ratio is a **sparkopt heuristic**. It measures *partition* row skew of the profiled frame,
which is not necessarily *key* skew in the join. Confirm with the key-count query above.

Generated output (bundled copy, `join_cols=["user_id"]`):

```python
SALT_BUCKETS = 10

left_salted = left_df \
    .withColumn("_salt", (F.rand() * SALT_BUCKETS).cast("int")) \
    .withColumn("user_id_s", F.concat(F.col("user_id"), F.lit("_"), F.col("_salt")))

salt_range    = spark.range(SALT_BUCKETS).withColumnRenamed("id", "_salt")
right_exploded = right_df.crossJoin(salt_range) \
    .withColumn("user_id_s", F.concat(F.col("user_id"), F.lit("_"), F.col("_salt"))) \
    .drop("user_id", "_salt")   # sol taraftaki kopyalarla çakışmasın (ambiguous column)

result = left_salted.join(right_exploded, on="user_id_s", how="inner") \
                    .drop("_salt", "user_id_s")
```

The right side drops its key and `_salt` before the join, so no column is ambiguous; this snippet was
run on PySpark 3.5 and returned the same row count as a plain join. (Original sparkopt kept both
copies, so `.drop("_salt")` and later `key` references were ambiguous, and its `explain()` printed
`salt_buckets` as the skew ratio; the bundled copy fixes both.)

**Corrections**:
1. **AQE first.** On Spark 3.x (AQE on by default since 3.2), `spark.sql.adaptive.skewJoin.enabled`
   splits oversized partitions automatically, with no code change. Salting is the fallback.
2. Salting *all* keys multiplies the right side by N. Salt only the hot keys and join the rest normally.
3. The string concat key is unnecessary: join on `[key, "_salt"]` instead (below).

## Before / After

```python
# Before
result = events.join(users, "user_id")

# After, step 1: AQE (preferred)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
result = events.join(users, "user_id")

# After, step 2: targeted salting (fallback). Full version in examples/skew_join.py
N = 16
hot = [r.user_id for r in events.groupBy("user_id").count()
       .filter(F.col("count") > 1_000_000).select("user_id").collect()]
ev = events.withColumn("_salt", F.when(F.col("user_id").isin(hot),
                       (F.rand(seed=42) * N).cast("int")).otherwise(F.lit(0)))
salts = spark.range(N).select(F.col("id").cast("int").alias("_salt"))
us = (users.filter(F.col("user_id").isin(hot)).crossJoin(salts)
      .unionByName(users.filter(~F.col("user_id").isin(hot)).withColumn("_salt", F.lit(0))))
result = ev.join(us, ["user_id", "_salt"]).drop("_salt")
```

Joining on `["user_id", "_salt"]` (a list of names) keeps a single `user_id` column, so there is no
string concat and no ambiguity.

## Configs

| Config | Default | Notes |
|---|---|---|
| `spark.sql.adaptive.enabled` | `true` (3.2+) | Required for skew join handling |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Split skewed partitions of SMJ/SHJ |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | `5` | Skewed if > factor x median partition size... |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | `256MB` | ...AND larger than this |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `64MB` | Target size of the split pieces |
| `spark.sql.adaptive.forceOptimizeSkewedJoin` | `false` (3.3+) | Allow the split even if it adds an extra shuffle |

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| AQE on, still one straggler | Partition below 256MB/5x thresholds, or many hot keys spread unevenly | Lower `skewedPartitionThresholdInBytes` / factor |
| AQE doesn't split | Join type/side not supported (e.g. full outer), or no shuffle in that stage | Salting, or broadcast the other side |
| Non-equi or range join skew | AQE handles equi-joins only | Salting / bucketing ranges |
| Salted join returns too many rows | Right side not exploded for every salt value used on the left | Cross join the hot keys with all `0..N-1` salts |
| Salted join returns too few rows | Non-hot rows on the left got random salts | Salt only hot keys; non-hot get `0` |
| Skew is on `NULL` keys | Nulls never match in an inner join anyway | Filter `key IS NOT NULL` before the join (or handle nulls separately for outer joins) |
| Right side huge after salting | N too large, or all keys salted | Smaller N, hot keys only |

## Related cases

- `broadcast_join`: if one side is small, broadcast and skew disappears.
- `repartition_before_agg` (aggregation skill): skewed aggregation needs two-phase salting, not repartition.
- `adaptive_qe` (partition skill): enables the AQE settings above.
- `sort_merge_join`: AQE skew split acts on SMJ.
