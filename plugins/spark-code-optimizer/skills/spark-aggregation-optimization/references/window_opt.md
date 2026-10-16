# window_opt

## Problem

Window functions (`row_number`, `lag`, running sums, rolling averages) need every row of a window
partition sorted together in one task. They never partially aggregate. Large or skewed window
partitions, or a window with no `partitionBy` at all, produce huge single tasks, heavy spill and OOM.
Window functions are also often used where a `groupBy` would do the same job much more cheaply.

## How Spark executes it

```
Window [avg(value) windowspecdefinition(user_id, ts ASC, specifiedwindowframe(...))], [user_id], [ts ASC]
+- Sort [user_id ASC, ts ASC], false
   +- Exchange hashpartitioning(user_id, 200)
```

1. Shuffle by `partitionBy` columns. Without `partitionBy` this becomes `Exchange SinglePartition`
   and Spark logs `WARN WindowExec: No Partition Defined for Window operation! Moving all data to a
   single partition`.
2. Sort by partition + order keys.
3. `WindowExec` buffers each window partition (all rows of one key) in an
   `ExternalAppendOnlyUnsafeRowArray`. Past `spark.sql.windowExec.buffer.in.memory.threshold` rows it
   moves to a spillable sorter, and past `spark.sql.windowExec.buffer.spill.threshold` it spills to disk.
4. Frame evaluation:
   - `unboundedPreceding` > `currentRow` (running): incremental, O(n). Cheap.
   - `unboundedPreceding` > `unboundedFollowing` (whole partition): computed once per partition.
   - sliding `rowsBetween(-k, 0)` / `rangeBetween(-x, 0)`: recomputed per row, O(n x window) for most
     aggregates.

Several window columns with the **same** `partitionBy`/`orderBy` share one `Exchange` + `Sort` + `Window`.
Different specs each add their own sort, and maybe their own shuffle.

## sparkopt rule

| Field | Value |
|---|---|
| Class | `WindowOptCase()` |
| Priority | `HIGH` (90) |
| matches() | `profile.row_count > 500_000` |
| Output | `add_import("from pyspark.sql.window import Window")`, `add_hint("window_opt", ...)` |

500K rows is a **sparkopt heuristic**. The profile doesn't check whether windows are used at all.

Library output (original):

```python
# unboundedPreceding loads the whole partition into memory, OOM risk
win = (Window.partitionBy("user_id")
             .orderBy(F.unix_timestamp("ts"))
             .rangeBetween(-7 * 86400, 0))  # last 7 days
df = df.withColumn("rolling_avg", F.avg("value").over(win))
```

**Corrections**:
1. The premise is inaccurate. The window *partition* is buffered whatever the frame is, and that
   buffer spills. A running `unboundedPreceding` frame is the *cheapest* frame to compute. A bounded
   frame does not reduce what is buffered.
2. Switching to `rangeBetween(-7 days, 0)` changes the result (running total becomes a 7-day rolling
   value). It is a business-logic change, not an optimization.
3. The real fixes are: always `partitionBy`, fix skewed partition keys, avoid `collect_list` /
   `collect_set` over whole-partition frames, share one `WindowSpec`, and replace windows with
   `groupBy` where possible.
4. `F.col("ts").cast("long")` is the usual way to order by epoch seconds for a range frame.
   `unix_timestamp(ts)` also works for timestamp columns.

## Before / After

```python
from pyspark.sql import Window, functions as F

# Before: global window, every row in one task
w_bad = Window.orderBy("ts")
df = df.withColumn("rn", F.row_number().over(w_bad))

# After: partitioned window, one spec reused for several columns (one sort)
w = Window.partitionBy("user_id").orderBy("ts")
df = (df.withColumn("rn", F.row_number().over(w))
        .withColumn("prev_value", F.lag("value").over(w))
        .withColumn("running_sum", F.sum("value").over(
            w.rowsBetween(Window.unboundedPreceding, Window.currentRow))))

# Rolling 7 days, only when that is the requirement
w7 = (Window.partitionBy("user_id").orderBy(F.col("ts").cast("long"))
            .rangeBetween(-7 * 86400, 0))
df = df.withColumn("avg_7d", F.avg("value").over(w7))

# Before: latest row per key via row_number window
# latest = df.withColumn("rn", F.row_number().over(w.orderBy(F.desc("ts")))).filter("rn = 1")
# After: groupBy + max_by (Spark 3.3+ Python API), partial aggregation, no sort
latest = df.groupBy("user_id").agg(F.max("ts").alias("ts"),
                                   F.max_by("value", "ts").alias("value"))
```

## Configs

| Config | Default | Notes |
|---|---|---|
| `spark.sql.windowExec.buffer.in.memory.threshold` | `4096` | Rows held in an in-memory array before a spillable sorter is used |
| `spark.sql.windowExec.buffer.spill.threshold` | version-dependent | Rows before the window buffer spills to disk |
| `spark.sql.shuffle.partitions` | `200` | Shuffle parallelism for the `partitionBy` exchange |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | AQE can coalesce, but it can't split a single window key |

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `No Partition Defined for Window operation!` | Missing `partitionBy` | Add a partition key. For a global rank, use a two-step approach or `monotonically_increasing_id` if order is irrelevant. |
| One task spills GBs | Hot `partitionBy` key | Partition by a finer key (e.g. user + day), or handle hot keys separately |
| Multiple `Sort` nodes | Different window specs | Align specs. Compute columns sharing a spec together. |
| `AnalysisException` on `rangeBetween` | Multiple or non-numeric `orderBy` expressions | Single numeric/date order key (cast timestamp to long) |
| Slow sliding frame | O(n x window) recomputation | Reduce frame, or pre-aggregate per time bucket with `groupBy` first |
| `collect_list` window OOM | List of the whole partition per row | Aggregate with `groupBy` instead |

## Related cases

- `partial_agg`: `groupBy` alternatives get map-side aggregation, while windows don't.
- `repartition_before_agg`: a window on a repartitioned frame with the same key reuses the partitioning.
- `skew_join` (join skill): the same hot-key diagnosis applies to window partition keys.
- `spill_prevention` (persistence skill): memory sizing when windows spill.
