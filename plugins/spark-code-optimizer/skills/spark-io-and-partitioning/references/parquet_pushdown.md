# parquet_pushdown: column pruning and predicate pushdown

## Problem

A Parquet scan that reads every column and every row group costs I/O, network and decode time on data the
query then throws away. Parquet is columnar and stores min/max statistics per row group, so Spark can
skip most of that work if it can see which columns and which predicates the query needs **at scan time**.
Two optimizations make that possible:

- **Column pruning:** only the columns that the plan references are read (`ReadSchema`).
- **Predicate pushdown:** simple comparisons are handed to the Parquet reader, which skips row groups
  whose min/max cannot match (`PushedFilters`). Filters on partition columns prune whole directories
  (`PartitionFilters`).

## How Spark executes it

Catalyst's optimizer pushes `Filter` and `Project` nodes down to the `FileScan` regardless of the order
you wrote them in. `select().filter()` and `filter().select()` produce the same physical plan. Check the
`FileScan` line of `df.explain()`:

```
+- *(1) Filter (isnotnull(event_date#3) AND (event_date#3 >= 2024-01-01))
   +- *(1) ColumnarToRow
      +- FileScan parquet [user_id#1,amount#2,event_date#3] Batched: true,
         DataFilters: [isnotnull(event_date#3), (event_date#3 >= 2024-01-01)],
         Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/data/events],
         PartitionFilters: [],
         PushedFilters: [IsNotNull(event_date), GreaterThanOrEqual(event_date,2024-01-01)],
         ReadSchema: struct<user_id:bigint,amount:double,event_date:date>
```

What to read:

| Field | Healthy | Problem |
|---|---|---|
| `ReadSchema` | only the columns used downstream | every column of the table |
| `PushedFilters` | your predicates as `EqualTo`, `GreaterThan`, `In`, `IsNotNull`... | `[]` although you filter |
| `PartitionFilters` | predicate on the partition column | `[]` when filtering a partition column (directory scan) |
| `Filter` above the scan | still present, which is normal | the only place your predicate appears |

The `Filter` node stays above the scan even when pushdown works, because row-group skipping is
coarse-grained and Spark re-checks every row.

## sparkopt rule

```python
class ParquetPushdownCase(OptimizationCase):
    name = "parquet_pushdown"
    priority = Priority.HIGH
    def matches(self, profile): return True
```

- **Matches:** always. It is a reminder, not a diagnosis.
- **Generates:** configs `spark.sql.parquet.filterPushdown=true` and
  `spark.sql.parquet.columnarReaderBatchSize=4096`, plus a `.select(...).filter(...)` snippet.
- **Correction:** both configs are **Spark defaults already**, so emitting them changes nothing unless the
  job explicitly disabled them. The value of the case is the snippet and the habit of checking the plan.
  The generated filter `F.col("event_date") >= "2024-01-01"` works (Spark casts the string literal to the
  column's date type), but an explicit `F.lit(...).cast("date")` or `F.to_date(F.lit(...))` is clearer.

## Before / After

```python
# BEFORE: every column read; the filter hides inside a UDF, so nothing is pushed
from pyspark.sql import functions as F, types as T
is_recent = F.udf(lambda d: d is not None and d.year >= 2024, T.BooleanType())
df = spark.read.parquet(path).filter(is_recent("event_date"))
result = df.groupBy("user_id").agg(F.sum("amount"))

# AFTER: native expressions, explicit columns
df = (spark.read.parquet(path)
      .select("user_id", "amount", "event_date")
      .filter(F.col("event_date") >= F.to_date(F.lit("2024-01-01"))))
result = df.groupBy("user_id").agg(F.sum("amount"))
```

## What blocks pushdown

| Pattern | Pushed? | Fix |
|---|---|---|
| `F.col("c") == 5`, `>`, `<`, `isin`, `isNull`, `isNotNull` | yes | none |
| `F.col("s").startswith("ab")` | yes (as `StringStartsWith`) | none |
| `F.year("d") == 2024` | no | `d >= '2024-01-01' AND d < '2025-01-01'` |
| `F.lower("s") == "x"` | no | store normalized values, or accept the scan |
| `F.col("c").cast("string") == "5"` | no | compare in the column's own type |
| Python / pandas UDF predicate | no | rewrite with built-in functions |
| `OR` across columns | partly | usually pushed if each side is pushable |
| nested struct field `a.b > 1` | yes since 3.x (`nestedPredicatePushdown`) | keep `spark.sql.optimizer.nestedSchemaPruning.enabled=true` |

## Configs

| Config | Default | Note |
|---|---|---|
| `spark.sql.parquet.filterPushdown` | `true` | sparkopt sets it; already on |
| `spark.sql.parquet.columnarReaderBatchSize` | `4096` | sparkopt sets it; already the default |
| `spark.sql.optimizer.nestedSchemaPruning.enabled` | `true` (3.0+) | prune nested struct fields |
| `spark.sql.parquet.aggregatePushdown` | `false` (3.3+) | push `MIN/MAX/COUNT` into Parquet footers |
| `spark.sql.files.maxPartitionBytes` | `128MB` | split size of scan partitions |

## Gotchas / troubleshooting

| Symptom | Likely cause | Action |
|---|---|---|
| `PushedFilters: []` | UDF or function around the column | rewrite the predicate as a plain comparison |
| Filter pushed but scan still reads everything | data not sorted/clustered on the column, so every row group's min/max range overlaps | sort within partitions on write, or Z-order (Delta) |
| `ReadSchema` still wide | `df.cache()` / `toPandas()` / `collect()` on the wide DataFrame before `select` | select first, then cache |
| Partition column filter in `DataFilters` only | column name or type mismatch with directory layout | check `PartitionFilters`; filter on the exact partition column |
| CSV/JSON source | no row-group stats | convert to Parquet/Delta once |
| Pushdown fine, still slow | many tiny files | see `coalesce_write.md`, `delta_zorder.md` |

## Related cases

- `delta_zorder`: clusters data so that pushed filters actually skip files.
- `coalesce_write`: fewer, larger files mean fewer footers to read.
- `adaptive_qe`: shuffle-side tuning once the scan is lean.
- `spark-plan-analysis` skill: reading `FileScan` lines and why `estimated_reuse` keys on them.
