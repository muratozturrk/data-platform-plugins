# delta_zorder: cluster a Delta table for data skipping

## Problem

Delta Lake stores per-file min/max statistics in the transaction log. A query with a selective predicate
can skip every file whose range cannot match, but only if the data is **clustered**: when values of
`user_id` are spread randomly across files, every file's range is roughly `[min, max]` of the whole
table and nothing is skipped. Z-ordering rewrites files so that rows with nearby values of 1-4 chosen
columns land in the same files (a space-filling curve), which keeps each file's range narrow.

## How Spark executes it

`OPTIMIZE ... ZORDER BY` is a batch rewrite: it reads the selected files, range-partitions them by the
interleaved Z-value of the columns, writes new compacted files and commits a `remove`/`add` set to the
Delta log. Readers keep working on the old snapshot during the rewrite.

The signals that you need it:

- `df.explain()` shows a `FileScan parquet` (Delta reads Parquet) with `PushedFilters` present, yet the
  scan is slow.
- On Databricks the SQL UI / `EXPLAIN` metrics show `number of files pruned` close to 0, or the Spark UI
  scan node shows "files read" equal to the total file count.
- `DESCRIBE DETAIL t` reports `numFiles` far above `sizeInBytes / 256MB` (small files: compaction helps
  even without Z-order).

## sparkopt rule

```python
class DeltaZOrderCase(OptimizationCase):
    name = "delta_zorder"
    priority = Priority.LOW
    def __init__(self, zorder_cols=None):
        self._cols = zorder_cols or ["event_date", "user_id"]
    def matches(self, profile): return profile.is_large
# DataProfile.is_large: estimated_bytes >= 10 * 1024**3   (10 GiB)
```

- **Matches:** `estimated_bytes >= 10 GiB`. sparkopt computes `estimated_bytes` as
  `row_count x sum(per-type width)` with fixed widths (string = 32 B, binary = 64 B, long = 8 B...),
  so the 10 GiB figure is a rough sparkopt heuristic, not a measured size. The case also fires for
  non-Delta tables; exclude it with `--exclude delta_zorder` when you are not on Delta.
- **Generates:** a note (not code that runs automatically):
  `DeltaTable.forPath(spark, "/path/to/delta_table").optimize().executeZOrderBy("event_date", "user_id")`.
- **Corrections:**
  1. `["event_date", "user_id"]` is a placeholder. Replace it with the columns that your queries filter on.
  2. Delta **rejects Z-ordering on a partition column**, and `event_date` is very often the partition
     column. Partitioning already gives skipping on it; Z-order the *other* filter columns.
  3. The library's "80%+ data skipping" claim is not a guarantee. The real gain depends on the
     cardinality, the selectivity of the predicate and how many columns share the curve.

## Before / After

```python
# BEFORE: 40k small files, user_id scattered, every query scans all of them
spark.read.format("delta").load(path).filter("user_id = 42").count()

# AFTER: compact + cluster; limit to recent partitions to bound the rewrite cost
from delta.tables import DeltaTable
(DeltaTable.forPath(spark, path)
   .optimize()
   .where("event_date >= '2024-01-01'")      # partition predicate only
   .executeZOrderBy("user_id"))

# SQL equivalent
spark.sql(f"OPTIMIZE delta.`{path}` WHERE event_date >= '2024-01-01' ZORDER BY (user_id)")
```

## Configs and requirements

| Item | Value | Note |
|---|---|---|
| Delta Lake OSS | 2.0+ for `optimize().executeZOrderBy` | `delta-spark` matching your Spark version |
| Session configs | `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`, `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog` | set in the builder |
| `delta.dataSkippingNumIndexedCols` | `32` | stats are only collected for the first N columns; Z-order columns must be among them |
| `spark.databricks.delta.optimize.maxFileSize` | `1GB` | target size of compacted files |
| Liquid clustering | Delta 3.1+ / Databricks: `CLUSTER BY (col)` | incremental replacement for Z-order + partitioning |

## Choosing columns

- Use columns that appear in `WHERE` / join keys of frequent, selective queries.
- Use high cardinality (ids, timestamps). Low-cardinality columns belong in partitioning, or nowhere.
- Use 1-4 columns: each extra column dilutes the locality of the others.
- Don't use partition columns (that raises an error) or columns without stats.

## Gotchas / troubleshooting

| Symptom | Cause | Action |
|---|---|---|
| `AnalysisException ... partition column` | Z-order includes a partition column | drop it from the list |
| `Z-Ordering on [c] will be ineffective, no statistics` | column beyond `dataSkippingNumIndexedCols` | move the column or raise the setting, then rewrite |
| OPTIMIZE runs for hours | whole table rewritten | add `.where(<partition predicate>)`, schedule incrementally |
| No skipping after OPTIMIZE | predicate wraps the column in a function | compare the raw column (see `parquet_pushdown.md`) |
| Concurrent writers conflict | OPTIMIZE vs. writes to the same partitions | run on closed partitions, or use liquid clustering |
| Not a Delta table | case still fires on size | `--exclude delta_zorder`; for plain Parquet, `sortWithinPartitions` on write |

## Related cases

- `parquet_pushdown`: needed so that the filter reaches the scan at all.
- `coalesce_write`: avoids creating small files in the first place.
- `adaptive_qe`: unrelated to skipping, but shrinks the shuffle that follows the scan.
