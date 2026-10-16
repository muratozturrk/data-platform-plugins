# Sorter

> Category: data storage, records organization. Batch; table formats, data warehouses.

## Problem

You store data in weekly tables to use the Fast Metadata Cleaner. Maintenance got easier, but
queries didn't get faster. You want to keep that idempotency strategy and still cut access latency.
You know the queries: most of them **filter or sort by event time**.

## Solution / Mechanics

If you know which columns queries filter or sort on, store rows in that order.

1. **Identify the sorting column(s)** from real query predicates and `ORDER BY`s.
2. **Declare them** in the table definition (`CLUSTER BY`, sort keys), or sort at write time.
3. **The database keeps written rows ordered**, so each data block covers a narrow value range.
4. **Readers skip blocks** whose min/max range can't match the predicate, using the metadata
   attached to each block (see Metadata Enhancer).
5. **Pick lexicographic or curved order**:
   - *lexicographic*: sort by col1, then col2. Good when the leading column is filtered;
   - *curved sorts* such as **Z-order**: colocate rows in multi-dimensional space, so a predicate
     on any of the columns skips blocks. Lexicographic order reads 9 blocks where
     Z-order reads 7 for the same two-column predicate.

```
 unsorted files                  sorted by visit_time
 f1 [08:00 .. 23:59]             f1 [08:00 .. 11:59]   <- skipped
 f2 [08:10 .. 23:50]             f2 [12:00 .. 15:59]   <- read
 f3 [08:05 .. 23:58]             f3 [16:00 .. 19:59]   <- skipped
 WHERE visit_time BETWEEN 13:00 AND 14:00
 -> every file overlaps         -> one file overlaps (footer min/max decides)

 lexicographic (x, y)            Z-order (x, y)
 x=1: y1 y2 y3 y4                interleaves bits of x and y: blocks cover small
 x=2: y1 y2 y3 y4                squares of the (x, y) space, so a filter on y alone
 filter on y alone: all blocks   still skips blocks
```

Where it exists: Delta Lake and Iceberg (Z-order), Amazon Redshift (interleaved sort
keys, Z-curve based), BigQuery and Snowflake (clustered tables, classical sort). Z-order is filed
under Sorter, not under clustering, because it really sorts data on disk.

## Consequences & trade-offs

| Consequence  | What happens | Mitigation |
| --- | --- | --- |
| Writer cost | Presorting helps readers but slows writers | Sort off the critical path (scheduled OPTIMIZE) |
| Unsorted segments | Sorting isn't instant; new writes create unsorted blocks with no skipping | Schedule re-sorts in the writer job (longer runtime) or outside it (maintenance job) |
| Composite sort keys | With lexicographic keys, queries must reference the leading columns; a filter only on `page` in `(visit_time, page)` scans most blocks | Order keys by predicate frequency; use Z-order for independent predicates |
| Mutability | Changing sort keys may re-sort the entire table | Budget the rewrite; prefer engines with incremental re-clustering |

## Key design decisions

- **Columns**: those in most `WHERE`/`ORDER BY` clauses. A few columns only; Z-order loses
  effectiveness beyond 3-4 columns.
- **Lexicographic vs Z-order**: one dominant leading predicate -> lexicographic; several
  independent predicates -> Z-order.
- **When to sort**: at write time (`sortWithinPartitions`, sort keys) vs scheduled compaction
  (`OPTIMIZE ... ZORDER BY`). Trade writer latency against freshness of the layout.
- **One layout per table**: a Z-order rewrite replaces an earlier lexicographic order. Choose
  from the dominant access pattern.
- **Scope**: sort within partitions (Horizontal Partitioner) so re-sorts touch recent partitions only.

## When to use / when not

| Situation | Use Sorter? | Alternative |
| --- | --- | --- |
| Range filters or sorts on a known column (event_time) | Yes, lexicographic | - |
| Filters on any of 2-4 columns independently | Yes, Z-order | - |
| Equality filter/join on high-cardinality id | Possible | Bucket (pruning + shuffle-free joins) |
| Low-cardinality filter | Not first choice | Horizontal Partitioner |
| Write-heavy table, rarely queried | No, writer cost without payoff | - |
| Kafka topic | Not applicable (offset order only) | Sink to a table format and sort there |

## Gotchas & troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Filter on second sort column scans everything | Lexicographic composite key, leading column not filtered | Add the leading column to the predicate or switch to Z-order |
| Skipping degrades over days | Appends created unsorted segments | Scheduled `OPTIMIZE`/re-clustering |
| Event-time skipping gone after OPTIMIZE | Z-order on other columns rewrote the files | Include the column in the Z-order set, or keep one layout |
| Re-sort job very expensive | Whole-table rewrite | Restrict to recent partitions (`OPTIMIZE ... WHERE`) |
| Delta stats not used for a column | Column outside the first 32 indexed columns | Reorder the schema or set `delta.dataSkippingNumIndexedCols` |
| Sorted write slow | Global `orderBy` shuffle | `sortWithinPartitions` when per-file order is enough |

## Implementation notes per stack

**Spark** - `examples/sorter/spark/spark_sorter.py`: writes Delta files sorted by `event_time`
(`sortWithinPartitions`) and queries them while that layout holds. Then it Z-orders on
`(visit_id, page)` and queries on either column. `--optimize-only` is the
scheduled re-sort for unsorted segments.

```python
(visits.repartition(10, "page")           # 10 files...
 .sortWithinPartitions("event_time")      # ...each internally sorted
 .write.mode("overwrite").format("delta").save(output_dir))
table().filter("event_time BETWEEN '2024-07-01 01:00:00' AND '2024-07-01 02:00:00'").show(3)
# Z-order REWRITES the files: the event_time order above is replaced by the curve.
DeltaTable.forPath(spark, output_dir).optimize().executeZOrderBy(["visit_id", "page"])
table().filter("page = 'page_7'").count()  # either Z-ordered column alone skips files
```

**Kafka** - `examples/sorter/kafka/README.md`: no code on purpose. A partition is ordered by
offset only. Per-key order comes from keying, time-based skipping from the broker's time index
(`offsets_for_times`, see Metadata Enhancer), and sorted analytics storage from a downstream sink
that applies the Sorter.

**Airflow** - `examples/sorter/airflow/sorter_dag.py`: a BigQuery clustered table that helps predicates on `visit_id`, or `visit_id` + `page`, but not `page` alone.
A nightly `SparkSubmitOperator` runs the Z-order maintenance outside the write path.

```python
CLUSTERED_DDL = """
CREATE TABLE IF NOT EXISTS `dp.visits.raw_visits` (
  visit_id STRING, event_time TIMESTAMP, user_id STRING, page STRING)
PARTITION BY DATE(event_time)
CLUSTER BY visit_id, page
"""
zorder_delta_table = SparkSubmitOperator(
    task_id="zorder_delta_table", application="/opt/jobs/spark_sorter.py",
    application_args=["/lake/visits_sorted", "--optimize-only"],
    packages="io.delta:delta-spark_2.12:3.2.0", conn_id="spark_default")
```

## Related patterns

- **Metadata Enhancer**: sorting only pays off through per-block min/max statistics.
- **Horizontal Partitioner / Fast Metadata Cleaner**: partitions (or weekly tables) give coarse
  pruning; the Sorter adds fine-grained skipping inside them.
- **Bucket**: a hash-based alternative for equality predicates on high-cardinality keys.
- **Compactor** (data ingestion patterns): Z-order is applied while compacting files, so the two jobs merge.
