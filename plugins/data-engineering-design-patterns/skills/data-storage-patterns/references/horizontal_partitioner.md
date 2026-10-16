# Horizontal Partitioner

> Category: data storage, partitioning. Batch and streaming, file systems, table formats, RDBMS, Kafka.

## Problem

A batch job computes rolling aggregates over the previous four days. It ran fine for months, but
as more data arrived the job slowed down. Most of the time goes into the filter that drops records
older than four days. More compute hid the problem for a while and raised the bill. You need the
original cost and a runtime that stays flat as new data is ingested.

Root cause: an incremental job reads the **whole** dataset to use only a **slice** of it.

## Solution / Mechanics

The rolling aggregate is incremental processing over part of the dataset, which is the textbook
case for the Horizontal Partitioner.

1. **Choose the partitioning attribute** (the *distribution key*). Each distinct value gets a
   physically isolated storage space: a directory, a child table, a topic partition.
2. **Decide where the time comes from** if the key is time-based:
   - *job execution context*: every record of a run lands in the run-date partition
     (`2024-12-31` run -> one partition);
   - *the dataset* (event time): semantically correct, but late data means a run writes to
     several partitions.
3. **Business keys and nesting** are valid too: customer ID, partner ID, region, or nested
   `event_time/country`.
4. **Declare it** in one of two ways:
   - *declarative / store-side*: `CREATE TABLE ... PARTITIONED BY` (Databricks, BigQuery),
     PostgreSQL `PARTITION BY RANGE`. The producer doesn't need to know the layout;
   - *producer-side*: Spark `partitionBy(col)`, Kafka custom partitioner. The producer computes
     the value, possibly from a complex expression.
5. **Read with a predicate on the key** so the engine prunes all other partitions.
6. **Use partition metadata** where the store exposes it: BigQuery `INFORMATION_SCHEMA.PARTITIONS`,
   Databricks `DESCRIBE TABLE EXTENDED`, Iceberg `SELECT * FROM cat.ns.table.partitions`.

```
 writer (partition value = f(record) or f(run))
     |
     v
 visits/
 +-- year=2024/month=12/day=28/   <-- skipped
 +-- year=2024/month=12/day=29/   <-- read  \
 +-- year=2024/month=12/day=30/   <-- read   |  WHERE day BETWEEN 28+1 AND 31
 +-- year=2024/month=12/day=31/   <-- read  /   (partition pruning: the engine lists and
 +-- year=2024/month=12/day=27/   <-- skipped    opens only matching directories)
```

Partitions are also an **idempotency** building block: a rerun overwrites or truncates exactly
one partition (Fast Metadata Cleaner, Data Overwrite).

Partitioning vs sharding: sharding splits data across *machines*; it is a
special case of horizontal partitioning at the hardware layer.

## Consequences & trade-offs

| Consequence  | What happens | Mitigation |
| --- | --- | --- |
| Granularity and metadata overhead | Partitioning visits by username creates one million partitions per day: slow listing, small files, metastore limits | Low-cardinality keys only (day, hour, country); Bucket for high-cardinality columns |
| Skew | Partitions aren't balanced; in micro-batch streaming the largest partition sets the batch duration and blocks the others | Backpressure buffer: defer extra records of the hot partition to the next micro-batch. Latency rises only for that partition |
| Mutability | Changing the key means moving all written data | Iceberg partition evolution: metadata-only, old files keep the old layout, new writes use the new one |
| Static layout | The partition scheme is fixed at write time and fits only the access patterns known then | Combine with Sorter / Metadata Enhancer for other predicates |

## Key design decisions

- **Attribute**: the column present in most predicates of incremental jobs and queries.
- **Event time vs execution time**: correctness and late data vs simple single-partition reruns.
- **Granularity**: hour vs day vs month. Aim for partitions of hundreds of MB to a few GB.
- **Store-managed vs producer-managed** partition values (hidden partitioning vs explicit columns).
- **Kafka**: keep the default key-hash partitioner unless routing is a real requirement.

## When to use / when not

| Situation | Use Horizontal Partitioner? | Alternative |
| --- | --- | --- |
| Incremental jobs read recent time ranges | Yes, partition by event or run date | - |
| Filters on a low-cardinality business attribute (country, tenant) | Yes, possibly nested under date | - |
| Filters/joins on a high-cardinality id (user_id, device_id) | No, too many partitions | Bucket |
| Need to skip blocks *inside* a partition | Not enough on its own | Sorter, Metadata Enhancer |
| Groups of columns need different retention/access | No, wrong axis | Vertical Partitioner |
| Idempotent daily reprocessing | Yes, overwrite one partition | Fast Metadata Cleaner, Data Overwrite |

## Gotchas & troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Query still scans everything | Predicate on an expression (`date(event_time)`) instead of the partition column | Filter on the partition column itself, or use generated/hidden partitions |
| Thousands of tiny files per partition | Key too fine-grained or many small micro-batches | Coarser key, Compactor, `repartition(partition_cols)` before write |
| Rerun deleted the whole table | Spark static overwrite mode | `spark.sql.sources.partitionOverwriteMode=dynamic` or Delta `replaceWhere` |
| PostgreSQL insert fails "no partition found" | Child partition for the day missing, or bound `23:59:59` leaves a gap | Create partitions ahead of load; use exclusive upper bound `[day, next_day)` |
| Range predicates awkward | Separate `year/month/day` ints | A single `event_date` column is often simpler |
| One streaming task always slow | Skewed key value | Backpressure buffer, salting, or a different key |

## Implementation notes per stack

**Spark** - `examples/horizontal_partitioner/spark/spark_horizontal_partitioner.py`: derives
partition columns from event time, writes Delta with `partitionBy`, checks pruning in `explain()`,
and overwrites a single partition with `replaceWhere`.

```python
partitioned = (visits
               .withColumn("year", F.year("event_time"))
               .withColumn("month", F.month("event_time"))
               .withColumn("day", F.dayofmonth("event_time"))
               .withColumn("hour", F.hour("event_time")))
(partitioned.write.mode("overwrite").format("delta")
 .partitionBy("year", "month", "day", "hour").save(output_dir))
last_4_days = (spark.read.format("delta").load(output_dir)
               .filter("year = 2024 AND month = 12 AND day BETWEEN 28 AND 31"))
last_4_days.explain()  # look for PartitionFilters in the scan node
```

**Kafka** - `examples/horizontal_partitioner/kafka/kafka_range_partitioner.py`: Python port of the
a Java `RangePartitioner`. confluent-kafka has no pluggable Python
partitioner class, so the routing is computed and passed as `partition=`. In Java you register the
class with `partitioner.class`.

```python
RANGES_PER_PARTITION = {"A": 0, "B": 0}
DEFAULT_PARTITION = 1

def range_partition(key: str) -> int:
    return RANGES_PER_PARTITION.get(key, DEFAULT_PARTITION)

producer.produce(topic, key=key, value=json.dumps(event),
                 partition=range_partition(key), on_delivery=delivery_report)
```

**Airflow** - `examples/horizontal_partitioner/airflow/horizontal_partitioner_dag.py`: daily
TaskFlow DAG over a PostgreSQL `PARTITION BY RANGE(event_time)` parent. It
creates the child partition for the data interval, truncates it (rerun-safe), and loads the day.

```python
PostgresHook(postgres_conn_id=CONN_ID).run(f"""
    CREATE TABLE IF NOT EXISTS dp.visits_all_{ds_nodash}
    PARTITION OF dp.visits_all
    FOR VALUES FROM ('{data_interval_start}') TO ('{data_interval_end}');
    TRUNCATE dp.visits_all_{ds_nodash};""")
```

## Related patterns

- **Bucket**: the answer when the filter column has too high a cardinality for partitions.
- **Vertical Partitioner**: splits columns instead of rows.
- **Sorter**: orders rows inside each partition so block-level skipping works.
- **Metadata Enhancer**: file and row-group statistics skip data within partitions.
- **Fast Metadata Cleaner / Data Overwrite** (idempotency patterns): use partitions as the unit of idempotent reruns.
- **Compactor** (data ingestion patterns): fixes the small-files problem that over-partitioning creates.
- **Late Data patterns** (error management patterns): event-time partitioning must handle records for past partitions.
