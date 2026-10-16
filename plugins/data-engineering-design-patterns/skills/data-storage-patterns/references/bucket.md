# Bucket

> Category: data storage, records organization. Batch; Kafka partitions are the streaming analogue.

## Problem

A business attribute is in the predicate of most queries (80% of operations). You wanted it as
the partition column, but its cardinality is too high: too many partitions, eventually hitting the
data store's metadata limits. You still want to optimize storage for it.

Example: a unique `user_id`, used in filters and joins.

## Solution / Mechanics

Partitioning colocates rows that share **one value**. Bucketing colocates **groups of values** in
a fixed number of containers.

1. **Analyze the data** and choose the bucket column(s). If the dataset is already partitioned,
   bucket columns act as a secondary grouping key under the partition key.
2. **Choose the number of buckets** from the key's cardinality: more buckets means more, smaller
   buckets; fewer means fewer, bigger ones.
3. **Assign records** with modular hashing: `bucket = hash(key) % buckets_number`.
4. **Readers benefit** in two ways:
   - **bucket pruning**: an equality predicate on the bucket column is hashed by the engine,
     which reads only the matching bucket;
   - **shuffle elimination**: a join between tables bucketed identically (same column, same count,
     same hash) loads matching buckets into the same task, with no network exchange.

```
 users table (bucketBy 4, user_id)        visits table (bucketBy 4, user_id)
 +---------+---------+---------+---------+  +---------+---------+---------+---------+
 | b0      | b1      | b2      | b3      |  | b0      | b1      | b2      | b3      |
 +----+----+----+----+----+----+----+----+  +----+----+----+----+----+----+----+----+
      |         |         |         |            |         |         |         |
      +---------|---------|---------|------------+         |         |         |
      task 0    +---------|---------|----------------------+         |         |
                task 1    +---------|--------------------------------+         |
                          task 2    +------------------------------------------+
                                    task 3          join b_i with b_i: no shuffle

 WHERE user_id = '42'  ->  hash('42') % 4 = 2  ->  read only b2 (bucket pruning)
```

Apache Hive popularized bucketing; Spark and AWS Athena implement it too.

## Consequences & trade-offs

| Consequence  | What happens | Mitigation |
| --- | --- | --- |
| Mutability | The bucketing schema is immutable in practice. Changing the column or count is possible but needs a backfill | Choose carefully up front; treat the count as a contract (drift check) |
| Bucket size | Sizing on today's volume gives big buckets later; sizing for predicted growth may create too many small ones now | Pick one of the two and plan a rebucketing backfill; or use evolvable clustering (liquid clustering, Iceberg sort orders) |
| Static data | Like partitions, the layout is fixed at write time | Combine with Sorter inside buckets (`sortBy`) |
| Write cost | The writer must hash-distribute records, i.e. a shuffle at write time | Paid once per write, saved on every join/filter |

## Key design decisions

- **Column**: high-cardinality, used in equality filters or join keys (user_id, device_id).
- **Count**: about table size / target file size per partition. All tables joined together need
  the **same** count (or multiples, depending on the engine).
- **Hash compatibility**: Spark's murmur3 vs Hive's hash. Athena needs
  `'bucketing_format' = 'spark'` to read Spark-written buckets.
- **Sort within buckets** (`sortBy`): cheap extra skipping, and enables sort-merge without a sort.
- **Combined with partitions**: partition by date, bucket by user_id within each date.

## When to use / when not

| Situation | Use Bucket? | Alternative |
| --- | --- | --- |
| Frequent equality filter on a high-cardinality column | Yes | Sorter + Metadata Enhancer (range skipping) |
| Big-to-big joins on the same key, repeated often | Yes, bucket both sides identically | Broadcast join if one side is small |
| Low-cardinality filter (date, country) | No | Horizontal Partitioner |
| Range predicates (`BETWEEN`) on the column | Weak, hashing destroys order | Sorter |
| Volume grows unpredictably, layout must evolve | Risky | Liquid clustering / Z-order (Sorter) |
| Engine doesn't support bucketing on writes (Athena INSERT) | Write with Spark/Hive, read with Athena | - |

## Gotchas & troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `bucketBy` raises "not supported for save" | Path-based `save()` | Use `saveAsTable` (metastore table) |
| Join still has `Exchange hashpartitioning` | Different bucket counts or columns, bucketing disabled, or broadcast/AQE changed the plan | Same column and count on both sides; `spark.sql.sources.bucketing.enabled=true` |
| Athena `INSERT INTO` fails on bucketed table | Athena applies bucketing only logically on reads | Write files with Spark, register with `CLUSTERED BY ... INTO n BUCKETS` |
| Athena ignores Spark buckets | Missing `bucketing_format` | `TBLPROPERTIES ('bucketing_format' = 'spark')` |
| Too many small files | buckets x write tasks files per write | `repartition(n, col)` before `bucketBy`; fewer buckets |
| Pruning not used | Predicate on an expression or wrong type (`user_id = 42` on a string) | Literal of the column's exact type |
| Kafka join wrong after scaling | Partition count changed, keys remapped | Partition counts are bucket counts: never change them in place |

## Implementation notes per stack

**Spark** - `examples/bucket/spark/spark_bucket.py`: two tables bucketed identically on `user_id`
(count from `--buckets`, default 8; `bucket_dag.py` passes 50). Bucket pruning appears in the
filter's `explain()`, and the join plan has no `Exchange`.

```python
(visits.write.mode("overwrite").bucketBy(buckets, "user_id").sortBy("user_id")
 .saveAsTable("visits_bucketed"))
(users.write.mode("overwrite").bucketBy(buckets, "user_id").sortBy("user_id")
 .saveAsTable("users_bucketed"))
# Bucket pruning: look for "SelectedBucketsCount: 1 out of <buckets>" in the scan node.
spark.table("visits_bucketed").filter("user_id = '42'").explain()
joined = spark.table("visits_bucketed").join(spark.table("users_bucketed"), "user_id")
plan = joined._jdf.queryExecution().executedPlan().toString()
print("shuffle present:", "Exchange hashpartitioning" in plan)
```

**Kafka** - `examples/bucket/kafka/kafka_copartitioning_check.py`: a topic partition is a bucket
(`murmur2(key) % partitions`). Two topics with the same key and partition count are
co-partitioned, so they join locally. The script checks partition counts and that a key lands on
the same partition number in both topics.

```python
counts = partition_counts(AdminClient({"bootstrap.servers": BOOTSTRAP}), [left, right])
if counts[left] != counts[right]:
    raise SystemExit(f"NOT co-partitioned: {counts}. Recreate with equal partition counts.")
producer = Producer({"bootstrap.servers": BOOTSTRAP, "partitioner": "murmur2_random"})
```

**Airflow** - `examples/bucket/airflow/bucket_dag.py`: registers the Athena external table, submits the Spark writer with the same `BUCKETS` value, then checks the Glue
table's `NumberOfBuckets` for drift.

```python
ATHENA_DDL = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS dp.visits_bucketed (
  visit_id STRING, user_id STRING, page STRING, event_time TIMESTAMP)
CLUSTERED BY (`user_id`) INTO {BUCKETS} BUCKETS
STORED AS PARQUET
LOCATION 's3://dp-lake/visits_bucketed/'
TBLPROPERTIES ('bucketing_format' = 'spark')
"""
```

## Related patterns

- **Horizontal Partitioner**: buckets solve its high-cardinality limit; use both (partition, then bucket).
- **Sorter**: sorting inside buckets (`sortBy`) adds range skipping; Z-order is the evolvable alternative.
- **Distributed Aggregator** (data value patterns): the shuffle that bucketed joins eliminate.
- **Normalizer**: bucketing large entity and fact tables on the join key keeps normalized joins local.
- **Compactor** (data ingestion patterns): bucket count x tasks can create small files that need compaction.
