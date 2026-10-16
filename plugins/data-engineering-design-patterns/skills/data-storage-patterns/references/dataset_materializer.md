# Dataset Materializer

> Category: read path optimization. Batch (materialized views, tables) and streaming (materialized aggregates).

## Problem

Consumers need "the last three weeks" of a dataset that lives in several partitioned (weekly) tables.
You give them a single access point as a view, but the view reruns its UNION/JOIN query on every
read. Latency is high and the same costly computation runs again and again.
You need one point of access that reads fast.

Root cause: a regular view stores only the *query*. Each reader pays the full cost of shuffles,
joins and CPU-heavy transformations, every time.

## Solution / Mechanics

Run the expensive query once and **store its result**. Data duplication buys read performance.

1. **Identify** the datasets to materialize: slow (shuffle, join, heavy UDF) and read often.
2. **Write the defining query**: `SELECT` with `UNION` / `JOIN` / `GROUP BY` over the sources.
3. **Pick the storage**:
   - **Materialized view** (MV): the database owns refreshes. Manual (`REFRESH MATERIALIZED VIEW`)
     or automatic (Redshift `AUTO REFRESH YES`; also BigQuery, Databricks, Snowflake). Automatic
     refresh is *not* immediate: it depends on database load and refresh size, so it is less
     predictable.
   - **Table**: you own the refresh (scheduled job, no auto refresh). In exchange the table can use
     partitions, buckets and sorting, which MVs may not support.
4. **Refresh**: either a full rerun of the query, or **incremental**: take only rows added since the
   last run and merge them. That combines **Incremental Loader + Merger**.

```
           every read (plain view)                       Dataset Materializer
 reader --> view --> UNION/JOIN/GROUP BY over      reader --> materialized result (MV / table)
                     week_1, week_2, week_3                         ^
                     (full cost each read)                          | refresh (full or incremental)
                                                                    |
                                     week_1, week_2, week_3 --> [defining query once]
                                                                    |
 incremental: rows WHERE insertion_time > last_watermark --> GROUP BY --> MERGE (count + new)
```

## Consequences & trade-offs

| Consequence  | What happens | Mitigation |
| --- | --- | --- |
| Refresh cost | A full refresh reruns the costly setup query and takes resources from users of other tables | Incremental refresh for insert-only sources; off-peak schedules |
| Partial incremental support | Databricks and BigQuery support incremental refresh, but not for every SQL operation; sometimes they still refresh everything | Keep MV queries to supported operations, or use a table + your own MERGE |
| Unpredictable auto refresh | BigQuery aims to refresh within ~5 min of a base change, later when capacity is short | Manually refreshed MV or table when freshness must be deterministic |
| Data access | The result combines several tables, so retention and access rules are hard to keep consistent | Deny the MV to anyone denied a source table, or use Fine-Grained Accessor |
| Storage overhead | Materialization duplicates data | Mixed implementation: materialize only the costly parts and keep the rest as recomputed views |
| Staleness | Readers see the state as of the last refresh | Expose a refresh timestamp; trigger refresh on upstream arrival |

## Key design decisions

- **MV vs table**: automatic but constrained, or flexible (layout patterns, any SQL) but self-managed.
- **Full vs incremental**: incremental only when the source is insert-only and `insertion_time`
  (write time, not event time) exists. Updates/deletes need CDC-aware merges or periodic full refresh.
- **Trigger**: cron, upstream data arrival (sensor / dataset scheduling), or on demand.
- **Idempotency of incremental merges**: `count = count + input.visits` double-counts on a replay.
  Track processed intervals or source versions transactionally with the merge.
- **Scope**: which parts of a multi-source view to materialize (mixed materialization).

## When to use / when not

| Situation | Use Dataset Materializer? | Alternative |
| --- | --- | --- |
| Same expensive query read many times a day | Yes | - |
| Query cheap, or read rarely | No | Plain view |
| Need real-time freshness on fast-changing base tables | Only with incremental/streaming refresh | Query the base tables; streaming aggregate |
| Result needs partitions/sort keys for its readers | Yes, as a **table** | MV if the store supports clustering |
| Main cost is joins of a normalized model | Often | Denormalizer (One Big Table is a materialization) |
| Main cost is file listing or scanning irrelevant files | No | Manifest, Metadata Enhancer, Sorter |

## Gotchas & troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| PostgreSQL readers blocked during refresh | Plain `REFRESH` takes an exclusive lock | `REFRESH ... CONCURRENTLY` (needs a unique index and an already populated MV) |
| `CONCURRENTLY` fails on first run | MV created `WITH NO DATA` | First refresh without `CONCURRENTLY` (see the DAG) |
| Counter inflated after a retry | Non-idempotent `count + x` MERGE replayed | Run-marker table in the same transaction, or source-version tracking |
| Late events never counted | Watermark on event time | Watermark on `insertion_time` (write time) |
| Incremental MV silently does full refresh | Unsupported operation in MV query | Check the store's incremental refresh limitations |
| Users read data they can't see in sources | ACL not propagated to MV | Mirror grants; Fine-Grained Accessor |

## Implementation notes per stack

**Spark** - `examples/dataset_materializer/spark/spark_dataset_materializer.py`: full refresh
(UNION of weekly Delta tables, aggregate to a table) and incremental refresh (watermark on
`insertion_time` + Delta `MERGE`, in PySpark).

```python
new_rows = spark.read.format("delta").load(source).filter(F.col("insertion_time") > F.lit(last))
new_max = new_rows.agg(F.max("insertion_time")).first()[0]
if new_max is None:
    return
increment = new_rows.groupBy("user_id").agg(F.count("*").alias("visits"))
(DeltaTable.forPath(spark, target).alias("target")
 .merge(increment.alias("input"), "target.user_id = input.user_id")
 .whenMatchedUpdate(set={"visits": "target.visits + input.visits"})
 .whenNotMatchedInsertAll()
 .execute())
```
The watermark is written after the MERGE. A crash in between double-counts (flagged with a
`ponytail:` comment). Track the Delta source version if exactly-once matters.

**Kafka** - `examples/dataset_materializer/kafka/kafka_materialized_counter.py`: a per-user counter
materialized into a compacted topic (`visits_per_user`). Only changed keys are republished
(incremental refresh). State is rebuilt from the topic on restart, and offsets are committed after
the flush.

```python
if dirty and time.monotonic() - last_flush > FLUSH_EVERY_S:
    for user_id in dirty:  # incremental refresh: only changed keys
        producer.produce(VIEW_TOPIC, key=user_id,
                         value=json.dumps({"user_id": user_id, "visits": counts[user_id]}))
    producer.flush()
    consumer.commit(asynchronous=False)
    dirty.clear()
```

**Airflow** - `examples/dataset_materializer/airflow/dataset_materializer_dag.py`: a PostgreSQL MV
over three weekly tables, refreshed `CONCURRENTLY` once populated. Also an interval-scoped
incremental `MERGE` (PostgreSQL 15+) guarded by a run-marker row inserted in the same transaction.

```python
with conn, conn.cursor() as cur:  # one transaction: MERGE + run marker
    cur.execute("INSERT INTO dp.visits_counter_runs VALUES (%s) ON CONFLICT DO NOTHING",
                (data_interval_start,))
    if cur.rowcount == 0:
        raise AirflowSkipException("interval already merged")
    cur.execute("""MERGE INTO dp.visits_counter AS target USING (...) AS input ...""")
```
Cloud equivalents: BigQuery `CREATE MATERIALIZED VIEW ... OPTIONS (enable_refresh = true,
refresh_interval_minutes = 15)`, Redshift `AUTO REFRESH YES`.

## Related patterns

- **Incremental Loader**: supplies the "only new rows" half of the incremental refresh.
- **Merger**: supplies the `MERGE` half that combines new aggregates with existing ones.
- **Denormalizer**: a One Big Table is a materialized join of a normalized model.
- **Vertical Partitioner**: expose its split tables recombined through a materialized view.
- **Fine-Grained Accessor**: keeps access control consistent on the combined result.
- **Horizontal Partitioner / Bucket / Sorter**: only a table-based materialization can use them.
- **Readiness Marker / External Trigger**: trigger refreshes on upstream completion instead of cron.
