# Local Aggregator

> Category: data aggregation without shuffle. Streaming and batch.

## Problem

A streaming job builds windows over visits stored in a partitioned broker. The data volume is
static and you expect no sudden changes in the partitioning, so the partition count will never
change. You want to optimize the job by removing the grouping shuffle that the framework adds
automatically.

## Solution / Mechanics

Three factors favor the Local Aggregator over the Distributed Aggregator: a costly shuffle, static
partitioning of the source, and related records stored together. If every record of
a grouping key is already in the same input partition, aggregate inside that partition. The only
network exchange left is reading the input.

Preconditions:

1. **Producer side**: a static per-record partitioning key, so a key always goes to the same
   physical partition. Most of the implementation effort is here.
2. **Storage side**: an immutable number of partitions.
3. **Consumer side**: grouping by that same key.

A side benefit is that tasks are **fully isolated**. A task doesn't wait for other tasks' data, so
one slow partition does not delay the others. This matters most for streaming.

How the tools expose it:

| Tool | Mechanism |
| --- | --- | --- |
| Kafka Streams | `groupByKey()` uses each record's key as-is, with no repartition topic (unlike `groupBy()`) |
| Apache Spark | No "trust my partitioning" hint. Use per-partition operations: `sortWithinPartitions` + `foreachPartition` / `mapPartitions` |
| Spark / Hive tables | Tables bucketed on the same key with the same bucket count skip the Exchange |
| AWS Redshift | `DISTSTYLE KEY DISTKEY(col)` colocates by key; `DISTSTYLE ALL` copies a small table to every node |
| Small dataset | Load it all into one node and aggregate in shared memory; partitioning doesn't matter |

Spark flow:

```
 Kafka topic "visits" (12 partitions, key = visit_id, fixed)
   p0: v1 v7 v1 v9 v7 ...      p1: v2 v2 v5 ...      ...      p11: ...
    |                           |                              |
    |  1 Spark partition per topic partition (no minPartitions, no repartition)
    v                           v                              v
 sortWithinPartitions(visit_id, event_time)          <-- local sort, no Exchange
   v1 v1 v7 v7 v9               v2 v2 v5
    |                           |
 foreachPartition(KafkaWriter): keep ONE in-flight visit; when visit_id changes
    |                           |   emit {visit_id, pages, start, end}; flush on close
    v                           v
 visits-aggregated (key = visit_id)          no network exchange between tasks
```

## Consequences & trade-offs

The pattern requires immutability of the storage and of the grouping keys, which you can't always
guarantee.

| Consequence | What happens | Mitigation |
| --- | --- | --- |
| Scaling | The most visible issue. If you change the storage partitions, a key can show up in several partitions, and you get one or several aggregates for the same key | Treat rescaling as a migration: a dedicated reorganization task rewrites all records into the new layout. Size the partition count for future throughput up front |
| Scaling in streaming | The reorganization needs a stop-the-world: drain the old partitions before producers write to the new ones | Plan a maintenance window, or switch to the Distributed Aggregator during the move |
| Grouping keys | One layout serves one grouping key for all consumers. Serving another would mean writing each record several times, once per key | Consumers that group by another attribute (change type vs user id) use the Distributed Aggregator |
| Producer dependency | Correctness depends on every producer using the same partitioner. A client upgrade that changes the default partitioner silently splits keys | Explicit partition assignment with a stable hash, checked at startup |

## Key design decisions

- **Partition key = grouping key**, set explicitly by the producer. Never rely on round-robin or
  sticky partitioning.
- **Partition count**: fixed and sized for growth. The producer, the consumer and the orchestrator
  all check it.
- **Fold strategy**: for sorted input, fold one key at a time (constant memory). For
  unsorted streaming input, keep per-partition state and flush on time and on rebalance.
- **Output layout**: write the aggregates keyed by the same key, ideally to the same partition
  number, so downstream jobs keep the locality.
- **Shuffle verification**: fail the job when the physical plan contains `Exchange`. Don't settle
  for "it was fast yesterday".
- **Replay safety**: key the output by the grouping key and write to a compacted topic or an upsert
  sink, so a retried task overwrites its aggregates.

## When to use / when not

| Situation | Use Local Aggregator? | Alternative |
| --- | --- | --- |
| Keyed broker topic, fixed partitions, consumers group by that key | Yes | none |
| Tables bucketed on the grouping key with a stable bucket count | Yes | none |
| Dataset fits on one node | Yes (single-node aggregation) | none |
| Partition count expected to grow with traffic | No | Distributed Aggregator |
| Several consumers group by different attributes | Only for the key that matches the layout | Distributed Aggregator for the others |
| Producers outside your control (partner feeds, default partitioners) | No | Distributed Aggregator; salting for skew |
| Joins with a small dimension | Yes, in its replicated form | Broadcast join (Static Joiner), Redshift `DISTSTYLE ALL` |

## Gotchas & troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Two aggregates for one visit | Topic partitions were added, or a producer used another partitioner | Startup partition-count check; explicit `partition=crc32(key) % N`; reorganize the data |
| `Exchange hashpartitioning` in `explain()` | `repartition`, `groupBy`, a join, or AQE re-planning | Remove it, or accept the Distributed Aggregator; `assert_no_shuffle()` fails fast |
| Split aggregates in Spark despite a keyed topic | `minPartitions` splits one topic partition into several Spark partitions | Don't set `minPartitions` for this job |
| Last visit of a partition missing | Writer never emitted the in-flight aggregate | Emit on partition close (`fold_sorted_visits` yields the tail) |
| Aggregates lost after a consumer crash | Offsets committed before the aggregates were acked | `producer.flush()` then commit that partition's offset |
| Duplicate aggregates after a rebalance | State not flushed on revoke, so the new owner re-reads and re-emits | Flush and commit in `on_revoke`; key the output for compaction |
| Memory growth in the consumer | High key cardinality per partition, long flush interval | Shorter `FLUSH_SECONDS`, or sorted batch folding |
| Bucketed groupBy still shuffles | Bucketing disabled, bucket counts differ, or not read as a table | `spark.sql.sources.bucketing.enabled=true`, same `bucketBy(N, key)`, `spark.table(...)` |

## Implementation notes per stack

**Spark**: `examples/local_aggregator/spark/spark_local_aggregator_sorted_partitions.py`. The plan is checked, then the job folds each partition:

```python
sorted_visits = visits.sortWithinPartitions("visit_id", "event_time")
plan = sorted_visits._jdf.queryExecution().executedPlan().toString()
if "Exchange" in plan:
    raise RuntimeError("input partitioning is not key-aligned")
sorted_visits.foreachPartition(write_partition(cfg))   # KafkaWriter: fold, send, flush, raise on errors
```

`--mode bucketed` reads a table written with `bucketBy(cfg.buckets, "visit_id")` and runs
`groupBy("visit_id")` under the same no-Exchange assertion.

**Kafka**: Kafka Streams `stream("visits").groupByKey().aggregate(...)` is the native JVM form
. `examples/local_aggregator/kafka/kafka_local_aggregator.py` is the Python analogue:

```python
producer.produce(SOURCE_TOPIC, key=visit_id, value=line,
                 partition=zlib.crc32(visit_id.encode()) % EXPECTED_PARTITIONS)
# consumer: state[msg.partition()].add(visit, offset)  -- never re-keyed
# flush_partition(p): produce aggregates to partition p of the output -> flush -> commit(p, last+1)
```

Both roles call `check_partition_count()` and exit when the topic layout changed.

**Airflow**: `examples/local_aggregator/airflow/local_aggregator_dag.py`. `@task.branch`
compares `AdminClient.list_topics()` with the `local_agg_expected_partitions` Variable. If they
match, it runs the local Spark job and validates the output high watermarks. If not, it alerts and
runs the Distributed Aggregator fallback.

**Redshift**:

```sql
CREATE TABLE visits (visit_id INT, user_id INT, ...) DISTSTYLE KEY DISTKEY(visit_id);
CREATE TABLE users  (user_id INT, ...)               DISTSTYLE ALL;   -- joins with users stay local
```

## Related patterns

- **Distributed Aggregator**: the general fallback when partitioning is not guaranteed; same API, with a shuffle.
- **Bucket pattern**: the storage layout that gives tables a stable per-key placement.
- **Horizontal Partitioner**: the partition-count decisions this pattern depends on.
- **Bin Pack Orderer**: reuses `sortWithinPartitions` + `foreachPartition` for ordered delivery.
- **Static Joiner**: broadcast or `DISTSTYLE ALL` is the join equivalent of a local operation.
- **Stateful Sessionizer**: per-key state in streaming follows the same key-locality idea.
