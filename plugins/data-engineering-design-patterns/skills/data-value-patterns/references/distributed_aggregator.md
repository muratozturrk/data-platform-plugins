# Distributed Aggregator

Category: Data Aggregation.

## Problem
A job cleans raw visits from Bronze into Silver, stored in daily event-time partitions. One consumer needs an **OLAP cube** for dashboards: basic statistics (count, average duration, ...) across several axes (user geography, devices, ...), as **daily and weekly views**. The dataset is too big for one machine, and related records sit on different nodes, possibly in different stores.

## Solution / Mechanics
For data that fits on one machine, a group-by and reduce in your language of choice is enough. The Distributed Aggregator uses a **cluster**: servers that can't hold the dataset alone, but share the work as one execution unit. The code stays the same as the local version (group, then reduce, or a global reduce with no key). Execution adds a network **shuffle** that colocates related rows before the reduce. This is the MapReduce model, from disk-based Hadoop to memory-first Spark.

Steps (Spark terms):
1. **Read**: each executor loads its input splits. This is the first network exchange, because storage and compute are rarely colocated.
2. **Map + partial aggregation**: decomposable functions pre-aggregate locally per key (`HashAggregate(partial_count)`), so only small partials cross the network. Count is the textbook case: shuffle per-node counts and sum them.
3. **Shuffle**: rows or partials are hash-partitioned by key (`Exchange hashpartitioning(key, N)`).
4. **Reduce**: each reducer merges all partials of its keys (`HashAggregate(count)`).

```
 node 1: PL,mobile x3  US,web x1 ──partial──> (PL,mobile,3) (US,web,1) ─┐   Exchange
 node 2: PL,mobile x2  US,web x4 ──partial──> (PL,mobile,2) (US,web,4) ─┼─> hashpartitioning(key)
 node 3: US,web x2               ──partial──> (US,web,2)                ─┘          │
                          reducer A: (PL,mobile) 3+2 = 5  <──────────────────────────┤
                          reducer B: (US,web)  1+4+2 = 7  <──────────────────────────┘
```

The same mechanics combine **physically isolated stores**: Spark joins PostgreSQL (JDBC) and JSON files and shuffles both sides. Warehouses do the same with **external tables**: BigQuery over GCS, Redshift, Synapse, Snowflake.

Verify the shuffle with `explain()` and look for `Exchange hashpartitioning`:
```
+- SortMergeJoin [ctx.technical.dev_type, ctx.technical.dev_version], [type, version]
   :- Exchange hashpartitioning(ctx.technical.dev_type, ...)   <- JSON side
   +- Exchange hashpartitioning(type, version, 200)            <- JDBC side
```

## Consequences & trade-offs
| Consequence | What happens | Mitigation |
|---|---|---|
| Additional network exchange | The input read is one exchange. The shuffle is a second one and often the first latency troublemaker. | Partial aggregation. Broadcast a small join side. Avoid the shuffle entirely under Local Aggregator conditions. |
| Data skew | One key with far more rows than the others: its shuffle and reduce cost lands on one node (a straggler). | **Salting**: aggregate on (key, salt), then re-aggregate on the key. Spark **AQE** skew handling. |
| Scaling | A node that finished its reduce tasks is kept for fault tolerance (its shuffle files may be re-read), so it can't be reclaimed while the job runs. | A **shuffle service** stores and serves shuffle data: Spark External Shuffle Service, GCP Dataflow Shuffle. Executors can then be released at any time. |

Salting shape:
```python
(dataset.withColumn("salt", (F.rand() * 3).cast("int"))
   .groupBy("group_key", "salt").agg(...)      # stage 1: hot key spread over 3 reducers
   .groupBy("group_key").agg(...))             # stage 2: merge the partials
```

## When to use / when not
| Situation | Use | Alternative |
|---|---|---|
| Data doesn't fit on one node; related rows are spread across nodes | Distributed Aggregator | - |
| Join or aggregate across stores (DB + files, table + object storage) | Distributed Aggregator (JDBC + files, external tables) | Copy to one store first, if repeated often |
| Data already partitioned by the grouping key (Kafka key, bucketed table) | No | **Local Aggregator** (no shuffle) |
| A small dimension table joined to large facts | Partly | Broadcast join (one side stays local) |
| The dataset fits in one process | No | Native group-by (pandas, SQL on one DB) |
| Distinct counts or percentiles at scale | Only with sketches | HLL `approx_count_distinct`, `percentile_approx` |

## Key design decisions
| Decision | Guidance |
|---|---|
| Cube axes | `cube()` for all combinations, `rollup()` for hierarchies, `GROUPING SETS` for a chosen subset. Each extra axis multiplies the output rows. |
| NULL vs "all" | Cube marks "all values" with NULL. Coalesce real NULLs to `unknown` and keep `grouping_id()` in the output. |
| Measures | Store decomposable ones (count, sum, min, max, sum of squares). Derive avg and stddev at read time. |
| Weekly view | Roll it up from the 7 daily cubes (sum of sums divided by sum of counts). Don't rescan raw data, and never average averages. |
| Shuffle partitions | Start from AQE (`coalescePartitions`). Pin `spark.sql.shuffle.partitions` only for streaming state. |
| Skew handling | AQE handles joins. Use explicit salting for skewed group-by keys of decomposable aggregates. |
| Idempotency | Overwrite one view partition (`replaceWhere view_date = ...`). Don't append. |

## Implementation notes per stack
**Spark batch**: `examples/distributed_aggregator/spark/spark_distributed_aggregator_batch.py`. It reads JSON visits + JDBC devices, reduces per visit (shuffle 1), joins (shuffle 2), then cubes (shuffle 3), with optional salting and a weekly rollup.
```python
per_visit = with_devices(visits_per_session(load_visits(spark, cfg, day)), load_devices(spark, cfg))
cube = (per_visit.cube("country", "device_type")
        .agg(F.count("*").alias("visits"), F.sum("duration_s").alias("total_duration_s"),
             F.grouping_id().alias("grouping_id"))
        .withColumn("avg_duration_s", F.col("total_duration_s") / F.col("visits")))
plan = cube._jdf.queryExecution().executedPlan().toString()   # contains "Exchange hashpartitioning"
```
Credentials come from env vars (`DEVICES_DB_USER`, `DEVICES_DB_PASSWORD`). An inline `dp_test` password is for local demos only.

**Spark streaming**: `examples/distributed_aggregator/spark/spark_streaming_distributed_aggregator.py`. `--mode native` runs `groupBy(window("1 day"), ...)` with map-side partials. `--mode stateful` runs `applyInPandasWithState`, emitting running partials and then a final row at the watermark.
```python
visits.groupBy("event_date", "country", "device_type").applyInPandasWithState(
    aggregate_day, OUTPUT_DDL, STATE_DDL, outputMode="update",
    timeoutConf=GroupStateTimeout.EventTimeTimeout)
# inside: state.setTimeoutTimestamp(max(day_end_ms, watermark + 1)); on timeout -> is_final row
```
A pandas state function receives raw rows, so no partial aggregation runs before its shuffle. Prefer native aggregations when the emission semantics allow them.

**Kafka**: `examples/distributed_aggregator/kafka/kafka_distributed_aggregator.py`. Kafka has no implicit shuffle, so you build it: mappers re-key partial counts to a repartition topic (the default partitioner acts as hashpartitioning), and reducers own the keys of their partitions.
```python
producer.begin_transaction()
for key, count in map_partials(events, cfg.salt_buckets).items():   # map-side combine
    producer.produce(cfg.repartition_topic, key=key, value=json.dumps({"count": count}))
producer.send_offsets_to_transaction(consumer.position(consumer.assignment()),
                                     consumer.consumer_group_metadata())
producer.commit_transaction()                                       # exactly-once hop
```
Kafka Streams `groupBy(...).count()` creates the same repartition topic and a changelog.

**Airflow**: `examples/distributed_aggregator/airflow/distributed_aggregator_dag.py`. It runs a readiness sensor, then the daily cube, then a Sunday `short_circuit`, then the weekly rollup, with JDBC credentials taken from a connection.

## Gotchas & best practices
1. Merging partial medians, partial distinct counts or partial averages gives wrong numbers. Merge sums and counts, or use sketches.
2. Salting works only for decomposable aggregates and adds a second aggregation stage. Use a deterministic salt (a hash of the record id) when replays must land in the same bucket.
3. A broadcast hint on a big table sends it to every executor and can OOM the driver or executors. Check its size first.
4. `cube()` output can be larger than the input for high-cardinality axes. Prune the axes or use `GROUPING SETS`.
5. Variance from partials: clamp `sum_sq/n - avg^2` at 0 before `sqrt`. Floating-point error can make it slightly negative.
6. A sudden drop in output keys usually means upstream data loss. Validate key counts in the orchestrator.

## Troubleshooting
| Symptom | Likely cause | Fix |
|---|---|---|
| One task runs 10x longer than its siblings | A skewed key | Salting, AQE `skewJoin`, or isolate the hot key |
| `FetchFailedException` / lost shuffle files | Executor lost, no shuffle service | Enable the external shuffle service or Spark 3 decommissioning |
| Executors stay allocated after the reduce finished | Shuffle data pinned on executors | Shuffle service + dynamic allocation |
| Huge "Shuffle Write" in the Spark UI | Non-decomposable aggregate or `collect_list` before the exchange | Decomposable measures and sketches; reduce the columns before `groupBy` |
| Weekly avg differs from the dashboard | Averages of daily averages | Sum of sums divided by sum of counts |
| Cube rows with NULL dimensions are double-counted | Real NULLs mixed with cube's "all" marker | Coalesce to `unknown` and filter on `grouping_id` |
| No `Exchange` in the plan | Broadcast join, or data already partitioned by the key | Expected. This is the Local Aggregator case. |

## Related patterns
- **Local Aggregator**: the case where the shuffle can be dropped because data is already partitioned by the key.
- **Static Joiner**: enrichment joins trigger the same shuffle, or a broadcast when the dimension is small.
- **Incremental Sessionizer**: a grouping-plus-reduce, spread over consecutive partitions.
- **Readiness Marker**: the daily cube must start only after the partition is complete.
- **Fast Metadata Cleaner / partition overwrite**: the idempotent per-view overwrite used here.
