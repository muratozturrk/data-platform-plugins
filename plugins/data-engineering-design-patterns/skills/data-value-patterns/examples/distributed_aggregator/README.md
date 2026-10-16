# Distributed Aggregator examples

All files solve the same problem: build an OLAP cube of visits (count, average duration) across geography and device axes, with daily and weekly views.

- `spark/spark_distributed_aggregator_batch.py`: the main implementation. It joins JSON visits with PostgreSQL devices over JDBC (two isolated stores), runs a visit-level reduce, then `cube(country, device_type)` with decomposable measures and `grouping_id`. `SALT_BUCKETS=N` runs a two-stage salted aggregation, and AQE plus the external shuffle service are enabled. `--explain` logs whether `Exchange hashpartitioning` appears in the plan. `--view weekly` rolls 7 daily cubes up, and every write uses Delta `replaceWhere` on the view partition.
- `spark/spark_streaming_distributed_aggregator.py`: daily counts per (date, country, device) from Kafka. `--mode native` is `groupBy(window).count()`, which applies map-side partial aggregation. `--mode stateful` uses `applyInPandasWithState`: it emits running partials (`is_final=false`) and then one final row when the watermark passes the day end. The Delta MERGE never downgrades a final row.
- `kafka/kafka_distributed_aggregator.py`: MapReduce on plain consumers. Mappers combine partial counts per poll and re-key them to a repartition topic, which acts as the shuffle. Reducers sum them into a co-partitioned compacted topic. Both roles use Kafka transactions for exactly-once, and reducer state is restored from the output topic on assignment.
- `airflow/distributed_aggregator_dag.py`: a readiness sensor, the daily cube, a Sunday short-circuit, then the weekly rollup. JDBC credentials come from an Airflow connection.

Run `python <file> --self-test` for the pure-logic checks (no Spark or broker needed).
Prerequisites: PySpark 3.4+ with `delta-spark`, the PostgreSQL JDBC driver and `spark-sql-kafka`; `confluent-kafka`; Airflow 2.4+ with the spark and postgres providers.
