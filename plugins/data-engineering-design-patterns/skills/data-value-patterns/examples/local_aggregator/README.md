# Local Aggregator examples

Aggregation inside each input partition with no shuffle. It only works while producers keep every key in one partition and the partition count never changes.

- `spark/spark_local_aggregator_sorted_partitions.py`: batch read of a keyed Kafka topic (one Spark partition per topic partition, no `minPartitions`), `sortWithinPartitions(visit_id, event_time)` + `foreachPartition` with a partition-based Kafka writer that emits one aggregate per visit. Fails before running if the physical plan contains an `Exchange`. `--mode bucketed` shows the bucketed-table variant (`bucketBy` + `groupBy` without Exchange).
- `kafka/kafka_local_aggregator.py`: `produce` sends each visit to `crc32(visit_id) % EXPECTED_PARTITIONS`. `aggregate` keeps per-partition state, the Python analogue of Kafka Streams `groupByKey()`, flushes on a timer and on rebalance revoke, and commits after the producer ack. Both modes refuse to start when the partition count changed.
- `airflow/local_aggregator_dag.py`: a branch task checks the topic partition count with `AdminClient`, then runs the local job or alerts and falls back to the Distributed Aggregator. Output high watermarks are compared before and after the run.

Prerequisites: PySpark 3.3+ with `spark-sql-kafka-0-10`, `confluent-kafka` on driver and executors, Airflow 2.4+ with the Spark provider.
Self-tests: `python spark/spark_local_aggregator_sorted_partitions.py --self-test`, `python kafka/kafka_local_aggregator.py --self-test`.
