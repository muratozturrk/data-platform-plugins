# Passthrough Replicator examples

Copy production data as is to dev/staging when the original loader cannot be replayed there.

- `spark/spark_passthrough_file_replicator.py` - `--mode text`: `read.text` -> `write.text` (lines untouched, layout not kept); `--mode files`: Hadoop `FileUtil.copy` of the directory (same file names and bytes, `_delta_log`/`_SUCCESS` included; text mode is refused on table formats). Temp dir + swap, verification of line count or file count/bytes. `--self-test` included.
- `spark/spark_passthrough_kafka_replicator.py` - Kafka -> Kafka Structured Streaming: key, value, partition, headers; `sortWithinPartitions("offset")` in `foreachBatch`; checks equal partition counts first.
- `kafka/mirrormaker2_passthrough.properties` + `kafka/register_mirrormaker2.py` - the infrastructure alternative: MirrorMaker 2 (source, checkpoint, heartbeat connectors) with `IdentityReplicationPolicy` to keep topic names.
- `airflow/passthrough_replicator_dag.py` - push DAG in the prod Airflow: sensor on the prod `_SUCCESS`, one replication task per lower environment in a dedicated pool, marker written last.

Infrastructure-level file replication (S3 replication rules) needs no code here; see the reference.
Prerequisites: PySpark 3.4+ (`spark-sql-kafka-0-10` for the Kafka job), two Kafka clusters + Kafka Connect for MM2, Airflow 2.4+ with the Spark provider.
