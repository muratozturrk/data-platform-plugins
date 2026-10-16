# Bin Pack Orderer examples

- `spark/spark_bin_pack_orderer_kinesis.py`: `sortWithinPartitions(visit_id, event_time)` + `foreachPartition`. Records are packed so each bin holds at most one record per entity, and bins go through Kinesis `PutRecords` sequentially, retrying only the failed items. Includes `--self-test` (pure Python, no Spark).
- `kafka/kafka_bin_pack_orderer_es_sink.py`: Kafka consumer feeding the Elasticsearch `_bulk` API, which can commit partially. Offsets are committed only after all bins are delivered, and a deterministic `_id` makes replays idempotent.
- `airflow/bin_pack_orderer_dag.py`: a 10-minute partner sync with one mapped task per partner. Uses `max_active_runs=1` + `depends_on_past` for ordering across runs.

Prerequisites: PySpark, boto3 (AWS creds from the provider chain), `confluent-kafka` + `requests`, and Airflow 2.4+.
Run: `python spark/spark_bin_pack_orderer_kinesis.py --self-test`.
Note: bins larger than 500 records are chunked for Kinesis `PutRecords` in both the Spark job and the DAG.
