# Change Data Capture examples

Stream every committed change, including hard deletes, from the commit log.

- `kafka/setup_postgres_for_cdc.sql` — enable logical replication, create the Debezium replication user and the publication. Run with `psql -v debezium_pw="$DEBEZIUM_DB_PASSWORD" -f setup_postgres_for_cdc.sql`.
- `kafka/debezium_postgres_connector.json` + `kafka/register_debezium_connector.py` — Debezium PostgreSQL connector, POSTed once to Kafka Connect. The JSON uses `${env:DEBEZIUM_DB_PASSWORD}` (needs `config.providers=env` on the worker).
- `spark/spark_cdc_streaming_consumer.py` — Structured Streaming: parse the Debezium envelope, dedupe per key by LSN, `foreachBatch` MERGE for c/u/r and DELETE for d into Delta.
- `spark/spark_cdc_batch_replay.py` — bounded `spark.read` over an offset range for backfill or reconciliation, globally ordered by LSN.
- `spark/spark_delta_change_data_feed.py` — lake-native CDC: table with `delta.enableChangeDataFeed`, `readStream` with `readChangeFeed`, `startingVersion`, `maxFilesPerTrigger`; drops `update_preimage`, keeps the latest change per key and MERGEs inserts, updates and deletes.
- `airflow/cdc_pipeline_monitor_dag.py` — watchdog: connector health, consumer lag, auto-restart, trigger a reconciliation DAG.

Prerequisites: PostgreSQL with `wal_level=logical`, Kafka + Kafka Connect with the Debezium plugin, PySpark with `spark-sql-kafka` and `delta-spark`, Airflow 2.x.
