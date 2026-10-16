# Transactional Writer -- examples

Output becomes visible only at commit; a failed attempt exposes nothing.

| File | Shows |
|---|---|
| `spark/spark_transactional_writer.py` | Job-level transaction: one DataFrameWriter call = one `_delta_log` commit; `replaceWhere` + `txnAppId`/`txnVersion` so a retried job that already committed is skipped. `--self-test`. |
| `spark/spark_streaming_transactional_writer.py` | `foreachBatch`: one Delta commit per table; MERGE for orders, append with `txnVersion=batch_id` for metrics; no cross-table atomicity. |
| `kafka/kafka_transactional_writer.py` | confluent-kafka transactions: `init/begin/commit/abort_transaction`, `send_offsets_to_transaction`, explicit `transaction.timeout.ms`, seek back to the batch start on abort; readers use `read_committed`. |
| `airflow/transactional_writer_dag.py` | Two temp tables + `COPY` + `MERGE` on one PostgreSQL connection, single `COMMIT`, `ROLLBACK` on any failure, then a consistency check. Self-test with `python transactional_writer_dag.py`. |

Prerequisites: PySpark 3.5 + delta-spark 3.x (2.0+ for txn options), `confluent-kafka` (kafka-python has no transactions API), Airflow 2.x + Postgres provider, PostgreSQL 15+ for MERGE.
