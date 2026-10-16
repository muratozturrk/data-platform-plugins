# External Trigger examples

Start ingestion when the producer says data exists, not on a clock.

- `airflow/lambda_dag_trigger.py` — push trigger: AWS Lambda on S3 `ObjectCreated` → `POST /api/v1/dags/devices-loader/dagRuns` with event, trigger identity and `file_to_load` in `conf`; deterministic `dag_run_id` (409 = already triggered); stdlib only, credentials from env. `--self-test` included.
- `airflow/devices_loader_dag.py` — the triggered DAG: `schedule=None`, `max_active_runs=5`, reads `dag_run.conf`, fails fast on malformed triggers.

- `spark/spark_external_trigger_processor.py` — pull variant (valid but polls even when nothing is new): DB trigger queue claimed with `SELECT ... FOR UPDATE SKIP LOCKED`; pending → processing → completed/failed, retry limit, `EVENT_HANDLERS` routing.
- `spark/spark_s3_file_trigger.py` — pull variant over SQS: S3 event notifications via SQS long polling; `processed_s3_files` dedup table; the SQS message is deleted only after all its files succeed.
- `kafka/kafka_pipeline_trigger.py` — `TriggerPublisher` / `TriggerConsumer` library: completion events, a handler per dataset, exponential backoff, DLQ, commit after success or DLQ.
- `airflow/external_trigger_dag.py` — `trigger_receiver_dag` and `triggered_pipeline_orders`, both `schedule_interval=None`, driven only by `dag_run.conf`; post-run validation against the row count in the trigger payload.

Prerequisites: AWS Lambda (Python 3.12) + Airflow stable REST API with basic auth for the push example, PySpark + `delta-spark`, PostgreSQL (`DB_PASSWORD` env var), `boto3` + AWS credentials for the SQS/S3 example, `kafka-python`, Airflow 2.x.
Trigger: `airflow dags trigger trigger_receiver_dag --conf '{"dataset": "orders", "partition_date": "2024-01-10", "source_system": "erp", "row_count": 1000}'` or POST to `/api/v1/dags/<dag_id>/dagRuns`.
