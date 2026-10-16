# Dynamic Late Data Integrator: examples

| File | Shows |
| --- | --- |
| `spark/spark_dynamic_late_integrator.py` | State table on Delta (partition, last processed / last update version, `is_processed`), last version per partition read from `_delta_log/*.json`, planner query, per-run plan file |
| `airflow/dynamic_late_integrator_dag.py` | Concurrent main DAG (`max_active_runs=5`, `depends_on_past` on the marker and planner), plan read and `TriggerDagRunOperator` mapped per late partition to a backfill DAG |
| `kafka/kafka_partition_update_tracker.py` | Builds `last_update_time` per partition from the ingestion stream for stores without that metadata; Postgres planner using `FOR UPDATE SKIP LOCKED` |

Self-tests: `python <spark or kafka file> --self-test`.
Prerequisites: PySpark + `delta-spark` 3.x, Airflow 2.6+ (<3 for `execution_date`), `kafka-python`, `psycopg2-binary`. Env: `DP_DYNAMIC_LATE_BASE_DIR`, `PG_DSN`.
