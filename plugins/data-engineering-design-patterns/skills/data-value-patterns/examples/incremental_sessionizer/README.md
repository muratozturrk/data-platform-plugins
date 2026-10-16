# Incremental Sessionizer examples

Every file follows the same design: three storage spaces (input, private pending sessions, public completed sessions), a FULL OUTER JOIN of the new partition with the previous run's pending sessions, and idempotent cleanup of `execution_time_id >= current`.

- `airflow/incremental_sessionizer_sql_dag.py`: the reference PostgreSQL version, on a daily schedule. It runs idempotent DDL, then the two parallel DELETE tasks, then one transactional generation query (temporary input table, FULL OUTER JOIN, `expiration_batch_id` split), then run stats. The inactivity runs, closing pages and partial-session emission are all set through `params`.
- `spark/spark_incremental_sessionizer.py`: the same flow in PySpark + Delta on hourly partitions. It supports `--action clean-sessions|clean-pending|generate|all` and `--execution-time YYYY-MM-DDTHH:00`. Writes use `replaceWhere` on the run id. An empty or missing hour still expires pending sessions. Optional partial sessions are written with `is_completed=false`.
- `airflow/incremental_sessionizer_dag.py`: hourly orchestration of the Spark job with the same task graph. A readiness-marker sensor runs first, then the two parallel cleans, then generation. It uses `depends_on_past` and `max_active_runs=1`, and documents the backfill procedure.

Run `python spark/spark_incremental_sessionizer.py --self-test` for the run-id arithmetic checks.
Prerequisites: Airflow 2.4+ (common-sql, postgres, apache-spark providers), PostgreSQL, PySpark 3.4+ with `delta-spark`.
There are no streaming or Kafka variants because the pattern is batch only. Streaming sessionization is the Stateful Sessionizer (`../stateful_sessionizer/`).
