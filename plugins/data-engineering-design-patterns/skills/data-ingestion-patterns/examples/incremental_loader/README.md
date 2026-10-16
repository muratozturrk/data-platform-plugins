# Incremental Loader examples

Load only the part of a growing dataset added since the last run.

- `spark/incremental_loader_jobs.py` - both implementations: `--mode partition` copies `date=<ds>`; `--mode delta-column` keeps rows with `ingestion_time` in the `[date_from, date_to)` window from the orchestrator. Raw lines copied, output overwritten per partition/window (idempotent). `--self-test` included.
- `spark/jdbc_watermark_incremental_load.py` - delta column with a remembered watermark (state table, look-back buffer, bounded `max_window_hours` per run), parallel JDBC reads with dynamic bounds, dedup + Delta MERGE, watermark saved last. `--self-test` covers the window logic.
- `airflow/incremental_loader_dags.py` - `visits_incremental_partition` (FileSensor on the next partition, then the job with `{{ ds }}`) and `visits_incremental_delta_column` (hourly windows from `data_interval_start/end`, no sensor).
- `kafka/` - local Docker scenario: Kafka Connect JDBC source (`timestamp+incrementing` = delta column + id tie-breaker) plus a Python consumer. See `kafka/README.md`.

Prerequisites: PySpark 3.4+ (`delta-spark` and the PostgreSQL JDBC driver for the watermark job), Airflow 2.4+ with the Spark provider, Docker for the Kafka scenario.
Secrets: export `DB_PASSWORD` (and `POSTGRES_PASSWORD` for docker-compose).
