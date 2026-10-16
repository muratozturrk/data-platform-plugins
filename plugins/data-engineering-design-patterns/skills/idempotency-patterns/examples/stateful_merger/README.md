# Stateful Merger -- examples

A Merger plus a state table, so a backfill first rewinds the table to the state the replayed execution originally saw.

| File | Shows |
|---|---|
| `spark/delta_version_restore_merger.py` | Main implementation. `versions` state table, `decide_restore()` (empty / nothing / `restoreToVersion`, with the compaction-aware `current - 1` rule and a vacuum guard), MERGE with soft deletes, state upsert. `--step restore|merge|update-state|all`. |
| `airflow/stateful_merger_dag.py` | Restore -> merge -> update state as three SparkSubmit tasks. Execution times come from `ds` / `ds_add(ds, -1)`. `max_active_runs=1`, `depends_on_past=True`. |
| `kafka/kafka_history_table_stateful_merger.py` | Variant for stores without versioning. A Kafka time slice (`offsets_for_times`) goes into `devices_history`. On a backfill, later history rows are deleted and `dp.devices` is rebuilt from history, then merged, all in one PostgreSQL transaction. |

Self-tests without dependencies: `python <file> --self-test` (Spark and Kafka files) cover the normal run, backfill and compaction-aware restore scenarios.
Prerequisites: PySpark 3.5 + delta-spark 3.x, with the target table `default.devices` created upfront; `confluent-kafka`, `psycopg2`, PostgreSQL 15+ (`PG_PASSWORD` from env); Airflow 2.6+ with the Spark provider.
