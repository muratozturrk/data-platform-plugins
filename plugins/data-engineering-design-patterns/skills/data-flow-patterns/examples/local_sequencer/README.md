# Local Sequencer examples

| File | Shows |
|---|---|
| `airflow/local_sequencer_dag.py` | `sensor >> load >> expose` chain; each task is an independent retry boundary |
| `spark/local_sequencer_job.py` | Step functions chained via DataFrames, intermediate dataset materialized as a restart boundary (`--stage final` replays only the last step) |
| `kafka/README.md` | Why Kafka has no direct equivalent; in-process chain vs chained topics |

Prerequisites: Airflow 2.6+ with `common-sql`/`postgres` providers and a `dp_postgres` connection;
PySpark 3.3+. Run the DAG with `airflow dags test local_sequencer 2024-01-01`, the job with
`spark-submit local_sequencer_job.py --input ... --work ... --output ...`.
