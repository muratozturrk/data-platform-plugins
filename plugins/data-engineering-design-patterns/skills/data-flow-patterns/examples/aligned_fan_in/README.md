# Aligned Fan-In examples

| File | Shows |
|---|---|
| `airflow/aligned_fan_in_dag.py` | 24 dynamically generated hourly branches merging into `generate_trends` (default `all_success`), `max_active_tasks` against spikes |
| `spark/aligned_fan_in_job.py` | `unionByName` of 24 hourly partials (vertical) + join with devices (horizontal); fails if any hour is missing |
| `kafka/README.md` | Multi-topic subscribe / stream joins / watermarks as the streaming equivalent |

Prerequisites: Airflow 2.6+ with `common-sql`/`postgres` providers and `dp_postgres` connection; PySpark 3.3+.
Try the Spark job locally with `--demo` to generate inputs.
