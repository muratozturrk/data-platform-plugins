# Unaligned Fan-In examples

| File | Shows |
|---|---|
| `airflow/unaligned_fan_in_dag.py` | `trigger_rule=ALL_DONE` child, `is_approximate` flag + completeness table in SQL, `on_failure_callback` alerts instead of extra failure branches |
| `spark/unaligned_fan_in_job.py` | Explicit parent-outcome evaluation, merge of available partitions, completeness metadata, optional private output when partial |
| `kafka/README.md` | Windows/outer joins, completeness flags and late fills as the streaming equivalent |

Prerequisites: Airflow 2.6+ with `common-sql`/`postgres` providers and `dp_postgres` connection
(tables `dp.visits_raw`, `dp.visits_cube`, `dp.visits_cube_completeness`); PySpark 3.3+.
