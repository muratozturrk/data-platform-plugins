# Sidecar - Airflow

The Sidecar is a **streaming-job** pattern: one running job combines several inputs and writes
several outputs. Airflow does not run streams, so no DAG is shown here.

The orchestration equivalents are the data flow (fan-in/fan-out) patterns that Sidecar groups together:
- **Aligned Fan-In**: wait for all inputs, for example several sensors upstream of one task (`[sensor_a, sensor_b] >> process`).
- **Unaligned Fan-In**: `trigger_rule="one_success"` / `"none_failed"` on the consuming task.
- **Parallel Split**: one task fanning out to several writer tasks, where every task must succeed.
- **Exclusive Choice**: `@task.branch` to choose the writer, or optional writers with `trigger_rule="all_done"`.

For the storage-based fan-out (write once to the broker, then one job per sink), Airflow can schedule
those downstream sink jobs.
