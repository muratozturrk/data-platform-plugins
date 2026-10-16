# Batch Reprocessor - examples

| File | Shows |
|------|-------|
| `spark/visits_logic.py` | Business logic extracted from I/O, plus the origin decoration step |
| `spark/real_time_job.py` | Real-time Kafka -> Kafka job using the shared logic (`origin=real_time`) |
| `spark/batch_reprocessor_job.py` | Stateless offset-range reprocessing; sessions mode rebuilding whole sessions from adjacent partitions |
| `airflow/batch_reprocessing_dag.py` | On-demand DAG triggered by CI/CD via REST, with typed params |
| `kafka/README.md` | Kafka as output only: raw copy, origin headers, idempotent consumers |

Run Spark files with `--py-files visits_logic.py`. Prerequisites: Delta table `visits_raw` (raw copy of the
topic with value/offset/event_time), Kafka on `localhost:9094`, Spark 3.5 + Delta 3.x.
