# Sorter examples

| File | Shows |
|------|-------|
| `spark/spark_sorter.py` | `sortWithinPartitions(event_time)` on write for narrow per-file stats; Delta `optimize().executeZOrderBy(["visit_id","page"])`; `--optimize-only` for scheduled re-sorts |
| `kafka/README.md` | Why Kafka has no Sorter (offset order only) and what to use instead |
| `airflow/sorter_dag.py` | BigQuery `PARTITION BY ... CLUSTER BY visit_id, page` + nightly Z-order maintenance via `SparkSubmitOperator` |

Prerequisites: `pip install pyspark delta-spark`; Airflow 2.x with google + apache-spark providers, credentials
from connections/env. Run: `python spark/spark_sorter.py /tmp/out`.
