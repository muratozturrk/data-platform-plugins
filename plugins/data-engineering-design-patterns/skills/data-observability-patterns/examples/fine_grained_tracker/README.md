# Fine-Grained Tracker - examples

| File | Shows |
|------|-------|
| `spark/row_level_lineage.py` | Row-level lineage in Kafka headers: job_name/job_version/batch_version + `parent_lineage` |
| `kafka/row_lineage_propagation.py` | Same propagation with plain confluent-kafka clients (`--self-test`) |
| `airflow/column_lineage_dag.py` | Custom column-level lineage by parsing SQL with `sqlglot.lineage`, versioned per run |

Column lineage for Spark comes from the OpenLineage listener used in `../dataset_tracker/spark/`.
Managed alternatives: Unity Catalog `system.access.column_lineage`, Azure Purview.
Prerequisites: Kafka on `localhost:9094`, `pip install confluent-kafka sqlglot`.
