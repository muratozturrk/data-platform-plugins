# Dataset Tracker - examples

| File | Shows |
|------|-------|
| `spark/openlineage_spark_job.py` | `OpenLineageSparkListener` on a SparkSession; join job reported as orders + users -> orders_with_users |
| `airflow/openlineage_dag.py` | Automatic SQL lineage via the OpenLineage provider + manual `inlets`/`outlets` for a custom task |
| `kafka/manual_lineage_emitter.py` | Custom lineage: a plain Kafka consumer/producer emitting START/COMPLETE/FAIL RunEvents |

Prerequisites: Marquez (`docker run -p 5000:5000 -p 3000:3000 marquezproject/marquez` or its compose file),
UI on http://localhost:3000. `pip install openlineage-python apache-airflow-providers-openlineage confluent-kafka`.
Managed alternatives: Databricks Unity Catalog lineage, GCP Dataplex, Azure Purview.
