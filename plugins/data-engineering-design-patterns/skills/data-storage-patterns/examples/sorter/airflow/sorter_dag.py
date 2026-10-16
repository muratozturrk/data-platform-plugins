"""Sorter - Airflow 2.x: BigQuery clustered table + scheduled Delta Z-order.

* `create_clustered_table` - BigQuery implements the Sorter via clustering:
  PARTITION BY DATE(event_time) CLUSTER BY visit_id, page. Helps predicates on
  visit_id, or visit_id + page, but NOT page alone (composite-key gotcha).
* `zorder_delta_table` - nightly re-sort of the Delta table with Z-order so the
  unsorted segments created by the day's appends get reorganized off the
  critical write path.

Run: put in dags/; requires apache-airflow-providers-google and
apache-airflow-providers-apache-spark. GCP credentials via the
`google_cloud_default` connection (GOOGLE_APPLICATION_CREDENTIALS), not in code.
"""
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

CLUSTERED_DDL = """
CREATE TABLE IF NOT EXISTS `dp.visits.raw_visits` (
  visit_id STRING, event_time TIMESTAMP, user_id STRING, page STRING)
PARTITION BY DATE(event_time)
CLUSTER BY visit_id, page
"""


@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False,
     tags=["dp", "storage", "sorter"])
def sorter():
    create_clustered_table = BigQueryInsertJobOperator(
        task_id="create_clustered_table",
        configuration={"query": {"query": CLUSTERED_DDL, "useLegacySql": False}})

    load_day = BigQueryInsertJobOperator(
        task_id="load_day",
        configuration={"query": {"useLegacySql": False, "query": """
            INSERT INTO `dp.visits.raw_visits`
            SELECT visit_id, event_time, user_id, page FROM `dp.visits.staging_visits`
            WHERE DATE(event_time) = '{{ ds }}'"""}})

    zorder_delta_table = SparkSubmitOperator(
        task_id="zorder_delta_table",
        application="/opt/jobs/spark_sorter.py",
        application_args=["/lake/visits_sorted", "--optimize-only"],
        packages="io.delta:delta-spark_2.12:3.2.0",
        conn_id="spark_default")

    create_clustered_table >> load_day
    zorder_delta_table  # independent nightly maintenance


sorter()
