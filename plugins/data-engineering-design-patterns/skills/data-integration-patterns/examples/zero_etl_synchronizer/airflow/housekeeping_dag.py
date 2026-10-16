"""Zero-ETL Synchronizer - orchestrate what the sync does NOT do for you (Airflow 2.x).

Zero-ETL removes the ingestion pipeline, not operations. This DAG runs nightly the
freshness check + compaction/snapshot expiration + derived-table refresh
implemented in ../spark/synced_table_housekeeping.py.

Run:  copy to $AIRFLOW_HOME/dags; connection `spark_default`.
Deps: apache-airflow>=2.6, apache-airflow-providers-apache-spark
"""
import os

import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

JOB = os.path.join(os.path.dirname(__file__), "..", "spark", "synced_table_housekeeping.py")


@dag(schedule="0 2 * * *", start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False,
     max_active_runs=1, tags=["zero_etl", "housekeeping"])
def zero_etl_visits_housekeeping():
    SparkSubmitOperator(
        task_id="freshness_compaction_transform", conn_id="spark_default", application=JOB,
        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
                 "org.apache.iceberg:iceberg-aws-bundle:1.6.1",
        env_vars={"ICEBERG_REST_URI": os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")})


zero_etl_visits_housekeeping()
