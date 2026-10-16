"""API Gateway - schedule the fallback replay only when undelivered records exist (Airflow 2.x).

Every 15 minutes: a short-circuit task checks the gateway's fallback directory;
if files are there, spark-submit runs the availableNow replay job. Also alerts when
the fallback keeps growing (the broker may still be unreachable for the gateway).

Run:  copy to $AIRFLOW_HOME/dags; connection `spark_default`.
Deps: apache-airflow>=2.6, apache-airflow-providers-apache-spark
"""
import os
import pathlib

import pendulum
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

FALLBACK_DIR = pathlib.Path(os.environ.get("FALLBACK_DIR", "/tmp/dp/fallback/topic=visits"))
ALERT_FILE_COUNT = 1000


@dag(schedule="*/15 * * * *", start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False,
     max_active_runs=1, tags=["ingestion", "api_gateway"])
def gateway_fallback_replay():

    @task.short_circuit
    def has_fallback_files() -> bool:
        files = [f for f in FALLBACK_DIR.glob("*.json")] if FALLBACK_DIR.exists() else []
        if len(files) > ALERT_FILE_COUNT:
            print(f"[ALERT] {len(files)} fallback files - check gateway -> broker connectivity")
        return bool(files)

    replay = SparkSubmitOperator(
        task_id="replay_fallback", conn_id="spark_default",
        application=os.path.join(os.path.dirname(__file__), "..", "spark", "fallback_replay_job.py"),
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        env_vars={"FALLBACK_DIR": str(FALLBACK_DIR)})

    has_fallback_files() >> replay


gateway_fallback_replay()
