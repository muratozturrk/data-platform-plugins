"""Vertical Partitioner (data removal) - Airflow 2.x TaskFlow DAG.

Daily pipeline processing personal-data removal requests against the
vertically partitioned PII storage:
  1. collect pending requests (batch them: one run for N users)
  2. DELETE the users from the Delta PII table, then VACUUM
  3. send tombstones to the compacted Kafka `user_context` topic
  4. mark requests as done

Because PII lives in a dedicated, deduplicated store, each user is ONE row,
not one row per event.

Run: drop into $AIRFLOW_HOME/dags; requires delta-spark, pyspark, confluent-kafka
on the worker and env USERS_TABLE, KAFKA_BOOTSTRAP, REQUESTS_FILE.
"""
import json
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task

USERS_TABLE = os.environ.get("USERS_TABLE", "/tmp/dp/users")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
REQUESTS_FILE = os.environ.get("REQUESTS_FILE", "/tmp/dp/removal_requests.json")


@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False,
     max_active_runs=1, default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
     tags=["security", "data-removal"])
def vertical_partitioner_user_removal():

    @task
    def collect_requests() -> list:
        if not os.path.exists(REQUESTS_FILE):
            return []
        with open(REQUESTS_FILE) as f:
            return sorted({r["user_id"] for r in json.load(f) if r.get("status") == "pending"})

    @task
    def delete_from_delta(user_ids: list) -> list:
        if not user_ids:
            return user_ids
        from delta import DeltaTable, configure_spark_with_delta_pip
        from pyspark.sql import SparkSession, functions as F
        spark = configure_spark_with_delta_pip(
            SparkSession.builder.appName("pii-removal")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        ).getOrCreate()
        table = DeltaTable.forPath(spark, USERS_TABLE)
        table.delete(F.col("user_id").isin(user_ids))
        table.vacuum(float(os.environ.get("VACUUM_RETENTION_HOURS", "168")))
        return user_ids

    @task
    def send_tombstones(user_ids: list) -> list:
        if not user_ids:
            return user_ids
        from confluent_kafka import Producer
        producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "enable.idempotence": True})
        for user_id in user_ids:
            producer.produce("user_context", key=user_id.encode(), value=None)
        if producer.flush(60) > 0:
            raise RuntimeError("some tombstones were not delivered")
        return user_ids

    @task
    def mark_done(user_ids: list) -> None:
        if not user_ids:
            return
        with open(REQUESTS_FILE) as f:
            requests = json.load(f)
        for r in requests:
            if r["user_id"] in user_ids:
                r["status"] = "done"
        with open(REQUESTS_FILE, "w") as f:
            json.dump(requests, f)

    mark_done(send_tombstones(delete_from_delta(collect_requests())))


vertical_partitioner_user_removal()
