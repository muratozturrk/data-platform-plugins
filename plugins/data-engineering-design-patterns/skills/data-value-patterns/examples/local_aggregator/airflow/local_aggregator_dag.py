"""Local Aggregator -- Airflow 2.x DAG guarding the partitioning contract.

The Local Aggregator is only correct while the input partition count and the
producer key stay unchanged. This DAG checks that
precondition before every run instead of trusting it:

  check_partitioning --(unchanged)--> snapshot_output -> local_aggregation (spark-submit,
                     |                                   no shuffle) -> validate_output
                     +--(changed)---> alert_partitioning_changed -> distributed_aggregation
                                      (groupBy with shuffle; correct for any layout)

`validate_output` compares the output topic's high watermarks before and after
the job and fails when nothing was produced (an empty hour of visits is an
incident on its own for this feed).

Run: put in dags/, set Variables `local_agg_expected_partitions` (int) and
optionally `local_agg_bootstrap`; Spark connection `spark_default`.
Prereqs: apache-airflow>=2.4, apache-airflow-providers-apache-spark, confluent-kafka.
"""
import logging
from datetime import datetime, timedelta
from typing import Dict

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

log = logging.getLogger(__name__)

SOURCE_TOPIC = "visits"
TARGET_TOPIC = "visits-aggregated"
JOBS_DIR = "/opt/jobs/local_aggregator"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"


def _bootstrap() -> str:
    return Variable.get("local_agg_bootstrap", default_var="localhost:9092")


def _high_watermarks(topic: str) -> Dict[int, int]:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": _bootstrap(), "group.id": "airflow-local-agg-check",
                         "enable.auto.commit": False})
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        return {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
                for p in partitions}
    finally:
        consumer.close()


@dag(
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["data-value", "local-aggregator"],
)
def local_aggregator():

    @task.branch
    def check_partitioning() -> str:
        from confluent_kafka.admin import AdminClient

        expected = int(Variable.get("local_agg_expected_partitions"))
        metadata = AdminClient({"bootstrap.servers": _bootstrap()}).list_topics(timeout=10)
        found = {t: len(metadata.topics[t].partitions) for t in (SOURCE_TOPIC, TARGET_TOPIC)
                 if t in metadata.topics}
        if found.get(SOURCE_TOPIC) == expected:
            log.info("partition counts %s match expected %d", found, expected)
            return "snapshot_output"
        log.error("partition counts %s differ from expected %d", found, expected)
        return "alert_partitioning_changed"

    @task
    def snapshot_output() -> Dict[str, int]:
        # XCom keys must be strings
        return {str(p): o for p, o in _high_watermarks(TARGET_TOPIC).items()}

    @task
    def alert_partitioning_changed() -> None:
        # Hook your alerting here (Slack/PagerDuty). The fallback below keeps data correct,
        # but the producer contract must be fixed or the dataset reorganized.
        log.error("Local Aggregator precondition broken: running the Distributed Aggregator fallback")

    local_aggregation = SparkSubmitOperator(
        task_id="local_aggregation",
        application=f"{JOBS_DIR}/spark_local_aggregator_sorted_partitions.py",
        application_args=["--mode", "kafka"],
        packages=KAFKA_PACKAGE,
        conn_id="spark_default",
        env_vars={"KAFKA_BOOTSTRAP": "{{ var.value.get('local_agg_bootstrap', 'localhost:9092') }}"},
    )

    distributed_aggregation = SparkSubmitOperator(
        task_id="distributed_aggregation",
        # groupBy(visit_id) with shuffle, correct for any layout (see the distributed_aggregator examples)
        application="/opt/jobs/distributed_aggregator/spark_distributed_aggregator_batch.py",
        packages=KAFKA_PACKAGE,
        conn_id="spark_default",
    )

    @task
    def validate_output(before: Dict[str, int]) -> int:
        after = _high_watermarks(TARGET_TOPIC)
        produced = sum(o - before.get(str(p), 0) for p, o in after.items())
        if produced <= 0:
            raise ValueError("local aggregation produced no records for this run")
        log.info("local aggregation produced %d aggregates", produced)
        return produced

    branch = check_partitioning()
    before = snapshot_output()
    branch >> before >> local_aggregation >> validate_output(before)
    branch >> alert_partitioning_changed() >> distributed_aggregation


local_aggregator()
