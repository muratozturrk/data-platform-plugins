"""FIFO Orderer -- Airflow 2.x TaskFlow DAG publishing to GCP Pub/Sub with ordering keys.

Pattern: deliver records oldest-first, waiting for each acknowledgment
before the next one. Pub/Sub guarantees FIFO only per ordering_key, only for
one publisher in one region, and only with enable_message_ordering=True.

Ordering across runs: max_active_runs=1 + depends_on_past=True, so hour H+1
is never published before hour H. A task retry re-publishes records already
acknowledged -> consumers dedup on event_id (FIFO is not exactly-once).

Run: drop into dags/, set Variables `fifo_input_dir` and `fifo_topic_path`
(projects/<p>/topics/<t>); creds via GOOGLE_APPLICATION_CREDENTIALS.
Prereqs: apache-airflow>=2.4, google-cloud-pubsub.
"""
import json
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable


@dag(
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"depends_on_past": True, "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["data-value", "ordering"],
)
def fifo_orderer():

    @task
    def publish_in_order(data_interval_start=None) -> int:
        from google.cloud import pubsub_v1

        path = Path(Variable.get("fifo_input_dir")) / f"{data_interval_start:%Y%m%d%H}.jsonl"
        records = [json.loads(l) for l in path.read_text().splitlines() if l] if path.exists() else []
        records.sort(key=lambda r: r["event_time"])          # oldest first

        publisher = pubsub_v1.PublisherClient(
            publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=True))
        topic = Variable.get("fifo_topic_path")
        for r in records:
            future = publisher.publish(topic, json.dumps(r).encode(), ordering_key=r["visit_id"])
            try:
                future.result(timeout=60)                    # ack before the next record
            except Exception:
                # after a failure Pub/Sub pauses the key; resume so a retry can publish again
                publisher.resume_publish(topic, r["visit_id"])
                raise
        return len(records)

    publish_in_order()


fifo_orderer()
