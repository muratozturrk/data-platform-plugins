"""Dataset Tracker - manual (custom) lineage for a plain Kafka consumer/producer.

Plain Kafka clients have no lineage integration, so the job reports its own
inputs/outputs to OpenLineage (manual implementation):
START when it begins, COMPLETE/FAIL when it stops. Topic datasets use the
`kafka://<bootstrap>` namespace so Marquez links them with Spark/Flink jobs
reading the same topics.

Run:  python manual_lineage_emitter.py   (Ctrl+C to stop -> COMPLETE event)
Env:  KAFKA_BOOTSTRAP, OPENLINEAGE_URL (default http://localhost:5000)
Deps: confluent-kafka, openlineage-python>=1.15
"""
import os
import uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import InputDataset, Job, OutputDataset, Run, RunEvent, RunState

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
KAFKA_NS = f"kafka://{BOOTSTRAP}"
JOB_NAMESPACE = "visits_team"
JOB_NAME = "visits_enricher"
INPUT_TOPIC, OUTPUT_TOPIC = "visits", "visits-enriched"

lineage = OpenLineageClient(url=os.environ.get("OPENLINEAGE_URL", "http://localhost:5000"))
run = Run(runId=str(uuid.uuid4()))


def emit(state: RunState) -> None:
    lineage.emit(RunEvent(
        eventType=state, eventTime=datetime.now(timezone.utc).isoformat(), run=run,
        job=Job(namespace=JOB_NAMESPACE, name=JOB_NAME), producer="dp/manual_lineage_emitter",
        inputs=[InputDataset(namespace=KAFKA_NS, name=INPUT_TOPIC)],
        outputs=[OutputDataset(namespace=KAFKA_NS, name=OUTPUT_TOPIC)]))


def main() -> None:
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": JOB_NAME, "auto.offset.reset": "earliest"})
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    consumer.subscribe([INPUT_TOPIC])
    emit(RunState.START)
    state = RunState.COMPLETE
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            producer.produce(OUTPUT_TOPIC, key=msg.key(), value=msg.value())  # enrichment omitted
            producer.poll(0)
    except KeyboardInterrupt:
        pass
    except Exception:
        state = RunState.FAIL
        raise
    finally:
        producer.flush(10)
        consumer.close()
        emit(state)


if __name__ == "__main__":
    main()
