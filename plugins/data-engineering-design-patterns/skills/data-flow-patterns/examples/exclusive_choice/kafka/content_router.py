"""Exclusive Choice -- Kafka content-based router.

Each record goes to exactly ONE downstream topic, chosen by a cheap condition evaluated on
record metadata first (a header set by the producer: `schema_version`), falling back to the
payload only when the header is absent. Unknown cases go to a dead-letter topic instead of
being silently dropped -- every possible outcome stays visible.

The router is transactional (read-process-write with exactly-once): the routed write and the
input offset commit happen atomically, so a crash never routes a record twice.

Run:
    export KAFKA_BOOTSTRAP=localhost:9092
    python content_router.py
Requires: confluent-kafka>=2.0; topics devices, devices_v1, devices_v2, devices_dlq.
"""
import json
import os

from confluent_kafka import Consumer, KafkaException, Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
INPUT_TOPIC = "devices"
ROUTES = {"1": "devices_v1", "2": "devices_v2"}
DLQ = "devices_dlq"


def choose_route(msg) -> str:
    headers = dict(msg.headers() or [])
    version = headers.get("schema_version")
    if version is not None:  # metadata-based condition: no payload parsing
        return ROUTES.get(version.decode(), DLQ)
    try:
        payload = json.loads(msg.value())
    except (TypeError, ValueError):
        return DLQ
    return ROUTES["2"] if "version" in payload else ROUTES["1"]


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "devices-router",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
    })
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "transactional.id": "devices-router-1"})
    producer.init_transactions()
    consumer.subscribe([INPUT_TOPIC])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            producer.begin_transaction()
            try:
                producer.produce(choose_route(msg), key=msg.key(), value=msg.value(),
                                 headers=msg.headers())
                producer.send_offsets_to_transaction(
                    consumer.position(consumer.assignment()), consumer.consumer_group_metadata())
                producer.commit_transaction()
            except KafkaException:
                producer.abort_transaction()
                raise
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
