"""Denormalizer - Kafka stream-table enrichment into a flattened topic.

Loads the compacted `devices` topic (entity table, key = device_id) into memory,
keeps it updated, and enriches every `visits` event with the device attributes
before writing it to `visits_enriched`. Downstream consumers read one
self-contained record per visit - no lookup, no join - at the price of
duplicated device attributes and stale copies when a device changes later
(the enriched record is a snapshot of the device at processing time).

Run:
    pip install confluent-kafka
    KAFKA_BOOTSTRAP=localhost:9092 python kafka_stream_table_enricher.py
"""
import json
import os

from confluent_kafka import Consumer, Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")


def enrich(visit: dict, devices: dict[str, dict]) -> dict:
    device = devices.get(visit.get("device_id"), {})
    return {**visit, **{f"device_{k}": v for k, v in device.items() if k != "device_id"}}


def main() -> None:
    # Unique group per instance + earliest: every instance sees the whole device table.
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"enricher-{os.getpid()}",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
    # ponytail: devices and visits in one consumer means visits may be enriched before the
    # device table is fully loaded; bootstrap the table first (or use Kafka Streams GlobalKTable).
    consumer.subscribe(["devices", "visits"])
    devices: dict[str, dict] = {}
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            if msg.topic() == "devices":
                key = msg.key().decode()
                if msg.value() is None:  # tombstone
                    devices.pop(key, None)
                else:
                    devices[key] = json.loads(msg.value())
                continue
            visit = json.loads(msg.value())
            producer.produce("visits_enriched", key=msg.key(), value=json.dumps(enrich(visit, devices)))
            producer.poll(0)
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    out = enrich({"visit_id": "v1", "device_id": "d1"}, {"d1": {"device_id": "d1", "full_name": "Yoga 7i"}})
    assert out == {"visit_id": "v1", "device_id": "d1", "device_full_name": "Yoga 7i"}
    main()
