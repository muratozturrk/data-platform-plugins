"""Horizontal Partitioner - Kafka producer with custom partition routing.

Python equivalent of a Java `RangePartitioner`: keys "A" and "B" go to
partition 0, everything else to partition 1. confluent-kafka has no pluggable
Python partitioner class, so the routing is computed and passed via `partition=`.
Prefer the default (murmur2 key hash) partitioner unless you have a real need:
custom routing is extra code and an easy source of skew.

Run:
    pip install confluent-kafka
    kafka-topics --create --topic visits --partitions 2 --bootstrap-server localhost:9092
    KAFKA_BOOTSTRAP=localhost:9092 python kafka_range_partitioner.py
"""
import json
import os

from confluent_kafka import Producer

RANGES_PER_PARTITION = {"A": 0, "B": 0}
DEFAULT_PARTITION = 1


def range_partition(key: str) -> int:
    return RANGES_PER_PARTITION.get(key, DEFAULT_PARTITION)


def delivery_report(err, msg):
    if err:
        print(f"FAILED {msg.key()}: {err}")
    else:
        print(f"key={msg.key().decode()} -> partition {msg.partition()} offset {msg.offset()}")


def main() -> None:
    producer = Producer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
                         "enable.idempotence": True})
    topic = os.environ.get("TOPIC", "visits")
    for i, key in enumerate(["A", "B", "C", "D", "A"]):
        event = {"visit_id": f"v{i}", "segment": key, "page": f"page_{i}"}
        producer.produce(topic, key=key, value=json.dumps(event),
                         partition=range_partition(key), on_delivery=delivery_report)
        producer.poll(0)
    producer.flush()


if __name__ == "__main__":
    assert range_partition("A") == 0 and range_partition("Z") == 1
    main()
