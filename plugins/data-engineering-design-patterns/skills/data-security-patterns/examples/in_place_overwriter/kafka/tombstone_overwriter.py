"""In-Place Overwriter (data removal) - Kafka compacted topic.

For a key-based, compacted topic the "overwrite" is:
  1. read the topic from the beginning up to the current end offsets
  2. for every record belonging to a removed user, produce (record.key, None)
Compaction keeps only the latest value per key, so the PII versions are dropped
and only the tombstone remains (until delete.retention.ms).

Requires: key-based records + cleanup.policy=compact. Not applicable to
append-only event topics (many events per key) - use retention or rewrite instead.

Run:
  pip install confluent-kafka
  export KAFKA_BOOTSTRAP=localhost:9092 TOPIC=users
  python tombstone_overwriter.py <user_id> [<user_id> ...]
"""
import json
import os
import sys
import uuid

from confluent_kafka import Consumer, Producer, TopicPartition

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "users")


def find_keys_to_remove(user_ids: set) -> set:
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"removal-{uuid.uuid4()}",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    partitions = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
    end_offsets = {}
    assignments = []
    for p in partitions:
        _, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)
        end_offsets[p] = high
        assignments.append(TopicPartition(TOPIC, p, 0))
    consumer.assign(assignments)

    keys, done = set(), {p for p, high in end_offsets.items() if high == 0}
    while len(done) < len(end_offsets):
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        if msg.value() is not None:
            payload = json.loads(msg.value())
            if payload.get("user_id") in user_ids:
                keys.add(msg.key())
        if msg.offset() + 1 >= end_offsets[msg.partition()]:
            done.add(msg.partition())
    consumer.close()
    return keys


def send_tombstones(keys: set) -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
    for key in keys:
        producer.produce(TOPIC, key=key, value=None)
    if producer.flush(60) > 0:
        raise RuntimeError("tombstones not fully delivered, rerun (operation is idempotent)")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    removed = set(sys.argv[1:])
    to_remove = find_keys_to_remove(removed)
    send_tombstones(to_remove)
    print(f"sent {len(to_remove)} tombstones")
