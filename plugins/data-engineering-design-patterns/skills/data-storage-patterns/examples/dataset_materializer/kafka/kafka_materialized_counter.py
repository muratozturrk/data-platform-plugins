"""Dataset Materializer - Kafka: materialize an aggregate into a compacted topic.

Consumes `visits`, keeps a per-user visit counter, and periodically publishes
the current count to `visits_per_user` (cleanup.policy=compact, key=user_id).
Readers get the precomputed result by reading the compacted topic (or a
Kafka Connect sink of it) instead of re-aggregating the raw stream - the
streaming twin of an incrementally refreshed materialized view.
On restart the state is rebuilt from the compacted topic itself.

Run:
    pip install confluent-kafka
    kafka-topics --create --topic visits_per_user --config cleanup.policy=compact ...
    KAFKA_BOOTSTRAP=localhost:9092 python kafka_materialized_counter.py
"""
import json
import os
import time

from confluent_kafka import Consumer, Producer, TopicPartition

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
VIEW_TOPIC = "visits_per_user"
FLUSH_EVERY_S = 10


def restore_state() -> dict[str, int]:
    """Read the compacted view topic to the end to rebuild counters."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "restore-tmp", "enable.auto.commit": False})
    parts = c.list_topics(VIEW_TOPIC, timeout=10).topics[VIEW_TOPIC].partitions
    c.assign([TopicPartition(VIEW_TOPIC, p, 0) for p in parts])
    state: dict[str, int] = {}
    while (msg := c.poll(2.0)) is not None:
        if not msg.error() and msg.value() is not None:
            state[msg.key().decode()] = json.loads(msg.value())["visits"]
    c.close()
    return state


def main() -> None:
    counts = restore_state()
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "visits-materializer",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
    consumer.subscribe(["visits"])
    dirty: set[str] = set()
    last_flush = time.monotonic()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error():
                user_id = json.loads(msg.value())["user_id"]
                counts[user_id] = counts.get(user_id, 0) + 1
                dirty.add(user_id)
            if dirty and time.monotonic() - last_flush > FLUSH_EVERY_S:
                for user_id in dirty:  # incremental refresh: only changed keys
                    producer.produce(VIEW_TOPIC, key=user_id,
                                     value=json.dumps({"user_id": user_id, "visits": counts[user_id]}))
                producer.flush()
                consumer.commit(asynchronous=False)
                dirty.clear()
                last_flush = time.monotonic()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
