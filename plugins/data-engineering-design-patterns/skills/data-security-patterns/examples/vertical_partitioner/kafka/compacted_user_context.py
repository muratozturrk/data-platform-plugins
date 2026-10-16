"""Vertical Partitioner (data removal) - Kafka side with confluent-kafka.

The PII part of the record lives in a COMPACTED topic keyed by user_id (one
logical record per user). Removing a user = producing a tombstone (key, null
value). Log compaction then drops every older record for that key and, after
`delete.retention.ms`, the tombstone itself.

Do NOT enable compaction on the non-PII visits topic: many events share a key
there and compaction would keep only the last one.

Run:
  pip install confluent-kafka
  export KAFKA_BOOTSTRAP=localhost:9092
  python compacted_user_context.py create            # create compacted topic
  python compacted_user_context.py delete <user_id>  # send tombstone
"""
import os
import sys

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("USER_CONTEXT_TOPIC", "user_context")


def create_compacted_topic() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    topic = NewTopic(
        TOPIC,
        num_partitions=3,
        replication_factor=1,
        config={
            "cleanup.policy": "compact",
            # how long tombstones stay visible to lagging consumers
            "delete.retention.ms": str(24 * 3600 * 1000),
            # force segment roll so compaction can kick in within the legal delay
            "segment.ms": str(6 * 3600 * 1000),
            "min.cleanable.dirty.ratio": "0.1",
        },
    )
    for name, future in admin.create_topics([topic]).items():
        try:
            future.result()
            print(f"created {name}")
        except Exception as error:  # topic may already exist
            print(f"{name}: {error}")


def send_tombstone(user_id: str) -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
    delivered = []

    def on_delivery(err, msg):
        if err:
            raise RuntimeError(f"tombstone not delivered: {err}")
        delivered.append(msg.offset())

    # value=None is the tombstone; same key => same partition as the PII record
    producer.produce(TOPIC, key=user_id.encode("utf-8"), value=None, on_delivery=on_delivery)
    producer.flush(30)
    if not delivered:
        raise RuntimeError("tombstone delivery not confirmed")
    print(f"tombstone for {user_id} at offset {delivered[0]}")


if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "create"
    if action == "create":
        create_compacted_topic()
    elif action == "delete":
        send_tombstone(sys.argv[2])
    else:
        raise SystemExit(f"unknown action {action}")
