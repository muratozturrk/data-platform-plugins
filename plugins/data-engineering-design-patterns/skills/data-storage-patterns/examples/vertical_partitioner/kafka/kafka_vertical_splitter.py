"""Vertical Partitioner - Kafka consume-split-produce.

Reads full visit events from `visits_raw` and writes:
  * `visits_events`     - mutable attributes, one message per event
  * `visits_technical`  - immutable context keyed by visit_id; create it with
                          cleanup.policy=compact so only the latest copy per visit is kept.
The immutable part is only sent the first time a visit_id is seen by this
consumer (local cache) - compaction removes any leftover duplicates.
Note the producer-side cost: one input message -> up to two writes.

Run:
    pip install confluent-kafka
    kafka-topics --create --topic visits_technical --config cleanup.policy=compact ...
    KAFKA_BOOTSTRAP=localhost:9092 python kafka_vertical_splitter.py
"""
import json
import os

from confluent_kafka import Consumer, Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
IMMUTABLE_FIELDS = ("browser", "browser_version", "device_type", "ip")


def split(visit: dict) -> tuple[dict, dict]:
    technical = {k: visit.get(k) for k in IMMUTABLE_FIELDS}
    technical["visit_id"] = visit["visit_id"]
    mutable = {k: v for k, v in visit.items() if k not in IMMUTABLE_FIELDS}
    return mutable, technical


def main() -> None:
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "vertical-splitter",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
    consumer.subscribe(["visits_raw"])
    seen_visits: set[str] = set()  # ponytail: unbounded in-memory set, use a TTL cache for long runs
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(msg.error())
                continue
            mutable, technical = split(json.loads(msg.value()))
            key = mutable["visit_id"]
            producer.produce("visits_events", key=key, value=json.dumps(mutable))
            if key not in seen_visits:
                producer.produce("visits_technical", key=key, value=json.dumps(technical))
                seen_visits.add(key)
            producer.flush()
            consumer.commit(msg, asynchronous=False)  # at-least-once; compaction absorbs dups
    finally:
        consumer.close()


if __name__ == "__main__":
    m, t = split({"visit_id": "v1", "page": "home", "browser": "firefox", "ip": "1.1.1.1"})
    assert "browser" not in m and t["browser"] == "firefox" and t["visit_id"] == "v1"
    main()
