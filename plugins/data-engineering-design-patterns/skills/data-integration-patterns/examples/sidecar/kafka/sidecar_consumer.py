"""Sidecar - fan-in of two topics, fan-out to two outputs (confluent-kafka).

* Fan-in (Unaligned): one consumer subscribes to both topics; records are processed as
  they come, each harmonized to a common schema first.
* Fan-out: every batch produces (a) the enriched raw record to `visits-enriched` and
  (b) per-batch page counts to `visits-page-stats`.
  - REQUIRED sinks (Parallel Split): offsets are committed only if every required
    delivery succeeded; otherwise the batch is re-consumed (at-least-once -> idempotent
    consumers or keyed outputs downstream).
  - OPTIONAL sinks (Exclusive Choice): failures are logged, offsets still committed.

Run:  python sidecar_consumer.py
Test: python sidecar_consumer.py --self-test
Env:  KAFKA_BOOTSTRAP
Deps: confluent-kafka
"""
import json
import os
import sys
from collections import Counter

from confluent_kafka import Consumer, Producer, TopicPartition

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
REQUIRED_SINKS = {"visits-enriched"}
OPTIONAL_SINKS = {"visits-page-stats"}


def harmonize(topic: str, raw: dict) -> dict:
    if topic == "visits-web":
        return {"visit_id": raw["visit_id"], "event_time": raw["event_ts"], "page": raw["page"], "source": "web"}
    return {"visit_id": raw["id"], "event_time": raw["event_timestamp"], "page": raw["screen"], "source": "mobile"}


def page_stats(visits: list[dict]) -> dict[str, int]:
    return dict(Counter(v["page"] for v in visits))


def main() -> None:
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "visits-sidecar",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
    consumer.subscribe(["visits-web", "visits-mobile"])
    try:
        while True:
            messages = [m for m in consumer.consume(500, timeout=1.0) if not m.error()]
            if not messages:
                continue
            visits = [harmonize(m.topic(), json.loads(m.value())) for m in messages]
            failures: set[str] = set()

            def on_delivery(err, msg):
                if err is not None:
                    failures.add(msg.topic())

            for v in visits:
                producer.produce("visits-enriched", key=v["visit_id"], value=json.dumps(v), on_delivery=on_delivery)
            producer.produce("visits-page-stats", value=json.dumps(page_stats(visits)), on_delivery=on_delivery)
            if producer.flush(30) > 0:
                failures |= REQUIRED_SINKS  # timeout: treat required sinks as failed

            if failures & REQUIRED_SINKS:
                print(f"required sink failed {failures}; rewinding batch")
                first_offsets: dict[tuple[str, int], int] = {}
                for m in messages:  # rewind each partition to its first record of this batch
                    first_offsets.setdefault((m.topic(), m.partition()), m.offset())
                for (topic, partition), offset in first_offsets.items():
                    consumer.seek(TopicPartition(topic, partition, offset))
                continue
            if failures & OPTIONAL_SINKS:
                print(f"optional sink failed {failures & OPTIONAL_SINKS}; continuing")
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()


def _self_test() -> None:
    web = harmonize("visits-web", {"visit_id": "1", "event_ts": "t", "page": "home"})
    mob = harmonize("visits-mobile", {"id": "2", "event_timestamp": "t", "screen": "home"})
    assert web.keys() == mob.keys()
    assert page_stats([web, mob]) == {"home": 2}
    print("self-test OK")


if __name__ == "__main__":
    _self_test() if "--self-test" in sys.argv else main()
