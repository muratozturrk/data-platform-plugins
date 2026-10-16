"""Proxy adapted to Kafka: one write-once topic per snapshot + a pointer topic.

Kafka has no views, so this is the manifest-based approach:

  publish:  create devices_v<run_start> (fails if it exists, retention.ms=-1)
            -> produce the full snapshot (idempotent producer) -> flush, check errors
            -> write pointer record  key="devices" -> {"topic": ..., "records": n}
               to the compacted topic devices_proxy_pointer
  read:     read the latest pointer (compaction keeps one record per key)
            -> consume the snapshot topic from the beginning to its end offsets

A rerun never appends to an older snapshot topic: the run start time gives it a
new topic and only the pointer moves. A failed publish leaves an orphan topic
that the pointer never references.

Immutability enforcement (outside this script): ACLs that DENY Write on
`devices_v*` for every principal except the publisher, and remove the
publisher's Write on a topic once the pointer has moved (Kafka ACLs are
per-topic or prefixed). Deleting old topics stays an admin-only operation.

Run:  pip install confluent-kafka
      KAFKA_BOOTSTRAP=localhost:9092 python kafka_versioned_topics_proxy.py publish --input devices.jsonl
      python kafka_versioned_topics_proxy.py read
      python kafka_versioned_topics_proxy.py --self-test
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("kafka_proxy")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
POINTER_TOPIC = os.environ.get("PROXY_POINTER_TOPIC", "devices_proxy_pointer")
DATASET = "devices"
PARTITIONS = int(os.environ.get("SNAPSHOT_PARTITIONS", "3"))
REPLICATION = int(os.environ.get("SNAPSHOT_REPLICATION", "3"))
SNAPSHOT_RE = re.compile(r"^devices_v\d{8}_\d{6}$")


# --------------------------------------------------------------------------- pure helpers
def snapshot_topic(run_start: datetime) -> str:
    return f"{DATASET}_v{run_start.strftime('%Y%m%d_%H%M%S')}"


def pointer_value(topic: str, records: int) -> bytes:
    if not SNAPSHOT_RE.match(topic) or records <= 0:
        raise ValueError(f"invalid pointer target {topic!r} with {records} records")
    return json.dumps({"topic": topic, "records": records,
                       "published_at": datetime.now(timezone.utc).isoformat()}).encode()


def parse_pointer(raw: bytes) -> str:
    topic = json.loads(raw)["topic"]
    if not SNAPSHOT_RE.match(topic):
        raise ValueError(f"pointer references unexpected topic {topic!r}")
    return topic


def _self_check() -> None:
    t = snapshot_topic(datetime(2024, 1, 8, 3, 15, 0))
    assert t == "devices_v20240108_031500"
    assert parse_pointer(pointer_value(t, 10)) == t
    for bad in (lambda: pointer_value(t, 0), lambda: pointer_value("devices", 5),
                lambda: parse_pointer(b'{"topic": "payments"}')):
        try:
            bad()
            raise AssertionError("expected ValueError")
        except ValueError:
            pass
    print("kafka_versioned_topics_proxy self-check OK")


# --------------------------------------------------------------------------- publish
def ensure_pointer_topic(admin) -> None:
    from confluent_kafka.admin import NewTopic
    if POINTER_TOPIC in admin.list_topics(timeout=10).topics:
        return
    admin.create_topics([NewTopic(POINTER_TOPIC, num_partitions=1, replication_factor=REPLICATION,
                                  config={"cleanup.policy": "compact"})])[POINTER_TOPIC].result()


def create_snapshot_topic(admin, topic: str) -> None:
    from confluent_kafka.admin import NewTopic
    # No "if exists" shortcut: an existing name means a second writer; fail loudly.
    admin.create_topics([NewTopic(topic, num_partitions=PARTITIONS, replication_factor=REPLICATION,
                                  config={"retention.ms": "-1", "retention.bytes": "-1",
                                          "cleanup.policy": "delete"})])[topic].result()
    log.info("Created write-once topic %s", topic)


def _producer():
    from confluent_kafka import Producer
    return Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True, "acks": "all"})


def produce_all(producer, topic: str, records) -> int:
    errors: list[str] = []
    sent = 0
    for rec in records:
        producer.produce(topic, key=f"{rec['type']}|{rec['version']}".encode(),
                         value=json.dumps(rec).encode(),
                         on_delivery=lambda err, _m: errors.append(str(err)) if err else None)
        sent += 1
        producer.poll(0)
    if producer.flush(60) or errors:
        raise RuntimeError(f"snapshot {topic} incomplete: {errors[:3]}")
    return sent


def publish(input_file: str, run_start: datetime) -> str:
    from confluent_kafka.admin import AdminClient
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    ensure_pointer_topic(admin)
    topic = snapshot_topic(run_start)
    create_snapshot_topic(admin, topic)

    with open(input_file, encoding="utf-8") as fh:
        records = [json.loads(line) for line in fh if line.strip()]
    if not records:
        raise ValueError(f"{input_file} is empty; not publishing an empty snapshot")
    producer = _producer()
    count = produce_all(producer, topic, records)
    # Only after every snapshot record is acknowledged does the proxy move.
    producer.produce(POINTER_TOPIC, key=DATASET.encode(), value=pointer_value(topic, count))
    if producer.flush(30):
        raise RuntimeError("pointer update not acknowledged; previous snapshot stays current")
    log.info("Pointer %s/%s -> %s (%d records)", POINTER_TOPIC, DATASET, topic, count)
    return topic


# --------------------------------------------------------------------------- read
def resolve_pointer() -> str:
    """Read the pointer topic to its end and return the newest target for DATASET."""
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"proxy-resolver-{os.getpid()}",
                  "enable.auto.commit": False})
    try:
        tp = TopicPartition(POINTER_TOPIC, 0)
        _low, high = c.get_watermark_offsets(tp, timeout=10)
        if high == 0:
            raise LookupError("no snapshot published yet")
        c.assign([TopicPartition(POINTER_TOPIC, 0, 0)])
        latest = None
        while True:
            msg = c.poll(5.0)
            if msg is None:
                break
            if not msg.error() and msg.key() == DATASET.encode():
                latest = msg.value()
            if msg.offset() >= high - 1:
                break
        if latest is None:
            raise LookupError(f"no pointer for {DATASET}")
        return parse_pointer(latest)
    finally:
        c.close()


def read_latest():
    """Yield every record of the snapshot the proxy currently points to."""
    from confluent_kafka import Consumer, TopicPartition
    topic = resolve_pointer()
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"proxy-reader-{os.getpid()}",
                  "enable.auto.commit": False})
    try:
        parts = c.list_topics(topic, timeout=10).topics[topic].partitions
        ends = {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts}
        pending = {p for p, end in ends.items() if end > 0}
        c.assign([TopicPartition(topic, p, 0) for p in pending])
        while pending:
            msg = c.poll(5.0)
            if msg is None:
                raise TimeoutError(f"{topic}: stalled with partitions {sorted(pending)} unread")
            if msg.error():
                continue
            yield json.loads(msg.value())
            if msg.offset() >= ends[msg.partition()] - 1:
                pending.discard(msg.partition())
    finally:
        c.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Kafka Proxy: versioned topics + pointer")
    p.add_argument("command", nargs="?", choices=["publish", "read"])
    p.add_argument("--input", help="JSON lines snapshot (publish)")
    p.add_argument("--self-test", action="store_true")
    a = p.parse_args(sys.argv[1:])
    if a.self_test:
        _self_check()
    elif a.command == "publish" and a.input:
        publish(a.input, datetime.now(timezone.utc))
    elif a.command == "read":
        for r in read_latest():
            print(json.dumps(r))
    else:
        p.error("use 'publish --input FILE', 'read' or --self-test")
