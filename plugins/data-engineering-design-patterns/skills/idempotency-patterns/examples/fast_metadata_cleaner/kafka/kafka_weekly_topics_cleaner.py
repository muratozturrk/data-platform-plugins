"""Fast Metadata Cleaner adapted to Kafka: one topic per week.

Kafka has no DELETE or TRUNCATE of individual records, but topic deletion and
creation are metadata operations handled by the controller. The weekly visits
stream therefore goes to one topic per idempotency unit (`visits_2024_w02`):

  new unit (Monday / Jan 1) or explicit --replay-week:
      delete_topics(week) -> wait until the topic is really gone -> create_topics(week)
  every run:
      produce the day's records into the weekly topic

Consumers read the "view": a regex subscription `^visits_\\d{4}_w\\d{2}$` that
picks up new weekly topics automatically (metadata refresh interval applies).

Consequences specific to Kafka:
- Metadata limits: every week adds a topic with N partitions. Cluster-wide
  partition counts (and managed-service quotas) cap how many units you keep;
  a freezing step can copy closed weeks into a yearly topic and delete them.
- A replay of the unit invalidates consumer offsets for that topic: consumers
  must re-read it from the beginning (auto.offset.reset=earliest).
- delete.topic.enable must be true on the brokers.

Run:  pip install confluent-kafka
      KAFKA_BOOTSTRAP=localhost:9092 python kafka_weekly_topics_cleaner.py \
          --execution-date 2024-01-08 --input-file visits_2024-01-08.jsonl
      python kafka_weekly_topics_cleaner.py --consume       # read through the "view"
      python kafka_weekly_topics_cleaner.py --self-test
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import date, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("kafka_weekly_topics_cleaner")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
PARTITIONS = int(os.environ.get("WEEKLY_TOPIC_PARTITIONS", "6"))
REPLICATION = int(os.environ.get("WEEKLY_TOPIC_REPLICATION", "3"))
VIEW_PATTERN = r"^visits_\d{4}_w\d{2}$"


# --------------------------------------------------------------------------- pure helpers
def week_topic_name(day: date) -> str:
    return f"visits_{day.year}_w{day.strftime('%W')}"


def should_start_new_unit(day: date) -> bool:
    return week_topic_name(day) != week_topic_name(day - timedelta(days=1))


def _self_check() -> None:
    assert week_topic_name(date(2024, 1, 8)) == "visits_2024_w02"
    assert re.match(VIEW_PATTERN, week_topic_name(date(2025, 1, 1)))
    assert not re.match(VIEW_PATTERN, "visits_2024_w02_frozen")
    assert should_start_new_unit(date(2024, 1, 8)) and not should_start_new_unit(date(2024, 1, 9))
    assert should_start_new_unit(date(2025, 1, 1))  # Wednesday, but Jan 1
    print("kafka_weekly_topics_cleaner self-check OK")


# --------------------------------------------------------------------------- admin side
def _admin():
    from confluent_kafka.admin import AdminClient
    return AdminClient({"bootstrap.servers": BOOTSTRAP})


def topic_exists(admin, topic: str) -> bool:
    return topic in admin.list_topics(timeout=10).topics


def recreate_topic(admin, topic: str, timeout_s: float = 120.0) -> None:
    """DROP + CREATE of the weekly unit; both are controller metadata operations."""
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import NewTopic

    if topic_exists(admin, topic):
        for t, fut in admin.delete_topics([topic], operation_timeout=30).items():
            fut.result()
            log.info("Delete requested for %s", t)
        # Deletion is asynchronous: wait until the metadata no longer lists it,
        # otherwise create_topics fails with TOPIC_ALREADY_EXISTS.
        deadline = time.monotonic() + timeout_s
        while topic_exists(admin, topic):
            if time.monotonic() > deadline:
                raise TimeoutError(f"{topic} still listed {timeout_s}s after delete")
            time.sleep(2)

    new = NewTopic(topic, num_partitions=PARTITIONS, replication_factor=REPLICATION,
                   config={"retention.ms": "-1", "cleanup.policy": "delete"})
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            admin.create_topics([new], operation_timeout=30)[topic].result()
            log.info("Created %s (%d partitions)", topic, PARTITIONS)
            return
        except KafkaException as exc:
            # The controller may still be finishing the deletion on some brokers.
            if "TOPIC_ALREADY_EXISTS" not in str(exc) or time.monotonic() > deadline:
                raise
            time.sleep(2)


# --------------------------------------------------------------------------- produce / consume
def produce_day(topic: str, input_file: str, day: date) -> int:
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True,
                         "acks": "all", "linger.ms": 20})
    errors: list[str] = []

    def on_delivery(err, _msg):
        if err is not None:
            errors.append(str(err))

    sent = 0
    with open(input_file, encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            visit = json.loads(line)
            if not str(visit.get("visit_time", "")).startswith(day.isoformat()):
                continue  # only the execution day belongs to this run
            producer.produce(topic, key=str(visit["user_id"]).encode(),
                             value=json.dumps(visit).encode(), on_delivery=on_delivery)
            sent += 1
            producer.poll(0)
    remaining = producer.flush(60)
    if remaining or errors:
        raise RuntimeError(f"{remaining} undelivered, {len(errors)} errors: {errors[:3]}")
    if sent == 0:
        raise ValueError(f"no visits for {day} in {input_file}")
    log.info("Produced %d visits for %s into %s", sent, day, topic)
    return sent


def consume_view(group_id: str, max_idle_s: float = 10.0) -> None:
    from confluent_kafka import Consumer, KafkaException

    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group_id,
                         "auto.offset.reset": "earliest", "enable.auto.commit": True,
                         "topic.metadata.refresh.interval.ms": 30000})
    consumer.subscribe([VIEW_PATTERN])  # leading ^ makes it a regex subscription
    idle_since = time.monotonic()
    try:
        while time.monotonic() - idle_since < max_idle_s:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            idle_since = time.monotonic()
            print(msg.topic(), msg.partition(), msg.offset(), msg.value().decode())
    finally:
        consumer.close()


def run(args: argparse.Namespace) -> None:
    day = date.fromisoformat(args.execution_date)
    topic = week_topic_name(day)
    admin = _admin()
    if args.replay_week or should_start_new_unit(day) or not topic_exists(admin, topic):
        recreate_topic(admin, topic)
    else:
        # ponytail: mid-week retries append again; replay the week (--replay-week from
        # Monday) when a day must be fixed, that is the pattern's backfill boundary.
        log.info("%s continues unit %s", day, topic)
    produce_day(topic, args.input_file, day)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Weekly-topic Fast Metadata Cleaner")
    p.add_argument("--execution-date")
    p.add_argument("--input-file", help="JSON lines with user_id and visit_time")
    p.add_argument("--replay-week", action="store_true", help="force DROP+CREATE of the unit")
    p.add_argument("--consume", action="store_true", help="read all weeks via regex subscription")
    p.add_argument("--group-id", default="visits-view-reader")
    p.add_argument("--self-test", action="store_true")
    a = p.parse_args(sys.argv[1:])
    if a.self_test:
        _self_check()
    elif a.consume:
        consume_view(a.group_id)
    elif a.execution_date and a.input_file:
        run(a)
    else:
        p.error("--execution-date and --input-file are required")
