"""Hybrid Source - bounded historical topic, then real-time topic (confluent-kafka).

Equivalent of Flink's HybridSource with plain clients:
  1. historical topic: the END offsets are captured at start-up (closed boundary). The
     topic may still be receiving data, so we never wait for "end of topic", and we never
     use a business filter (user_id range) as the boundary;
  2. real-time topic: assigned from the switch timestamp via offsets_for_times.
State (here: per-user visit counts) is kept across the switch in the same process.

Run:  python hybrid_consumer.py --historical visits-archive --real-time visits --switch-ms 1762144261000
Test: python hybrid_consumer.py --self-test
Env:  KAFKA_BOOTSTRAP
Deps: confluent-kafka
"""
import argparse
import json
import os
from collections import Counter

from confluent_kafka import Consumer, TopicPartition

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
state: Counter = Counter()


def to_common_schema(raw: dict) -> dict:
    """Historical and real-time topics use different field names; harmonize once, here."""
    return {"user_id": raw.get("user_id") or raw.get("userId"),
            "event_time": raw.get("event_time") or raw.get("eventTimestamp")}


def process(record: dict) -> None:
    state[record["user_id"]] += 1


def new_consumer() -> Consumer:
    return Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "visits-hybrid", "enable.auto.commit": False})


def read_bounded(topic: str) -> None:
    consumer = new_consumer()
    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    bounds = {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10) for p in partitions}
    end = {p: high for p, (_, high) in bounds.items()}
    remaining = {p for p, (low, high) in bounds.items() if high > low}
    consumer.assign([TopicPartition(topic, p, bounds[p][0]) for p in remaining])
    while remaining:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        if msg.offset() < end[msg.partition()]:
            process(to_common_schema(json.loads(msg.value())))
        if msg.offset() >= end[msg.partition()] - 1:
            remaining.discard(msg.partition())
            consumer.incremental_unassign([TopicPartition(topic, msg.partition())])
    consumer.close()


def read_real_time(topic: str, switch_ms: int) -> None:
    consumer = new_consumer()
    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    starts = consumer.offsets_for_times([TopicPartition(topic, p, switch_ms) for p in partitions], timeout=10)
    consumer.assign(starts)  # offset -1 (no record after ts) = end of partition
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            process(to_common_schema(json.loads(msg.value())))
    finally:
        consumer.close()


def _self_test() -> None:
    assert to_common_schema({"userId": "u1", "eventTimestamp": 1}) == {"user_id": "u1", "event_time": 1}
    assert to_common_schema({"user_id": "u2", "event_time": 2})["user_id"] == "u2"
    print("self-test OK")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--historical", default="visits-archive")
    parser.add_argument("--real-time", default="visits")
    parser.add_argument("--switch-ms", type=int, default=1762144261000)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        _self_test()
    else:
        read_bounded(args.historical)
        read_real_time(args.real_time, args.switch_ms)
