"""Flow Interruption Detector - Kafka topic monitor (confluent-kafka).

Every evaluation period the monitor reads the high watermark of each partition and
computes how many records arrived since the previous check.

* continuous mode: alert when ONE period has no new record (e.g. 1 record/min expected)
* irregular mode : alert only when N consecutive periods are empty (accepted no-data window)

The Prometheus equivalent is in ../README.md.

Run:  python topic_flow_monitor.py --topic visits --period 60 --mode irregular --window 5
Test: python topic_flow_monitor.py --self-test
Env:  KAFKA_BOOTSTRAP (default localhost:9094)
Deps: confluent-kafka
"""
import argparse
import os
import time
from collections import deque

from confluent_kafka import Consumer, TopicPartition


def total_high_watermark(consumer: Consumer, topic: str) -> int:
    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    return sum(consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
               for p in partitions)


def is_interrupted(new_records_history: deque, mode: str, window: int, min_records: int = 1) -> bool:
    """continuous: last period below min_records. irregular: `window` consecutive empty periods."""
    if not new_records_history:
        return False
    if mode == "continuous":
        return new_records_history[-1] < min_records
    recent = list(new_records_history)[-window:]
    return len(recent) == window and all(n < min_records for n in recent)


def alert(message: str) -> None:
    # ponytail: stdout alert; plug Slack/PagerDuty/Alertmanager here.
    print(f"[ALERT] {message}", flush=True)


def monitor(topic: str, period: int, mode: str, window: int, min_records: int) -> None:
    consumer = Consumer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"),
                         "group.id": "flow-interruption-monitor", "enable.auto.commit": False})
    history: deque = deque(maxlen=max(window, 1))
    previous = total_high_watermark(consumer, topic)
    try:
        while True:
            time.sleep(period)
            current = total_high_watermark(consumer, topic)
            history.append(current - previous)
            previous = current
            if is_interrupted(history, mode, window, min_records):
                alert(f"Flow interruption on '{topic}': new records per period = {list(history)}")
    finally:
        consumer.close()


def _self_test() -> None:
    assert is_interrupted(deque([5, 0]), "continuous", 3)
    assert not is_interrupted(deque([0, 0, 4]), "continuous", 3)
    assert not is_interrupted(deque([0, 0]), "irregular", 3)       # window not full yet
    assert is_interrupted(deque([0, 0, 0]), "irregular", 3)
    assert not is_interrupted(deque([0, 3, 0]), "irregular", 3)
    assert is_interrupted(deque([50]), "continuous", 1, min_records=100)  # volume-based threshold
    print("self-test OK")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="visits")
    parser.add_argument("--period", type=int, default=60, help="evaluation period in seconds")
    parser.add_argument("--mode", choices=["continuous", "irregular"], default="continuous")
    parser.add_argument("--window", type=int, default=5, help="empty periods tolerated (irregular)")
    parser.add_argument("--min-records", type=int, default=1)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        _self_test()
    else:
        monitor(args.topic, args.period, args.mode, args.window, args.min_records)
