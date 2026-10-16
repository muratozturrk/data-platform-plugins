"""Lag Detector - consumer group lag for any Kafka consumer (confluent-kafka).

lag(partition) = last available offset (high watermark) - committed offset of the group.
Reports MAX (worst case, may only reflect one skewed partition) and P90 (how most
partitions perform). Never alert on the average: it hides lagging partitions.

Run:  python consumer_group_lag.py --group visits-sessionizer --topic visits --max-p90 1000
Test: python consumer_group_lag.py --self-test
Env:  KAFKA_BOOTSTRAP (default localhost:9094)
Deps: confluent-kafka
"""
import argparse
import os
import statistics
import sys

from confluent_kafka import OFFSET_INVALID, Consumer, TopicPartition


def partition_lags(group: str, topic: str) -> dict[int, int]:
    # Same group.id to read its committed offsets; we never subscribe, so no rebalance is triggered.
    consumer = Consumer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"),
                         "group.id": group, "enable.auto.commit": False})
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        committed = consumer.committed([TopicPartition(topic, p) for p in partitions], timeout=10)
        lags = {}
        for tp in committed:
            low, high = consumer.get_watermark_offsets(tp, timeout=10)
            position = low if tp.offset == OFFSET_INVALID else tp.offset  # nothing committed yet
            lags[tp.partition] = high - position
        return lags
    finally:
        consumer.close()


def summarize(lags: list[int]) -> dict[str, float]:
    p90 = statistics.quantiles(lags, n=10, method="inclusive")[-1] if len(lags) > 1 else lags[0]
    return {"max": max(lags), "p90": p90, "avg": statistics.fmean(lags)}


def _self_test() -> None:
    s = summarize([10, 5, 30, 2, 3, 5, 3])   # the "average trap" example
    assert round(s["avg"]) == 8 and s["p90"] == 18 and s["max"] == 30  # avg ~8 hides the P90 of 18
    print("self-test OK")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", default="visits-sessionizer")
    parser.add_argument("--topic", default="visits")
    parser.add_argument("--max-p90", type=float, default=1000)
    parser.add_argument("--max-worst", type=float, default=10000)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        _self_test()
        sys.exit(0)
    lags = partition_lags(args.group, args.topic)
    stats = summarize(list(lags.values()))
    print(f"lag per partition={lags} summary={stats}")
    if stats["p90"] > args.max_p90 or stats["max"] > args.max_worst:
        print("[ALERT] consumer is falling behind - scale out or rebalance skewed partitions")
        sys.exit(1)
