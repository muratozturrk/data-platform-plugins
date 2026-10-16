"""Skew Detector - Kafka partition storage skew (confluent-kafka).

Computes the ratio of a standard deviation (STDDEV/AVG * 100) of the number of
records retained per partition. Same idea as this PromQL:

    stddev(sum(kafka_log_size{topic='visits'}) by (partition))
      / avg(kafka_log_size{topic='visits'}) * 100

Records per partition = high watermark - low watermark (retention aware).

Run:  python partition_skew_detector.py --topic visits --threshold 30
Test: python partition_skew_detector.py --self-test
Env:  KAFKA_BOOTSTRAP (default localhost:9094)
Deps: confluent-kafka
"""
import argparse
import os
import statistics
import sys

from confluent_kafka import Consumer, TopicPartition


def records_per_partition(topic: str) -> dict[int, int]:
    consumer = Consumer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"),
                         "group.id": "skew-detector", "enable.auto.commit": False})
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        sizes = {}
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            sizes[p] = high - low
        return sizes
    finally:
        consumer.close()


def skew_ratio_pct(values: list[int]) -> float:
    mean = statistics.fmean(values)
    return 0.0 if mean == 0 else statistics.pstdev(values) / mean * 100


def _self_test() -> None:
    assert skew_ratio_pct([100, 100, 100]) == 0.0
    assert skew_ratio_pct([0, 0]) == 0.0
    assert round(skew_ratio_pct([10, 30])) == 50     # stddev 10 / mean 20
    print("self-test OK")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="visits")
    parser.add_argument("--threshold", type=float, default=30.0, help="max accepted skew in percent")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        _self_test()
        sys.exit(0)
    sizes = records_per_partition(args.topic)
    ratio = skew_ratio_pct(list(sizes.values()))
    print(f"{args.topic}: records per partition={sizes} skew={ratio:.1f}%")
    if ratio > args.threshold:
        print(f"[ALERT] partition skew {ratio:.1f}% > {args.threshold}% - check the partitioning key")
        sys.exit(1)
