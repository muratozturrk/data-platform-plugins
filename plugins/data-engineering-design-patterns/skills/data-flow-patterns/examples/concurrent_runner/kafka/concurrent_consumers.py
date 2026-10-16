"""Concurrent Runner -- Kafka consumer group with N parallel workers.

Records of independent datasets/keys can be processed concurrently. A consumer group gives
concurrency up to the number of partitions; per-key ordering is still kept because one
partition is owned by exactly one worker at a time. Raising --workers above the partition
count only adds idle members (the concurrency ceiling = partitions).

Shared-state gotcha: workers must not mutate shared state (a global counter/file); write
per-key/per-partition outputs or use idempotent upserts.

Run:
    export KAFKA_BOOTSTRAP=localhost:9092
    python concurrent_consumers.py --workers 4
Requires: confluent-kafka>=2.0; topic devices_raw with >= 4 partitions.
"""
import argparse
import multiprocessing
import os

from confluent_kafka import Consumer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "devices_raw"


def worker(worker_id: int) -> None:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "devices-ingestion",  # same group -> partitions are shared out
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC], on_assign=lambda c, parts: print(
        f"worker {worker_id} owns partitions {[p.partition for p in parts]}"))
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            print(f"worker {worker_id} p{msg.partition()}@{msg.offset()}: {msg.key()}")
            consumer.commit(msg, asynchronous=False)
    finally:
        consumer.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    processes = [multiprocessing.Process(target=worker, args=(i,)) for i in range(args.workers)]
    for p in processes:
        p.start()
    for p in processes:
        p.join()


if __name__ == "__main__":
    main()
