"""Online Observer - inline observation in a consume-transform-produce app, confluent-kafka.

The processing loop (visits -> visits_enriched) updates observation counters for every
record it handles and emits an observation record to `visits_observations` every
OBSERVE_EVERY messages, including its own lag (high watermark - position). Observation
code is wrapped so a bug there never stops data delivery.

Run: pip install confluent-kafka; python kafka_online_observer.py
     python kafka_online_observer.py --self-check   (observer logic only, no broker)
Env: KAFKA_BOOTSTRAP (localhost:9094).
"""
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone

OBSERVE_EVERY = 1000
REQUIRED = ("visit_id", "event_time", "user_id", "page")


class InlineObserver:
    def __init__(self):
        self.counters = Counter()

    def observe(self, visit: dict) -> None:
        self.counters["all_events"] += 1
        for field in REQUIRED:
            if not visit.get(field):
                self.counters[f"invalid_{field}"] += 1

    def flush(self, lag: dict) -> dict:
        dump = {"@timestamp": datetime.now(timezone.utc).isoformat(), **self.counters, "lag": lag}
        self.counters = Counter()
        return dump


def transform(visit: dict) -> dict:
    return {**visit, "processed_at": datetime.now(timezone.utc).isoformat()}


def current_lag(consumer) -> dict:
    lag = {}
    for tp in consumer.position(consumer.assignment()):
        high = consumer.get_watermark_offsets(tp, cached=True)  # cached: no extra broker call
        if high and tp.offset >= 0:
            lag[tp.partition] = high[1] - tp.offset
    return lag


def main():
    from confluent_kafka import Consumer, Producer

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "visits-processor",
                         "auto.offset.reset": "earliest", "statistics.interval.ms": 5000})
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
    observer, processed = InlineObserver(), 0
    consumer.subscribe(["visits"])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            visit = json.loads(msg.value())
            producer.produce("visits_enriched", key=msg.key(), value=json.dumps(transform(visit)).encode())
            processed += 1
            try:  # observation isolated from delivery
                observer.observe(visit)
                if processed % OBSERVE_EVERY == 0:
                    producer.produce("visits_observations", json.dumps(observer.flush(current_lag(consumer))).encode())
            except Exception as error:
                print(f"Observation error ignored: {error}")
            producer.poll(0)
    finally:
        producer.flush()
        consumer.close()


def self_check():
    obs = InlineObserver()
    obs.observe({"visit_id": "1", "event_time": "t", "user_id": "u", "page": "p"})
    obs.observe({"visit_id": "2", "page": ""})
    dump = obs.flush(lag={0: 5})
    assert dump["all_events"] == 2 and dump["invalid_page"] == 1 and dump["invalid_user_id"] == 1
    assert "invalid_visit_id" not in dump and not obs.counters
    print("self-check ok")


if __name__ == "__main__":
    self_check() if "--self-check" in sys.argv else main()
