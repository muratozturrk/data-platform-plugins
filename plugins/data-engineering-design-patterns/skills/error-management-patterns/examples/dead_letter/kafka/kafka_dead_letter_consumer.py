"""Dead-Letter pattern - Kafka consumer with a dead-letter topic.

Separates the two error kinds:
  * transient errors (TransientError: DB hiccup, timeout) -> bounded retry with
    backoff; if still failing the consumer stops WITHOUT committing, so the
    record is re-read after restart (never dead-lettered for a temporary issue);
  * non-transient errors (poison pills: bad JSON, missing field) -> written once
    to the dead-letter topic, decorated with the failure metadata, and skipped.

Writing performance: DLQ sends are asynchronous and flushed once per poll batch,
right before the offset commit (flush-then-commit keeps at-least-once).
Monitoring / "error or failure?": a sliding error-ratio window stops the
consumer when too many records are dead-lettered.
Replay (optional): ``replay`` mode re-publishes dead letters to the source topic
with a ``was_dead_lettered`` header + replay metadata.

Run:
  python kafka_dead_letter_consumer.py consume
  python kafka_dead_letter_consumer.py replay --max-messages 1000
  python kafka_dead_letter_consumer.py --self-test
Requires: pip install kafka-python. Env: KAFKA_BOOTSTRAP_SERVERS.
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import json
import logging
import os
import socket
import sys
import time
from typing import Callable, Deque, Optional

log = logging.getLogger("kafka_dead_letter")

SOURCE_TOPIC = os.environ.get("SOURCE_TOPIC", "visits")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "visits_dead_letter")
GROUP_ID = os.environ.get("CONSUMER_GROUP", "visits-processor")
JOB_VERSION = "1.0.0"


class TransientError(Exception):
    """Raised by handlers for errors that recover by themselves."""


class ErrorRateGuard:
    """Sliding window over the last N outcomes; trips above max_ratio."""

    def __init__(self, window: int = 1000, max_ratio: float = 0.05, min_samples: int = 100):
        self.outcomes: Deque[bool] = collections.deque(maxlen=window)
        self.max_ratio, self.min_samples = max_ratio, min_samples

    def record(self, failed: bool) -> None:
        self.outcomes.append(failed)

    def tripped(self) -> bool:
        n = len(self.outcomes)
        return n >= self.min_samples and sum(self.outcomes) / n > self.max_ratio


def dead_letter_envelope(raw: bytes, topic: str, partition: int, offset: int,
                         error: Exception) -> dict:
    """Metadata Decorator: the original payload + everything needed to debug it."""
    return {
        "original_value": raw.decode("utf-8", errors="replace"),
        "source": {"topic": topic, "partition": partition, "offset": offset},
        "error": {"type": type(error).__name__, "message": str(error)[:2000]},
        "processor": {"group": GROUP_ID, "version": JOB_VERSION, "host": socket.gethostname()},
        "dead_lettered_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }


def process_visit(raw: bytes) -> dict:
    """Business logic. Raises ValueError/KeyError for poison pills."""
    visit = json.loads(raw)
    event_time = dt.datetime.fromisoformat(visit["event_time"])
    return {"visit_id": visit["visit_id"], "event_time": event_time.isoformat(),
            "page": visit["page"]}


def handle_with_retries(handler: Callable[[bytes], dict], raw: bytes, retries: int,
                        backoff_s: float) -> dict:
    for attempt in range(retries + 1):
        try:
            return handler(raw)
        except TransientError:
            if attempt == retries:
                raise
            time.sleep(backoff_s * 2 ** attempt)
    raise AssertionError("unreachable")


def consume(args: argparse.Namespace, sink: Callable[[dict], None]) -> None:
    from kafka import KafkaConsumer, KafkaProducer

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    consumer = KafkaConsumer(SOURCE_TOPIC, bootstrap_servers=bootstrap, group_id=GROUP_ID,
                             enable_auto_commit=False, auto_offset_reset="earliest",
                             max_poll_records=args.max_poll_records)
    # acks=all + retries: the dead-letter store itself must be resilient.
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=10, linger_ms=50,
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    guard = ErrorRateGuard(args.guard_window, args.max_error_ratio, args.guard_min_samples)
    try:
        while True:
            polled = consumer.poll(timeout_ms=1000)
            dead_lettered = 0
            for tp, records in polled.items():
                for record in records:
                    try:
                        visit = handle_with_retries(process_visit, record.value,
                                                    args.retries, args.backoff_s)
                    except TransientError:
                        log.error("transient error persisted at %s:%s; stopping without commit",
                                  tp, record.offset)
                        raise
                    except Exception as error:  # noqa: BLE001 - non-transient -> DLQ
                        producer.send(DLQ_TOPIC, key=record.key, value=dead_letter_envelope(
                            record.value, record.topic, record.partition, record.offset, error))
                        guard.record(True)
                        dead_lettered += 1
                        continue
                    sink(visit)  # sink errors are infrastructure issues: let them fail the job
                    guard.record(False)
            if polled:
                producer.flush()   # DLQ writes durable BEFORE the offsets move on
                consumer.commit()
                if dead_lettered:
                    log.warning("dead-lettered %s record(s) in this poll", dead_lettered)
            if guard.tripped():
                raise RuntimeError("error ratio above threshold: failure, not error - stopping")
    finally:
        producer.close()
        consumer.close()


def replay(args: argparse.Namespace) -> int:
    """Re-publish dead letters to the source topic, tagged as replayed."""
    from kafka import KafkaConsumer, KafkaProducer

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    consumer = KafkaConsumer(DLQ_TOPIC, bootstrap_servers=bootstrap, group_id=f"{GROUP_ID}-replay",
                             enable_auto_commit=False, auto_offset_reset="earliest",
                             consumer_timeout_ms=5000)
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=10)
    replay_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S")
    count = 0
    try:
        for record in consumer:
            envelope = json.loads(record.value)
            producer.send(SOURCE_TOPIC, key=record.key,
                          value=envelope["original_value"].encode("utf-8"),
                          headers=[("was_dead_lettered", b"true"),
                                   ("replay_id", replay_id.encode()),
                                   ("replay_job_version", JOB_VERSION.encode())])
            count += 1
            if count % 500 == 0 or count >= args.max_messages:
                producer.flush()
                consumer.commit()
            if count >= args.max_messages:
                break
        producer.flush()
        consumer.commit()
    finally:
        producer.close()
        consumer.close()
    log.info("replayed %s dead letters (replay_id=%s); downstream consumers may need a backfill",
             count, replay_id)
    return count


def _self_check() -> None:
    guard = ErrorRateGuard(window=10, max_ratio=0.2, min_samples=5)
    for failed in (False, False, True, False):
        guard.record(failed)
    assert not guard.tripped()  # not enough samples
    guard.record(True)
    assert guard.tripped()      # 2/5 > 20 %
    env = dead_letter_envelope(b"{bad", "visits", 1, 42, ValueError("boom"))
    assert env["source"]["offset"] == 42 and env["error"]["type"] == "ValueError"
    assert process_visit(b'{"visit_id":"1","event_time":"2024-01-01T10:00:00","page":"p"}')["page"] == "p"
    calls = {"n": 0}

    def flaky(_: bytes) -> dict:
        calls["n"] += 1
        if calls["n"] < 3:
            raise TransientError("db down")
        return {"ok": True}
    assert handle_with_retries(flaky, b"", retries=3, backoff_s=0) == {"ok": True}
    print("self-test OK")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if "--self-test" in sys.argv:
        _self_check()
        return
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("mode", choices=["consume", "replay"])
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--backoff-s", type=float, default=0.5)
    p.add_argument("--max-poll-records", type=int, default=500)
    p.add_argument("--max-error-ratio", type=float, default=0.05)
    p.add_argument("--guard-window", type=int, default=1000)
    p.add_argument("--guard-min-samples", type=int, default=100)
    p.add_argument("--max-messages", type=int, default=10_000)
    args = p.parse_args()
    if args.mode == "consume":
        consume(args, sink=lambda visit: log.debug("processed %s", visit))
    else:
        replay(args)


if __name__ == "__main__":
    main()
