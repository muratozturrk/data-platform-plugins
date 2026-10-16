"""Audit-Write-Audit-Publish (AWAP) - window-based streaming variant, confluent-kafka.

Consumes `visits`, buffers a processing-time window (or max N records), audits the
buffer at record level (required fields) and dataset level (invalid ratio), then:
  - fail     : invalid ratio above threshold -> don't publish, don't commit, exit
  - dispatch : valid -> `visits_audited`, invalid -> `visits_audit_errors`
Offsets are committed only after the window is published (at-least-once).

Run: pip install confluent-kafka; python kafka_window_awap.py
     python kafka_window_awap.py --self-check   (audit logic only, no broker)
Env: KAFKA_BOOTSTRAP (default localhost:9094).
"""
import json
import os
import sys
import time

REQUIRED = ("visit_id", "event_time", "user_id", "page")
WINDOW_SECONDS, WINDOW_MAX_RECORDS, MAX_INVALID_RATIO = 15, 500, 0.2


def audit_window(records: list) -> tuple:
    """Return (valid, invalid). Raise if the dataset-level rule fails."""
    valid, invalid = [], []
    for r in records:
        missing = [f for f in REQUIRED if not r.get(f)]
        (invalid if missing else valid).append({**r, "_errors": missing} if missing else r)
    if records and len(invalid) / len(records) > MAX_INVALID_RATIO:
        raise ValueError(f"{len(invalid)}/{len(records)} invalid records exceed {MAX_INVALID_RATIO:.0%}")
    return valid, invalid


def main():
    from confluent_kafka import Consumer, Producer

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "visits-awap",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
    consumer.subscribe(["visits"])
    buffer, window_start = [], time.monotonic()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error():
                try:
                    buffer.append(json.loads(msg.value()))
                except json.JSONDecodeError:
                    buffer.append({"_raw": msg.value().decode("utf-8", "replace")})
            window_closed = time.monotonic() - window_start >= WINDOW_SECONDS or len(buffer) >= WINDOW_MAX_RECORDS
            if not window_closed or not buffer:
                continue
            valid, invalid = audit_window(buffer)  # raises -> fail strategy, offsets not committed
            for topic, rows in (("visits_audited", valid), ("visits_audit_errors", invalid)):
                for row in rows:
                    producer.produce(topic, json.dumps(row).encode("utf-8"))
            producer.flush()
            consumer.commit(asynchronous=False)
            buffer, window_start = [], time.monotonic()
    finally:
        consumer.close()


def self_check():
    ok = {"visit_id": "1", "event_time": "t", "user_id": "u", "page": "p"}
    valid, invalid = audit_window([ok] * 9 + [{"visit_id": "2"}])
    assert len(valid) == 9 and invalid[0]["_errors"] == ["event_time", "user_id", "page"]
    try:
        audit_window([ok, {}, {}])
        raise AssertionError("expected dataset-level failure")
    except ValueError:
        pass
    print("self-check ok")


if __name__ == "__main__":
    self_check() if "--self-check" in sys.argv else main()
