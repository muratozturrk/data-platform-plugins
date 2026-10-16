"""Metadata Decorator -- confluent-kafka forwarder that puts technical context in headers.

Modes:
  decorate  consume `visits`, forward key+value byte-for-byte to `visits-decorated`,
            add job_version / processing_time / attempt / source headers.
            Offsets are committed only after the producer has flushed (no loss on crash).
  inspect   operator tool: read the decorated topic and print records whose headers
            match a job_version, e.g. to check what the last release produced.
            Business consumers never need this: the value has no technical fields.

Kinesis Data Streams has no headers; there, use a context table
(see ../airflow/metadata_decorator_dag.py) instead.

Run:
  export KAFKA_BOOTSTRAP=localhost:9092 JOB_VERSION=v1.0.3
  python kafka_headers_metadata_decorator.py decorate
  python kafka_headers_metadata_decorator.py inspect --job-version v1.0.3 --limit 20
  python kafka_headers_metadata_decorator.py --self-test
Prereqs: pip install confluent-kafka
"""
import argparse
import logging
import os
import signal
import sys
import time
from typing import Dict, List, Optional, Sequence, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("metadata-decorator-kafka")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
SOURCE_TOPIC = os.environ.get("SOURCE_TOPIC", "visits")
TARGET_TOPIC = os.environ.get("TARGET_TOPIC", "visits-decorated")
JOB_VERSION = os.environ.get("JOB_VERSION", "v1.0.3")
# Restart counter set by the deployment (k8s restartCount, systemd, ...): the "attempt".
ATTEMPT = os.environ.get("JOB_ATTEMPT", "1")
COMMIT_EVERY = int(os.environ.get("COMMIT_EVERY", "500"))
OWNED_KEYS = ("job_version", "job_attempt", "processing_time_ms", "source")

Header = Tuple[str, bytes]


def build_headers(upstream: Optional[Sequence[Header]], job_version: str, attempt: str,
                  processing_time_ms: int, topic: str, partition: int, offset: int) -> List[Header]:
    """Keep upstream headers (e.g. trace ids), replace the keys this job owns. Pure function."""
    kept = [(k, v) for k, v in (upstream or []) if k not in OWNED_KEYS]
    return kept + [
        ("job_version", job_version.encode()),
        ("job_attempt", attempt.encode()),
        ("processing_time_ms", str(processing_time_ms).encode()),
        ("source", f"{topic}/{partition}/{offset}".encode()),
    ]


def headers_as_dict(headers: Optional[Sequence[Header]]) -> Dict[str, str]:
    return {k: (v.decode("utf-8", "replace") if v is not None else "") for k, v in (headers or [])}


class Decorator:
    def __init__(self) -> None:
        from confluent_kafka import Consumer, Producer

        self.consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "headers-decorator",
                                  "enable.auto.commit": False, "auto.offset.reset": "earliest"})
        # idempotent producer: broker-side retries cannot duplicate or reorder
        self.producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True,
                                  "linger.ms": 50})
        self.failed = 0
        self.running = True

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery failed for %s/%s: %s", msg.topic(), msg.partition(), err)

    def _flush_and_commit(self) -> None:
        remaining = self.producer.flush(30)
        if remaining or self.failed:
            # do not move offsets: the uncommitted records are re-read after restart
            raise RuntimeError(f"{remaining} undelivered, {self.failed} failed; offsets not committed")
        self.consumer.commit(asynchronous=False)

    def run(self) -> None:
        from confluent_kafka import KafkaError

        self.consumer.subscribe([SOURCE_TOPIC])
        since_commit = 0
        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.warning("consumer error: %s", msg.error())
                    continue
                headers = build_headers(msg.headers(), JOB_VERSION, ATTEMPT, int(time.time() * 1000),
                                        msg.topic(), msg.partition(), msg.offset())
                # value and key untouched: the decoration lives only in the metadata layer
                self.producer.produce(TARGET_TOPIC, key=msg.key(), value=msg.value(),
                                      headers=headers, on_delivery=self._on_delivery)
                self.producer.poll(0)
                since_commit += 1
                if since_commit >= COMMIT_EVERY:
                    self._flush_and_commit()
                    since_commit = 0
        finally:
            if since_commit:
                self._flush_and_commit()
            self.consumer.close()


def inspect(job_version: Optional[str], limit: int, timeout_s: float) -> int:
    """Read the decorated topic from the start with a throwaway group; print matching records."""
    from confluent_kafka import Consumer

    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"headers-inspect-{os.getpid()}",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    consumer.subscribe([TARGET_TOPIC])
    shown, deadline = 0, time.time() + timeout_s
    try:
        while shown < limit and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            meta = headers_as_dict(msg.headers())
            if job_version and meta.get("job_version") != job_version:
                continue
            print(f"{msg.partition()}/{msg.offset()} key={msg.key()!r} meta={meta}")
            shown += 1
    finally:
        consumer.close()
    return shown


def _self_test() -> None:
    upstream = [("trace_id", b"abc"), ("job_version", b"v0.9")]
    h = build_headers(upstream, "v1.0.3", "2", 1718013720000, "visits", 3, 42)
    d = headers_as_dict(h)
    assert d["trace_id"] == "abc"                       # upstream header kept
    assert d["job_version"] == "v1.0.3"                 # owned key replaced, not duplicated
    assert [k for k, _ in h].count("job_version") == 1
    assert d["source"] == "visits/3/42" and d["job_attempt"] == "2"
    assert headers_as_dict(None) == {}
    print("self-test OK")


def main() -> None:
    if "--self-test" in sys.argv:
        _self_test()
        return
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="mode", required=True)
    sub.add_parser("decorate")
    ins = sub.add_parser("inspect")
    ins.add_argument("--job-version")
    ins.add_argument("--limit", type=int, default=20)
    ins.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args()

    if args.mode == "inspect":
        log.info("matched %d records", inspect(args.job_version, args.limit, args.timeout))
        return
    decorator = Decorator()
    signal.signal(signal.SIGTERM, lambda *_: setattr(decorator, "running", False))
    decorator.run()


if __name__ == "__main__":
    main()
