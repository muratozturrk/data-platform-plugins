"""Wrapper -- Kafka consumer wrapping visits from several providers in one envelope.

Problem: visits from different providers, different schemas; keep the original for
debugging.

Every provider topic has its own schema. The job extracts the same computed
fields from each one and produces:

  {"raw": "<original value, byte-for-byte as a string>",
   "computed": {"visit_id", "event_time", "page", "is_connected", "page_referral_key"},
   "processing_context": {"job_version", "provider", "source", "processing_time"}}

`raw` is the undecoded original string, not a re-serialized dict, so key order,
number formatting and unknown fields survive. Records whose computed part
can't be extracted go to a dead-letter topic with the same raw value.

Delivery: produce -> flush -> commit, so a crash re-sends (at-least-once);
consumers dedup on computed.visit_id + event_time.

Run:
  export KAFKA_BOOTSTRAP=localhost:9092 JOB_VERSION=v1.0.3
  python kafka_record_wrapper.py
  python kafka_record_wrapper.py --self-test   # pure logic, no Kafka
Prereqs: pip install confluent-kafka
"""
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Callable, Dict, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("record-wrapper")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
JOB_VERSION = os.environ.get("JOB_VERSION", "dev")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "visits-wrapped")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "visits-wrapped-dlq")
COMMIT_EVERY = int(os.environ.get("COMMIT_EVERY", "500"))


def _provider_a(v: dict) -> dict:
    # {"visit_id", "page", "event_time", "referral", "user": {"connected_since"}}
    return {"visit_id": v["visit_id"], "event_time": v["event_time"], "page": v["page"],
            "referral": v.get("referral"),
            "is_connected": (v.get("user") or {}).get("connected_since") is not None}


def _provider_b(v: dict) -> dict:
    # {"id", "url", "ts_ms", "ref", "session": {"logged_in": bool}}
    return {"visit_id": str(v["id"]),
            "event_time": datetime.fromtimestamp(v["ts_ms"] / 1000, tz=timezone.utc).isoformat(),
            "page": v["url"], "referral": v.get("ref"),
            "is_connected": bool((v.get("session") or {}).get("logged_in"))}


# topic -> (provider name, extractor). Adding a provider = one entry here.
PROVIDERS: Dict[str, tuple] = {
    "visits.provider_a": ("provider_a", _provider_a),
    "visits.provider_b": ("provider_b", _provider_b),
}


def wrap(raw_value: str, provider: str, extractor: Callable[[dict], dict], source: str,
         now: Optional[str] = None) -> dict:
    """Build the envelope. Raises ValueError/KeyError/TypeError if the record can't be wrapped."""
    fields = extractor(json.loads(raw_value))
    computed = {
        "visit_id": fields["visit_id"],
        "event_time": fields["event_time"],
        "page": fields["page"],
        "is_connected": fields["is_connected"],
        # same derivation as CONCAT_WS('-', page, referral) in the Spark example (NULLs skipped)
        "page_referral_key": "-".join(x for x in (fields["page"], fields["referral"]) if x),
    }
    return {"raw": raw_value, "computed": computed, "processing_context": {
        "job_version": JOB_VERSION, "provider": provider, "source": source,
        "processing_time": now or datetime.now(timezone.utc).isoformat()}}


def run() -> None:
    from confluent_kafka import Consumer, KafkaException, Producer

    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "record-wrapper",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True, "acks": "all"})
    consumer.subscribe(list(PROVIDERS))
    pending = 0

    def commit() -> None:
        if producer.flush(30) > 0:
            raise KafkaException("records not delivered; offsets not committed")
        consumer.commit(asynchronous=False)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("consumer error: %s", msg.error())
                continue
            provider, extractor = PROVIDERS[msg.topic()]
            source = f"{msg.topic()}/{msg.partition()}/{msg.offset()}"
            try:
                raw_value = msg.value().decode("utf-8")  # UnicodeDecodeError is a ValueError
                envelope = wrap(raw_value, provider, extractor, source)
                producer.produce(OUTPUT_TOPIC, key=envelope["computed"]["visit_id"],
                                 value=json.dumps(envelope))
            except (ValueError, KeyError, TypeError) as err:
                log.error("cannot wrap %s: %s", source, err)
                producer.produce(DLQ_TOPIC, key=msg.key(), value=msg.value(), headers=[
                    ("error", str(err).encode()), ("source", source.encode())])
            producer.poll(0)
            pending += 1
            if pending >= COMMIT_EVERY:
                commit()
                pending = 0
    finally:
        commit()
        consumer.close()


def _self_test() -> None:
    raw_a = '{"visit_id": "v1", "page": "home", "event_time": "2024-06-01T10:00:00Z", ' \
            '"referral": "ads", "user": {"connected_since": "2023-01-01"}, "extra": 1.50}'
    env = wrap(raw_a, "provider_a", _provider_a, "visits.provider_a/0/7", now="t")
    assert env["raw"] == raw_a, "raw must be byte-for-byte identical"
    assert env["computed"] == {"visit_id": "v1", "event_time": "2024-06-01T10:00:00Z", "page": "home",
                               "is_connected": True, "page_referral_key": "home-ads"}
    raw_b = '{"id": 9, "url": "/cart", "ts_ms": 1717236000000, "session": {"logged_in": false}}'
    env = wrap(raw_b, "provider_b", _provider_b, "visits.provider_b/1/3", now="t")
    assert env["computed"]["visit_id"] == "9" and env["computed"]["page_referral_key"] == "/cart"
    assert env["computed"]["event_time"].startswith("2024-06-01T10:00:00")
    assert env["computed"]["is_connected"] is False and env["processing_context"]["provider"] == "provider_b"
    try:
        wrap('{"url": "/x"}', "provider_b", _provider_b, "s")
        raise AssertionError("missing id must fail")
    except KeyError:
        pass
    print("self-test OK")


if __name__ == "__main__":
    _self_test() if "--self-test" in sys.argv else run()
