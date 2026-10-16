"""API Gateway - HTTP ingestion gateway in front of Kafka (Flask + confluent-kafka).

Producers POST JSON arrays to
    /ingestion/ingest/<topic>/<event_key>
The gateway:
  * validates each record (technical schema + one semantic rule) and rejects bad payloads
    with 400 so malformed data never reaches the topic (shift-left validation);
  * adds headers (source=api, referral);
  * delivers to Kafka, waiting up to 20s for acks;
  * writes UNDELIVERED records to fallback (dead-letter) storage and still answers 202,
    so a broker outage is transparent to producers. ../spark/fallback_replay_job.py replays them.

Run:  FLASK_APP=gateway_app.py flask run --port 8080   (behind a load balancer in prod)
      curl -X POST localhost:8080/ingestion/ingest/visits/user1 -H 'Referer: web' \
           -H 'Content-Type: application/json' -d '[{"visit_id":"v1","page":"home","event_time":"2024-06-01T10:00:00Z"}]'
Env:  KAFKA_BOOTSTRAP, FALLBACK_DIR, ALLOWED_TOPICS (comma separated)
Deps: flask, confluent-kafka
"""
import json
import os
import pathlib
import uuid

from confluent_kafka import Producer
from flask import Flask, jsonify, request

app = Flask(__name__)
producer = Producer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"),
                     "acks": "all", "enable.idempotence": True})
FALLBACK_DIR = pathlib.Path(os.environ.get("FALLBACK_DIR", "/tmp/dp/fallback"))
ALLOWED_TOPICS = set(os.environ.get("ALLOWED_TOPICS", "visits").split(","))
REQUIRED_FIELDS = {"visit_id": str, "page": str, "event_time": str}
MAX_RECORDS_PER_REQUEST = 1000


def validation_error(record: dict) -> str | None:
    for field, expected in REQUIRED_FIELDS.items():
        if not isinstance(record.get(field), expected):
            return f"'{field}' missing or not {expected.__name__}"
    if not record["page"].strip():  # semantic rule: keep critical ones only, not all business logic
        return "'page' must not be blank"
    return None


@app.route("/ingestion/ingest/<string:topic>/<string:event_key>", methods=["POST"])
def new_data_with_fallback(topic: str, event_key: str):
    if topic not in ALLOWED_TOPICS:  # never let callers write to arbitrary topics
        return jsonify(error=f"unknown topic {topic}"), 404
    records = request.get_json(silent=True)
    if not isinstance(records, list) or not 0 < len(records) <= MAX_RECORDS_PER_REQUEST:
        return jsonify(error=f"body must be a JSON array of 1..{MAX_RECORDS_PER_REQUEST} records"), 400
    errors = {i: e for i, r in enumerate(records) if (e := validation_error(r if isinstance(r, dict) else {}))}
    if errors:
        return jsonify(error="invalid records", details=errors), 400

    referral = request.headers.get("Referer", "unknown")
    headers = [("source", b"api"), ("referral", referral.encode("utf-8"))]
    # Callbacks run inside flush(); each request only tracks its own records.
    delivered = [False] * len(records)

    def on_delivery(err, msg, index):
        if err is None:
            delivered[index] = True

    for i, record in enumerate(records):
        try:
            producer.produce(topic=topic, key=event_key.encode("utf-8"),
                             value=json.dumps(record).encode("utf-8"), headers=headers,
                             on_delivery=lambda err, msg, i=i: on_delivery(err, msg, i))
        except BufferError:  # local queue full -> stays undelivered -> fallback
            pass
    producer.flush(timeout=20)
    # Not acked within 20s (error OR still in flight) -> fallback. An in-flight record may still
    # land later, so the replay can duplicate it: consumers must be idempotent (see idempotency-patterns).
    failed = [r for r, ok in zip(records, delivered) if not ok]

    if failed:
        target = FALLBACK_DIR / f"topic={topic}"
        target.mkdir(parents=True, exist_ok=True)
        lines = [json.dumps({"key": event_key, "payload": r, "referral": referral}) for r in failed]
        (target / f"{uuid.uuid4()}.json").write_text("\n".join(lines))
    return jsonify(accepted=len(records), buffered_for_replay=len(failed)), 202


@app.route("/health")
def health():
    return jsonify(status="ok")
