"""Constraints Enforcer - Avro serializer as type/nullability constraint, confluent-kafka.

The Avro schema enforces types and required (non-union => non-null) fields at produce
time: `AvroSerializer` raises before the record ever reaches the broker. Value
constraints (Avro has none) are added through a small rule hook, mirroring what
Protobuf + protovalidate gives declaratively:

    string visit_id = 1 [(buf.validate.field).string.min_len = 5];
    google.protobuf.Timestamp event_time = 2 [(buf.validate.field).timestamp.lt_now = true];
    string page = 4 [(buf.validate.field).cel = {expression: "this.endsWith('html') == false"}];

Run: pip install "confluent-kafka[avro]"; python kafka_avro_constraints_producer.py
Env: KAFKA_BOOTSTRAP (localhost:9094), SCHEMA_REGISTRY_URL (http://localhost:8081).
"""
import os
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError

TOPIC = "visits_constrained"
VISIT_SCHEMA = """
{"type": "record", "name": "Visit", "namespace": "com.dp",
 "fields": [
   {"name": "visit_id",   "type": "string"},
   {"name": "event_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
   {"name": "user_id",    "type": "string"},
   {"name": "page",       "type": "string"},
   {"name": "referral",   "type": ["null", "string"], "default": null}
 ]}
"""


def value_constraints(visit: dict, ctx) -> dict:
    """to_dict hook: value rules Avro can't express. Raise to reject the record."""
    errors = []
    if len(visit["visit_id"]) < 5:
        errors.append("visit_id shorter than 5")
    if visit["event_time"] > int(time.time() * 1000):
        errors.append("event_time in the future")
    if visit["page"].endswith("html"):
        errors.append("page cannot end with an html extension")
    if errors:
        raise ValueError("; ".join(errors))
    return visit


def main():
    registry = SchemaRegistryClient({"url": os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")})
    serializer = AvroSerializer(registry, VISIT_SCHEMA, to_dict=value_constraints)
    producer = Producer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")})
    now = int(time.time() * 1000)
    visits = [
        {"visit_id": "visit-1", "event_time": now, "user_id": "u1", "page": "home"},       # ok
        {"visit_id": "visit-2", "event_time": now, "user_id": None, "page": "contact"},     # type/null
        {"visit_id": "v3", "event_time": now + 3_600_000, "user_id": "u3", "page": "a.html"},  # values
    ]
    for visit in visits:
        try:
            value = serializer(visit, SerializationContext(TOPIC, MessageField.VALUE))
        except (SerializationError, ValueError, TypeError) as error:
            print(f"Rejected {visit['visit_id']}: {error}")
            continue
        producer.produce(TOPIC, key=visit["visit_id"].encode(), value=value)
    producer.flush()


if __name__ == "__main__":
    main()
