"""Schema Migrator - regroup flat user fields into a `user` struct, confluent-kafka + Avro.

PHASE=transition : schema has BOTH the deprecated flat fields (login, email, age, with
                   defaults) and the new `user` record; the producer fills both from
                   the same values, so consumers migrate at their own pace.
PHASE=final      : after the deadline, flat fields are dropped. Allowed because they
                   have defaults and the subject uses non-transitive FORWARD/FULL mode.

Run: pip install "confluent-kafka[avro]"; PHASE=transition python kafka_schema_migrator_producer.py
Env: KAFKA_BOOTSTRAP (localhost:9094), SCHEMA_REGISTRY_URL (http://localhost:8081), PHASE.
"""
import json
import os

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

TOPIC = "visits_migrated"
USER_RECORD = {"type": "record", "name": "User", "fields": [
    {"name": "login", "type": "string"}, {"name": "email", "type": "string"}, {"name": "age", "type": "int"}]}
BASE_FIELDS = [{"name": "visit_id", "type": "string"}, {"name": "page", "type": "string"}]
DEPRECATED = [  # defaults make their later removal compatible
    {"name": "login", "type": ["null", "string"], "default": None, "doc": "DEPRECATED: use user.login"},
    {"name": "email", "type": ["null", "string"], "default": None, "doc": "DEPRECATED: use user.email"},
    {"name": "age", "type": ["null", "int"], "default": None, "doc": "DEPRECATED: use user.age"},
]
NEW_USER = [{"name": "user", "type": ["null", USER_RECORD], "default": None}]
SCHEMAS = {
    "transition": BASE_FIELDS + DEPRECATED + NEW_USER,
    "final": BASE_FIELDS + NEW_USER,
}


def to_record(visit: dict, phase: str) -> dict:
    user = {"login": visit["login"], "email": visit["email"], "age": visit["age"]}
    record = {"visit_id": visit["visit_id"], "page": visit["page"], "user": user}
    if phase == "transition":
        record.update(user)  # dual write from a single source of truth
    return record


def main():
    phase = os.environ.get("PHASE", "transition")
    schema = json.dumps({"type": "record", "name": "Visit", "namespace": "com.dp", "fields": SCHEMAS[phase]})
    registry = SchemaRegistryClient({"url": os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")})
    registry.set_compatibility(subject_name=f"{TOPIC}-value", level="FULL")  # non-transitive on purpose
    serializer = AvroSerializer(registry, schema, to_dict=lambda v, ctx: to_record(v, phase))
    producer = Producer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")})
    visit = {"visit_id": "v1", "page": "home", "login": "user1", "email": "u1@example.com", "age": 31}
    producer.produce(TOPIC, key=visit["visit_id"].encode(),
                     value=serializer(visit, SerializationContext(TOPIC, MessageField.VALUE)))
    producer.flush()
    print(f"Produced visit with {phase} schema")


if __name__ == "__main__":
    main()
