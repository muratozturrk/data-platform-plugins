"""Schema Compatibility Enforcer - Kafka Schema Registry (external service mode).

1. Set FORWARD compatibility on subject `visits_forward-value`
   (old consumers must read data produced with the new schema).
2. Register v1 {visit_id, event_time}.
3. Test, then try to register v2 without the required `visit_id` -> rejected with
   409 READER_FIELD_MISSING_DEFAULT_VALUE.
4. Register v3 that only adds a field -> accepted under FORWARD.

Run: pip install confluent-kafka; python schema_registry_compatibility.py
Env: SCHEMA_REGISTRY_URL (default http://localhost:8081).
"""
import json
import os

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

SUBJECT = "visits_forward-value"


def avro(fields: list) -> Schema:
    return Schema(json.dumps({"type": "record", "namespace": "com.waitingforcode.model",
                              "name": "Visit", "fields": fields}), schema_type="AVRO")


V1 = avro([{"name": "visit_id", "type": "string"},
           {"name": "event_time", "type": {"type": "int", "logicalType": "time-millis"}}])
V2_BREAKING = avro([{"name": "event_time", "type": {"type": "int", "logicalType": "time-millis"}}])
V3_ADDITIVE = avro([{"name": "visit_id", "type": "string"},
                    {"name": "event_time", "type": {"type": "int", "logicalType": "time-millis"}},
                    {"name": "page", "type": "string"}])


def main():
    client = SchemaRegistryClient({"url": os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")})
    client.set_compatibility(subject_name=SUBJECT, level="FORWARD")
    print("v1 id:", client.register_schema(SUBJECT, V1))

    for name, schema in (("v2 (drop required visit_id)", V2_BREAKING), ("v3 (add page)", V3_ADDITIVE)):
        # dry run first: what a CI gate would call before deploying a producer
        compatible = client.test_compatibility(SUBJECT, schema)
        print(f"{name}: compatible={compatible}")
        try:
            print(f"  registered id: {client.register_schema(SUBJECT, schema)}")
        except SchemaRegistryError as error:
            # e.g. 409 Schema being registered is incompatible with an earlier schema ...
            print(f"  rejected: {error.http_status_code} {error.error_message}")


if __name__ == "__main__":
    main()
