"""Secrets Pointer - Kafka SASL credentials from AWS Secrets Manager, with a TTL cache and refresh.

The consumer references the secret by name only. Credentials are cached for
SECRET_TTL_SECONDS to avoid a Secrets Manager call per (re)connection. When
the broker rejects authentication (secret rotated), the cache is invalidated
and the client is rebuilt with fresh credentials - processing resumes from
committed offsets, so writes downstream must be idempotent.

Run:
  pip install boto3 confluent-kafka
  export KAFKA_BOOTSTRAP=broker:9096 KAFKA_SECRET_ID=dp/kafka/visits-consumer AWS_REGION=eu-west-1
  python sasl_from_secrets_manager.py
"""
import json
import os
import time

import boto3
from confluent_kafka import Consumer, KafkaError

SECRET_ID = os.environ.get("KAFKA_SECRET_ID", "dp/kafka/visits-consumer")
TTL_SECONDS = int(os.environ.get("SECRET_TTL_SECONDS", "900"))


class SecretCache:
    def __init__(self, secret_id: str, ttl_seconds: int):
        self.secret_id, self.ttl = secret_id, ttl_seconds
        self.client = boto3.client("secretsmanager", region_name=os.environ.get("AWS_REGION"))
        self.value, self.fetched_at = None, 0.0

    def get(self) -> dict:
        if self.value is None or time.monotonic() - self.fetched_at > self.ttl:
            raw = self.client.get_secret_value(SecretId=self.secret_id)["SecretString"]
            self.value, self.fetched_at = json.loads(raw), time.monotonic()
        return self.value

    def invalidate(self) -> None:
        self.value = None


def build_consumer(secrets: SecretCache) -> Consumer:
    creds = secrets.get()
    return Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9096"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "SCRAM-SHA-512",
        "sasl.username": creds["username"],
        "sasl.password": creds["password"],
        "group.id": "visits-enricher",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })


def run() -> None:
    secrets = SecretCache(SECRET_ID, TTL_SECONDS)
    consumer = build_consumer(secrets)
    consumer.subscribe(["visits"])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._AUTHENTICATION:
                    print("authentication failed - refreshing credentials from Secrets Manager")
                    consumer.close()
                    secrets.invalidate()
                    consumer = build_consumer(secrets)
                    consumer.subscribe(["visits"])
                continue
            print(msg.key(), len(msg.value() or b""))  # process; never log credentials
            consumer.commit(msg, asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
