"""Secretless Connector - Kafka mutual TLS (mTLS) client with confluent-kafka.

The client authenticates with a certificate signed by the cluster's CA; the
broker maps the certificate subject (e.g. CN=visits-processor) to a principal
used by ACLs (`ssl.principal.mapping.rules`). No username/password to store.

Rotation: issue the new cert before the old one expires, deploy it, keep the
CA trusting both until every client switched. Short-lived certs from an
automated PKI (cert-manager, Vault PKI, ACM PCA) remove the manual work.
If the private key is password-protected, that password IS a secret again -
prefer unencrypted keys on a restricted, platform-mounted volume.

Run:
  pip install confluent-kafka
  export KAFKA_BOOTSTRAP=broker:9094 KAFKA_CA=/certs/ca.pem \
         KAFKA_CLIENT_CERT=/certs/client.pem KAFKA_CLIENT_KEY=/certs/client.key
  python mtls_client.py produce|consume
"""
import json
import os
import sys

from confluent_kafka import Consumer, Producer

TOPIC = os.environ.get("TOPIC", "visits")

MTLS_CONFIG = {
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"),
    "security.protocol": "SSL",
    "ssl.ca.location": os.environ.get("KAFKA_CA", "/certs/ca.pem"),
    "ssl.certificate.location": os.environ.get("KAFKA_CLIENT_CERT", "/certs/client.pem"),
    "ssl.key.location": os.environ.get("KAFKA_CLIENT_KEY", "/certs/client.key"),
    "ssl.endpoint.identification.algorithm": "https",
}


def produce() -> None:
    producer = Producer({**MTLS_CONFIG, "enable.idempotence": True})
    for i in range(3):
        producer.produce(TOPIC, key=f"v{i}".encode(), value=json.dumps({"visit_id": f"v{i}"}).encode())
    if producer.flush(30) > 0:
        raise RuntimeError("undelivered messages - check certificate/ACLs")


def consume() -> None:
    consumer = Consumer({**MTLS_CONFIG, "group.id": "visits-processor", "auto.offset.reset": "earliest"})
    consumer.subscribe([TOPIC])
    try:
        for _ in range(30):
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(msg.error())  # SSL handshake errors = cert/CA mismatch or expired cert
                continue
            print(msg.key(), msg.value())
    finally:
        consumer.close()


if __name__ == "__main__":
    {"produce": produce, "consume": consume}[sys.argv[1] if len(sys.argv) > 1 else "produce"]()
