"""Encryptor - Kafka encryption in transit (TLS) + optional client-side payload encryption.

* In transit: `security.protocol=SSL` with a CA bundle and hostname verification;
  the broker should only expose TLS listeners with TLS >= 1.2.
* At rest (client-side): the producer encrypts the payload with Fernet
  (AES-128-CBC + HMAC) before sending, so brokers/disks only see ciphertext.
  The producer and consumer own the key - lose it and the data is gone.
  Key comes from env (fed by a secrets manager), never from code.

Run:
  pip install confluent-kafka cryptography
  export KAFKA_BOOTSTRAP=broker:9093 KAFKA_CA=/etc/kafka/ca.pem \
         PAYLOAD_KEY=$(python -c "from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())")
  python encrypted_producer_consumer.py produce|consume
"""
import json
import os
import sys

from confluent_kafka import Consumer, Producer
from cryptography.fernet import Fernet, MultiFernet

TOPIC = os.environ.get("TOPIC", "visits_encrypted")

TLS_CONFIG = {
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9093"),
    "security.protocol": "SSL",
    "ssl.ca.location": os.environ.get("KAFKA_CA", "/etc/kafka/ca.pem"),
    "ssl.endpoint.identification.algorithm": "https",  # verify broker hostname
}


def cipher() -> MultiFernet:
    """PAYLOAD_KEY = current key; PAYLOAD_OLD_KEYS = comma-separated keys still accepted for decryption
    (lets you rotate without breaking consumers of older records)."""
    keys = [os.environ["PAYLOAD_KEY"]] + [k for k in os.environ.get("PAYLOAD_OLD_KEYS", "").split(",") if k]
    return MultiFernet([Fernet(k) for k in keys])


def produce() -> None:
    producer = Producer({**TLS_CONFIG, "enable.idempotence": True})
    f = cipher()
    for i in range(5):
        visit = {"visit_id": f"v{i}", "user_id": f"u{i}", "ip": f"10.0.0.{i}"}
        producer.produce(TOPIC, key=visit["visit_id"].encode(), value=f.encrypt(json.dumps(visit).encode()))
    if producer.flush(30) > 0:
        raise RuntimeError("undelivered messages")


def consume() -> None:
    consumer = Consumer({**TLS_CONFIG, "group.id": "visits-decryptor", "auto.offset.reset": "earliest"})
    consumer.subscribe([TOPIC])
    f = cipher()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(msg.error())
                continue
            print(json.loads(f.decrypt(msg.value())))
    finally:
        consumer.close()


if __name__ == "__main__":
    {"produce": produce, "consume": consume}[sys.argv[1] if len(sys.argv) > 1 else "produce"]()
