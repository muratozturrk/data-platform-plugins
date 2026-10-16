"""Single Runner -- Kafka: guarantee one active instance with transactional fencing.

A stateful, order-sensitive processor (e.g. incremental sessionizer) must never run twice in
parallel. Kafka gives two building blocks:
  * a STABLE `transactional.id`: when a second instance calls init_transactions() with the
    same id, the broker bumps the producer epoch and FENCES the older one -- its next
    transactional call fails with a fatal error, so it stops instead of writing duplicates;
  * a consumer group on the input topic: offsets + writes are committed atomically.

Run two copies of this script: the older one exits with "fenced" once the newer one starts.
    export KAFKA_BOOTSTRAP=localhost:9092
    python single_runner_fencing.py
Requires: confluent-kafka>=2.0; topics visits, sessions.
"""
import os
import sys

from confluent_kafka import Consumer, KafkaException, Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TRANSACTIONAL_ID = "visits-sessionizer"  # same for every instance -> only one survives


def main() -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "transactional.id": TRANSACTIONAL_ID})
    producer.init_transactions()  # fences any previous instance with this id
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "visits-sessionizer",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
    })
    consumer.subscribe(["visits"])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                producer.begin_transaction()
                producer.produce("sessions", key=msg.key(), value=msg.value())
                producer.send_offsets_to_transaction(
                    consumer.position(consumer.assignment()), consumer.consumer_group_metadata())
                producer.commit_transaction()
            except KafkaException as error:
                if error.args[0].fatal():  # e.g. PRODUCER_FENCED: a newer runner took over
                    sys.exit(f"fenced, another instance is active: {error}")
                producer.abort_transaction()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
