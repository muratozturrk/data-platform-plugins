"""FIFO Orderer -- Kafka consume -> filter -> forward in processing order.

Pattern: forward a subset of events as soon as possible, oldest first, with
no custom sorting. Ordering comes from (a) never having two unacknowledged
writes that can be reordered and (b) committing source offsets only after
the forward write is acknowledged.

Three producer modes, pick with FIFO_MODE:
  single     produce() + flush() per record     -> 1 request/record, slowest
  inflight1  max.in.flight=1 + linger buffering  -> bulk, but 1 request at a time
  idempotent enable.idempotence + in.flight<=5   -> bulk + ordering + no dup on retry

FIFO is NOT exactly-once: a crash between produce-ack and offset commit
re-sends the record on restart. Downstream dedup / idempotency
or Kafka transactions are needed for exactly-once.

Run:
  export KAFKA_BOOTSTRAP=localhost:9092 FIFO_MODE=idempotent
  python kafka_fifo_orderer.py
Prereqs: pip install confluent-kafka
"""
import json
import logging
import os

from confluent_kafka import Consumer, KafkaException, Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fifo-orderer")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
SOURCE, TARGET = os.environ.get("SOURCE_TOPIC", "visits"), os.environ.get("TARGET_TOPIC", "visits-alerts")
MODE = os.environ.get("FIFO_MODE", "idempotent")

PRODUCER_CONFIGS = {
    "single": {"acks": "all"},
    "inflight1": {"acks": "all", "max.in.flight.requests.per.connection": 1,
                  "queue.buffering.max.ms": 1000},
    "idempotent": {"enable.idempotence": True, "acks": "all",
                   "max.in.flight.requests.per.connection": 5, "queue.buffering.max.ms": 2000},
}


def is_interesting(event: dict) -> bool:
    return event.get("page", "").startswith("/checkout")


def on_delivery(err, msg) -> None:
    if err is not None:
        # With retries exhausted the order for this key is already broken: stop.
        raise KafkaException(err)


def main() -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP, **PRODUCER_CONFIGS[MODE]})
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"fifo-orderer-{MODE}",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    consumer.subscribe([SOURCE])
    since_commit = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            event = json.loads(msg.value())
            if is_interesting(event):
                # keep the source key: same key -> same target partition -> same order
                producer.produce(TARGET, key=msg.key(), value=msg.value(), on_delivery=on_delivery)
                if MODE == "single":
                    producer.flush()          # wait for the ack before the next record
                else:
                    producer.poll(0)          # serve delivery callbacks
            since_commit += 1
            if MODE == "single" or since_commit >= 500:
                producer.flush()              # every forwarded record acked ...
                consumer.commit(asynchronous=False)  # ... before offsets move
                since_commit = 0
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()
