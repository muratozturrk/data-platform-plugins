# kafka_static_joiner_consumer.py
#
# Static Joiner -- Kafka Consumer with In-Process Reference Dict
#
# Loads the reference dataset once at startup into a Python dict.
# Enriches every consumed event via O(1) dict lookup (no DB round-trip).
# Reference refreshed every REFERENCE_REFRESH_INTERVAL_S seconds
# by a background thread without stopping the consumer.
# Publishes enriched events to the output topic.
#
# Usage: python kafka_static_joiner_consumer.py
# Requirements: pip install kafka-python

import json
import logging
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional

from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# -- Config ------------------------------------------------------------------
KAFKA_BROKERS               = ["localhost:9092"]
SOURCE_TOPIC                = "silver.clickstream"
OUTPUT_TOPIC                = "gold.clickstream_enriched"
REFERENCE_TOPIC             = "dim.products"         # compacted topic
CONSUMER_GROUP              = "static-joiner"
PRIMARY_JOIN_KEY            = "product_id"
REFERENCE_JOIN_KEY          = "product_id"
REFERENCE_COLS              = [
    "product_name", "category", "brand", "price_tier"
]
# Refresh reference dict every 3600 seconds (1 hour)
REFERENCE_REFRESH_INTERVAL_S = 3600
# Max reference dict size -- alert if exceeded
MAX_REFERENCE_KEYS           = 500_000


# -- Reference loader: reads from a compacted Kafka topic --------------------
class KafkaReferenceLoader:
    """
    Loads a reference dataset from a Kafka compacted topic.
    Reads from the beginning of the topic to consume the latest
    value per key (log compaction retains latest per key).
    Returns a dict: {join_key_value -> {field: value, ...}}
    """

    def __init__(
        self,
        brokers:    list[str],
        topic:      str,
        join_key:   str,
        keep_cols:  list[str],
    ):
        self._brokers  = brokers
        self._topic    = topic
        self._join_key = join_key
        self._keep_cols = keep_cols

    def load(self) -> dict:
        """
        Reads all messages from the reference topic.
        Returns dict keyed by join_key value.
        Tombstone messages (value=None) are excluded.
        """
        consumer = KafkaConsumer(
            self._topic,
            bootstrap_servers  = self._brokers,
            auto_offset_reset  = "earliest",
            enable_auto_commit = False,
            # No group_id: this is a one-time full read, not ongoing consume
            value_deserializer = lambda v: json.loads(v.decode("utf-8"))
                                           if v else None,
            consumer_timeout_ms = 5_000,   # stop after 5s of no messages
        )

        ref_dict  = {}
        count     = 0

        for msg in consumer:
            if msg.value is None:
                # Tombstone: remove key (GDPR deletion propagation)
                key_val = msg.key.decode("utf-8") if msg.key else None
                if key_val and key_val in ref_dict:
                    del ref_dict[key_val]
                continue

            record   = msg.value
            join_val = str(record.get(self._join_key, "") or "")
            if not join_val:
                continue

            # Keep only needed columns to minimise dict memory
            entry = {
                col: record.get(col)
                for col in self._keep_cols
                if col in record
            }
            ref_dict[join_val] = entry
            count += 1

        consumer.close()
        log.info(
            "Reference loaded from topic=%s | keys=%d",
            self._topic, len(ref_dict),
        )
        return ref_dict


# -- Reference state with background refresh ---------------------------------
class ReferenceDict:
    """
    Thread-safe in-process reference dict.
    Background thread refreshes the dict every
    REFERENCE_REFRESH_INTERVAL_S seconds.
    Lookup is O(1) dict access -- no network round-trip per record.
    """

    def __init__(self, loader: KafkaReferenceLoader):
        self._loader  = loader
        self._dict    = {}
        self._version = 0
        self._lock    = threading.RLock()
        self._stop    = threading.Event()

        # Initial load -- synchronous, before consumer starts
        self._reload()

    def lookup(self, key: str) -> Optional[dict]:
        with self._lock:
            return self._dict.get(str(key))

    def size(self) -> int:
        with self._lock:
            return len(self._dict)

    def version(self) -> int:
        with self._lock:
            return self._version

    def _reload(self):
        new_dict = self._loader.load()
        if len(new_dict) > MAX_REFERENCE_KEYS:
            log.warning(
                "Reference dict size=%d exceeds MAX_REFERENCE_KEYS=%d. "
                "Consider moving to Spark batch join for this reference.",
                len(new_dict), MAX_REFERENCE_KEYS,
            )
        with self._lock:
            self._dict    = new_dict
            self._version += 1
        log.info(
            "Reference dict updated | version=%d size=%d",
            self._version, len(new_dict),
        )

    def start_refresh_thread(self):
        def _refresh_loop():
            while not self._stop.is_set():
                self._stop.wait(REFERENCE_REFRESH_INTERVAL_S)
                if self._stop.is_set():
                    break
                try:
                    self._reload()
                except Exception as e:
                    log.error(
                        "Reference refresh failed: %s. "
                        "Retaining previous dict.",
                        str(e),
                    )

        t = threading.Thread(target=_refresh_loop, daemon=True)
        t.start()
        return t

    def stop(self):
        self._stop.set()


# -- Enrichment logic --------------------------------------------------------
def enrich_record(
    record:    dict,
    ref:       ReferenceDict,
    join_key:  str,
) -> dict:
    """
    Looks up the reference entry for this record's join key.
    Merges reference fields into the record.
    Adds _ref_version and _ref_miss flag for downstream visibility.
    """
    join_val  = str(record.get(join_key, "") or "")
    ref_entry = ref.lookup(join_val)

    enriched = {
        **record,
        "_ref_version": ref.version(),
        "_enriched_at": datetime.now(tz=timezone.utc).isoformat(),
    }

    if ref_entry is not None:
        enriched.update(ref_entry)
        enriched["_ref_miss"] = False
    else:
        enriched["_ref_miss"] = True

    return enriched


# -- Main consumer loop ------------------------------------------------------
def run():
    loader = KafkaReferenceLoader(
        brokers   = KAFKA_BROKERS,
        topic     = REFERENCE_TOPIC,
        join_key  = REFERENCE_JOIN_KEY,
        keep_cols = REFERENCE_COLS,
    )
    ref = ReferenceDict(loader)
    ref.start_refresh_thread()

    consumer = KafkaConsumer(
        SOURCE_TOPIC,
        bootstrap_servers  = KAFKA_BROKERS,
        group_id           = CONSUMER_GROUP,
        enable_auto_commit = False,
        auto_offset_reset  = "earliest",
        value_deserializer = lambda v: json.loads(v.decode("utf-8")),
        key_deserializer   = lambda k: k.decode("utf-8") if k else None,
        max_poll_records   = 500,
        session_timeout_ms = 30_000,
        heartbeat_interval_ms = 10_000,
    )

    producer = KafkaProducer(
        bootstrap_servers = KAFKA_BROKERS,
        acks              = "all",
        retries           = 5,
        max_in_flight_requests_per_connection = 1,
        value_serializer  = lambda v: json.dumps(v).encode("utf-8"),
        key_serializer    = lambda k: str(k).encode("utf-8") if k else None,
    )

    stats    = defaultdict(int)
    last_log = time.time()

    log.info(
        "Kafka static joiner started | %s -> %s | ref_size=%d",
        SOURCE_TOPIC, OUTPUT_TOPIC, ref.size(),
    )

    try:
        while True:
            poll_result = consumer.poll(timeout_ms=5_000)
            if not poll_result:
                continue

            for partition, messages in poll_result.items():
                for msg in messages:
                    record   = msg.value
                    join_val = str(record.get(PRIMARY_JOIN_KEY, "") or "")

                    if not join_val:
                        stats["no_key"] += 1
                        continue

                    enriched = enrich_record(record, ref, PRIMARY_JOIN_KEY)
                    producer.send(
                        OUTPUT_TOPIC,
                        key   = join_val,
                        value = enriched,
                    )

                    if enriched["_ref_miss"]:
                        stats["ref_miss"] += 1
                    else:
                        stats["enriched"] += 1

            # flush before commit: an offset only moves once its output is acked
            producer.flush()
            consumer.commit()

            if time.time() - last_log > 60:
                miss_rate = (
                    stats["ref_miss"] /
                    max(stats["enriched"] + stats["ref_miss"], 1)
                    * 100
                )
                log.info(
                    "Stats | enriched=%d ref_miss=%d "
                    "(%.1f%%) no_key=%d ref_size=%d ref_v=%d",
                    stats["enriched"],
                    stats["ref_miss"],
                    miss_rate,
                    stats["no_key"],
                    ref.size(),
                    ref.version(),
                )
                last_log = time.time()

    except KeyboardInterrupt:
        log.info("Shutting down.")
    finally:
        ref.stop()
        producer.flush()
        producer.close()
        consumer.close()


if __name__ == "__main__":
    run()