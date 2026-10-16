# kafka_topic_compaction_setup.py
#
# Configures Kafka log compaction on topics that contain entity state
# (e.g. CDC topics, customer profiles, product catalog).
# Log compaction retains only the latest message per key —
# old versions are cleaned up by the Kafka broker automatically.
#
# Also demonstrates how to check compaction lag and monitor topic health.
#
# Usage: python kafka_topic_compaction_setup.py
# Requirements: pip install kafka-python

import json
import logging
from kafka import KafkaAdminClient
from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BROKERS = ["localhost:9092"]

# ── Topic compaction configurations ───────────────────────────────────────────

COMPACTED_TOPICS = [
    {
        "name":       "silver.orders",
        "partitions": 12,
        "replication_factor": 3,
        "config": {
            # Enable log compaction — broker retains latest value per key
            "cleanup.policy": "compact",

            # How long to retain a message AFTER it has been superseded
            # by a newer message with the same key. 24 hours = old versions
            # kept for 24h even after a newer version arrives.
            "min.compaction.lag.ms": str(24 * 60 * 60 * 1000),

            # Maximum time before a message can be compacted away
            # (even if not superseded). Prevents indefinite retention of
            # messages with unique keys.
            "max.compaction.lag.ms": str(7 * 24 * 60 * 60 * 1000),

            # Dirty ratio: fraction of log that is "dirty" (uncompacted)
            # before the compaction thread kicks in.
            # Lower = more frequent compaction, less lag.
            "min.cleanable.dirty.ratio": "0.1",

            # Segment size: smaller segments allow more frequent compaction
            # of recent data. 256 MB is a good balance.
            "segment.bytes": str(256 * 1024 * 1024),

            # Keep deleted records (tombstones) for 24 hours before
            # the compaction thread removes them. This gives consumers
            # time to see the delete event.
            "delete.retention.ms": str(24 * 60 * 60 * 1000),

            # Compression: lz4 for good balance of speed and ratio
            "compression.type": "lz4",
        }
    },
    {
        "name":       "silver.customers",
        "partitions": 6,
        "replication_factor": 3,
        "config": {
            # Combine compaction + time-based deletion:
            # keep latest per key AND delete records older than 90 days
            "cleanup.policy":         "compact,delete",
            "retention.ms":           str(90 * 24 * 60 * 60 * 1000),
            "min.compaction.lag.ms":  str(24 * 60 * 60 * 1000),
            "min.cleanable.dirty.ratio": "0.1",
            "segment.bytes":          str(256 * 1024 * 1024),
            "delete.retention.ms":    str(24 * 60 * 60 * 1000),
            "compression.type":       "lz4",
        }
    },
]


def create_or_update_topics():
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS)

    for topic_def in COMPACTED_TOPICS:
        name   = topic_def["name"]
        config = topic_def["config"]

        # ── Create topic if it doesn't exist ──────────────────────────
        try:
            admin.create_topics([
                NewTopic(
                    name               = name,
                    num_partitions     = topic_def["partitions"],
                    replication_factor = topic_def["replication_factor"],
                    topic_configs      = config,
                )
            ])
            log.info("Created topic: %s", name)
        except TopicAlreadyExistsError:
            # ── Update config on existing topic ────────────────────────
            log.info("Topic exists, updating config: %s", name)
            config_resource = ConfigResource(
                resource_type = ConfigResourceType.TOPIC,
                name          = name,
                configs       = config,
            )
            admin.alter_configs([config_resource])
            log.info("Config updated: %s", name)

    admin.close()


def describe_topic_config(topic_name: str):
    """Print current compaction config for a topic."""
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS)
    config_resource = ConfigResource(
        resource_type = ConfigResourceType.TOPIC,
        name          = topic_name,
    )
    configs = admin.describe_configs([config_resource])

    log.info("Config for topic '%s':", topic_name)
    for resource, config_entries in configs.items():
        for key, entry in config_entries.items():
            if key in ("cleanup.policy", "min.compaction.lag.ms",
                       "retention.ms", "min.cleanable.dirty.ratio",
                       "segment.bytes", "compression.type"):
                log.info("  %s = %s", key, entry.value)
    admin.close()


if __name__ == "__main__":
    create_or_update_topics()

    log.info("\nVerifying configs:")
    for topic_def in COMPACTED_TOPICS:
        describe_topic_config(topic_def["name"])