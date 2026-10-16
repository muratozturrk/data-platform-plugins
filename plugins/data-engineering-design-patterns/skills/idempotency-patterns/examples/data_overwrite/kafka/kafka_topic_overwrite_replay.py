# kafka_topic_overwrite_replay.py
#
# Kafka "overwrite" via consumer offset reset + corrected replay.
# Used when a Silver Kafka topic contains records produced by a
# buggy transformation and must be replaced with correct versions.
#
# Strategy:
#   1. Pause the downstream consumer group
#   2. Remember the log-end offset (first offset the corrections will get)
#   3. Re-publish corrected records to the Silver topic
#      (keyed so log compaction retains only the latest version)
#   4. Move the downstream group to the offset from step 2 and resume:
#      it reads only the corrected versions. Seeking back to the start of
#      the buggy window would re-deliver the buggy records first, because
#      compaction is asynchronous and has not removed them yet.
#   The group must be inactive (consumers stopped) during step 4, or the
#   offset commit is rejected / overwritten by the live members.
#
# Kafka log compaction is required on the Silver topic for this
# to work correctly: compaction retains only the latest message
# per key, so the corrected record overwrites the stale one.
#
# Usage: python kafka_topic_overwrite_replay.py \
#          --source-topic silver.orders \
#          --start-offset 4200000 \
#          --end-offset   4820000 \
#          --consumer-group silver-orders-processor
#
# Requirements: pip install kafka-python

import json
import os
import logging
import argparse
from datetime import datetime, timezone
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka import TopicPartition, OffsetAndMetadata
from kafka.admin import ConfigResource, ConfigResourceType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092").split(",")


def verify_topic_is_compacted(
    admin:      KafkaAdminClient,
    topic_name: str,
) -> bool:
    """
    Confirms the Silver topic has cleanup.policy=compact.
    Overwrite-via-replay only works correctly on compacted topics --
    without compaction, old and new records coexist and downstream
    consumers see both.
    Returns True if compacted, False otherwise.
    """
    config_resource = ConfigResource(
        resource_type = ConfigResourceType.TOPIC,
        name          = topic_name,
    )
    # kafka-python returns DescribeConfigsResponse objects; each resource is
    # (error_code, error_message, type, name, [(key, value, ...), ...]).
    for response in admin.describe_configs([config_resource]):
        for resource in response.resources:
            for entry in resource[4]:
                if entry[0] == "cleanup.policy":
                    is_compact = "compact" in (entry[1] or "")
                    log.info(
                        "Topic %s cleanup.policy=%s compacted=%s",
                        topic_name, entry[1], is_compact,
                    )
                    return is_compact
    return False


def get_log_end_offset(topic: str, partition: int = 0) -> int:
    """Offset the next produced record will get on topic/partition."""
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BROKERS)
    tp = TopicPartition(topic, partition)
    end = consumer.end_offsets([tp])[tp]
    consumer.close()
    return end


def get_current_committed_offsets(
    consumer_group: str,
    topic:          str,
    partition:      int = 0,
) -> dict[int, int]:
    """
    Returns the current committed offsets for the consumer group.
    Saved before the reset so the group can be restored if replay fails.
    Returns {partition: offset}.
    """
    consumer = KafkaConsumer(
        bootstrap_servers  = KAFKA_BROKERS,
        group_id           = consumer_group,
        enable_auto_commit = False,
    )
    tp     = TopicPartition(topic, partition)
    consumer.assign([tp])
    committed = consumer.committed(tp)
    consumer.close()

    offset = committed.offset if committed else 0
    log.info(
        "Current committed offset | group=%s topic=%s partition=%d "
        "offset=%d",
        consumer_group, topic, partition, offset,
    )
    return {partition: offset}


def reset_consumer_group_offset(
    consumer_group: str,
    topic:          str,
    partition:      int,
    target_offset:  int,
):
    """
    Seeks the consumer group offset back to target_offset.
    The downstream consumer resumes from this offset on its next poll.
    Called with the first offset of the corrected records, so the buggy
    versions are skipped even before compaction removes them.
    """
    consumer = KafkaConsumer(
        bootstrap_servers  = KAFKA_BROKERS,
        group_id           = consumer_group,
        enable_auto_commit = False,
    )
    tp = TopicPartition(topic, partition)
    consumer.assign([tp])
    consumer.seek(tp, target_offset)
    consumer.commit({tp: OffsetAndMetadata(target_offset, None)})
    consumer.close()
    log.info(
        "Consumer group offset reset | group=%s topic=%s "
        "partition=%d new_offset=%d",
        consumer_group, topic, partition, target_offset,
    )


def republish_corrected_records(
    source_topic:    str,
    target_topic:    str,
    start_offset:    int,
    end_offset:      int,
    transform_fn,
    dry_run:         bool = False,
) -> int:
    """
    Reads records from source_topic[start_offset:end_offset],
    applies transform_fn (the corrected transformation logic),
    and re-publishes to target_topic using the original message key.

    Using the original key is critical: log compaction uses the key
    to identify which records supersede which. Re-publishing with the
    same key ensures the compacted topic retains only the corrected version.

    Returns the count of records republished.
    """
    consumer = KafkaConsumer(
        bootstrap_servers  = KAFKA_BROKERS,
        group_id           = f"overwrite-replay-{source_topic}",
        enable_auto_commit = False,
        auto_offset_reset  = "earliest",
        value_deserializer = lambda v: json.loads(v.decode("utf-8")),
        key_deserializer   = lambda k: k.decode("utf-8") if k else None,
        max_poll_records   = 500,
    )

    producer = KafkaProducer(
        bootstrap_servers = KAFKA_BROKERS,
        value_serializer  = lambda v: json.dumps(v).encode("utf-8"),
        key_serializer    = lambda k: str(k).encode("utf-8") if k else None,
        acks              = "all",
        retries           = 5,
        max_in_flight_requests_per_connection = 1,
    )

    tp = TopicPartition(source_topic, 0)
    consumer.assign([tp])
    consumer.seek(tp, start_offset)

    republished = 0
    now_iso     = datetime.now(tz=timezone.utc).isoformat()

    try:
        while True:
            poll_result = consumer.poll(timeout_ms=5_000)
            if not poll_result:
                break

            for partition, messages in poll_result.items():
                for msg in messages:
                    if msg.offset >= end_offset:
                        # Past the overwrite window -- stop
                        producer.flush()
                        log.info(
                            "Replay complete at offset %d | "
                            "republished=%d",
                            msg.offset, republished,
                        )
                        return republished

                    # Apply corrected transformation
                    try:
                        corrected = transform_fn(msg.value)
                    except Exception as e:
                        log.warning(
                            "transform_fn failed for offset=%d key=%s: %s "
                            "-- skipping record",
                            msg.offset, msg.key, e,
                        )
                        continue

                    corrected["_overwrite_replay"] = True
                    corrected["_replayed_at"]      = now_iso

                    if not dry_run:
                        producer.send(
                            target_topic,
                            key   = msg.key,   # preserve original key
                            value = corrected,
                        )
                    republished += 1

            if not dry_run:
                producer.flush()

    finally:
        producer.close()
        consumer.close()

    log.info(
        "Replay ended | republished=%d dry_run=%s",
        republished, dry_run,
    )
    return republished


def run(
    source_topic:    str,
    start_offset:    int,
    end_offset:      int,
    consumer_group:  str,
    dry_run:         bool = False,
):
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS)

    # Guard: compaction must be enabled for overwrite semantics to hold
    if not verify_topic_is_compacted(admin, source_topic):
        raise RuntimeError(
            f"Topic {source_topic} does not have cleanup.policy=compact. "
            "Overwrite-via-replay requires log compaction -- without it, "
            "old and corrected records coexist and downstream consumers "
            "see both. Enable compaction before proceeding."
        )

    # Save current offset for rollback
    original_offsets = get_current_committed_offsets(
        consumer_group, source_topic, partition=0
    )
    log.info("Original offsets saved for rollback: %s", original_offsets)

    # Define the corrected transformation (replace with actual fix)
    def corrected_transform(record: dict) -> dict:
        """
        Apply the corrected business logic here.
        Example: fix amount_usd that was incorrectly divided by 1000
        instead of 100.
        """
        if "amount_usd" in record and record["amount_usd"] is not None:
            # Re-derive from raw cents (ord_amt) if available
            if "ord_amt" in record:
                record["amount_usd"] = round(
                    float(record["ord_amt"]) / 100, 2
                )
        return record

    # First offset the corrected records will occupy
    corrections_start = get_log_end_offset(source_topic, 0)
    if not dry_run and original_offsets[0] < corrections_start:
        # Moving a lagging group to corrections_start would skip records it
        # has not read yet. Let it catch up (then stop it) and rerun.
        raise RuntimeError(
            f"Group {consumer_group} is at {original_offsets[0]}, behind the "
            f"log end {corrections_start}. Let it catch up before replaying."
        )

    # Republish corrected records
    count = republish_corrected_records(
        source_topic  = source_topic,
        target_topic  = source_topic,   # re-publish to same topic
        start_offset  = start_offset,
        end_offset    = end_offset,
        transform_fn  = corrected_transform,
        dry_run       = dry_run,
    )

    # Downstream resumes at the corrected versions only
    if not dry_run and count > 0:
        reset_consumer_group_offset(
            consumer_group, source_topic, 0, corrections_start
        )

    log.info(
        "Kafka overwrite complete | topic=%s offsets=%d-%d "
        "republished=%d dry_run=%s",
        source_topic, start_offset, end_offset, count, dry_run,
    )
    admin.close()
    return {"republished": count, "original_offsets": original_offsets}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-topic",    required=True)
    parser.add_argument("--start-offset",    type=int, required=True)
    parser.add_argument("--end-offset",      type=int, required=True)
    parser.add_argument("--consumer-group",  required=True)
    parser.add_argument("--dry-run",         action="store_true")
    args = parser.parse_args()

    run(
        source_topic   = args.source_topic,
        start_offset   = args.start_offset,
        end_offset     = args.end_offset,
        consumer_group = args.consumer_group,
        dry_run        = args.dry_run,
    )