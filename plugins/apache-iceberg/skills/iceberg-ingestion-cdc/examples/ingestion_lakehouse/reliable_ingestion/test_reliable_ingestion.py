import json

import pytest
from pyspark.errors import StreamingQueryException

from common.kafka_io import produce
from common.lakehouse import drop_last_commit, new_checkpoint, snapshots
from reliable_ingestion.pipeline import (
    create_tables, payments, plain_append_handler, reliable_handler, start_foreach_batch,
    start_native_sink,
)


def run(query):
    query.awaitTermination()
    return query


def dupes(spark, table):
    return spark.sql(f"SELECT count(*) - count(DISTINCT event_id) d FROM {table}").first().d


def test_native_iceberg_sink_is_exactly_once_after_replay(spark, ns, topic):
    t = f"{ns}.native_sink"
    create_tables(spark, t)
    cp = new_checkpoint("native")
    produce(topic, payments(1000))
    run(start_native_sink(spark, topic, t, cp))
    produce(topic, payments(500, start_id=1000))
    run(start_native_sink(spark, topic, t, cp))
    assert spark.table(t).count() == 1500

    # Crash after the Iceberg commit, before Spark's commit log: the batch is re-run.
    replayed = drop_last_commit(cp)
    run(start_native_sink(spark, topic, t, cp))

    snaps = snapshots(spark, t)
    assert spark.table(t).count() == 1500 and dupes(spark, t) == 0
    assert [s.summary["spark.sql.streaming.epochId"] for s in snaps] == ["0", str(replayed)]
    assert len({s.summary["spark.sql.streaming.queryId"] for s in snaps}) == 1


def test_plain_foreach_batch_append_is_at_least_once(spark, ns, topic):
    t = f"{ns}.plain_append"
    create_tables(spark, t)
    cp = new_checkpoint("plain")
    produce(topic, payments(1000))
    run(start_foreach_batch(spark, topic, plain_append_handler(t), cp))
    drop_last_commit(cp)
    run(start_foreach_batch(spark, topic, plain_append_handler(t), cp))
    assert spark.table(t).count() == 2000 and dupes(spark, t) == 1000


def test_idempotent_merge_consumer_survives_replay(spark, ns, topic):
    t, dlq = f"{ns}.idempotent", f"{ns}.idempotent_dlq"
    create_tables(spark, t, dlq)
    cp = new_checkpoint("idem")
    produce(topic, payments(1000))
    run(start_foreach_batch(spark, topic, reliable_handler(t, dlq), cp))
    drop_last_commit(cp)
    run(start_foreach_batch(spark, topic, reliable_handler(t, dlq), cp))
    assert spark.table(t).count() == 1000 and dupes(spark, t) == 0


BAD = [
    "{not json",                                                         # malformed
    json.dumps({"account_id": 1, "amount": "5.00", "event_ts": "2025-01-10T00:00:00"}),  # no key
    json.dumps({"event_id": 9001, "account_id": 1, "amount": "ten", "event_ts": "2025-01-10T00:00:00"}),
    json.dumps({"event_id": 9002, "account_id": 1, "amount": "1.00", "event_ts": "yesterday"}),
]


def test_dlq_keeps_the_stream_running_and_casts_optional_fields(spark, ns, topic):
    t, dlq = f"{ns}.with_dlq", f"{ns}.with_dlq_rejects"
    create_tables(spark, t, dlq)
    good = payments(200)
    good[0]["channel"] = {"nested": "object"}       # optional field, wrong shape -> kept as text
    good[1]["currency"] = None                       # optional field missing -> NULL
    metrics = []
    produce(topic, good + BAD)
    run(start_foreach_batch(spark, topic, reliable_handler(t, dlq, metrics=metrics), new_checkpoint("dlq")))

    assert spark.table(t).count() == 200
    errors = sorted(r.error for r in spark.table(dlq).collect())
    assert errors == ["invalid amount: ten", "invalid event_ts: yesterday",
                      "malformed json", "missing event_id"]
    assert spark.sql(f"SELECT currency FROM {t} WHERE event_id = 1").first().currency is None
    assert sum(m["rejected"] for m in metrics) == 4


def test_fail_fast_stops_the_stream_and_commits_nothing(spark, ns, topic):
    t, dlq = f"{ns}.fail_fast", f"{ns}.fail_fast_rejects"
    create_tables(spark, t, dlq)
    produce(topic, payments(200) + BAD[2:3])
    q = start_foreach_batch(spark, topic, reliable_handler(t, dlq, mode="fail_fast"),
                            new_checkpoint("ff"))
    with pytest.raises(StreamingQueryException, match="invalid amount: ten"):
        q.awaitTermination()
    assert spark.table(t).count() == 0 and snapshots(spark, t) == []


def drifted(n, start_id):
    rows = payments(n, start_id)
    for r in rows:
        r["device_os"] = "android" if r["event_id"] % 2 else "ios"
    return rows


def test_schema_drift_new_field_is_added_to_the_table(spark, ns, topic):
    t, dlq = f"{ns}.drift", f"{ns}.drift_rejects"
    create_tables(spark, t, dlq)
    cp, metrics = new_checkpoint("drift"), []
    produce(topic, payments(100))
    run(start_foreach_batch(spark, topic, reliable_handler(t, dlq, metrics=metrics), cp))
    produce(topic, drifted(100, 100))
    run(start_foreach_batch(spark, topic, reliable_handler(t, dlq, metrics=metrics), cp))

    assert "device_os" in spark.table(t).columns
    counts = spark.sql(f"SELECT count(device_os) evolved, count(*) total FROM {t}").first()
    assert (counts.evolved, counts.total) == (100, 200)     # old rows read NULL, no rewrite
    assert metrics[-1]["new_fields"] == ["device_os"]
    schema_ids = {r.latest_schema_id for r in spark.sql(
        f"SELECT latest_schema_id FROM {t}.metadata_log_entries WHERE latest_schema_id IS NOT NULL").collect()}
    assert schema_ids == {0, 1}                               # schema history kept in metadata


def test_fixed_schema_parsing_silently_drops_new_fields(spark, ns, topic):
    t, dlq = f"{ns}.no_drift", f"{ns}.no_drift_rejects"
    create_tables(spark, t, dlq)
    metrics = []
    produce(topic, drifted(100, 0))
    run(start_foreach_batch(spark, topic, reliable_handler(t, dlq, evolve_schema=False, metrics=metrics),
                            new_checkpoint("nodrift")))
    assert spark.table(t).count() == 100 and "device_os" not in spark.table(t).columns
    assert metrics[0]["ignored_fields"] == ["device_os"]     # only visible because we measure it
