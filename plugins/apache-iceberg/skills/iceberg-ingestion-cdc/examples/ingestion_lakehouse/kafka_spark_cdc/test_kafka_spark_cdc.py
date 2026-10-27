from datetime import datetime, timedelta

from common.kafka_io import produce
from common.lakehouse import drop_last_commit, new_checkpoint
from kafka_spark_cdc.cdc import (
    create_customers, create_events, start_cdc, start_partitioned_append, table_state,
)
from kafka_spark_cdc.cdc_source import CustomerDb


def run(q):
    q.awaitTermination()


def test_cdc_stream_converges_to_the_source_state(spark, ns, topic):
    t = f"{ns}.customers_sync"
    create_customers(spark, t)
    db, cp, metrics = CustomerDb(seed=1), new_checkpoint("cdc"), []
    events = db.random_ops(1500)
    produce(topic, events)
    run(start_cdc(spark, topic, t, cp, metrics, maxOffsetsPerTrigger=300))
    tombstones = sum(1 for _, v in events if v is None)
    print(f"\n{len(events)} Kafka records ({tombstones} tombstones), {len(metrics)} micro-batches, "
          f"{len(db.rows)} live keys")
    assert len(metrics) >= 5
    assert table_state(spark, t) == db.state()


def test_many_changes_to_one_key_in_one_batch(spark, ns, topic):
    t = f"{ns}.customers_one_key"
    create_customers(spark, t)
    db = CustomerDb()
    produce(topic, db.insert(1) + db.update(1, tier="pro") + db.delete(1)
            + db.insert(1, tier="enterprise") + db.update(1, email="final@example.com")
            + db.insert(2) + db.delete(2))
    run(start_cdc(spark, topic, t, new_checkpoint("onekey")))
    assert table_state(spark, t) == db.state() == {1: (1, "customer 1", "final@example.com", "enterprise")}


def test_cdc_replay_is_idempotent(spark, ns, topic):
    t = f"{ns}.customers_replay"
    create_customers(spark, t)
    db, cp = CustomerDb(seed=2), new_checkpoint("cdcreplay")
    produce(topic, db.random_ops(500))
    run(start_cdc(spark, topic, t, cp))
    produce(topic, db.random_ops(500))
    run(start_cdc(spark, topic, t, cp))
    drop_last_commit(cp)                      # crash before Spark recorded the batch
    run(start_cdc(spark, topic, t, cp))       # same offsets processed again
    assert table_state(spark, t) == db.state()


def test_late_older_change_does_not_overwrite_newer_state(spark, ns, topic):
    t = f"{ns}.customers_late"
    create_customers(spark, t)
    db, cp = CustomerDb(), new_checkpoint("late")
    produce(topic, db.insert(7) + db.update(7, tier="enterprise"))
    run(start_cdc(spark, topic, t, cp))
    old_lsn = db.lsn - 5                      # older than the current row version
    produce(topic, db.stale_update(7, lsn=old_lsn, tier="free"))
    run(start_cdc(spark, topic, t, cp))
    assert table_state(spark, t)[7][3] == "enterprise"


def test_cdc_merges_write_delete_files_not_rewrites(spark, ns, topic):
    t = f"{ns}.customers_mor"
    create_customers(spark, t)
    db = CustomerDb(seed=3)
    produce(topic, db.random_ops(600))
    run(start_cdc(spark, topic, t, new_checkpoint("mor"), maxOffsetsPerTrigger=200))
    files = spark.sql(f"SELECT content, count(*) n FROM {t}.files GROUP BY content").collect()
    by_content = {r.content: r.n for r in files}
    merges = spark.sql(f"SELECT summary FROM {t}.snapshots WHERE operation = 'overwrite'").collect()
    print(f"\nlive files by content (0=data, 1=position deletes): {by_content}; merge snapshots: {len(merges)}")
    assert by_content.get(1, 0) > 0                       # Spark MOR uses position deletes
    assert all("deleted-data-files" not in m.summary for m in merges)


def events(n):
    t0 = datetime(2025, 1, 10)
    countries = ["DE", "FR", "TR", "US", "BR"]
    return [{"event_id": i, "country": countries[i % 5],
             "event_ts": (t0 + timedelta(hours=(i * 7) % 72)).isoformat()} for i in range(n)]


def test_distribution_mode_and_fanout_for_partitioned_streams(spark, ns, topic):
    produce(topic, events(3000), key=lambda e: e["event_id"])   # 15 partitions, arrival unsorted
    results = {}
    for distribution, fanout in (("hash", False), ("none", False), ("none", True)):
        t = f"{ns}.events_{distribution}_{str(fanout).lower()}"
        create_events(spark, t)
        try:
            run(start_partitioned_append(spark, topic, t, new_checkpoint(t), distribution, fanout))
            results[(distribution, fanout)] = spark.sql(f"SELECT count(*) n FROM {t}.files").first().n
        except Exception as e:                       # noqa: BLE001 - record what the engine does
            results[(distribution, fanout)] = str(e)
    print("\ndata files written (or error) per (distribution-mode, fanout):",
          {k: v if isinstance(v, int) else v[:120] for k, v in results.items()})
    assert results[("hash", False)] == 15                      # shuffled: one file per partition
    # No shuffle: every one of the 4 Kafka tasks writes its own file per table partition.
    # Iceberg 1.8.1 did not raise the clustered-writer error for unsorted input here.
    assert results[("none", False)] == results[("none", True)] == 4 * 15
    assert spark.table(f"{ns}.events_none_true").count() == 3000
