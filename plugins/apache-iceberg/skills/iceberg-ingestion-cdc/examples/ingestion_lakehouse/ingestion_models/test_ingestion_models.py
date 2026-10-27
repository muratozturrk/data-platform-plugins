from datetime import date

from common.kafka_io import produce
from common.lakehouse import new_checkpoint
from ingestion_models.models import (
    clicks, commit_stats, create_click_table, create_order_tables, daily_batch, land_orders,
    measure_freshness, start_click_ingest,
)

D1, D2 = date(2025, 1, 10), date(2025, 1, 11)


def day_count(spark, table, day):
    return spark.sql(f"SELECT count(*) c FROM {table} WHERE order_date = DATE'{day}'").first().c


def setup_days(spark, ns, name):
    landing, curated = f"{ns}.{name}_landing", f"{ns}.{name}_curated"
    create_order_tables(spark, landing, curated)
    land_orders(spark, landing, D1, 1000)
    snap = land_orders(spark, landing, D2, 800, start_id=1000)
    daily_batch(spark, landing, curated, D1)
    return landing, curated, snap


def test_batch_rerun_with_append_duplicates_the_day(spark, ns):
    landing, curated, _ = setup_days(spark, ns, "append_rerun")
    daily_batch(spark, landing, curated, D2, mode="append")
    daily_batch(spark, landing, curated, D2, mode="append")      # retry after a "failure"
    assert day_count(spark, curated, D2) == 1600


def test_batch_rerun_with_overwrite_partitions_is_idempotent(spark, ns):
    landing, curated, _ = setup_days(spark, ns, "overwrite_rerun")
    daily_batch(spark, landing, curated, D2)
    before = spark.sql(f"SELECT count(*) c FROM {curated}.snapshots").first().c
    daily_batch(spark, landing, curated, D2)
    ops = [r.operation for r in spark.sql(f"SELECT operation FROM {curated}.snapshots ORDER BY committed_at").collect()]
    assert day_count(spark, curated, D2) == 800 and day_count(spark, curated, D1) == 1000
    assert ops[-1] == "overwrite" and len(ops) == before + 1   # each run is one atomic snapshot


def test_unbounded_rerun_reads_different_input_pinned_rerun_does_not(spark, ns):
    landing, curated, snap = setup_days(spark, ns, "pinned")
    daily_batch(spark, landing, curated, D2, source_snapshot_id=snap)
    land_orders(spark, landing, D2, 50, start_id=5000)          # late rows arrive in the source

    daily_batch(spark, landing, curated, D2, source_snapshot_id=snap)   # retry, bounded input
    assert day_count(spark, curated, D2) == 800
    daily_batch(spark, landing, curated, D2)                            # retry, "latest" input
    assert day_count(spark, curated, D2) == 850


def test_micro_batch_restart_processes_only_new_offsets(spark, ns, topic):
    t = f"{ns}.clicks_restart"
    create_click_table(spark, t)
    cp = new_checkpoint("restart")
    produce(topic, clicks(400))
    start_click_ingest(spark, topic, t, cp, {"availableNow": True}).awaitTermination()
    produce(topic, clicks(300, start_id=400))
    start_click_ingest(spark, topic, t, cp, {"availableNow": True}).awaitTermination()
    added = [int(r.summary["added-records"]) for r in
             spark.sql(f"SELECT summary FROM {t}.snapshots ORDER BY committed_at").collect()]
    assert added == [400, 300]
    assert spark.sql(f"SELECT count(DISTINCT click_id) c FROM {t}").first().c == 700


def test_smaller_micro_batches_create_more_commits_and_files(spark, ns, topic):
    produce(topic, clicks(2000), key=lambda r: r["click_id"])   # keyed: spread over 4 partitions
    stats = {}
    for per_trigger in (100, 1000):
        t = f"{ns}.clicks_per_trigger_{per_trigger}"
        create_click_table(spark, t)
        start_click_ingest(spark, topic, t, new_checkpoint(f"pt{per_trigger}"),
                           {"availableNow": True}, maxOffsetsPerTrigger=per_trigger).awaitTermination()
        stats[per_trigger] = commit_stats(spark, t)
        assert spark.table(t).count() == 2000
    print("\ncommit stats per maxOffsetsPerTrigger:", stats)
    # maxOffsetsPerTrigger is split across the 4 Kafka partitions proportionally to their lag,
    # so 2,000 records / 1,000 can take 3 batches: the cap is approximate, not exact.
    assert stats[100]["snapshots"] >= 20 and stats[1000]["snapshots"] in (2, 3)
    assert stats[100]["data_files"] >= 4 * stats[1000]["data_files"]
    assert stats[100]["avg_rows_per_file"] * 4 <= stats[1000]["avg_rows_per_file"]


def test_streaming_trigger_freshness_and_commit_rate(spark, ns, topic):
    t = f"{ns}.clicks_streaming"
    create_click_table(spark, t)
    q = start_click_ingest(spark, topic, t, new_checkpoint("stream"), {"processingTime": "2 seconds"})
    try:
        lags = measure_freshness(spark, topic, t, batches=3)
    finally:
        q.stop()
    print("\nseconds from produce to visible in Iceberg:", lags)
    assert all(lag < 30 for lag in lags)
    assert spark.table(t).count() == 600
    # One snapshot per micro-batch that read data; idle triggers commit nothing.
    non_empty = sum(1 for p in q.recentProgress if p["numInputRows"] > 0)
    print("micro-batches with data:", non_empty, "total progress events:", len(q.recentProgress))
    assert commit_stats(spark, t)["snapshots"] == non_empty
