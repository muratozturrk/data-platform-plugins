import pytest

from common.lakehouse import LEGACY_BUCKET, list_objects, list_uri, s3_client
from architecture_plan.pilot_migration import (migrate_by_rewrite, migrate_in_place, read_legacy,
                                                       read_warehouse)

TRADES = f"s3a://{LEGACY_BUCKET}/trades_history"


def file_formats(spark, table):
    return {r.file_format for r in spark.sql(f"SELECT file_format FROM {table}.data_files").collect()}


def test_add_files_registers_legacy_parquet_without_rewriting(spark, ns, lake_rows):
    r = migrate_in_place(spark, TRADES, f"{ns}.trades_in_place", "trade_date")
    assert r.verified and r.target_rows == lake_rows["trades_history"] == 10_000
    assert (r.data_files, r.files_outside_table_location, r.bytes_written) == (20, 20, 0)
    assert spark.sql(f"SELECT count(*) c FROM {ns}.trades_in_place.partitions").first().c == 5
    assert spark.sql(f"SELECT count(*) c FROM {ns}.trades_in_place.snapshots").first().c == 1


def test_rewrite_copies_the_same_data_into_the_table_location(spark, ns):
    r = migrate_by_rewrite(spark, spark.read.parquet(TRADES), f"{ns}.trades_rewritten", "trade_date")
    assert r.verified and r.target_rows == 10_000
    assert r.files_outside_table_location == 0 and r.bytes_written > 0
    assert 5 <= r.data_files <= 20                          # at least one file per day partition


def test_row_formats_and_warehouse_tables_are_converted_to_parquet(spark, ns, lake_rows):
    results = [
        migrate_by_rewrite(spark, read_legacy(spark, f"s3a://{LEGACY_BUCKET}/vendor_feeds", "csv"), f"{ns}.vendor_feeds"),
        migrate_by_rewrite(spark, read_legacy(spark, f"s3a://{LEGACY_BUCKET}/market_ticks", "json"), f"{ns}.market_ticks"),
        migrate_by_rewrite(spark, read_warehouse(spark, "reporting.daily_positions"), f"{ns}.daily_positions"),
        migrate_by_rewrite(spark, read_warehouse(spark, "trading.trades"), f"{ns}.trades"),
    ]
    assert all(r.verified for r in results)
    assert [r.target_rows for r in results] == [lake_rows["vendor_feeds"], lake_rows["market_ticks"], 310, 50_000]
    assert all(file_formats(spark, r.table) == {"PARQUET"} for r in results)


def test_drop_purge_on_an_add_files_table_deletes_the_legacy_files(spark, ns):
    """Fallback rule: an in-place table shares files with the legacy lake. Never PURGE it."""
    s3 = s3_client()
    for o in list_objects(LEGACY_BUCKET, "trades_history/trade_date=2024-01-01/"):
        s3.copy_object(Bucket=LEGACY_BUCKET, Key=o["Key"].replace("trades_history", "purge_probe"),
                       CopySource={"Bucket": LEGACY_BUCKET, "Key": o["Key"]})
    probe = f"s3a://{LEGACY_BUCKET}/purge_probe"
    assert len(list_uri(probe)) == 4

    migrate_in_place(spark, probe, f"{ns}.purge_probe", "trade_date")
    spark.sql(f"DROP TABLE {ns}.purge_probe")                       # rollback without PURGE
    assert len(list_uri(probe)) == 4 and spark.read.parquet(probe).count() == 2_000

    migrate_in_place(spark, probe, f"{ns}.purge_probe", "trade_date")
    spark.sql(f"DROP TABLE {ns}.purge_probe PURGE")
    assert list_uri(probe) == []                                     # the legacy copies are gone


def test_migrate_procedure_needs_an_iceberg_session_catalog(spark):
    spark.sql(f"CREATE TABLE IF NOT EXISTS spark_catalog.default.legacy_trades USING parquet LOCATION '{TRADES}'")
    try:
        with pytest.raises(Exception, match="Cannot migrate a table from a non-Iceberg Spark Session Catalog"):
            spark.sql("CALL lake.system.migrate('spark_catalog.default.legacy_trades')")
    finally:
        spark.sql("DROP TABLE IF EXISTS spark_catalog.default.legacy_trades")
    assert len(list_uri(TRADES)) == 20                               # nothing written next to the legacy files
