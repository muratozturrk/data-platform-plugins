"""The "data swamp" failure modes of a plain file lake, and what a table format changes.

Plain lake = Parquet files in a directory read by Spark (local file system, same semantics as HDFS;
object stores are worse because they have no atomic rename). Iceberg table = REST catalog + S3.
"""
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F

from common.lakehouse import list_keys, table_location
from common.orders import orders
from plain_data_lake.plain_lake import (
    WriterCrashed, overwrite_iceberg, overwrite_plain, poison, upload_uncommitted_files, write_batch_files)


def _df(spark, table: pa.Table):
    return spark.createDataFrame(table.to_pandas())


def test_plain_lake_readers_see_half_a_batch(spark, tmp_path):
    lake = f"{tmp_path}/orders"
    write_batch_files(lake, orders(4_000), n_files=4)
    with pytest.raises(WriterCrashed):
        write_batch_files(lake, orders(4_000, seed=8, first_id=4_001), n_files=4, crash_after=2)
    # No commit protocol: the 2 files that landed are now "the data". Nobody can tell they are partial.
    assert spark.read.parquet(lake).count() == 6_000


def test_iceberg_uncommitted_files_are_invisible(spark, ns):
    t = f"{ns}.orders_atomic"
    _df(spark, orders(4_000)).writeTo(t).create()
    snapshots_before = spark.sql(f"SELECT count(*) c FROM {t}.snapshots").first().c
    stray = upload_uncommitted_files(spark, t, orders(4_000, seed=8, first_id=4_001), n_files=2)

    assert set(stray) <= set(list_keys(table_location(spark, t) + "/data/"))   # physically there...
    assert spark.table(t).count() == 4_000                                     # ...but not in the table
    assert spark.sql(f"SELECT count(*) c FROM {t}.snapshots").first().c == snapshots_before
    assert not set(stray) & {r.file_path for r in spark.sql(f"SELECT file_path FROM {t}.data_files").collect()}


def test_plain_lake_failed_overwrite_destroys_previous_data(spark, tmp_path):
    lake = f"{tmp_path}/daily_snapshot"
    df = _df(spark, orders(2_000))
    overwrite_plain(df, lake)
    assert spark.read.parquet(lake).count() == 2_000

    with pytest.raises(Exception, match="bad record"):
        overwrite_plain(poison(df, bad_order_id=1_500), lake)
    # Spark deletes the target before it writes. The job failed, so the old data is simply gone.
    with pytest.raises(AnalysisException, match="UNABLE_TO_INFER_SCHEMA"):
        spark.read.parquet(lake).count()


def test_iceberg_failed_overwrite_keeps_previous_snapshot(spark, ns):
    t = f"{ns}.daily_snapshot"
    df = _df(spark, orders(2_000))
    df.writeTo(t).create()
    snap = spark.sql(f"SELECT snapshot_id FROM {t}.snapshots").first().snapshot_id

    with pytest.raises(Exception, match="bad record"):
        overwrite_iceberg(poison(df, bad_order_id=1_500), t)
    assert spark.table(t).count() == 2_000
    assert spark.sql(f"SELECT snapshot_id FROM {t}.snapshots ORDER BY committed_at DESC").first().snapshot_id == snap

    overwrite_iceberg(df.where("order_id <= 1000"), t)                 # a successful overwrite: one commit
    assert spark.table(t).count() == 1_000
    assert spark.sql(f"SELECT count(*) c FROM {t} VERSION AS OF {snap}").first().c == 2_000


def _drifted_batch() -> pa.Table:
    """Upstream team changed `amount` to a string and added `currency`, without telling anyone."""
    b = orders(1_000, seed=8, first_id=1_001)
    b = b.set_column(b.schema.get_field_index("amount"), "amount", pc.cast(b["amount"], pa.string()))
    return b.append_column("currency", pa.array(["EUR"] * b.num_rows))


def test_plain_lake_accepts_schema_drift_and_breaks_readers(spark, tmp_path):
    lake = f"{tmp_path}/orders_drift"
    write_batch_files(lake, orders(1_000), n_files=2)
    write_batch_files(lake, _drifted_batch(), n_files=2)          # the write "succeeds"

    df = spark.read.parquet(lake)
    # The schema comes from ONE file footer, whichever Spark picks first: either `currency` is silently
    # missing or `amount` is a string for every file. Which one varies between runs.
    old, new = orders(1).schema, _drifted_batch().schema
    assert set(df.columns) in ({*old.names}, {*new.names})
    with pytest.raises(Exception, match="Parquet column cannot be converted"):
        df.agg(F.sum("amount")).collect()                         # the reader fails at query time
    with pytest.raises(Exception, match="CANNOT_MERGE_SCHEMAS"):
        spark.read.option("mergeSchema", "true").parquet(lake).columns


def test_iceberg_rejects_drift_at_write_time_and_evolves_explicitly(spark, ns):
    t = f"{ns}.orders_drift"
    _df(spark, orders(1_000)).writeTo(t).create()

    with pytest.raises(AnalysisException):
        _df(spark, _drifted_batch()).writeTo(t).append()          # wrong types + unknown column: rejected
    assert spark.table(t).count() == 1_000

    with pytest.raises(Exception):
        spark.sql(f"ALTER TABLE {t} ALTER COLUMN amount TYPE string")   # unsafe type change: rejected

    spark.sql(f"ALTER TABLE {t} ADD COLUMN currency string")      # explicit, metadata-only evolution
    fixed = _drifted_batch()
    fixed = fixed.set_column(fixed.schema.get_field_index("amount"), "amount", pc.cast(fixed["amount"], pa.float64()))
    _df(spark, fixed).writeTo(t).append()
    rows = spark.sql(f"SELECT currency IS NULL AS old, count(*) c, round(sum(amount), 2) s FROM {t} GROUP BY 1").collect()
    assert {r.old: r.c for r in rows} == {True: 1_000, False: 1_000}   # old rows read as NULL, no rewrite
