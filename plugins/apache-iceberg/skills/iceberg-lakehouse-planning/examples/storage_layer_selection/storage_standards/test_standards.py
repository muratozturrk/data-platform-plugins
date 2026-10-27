import io

import pyarrow.parquet as pq
import pytest

from common.lakehouse import events_df
from common.s3 import client, count_requests, empty_bucket, ensure_bucket, split_uri

from .parquet_ranges import S3RangeFile, write_wide_table
from .s3_compat import CHECKS, run_checks

BUCKET = "standards"
STORES = ["seaweedfs", "rustfs"]


@pytest.fixture(scope="module")
def wide_file():
    for store in STORES:
        ensure_bucket(store, BUCKET)
    s3 = client("seaweedfs")
    size = write_wide_table(s3, BUCKET, "parquet/wide.parquet")
    yield s3, "parquet/wide.parquet", size
    for store in STORES:
        empty_bucket(store, BUCKET)


# ---------- Parquet on object storage ----------

def test_reader_starts_with_a_range_read_of_the_footer(wide_file, measure):
    s3, key, size = wide_file
    f = S3RangeFile(s3, BUCKET, key)
    meta = pq.ParquetFile(f).metadata
    measure("footer", f"file={size} B, requests={f.requests}, row_groups={meta.num_row_groups}")
    assert meta.num_row_groups == 10 and meta.num_columns == 10
    first_offset, _ = f.requests[0]
    assert first_offset > 0                       # the first GET targets the end of the file
    assert f.bytes_read < 0.05 * size             # schema + stats of all row groups, no data


def test_column_projection_reads_only_the_needed_column_chunks(wide_file, measure):
    s3, key, size = wide_file
    f = S3RangeFile(s3, BUCKET, key)
    col = pq.ParquetFile(f).read(columns=["m1"])
    measure("projection", f"1 of 10 columns: {f.bytes_read} of {size} B "
                          f"({f.bytes_read / size:.1%}) in {len(f.requests)} range GETs")
    assert col.num_rows == 200_000
    assert f.bytes_read < 0.15 * size


def test_row_group_statistics_skip_data_that_cannot_match(wide_file, measure):
    s3, key, size = wide_file
    f = S3RangeFile(s3, BUCKET, key)
    t = pq.read_table(f, filters=[("id", ">=", 45_000), ("id", "<", 55_000)])
    measure("predicate_pushdown", f"id in [45000, 55000): {f.bytes_read} of {size} B "
                                  f"({f.bytes_read / size:.1%}), rows={t.num_rows}")
    assert t.num_rows == 10_000
    assert f.bytes_read < 0.30 * size             # 2 of 10 row groups + footer


# ---------- Iceberg on top of Parquet ----------

def test_iceberg_tracks_columns_by_field_id_so_rename_needs_no_rewrite(spark, ns, measure):
    t = f"lake.{ns}.customers"
    spark.sql(f"CREATE TABLE {t} (id BIGINT, name STRING) USING iceberg")
    spark.sql(f"INSERT INTO {t} VALUES (1, 'Ada')")
    old_files = {r.file_path for r in spark.sql(f"SELECT file_path FROM {t}.files").collect()}
    spark.sql(f"ALTER TABLE {t} RENAME COLUMN name TO full_name")
    spark.sql(f"INSERT INTO {t} VALUES (2, 'Grace')")
    files = {r.file_path for r in spark.sql(f"SELECT file_path FROM {t}.files").collect()}
    schemas = {}
    for path in files:
        b, k = split_uri(path)
        schema = pq.ParquetFile(io.BytesIO(client().get_object(Bucket=b, Key=k)["Body"].read())).schema_arrow
        schemas[path in old_files] = {f.name: f.metadata[b"PARQUET:field_id"].decode() for f in schema}
    measure("parquet_field_ids", f"old file {schemas[True]}, new file {schemas[False]}")
    assert old_files < files                                    # old file kept, not rewritten
    assert schemas[True] == {"id": "1", "name": "2"}            # physical name is stale ...
    assert schemas[False] == {"id": "1", "full_name": "2"}      # ... but the field id is the same
    rows = spark.sql(f"SELECT id, full_name FROM {t} ORDER BY id").collect()
    assert [tuple(r) for r in rows] == [(1, "Ada"), (2, "Grace")]


def test_manifest_statistics_prune_data_files_before_any_data_is_read(spark, ns, measure):
    t = f"lake.{ns}.orders"
    spark.sql(f"CREATE TABLE {t} (order_id BIGINT, customer_id BIGINT, country STRING, amount DOUBLE, "
              f"event_day DATE) USING iceberg PARTITIONED BY (event_day)")
    events_df(spark, 40_000, n_days=8).writeTo(t).append()
    n_files = spark.sql(f"SELECT count(*) c FROM {t}.files").first().c
    spark.sql(f"REFRESH TABLE {t}")
    with count_requests("warehouse") as full:
        spark.sql(f"SELECT sum(amount) FROM {t}").collect()
    with count_requests("warehouse") as pruned:
        spark.sql(f"SELECT sum(amount) FROM {t} WHERE event_day = DATE '2025-01-03'").collect()
    measure("full_scan_requests", dict(full))
    measure("pruned_scan_requests", dict(pruned))
    assert n_files == 8
    # Each data file read costs a footer GET plus column chunk GETs; pruning skips 7 of 8 files.
    assert pruned.ranged_gets * 3 < full.ranged_gets


# ---------- S3 API compatibility ----------

@pytest.fixture(scope="module")
def compat(wide_file):
    return {store: run_checks(client(store), BUCKET, "compat") for store in STORES}


def test_both_stores_pass_every_check_iceberg_s3fileio_needs(compat, measure):
    required = [c.name for c in CHECKS if c.required]
    for store in STORES:
        measure(f"{store}_checks", compat[store])
        assert [n for n in required if not compat[store][n]] == []


def test_s3_compatible_stores_differ_in_namespace_semantics(compat):
    """SeaweedFS stores objects in a filer with real directories; RustFS behaves like AWS S3."""
    assert compat["rustfs"]["no_directory_objects"] and compat["rustfs"]["empty_prefix_vanishes"]
    assert not compat["seaweedfs"]["no_directory_objects"]
    assert not compat["seaweedfs"]["empty_prefix_vanishes"]
    for store in STORES:                                       # both implement S3 conditional writes
        assert compat[store]["if_none_match_put"] and compat[store]["if_match_put"]


def test_same_iceberg_workload_runs_unchanged_on_both_s3_implementations(spark, uid):
    results = {}
    for cat in ("lake", "rust"):
        t = f"{cat}.compat_{uid}.orders"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.compat_{uid}")
        spark.sql(f"CREATE TABLE {t} (order_id BIGINT, customer_id BIGINT, country STRING, "
                  f"amount DOUBLE, event_day DATE) USING iceberg PARTITIONED BY (event_day)")
        events_df(spark, 10_000).writeTo(t).append()
        spark.sql(f"DELETE FROM {t} WHERE country = 'US'")
        spark.sql(f"CALL {cat}.system.rewrite_data_files(table => 'compat_{uid}.orders')")
        results[cat] = tuple(spark.sql(
            f"SELECT count(*), round(sum(amount), 2), count(DISTINCT country) FROM {t}").first())
        locations = {r.file_path.split("/")[2] for r in spark.sql(f"SELECT file_path FROM {t}.files").collect()}
        assert locations == {"warehouse"}
        spark.sql(f"DROP TABLE {t} PURGE")
        spark.sql(f"DROP NAMESPACE {cat}.compat_{uid}")
    assert results["lake"] == results["rust"] and results["lake"][2] == 7


# ---------- Iceberg FileIO implementations ----------

def test_s3fileio_avoids_the_filesystem_emulation_calls_of_hadoopfileio(spark, ns, measure):
    """Same REST catalog, same table, same append: only the FileIO implementation differs."""
    t = f"{ns}.fileio"
    spark.sql(f"CREATE TABLE lake.{t} (order_id BIGINT, amount DOUBLE) USING iceberg")
    df = events_df(spark, 5_000).select("order_id", "amount").repartition(4)
    with count_requests("warehouse") as s3fileio:
        df.writeTo(f"lake.{t}").append()
    with count_requests("warehouse") as hadoopfileio:
        df.writeTo(f"lake_hadoop.{t}").append()
    measure("S3FileIO_append", dict(s3fileio))
    measure("HadoopFileIO_s3a_append", dict(hadoopfileio))
    assert s3fileio.by_type("LIST") == 0 and s3fileio.by_type("DELETE") == 0
    assert hadoopfileio.by_type("LIST") > 0                 # directory probes
    assert hadoopfileio.by_type("DELETE") > 0               # fake directory marker cleanup
    assert hadoopfileio.total > s3fileio.total
    spark.sql(f"REFRESH TABLE lake.{t}")                    # the other catalog committed
    assert spark.table(f"lake.{t}").count() == 10_000
