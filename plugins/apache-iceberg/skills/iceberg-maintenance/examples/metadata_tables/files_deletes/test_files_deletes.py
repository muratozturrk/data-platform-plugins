"""files, delete files, position deletes, entries and the all_* tables."""
from collections import Counter
from datetime import datetime, timedelta, timezone

import pyarrow.parquet as pq
import pytest

from common.reference_queries import run
from common.history import MOR, rows
from common.metaquery import delete_file_accumulation, file_lifecycle, file_size_distribution
from querying.inventory import pyiceberg_catalog


def _paths(spark, table, metadata_table, where="true"):
    return {r.file_path for r in spark.sql(
        f"SELECT file_path FROM {table}.{metadata_table} WHERE {where}").collect()}


def _first_file(spark, table, where):
    return spark.sql(f"SELECT file_path FROM {table}.all_data_files WHERE {where} "
                     "ORDER BY file_path").first().file_path


def test_files_tables_split_by_content(spark, hist):
    t = hist.table
    files = spark.sql(f"SELECT content, file_format, sort_order_id FROM {t}.files").collect()
    assert Counter(r.content for r in files) == {0: 6, 1: 2}
    assert {(r.content, r.file_format, r.sort_order_id) for r in files} == {
        (0, "PARQUET", 0), (1, "PARQUET", None)}
    assert _paths(spark, t, "files") == _paths(spark, t, "data_files") | _paths(spark, t, "delete_files")
    assert _paths(spark, t, "all_files") == _paths(spark, t, "all_data_files") | _paths(spark, t, "all_delete_files")


def test_all_data_files_adds_files_kept_for_time_travel_and_repeats_rows(spark, hist):
    t = hist.table
    all_rows = spark.sql(f"SELECT file_path FROM {t}.all_data_files").collect()
    assert (len(all_rows), len({r.file_path for r in all_rows})) == (15, 8)
    extra = _paths(spark, t, "all_data_files") - _paths(spark, t, "data_files")
    # The APAC file replaced by S4 (still in S3) and the file committed on branch dev.
    assert sorted(p.split("/data/")[1].split("/")[0] for p in extra) == ["region=APAC", "region=US"]


def test_position_deletes_point_at_the_deleted_rows(spark, hist):
    t = hist.table
    pd_rows = spark.sql(f"SELECT file_path, pos, row, delete_file_path FROM {t}.position_deletes").collect()
    assert len(pd_rows) == 3
    assert all(r.row is None for r in pd_rows)            # Spark does not write the deleted row
    assert {r.delete_file_path for r in pd_rows} == _paths(spark, t, "delete_files")
    # Read the referenced data files directly and look up the ids at the deleted positions.
    io = pyiceberg_catalog().load_table(t).io
    deleted_ids = set()
    for path in {r.file_path for r in pd_rows}:
        with io.new_input(path).open() as f:
            ids = pq.read_table(f, columns=["id"]).column("id").to_pylist()
        deleted_ids |= {ids[r.pos] for r in pd_rows if r.file_path == path}
    assert deleted_ids == {1, 2, 31}


def test_entries_status_and_file_lifecycle(spark, hist):
    t = hist.table
    current = Counter(r.status for r in spark.sql(f"SELECT status FROM {t}.entries").collect())
    assert current == {0: 5, 1: 3}
    every = Counter(r.status for r in spark.sql(f"SELECT status FROM {t}.all_entries").collect())
    assert every == {0: 7, 1: 10, 2: 1}
    replaced = _first_file(spark, t, "partition.region = 'APAC'")
    if replaced in _paths(spark, t, "data_files"):          # pick the S1 file, not its S4 replacement
        replaced = (_paths(spark, t, "all_data_files", "partition.region = 'APAC'") - {replaced}).pop()
    events = [(e["snapshot_id"], e["status"], e["committed_at"] is None, e["operation"])
              for e in file_lifecycle(spark, t, replaced)]
    # Added by expired S1 (no commit time any more), removed by the S4 overwrite.
    assert events == [(hist.s1, 1, True, None), (hist.s4, 2, False, "overwrite")]


def test_file_size_distribution(spark, hist):
    dist = file_size_distribution(spark, hist.table)
    assert dist["< 1 MiB"]["files"] == 6
    assert 6 * 1000 < dist["< 1 MiB"]["bytes"] < 6 * 2000
    assert sum(b["files"] for b in dist.values()) == 6


def test_delete_file_accumulation_v2(spark, hist):
    [eu] = delete_file_accumulation(spark, hist.table)
    assert eu["partition"]["region"] == "EU"
    assert {k: eu[k] for k in ("data_files", "live_delete_files", "live_delete_entries",
                               "live_deleted_rows", "reachable_delete_files")} == {
        "data_files": 2, "live_delete_files": 2, "live_delete_entries": 2,
        "live_deleted_rows": 3, "reachable_delete_files": 2}


def test_v3_deletion_vectors_share_one_puffin_file(spark, hist_v3):
    t = hist_v3.table
    dvs = spark.sql(f"""SELECT file_path, file_format, record_count, file_size_in_bytes,
        referenced_data_file, content_offset, content_size_in_bytes FROM {t}.delete_files""").collect()
    assert len(dvs) == 2 and {r.file_format for r in dvs} == {"PUFFIN"}
    assert len({r.file_path for r in dvs}) == 1                  # one Puffin file, two vectors
    assert len({r.file_size_in_bytes for r in dvs}) == 1          # each row reports the whole file
    assert {r.referenced_data_file for r in dvs} == _paths(spark, t, "data_files", "partition.region = 'EU' "
                                                           "AND spec_id = 0")
    assert sorted(r.record_count for r in dvs) == [1, 2]
    assert all(r.content_offset is not None and r.content_size_in_bytes > 0 for r in dvs)
    [eu] = delete_file_accumulation(spark, t)
    assert (eu["live_delete_files"], eu["live_delete_entries"], eu["reachable_delete_files"]) == (1, 2, 1)


@pytest.mark.parametrize("format_version", [2, 3])
def test_replaced_delete_files_stay_reachable_until_expiration(spark, ns, format_version):
    t = f"{ns}.deletes_v{format_version}"
    spark.sql(f"""CREATE TABLE {t} (id BIGINT, region STRING, amount DOUBLE, order_date DATE) USING iceberg
                  TBLPROPERTIES ('format-version'='{format_version}', {MOR})""")
    spark.sql(rows(1, 10)).coalesce(1).writeTo(t).append()        # one data file
    assert spark.sql(f"SELECT count(*) AS n FROM {t}.data_files").first().n == 1
    for i in (1, 2, 3):
        spark.sql(f"DELETE FROM {t} WHERE id = {i}")
    live = spark.sql(f"SELECT count(*) AS n, max(record_count) AS r FROM {t}.delete_files").first()
    assert (live.n, live.r) == (1, 3)              # one file-scoped delete / DV, rewritten each time
    assert len(_paths(spark, t, "all_delete_files")) == 3
    now = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=1)
    spark.sql(f"CALL lake.system.expire_snapshots(table => '{t}', "
              f"older_than => TIMESTAMP '{now:%Y-%m-%d %H:%M:%S}', retain_last => 1)")
    assert len(_paths(spark, t, "all_delete_files")) == 1


def test_reference_queries_files_and_deletes(spark, hist, hist_v3):
    t, t3 = hist.table, hist_v3.table
    # Small files: the commonly quoted query also returns the 2 delete files.
    assert (len(run(spark, "small-files", t)), len(run(spark, "small-files", t, fixed=True))) == (8, 6)
    assert {r.content for r in run(spark, "delete-files", t)} == {1}
    # LIKE '%parquet' keeps v2 position delete files (Parquet) and drops v3 DVs (Puffin).
    assert (len(run(spark, "readable-metrics", t)), len(run(spark, "readable-metrics", t3))) == (8, 6)
    assert run(spark, "count-position-deletes", t)[0][0] == 3
    [(part, n)] = [(r.partition.region, r.delete_count) for r in run(spark, "deletes-per-partition", t)]
    assert (part, n) == ("EU", 3)
    assert len(run(spark, "historic-size-range", t)) == 5      # includes dev's spec-1 US partition
    # Delete file totals: same answer on v2; on v3 COUNT(*) counts deletion vectors, not files.
    v2 = run(spark, "delete-file-totals", t)[0]
    v3, v3_fixed = run(spark, "delete-file-totals", t3)[0], run(spark, "delete-file-totals", t3, fixed=True)[0]
    assert (tuple(v2), tuple(v3), tuple(v3_fixed)) == ((2, 3), (2, 3), (1, 3))
    # Entries join: the inner join drops entries whose snapshot expired.
    assert (len(run(spark, "entries-join-snapshots", t)),
            len(run(spark, "entries-join-snapshots", t, fixed=True))) == (7, 18)
