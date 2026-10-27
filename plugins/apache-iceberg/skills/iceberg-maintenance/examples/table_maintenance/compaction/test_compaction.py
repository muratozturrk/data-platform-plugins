import pytest

from common.clickstream import create_events_table, day, micro_batch_ingest
from suboptimal_files.diagnose import diagnose, files_touched_by_lookup
from suboptimal_files.workloads import apply_refunds, set_row_level_mode
from compaction.compaction import (
    CompactionPolicy, closed_partitions_filter, compact, compact_mor_table, fingerprint,
    rewrite_data_files_sql, rewrite_manifests,
)

DAYS, BATCHES, ROWS, FILES = 3, 8, 2_000, 4


def build_degraded_table(spark, table, **props):
    """3 daily partitions, each written by 8 micro-batches of 4 files (96 small files)."""
    create_events_table(spark, table, **props)
    for d in range(DAYS):
        micro_batch_ingest(spark, table, BATCHES, ROWS, day(d), FILES, seed=100 * d, start_id=d * BATCHES * ROWS)


def data_files_by_day(spark, table) -> dict[str, set[str]]:
    rows = spark.sql(f"SELECT CAST(partition.event_ts_day AS STRING) d, file_path FROM {table}.data_files")
    out: dict[str, set[str]] = {}
    for r in rows.collect():
        out.setdefault(r.d, set()).add(r.file_path)
    return out


# ---------- pure unit test: no Spark needed ----------

def test_policy_renders_a_valid_procedure_call():
    sql = rewrite_data_files_sql("db.sales", CompactionPolicy(
        strategy="sort", sort_order="zorder(customer_id, product_id)",
        target_file_size_bytes=256 * 1024 * 1024, where="region = 'us-west'",
        delete_file_threshold=3,
    ))
    assert sql.startswith("CALL lake.system.rewrite_data_files(table => 'db.sales', strategy => 'sort'")
    assert "sort_order => 'zorder(customer_id, product_id)'" in sql
    assert """where => "region = 'us-west'\"""" in sql             # Spark has no '' escape
    assert "'target-file-size-bytes', '268435456'" in sql
    assert "'delete-file-threshold', '3'" in sql

    with pytest.raises(ValueError):
        rewrite_data_files_sql("db.sales", CompactionPolicy(strategy="sort"))


# ---------- integration tests against the local lakehouse ----------

def test_binpack_merges_small_files_without_changing_data(spark, ns):
    table = f"{ns}.binpack"
    build_degraded_table(spark, table)
    before, fp_before = diagnose(spark, table), fingerprint(spark, table)

    result = compact(spark, table, CompactionPolicy())

    after = diagnose(spark, table)
    assert before.data_files == DAYS * BATCHES * FILES == result.rewritten_data_files
    assert after.data_files == DAYS                    # one file per daily partition
    assert result.failed_data_files == 0
    assert fingerprint(spark, table) == fp_before      # same rows, same values


def test_compaction_materializes_mor_deletes(spark, ns):
    table = f"{ns}.mor_compaction"
    build_degraded_table(spark, table)
    set_row_level_mode(spark, table, "merge-on-read")
    for i in range(3):
        apply_refunds(spark, table, every_nth_checkout=20, refund_tag=i)
    assert diagnose(spark, table).position_delete_files > 0
    fp_before = fingerprint(spark, table)

    deletes_before = diagnose(spark, table).position_delete_files

    result, purged = compact_mor_table(spark, table, CompactionPolicy(
        delete_file_threshold=1, min_input_files=1))

    after = diagnose(spark, table)
    assert result.rewritten_data_files > 0 and after.data_files == DAYS
    assert purged == deletes_before                   # all old delete files were dangling
    assert after.position_delete_files == 0           # reads no longer merge delete files
    assert fingerprint(spark, table) == fp_before


def test_sort_compaction_restores_colocation(spark, ns):
    table = f"{ns}.sorted"
    build_degraded_table(spark, table)
    touched, total = files_touched_by_lookup(spark, table, "user_id", 42)
    assert touched / total > 0.9

    compact(spark, table, CompactionPolicy(
        strategy="sort", sort_order="user_id ASC NULLS LAST",
        target_file_size_bytes=64 * 1024, min_input_files=1, extra_options={"rewrite-all": "true"}))

    touched, total = files_touched_by_lookup(spark, table, "user_id", 42)
    assert total > DAYS                                # several sorted files per day
    assert touched <= DAYS                             # ~one file per partition holds user 42
    assert touched / total < 0.5


def test_scoped_compaction_leaves_the_open_partition_alone(spark, ns):
    table = f"{ns}.scoped"
    build_degraded_table(spark, table)
    files_before = data_files_by_day(spark, table)
    today = day(DAYS - 1)                              # the streaming job still writes here

    compact(spark, table, CompactionPolicy(where=closed_partitions_filter("event_ts", today)))

    files_after = data_files_by_day(spark, table)
    open_day = f"{today:%Y-%m-%d}"
    assert files_after[open_day] == files_before[open_day]     # untouched, no commit conflict
    for d, paths in files_after.items():
        if d != open_day:
            assert len(paths) == 1 and not paths & files_before[d]


def test_rewrite_manifests_consolidates_metadata(spark, ns):
    table = f"{ns}.manifest_sprawl"   # not "manifests": that name resolves to a metadata table
    build_degraded_table(spark, table)
    before = diagnose(spark, table)
    assert before.manifests >= DAYS * BATCHES          # one manifest per micro-batch commit

    rewritten, added = rewrite_manifests(spark, table)

    after = diagnose(spark, table)
    assert rewritten == before.manifests
    assert after.manifests == added < before.manifests
    assert after.avg_files_per_manifest > before.avg_files_per_manifest
    assert after.data_files == before.data_files       # metadata-only operation
