"""Every metadata table is queryable; schemas and counts are what we expect."""
import pytest

from common.metaquery import METADATA_TABLES
from querying.inventory import NO_TIME_TRAVEL, PYICEBERG_TABLES, pyiceberg_row_counts, row_counts, schema

# Row counts on the rich history (common/history.py), after S1 and S2 expired.
EXPECTED_COUNTS = {
    "history": 3,                # S3, S4, S5: main only, expired snapshots gone
    "metadata_log_entries": 11,  # 00000 ... 00010.metadata.json, current one included
    "snapshots": 4,              # S3, S4, S5 + S6 on branch dev
    "refs": 3,                   # main, dev, audit_s4
    "manifests": 3,
    "all_manifests": 14,         # one row per (manifest, snapshot that lists it)
    "partitions": 4,             # 3 under spec 0 + 1 under spec 1
    "entries": 8,
    "all_entries": 18,
    "files": 8,                  # 6 data + 2 position delete files
    "data_files": 6,
    "delete_files": 2,
    "all_files": 17,
    "all_data_files": 15,        # 8 distinct files, repeated across manifests
    "all_delete_files": 2,
    "position_deletes": 3,       # ids 1, 2, 31
}

# Columns commonly documented for each table: all of them exist in Iceberg 1.8.1.
DOCUMENTED_COLUMNS = {
    "history": "made_current_at snapshot_id parent_id is_current_ancestor",
    "snapshots": "committed_at snapshot_id parent_id operation manifest_list summary",
    "metadata_log_entries": "timestamp file latest_snapshot_id latest_schema_id latest_sequence_number",
    "manifests": "path length partition_spec_id added_snapshot_id added_data_files_count "
                 "existing_data_files_count deleted_data_files_count partition_summaries",
    "partitions": "partition spec_id record_count file_count total_data_file_size_in_bytes "
                  "position_delete_record_count position_delete_file_count equality_delete_record_count "
                  "equality_delete_file_count last_updated_at last_updated_snapshot_id",
    "files": "content file_path file_format spec_id record_count file_size_in_bytes column_sizes "
             "value_counts null_value_counts nan_value_counts lower_bounds upper_bounds split_offsets "
             "equality_ids sort_order_id readable_metrics",
    "position_deletes": "file_path pos row partition spec_id delete_file_path",
    "all_data_files": "file_path file_format partition record_count file_size_in_bytes column_sizes "
                      "value_counts null_value_counts lower_bounds upper_bounds spec_id sort_order_id "
                      "split_offsets",
    "all_delete_files": "file_path file_format partition record_count file_size_in_bytes column_sizes "
                        "value_counts null_value_counts lower_bounds upper_bounds split_offsets content",
    "all_entries": "status snapshot_id sequence_number file_sequence_number data_file readable_metrics",
    "all_manifests": "content path length partition_spec_id added_snapshot_id added_data_files_count "
                     "existing_data_files_count deleted_data_files_count partition_summaries "
                     "reference_snapshot_id",
    "refs": "name type snapshot_id max_reference_age_in_ms min_snapshots_to_keep max_snapshot_age_in_ms",
}

FILE_COLUMNS = ("content file_path file_format spec_id partition record_count file_size_in_bytes "
                "column_sizes value_counts null_value_counts nan_value_counts lower_bounds upper_bounds "
                "key_metadata split_offsets equality_ids sort_order_id referenced_data_file "
                "content_offset content_size_in_bytes readable_metrics").split()
MANIFEST_COLUMNS = ("content path length partition_spec_id added_snapshot_id added_data_files_count "
                    "existing_data_files_count deleted_data_files_count added_delete_files_count "
                    "existing_delete_files_count deleted_delete_files_count partition_summaries").split()
ENTRY_COLUMNS = "status snapshot_id sequence_number file_sequence_number data_file readable_metrics".split()

# Full top-level schema of every metadata table in Iceberg 1.8.1 (v2 table).
EXPECTED_COLUMNS = {
    "history": DOCUMENTED_COLUMNS["history"].split(),
    "snapshots": DOCUMENTED_COLUMNS["snapshots"].split(),
    "metadata_log_entries": DOCUMENTED_COLUMNS["metadata_log_entries"].split(),
    "refs": DOCUMENTED_COLUMNS["refs"].split(),
    "manifests": MANIFEST_COLUMNS,
    "all_manifests": MANIFEST_COLUMNS + ["reference_snapshot_id"],
    "partitions": DOCUMENTED_COLUMNS["partitions"].split(),
    "entries": ENTRY_COLUMNS,
    "all_entries": ENTRY_COLUMNS,
    **{m: FILE_COLUMNS for m in ("files", "data_files", "delete_files",
                                 "all_files", "all_data_files", "all_delete_files")},
    "position_deletes": DOCUMENTED_COLUMNS["position_deletes"].split(),
}


def test_every_metadata_table_is_queryable_with_exact_row_counts(spark, hist):
    assert row_counts(spark, hist.table) == EXPECTED_COUNTS


@pytest.mark.parametrize("metadata_table", METADATA_TABLES)
def test_schema_matches_the_iceberg_1_8_1_reference(spark, hist, metadata_table):
    cols = schema(spark, hist.table, metadata_table)
    assert list(cols) == EXPECTED_COLUMNS[metadata_table]
    documented = DOCUMENTED_COLUMNS.get(metadata_table, "").split()
    assert set(documented) <= set(cols), f"documented columns missing: {set(documented) - set(cols)}"


def test_column_types_of_key_fields(spark, hist):
    t = hist.table
    assert schema(spark, t, "snapshots")["summary"] == "map<string,string>"
    assert schema(spark, t, "history")["made_current_at"] == "timestamp"
    # The partition struct is the union of all specs' fields (spec 0 + months(order_date)).
    assert schema(spark, t, "partitions")["partition"] == "struct<region:string,order_date_month:int>"
    assert schema(spark, t, "manifests")["partition_summaries"] == (
        "array<struct<contains_null:boolean,contains_nan:boolean,lower_bound:string,upper_bound:string>>")
    assert schema(spark, t, "files")["readable_metrics"].startswith("struct<amount:struct<column_size:bigint")


def test_v3_position_deletes_add_deletion_vector_columns(spark, hist_v3):
    cols = schema(spark, hist_v3.table, "position_deletes")
    assert list(cols) == EXPECTED_COLUMNS["position_deletes"] + ["content_offset", "content_size_in_bytes"]
    assert row_counts(spark, hist_v3.table) == EXPECTED_COUNTS


def test_metadata_tables_support_time_travel(spark, hist):
    tt = [m for m in METADATA_TABLES if m not in NO_TIME_TRAVEL]
    at_s3 = row_counts(spark, hist.table, hist.s3, tt)
    assert (at_s3["files"], at_s3["data_files"], at_s3["delete_files"]) == (7, 5, 2)
    at_tag = row_counts(spark, hist.table, "audit_s4", tt)
    assert (at_tag["data_files"], at_tag["partitions"], at_tag["manifests"]) == (5, 3, 4)
    # Table-level views ignore the version: same counts as without it.
    assert {m: at_tag[m] for m in ("history", "snapshots", "refs", "metadata_log_entries")} == {
        m: EXPECTED_COUNTS[m] for m in ("history", "snapshots", "refs", "metadata_log_entries")}
    # A non-main branch name fails on metadata tables in 1.8.1 with a misleading error;
    # the branch head's snapshot id (from refs) works.
    with pytest.raises(Exception, match="Cannot find source column for partition field"):
        row_counts(spark, hist.table, "dev", ["data_files"])
    assert row_counts(spark, hist.table, hist.s6, ["data_files"]) == {"data_files": 7}
    assert row_counts(spark, hist.table, "main", ["data_files"]) == {"data_files": 6}
    with pytest.raises(Exception, match="Cannot find snapshot with ID"):
        row_counts(spark, hist.table, hist.s1, ["files"])           # expired
    for m in NO_TIME_TRAVEL:
        with pytest.raises(Exception, match="Cannot select snapshot in table"):
            row_counts(spark, hist.table, hist.s3, [m])


def test_pyiceberg_sees_the_same_metadata(hist):
    assert pyiceberg_row_counts(hist.table) == {m: EXPECTED_COUNTS[m] for m in PYICEBERG_TABLES}
