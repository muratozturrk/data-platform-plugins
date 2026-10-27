"""Metadata files, snapshots, sequence numbers and optimistic concurrency on real commits."""
import hashlib

import pyarrow as pa
import pytest
from pyiceberg.exceptions import CommitFailedException

from common.lakehouse import list_objects, metadata_location, read_metadata, read_object
from snapshots.snapshots import entries, manifest_paths


def current(md: dict) -> dict:
    return next(s for s in md["snapshots"] if s["snapshot-id"] == md["current-snapshot-id"])


@pytest.fixture(scope="module")
def history(spark, ns):
    """4 appends + 1 row-level delete on a v2 table; metadata.json location and bytes after each commit."""
    t = f"{ns}.ledger"
    spark.sql(f"CREATE TABLE {t} (id BIGINT, amount DOUBLE) USING iceberg "
              "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')")
    versions = [metadata_location(ns, "ledger")]
    for i in range(4):
        spark.sql(f"INSERT INTO {t} SELECT id, id * 1.5 FROM range({i * 100}, {(i + 1) * 100})")
        versions.append(metadata_location(ns, "ledger"))
    spark.sql(f"DELETE FROM {t} WHERE id % 10 = 0")
    versions.append(metadata_location(ns, "ledger"))
    return t, [(loc, hashlib.sha256(read_object(loc)).hexdigest()) for loc in versions]


def test_every_commit_writes_a_new_immutable_metadata_file(history, measure):
    _, versions = history
    locations = [loc for loc, _ in versions]
    assert len(set(locations)) == len(locations) == 6                  # create + 5 commits
    # Older files are never edited in place: same bytes after all later commits.
    assert all(hashlib.sha256(read_object(loc)).hexdigest() == digest for loc, digest in versions)
    md = read_metadata(locations[-1])
    assert [e["metadata-file"] for e in md["metadata-log"]] == locations[:-1]
    assert [e["snapshot-id"] for e in md["snapshot-log"]] == [s["snapshot-id"] for s in md["snapshots"]]
    measure("metadata_files", len(locations))
    measure("metadata_log_entries", len(md["metadata-log"]))
    measure("metadata_json_bytes", [len(read_object(loc)) for loc in locations])


def test_sequence_numbers_are_inherited_from_the_manifest_list(spark, history, measure):
    t, versions = history
    md = read_metadata(versions[-1][0])
    mine = entries(current(md))
    raw_nulls = sum(e.raw_sequence_number is None for e in mine)
    measure("entries", len(mine))
    measure("raw_null_sequence_numbers", raw_nulls)
    measure("data_seq_by_content", sorted({(e.content, e.sequence_number) for e in mine}))
    assert raw_nulls == len(mine)            # every entry was ADDED by the snapshot of its manifest
    assert {e.sequence_number for e in mine if e.content == 0} == {1, 2, 3, 4}
    assert {e.sequence_number for e in mine if e.content == 1} == {5}
    spark_view = {(r.file_path, r.sequence_number, r.file_sequence_number) for r in spark.sql(
        f"SELECT data_file.file_path, sequence_number, file_sequence_number FROM {t}.entries").collect()}
    assert spark_view == {(e.file_path, e.sequence_number, e.file_sequence_number) for e in mine}
    # A position delete applies to data files whose data sequence number is <= its own.
    assert all(e.sequence_number < 5 for e in mine if e.content == 0)


def test_manifests_are_reused_across_snapshots(history, measure):
    _, versions = history
    md = read_metadata(versions[-1][0])
    snaps = md["snapshots"]
    reused = [len(set(manifest_paths(a)) & set(manifest_paths(b))) for a, b in zip(snaps, snaps[1:])]
    measure("manifests_per_snapshot", [len(manifest_paths(s)) for s in snaps])
    measure("reused_from_parent", reused)
    assert reused == [1, 2, 3, 4]            # each append keeps all earlier manifests untouched


def test_compaction_keeps_the_data_sequence_number(spark, ns, measure):
    """rewrite_data_files: new files get the starting snapshot's data sequence number, new file seq."""
    t = f"{ns}.compacted"
    spark.sql(f"CREATE TABLE {t} (id BIGINT) USING iceberg TBLPROPERTIES ('format-version'='2')")
    for i in range(3):
        spark.sql(f"INSERT INTO {t} VALUES ({i})")
    spark.sql(f"CALL lake.system.rewrite_data_files(table => '{t}', options => map('min-input-files', '2'))")
    md = read_metadata(metadata_location(ns, "compacted"))
    snap = current(md)
    added = [e for e in entries(snap) if e.status == 1]
    measure("rewrite_snapshot", {"operation": snap["summary"]["operation"], "sequence_number": snap["sequence-number"]})
    measure("new_file", {"data_seq": added[0].sequence_number, "file_seq": added[0].file_sequence_number})
    assert snap["summary"]["operation"] == "replace" and snap["sequence-number"] == 4
    assert len(added) == 1 and added[0].raw_sequence_number == 3 and added[0].file_sequence_number == 4


def test_optimistic_concurrency_conflict_then_retry(spark, ns, catalog, measure):
    t = f"{ns}.contended"
    spark.sql(f"CREATE TABLE {t} (id BIGINT) USING iceberg TBLPROPERTIES ('format-version'='2')")
    spark.sql(f"INSERT INTO {t} VALUES (0)")                             # main exists before the race
    writer_a, writer_b = catalog.load_table(t), catalog.load_table(t)      # same base metadata
    rows = lambda v: pa.table({"id": pa.array([v], pa.int64())})
    writer_a.append(rows(1))
    winner = metadata_location(ns, "contended")
    with pytest.raises(CommitFailedException, match="branch main has changed") as err:
        writer_b.append(rows(2))
    measure("conflict", str(err.value)[:90])
    assert metadata_location(ns, "contended") == winner                    # the failed commit changed nothing
    writer_b.refresh()
    writer_b.append(rows(2))                                               # retry on the new base
    md = read_metadata(metadata_location(ns, "contended"))
    seqs = [s["sequence-number"] for s in md["snapshots"]]
    measure("sequence_numbers", seqs)
    assert seqs == [1, 2, 3] and md["last-sequence-number"] == 3
    assert sorted(r.id for r in spark.table(t).collect()) == [0, 1, 2]
    # The failed attempt left its data file behind, referenced by no snapshot: an orphan (clean with remove_orphan_files).
    referenced = {e.file_path for e in entries(current(md))}
    orphans = {u for u in list_objects(md["location"]) if u.endswith(".parquet")} - referenced
    measure("referenced_files", len(referenced))
    measure("orphan_files", len(orphans))
    assert len(referenced) == 3 and len(orphans) >= 1
