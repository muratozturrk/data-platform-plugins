"""Snapshot retention: expiration, tags as retention anchors, orphan cleanup."""
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import SparkSession

from common.lakehouse import CATALOG, list_objects, table_location


@dataclass(frozen=True)
class ExpireResult:
    deleted_data_files: int
    deleted_position_delete_files: int
    deleted_manifest_files: int
    deleted_manifest_lists: int


def _ts(value: datetime) -> str:
    return f"TIMESTAMP '{value:%Y-%m-%d %H:%M:%S.%f}'"


def expire_snapshots(spark: SparkSession, table: str, older_than: datetime | None = None,
                     retain_last: int | None = None) -> ExpireResult:
    """Expire snapshots and delete files only they reference.

    Without arguments the procedure falls back to the table's retention properties
    (history.expire.max-snapshot-age-ms, history.expire.min-snapshots-to-keep), which
    is how retention policy is usually declared per table in production.
    """
    args = [f"table => '{table}'"]
    if older_than is not None:
        args.append(f"older_than => {_ts(older_than)}")
    if retain_last is not None:
        args.append(f"retain_last => {retain_last}")
    row = spark.sql(f"CALL {CATALOG}.system.expire_snapshots({', '.join(args)})").first()
    return ExpireResult(
        deleted_data_files=row.deleted_data_files_count,
        deleted_position_delete_files=row.deleted_position_delete_files_count,
        deleted_manifest_files=row.deleted_manifest_files_count,
        deleted_manifest_lists=row.deleted_manifest_lists_count,
    )


def set_retention_policy(spark: SparkSession, table: str, max_snapshot_age_ms: int,
                         min_snapshots_to_keep: int, max_ref_age_ms: int | None = None) -> None:
    props = {
        "history.expire.max-snapshot-age-ms": max_snapshot_age_ms,
        "history.expire.min-snapshots-to-keep": min_snapshots_to_keep,
    }
    if max_ref_age_ms is not None:
        props["history.expire.max-ref-age-ms"] = max_ref_age_ms
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ("
              + ", ".join(f"'{k}'='{v}'" for k, v in props.items()) + ")")


def create_tag(spark: SparkSession, table: str, tag: str, snapshot_id: int | None = None,
               retain_days: int | None = None) -> None:
    """Pin a snapshot (audit, backfill, pre-migration state) so expiration keeps it."""
    as_of = f" AS OF VERSION {snapshot_id}" if snapshot_id else ""
    retain = f" RETAIN {retain_days} DAYS" if retain_days else ""
    spark.sql(f"ALTER TABLE {table} CREATE TAG `{tag}`{as_of}{retain}")


def drop_tag(spark: SparkSession, table: str, tag: str) -> None:
    spark.sql(f"ALTER TABLE {table} DROP TAG IF EXISTS `{tag}`")


def snapshot_ids(spark: SparkSession, table: str) -> list[int]:
    return [r.snapshot_id for r in
            spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at").collect()]


def tags(spark: SparkSession, table: str) -> dict[str, int]:
    return {r.name: r.snapshot_id for r in
            spark.sql(f"SELECT name, snapshot_id FROM {table}.refs WHERE type = 'TAG'").collect()}


def remove_orphan_files(spark: SparkSession, table: str, older_than: datetime,
                        listing: list[tuple[str, datetime]] | None = None) -> list[str]:
    """Delete files under the table location that no snapshot references
    (e.g. leftovers of failed or aborted writes). Returns the removed paths.

    The object listing is passed in through `file_list_view` instead of letting
    Spark list the directory with a Hadoop FileSystem. On S3 this is how large
    tables are handled in production: the listing comes from S3 Inventory.
    Iceberg refuses older_than values less than 24 hours ago: a younger file may
    belong to a write that has not committed yet.
    """
    if listing is None:
        listing = list_objects(table_location(spark, table))
    listing = [(uri, mtime.replace(tzinfo=None)) for uri, mtime in listing]
    view = "orphan_candidates_" + table.replace(".", "_")
    (spark.createDataFrame(listing, "file_path STRING, last_modified TIMESTAMP")
     .createOrReplaceTempView(view))
    rows = spark.sql(f"""
        CALL {CATALOG}.system.remove_orphan_files(
          table => '{table}', older_than => {_ts(older_than)}, file_list_view => '{view}')
    """).collect()
    return [r.orphan_file_location for r in rows]
