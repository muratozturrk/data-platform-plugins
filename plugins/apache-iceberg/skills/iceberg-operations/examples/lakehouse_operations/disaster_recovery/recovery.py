"""Rollback, purge and snapshot validation."""
from urllib.parse import urlparse

from botocore.exceptions import ClientError
from pyspark.sql import SparkSession

from common.lakehouse import CATALOG, s3_client

GOOD_TAG_PREFIX = "audited_"          # tags written by the Write-Audit-Publish job (auditing_and_wap/wap.py)


def current_snapshot_id(spark: SparkSession, table: str) -> int:
    return spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").first().snapshot_id


def last_good_snapshot(spark: SparkSession, table: str, prefix: str = GOOD_TAG_PREFIX) -> int:
    """Most recent snapshot that passed data quality checks (tagged restore point)."""
    row = spark.sql(f"""
        SELECT r.snapshot_id FROM {table}.refs r JOIN {table}.snapshots s ON r.snapshot_id = s.snapshot_id
        WHERE r.type = 'TAG' AND r.name LIKE '{prefix}%' ORDER BY s.committed_at DESC LIMIT 1""").first()
    if row is None:
        raise LookupError(f"no '{prefix}*' tag on {table}: no known-good restore point")
    return row.snapshot_id


def snapshots_after(spark: SparkSession, table: str, good: int) -> list[int]:
    """Snapshots on main's lineage committed after `good` (the ones a rollback abandons)."""
    parents = {r.snapshot_id: r.parent_id for r in spark.sql(
        f"SELECT snapshot_id, parent_id FROM {table}.snapshots").collect()}
    chain, sid = [], current_snapshot_id(spark, table)
    while sid is not None and sid != good:
        chain.append(sid)
        sid = parents.get(sid)
    if sid != good:
        raise ValueError(f"snapshot {good} is not an ancestor of main")
    return chain


def rollback(spark: SparkSession, table: str, snapshot_id: int) -> None:
    spark.sql(f"CALL {CATALOG}.system.rollback_to_snapshot('{table}', {snapshot_id})")


def purge_snapshots(spark: SparkSession, table: str, snapshot_ids: list[int]) -> int:
    """Expire exactly the abandoned snapshots (an older_than cutoff never reaches them, see tests)."""
    if not snapshot_ids:
        return 0
    ids = ", ".join(map(str, snapshot_ids))
    r = spark.sql(f"CALL {CATALOG}.system.expire_snapshots(table => '{table}', snapshot_ids => array({ids}))").first()
    return r.deleted_data_files_count


def _exists(uri: str) -> bool:
    url = urlparse(uri)
    try:
        s3_client().head_object(Bucket=url.netloc, Key=url.path.lstrip("/"))
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def validate_snapshot(spark: SparkSession, table: str, snapshot_id: int | None = None) -> dict[str, list[str]]:
    """Every file a snapshot needs must exist on storage: manifest list, manifests, data and delete files.

    Metadata tables only describe what *should* exist; HEAD requests prove it does.
    """
    sid = snapshot_id or current_snapshot_id(spark, table)
    manifest_list = spark.sql(f"SELECT manifest_list FROM {table}.snapshots WHERE snapshot_id = {sid}").first()[0]
    missing = {"manifest_list": [p for p in [manifest_list] if not _exists(p)], "manifests": [], "files": []}
    if missing["manifest_list"]:
        return missing                       # nothing below it can be read
    manifests = [r.path for r in spark.sql(f"SELECT path FROM {table}.manifests VERSION AS OF {sid}").collect()]
    missing["manifests"] = [p for p in manifests if not _exists(p)]
    if not missing["manifests"]:
        files = [r.file_path for r in spark.sql(
            f"SELECT file_path FROM {table}.files VERSION AS OF {sid}").collect()]
        missing["files"] = [p for p in files if not _exists(p)]
    return missing
