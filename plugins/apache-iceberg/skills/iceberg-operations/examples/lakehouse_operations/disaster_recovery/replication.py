"""Cross-region copies of an Iceberg table.

Iceberg 1.8.1 metadata stores absolute paths, so a byte-for-byte bucket copy (what S3 CRR does) still
points at the primary region. rewrite_table_path rewrites the metadata for the target prefix and
returns the list of files to copy: https://iceberg.apache.org/docs/latest/spark-procedures/#rewrite_table_path
"""
from urllib.parse import urlparse

from pyspark.sql import SparkSession

from common.lakehouse import CATALOG, list_objects, s3_client, table_location


def _split(uri: str) -> tuple[str, str]:
    url = urlparse(uri)
    return url.netloc, url.path.lstrip("/")


def copy_object(src: str, dst: str) -> None:
    sb, sk = _split(src)
    db, dk = _split(dst)
    s3_client().copy_object(Bucket=db, Key=dk, CopySource={"Bucket": sb, "Key": sk})


def naive_copy(spark: SparkSession, table: str, source_prefix: str, target_prefix: str) -> int:
    """What bucket replication does: copy every object, change nothing. Returns objects copied."""
    objects = list_objects(table_location(spark, table))
    for uri, _ in objects:
        copy_object(uri, uri.replace(source_prefix, target_prefix, 1))
    return len(objects)


def replicate_with_path_rewrite(spark: SparkSession, table: str, source_prefix: str, target_prefix: str,
                                staging: str) -> str:
    """rewrite_table_path + copy plan. Returns the replica's metadata.json to register."""
    r = spark.sql(f"""
        CALL {CATALOG}.system.rewrite_table_path(
          table => '{table}', source_prefix => '{source_prefix}', target_prefix => '{target_prefix}',
          staging_location => '{staging}')""").first()
    # The plan is a CSV of (source, target): staged rewritten metadata + unchanged data files.
    plan = spark.read.csv(r.file_list_location).collect()
    for src, dst in plan:
        copy_object(src, dst)
    return f"{target_prefix.rstrip('/')}/metadata/{r.latest_version}"


def replication_lag(spark: SparkSession, primary: str, replica: str) -> set[int]:
    """Snapshots committed on the primary that the replica does not have yet (ids survive the rewrite)."""
    ids = lambda t: {r.snapshot_id for r in spark.sql(f"SELECT snapshot_id FROM {t}.snapshots").collect()}
    return ids(primary) - ids(replica)
