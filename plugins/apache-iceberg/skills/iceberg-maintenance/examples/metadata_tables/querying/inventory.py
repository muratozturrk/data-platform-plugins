"""Inventory of a table's metadata tables: schemas and row counts, from Spark and PyIceberg."""
import os

from common.metaquery import METADATA_TABLES

# Inspect methods offered by PyIceberg 0.9.1 (no all_files, all_entries, all_data_files,
# all_delete_files or position_deletes).
PYICEBERG_TABLES = ["history", "metadata_log_entries", "snapshots", "refs", "manifests",
                    "all_manifests", "partitions", "entries", "files", "data_files", "delete_files"]


def schema(spark, table: str, metadata_table: str) -> dict[str, str]:
    """Top-level columns and Spark types, as DESCRIBE reports them."""
    cols = {}
    for r in spark.sql(f"DESCRIBE {table}.{metadata_table}").collect():
        if not r.col_name or r.col_name.startswith("#"):
            break                       # "# Partition Information" section follows
        cols[r.col_name] = r.data_type
    return cols


# all_* tables already span every snapshot: they reject VERSION AS OF.
# history, snapshots, refs and metadata_log_entries accept it but ignore it.
NO_TIME_TRAVEL = {"all_manifests", "all_entries", "all_files", "all_data_files", "all_delete_files"}


def row_counts(spark, table: str, version: str | int | None = None,
               tables: list[str] = METADATA_TABLES) -> dict[str, int]:
    """Row count of metadata tables, optionally at a snapshot id or a ref name."""
    if version is None:
        at = ""
    elif isinstance(version, str):
        at = f" VERSION AS OF '{version}'"
    else:
        at = f" VERSION AS OF {version}"
    return {m: spark.sql(f"SELECT count(*) AS n FROM {table}.{m}{at}").first().n
            for m in tables}


def pyiceberg_catalog():
    from pyiceberg.catalog import load_catalog
    return load_catalog("lake", **{
        "type": "rest",
        "uri": os.getenv("ICEBERG_REST_URI", "http://rest:8181"),
        "s3.endpoint": os.getenv("S3_ENDPOINT", "http://seaweedfs:8333"),
        "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "admin"),  # local dev default
        "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "password"),  # local dev default
        "s3.region": os.getenv("AWS_REGION", "us-east-1"),
        "s3.path-style-access": "true",
    })


def pyiceberg_row_counts(table: str) -> dict[str, int]:
    """Same inventory through PyIceberg's `table.inspect` API (PyArrow tables)."""
    tbl = pyiceberg_catalog().load_table(table)
    return {m: getattr(tbl.inspect, m)().num_rows for m in PYICEBERG_TABLES}
