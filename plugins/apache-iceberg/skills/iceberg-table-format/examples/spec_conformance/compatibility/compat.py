"""Format-version upgrades and reader/writer compatibility checks."""
import pyarrow as pa
from pyspark.sql import SparkSession

from common.lakehouse import read_metadata


class UnsupportedFormatVersion(Exception):
    pass


def open_metadata(metadata_location: str, supported_versions: set[int]) -> dict:
    """Reader gate from the spec: a reader must refuse a table whose format-version it does not know."""
    md = read_metadata(metadata_location)
    if md["format-version"] not in supported_versions:
        raise UnsupportedFormatVersion(f"format-version {md['format-version']} not in {sorted(supported_versions)}")
    return md


def upgrade(spark: SparkSession, table: str, version: int) -> None:
    """Explicit, metadata-only upgrade: writes one new metadata.json, rewrites no data or manifests."""
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('format-version'='{version}')")


def try_op(fn) -> str:
    """'ok' or the exception class and first line of its message (for a compatibility matrix)."""
    try:
        fn()
        return "ok"
    except Exception as e:                       # noqa: BLE001 - the error itself is the result
        java = getattr(e, "java_exception", None)    # Py4JJavaError: report the JVM message
        msg = java.getMessage() if java is not None else str(e)
        return f"{type(e).__name__}: {msg.splitlines()[0][:90]}"


def pyiceberg_matrix(catalog, tables: dict[str, str]) -> dict[str, dict[str, str]]:
    """Read (full scan) and append one row with PyIceberg, per table."""
    out = {}
    for label, ident in tables.items():
        def read():
            return catalog.load_table(ident).scan().to_arrow().num_rows

        def append():
            catalog.load_table(ident).append(pa.table({"id": pa.array([10_000], pa.int64())}))
        out[label] = {"read": try_op(read), "append": try_op(append)}
    return out
