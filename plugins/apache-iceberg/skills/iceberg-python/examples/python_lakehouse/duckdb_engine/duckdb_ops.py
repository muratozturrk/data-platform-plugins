"""DuckDB on Iceberg, three ways: over PyIceberg Arrow, iceberg_scan() on a metadata file,
and a REST catalog ATTACHed as a database (read and write)."""
import duckdb

from common.lakehouse import AWS_KEY, AWS_REGION, AWS_SECRET, REST_URI, S3_ENDPOINT

TOP_TRIPS = """
SELECT vendor_id, trip_distance, total_amount
FROM {source}
WHERE trip_distance > 5
ORDER BY total_amount DESC, trip_id
LIMIT 10
"""


def connect() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with the iceberg extension and an S3 secret for SeaweedFS.
    The extension binaries are pre-installed in the image from PyPI wheels, so only LOAD runs."""
    con = duckdb.connect()
    con.execute("LOAD iceberg; LOAD httpfs;")
    endpoint = S3_ENDPOINT.split("://", 1)[1]
    con.execute(f"""
        CREATE SECRET lake_s3 (TYPE s3, KEY_ID '{AWS_KEY}', SECRET '{AWS_SECRET}',
                               REGION '{AWS_REGION}', ENDPOINT '{endpoint}',
                               URL_STYLE 'path', USE_SSL false)""")
    return con


def top_trips_via_arrow(con, table, snapshot_id=None) -> list[tuple]:
    """No extension needed: PyIceberg plans and reads, DuckDB queries the Arrow table."""
    arrow = table.scan(snapshot_id=snapshot_id).to_arrow()
    con.register("trips_arrow", arrow)
    return con.execute(TOP_TRIPS.format(source="trips_arrow")).fetchall()


def top_trips_via_iceberg_scan(con, metadata_location: str, snapshot_id=None) -> list[tuple]:
    """Extension reads the metadata file directly (no catalog). Note the parameter name:
    snapshot_from_id, not snapshot_id (as older examples show)."""
    args = f"'{metadata_location}'" + (f", snapshot_from_id={snapshot_id}" if snapshot_id else "")
    return con.execute(TOP_TRIPS.format(source=f"iceberg_scan({args})")).fetchall()


def attach_rest_catalog(con, alias: str = "iceberg_catalog") -> None:
    """ATTACH the REST catalog. The fixture has no auth, so AUTHORIZATION_TYPE 'none' replaces
    a SECRET; the first argument is the warehouse name."""
    con.execute(f"ATTACH 'warehouse' AS {alias} (TYPE iceberg, ENDPOINT '{REST_URI}', "
                f"AUTHORIZATION_TYPE 'none')")
