"""Shared helpers: Spark, PyIceberg and Trino wired to the same Iceberg REST catalog and object store."""
import os

import trino
from pyspark.sql import SparkSession

CATALOG = "lake"            # Spark / PyIceberg catalog name
TRINO_CATALOG = "iceberg"   # Trino catalog name (config/trino/catalog/iceberg.properties)
REST_URI = os.getenv("ICEBERG_REST_URI", "http://rest:8181")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")  # local dev default
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "password")  # local dev default
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))


def get_spark(app_name: str = "lakehouse-consumption") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[4]"))
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", REST_URI)
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", "s3://warehouse/")
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", S3_ENDPOINT)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.http-client.apache.max-connections", "500")
        # Other engines commit to the same tables: never serve a stale cached table.
        .config(f"spark.sql.catalog.{CATALOG}.cache-enabled", "false")
        .config("spark.sql.defaultCatalog", CATALOG)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def get_pyiceberg_catalog():
    """PyIceberg client for the same REST catalog; S3 settings must be explicit for SeaweedFS."""
    from pyiceberg.catalog import load_catalog
    return load_catalog(CATALOG, **{
        "type": "rest",
        "uri": REST_URI,
        "s3.endpoint": S3_ENDPOINT,
        "s3.access-key-id": AWS_KEY,
        "s3.secret-access-key": AWS_SECRET,
        "s3.region": AWS_REGION,
        "s3.path-style-access": "true",
    })


def trino_connect(user: str = "admin", schema: str | None = None,
                  session_properties: dict[str, str] | None = None) -> trino.dbapi.Connection:
    """DB-API connection over the Trino client protocol (the same HTTP protocol the JDBC driver uses)."""
    return trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user=user, catalog=TRINO_CATALOG,
                               schema=schema, http_scheme="http", session_properties=session_properties or {})


def trino_query(sql: str, user: str = "admin") -> list[tuple]:
    with trino_connect(user) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        return [tuple(r) for r in cur.fetchall()]


def add_spark_catalog(spark: SparkSession, name: str) -> None:
    """Register the same REST catalog under another name at runtime (e.g. Trino's name, `iceberg`)."""
    for key, value in {"": "org.apache.iceberg.spark.SparkCatalog", ".type": "rest", ".uri": REST_URI,
                       ".io-impl": "org.apache.iceberg.aws.s3.S3FileIO", ".s3.endpoint": S3_ENDPOINT,
                       ".s3.path-style-access": "true", ".cache-enabled": "false"}.items():
        spark.conf.set(f"spark.sql.catalog.{name}{key}", value)
