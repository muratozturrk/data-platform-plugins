"""Shared helpers: Spark and PyIceberg wired to the same Iceberg REST catalog, S3 (SeaweedFS) access."""
import os
from functools import lru_cache
from urllib.parse import urlparse

import boto3
from pyspark.sql import SparkSession

CATALOG = "lake"
REST_URI = os.getenv("ICEBERG_REST_URI", "http://rest:8181")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
WAREHOUSE_BUCKET = "warehouse"


def get_spark(app_name: str = "lakehouse-basics") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[4]"))
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", REST_URI)
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", f"s3://{WAREHOUSE_BUCKET}/")
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", S3_ENDPOINT)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.http-client.apache.max-connections", "500")
        # Disable the client-side table cache so Spark sees commits made by other engines.
        .config(f"spark.sql.catalog.{CATALOG}.cache-enabled", "false")
        .config("spark.sql.defaultCatalog", CATALOG)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def pyiceberg_properties(**extra) -> dict:
    """Connection properties for a second engine (PyIceberg) on the same REST catalog and bucket."""
    return {
        "type": "rest",
        "uri": REST_URI,
        "s3.endpoint": S3_ENDPOINT,
        "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "admin"),  # local dev default
        "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "password"),  # local dev default
        "s3.region": os.getenv("AWS_REGION", "us-east-1"),
        "s3.path-style-access": "true",
        **extra,
    }


def get_pyiceberg_catalog(**extra):
    from pyiceberg.catalog import load_catalog
    return load_catalog(CATALOG, **pyiceberg_properties(**extra))


@lru_cache(maxsize=1)
def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "admin"),  # local dev default
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "password"),  # local dev default
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def split_uri(uri: str) -> tuple[str, str]:
    url = urlparse(uri)
    return url.netloc, url.path.lstrip("/")


def list_keys(prefix_uri: str) -> list[str]:
    """s3:// URIs of every object physically present under a prefix."""
    bucket, prefix = split_uri(prefix_uri)
    keys = []
    for page in s3_client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [f"s3://{bucket}/{o['Key']}" for o in page.get("Contents", [])]
    return keys


def table_location(spark: SparkSession, table: str) -> str:
    row = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").where("col_name = 'Location'").first()
    return row.data_type
