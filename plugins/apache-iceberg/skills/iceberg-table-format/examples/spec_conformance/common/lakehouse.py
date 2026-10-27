"""Shared helpers: Spark and PyIceberg on the same Iceberg REST catalog, raw S3 (SeaweedFS) access."""
import io
import json
import os
from functools import lru_cache
from urllib.parse import urlparse
from urllib.request import urlopen

import boto3
import fastavro
from pyspark.sql import SparkSession

CATALOG = "lake"
REST_URI = os.getenv("ICEBERG_REST_URI", "http://rest:8181")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")  # local dev default
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "password")  # local dev default
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def get_spark(app_name: str = "iceberg-spec") -> SparkSession:
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
        # Default pool (50) is too small for v3 deletion-vector writes.
        .config(f"spark.sql.catalog.{CATALOG}.http-client.apache.max-connections", "500")
        # PyIceberg commits to the same tables: never serve a stale cached table.
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
        "type": "rest", "uri": REST_URI,
        "s3.endpoint": S3_ENDPOINT, "s3.access-key-id": AWS_KEY,
        "s3.secret-access-key": AWS_SECRET, "s3.region": AWS_REGION,
        "s3.path-style-access": "true",
    })


def rest_get(path: str) -> dict:
    """Raw Iceberg REST catalog call (GET), e.g. /v1/config."""
    with urlopen(REST_URI.rstrip("/") + path, timeout=30) as resp:
        return json.loads(resp.read())


@lru_cache(maxsize=1)
def s3_client():
    return boto3.client("s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=AWS_KEY,
                        aws_secret_access_key=AWS_SECRET, region_name=AWS_REGION)


def split_uri(uri: str) -> tuple[str, str]:
    url = urlparse(uri)
    return url.netloc, url.path.lstrip("/")


def read_object(uri: str) -> bytes:
    bucket, key = split_uri(uri)
    return s3_client().get_object(Bucket=bucket, Key=key)["Body"].read()


def list_objects(location: str) -> list[str]:
    """s3:// URIs of every object physically present under a location."""
    bucket, prefix = split_uri(location.rstrip("/") + "/")
    keys = []
    for page in s3_client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [f"s3://{bucket}/{o['Key']}" for o in page.get("Contents", [])]
    return keys


def metadata_location(namespace: str, table: str) -> str:
    """The catalog pointer: where the current metadata.json lives."""
    return rest_get(f"/v1/namespaces/{namespace}/tables/{table}")["metadata-location"]


def read_metadata(uri: str) -> dict:
    return json.loads(read_object(uri))


def read_avro(uri: str) -> tuple[dict, dict, list[dict]]:
    """(writer schema, file key-value metadata, records) of an Avro manifest list or manifest."""
    reader = fastavro.reader(io.BytesIO(read_object(uri)))
    meta = {k: v.decode() if isinstance(v, bytes) else v for k, v in reader.metadata.items()}
    return reader.writer_schema, meta, list(reader)
