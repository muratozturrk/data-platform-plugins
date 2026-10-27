"""Shared helpers: Spark and PyIceberg wired to the same Iceberg REST catalog, S3 (SeaweedFS) access."""
import json
import os
from datetime import datetime
from functools import lru_cache
from urllib.parse import urlparse
from urllib.request import urlopen

import boto3
from pyspark.sql import DataFrame, SparkSession

CATALOG = "lake"
REST_URI = os.getenv("ICEBERG_REST_URI", "http://rest:8181")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")  # local dev default
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "password")  # local dev default
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def get_spark(app_name: str = "iceberg-core-features") -> SparkSession:
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
        # Static plans keep scan metrics on the leaf node (no AQE re-planning).
        .config("spark.sql.adaptive.enabled", "false")
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


def rest_get(path: str) -> dict:
    """Raw Iceberg REST catalog call (GET), e.g. /v1/namespaces."""
    with urlopen(REST_URI.rstrip("/") + path, timeout=30) as resp:
        return json.loads(resp.read())


@lru_cache(maxsize=1)
def s3_client():
    return boto3.client("s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=AWS_KEY,
                        aws_secret_access_key=AWS_SECRET, region_name=AWS_REGION)


def split_uri(uri: str) -> tuple[str, str]:
    url = urlparse(uri)
    return url.netloc, url.path.lstrip("/")


def table_location(spark: SparkSession, table: str) -> str:
    """s3://bucket/path of an Iceberg table, read from the table's own metadata."""
    return spark.sql(f"DESCRIBE TABLE EXTENDED {table}").where("col_name = 'Location'").first().data_type


def list_objects(location: str, sub_dir: str = "") -> list[tuple[str, datetime]]:
    """(s3:// URI, last modified) of every object physically present under a location."""
    bucket, prefix = split_uri(location.rstrip("/") + "/" + sub_dir)
    objects = []
    for page in s3_client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        objects += [(f"s3://{bucket}/{o['Key']}", o["LastModified"]) for o in page.get("Contents", [])]
    return objects


def read_object(uri: str) -> bytes:
    bucket, key = split_uri(uri)
    return s3_client().get_object(Bucket=bucket, Key=key)["Body"].read()


def put_object(uri: str, body: bytes) -> None:
    bucket, key = split_uri(uri)
    s3_client().put_object(Bucket=bucket, Key=key, Body=body)


def delete_prefix(location: str) -> None:
    bucket, _ = split_uri(location)
    keys = [split_uri(u)[1] for u, _ in list_objects(location)]
    for i in range(0, len(keys), 1000):
        s3_client().delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]]})


def scan_metrics(df: DataFrame) -> dict[str, int]:
    """Run the query and return the SQL metrics of its scan nodes.

    Iceberg scans report e.g. resultDataFiles / skippedDataFiles; plain file scans report
    numFiles ("number of files read"). This is what the Spark UI shows per scan node.
    """
    df.collect()
    metrics: dict[str, int] = {}
    stack = [df._jdf.queryExecution().executedPlan()]
    while stack:
        node = stack.pop()
        if node.nodeName().startswith(("BatchScan", "Scan")):
            it = node.metrics().iterator()
            while it.hasNext():
                kv = it.next()
                metrics[kv._1()] = metrics.get(kv._1(), 0) + int(kv._2().value())
        children = node.children()
        stack += [children.apply(i) for i in range(children.size())]
    return metrics
