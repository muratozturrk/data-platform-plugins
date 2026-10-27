"""Shared helpers: Spark session wired to the Iceberg REST catalog, S3 (SeaweedFS) access."""
import os
from datetime import datetime
from functools import lru_cache
from urllib.parse import urlparse

import boto3
import requests
from pyspark.sql import SparkSession

CATALOG = "lake"


def get_spark(app_name: str = "lakehouse-operations") -> SparkSession:
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
    return (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[4]"))
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", os.getenv("ICEBERG_REST_URI", "http://rest:8181"))
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", "s3://warehouse/")
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", s3_endpoint)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        # Default pool (50) is too small for maintenance procedures that open many files.
        .config(f"spark.sql.catalog.{CATALOG}.http-client.apache.max-connections", "500")
        .config("spark.sql.defaultCatalog", CATALOG)
        # Hadoop S3A for the few code paths that bypass S3FileIO (rewrite_table_path file list).
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


@lru_cache(maxsize=1)
def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://seaweedfs:8333"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "admin"),  # local dev default
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "password"),  # local dev default
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def table_location(spark: SparkSession, table: str) -> str:
    """s3://bucket/path of an Iceberg table, read from the table's own metadata."""
    row = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").where("col_name = 'Location'").first()
    return row.data_type


def list_objects(location: str, sub_dir: str = "") -> list[tuple[str, datetime]]:
    """(s3:// URI, last modified) of every object physically present under a table location."""
    url = urlparse(location.rstrip("/") + "/" + sub_dir)
    bucket, prefix = url.netloc, url.path.lstrip("/")
    objects = []
    for page in s3_client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        objects += [(f"s3://{bucket}/{o['Key']}", o["LastModified"]) for o in page.get("Contents", [])]
    return objects


def read_object(uri: str) -> bytes:
    url = urlparse(uri)
    return s3_client().get_object(Bucket=url.netloc, Key=url.path.lstrip("/"))["Body"].read()


def drop_table(spark: SparkSession, table: str) -> None:
    """DROP ... PURGE; if the table's metadata is gone (DR tests delete it), remove the catalog entry via REST."""
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    except Exception:
        ns, name = table.split(".")
        rest = os.getenv("ICEBERG_REST_URI", "http://rest:8181")
        resp = requests.delete(f"{rest}/v1/namespaces/{ns}/tables/{name}", timeout=30)
        if resp.status_code != 404:          # 404: the entry was removed before the purge step failed
            resp.raise_for_status()
