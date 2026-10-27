"""Shared helpers: Spark wired to the Iceberg REST catalog + S3, plus clients for the legacy platform."""
import os
from functools import lru_cache
from urllib.parse import urlparse

import boto3
import psycopg2
from pyspark.sql import SparkSession

CATALOG = "lake"
LEGACY_BUCKET = "legacy-lake"
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
REST_URI = os.getenv("ICEBERG_REST_URI", "http://rest:8181")


def get_spark(app_name: str = "migration-planning") -> SparkSession:
    key, secret = os.getenv("AWS_ACCESS_KEY_ID", "admin"), os.getenv("AWS_SECRET_ACCESS_KEY", "password")  # local dev default
    return (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", "1g")
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
        .config("spark.sql.defaultCatalog", CATALOG)
        # Hadoop S3A: how Spark reads the legacy lake (files outside any Iceberg table).
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", key)
        .config("spark.hadoop.fs.s3a.secret.key", secret)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # add_files writes its staging manifests through Hadoop FileIO into the table's s3:// metadata
        # folder, so the s3:// scheme must resolve to S3A as well (fails with "No FileSystem for scheme").
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


@lru_cache(maxsize=1)
def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "admin"),                # local dev default
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "password"),    # local dev default
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def list_objects(bucket: str, prefix: str = "") -> list[dict]:
    """Every object under a prefix: [{'Key', 'Size', 'LastModified', ...}]."""
    out = []
    for page in s3_client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        out += page.get("Contents", [])
    return out


def list_uri(uri: str) -> list[str]:
    """s3:// or s3a:// URIs of every object under a location."""
    url = urlparse(uri)
    return [f"s3://{url.netloc}/{o['Key']}" for o in list_objects(url.netloc, url.path.lstrip("/"))]


def pg_connect():
    conn = psycopg2.connect(os.getenv("PG_DSN", "host=postgres dbname=legacy user=auditor password=auditor"))  # local dev default
    conn.autocommit = True
    return conn


def pg_jdbc_options() -> dict:
    return {"url": os.getenv("PG_JDBC_URL", "jdbc:postgresql://postgres:5432/legacy"),
            "user": os.getenv("PG_USER", "auditor"), "password": os.getenv("PG_PASSWORD", "auditor"),  # local dev default
            "driver": "org.postgresql.Driver"}
