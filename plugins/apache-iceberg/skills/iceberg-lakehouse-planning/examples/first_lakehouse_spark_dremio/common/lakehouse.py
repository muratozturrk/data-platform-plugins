"""Shared helpers: Spark wired to the Nessie catalog, S3 (SeaweedFS) access."""
import os
from functools import lru_cache
from urllib.parse import urlparse

import boto3
from pyspark.sql import SparkSession

CATALOG = "nessie"
NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie:19120/api/v2")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
WAREHOUSE = "s3://warehouse/"


def get_spark(app_name: str = "Iceberg Ingestion") -> SparkSession:
    """Nessie SparkConf without spark.jars.packages: Iceberg 1.8.1 (incl. its Nessie client),
    the AWS bundle and the PostgreSQL driver are already on the image classpath."""
    c = f"spark.sql.catalog.{CATALOG}"
    return (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[4]"))
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(c, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{c}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config(f"{c}.uri", NESSIE_URI)
        .config(f"{c}.ref", "main")
        .config(f"{c}.authentication.type", "NONE")
        .config(f"{c}.warehouse", WAREHOUSE)
        .config(f"{c}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{c}.s3.endpoint", S3_ENDPOINT)          # container DNS name; no IP lookup needed
        .config(f"{c}.s3.path-style-access", "true")
        .config(f"{c}.http-client.apache.max-connections", "500")
        .config(f"{c}.cache-enabled", "false")            # see commits made by other engines at once
        .config("spark.sql.defaultCatalog", CATALOG)
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


def list_keys(prefix_uri: str) -> list[str]:
    """s3:// URIs of every object physically present under a prefix (the 'browse the bucket' step)."""
    url = urlparse(prefix_uri)
    bucket, prefix = url.netloc, url.path.lstrip("/")
    keys = []
    for page in s3_client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [f"s3://{bucket}/{o['Key']}" for o in page.get("Contents", [])]
    return keys


def table_location(spark: SparkSession, table: str) -> str:
    row = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").where("col_name = 'Location'").first()
    return row.data_type


def run_postgres(spark: SparkSession, statement: str) -> None:
    """Execute DML on the source database through the JDBC driver already loaded in Spark's JVM."""
    from spark_ingestion.ingest import JDBC_PROPERTIES, JDBC_URL
    conn = spark._jvm.java.sql.DriverManager.getConnection(
        JDBC_URL, JDBC_PROPERTIES["user"], JDBC_PROPERTIES["password"])
    try:
        conn.createStatement().execute(statement)
    finally:
        conn.close()
