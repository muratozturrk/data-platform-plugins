"""Shared wiring: one Spark session and PyIceberg clients for every catalog in the lab stack.

Catalog names used throughout the labs:
  rest     Apache Iceberg REST catalog reference server (apache/iceberg-rest-fixture)
  polaris  Apache Polaris REST catalog (one Polaris "catalog" = one warehouse)
  nessie   Project Nessie (native Nessie API v2, Iceberg NessieCatalog client)
  hadoop   Iceberg HadoopCatalog on a local directory (file-system catalog, deprecated)
"""
import os
from functools import lru_cache
from urllib.parse import urlparse

import boto3
from pyspark.sql import SparkSession

REST_URI = os.getenv("ICEBERG_REST_URI", "http://rest:8181")
POLARIS_URI = os.getenv("POLARIS_URI", "http://polaris:8181")
NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie:19120")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
# local dev default: Polaris bootstrap credentials of the compose stack; set the env vars elsewhere.
ROOT_CLIENT = (os.getenv("POLARIS_ROOT_CLIENT_ID", "root"), os.getenv("POLARIS_ROOT_CLIENT_SECRET", "s3cr3t"))
BUCKET = "warehouse"
HADOOP_WAREHOUSE = "file:///tmp/hadoop-warehouse"
# Polaris catalog that Spark uses (created by common.polaris.ensure_catalog in conftest).
SPARK_POLARIS_CATALOG = "spark_lab"

S3_STATIC = {
    "s3.endpoint": S3_ENDPOINT,
    "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "admin"),  # local dev default
    "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "password"),  # local dev default
    "s3.region": os.getenv("AWS_REGION", "us-east-1"),
    "s3.path-style-access": "true",
}


def _iceberg_common(prefix: str) -> dict[str, str]:
    return {
        f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{prefix}.s3.endpoint": S3_ENDPOINT,
        f"{prefix}.s3.path-style-access": "true",
        f"{prefix}.http-client.apache.max-connections": "500",
        # Other clients (PyIceberg, raw HTTP) commit to the same tables: never serve a cached version.
        f"{prefix}.cache-enabled": "false",
    }


def spark_conf() -> dict[str, str]:
    c = "spark.sql.catalog"
    conf = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": "rest",
        "spark.sql.shuffle.partitions": "4",
        "spark.sql.session.timeZone": "UTC",
        "spark.ui.showConsoleProgress": "false",
        # rest: the Iceberg REST reference server
        f"{c}.rest": "org.apache.iceberg.spark.SparkCatalog",
        f"{c}.rest.type": "rest",
        f"{c}.rest.uri": REST_URI,
        f"{c}.rest.warehouse": f"s3://{BUCKET}/rest/",
        # polaris: OAuth2 client credentials, storage credentials vended by the catalog
        f"{c}.polaris": "org.apache.iceberg.spark.SparkCatalog",
        f"{c}.polaris.type": "rest",
        f"{c}.polaris.uri": f"{POLARIS_URI}/api/catalog",
        f"{c}.polaris.warehouse": SPARK_POLARIS_CATALOG,
        f"{c}.polaris.credential": f"{ROOT_CLIENT[0]}:{ROOT_CLIENT[1]}",
        f"{c}.polaris.scope": "PRINCIPAL_ROLE:ALL",
        f"{c}.polaris.oauth2-server-uri": f"{POLARIS_URI}/api/catalog/v1/oauth/tokens",
        f"{c}.polaris.header.X-Iceberg-Access-Delegation": "vended-credentials",
        # Iceberg 1.8.1 executors call Polaris' credential-refresh endpoint without the OAuth token
        # (401 "Not authorized"). The vended keys are valid for 1 h, longer than any lab job.
        f"{c}.polaris.client.refresh-credentials-enabled": "false",
        # nessie: Iceberg's NessieCatalog against the Nessie REST API v2
        f"{c}.nessie": "org.apache.iceberg.spark.SparkCatalog",
        f"{c}.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
        f"{c}.nessie.uri": f"{NESSIE_URI}/api/v2",
        f"{c}.nessie.ref": "main",
        f"{c}.nessie.warehouse": f"s3://{BUCKET}/nessie/",
        # hadoop: no service at all, the "catalog" is a directory layout
        f"{c}.hadoop": "org.apache.iceberg.spark.SparkCatalog",
        f"{c}.hadoop.type": "hadoop",
        f"{c}.hadoop.warehouse": HADOOP_WAREHOUSE,
    }
    for name in ("rest", "polaris", "nessie"):
        conf.update(_iceberg_common(f"{c}.{name}"))
    return conf


def get_spark(app_name: str = "lakehouse-catalogs") -> SparkSession:
    builder = (SparkSession.builder.appName(app_name)
               .master(os.getenv("SPARK_MASTER", "local[2]"))
               .config("spark.driver.memory", "1g"))
    for k, v in spark_conf().items():
        builder = builder.config(k, v)
    return builder.getOrCreate()


def rest_catalog(name: str = "rest", **extra):
    """PyIceberg client for the REST reference server (static S3 keys: the fixture does not vend)."""
    from pyiceberg.catalog.rest import RestCatalog
    return RestCatalog(name, **{"uri": REST_URI, **S3_STATIC, **extra})


def sql_catalog(db_path: str, name: str = "jdbc", warehouse: str = f"s3://{BUCKET}/jdbc"):
    """PyIceberg SqlCatalog: the Python twin of Iceberg's JdbcCatalog (same table layout)."""
    from pyiceberg.catalog.sql import SqlCatalog
    return SqlCatalog(name, **{"uri": f"sqlite:///{db_path}", "warehouse": warehouse, **S3_STATIC})


@lru_cache(maxsize=1)
def s3_client():
    return boto3.client(
        "s3", endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_STATIC["s3.access-key-id"],
        aws_secret_access_key=S3_STATIC["s3.secret-access-key"],
        region_name=S3_STATIC["s3.region"],
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


def read_json(uri: str) -> dict:
    import json
    bucket, key = split_uri(uri)
    return json.loads(s3_client().get_object(Bucket=bucket, Key=key)["Body"].read())
