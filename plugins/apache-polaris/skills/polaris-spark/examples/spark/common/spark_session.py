"""The minimal Spark configuration for a Polaris REST catalog (FILE storage, no vending)."""
from __future__ import annotations

import pyspark
from pyspark.sql import SparkSession


def polaris_spark(principal_client_id: str, principal_client_secret: str) -> SparkSession:
    # Define sensitive variables
    POLARIS_URI = 'http://polaris:8181/api/catalog'
    POLARIS_CATALOG_NAME = 'polariscatalog'
    POLARIS_CREDENTIALS = f'{principal_client_id}:{principal_client_secret}'
    POLARIS_SCOPE = 'PRINCIPAL_ROLE:ALL'

    # Set up Spark configuration
    conf = (
        pyspark.SparkConf()
            .setAppName('PolarisSparkApp')
            # Add necessary JARs
            .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.4.0')
            # Enable Iceberg SQL extensions
            .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
            # Configure the Polaris catalog
            .set('spark.sql.catalog.polaris', 'org.apache.iceberg.spark.SparkCatalog')
            .set('spark.sql.catalog.polaris.warehouse', POLARIS_CATALOG_NAME)
            .set('spark.sql.catalog.polaris.catalog-impl', 'org.apache.iceberg.rest.RESTCatalog')
            .set('spark.sql.catalog.polaris.uri', POLARIS_URI)
            .set('spark.sql.catalog.polaris.credential', POLARIS_CREDENTIALS)
            .set('spark.sql.catalog.polaris.scope', POLARIS_SCOPE)
            .set('spark.sql.catalog.polaris.token-refresh-enabled', 'true')
    )

    # Start the Spark session
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark


def add_vended_s3_catalog(spark: SparkSession, name: str, warehouse: str, client_id: str, client_secret: str,
                          vended: bool, server: str = "http://polaris:8181") -> None:
    """A second catalog on S3 storage, configured at runtime. With vended=True
    the REST client asks Polaris for table-scoped credentials; otherwise it has none."""
    c = f"spark.sql.catalog.{name}"
    for k, v in {
        c: "org.apache.iceberg.spark.SparkCatalog",
        f"{c}.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
        f"{c}.uri": f"{server}/api/catalog",
        f"{c}.warehouse": warehouse,
        f"{c}.credential": f"{client_id}:{client_secret}",
        f"{c}.scope": "PRINCIPAL_ROLE:ALL",
        f"{c}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{c}.s3.endpoint": "http://seaweedfs:8333",
        f"{c}.s3.path-style-access": "true",
        f"{c}.client.region": "us-east-1",
    }.items():
        spark.conf.set(k, v)
    if vended:
        spark.conf.set(f"{c}.header.X-Iceberg-Access-Delegation", "vended-credentials")
