"""Spark session wired to the Iceberg REST catalog and S3 (SeaweedFS)."""
import os

from pyspark.sql import SparkSession

CATALOG = "lake"


def get_spark(app_name: str = "metadata-tables") -> SparkSession:
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
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", os.getenv("S3_ENDPOINT", "http://seaweedfs:8333"))
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.http-client.apache.max-connections", "500")
        .config("spark.sql.defaultCatalog", CATALOG)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
