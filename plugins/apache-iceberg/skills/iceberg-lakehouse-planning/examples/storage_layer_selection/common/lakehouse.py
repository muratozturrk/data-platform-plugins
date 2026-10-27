"""Spark session with one Iceberg catalog per storage option compared in these examples.

| Catalog      | Catalog type | FileIO                 | Storage                                  |
|--------------|--------------|------------------------|------------------------------------------|
| lake         | REST         | S3FileIO               | SeaweedFS (object storage, S3 API)       |
| rust         | REST         | S3FileIO               | RustFS (second S3 implementation)        |
| lake_hadoop  | REST         | HadoopFileIO via s3a   | SeaweedFS (same tables as `lake`)        |
| analyst      | REST         | S3FileIO, read-only key| SeaweedFS, identity `analyst`            |
| lake_softdelete | REST      | S3FileIO, delete off + delete tags, write tags | SeaweedFS |
| rust_sse     | REST         | S3FileIO, s3.sse.type=s3 | RustFS                                 |
| lake_ssec    | REST         | S3FileIO, s3.sse.type=custom (SSE-C) | SeaweedFS                  |
| local        | Hadoop       | HadoopFileIO (file://) | Local disk volume (block storage)        |
| s3a          | Hadoop       | HadoopFileIO via s3a   | SeaweedFS, rename-based commits          |

Pattern from the Iceberg docs: https://iceberg.apache.org/docs/1.8.1/aws/#s3-fileio and
https://iceberg.apache.org/docs/1.8.1/spark-configuration/#catalog-configuration
"""
import os

from pyspark.sql import SparkSession

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
RUSTFS_ENDPOINT = os.getenv("RUSTFS_ENDPOINT", "http://rustfs:9000")
BLOCK_WAREHOUSE = "file:///data/block/warehouse"
# Local-dev-only SSE-C key: bytes 0..31, base64, plus base64 MD5 of the key (required by S3FileIO).
SSE_C_KEY = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
SSE_C_KEY_MD5 = "tP/LI3N87DFaSk0aoqYgzg=="
S3A_WAREHOUSE = "s3a://warehouse/hadoop"


def _rest_catalog(b: SparkSession.Builder, name: str, uri: str, endpoint: str, **props: str):
    c = f"spark.sql.catalog.{name}"
    b = (b.config(c, "org.apache.iceberg.spark.SparkCatalog")
          .config(f"{c}.type", "rest")
          .config(f"{c}.uri", uri)
          .config(f"{c}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
          .config(f"{c}.s3.endpoint", endpoint)
          .config(f"{c}.s3.path-style-access", "true")
          .config(f"{c}.http-client.apache.max-connections", "500"))
    for k, v in props.items():
        b = b.config(f"{c}.{k}", v)
    return b


def get_spark(app_name: str = "storage-selection") -> SparkSession:
    rest_uri = os.getenv("ICEBERG_REST_URI", "http://rest:8181")
    b = (SparkSession.builder.appName(app_name)
         .master(os.getenv("SPARK_MASTER", "local[4]"))
         .config("spark.driver.memory", "2g")
         .config("spark.sql.extensions",
                 "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.defaultCatalog", "lake")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.showConsoleProgress", "false")
         # Hadoop S3A connector: used by HadoopFileIO for s3:// and s3a:// paths.
         .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
         .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "admin"))          # local dev default
         .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "password")))  # local dev default

    b = _rest_catalog(b, "lake", rest_uri, S3_ENDPOINT)
    b = _rest_catalog(b, "rust", os.getenv("ICEBERG_REST_RUSTFS_URI", "http://rest-rustfs:8181"),
                      RUSTFS_ENDPOINT,
                      **{"s3.access-key-id": os.getenv("RUSTFS_ACCESS_KEY", "rustfsadmin"),
                         "s3.secret-access-key": os.getenv("RUSTFS_SECRET_KEY", "rustfs-secret")})
    b = _rest_catalog(b, "analyst", rest_uri, S3_ENDPOINT,
                      **{"s3.access-key-id": os.getenv("ANALYST_ACCESS_KEY", "analyst"),             # local dev default
                         "s3.secret-access-key": os.getenv("ANALYST_SECRET_KEY", "analyst-secret")})
    b = _rest_catalog(b, "lake_softdelete", rest_uri, S3_ENDPOINT,
                      **{"s3.delete-enabled": "false", "s3.delete.tags.iceberg-deleted": "true",
                         "s3.write.tags.team": "finance"})
    b = _rest_catalog(b, "rust_sse", os.getenv("ICEBERG_REST_RUSTFS_URI", "http://rest-rustfs:8181"),
                      RUSTFS_ENDPOINT,
                      **{"s3.access-key-id": os.getenv("RUSTFS_ACCESS_KEY", "rustfsadmin"),
                         "s3.secret-access-key": os.getenv("RUSTFS_SECRET_KEY", "rustfs-secret"),
                         "s3.sse.type": "s3"})
    b = _rest_catalog(b, "lake_ssec", rest_uri, S3_ENDPOINT,
                      **{"s3.sse.type": "custom", "s3.sse.key": SSE_C_KEY, "s3.sse.md5": SSE_C_KEY_MD5})
    b = _rest_catalog(b, "lake_hadoop", rest_uri, S3_ENDPOINT,
                      **{"io-impl": "org.apache.iceberg.hadoop.HadoopFileIO"})
    for name, wh in (("local", BLOCK_WAREHOUSE), ("s3a", S3A_WAREHOUSE)):
        b = (b.config(f"spark.sql.catalog.{name}", "org.apache.iceberg.spark.SparkCatalog")
              .config(f"spark.sql.catalog.{name}.type", "hadoop")
              .config(f"spark.sql.catalog.{name}.warehouse", wh))
    return b.getOrCreate()


def events_df(spark: SparkSession, n: int, seed: int = 0, n_days: int = 4):
    """Synthetic order events: id, customer, country, amount, event day. Deterministic per seed."""
    from pyspark.sql import functions as F
    countries = ["US", "DE", "TR", "GB", "FR", "BR", "IN", "JP"]
    return (spark.range(n).withColumnRenamed("id", "order_id")
            .withColumn("customer_id", (F.rand(seed) * 5000).cast("bigint"))
            .withColumn("country", F.element_at(F.array(*map(F.lit, countries)),
                                                (F.rand(seed + 1) * len(countries)).cast("int") + 1))
            .withColumn("amount", F.round(F.rand(seed + 2) * 500, 2))
            .withColumn("event_day", F.date_add(F.lit("2025-01-01").cast("date"),
                                                (F.col("order_id") % n_days).cast("int"))))
