"""Shared helpers: Spark session wired to the Iceberg REST catalog (S3 on SeaweedFS) and Kafka."""
import os
import shutil
import tempfile

from pyspark.sql import SparkSession

CATALOG = "lake"
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "/tmp/ingestion-checkpoints")


def catalog_props() -> dict[str, str]:
    """Iceberg REST catalog properties, shared by Spark and by plain Java API clients (iceberg_writes)."""
    return {
        "uri": os.getenv("ICEBERG_REST_URI", "http://rest:8181"),
        "warehouse": "s3://warehouse/",
        "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "s3.endpoint": os.getenv("S3_ENDPOINT", "http://seaweedfs:8333"),
        "s3.path-style-access": "true",
        # Default pool (50) is too small when many files are written in parallel.
        "http-client.apache.max-connections": "500",
        # foreachBatch runs in a cloned session with its own catalog instance; without this
        # the test session keeps serving a cached (stale) table version.
        "cache-enabled": "false",
    }


def get_spark(app_name: str = "iceberg-ingestion") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[4]"))
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config("spark.sql.defaultCatalog", CATALOG)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    for k, v in catalog_props().items():
        builder = builder.config(f"spark.sql.catalog.{CATALOG}.{k}", v)
    return builder.getOrCreate()


def kafka_bootstrap() -> str:
    return os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")


def read_kafka(spark: SparkSession, topic: str, **options):
    """Kafka source, starting from the earliest retained offset."""
    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", kafka_bootstrap())
              .option("subscribe", topic)
              .option("startingOffsets", "earliest"))
    for k, v in options.items():
        reader = reader.option(k, str(v))
    return reader.load()


def new_checkpoint(name: str) -> str:
    """Fresh local checkpoint directory. Production: durable storage (S3/HDFS) per query."""
    path = tempfile.mkdtemp(prefix=f"{name}-", dir=_ensure(CHECKPOINT_ROOT))
    return path


def _ensure(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def drop_last_commit(checkpoint: str) -> int:
    """Simulate a crash after the sink committed but before Spark wrote its commit log.

    Spark then re-runs the last micro-batch (same offsets) on restart. Returns the batch id.
    """
    commits = os.path.join(checkpoint, "commits")
    last = max(int(f) for f in os.listdir(commits) if f.isdigit())
    for f in (str(last), f".{last}.crc"):
        p = os.path.join(commits, f)
        if os.path.exists(p):
            os.remove(p)
    return last


def remove_checkpoint(checkpoint: str) -> None:
    shutil.rmtree(checkpoint, ignore_errors=True)


def snapshots(spark: SparkSession, table: str) -> list:
    return spark.sql(
        f"SELECT snapshot_id, operation, summary FROM {table}.snapshots ORDER BY committed_at"
    ).collect()
