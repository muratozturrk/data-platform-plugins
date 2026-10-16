"""Encryptor - PySpark data-at-rest encryption, server-side and client-side.

1. Server-side (S3A SSE-KMS): S3 encrypts/decrypts with a KMS key. The job only
   needs kms:GenerateDataKey/Decrypt on the key + S3 access. Losing access to
   the key = losing the data, so keep a KMS deletion window (e.g. 14 days).
2. Client-side (Parquet modular encryption): Spark encrypts selected columns
   and footers before the bytes leave the executor; the writer owns the keys
   through a KMS client class (InMemoryKMS below is a DEMO only - plug in your
   KMS client in production).
In-transit: s3a uses HTTPS by default (`fs.s3a.connection.ssl.enabled=true`).

Run:
  export BUCKET=dp-visits KMS_KEY_ARN=arn:aws:kms:...:key/... \
         PARQUET_FOOTER_KEY_B64=<base64 16B> PARQUET_PII_KEY_B64=<base64 16B>
  spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 encrypted_storage_job.py sse|parquet
"""
import os
import sys

from pyspark.sql import SparkSession

BUCKET = os.environ.get("BUCKET", "dp-visits")


def sse_kms_session() -> SparkSession:
    return (SparkSession.builder.appName("encryptor-sse-kms")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
            .config("spark.hadoop.fs.s3a.encryption.algorithm", "SSE-KMS")
            .config("spark.hadoop.fs.s3a.encryption.key", os.environ["KMS_KEY_ARN"])
            .getOrCreate())


def parquet_encryption_session() -> SparkSession:
    footer_key = os.environ["PARQUET_FOOTER_KEY_B64"]
    pii_key = os.environ["PARQUET_PII_KEY_B64"]
    return (SparkSession.builder.appName("encryptor-parquet")
            .config("spark.hadoop.parquet.crypto.factory.class",
                    "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory")
            # DEMO KMS: keys passed in config. Replace with a KmsClient backed by AWS KMS/Vault.
            .config("spark.hadoop.parquet.encryption.kms.client.class",
                    "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS")
            .config("spark.hadoop.parquet.encryption.key.list", f"footerKey:{footer_key}, piiKey:{pii_key}")
            .getOrCreate())


def sample(spark: SparkSession):
    return spark.createDataFrame(
        [("v1", "u1", "10.0.0.1", "/home"), ("v2", "u2", "10.0.0.2", "/cart")],
        "visit_id STRING, user_id STRING, ip STRING, page STRING")


def run_sse() -> None:
    spark = sse_kms_session()
    path = f"s3a://{BUCKET}/encrypted/visits"
    sample(spark).write.mode("overwrite").parquet(path)
    # Reads decrypt transparently - only if the caller is allowed to use the KMS key.
    spark.read.parquet(path).show()


def run_parquet() -> None:
    spark = parquet_encryption_session()
    path = os.environ.get("OUTPUT_PATH", "/tmp/dp/encrypted_visits")
    (sample(spark).write.mode("overwrite")
     .option("parquet.encryption.column.keys", "piiKey:user_id,ip")
     .option("parquet.encryption.footer.key", "footerKey")
     .parquet(path))
    spark.read.parquet(path).show()  # works only with the key list configured


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "parquet"
    {"sse": run_sse, "parquet": run_parquet}[mode]()
