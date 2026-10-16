"""Secretless Connector - PySpark with certificate-based PostgreSQL auth and identity-based S3 access.

No password and no access key anywhere:
  * PostgreSQL: client certificate authentication (pg_hba.conf `hostssl ... cert`).
    `sslmode=verify-full` also checks the server hostname against its certificate.
    The private key is a file mounted from the platform (k8s secret / instance
    volume), rotated by the PKI - plan overlap so old and new certs both work.
  * S3: the S3A credential chain picks up the workload identity: IRSA on EKS
    (WebIdentityTokenCredentialsProvider) or the EC2/EMR instance profile.
    Credentials are short-lived STS tokens issued by AWS, never stored.

Run:
  export JDBC_URL=jdbc:postgresql://db.internal:5432/dp DB_USER=dp_job \
         PG_SSL_ROOT_CERT=/certs/root.crt PG_SSL_CERT=/certs/client.crt PG_SSL_KEY=/certs/client.pk8 \
         OUTPUT_PATH=s3a://dp-devices/export/
  spark-submit --packages org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4 \
      secretless_connector_job.py
"""
import os

from pyspark.sql import SparkSession

JDBC_URL = os.environ.get("JDBC_URL", "jdbc:postgresql://localhost:5432/dp")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "s3a://dp-devices/export/")

# IRSA first, instance profile as fallback - both identity based.
S3A_PROVIDERS = ",".join([
    "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
    "com.amazonaws.auth.InstanceProfileCredentialsProvider",
])


def main() -> None:
    spark = (SparkSession.builder.appName("secretless-connector")
             .config("spark.hadoop.fs.s3a.aws.credentials.provider", S3A_PROVIDERS)
             .getOrCreate())

    devices = spark.read.jdbc(
        url=JDBC_URL,
        table="dp.devices",
        properties={
            "driver": "org.postgresql.Driver",
            "user": os.environ.get("DB_USER", "dp_job"),  # identity, not a secret
            "ssl": "true",
            "sslmode": "verify-full",
            "sslrootcert": os.environ.get("PG_SSL_ROOT_CERT", "/certs/root.crt"),
            "sslcert": os.environ.get("PG_SSL_CERT", "/certs/client.crt"),
            "sslkey": os.environ.get("PG_SSL_KEY", "/certs/client.pk8"),  # PKCS-8 DER for pgJDBC
        },
    )
    devices.write.mode("overwrite").parquet(OUTPUT_PATH)


if __name__ == "__main__":
    main()
