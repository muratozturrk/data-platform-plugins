"""Fine-Grained Accessor for Resources - PySpark with per-bucket assumed roles (S3A).

Instead of one broad cluster role that can overwrite every dataset, the job
assumes a DIFFERENT narrowly-scoped IAM role per bucket:
  * input bucket  -> role allowing only s3:Get*/s3:List* on that bucket
  * output bucket -> role allowing only s3:PutObject/s3:DeleteObject on its prefix
A bug that writes to the input path fails with AccessDenied instead of
destroying the source data.

Run (EMR / any Spark 3.5 with hadoop-aws 3.3+):
  export INPUT_BUCKET=dp-visits-raw OUTPUT_BUCKET=dp-visits-clean \
         INPUT_ROLE_ARN=arn:aws:iam::<acct>:role/visits-raw-reader \
         OUTPUT_ROLE_ARN=arn:aws:iam::<acct>:role/visits-clean-writer
  spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 per_bucket_assumed_role_job.py
"""
import json
import os

from pyspark.sql import SparkSession, functions as F

INPUT_BUCKET = os.environ["INPUT_BUCKET"]
OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]
INPUT_ROLE_ARN = os.environ["INPUT_ROLE_ARN"]
OUTPUT_ROLE_ARN = os.environ["OUTPUT_ROLE_ARN"]
ASSUMED = "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"


def bucket_role_conf(bucket: str, role_arn: str) -> dict:
    prefix = f"spark.hadoop.fs.s3a.bucket.{bucket}"
    return {
        f"{prefix}.aws.credentials.provider": ASSUMED,
        f"{prefix}.assumed.role.arn": role_arn,
        f"{prefix}.assumed.role.session.name": f"visits-job-{bucket}",
        f"{prefix}.assumed.role.session.duration": "1h",
        # credentials used to call STS AssumeRole: instance profile / IRSA, no keys in code
        f"{prefix}.assumed.role.credentials.provider":
            "com.amazonaws.auth.InstanceProfileCredentialsProvider",
    }


# The policies each role should carry (attach via Terraform/IaC, shown here for reference).
READER_POLICY = {"Version": "2012-10-17", "Statement": [{
    "Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket"],
    "Resource": [f"arn:aws:s3:::{INPUT_BUCKET}", f"arn:aws:s3:::{INPUT_BUCKET}/*"]}]}
WRITER_POLICY = {"Version": "2012-10-17", "Statement": [{
    "Effect": "Allow", "Action": ["s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetObject"],
    "Resource": [f"arn:aws:s3:::{OUTPUT_BUCKET}", f"arn:aws:s3:::{OUTPUT_BUCKET}/visits/*"]}]}


def main() -> None:
    builder = SparkSession.builder.appName("fine-grained-resources")
    for conf in (bucket_role_conf(INPUT_BUCKET, INPUT_ROLE_ARN), bucket_role_conf(OUTPUT_BUCKET, OUTPUT_ROLE_ARN)):
        for key, value in conf.items():
            builder = builder.config(key, value)
    spark = builder.getOrCreate()

    visits = spark.read.json(f"s3a://{INPUT_BUCKET}/visits/")
    cleaned = visits.filter(F.col("visit_id").isNotNull()).dropDuplicates(["visit_id"])
    cleaned.write.mode("overwrite").parquet(f"s3a://{OUTPUT_BUCKET}/visits/")


if __name__ == "__main__":
    print("reader policy:", json.dumps(READER_POLICY))
    print("writer policy:", json.dumps(WRITER_POLICY))
    main()
