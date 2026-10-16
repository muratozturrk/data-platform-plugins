"""Metadata Decorator -- technical context in Kafka headers, value unchanged.

The streaming job forwards visits from `visits` to `visits-decorated`. Business
consumers read the value exactly as the producer wrote it; operators read the
job version, micro-batch id, processing time and source coordinates from the
record headers when they debug a release.

  input  : key | value | headers(from producer) | topic | partition | offset
  output : key | value (unchanged) | headers = producer headers (minus ours) + ours

Header values are BINARY in Spark (`array<struct<key:string,value:binary>>`) and
`includeHeaders=true` is needed on BOTH the reader and the writer.

Object-store sinks have no per-record metadata: decoration applies to the whole
file, so `--sink s3` writes each micro-batch to its own prefix and tags the
objects. Amazon Kinesis Data Streams has no headers at all
: fall back to a hidden column or a context table there.

Run:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      spark_kafka_headers_decorator.py [--sink kafka|s3]
Env: KAFKA_BOOTSTRAP, SOURCE_TOPIC, TARGET_TOPIC, JOB_VERSION, CHECKPOINT_DIR,
     S3_BUCKET, S3_PREFIX (s3 sink only; AWS creds from the provider chain).
Self-test (no Spark): python spark_kafka_headers_decorator.py --self-test
"""
import argparse
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("metadata-decorator-spark")

# Keys this job owns. Upstream headers with the same key are replaced, never duplicated.
OWNED_HEADER_KEYS = ("job_version", "batch_version", "processing_time", "source")
S3_MAX_TAGS = 10            # S3 allows at most 10 tags per object
S3_MAX_TAG_VALUE = 256


@dataclass(frozen=True)
class Config:
    bootstrap: str = field(default_factory=lambda: os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    source_topic: str = field(default_factory=lambda: os.environ.get("SOURCE_TOPIC", "visits"))
    target_topic: str = field(default_factory=lambda: os.environ.get("TARGET_TOPIC", "visits-decorated"))
    job_version: str = field(default_factory=lambda: os.environ.get("JOB_VERSION", "v1.0.3"))
    checkpoint: str = field(default_factory=lambda: os.environ.get("CHECKPOINT_DIR", "/tmp/chk/headers-decorator"))
    s3_bucket: str = field(default_factory=lambda: os.environ.get("S3_BUCKET", ""))
    s3_prefix: str = field(default_factory=lambda: os.environ.get("S3_PREFIX", "visits-decorated"))
    max_offsets_per_trigger: int = 50_000


def file_tags(job_version: str, batch_id: int, processing_time: str) -> List[Dict[str, str]]:
    """Per-file metadata for object stores (pure, testable). Enforces S3 tag limits."""
    tags = [{"Key": "job_version", "Value": job_version},
            {"Key": "batch_version", "Value": str(batch_id)},
            {"Key": "processing_time", "Value": processing_time}]
    if len(tags) > S3_MAX_TAGS:
        raise ValueError("too many tags for S3")
    for t in tags:
        if len(t["Value"]) > S3_MAX_TAG_VALUE:
            raise ValueError(f"tag {t['Key']} longer than {S3_MAX_TAG_VALUE} chars")
    return tags


def decorated_headers(batch_df, cfg: Config, batch_id: int, processing_time: str):
    """Column expression: upstream headers without our keys + our technical context."""
    from pyspark.sql import functions as F

    ours = F.array(
        F.struct(F.lit("job_version").alias("key"), F.lit(cfg.job_version).cast("binary").alias("value")),
        F.struct(F.lit("batch_version").alias("key"), F.lit(str(batch_id)).cast("binary").alias("value")),
        F.struct(F.lit("processing_time").alias("key"), F.lit(processing_time).cast("binary").alias("value")),
        F.struct(F.lit("source").alias("key"),
                 F.concat_ws("/", "topic", F.col("partition").cast("string"), F.col("offset").cast("string"))
                 .cast("binary").alias("value")),
    )
    upstream = F.coalesce(F.col("headers"), F.array().cast(batch_df.schema["headers"].dataType))
    upstream = F.filter(upstream, lambda h: ~h["key"].isin(*OWNED_HEADER_KEYS))
    return F.concat(upstream, ours)


def write_kafka(cfg: Config):
    def _write(batch_df, batch_id: int) -> None:
        processing_time = datetime.now(timezone.utc).isoformat()
        out = batch_df.select("key", "value",
                              decorated_headers(batch_df, cfg, batch_id, processing_time).alias("headers"))
        # Kafka sink is at-least-once: a replayed micro-batch re-sends records with the same
        # batch_version header, which is what makes the duplicates traceable.
        (out.write.format("kafka")
            .option("kafka.bootstrap.servers", cfg.bootstrap)
            .option("topic", cfg.target_topic)
            .option("includeHeaders", "true")
            .save())
        log.info("batch %d forwarded with headers job_version=%s", batch_id, cfg.job_version)
    return _write


def write_s3_tagged(cfg: Config):
    """Object-store variant: one prefix per micro-batch, every object tagged with the context."""
    if not cfg.s3_bucket:
        raise SystemExit("S3_BUCKET is required for --sink s3")

    def _write(batch_df, batch_id: int) -> None:
        import boto3

        processing_time = datetime.now(timezone.utc).isoformat()
        prefix = f"{cfg.s3_prefix}/batch_version={batch_id}/"
        # overwrite: a replayed batch rewrites its own prefix instead of appending duplicates
        (batch_df.selectExpr("CAST(value AS STRING) AS value")
            .write.mode("overwrite").text(f"s3a://{cfg.s3_bucket}/{prefix}"))
        s3 = boto3.client("s3")
        tagging = {"TagSet": file_tags(cfg.job_version, batch_id, processing_time)}
        tagged = 0
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=cfg.s3_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                s3.put_object_tagging(Bucket=cfg.s3_bucket, Key=obj["Key"], Tagging=tagging)
                tagged += 1
        log.info("batch %d: %d objects tagged under s3://%s/%s", batch_id, tagged, cfg.s3_bucket, prefix)
    return _write


def main() -> None:
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--sink", choices=["kafka", "s3"], default="kafka")
    args = parser.parse_args()
    cfg = Config()

    spark = SparkSession.builder.appName("metadata-decorator-headers").getOrCreate()
    visits = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", cfg.bootstrap)
              .option("subscribe", cfg.source_topic)
              .option("includeHeaders", "true")          # keep upstream headers
              .option("startingOffsets", "earliest")
              .option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)
              .load())
    sink = write_kafka(cfg) if args.sink == "kafka" else write_s3_tagged(cfg)
    query = (visits.writeStream.foreachBatch(sink)
             .option("checkpointLocation", f"{cfg.checkpoint}/{args.sink}")
             .queryName(f"metadata-decorator-{args.sink}")
             .start())
    log.info("started %s sink, job_version=%s", args.sink, cfg.job_version)
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()


def _self_test() -> None:
    tags = file_tags("v1.0.3", 7, "2024-06-10T10:02:00+00:00")
    assert [t["Key"] for t in tags] == ["job_version", "batch_version", "processing_time"]
    assert tags[1]["Value"] == "7"
    try:
        file_tags("v" * 300, 1, "t")
        raise AssertionError("expected tag length check")
    except ValueError:
        pass
    print("self-test OK")


if __name__ == "__main__":
    _self_test() if "--self-test" in sys.argv else main()
