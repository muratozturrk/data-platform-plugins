# spark_readiness_marker_writer.py
#
# Readiness Marker pattern: writes a _READY marker file to a Delta
# partition after the Spark job successfully commits all data.
# The marker file contains JSON metadata (row count, batch_id, schema hash)
# so consumers can validate completeness before starting.
#
# This module is imported by any Spark job that produces data
# consumed by downstream pipelines.
#
# Requirements: delta-spark, pyspark

import json
import hashlib
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable

log = logging.getLogger(__name__)


# ── Marker dataclass ───────────────────────────────────────────────────────────

@dataclass
class ReadinessMarker:
    """
    Metadata written to the _READY marker file.
    Downstream jobs read this to validate completeness before processing.
    """
    dataset:        str          # logical name of the dataset
    partition_date: str          # date partition this marker covers
    batch_id:       str          # unique run identifier
    row_count:      int          # exact row count written
    schema_hash:    str          # hash of column names — detects schema drift
    written_at:     str          # UTC ISO timestamp of when marker was written
    delta_version:  Optional[int] = None   # Delta table version after commit
    extra:          Optional[dict] = None  # any additional metadata


# ── Marker writer ──────────────────────────────────────────────────────────────

class ReadinessMarkerWriter:
    """
    Writes and manages readiness markers for Spark pipeline outputs.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._hadoop_conf = spark.sparkContext._jvm \
            .org.apache.hadoop.conf.Configuration()

    def _get_fs(self, path: str):
        return self.spark.sparkContext._jvm \
            .org.apache.hadoop.fs.FileSystem.get(
                self.spark.sparkContext._jvm.java.net.URI.create(path),
                self._hadoop_conf
            )

    def _schema_hash(self, df: DataFrame) -> str:
        """Returns a stable hash of the DataFrame's column names."""
        col_signature = ",".join(sorted(df.columns))
        return hashlib.md5(col_signature.encode()).hexdigest()[:8]

    def write_marker(
        self,
        df:             DataFrame,
        output_path:    str,
        dataset:        str,
        partition_date: str,
        batch_id:       str,
        extra:          Optional[dict] = None,
    ) -> ReadinessMarker:
        """
        Writes a _READY marker file to output_path/_ingestion_date=partition_date/
        IMPORTANT: Call this ONLY after the DataFrame has been successfully
        written and committed — never before.
        """
        marker_dir  = f"{output_path}/_ingestion_date={partition_date}"
        marker_path = f"{marker_dir}/_READY"

        # Collect metadata
        row_count    = df.count()
        schema_hash  = self._schema_hash(df)
        written_at   = datetime.now(tz=timezone.utc).isoformat()

        # Get Delta table version if this is a Delta table
        delta_version = None
        try:
            dt = DeltaTable.forPath(self.spark, output_path)
            history = dt.history(1).collect()
            if history:
                delta_version = history[0]["version"]
        except Exception:
            pass   # not a Delta table — that is fine

        marker = ReadinessMarker(
            dataset        = dataset,
            partition_date = partition_date,
            batch_id       = batch_id,
            row_count      = row_count,
            schema_hash    = schema_hash,
            written_at     = written_at,
            delta_version  = delta_version,
            extra          = extra or {},
        )

        # Write marker file atomically using Hadoop FileSystem API
        # Writing to a temp file then renaming ensures atomicity —
        # a partially-written marker is never visible to consumers
        temp_path   = marker_path + ".tmp"
        fs          = self._get_fs(marker_path)
        path_obj    = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path

        # Write to temp
        output_stream = fs.create(path_obj(temp_path), True)
        marker_bytes  = json.dumps(asdict(marker), indent=2).encode("utf-8")
        output_stream.write(marker_bytes)
        output_stream.close()

        # Atomic rename: temp → final marker path
        fs.rename(path_obj(temp_path), path_obj(marker_path))

        log.info(
            "Readiness marker written | dataset=%s | date=%s | "
            "rows=%d | batch_id=%s | path=%s",
            dataset, partition_date, row_count, batch_id, marker_path,
        )
        return marker

    def read_marker(
        self,
        output_path:    str,
        partition_date: str,
    ) -> Optional[ReadinessMarker]:
        """
        Reads and returns the readiness marker for a given partition,
        or None if the marker does not exist yet.
        """
        marker_path = (
            f"{output_path}/_ingestion_date={partition_date}/_READY"
        )
        fs       = self._get_fs(marker_path)
        path_obj = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path

        if not fs.exists(path_obj(marker_path)):
            return None

        input_stream = fs.open(path_obj(marker_path))
        content = bytes(input_stream.read()).decode("utf-8")
        input_stream.close()

        data = json.loads(content)
        return ReadinessMarker(**data)

    def marker_exists(self, output_path: str, partition_date: str) -> bool:
        """Returns True if the readiness marker exists for the partition."""
        return self.read_marker(output_path, partition_date) is not None

    def delete_marker(self, output_path: str, partition_date: str):
        """
        Deletes a marker — used when a partition needs to be reprocessed.
        The downstream sensor will then wait again until the marker is re-written.
        """
        marker_path = (
            f"{output_path}/_ingestion_date={partition_date}/_READY"
        )
        fs       = self._get_fs(marker_path)
        path_obj = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path

        if fs.exists(path_obj(marker_path)):
            fs.delete(path_obj(marker_path), False)
            log.info("Marker deleted: %s", marker_path)


# ── Example usage: Silver transformation job writes marker on success ──────────

def run_silver_job_with_marker(
    spark: SparkSession,
    processing_date: str,
    batch_id: str,
):
    """
    Example showing how a Silver transformation job writes
    the readiness marker as its final step.
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    BRONZE_PATH = "/data/raw/orders"
    SILVER_PATH = "/data/silver/orders"

    marker_writer = ReadinessMarkerWriter(spark)

    # Step 1: Read and transform (abbreviated — full logic in Transformation Replicator)
    bronze_df = (
        spark.read.format("delta").load(BRONZE_PATH)
        .filter(F.col("_ingestion_date") == processing_date)
    )
    silver_df = bronze_df.withColumn("loaded_at", F.current_timestamp())

    # Step 2: Write Silver data
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        DeltaTable.forPath(spark, SILVER_PATH).alias("t") \
            .merge(silver_df.alias("s"), "t.id = s.id") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        silver_df.write.format("delta").mode("overwrite").save(SILVER_PATH)

    # Step 3: Write readiness marker AFTER data is committed
    # This is the ONLY correct place — never write the marker before the data write
    marker = marker_writer.write_marker(
        df             = silver_df,
        output_path    = SILVER_PATH,
        dataset        = "orders_silver",
        partition_date = processing_date,
        batch_id       = batch_id,
        extra          = {"source": "transformation_replicator"},
    )

    log.info("Silver job complete. Marker written: %s", marker)