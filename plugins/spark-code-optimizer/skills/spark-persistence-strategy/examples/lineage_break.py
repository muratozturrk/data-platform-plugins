"""lineage_break: reliable checkpoint for a deep plan, plus checkpoint-directory cleanup.

* SPARK_CHECKPOINT_DIR must be a shared FS on a cluster (hdfs://, s3a://, abfss://, gs://).
  The file:// fallback below is only valid in local mode.
* Spark writes into <dir>/<app-uuid>/; getCheckpointDir() returns that subdirectory, so cleanup
  deletes only this application's files. Path.getFileSystem() resolves the right FS for the URI
  (FileSystem.get() would return the default FS, wrong for s3a:// paths).
* Alternative: --conf spark.cleaner.referenceTracking.cleanCheckpoints=true at launch.

Run: spark-submit lineage_break.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def delete_checkpoint_dir(spark):
    """Recursively delete this app's checkpoint subdirectory. Call after the last action."""
    sc = spark.sparkContext
    ckpt = sc.getCheckpointDir()        # PySpark 3.1+; None if never set
    if not ckpt:
        return
    jvm = sc._jvm
    path = jvm.org.apache.hadoop.fs.Path(ckpt)
    path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(path, True)


def main():
    spark = SparkSession.builder.master("local[2]").appName("lineage_break").getOrCreate()
    spark.sparkContext.setCheckpointDir(
        os.environ.get("SPARK_CHECKPOINT_DIR", "file:///tmp/spark-ckpt"))

    df = spark.range(200_000).withColumn("v", F.col("id") % 97)
    try:
        for i in range(25):                              # deep chain (depth > 20)
            df = df.withColumn(f"c{i}", F.col("v") * i + F.col("id") % (i + 2))
            if i == 12:
                df = df.checkpoint(eager=True)           # truncate after the heavy part
                df.explain()                             # Scan ExistingRDD / LogicalRDD
        result = df.groupBy("v").agg(F.sum("c24").alias("s"))
        print(result.orderBy("v").limit(3).collect())
    finally:
        delete_checkpoint_dir(spark)
        spark.stop()


if __name__ == "__main__":
    main()
