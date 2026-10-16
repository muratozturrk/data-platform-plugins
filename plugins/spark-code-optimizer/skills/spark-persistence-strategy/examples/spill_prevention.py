"""spill_prevention: reduce shuffle spill for data larger than memory.

Notes vs sparkopt's generated code:
  * spark.memory.fraction / storageFraction are launch-time configs: builder or spark-submit --conf
    (runtime spark.conf.set is rejected in Spark 3.x). The bundled copy emits them as --conf
    comments; original sparkopt emitted spark.conf.set.
  * correction: size the existing shuffle (shuffle.partitions + AQE advisory size) instead of an
    extra repartition();
  * MEMORY_AND_DISK (original sparkopt: MEMORY_AND_DISK_SER); persist only because two
    aggregations reuse the input.
Check Spark UI -> Stages -> Spill (Memory) / Spill (Disk) before and after.

Run: spark-submit spill_prevention.py [size_mb]
"""
import sys

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def shuffle_partitions(size_mb, target_mb=128, minimum=200):
    """sparkopt rule: max(200, size_mb / 128) partitions of ~128 MB."""
    return max(minimum, int(size_mb / target_mb))


def main(size_mb):
    assert shuffle_partitions(10_000) == 200
    assert shuffle_partitions(100_000) == 781

    spark = (SparkSession.builder.master("local[2]").appName("spill_prevention")
             # Launch-time memory settings; change from defaults (0.6 / 0.5) only with evidence.
             .config("spark.memory.fraction", "0.7")
             .config("spark.memory.storageFraction", "0.4")
             .getOrCreate())

    # Runtime SQL configs are fine to set here.
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions(size_mb)))

    df = spark.range(3_000_000).select(
        (F.col("id") % 1000).alias("cat"),
        (F.col("id") % 37).alias("region"),
        F.rand(seed=7).alias("val"),
    ).persist(StorageLevel.MEMORY_AND_DISK)
    df.count()
    try:
        df.groupBy("cat").agg(F.sum("val").alias("s")).orderBy("cat").show(3)
        df.groupBy("region").agg(F.count("*").alias("n")).orderBy("region").show(3)
    finally:
        df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main(float(sys.argv[1]) if len(sys.argv) > 1 else 50_000)
