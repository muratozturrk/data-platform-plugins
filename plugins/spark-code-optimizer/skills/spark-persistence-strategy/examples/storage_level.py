"""storage_level: PySpark-valid StorageLevel selection and legacy (_SER) name mapping.

PySpark has no MEMORY_ONLY_SER / MEMORY_AND_DISK_SER / *_SER_2 (Scala/Java only). DataFrame cache is
columnar and compressed already, so map them to the non-SER level. spark.serializer (Kryo) is a
launch-time config set in the builder; it does not change DataFrame cache size.

Run: spark-submit storage_level.py
"""
from pyspark import StorageLevel
from pyspark.sql import SparkSession

LEGACY = {
    "MEMORY_ONLY_SER": "MEMORY_ONLY",
    "MEMORY_ONLY_SER_2": "MEMORY_ONLY_2",
    "MEMORY_AND_DISK_SER": "MEMORY_AND_DISK",
    "MEMORY_AND_DISK_SER_2": "MEMORY_AND_DISK_2",
}


def to_storage_level(name):
    """Resolve a level name (including Scala-only _SER names) to a PySpark StorageLevel."""
    name = LEGACY.get(name.upper(), name.upper())
    level = getattr(StorageLevel, name, None)
    if not isinstance(level, StorageLevel):
        raise ValueError(f"Unknown PySpark StorageLevel: {name}")
    return level


def recommended_level(df_size_mb, executor_memory_mb=4096, executor_count=10):
    """sparkopt MemoryProfile heuristic (0.6 * 0.5 usable, 0.8 headroom, /3 'serialized')."""
    usable = executor_memory_mb * executor_count * 0.6 * 0.5
    if df_size_mb < usable * 0.8:
        return StorageLevel.MEMORY_ONLY
    if df_size_mb / 3.0 < usable * 0.8:
        return StorageLevel.MEMORY_AND_DISK
    return StorageLevel.DISK_ONLY


def main():
    assert to_storage_level("MEMORY_AND_DISK_SER") is StorageLevel.MEMORY_AND_DISK
    assert to_storage_level("memory_only_ser_2") is StorageLevel.MEMORY_ONLY_2
    assert to_storage_level("DISK_ONLY_3") is StorageLevel.DISK_ONLY_3
    assert not hasattr(StorageLevel, "MEMORY_ONLY_SER")

    spark = (SparkSession.builder.master("local[2]").appName("storage_level")
             # Kryo goes here (launch-time), never spark.conf.set at runtime:
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             .getOrCreate())
    df = spark.range(1_000_000).selectExpr("id", "id % 10 AS k")

    df.cache()
    df.count()
    print("cache() level:", df.storageLevel)        # MEMORY_AND_DISK_DESER for DataFrames
    df.unpersist()

    level = recommended_level(df_size_mb=15)
    df.persist(level)                               # must unpersist before changing level
    df.count()
    print("recommended:", df.storageLevel)
    df.groupBy("k").count().show()
    df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
