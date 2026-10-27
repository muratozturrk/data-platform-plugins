"""Wave-1 pilot migration ("staged migration plan", "fallback procedures" of the architecture guide).

Two ways to bring existing data under Iceberg, both verified by row count and content fingerprint:
  * in place: CREATE TABLE + add_files registers the existing Parquet files; no data is rewritten
  * rewrite:  CTAS copies data into new Parquet files under the Iceberg table location
              (the only option for CSV/JSON files and for warehouse tables read over JDBC)
"""
import time
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.lakehouse import CATALOG, pg_jdbc_options


@dataclass
class MigrationResult:
    table: str
    strategy: str
    source_rows: int
    target_rows: int
    fingerprint_match: bool
    data_files: int
    files_outside_table_location: int      # data files that still live in the legacy location
    bytes_written: int                     # new data bytes under the Iceberg table location
    seconds: float

    @property
    def verified(self) -> bool:
        return self.source_rows == self.target_rows and self.fingerprint_match


def fingerprint(df: DataFrame) -> tuple[int, int]:
    """(rows, order-independent content hash). Column order is normalized so source and target compare."""
    cols = sorted(df.columns)
    row = df.select(F.xxhash64(*cols).alias("h")).agg(F.count("*").alias("n"), F.sum("h").alias("s")).first()
    return row.n, row.s or 0


def read_legacy(spark: SparkSession, uri: str, fmt: str) -> DataFrame:
    reader = spark.read
    if fmt == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")
    return reader.format(fmt).load(uri)


def read_warehouse(spark: SparkSession, dbtable: str) -> DataFrame:
    return spark.read.format("jdbc").options(**pg_jdbc_options(), dbtable=dbtable).load()


def _ddl(df: DataFrame) -> str:
    return ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields)


def _measure(spark, table, strategy, source: DataFrame, started) -> MigrationResult:
    src_rows, src_hash = fingerprint(source)
    tgt_rows, tgt_hash = fingerprint(spark.table(table))
    location = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").where("col_name = 'Location'").first().data_type
    files = spark.sql(f"SELECT file_path, file_size_in_bytes FROM {table}.data_files").collect()
    inside = [f for f in files if f.file_path.split("://", 1)[1].startswith(location.split("://", 1)[1])]
    return MigrationResult(table, strategy, src_rows, tgt_rows, src_hash == tgt_hash, len(files),
                           len(files) - len(inside), sum(f.file_size_in_bytes for f in inside),
                           round(time.monotonic() - started, 2))


def migrate_in_place(spark: SparkSession, source_uri: str, table: str, partition_col: str) -> MigrationResult:
    """Register existing Hive-style Parquet files with add_files. The target spec must match the directory layout."""
    started = time.monotonic()
    source = spark.read.parquet(source_uri)                # Hive partition discovery adds `partition_col`
    spark.sql(f"CREATE TABLE {table} ({_ddl(source)}) USING iceberg PARTITIONED BY ({partition_col})")
    spark.sql(f"CALL {CATALOG}.system.add_files(table => '{table}', source_table => '`parquet`.`{source_uri}`')")
    return _measure(spark, table, "add_files", source, started)


def migrate_by_rewrite(spark: SparkSession, source: DataFrame, table: str,
                       partition_col: str | None = None) -> MigrationResult:
    """CTAS: full copy into Iceberg-managed Parquet files."""
    started = time.monotonic()
    writer = source.writeTo(table).using("iceberg")
    if partition_col:
        writer = writer.partitionedBy(F.col(partition_col))
    writer.create()
    return _measure(spark, table, "rewrite", source, started)
