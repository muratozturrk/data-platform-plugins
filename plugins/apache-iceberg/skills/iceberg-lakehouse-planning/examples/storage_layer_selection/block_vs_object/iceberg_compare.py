"""The same Iceberg table on block storage (local disk) and on object storage (S3 API).

| Catalog | Storage                  | How a commit becomes visible                          |
|---------|--------------------------|-------------------------------------------------------|
| local   | Local volume (file://)   | Hadoop catalog: write vN.metadata.json, atomic rename |
| s3a     | SeaweedFS via s3a://     | Hadoop catalog: same rename, emulated as COPY+DELETE  |
| lake    | SeaweedFS via S3FileIO   | REST catalog swaps the metadata pointer, no rename    |
"""
import os
import time
from dataclasses import dataclass

from common.lakehouse import events_df
from common.s3 import list_keys, split_uri

DDL = ("CREATE TABLE {t} (order_id BIGINT, customer_id BIGINT, country STRING, amount DOUBLE, "
       "event_day DATE) USING iceberg PARTITIONED BY (event_day)")


@dataclass(frozen=True)
class TableRun:
    catalog: str
    rows: int
    amount: float
    write_s: float
    scan_s: float
    metadata_files: list[str]


def table_location(spark, table: str) -> str:
    return spark.sql(f"DESCRIBE TABLE EXTENDED {table}").where("col_name = 'Location'").first().data_type


def metadata_file_names(location: str) -> list[str]:
    if location.startswith("file:"):
        return sorted(os.listdir(location[len("file:"):].replace("///", "/") + "/metadata"))
    bucket, prefix = split_uri(location)
    return sorted(k.rsplit("/", 1)[-1] for k in list_keys("seaweedfs", bucket, prefix + "/metadata/"))


def build_table(spark, catalog: str, ns: str, name: str, commits: int = 3, rows_per_commit: int = 20_000) -> TableRun:
    table = f"{catalog}.{ns}.{name}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{ns}")
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    spark.sql(DDL.format(t=table))
    t = time.perf_counter()
    for i in range(commits):
        events_df(spark, rows_per_commit, seed=i).writeTo(table).append()
    write_s = time.perf_counter() - t
    t = time.perf_counter()
    row = spark.sql(f"SELECT count(*) n, round(sum(amount), 2) s FROM {table}").first()
    scan_s = time.perf_counter() - t
    return TableRun(catalog, row.n, row.s, round(write_s, 2), round(scan_s, 2),
                    metadata_file_names(table_location(spark, table)))
