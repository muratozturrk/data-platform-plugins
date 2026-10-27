"""The five lakehouse layers (storage, ingestion, catalog, federation, consumption), mapped to this lab stack, with a probe per layer.

Spark writes, PyIceberg writes and reads, DuckDB queries: three engines, one REST catalog,
one object store. PyIceberg API: https://py.iceberg.apache.org/api/
"""
from dataclasses import dataclass

import duckdb
import pyarrow as pa
from pyspark.sql import SparkSession

from common.lakehouse import REST_URI, S3_ENDPOINT, rest_get, s3_client


@dataclass(frozen=True)
class Layer:
    name: str
    typical_examples: str
    in_this_lab: str


LAYERS = [
    Layer("storage", "S3, ADLS, GCS, HDFS, MinIO, Ceph", f"SeaweedFS S3 API at {S3_ENDPOINT}"),
    Layer("ingestion", "Spark, Flink, Kafka Connect, Fivetran", "Spark batch writes, PyIceberg micro-batch appends"),
    Layer("catalog", "Polaris, Nessie, Glue, Gravitino, Lakekeeper, REST", f"Iceberg REST catalog at {REST_URI}"),
    Layer("federation", "Dremio, Trino", "DuckDB joining an Iceberg table with a non-Iceberg source"),
    Layer("consumption", "BI tools, notebooks, AI/ML, data APIs", "Python consumer reading an Arrow result"),
]


def probe_storage(bucket: str = "warehouse") -> bool:
    return bucket in {b["Name"] for b in s3_client().list_buckets()["Buckets"]}


def probe_catalog() -> bool:
    return "defaults" in rest_get("/v1/config") and "namespaces" in rest_get("/v1/namespaces")


def probe_ingestion(spark: SparkSession, catalog, table: str, arrow_batch: pa.Table) -> bool:
    before = spark.table(table).count()
    catalog.load_table(table).append(arrow_batch)
    return spark.table(table).count() == before + arrow_batch.num_rows


def federated_revenue(catalog, table: str, customers: pa.Table) -> pa.Table:
    """Join the Iceberg orders table with CRM data that never left its own system."""
    con = duckdb.connect()
    con.register("orders", catalog.load_table(table).scan(selected_fields=("customer_id", "amount")).to_arrow())
    con.register("crm", customers)
    return con.execute("""
        SELECT c.segment, round(sum(o.amount), 2) AS revenue, count(*) AS orders
        FROM orders o JOIN crm c USING (customer_id)
        GROUP BY c.segment ORDER BY c.segment
    """).arrow()


def probe_all(spark: SparkSession, catalog, table: str, arrow_batch: pa.Table, customers: pa.Table) -> dict[str, bool]:
    status = {"storage": probe_storage(), "catalog": probe_catalog(),
              "ingestion": probe_ingestion(spark, catalog, table, arrow_batch)}
    result = federated_revenue(catalog, table, customers)
    return status | {
        "federation": result.num_rows > 0,
        "consumption": sum(result.column("orders").to_pylist()) == spark.table(table).count(),
    }
