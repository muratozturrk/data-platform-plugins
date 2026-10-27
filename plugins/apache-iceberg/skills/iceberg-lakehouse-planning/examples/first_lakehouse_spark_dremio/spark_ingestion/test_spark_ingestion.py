"""PostgreSQL -> Spark -> Iceberg table in Nessie, files in object storage."""
import pytest
import requests
from pyspark.sql import functions as F

from common.lakehouse import NESSIE_URI, list_keys, run_postgres, table_location
from spark_ingestion.ingest import full_reload, incremental_append, read_source

FINGERPRINT = "count(*) AS n, sum(sales_amount) AS total, min(sales_date) AS first_day, max(sales_date) AS last_day"


@pytest.fixture(scope="module")
def table(spark, ns):
    return full_reload(spark, ns)


def test_iceberg_table_matches_the_source(spark, table):
    source = read_source(spark)
    source.createOrReplaceTempView("pg_fashion_sales")
    expected = spark.sql(f"SELECT {FINGERPRINT} FROM pg_fashion_sales").first()
    actual = spark.sql(f"SELECT {FINGERPRINT} FROM {table}").first()
    assert actual == expected
    assert expected.n == 3000
    # The five hand-written seed rows arrive unchanged.
    first5 = spark.table(table).where("id <= 5").orderBy("id").select("product_name", "sales_amount").collect()
    assert [r.product_name for r in first5] == [
        "Slim Fit Jeans", "Leather Jacket", "Graphic T-Shirt", "Summer Dress", "Casual Sneakers"]
    assert float(first5[1].sales_amount) == 249.99


def test_jdbc_types_map_to_iceberg_types(spark, table):
    types = {r.col_name: r.data_type for r in spark.sql(f"DESCRIBE TABLE {table}").collect()
             if r.col_name and not r.col_name.startswith("#")}
    assert types["id"] == "int"                      # SERIAL
    assert types["sales_amount"] == "decimal(10,2)"  # exact money type preserved, not double
    assert types["sales_date"] == "date"
    assert types["product_name"] == "string"         # VARCHAR(n) -> string (length is not kept)


def test_storage_holds_parquet_data_and_iceberg_metadata(spark, table):
    location = table_location(spark, table)
    assert location.startswith(f"s3://warehouse/{table.split('.')[0]}/fashion_sales_")   # <ns>/<table>_<uuid>
    keys = list_keys(location + "/")
    data = [k for k in keys if "/data/" in k]
    meta = [k for k in keys if "/metadata/" in k]
    assert data and all(k.endswith(".parquet") for k in data)
    assert any(k.endswith(".metadata.json") for k in meta)                    # table metadata (JSON)
    assert any(k.split("/")[-1].startswith("snap-") and k.endswith(".avro") for k in meta)   # manifest list
    assert any(k.endswith("-m0.avro") for k in meta)                           # manifest


def test_nessie_records_the_table_and_each_commit(spark, ns, table):
    entries = requests.get(f"{NESSIE_URI}/trees/main/entries").json()["entries"]
    names = {".".join(e["name"]["elements"]): e["type"] for e in entries}
    assert names[table] == "ICEBERG_TABLE"
    assert names[ns] == "NAMESPACE"

    content = requests.get(f"{NESSIE_URI}/trees/main/contents/{table}").json()["content"]
    current = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    assert content["snapshotId"] == current.snapshot_id                 # Nessie points at the latest snapshot
    assert content["metadataLocation"].startswith(table_location(spark, table))

    log = requests.get(f"{NESSIE_URI}/trees/main/history", params={"maxRecords": 200}).json()["logEntries"]
    messages = [e["commitMeta"]["message"] for e in log]
    assert any(table in m for m in messages)                            # every catalog change is a Nessie commit

    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    assert len(props["nessie.commit.id"]) == 64  # Nessie commit hash recorded on the table
    assert props["gc.enabled"] == "false"          # Nessie tables refuse DROP TABLE ... PURGE


def test_create_or_replace_is_a_full_reload_not_a_duplicate(spark, ns, table):
    before = spark.table(table).count()
    snapshots_before = spark.sql(f"SELECT count(*) c FROM {table}.snapshots").first().c
    full_reload(spark, ns)
    assert spark.table(table).count() == before                          # replaced, not appended
    assert spark.sql(f"SELECT count(*) c FROM {table}.snapshots").first().c == snapshots_before + 1


def test_incremental_append_moves_only_new_source_rows(spark, table):
    max_id = spark.table(table).agg(F.max("id")).first()[0]
    run_postgres(spark, """
        INSERT INTO fashion_sales (product_name, category, sales_amount, sales_date, store_location,
                                   customer_age_group, campaign_name)
        VALUES ('Rain Jacket', 'Outerwear', 159.00, '2024-06-01', 'Seattle', '25-34', 'Summer Kickoff'),
               ('Silk Scarf', 'Accessories', 49.50, '2024-06-01', 'Miami', '35-44', 'Summer Kickoff')""")
    try:
        assert incremental_append(spark, table) == 2
        assert incremental_append(spark, table) == 0                    # idempotent: nothing new
        added = spark.table(table).where(f"id > {max_id}").orderBy("id").collect()
        assert [r.product_name for r in added] == ["Rain Jacket", "Silk Scarf"]
        last = spark.sql(f"SELECT operation, summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
        assert last.operation == "append" and last.summary["added-records"] == "2"
    finally:
        run_postgres(spark, f"DELETE FROM fashion_sales WHERE id > {max_id}")
