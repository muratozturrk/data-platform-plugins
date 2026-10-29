"""The Spark DataFrame API against Polaris, every snippet in order on
polaris.db.events (the FILE catalog from bootstrap.py)."""
from pyspark.sql.functions import col, lit

from conftest import record

columns = ["id", "name", "event_date"]


def rows(spark, table="polaris.db.events"):
    return sorted(tuple(r) for r in spark.table(table).collect())


def test_create_table_with_writerv2(spark, checks, measure):
    data = [
        (1, "Alice", "2023-01-01"),
        (2, "Bob", "2023-01-02"),
        (3, "Charlie", "2023-01-03"),
    ]
    df = spark.createDataFrame(data, columns)
    df.writeTo("polaris.db.events") \
      .partitionedBy("event_date") \
      .tableProperty("write.format.default", "parquet") \
      .create()
    schema = {f.name: f.dataType.simpleString() for f in spark.table("polaris.db.events").schema}
    spec = spark.sql("SELECT * FROM polaris.db.events.partitions").columns
    assert schema == {"id": "bigint", "name": "string", "event_date": "string"}
    record(checks, "DataFrame: writeTo().partitionedBy().create()", "ok", "as-is",
           "event_date is a STRING column (Python strings); the SQL section declares DATE")
    measure("created_schema", {"schema": schema, "partitions_table_columns": spec})


def test_query_and_filter(spark, checks, measure):
    df = spark.table("polaris.db.events")
    df.show()
    filtered_df = df.filter(col("event_date") > lit("2023-01-01"))
    filtered_df.show()
    assert filtered_df.count() == 2
    record(checks, "DataFrame: spark.table() + filter(col > lit)", "ok", "as-is",
           "string comparison, correct only because the dates are ISO formatted")
    measure("filtered_rows", filtered_df.count())


def test_merge_into(spark, checks, measure):
    updates = [
        (1, "Alice Updated", "2023-01-01"),
        (4, "David", "2023-01-04"),
    ]
    updates_df = spark.createDataFrame(updates, columns)
    updates_df.createOrReplaceTempView("updates")
    spark.sql("""
        MERGE INTO polaris.db.events t
        USING updates u
        ON t.id = u.id
        WHEN MATCHED THEN UPDATE SET t.name = u.name
        WHEN NOT MATCHED THEN INSERT (id, name, event_date) VALUES (u.id, u.name,
        u.event_date)
    """)
    assert rows(spark)[0] == (1, "Alice Updated", "2023-01-01") and len(rows(spark)) == 4
    record(checks, "DataFrame section: MERGE INTO from temp view", "ok", "as-is")
    measure("after_merge", rows(spark))


def test_delete_rows(spark, checks, measure):
    spark.sql("""
        DELETE FROM polaris.db.events
        WHERE event_date < '2023-01-02'
    """)
    assert [r[0] for r in rows(spark)] == [2, 3, 4]
    record(checks, "DataFrame section: DELETE FROM ... WHERE event_date < '2023-01-02'", "ok", "as-is")


def test_append(spark, checks, measure):
    new_data = [
        (5, "Eve", "2023-01-05"),
        (6, "Frank", "2023-01-06"),
    ]
    new_df = spark.createDataFrame(new_data, columns)
    new_df.writeTo("polaris.db.events").append()
    assert [r[0] for r in rows(spark)] == [2, 3, 4, 5, 6]
    record(checks, "DataFrame: writeTo().append()", "ok", "as-is")


def test_metadata_tables(spark, checks, measure):
    snapshots_df = spark.table("polaris.db.events.snapshots")
    snapshots_df.show()
    history_df = spark.table("polaris.db.events.history")
    history_df.show()
    ops = [r.operation for r in snapshots_df.orderBy("committed_at").collect()]
    files = spark.table("polaris.db.events.files").count()
    assert ops[0] == "append" and len(ops) == 4 and history_df.count() == 4
    record(checks, "DataFrame: .snapshots and .history metadata tables", "ok", "as-is")
    # Copy-on-write is the Iceberg 1.5 default: MERGE and DELETE rewrite whole partition files.
    measure("snapshot_operations", {"operations": ops, "data_files": files,
                                        "table_properties": spark.sql("SHOW TBLPROPERTIES polaris.db.events").collect()})
