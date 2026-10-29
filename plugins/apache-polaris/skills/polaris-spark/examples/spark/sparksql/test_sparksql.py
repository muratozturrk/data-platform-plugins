"""SparkSQL against Polaris, every statement in order. The
statements reuse the table name polaris.db.events, which the DataFrame section already created; after
recording that, the statements run against polaris.db.sql_events (only the name changed)."""
import time

import pytest

from conftest import jerr, record

CREATE = """
    CREATE TABLE polaris.db.events (
        id BIGINT,
        name STRING,
        event_date DATE
    )
    USING iceberg
    PARTITIONED BY (event_date)
    TBLPROPERTIES ('write.format.default' = 'parquet')
"""


def sql(spark, text):
    return spark.sql(text.replace("polaris.db.events", "polaris.db.sql_events"))


def ids(spark):
    return [r[0] for r in spark.sql("SELECT id FROM polaris.db.sql_events ORDER BY id").collect()]


def test_create_table_name_collides_with_dataframe_section(spark, checks, measure):
    with pytest.raises(Exception) as err:
        spark.sql(CREATE)
    assert "already exists" in str(err.value)
    sql(spark, CREATE)
    types = {f.name: f.dataType.simpleString() for f in spark.table("polaris.db.sql_events").schema}
    assert types["event_date"] == "date"
    record(checks, "SQL: CREATE TABLE polaris.db.events", "fails", "fails if run after the DataFrame section",
           "TableAlreadyExists; use another name or DROP TABLE first")
    measure("create_collision", jerr(err.value))


def verbatim_or_fixed(spark, text, fixed, checks, example, fix):
    """Run the original statement; if it fails, record why and run the corrected one."""
    try:
        sql(spark, text)
        record(checks, example, "ok", "as-is")
        return None
    except Exception as e:                      # noqa: BLE001 - the error text is the result
        record(checks, example, "fails", "fails", fix)
        sql(spark, fixed)
        return jerr(e)


def test_insert_string_literals_into_date(spark, checks, measure):
    original = """
        INSERT INTO polaris.db.events
        VALUES
            (1, 'Alice', '2023-01-01'),
            (2, 'Bob', '2023-01-02'),
            (3, 'Charlie', '2023-01-03')
    """
    fixed = original.replace("'2023", "DATE '2023")
    error = verbatim_or_fixed(spark, original, fixed, checks, "SQL: INSERT INTO ... VALUES with date strings",
                              "Iceberg refuses STRING to DATE; write DATE '2023-01-01' literals")
    assert error and "CANNOT_SAFELY_CAST" in error
    assert ids(spark) == [1, 2, 3]
    measure("insert_error", error)


def test_select_and_aggregate(spark, checks, measure):
    sql(spark, "SELECT * FROM polaris.db.events").show()
    agg = sql(spark, """
        SELECT event_date, COUNT(*) AS event_count
        FROM polaris.db.events
        WHERE event_date > '2023-01-01'
        GROUP BY event_date
        ORDER BY event_date
    """).collect()
    assert [str(r.event_date) for r in agg] == ["2023-01-02", "2023-01-03"]
    record(checks, "SQL: SELECT * and filtered GROUP BY", "ok", "as-is")


def test_update(spark, checks, measure):
    sql(spark, """
        UPDATE polaris.db.events
        SET name = 'Alice Updated'
        WHERE id = 1
    """)
    assert sql(spark, "SELECT name FROM polaris.db.events WHERE id = 1").collect()[0][0] == "Alice Updated"
    record(checks, "SQL: UPDATE ... WHERE id = 1", "ok", "as-is")


def test_delete(spark, checks, measure):
    sql(spark, """
        DELETE FROM polaris.db.events
        WHERE event_date < '2023-01-02'
    """)
    assert ids(spark) == [2, 3]
    record(checks, "SQL: DELETE FROM ... WHERE event_date < '2023-01-02'", "ok", "as-is")


def test_merge_with_inline_source(spark, checks, measure):
    original = """
        MERGE INTO polaris.db.events t
        USING (
            SELECT 1 AS id, 'Alice Updated' AS name, '2023-01-01' AS event_date
            UNION ALL
            SELECT 4, 'David', '2023-01-04'
        ) u
        ON t.id = u.id
        WHEN MATCHED THEN UPDATE SET t.name = u.name
        WHEN NOT MATCHED THEN INSERT (id, name, event_date) VALUES (u.id, u.name,
        u.event_date)
    """
    fixed = original.replace("'2023-01-01' AS event_date", "DATE '2023-01-01' AS event_date").replace("'David', '2023-01-04'", "'David', DATE '2023-01-04'")
    error = verbatim_or_fixed(spark, original, fixed, checks, "SQL: MERGE INTO with UNION ALL source",
                              "string event_date in the source; use DATE literals")
    # id 1 was deleted by the previous statement, so the "update" branch inserts it again.
    assert ids(spark) == [1, 2, 3, 4]
    measure("merge_error", error)


def test_metadata_tables(spark, checks, measure):
    snaps = sql(spark, "SELECT * FROM polaris.db.events.snapshots").collect()
    hist = sql(spark, "SELECT * FROM polaris.db.events.history").collect()
    assert len(snaps) == len(hist) == 4
    record(checks, "SQL: SELECT * FROM .snapshots / .history", "ok", "as-is")
    measure("sql_snapshot_operations", [r.operation for r in sorted(snaps, key=lambda r: r.committed_at)])


def test_time_travel_placeholders(spark, checks, measure):
    errors = {}
    for label, clause in [("timestamp", "TIMESTAMP AS OF '2023-01-02 12:00:00'"), ("version", "VERSION AS OF 10963874102873")]:
        with pytest.raises(Exception) as err:
            sql(spark, f"SELECT * FROM polaris.db.events {clause}").collect()
        errors[label] = jerr(err.value)
        record(checks, f"SQL: time travel {clause}", "fails", "placeholder value",
               "use a committed_at / snapshot_id from the .snapshots table")
    snaps = sql(spark, "SELECT committed_at, snapshot_id FROM polaris.db.events.snapshots ORDER BY committed_at").collect()
    first = snaps[0]
    by_version = sql(spark, f"SELECT id FROM polaris.db.events VERSION AS OF {first.snapshot_id} ORDER BY id").collect()
    by_time = sql(spark, f"SELECT id FROM polaris.db.events TIMESTAMP AS OF '{first.committed_at}' ORDER BY id").collect()
    assert [r.id for r in by_version] == [r.id for r in by_time] == [1, 2, 3]
    measure("time_travel", {"errors": errors, "first_snapshot_ids": [r.id for r in by_version]})
