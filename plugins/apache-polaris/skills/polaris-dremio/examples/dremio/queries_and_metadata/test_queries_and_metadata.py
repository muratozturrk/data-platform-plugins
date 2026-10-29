"""Querying tables and Iceberg metadata functions from Dremio, the
reference statements on Dremio OSS (source `lake` in place of `PolarisCatalog`)."""
import pytest

from conftest import LAKE, run_sql

ORDERS = f"{LAKE}.sales.orders"
DAYS = ["2023-12-30", "2023-12-31", "2024-01-01", "2024-01-02"]


@pytest.fixture(scope="module")
def orders(dremio, s3):
    """sales.orders as many small commits (one INSERT per batch), like trickle ingestion."""
    dremio.sql(f"CREATE TABLE IF NOT EXISTS {ORDERS} (order_id BIGINT, customer_id INT, order_date DATE, "
               f"total_amount DOUBLE) PARTITION BY (order_date)")
    order_id = 0
    for batch in range(12):
        values = []
        for _ in range(5):
            order_id += 1
            values.append(f"({order_id}, {order_id % 7}, DATE '{DAYS[order_id % 4]}', {order_id * 97.5})")
        dremio.sql(f"INSERT INTO {ORDERS} VALUES {', '.join(values)}")
    s3.put_object(Bucket="landing", Key="lookups/customers.csv",
                  Body="customer_id,segment\n" + "".join(f"{i},{'gold' if i % 2 else 'silver'}\n" for i in range(7)))
    return ORDERS


def test_select_with_fully_qualified_name(dremio, orders, checks, measure):
    rows = run_sql(dremio, checks, "SELECT * FROM PolarisCatalog.sales.customers LIMIT 10",
                    "SELECT * FROM PolarisCatalog.sales.orders LIMIT 10", "source lake, table orders")
    assert len(rows) == 10
    with pytest.raises(Exception) as err:          # no default catalog: a two-part name does not resolve
        dremio.sql("SELECT * FROM sales.orders LIMIT 1")
    measure("unqualified_name_error", str(err.value)[:200])


def test_join_with_a_file_in_another_source(dremio, orders, measure):
    """Federation: join a catalog table with a CSV file in another source."""
    rows = dremio.sql(f"""
        SELECT c.segment, COUNT(*) AS orders
        FROM {ORDERS} o
        JOIN TABLE(landing.landing.lookups."customers.csv"(type => 'text', fieldDelimiter => ',', extractHeader => true)) c
          ON o.customer_id = CAST(c.customer_id AS INT)
        GROUP BY c.segment ORDER BY c.segment""")
    measure("join_with_csv", rows)
    assert sum(r["orders"] for r in rows) == 60


@pytest.mark.parametrize("function", ["table_files", "table_history", "table_snapshot", "table_manifests",
                                      "table_partitions", "clustering_information"])
def test_metadata_functions(dremio, orders, function, checks, measure):
    sql = f"SELECT * FROM TABLE({function}('PolarisCatalog.sales.orders'))"
    try:
        rows = run_sql(dremio, checks, f"TABLE({function}(...))", sql, "source lake")
        measure(f"{function}", {"rows": len(rows), "columns": sorted(rows[0]) if rows else []})
        assert rows
    except Exception as e:                        # noqa: BLE001
        measure(f"{function}", {"error": str(e)[:200]})
        assert function == "clustering_information", e     # only clustering is expected to be missing


def test_time_travel_at_snapshot_and_timestamp(dremio, orders, measure):
    snaps = dremio.sql(f"SELECT snapshot_id, committed_at FROM TABLE(table_snapshot('{ORDERS}')) ORDER BY committed_at")
    create, first_insert = snaps[0], snaps[1]          # CREATE TABLE commits an empty snapshot
    count = lambda clause: dremio.sql(f"SELECT COUNT(*) AS n FROM {ORDERS} {clause}")[0]["n"]
    result = {"snapshots": len(snaps),
              "rows_at_create_snapshot": count(f"AT SNAPSHOT '{create['snapshot_id']}'"),
              "rows_at_first_insert_snapshot": count(f"AT SNAPSHOT '{first_insert['snapshot_id']}'"),
              "rows_at_first_insert_time": count(f"AT TIMESTAMP '{first_insert['committed_at']}'"),
              "rows_now": count("")}
    measure("time_travel", result)
    assert (result["rows_at_create_snapshot"], result["rows_at_first_insert_snapshot"], result["rows_now"]) == (0, 5, 60)
