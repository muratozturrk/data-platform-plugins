"""Creating tables, CTAS and COPY INTO from Dremio. The reference statements on Dremio OSS
(source names replaced), then the CTAS
result handed to Polaris with registerTable and read by another engine through Polaris."""
import json

import pytest
import requests

from conftest import LAKE, POLARIS_CATALOG, run_sql

FILES = {
    "path/to/files/part1.csv": "id,name\n1,alpha\n2,beta\n",
    "path/to/files/part2.csv": "id,name\n3,gamma\n",
    "logs/2024-01/a.json": '{"event_id": 1, "url": "/home"}\n{"event_id": 2, "url": "/cart"}\n',
    "logs/2024-01/b.json": '{"event_id": 3, "url": "/pay"}\n',
    "orders/2023/orders_2023-11-30.csv": "order_id,amount\n1,10.0\n",
    "orders/2023/orders_2023-12-01.csv": "order_id,amount\n2,20.0\n",
    "orders/2023/orders_2023-12-02.csv": "order_id,amount\n3,30.0\n",
    "hr_data/new_hires.csv": "emp_id|name|dept\n1|Ann|NA\n2|Bob|Sales\nthree|Cid|Ops\n",
    "sales/jan2024/sales.csv": "zip,city,ignored,price,sale_date\n10115,Berlin,x,12.5,2024-01-03\n80331,Munich,y,7.25,2024-01-04\n",
}
TARGETS = [
    f"CREATE TABLE IF NOT EXISTS {LAKE}.sales.my_table (id INT, name VARCHAR)",
    f"CREATE TABLE IF NOT EXISTS {LAKE}.logs.web_events (event_id BIGINT, url VARCHAR)",
    f"CREATE TABLE IF NOT EXISTS {LAKE}.sales.daily_orders (order_id INT, amount DOUBLE)",
    f"CREATE TABLE IF NOT EXISTS {LAKE}.hr.employees (emp_id INT, name VARCHAR, dept VARCHAR)",
    f"CREATE TABLE IF NOT EXISTS {LAKE}.sales.transactions (city VARCHAR, zip VARCHAR, sale_date VARCHAR, price DOUBLE)",
]


@pytest.fixture(scope="module")
def landing(dremio, s3):
    for key, body in FILES.items():
        s3.put_object(Bucket="landing", Key=key, Body=body.encode())
    for sql in TARGETS:
        dremio.sql(sql)
    return dremio


def count(dremio, table):
    return dremio.sql(f"SELECT COUNT(*) AS n FROM {LAKE}.{table}")[0]["n"]


def test_create_empty_table(dremio, s3, checks, measure):
    run_sql(dremio, checks, "CREATE TABLE PolarisCatalog.sales.new_orders (...)", """
        CREATE TABLE PolarisCatalog.sales.new_orders (
            order_id BIGINT,
            customer_id INT,
            order_date DATE,
            total_amount DOUBLE
        )""", "source lake")
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket="warehouse", Prefix="nessie/sales/new_orders").get("Contents", [])]
    measure("empty_table_objects", keys)
    assert any(k.endswith(".metadata.json") for k in keys) and not any("/data/" in k for k in keys)


def test_ctas(dremio, checks, measure):
    """Needs sales.orders from queries_and_metadata (60 orders, amounts 97.5 .. 5850)."""
    run_sql(dremio, checks, "CTAS PolarisCatalog.sales.high_value_orders", """
        CREATE TABLE PolarisCatalog.sales.high_value_orders AS
        SELECT customer_id, COUNT(*) as order_count, MAX(total_amount) as max_order
        FROM PolarisCatalog.sales.orders
        WHERE total_amount > 1000
        GROUP BY customer_id""", "source lake")
    n = count(dremio, "sales.high_value_orders")
    measure("ctas_rows", n)
    assert n == 7


def test_ctas_table_is_readable_through_polaris(dremio, admin, root, measure):
    """After CTAS, any engine connected to the same Polaris catalog should see the new table. Here Dremio's catalog is Nessie, so the table is registered in Polaris by its
    metadata location, then read with PyIceberg through Polaris (vended credentials)."""
    content = requests.get("http://nessie:19120/api/v2/trees/main/contents/sales.high_value_orders", timeout=30).json()
    location = content["content"]["metadataLocation"]
    c = root.catalog(POLARIS_CATALOG)
    if "dremio" not in c.list_namespaces():
        c.create_namespace("dremio")
    r = c.c("POST", "/namespaces/dremio/register", json={"name": "high_value_orders", "metadata-location": location})
    rows = root.pyiceberg(POLARIS_CATALOG).load_table("dremio.high_value_orders").scan().to_arrow()
    measure("polaris_register", {"status": r.status_code, "metadata_location": location.rsplit("/", 1)[-1],
                                     "rows_via_polaris": rows.num_rows, "columns": rows.column_names})
    assert rows.num_rows == count(dremio, "sales.high_value_orders")


COPY_EXAMPLES = [
    ("COPY INTO ... FROM '@MySource/path/to/files/' FILE_FORMAT 'csv'", "sales.my_table", """
        COPY INTO PolarisCatalog.sales.my_table
        FROM '@MySource/path/to/files/'
        FILE_FORMAT 'csv'"""),
    ("COPY INTO ... FROM '@CloudSource/logs/2024-01/' FILE_FORMAT 'json'", "logs.web_events", """
        COPY INTO PolarisCatalog.logs.web_events
        FROM '@CloudSource/logs/2024-01/'
        FILE_FORMAT 'json'"""),
    ("COPY INTO ... REGEX '.*_2023-12-.*\\\\.csv'", "sales.daily_orders", r"""
        COPY INTO PolarisCatalog.sales.daily_orders
        FROM '@S3Data/orders/2023/'
        REGEX '.*_2023-12-.*\\.csv'
        FILE_FORMAT 'csv'"""),
    ("COPY INTO ... FILES (...) (FILE_FORMAT 'csv', FIELD_DELIMITER '|', ON_ERROR 'continue', NULL_IF ('NA'))", "hr.employees", """
        COPY INTO PolarisCatalog.hr.employees
        FROM '@S3Bucket/hr_data/'
        FILES ('new_hires.csv')
        (FILE_FORMAT 'csv', FIELD_DELIMITER '|', ON_ERROR 'continue', NULL_IF ('NA'))"""),
    ("COPY INTO ... (city, zip, sale_date, price) FROM (SELECT ... t.$2 ...)", "sales.transactions", """
        COPY INTO PolarisCatalog.sales.transactions(city, zip, sale_date, price)
        FROM (
          SELECT SUBSTR(t.$2, 1, 20), t.$1, t.$5, CAST(t.$4 AS DOUBLE)
          FROM '@S3Bucket/sales/jan2024/sales.csv' t
        )
        FILE_FORMAT 'csv'"""),
]


FIXES = {
    "sales.daily_orders": r"SQL strings do not escape backslashes: REGEX '.*_2023-12-.*\.csv' (one backslash)",
    "hr.employees": "options follow the format: FILE_FORMAT 'csv' (FIELD_DELIMITER '|', ON_ERROR 'continue', NULL_IF ('NA'))",
    "sales.transactions": "not supported by Dremio OSS 26.0.5; load into a staging table, then INSERT ... SELECT",
}


@pytest.mark.parametrize("example,table,sql", COPY_EXAMPLES, ids=[t for _, t, _ in COPY_EXAMPLES])
def test_copy_into_examples(landing, example, table, sql, checks, measure):
    before = count(landing, table)
    try:
        result = run_sql(landing, checks, example, sql, "source names")
        outcome = {"status": "ok", "result": result, "rows_added": count(landing, table) - before}
        if outcome["rows_added"] == 0:            # ran, but loaded nothing
            checks[-1].update(status="0 rows", verdict="matches no file", fix=FIXES[table])
    except Exception as e:                        # noqa: BLE001 - recorded in checks
        outcome = {"status": "fails", "error": str(e)[:300]}
        checks[-1]["fix"] = FIXES.get(table, "")
    measure(f"copy[{table}]", outcome)
    assert outcome.get("rows_added", 0) > 0 or table in FIXES


def test_copy_into_examples_fixed(landing, measure):
    """Corrected forms for the examples that fail as printed (see measurements)."""
    fixes = {
        "sales.daily_orders": rf"""COPY INTO {LAKE}.sales.daily_orders FROM '@landing/landing/orders/2023/'
                                    REGEX '.*_2023-12-.*\.csv' FILE_FORMAT 'csv'""",
        "hr.employees": f"""COPY INTO {LAKE}.hr.employees FROM '@landing/landing/hr_data/' FILES ('new_hires.csv')
                             FILE_FORMAT 'csv' (FIELD_DELIMITER '|', ON_ERROR 'continue', NULL_IF ('NA'))""",
    }
    out = {}
    for table, sql in fixes.items():
        before = count(landing, table)
        try:
            out[table] = {"result": landing.sql(sql), "rows_added": count(landing, table) - before}
        except Exception as e:                    # noqa: BLE001
            out[table] = {"error": str(e)[:300]}
    measure("copy_fixed", out)
    assert out["sales.daily_orders"].get("rows_added") == 2
    assert out["hr.employees"].get("rows_added") == 2          # 'three' is not an INT: skipped by ON_ERROR 'continue'


def test_copy_errors_history(landing, checks, measure):
    try:
        hist = run_sql(landing, checks, "SELECT * FROM sys.copy_errors_history WHERE table_name = ...",
                        "SELECT * FROM sys.copy_errors_history WHERE table_name = 'hr.employees'", "table name")
        if not hist:
            checks[-1].update(status="0 rows", verdict="table_name is source-qualified", fix="WHERE table_name = 'lake.hr.employees'")
    except Exception:                             # noqa: BLE001
        hist = []
    all_rows = landing.sql("SELECT * FROM sys.copy_errors_history")
    measure("copy_errors_history", {"rows_with_unqualified_filter": len(hist), "all_rows": len(all_rows),
                                        "columns": sorted(all_rows[0]) if all_rows else [],
                                        "table_names": sorted({str(r.get("table_name")) for r in all_rows})})
    assert all_rows
    job_id = all_rows[-1]["job_id"]
    errors = landing.sql(f"SELECT * FROM TABLE(copy_errors('{LAKE}.hr.employees', '{job_id}'))")
    measure("copy_errors_function", errors)
    assert errors and "three" in json.dumps(errors)


def test_registration_needs_unstructured_locations(dremio, admin, root, measure):
    """The same registration with the catalog's default location rules (flag switched off for the
    test; a second catalog cannot share s3://warehouse/nessie because catalog locations may not overlap)."""
    from common.polaris import ApiError

    def set_flag(value):
        cat = admin.m("GET", f"/catalogs/{POLARIS_CATALOG}").json()
        props = {**cat["properties"], "polaris.config.allow.unstructured.table.location": value}
        admin.m("PUT", f"/catalogs/{POLARIS_CATALOG}", json={"currentEntityVersion": cat["entityVersion"], "properties": props})

    location = requests.get("http://nessie:19120/api/v2/trees/main/contents/sales.high_value_orders",
                            timeout=30).json()["content"]["metadataLocation"]
    c = root.catalog(POLARIS_CATALOG)
    set_flag("false")
    try:
        with pytest.raises(ApiError) as custom_ns:
            c.create_namespace("dremio_custom", {"location": "s3://warehouse/nessie/sales"})
        with pytest.raises(ApiError) as register:
            c.c("POST", "/namespaces/dremio/register", json={"name": "hvo_strict", "metadata-location": location})
    finally:
        set_flag("true")
    measure("strict_locations", {"custom_namespace_location": custom_ns.value.status,
                                     "register": register.value.status, "message": register.value.text[:120]})
    assert (custom_ns.value.status, register.value.status) == (400, 403)
