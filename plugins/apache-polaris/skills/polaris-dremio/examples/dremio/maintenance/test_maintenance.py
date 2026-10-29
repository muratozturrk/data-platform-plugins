"""Table maintenance and table properties from Dremio, on the
small-file table of queries_and_metadata (12 INSERT commits into 4 daily partitions)."""
import json

import requests

from conftest import LAKE, run_sql

T = f"{LAKE}.sales.orders"


def state(dremio):
    one = lambda sql: dremio.sql(sql)[0]["n"]
    return {"data_files": one(f"SELECT COUNT(*) AS n FROM TABLE(table_files('{T}'))"),
            "snapshots": one(f"SELECT COUNT(*) AS n FROM TABLE(table_snapshot('{T}'))"),
            "manifests": one(f"SELECT COUNT(*) AS n FROM TABLE(table_manifests('{T}'))"),
            "rows": one(f"SELECT COUNT(*) AS n FROM {T}")}


def test_vacuum_examples(dremio, checks, measure):
    """Three documented VACUUM TABLE forms. Dremio OSS refuses VACUUM TABLE on a Nessie source,
    so on this stack they cannot expire anything; the error is the result."""
    out = {}
    for label, sql in [
        ("VACUUM ... older_than '2023-04-20 ...' retain_last 20", """VACUUM TABLE PolarisCatalog.sales.orders
            EXPIRE SNAPSHOTS older_than '2023-04-20 00:00:00.000' retain_last 20"""),
        ("VACUUM ... retain_last 100", "VACUUM TABLE PolarisCatalog.sales.orders EXPIRE SNAPSHOTS retain_last 100"),
        ("VACUUM ... EXPIRE SNAPSHOTS (defaults)", "VACUUM TABLE PolarisCatalog.sales.orders EXPIRE SNAPSHOTS"),
    ]:
        try:
            out[label] = run_sql(dremio, checks, label, sql, "source lake")
        except Exception as e:                    # noqa: BLE001 - recorded in checks
            out[label] = str(e)[:160]
            checks[-1]["fix"] = "not available for Nessie sources in Dremio OSS 26.0.5 (untested on a REST source)"
    measure("vacuum", out)
    assert all("not supported for this source" in str(v) for v in out.values())


def test_optimize_examples(dremio, checks, measure):
    out = {"before": state(dremio)}
    for label, sql in [
        ("OPTIMIZE ... FOR PARTITIONS order_date >= '2024-01-01'", """OPTIMIZE TABLE PolarisCatalog.sales.orders
            REWRITE DATA USING BIN_PACK
            FOR PARTITIONS order_date >= '2024-01-01'"""),
        ("OPTIMIZE ... BIN_PACK (TARGET 256, MIN 100, MAX 1000, MIN_INPUT_FILES 10)", """OPTIMIZE TABLE PolarisCatalog.sales.orders
            REWRITE DATA USING BIN_PACK (
                TARGET_FILE_SIZE_MB = 256,
                MIN_FILE_SIZE_MB = 100,
                MAX_FILE_SIZE_MB = 1000,
                MIN_INPUT_FILES = 10
            )"""),
        ("OPTIMIZE TABLE (defaults)", "OPTIMIZE TABLE PolarisCatalog.sales.orders"),
        ("OPTIMIZE ... REWRITE MANIFESTS", "OPTIMIZE TABLE PolarisCatalog.sales.orders REWRITE MANIFESTS"),
    ]:
        result = run_sql(dremio, checks, label, sql, "source lake")
        out[label] = {"result": result, "after": state(dremio)}
    measure("optimize", out)
    final = out["OPTIMIZE ... REWRITE MANIFESTS"]["after"]
    assert final["rows"] == out["before"]["rows"]
    assert final["data_files"] == 4 and final["manifests"] == 1        # one file per daily partition


def test_show_tblproperties(dremio, s3, checks, measure):
    """Documented sample output lists properties such as commit.retry.num-retries."""
    table = f"{LAKE}.sales.high_value_orders"
    rows = run_sql(dremio, checks, "SHOW TBLPROPERTIES PolarisCatalog.sales.high_value_orders",
                    "SHOW TBLPROPERTIES PolarisCatalog.sales.high_value_orders", "source lake")
    dremio.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('commit.retry.num-retries' = '3')")
    after = dremio.sql(f"SHOW TBLPROPERTIES {table}")
    content = requests.get("http://nessie:19120/api/v2/trees/main/contents/sales.high_value_orders", timeout=30).json()
    location = content["content"]["metadataLocation"]
    bucket, key = location.removeprefix("s3://").split("/", 1)
    in_metadata = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read()).get("properties", {})
    measure("tblproperties", {"after_ctas": rows, "after_alter": after, "iceberg_metadata_properties": in_metadata})
    if not rows:
        checks[-1].update(status="0 rows", verdict="no rows although the metadata holds 6 properties",
                             fix="read the properties from the table metadata (e.g. PyIceberg / Spark SHOW TBLPROPERTIES)")
    # Observed on Dremio OSS 26.0.5 with a Nessie source: the statement runs and returns nothing,
    # while ALTER TABLE ... SET TBLPROPERTIES did write the property into the Iceberg metadata.
    assert rows == [] and after == [] and in_metadata.get("commit.retry.num-retries") == "3"
