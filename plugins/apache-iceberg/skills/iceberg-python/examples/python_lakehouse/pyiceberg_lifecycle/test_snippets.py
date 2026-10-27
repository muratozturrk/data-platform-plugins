"""Widely published PyIceberg snippets, run verbatim where possible (local SQL catalog).

Each test runs the snippet as published; where it fails with the pinned versions, the test asserts
the exact failure and then runs the smallest fix.
"""
from importlib.metadata import metadata

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from pyarrow import parquet as pq

from common.trips import make_trips


@pytest.fixture
def workdir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    trips = make_trips(1000, days=5)
    pq.write_table(trips, "trips.parquet")
    pq.write_table(make_trips(10, first_id=5001).append_column(
        "pickup_borough", pa.array(["Bronx"] * 10, pa.large_string())), "updated_trips.parquet")
    return tmp_path


def _sql_catalog(tmp_path):
    from pyiceberg.catalog import load_catalog
    return load_catalog("default", type="sql", uri="sqlite:///demo.db",
                        warehouse=f"file://{tmp_path}/warehouse")


def test_install_extra_sql_does_not_exist():
    # Published snippet: pip install pyiceberg[sql] pyarrow. 0.9.1 has no "sql" extra: pip only warns and
    # SQLAlchemy is not installed. The extra for a SQLite catalog is sql-sqlite.
    extras = metadata("pyiceberg").get_all("Provides-Extra")
    assert "sql" not in extras
    assert {"sql-sqlite", "sql-postgres", "pyarrow"} <= set(extras)


def test_create_append_scan_runs_as_is(workdir):
    catalog = _sql_catalog(workdir)                   # only the warehouse path is local
    catalog.create_namespace("default")
    table = catalog.create_table("default.taxi_data", schema=pq.read_schema("trips.parquet"))
    arrow_table = pq.read_table("trips.parquet")
    table.append(arrow_table)
    result = table.scan().to_arrow()
    assert len(result) == 1000


def test_schema_evolution_needs_a_type_object(workdir):
    from pyiceberg.types import StringType
    catalog = _sql_catalog(workdir)
    catalog.create_namespace("default")
    table = catalog.create_table("default.taxi_data", schema=pq.read_schema("trips.parquet"))
    table.append(pq.read_table("trips.parquet"))
    update = table.update_schema()
    with pytest.raises(NotImplementedError, match="non-type: string"):
        update.add_column("pickup_borough", "string").commit()
    # Fix: pass an Iceberg type object (a context manager commits on exit).
    with table.update_schema() as update:
        update.add_column("pickup_borough", StringType())
    new_data = pq.read_table("updated_trips.parquet")
    table.append(new_data)
    rows = table.scan().to_arrow()
    assert len(rows) == 1010
    assert pc.sum(pc.is_null(rows["pickup_borough"])).as_py() == 1000


def test_overwrite_snippet_fails_and_fix(workdir):
    from pyiceberg.expressions import EqualTo
    catalog = _sql_catalog(workdir)
    catalog.create_namespace("default")
    table = catalog.create_table("default.taxi_data", schema=pq.read_schema("trips.parquet"))
    table.append(pq.read_table("trips.parquet"))
    with pytest.raises(TypeError, match="where"):
        overwrite = table.overwrite(where=EqualTo("pickup_date", "2023-01-01"))
        overwrite.add_file("new_data.parquet").commit()
    # Fix: overwrite() takes the data plus overwrite_filter and commits in one call.
    new_data = make_trips(3, days=1, first_id=7001)          # all on 2023-01-01
    table.overwrite(new_data, overwrite_filter=EqualTo("pickup_date", "2023-01-01"))
    rows = table.scan().to_arrow()
    day1 = rows.filter(pc.equal(rows["pickup_date"], pa.scalar(new_data["pickup_date"][0])))
    assert sorted(day1["trip_id"].to_pylist()) == [7001, 7002, 7003]
    assert len(rows) == 1000 - 200 + 3


def test_filtered_scan_runs_as_is(workdir):
    from pyiceberg.expressions import GreaterThan
    catalog = _sql_catalog(workdir)
    catalog.create_namespace("default")
    table = catalog.create_table("default.taxi_data", schema=pq.read_schema("trips.parquet"))
    table.append(pq.read_table("trips.parquet"))
    expensive_trips = table.scan(row_filter=GreaterThan("total_amount", 100)).to_arrow()
    head = expensive_trips.to_pandas().head()
    assert len(head) == 5 and (head.total_amount > 100).all()


def test_sales_lifecycle_needs_the_same_overwrite_fix(workdir):
    from pyiceberg.expressions import EqualTo, GreaterThan
    sales = pa.table({"order_id": pa.array(range(1, 9), pa.int64()),
                      "region": ["US", "EU"] * 4,
                      "amount": [500.0, 1500.0, 2500.0, 50.0, 1200.0, 30.0, 999.0, 4000.0]})
    pq.write_table(sales, "sales_data.parquet")
    catalog = _sql_catalog(workdir)
    catalog.create_namespace("default")
    table = catalog.create_table("default.sales", schema=pq.read_schema("sales_data.parquet"))
    data = pq.read_table("sales_data.parquet")
    table.append(data)
    with pytest.raises(TypeError, match="where"):
        table.overwrite(where=EqualTo("region", "US"))
    us_fix = pa.table({"order_id": pa.array([100], pa.int64()), "region": ["US"], "amount": [7000.0]})
    table.overwrite(us_fix, overwrite_filter=EqualTo("region", "US"))
    results = table.scan(row_filter=GreaterThan("amount", 1000)).to_arrow()
    assert sorted(results["order_id"].to_pylist()) == [2, 8, 100]
    # Unpartitioned table: the filter cuts through files, so they are rewritten ("overwrite"),
    # not dropped whole ("delete") as in the partition-aligned case in test_pyiceberg.py.
    assert [s.summary.operation.value for s in table.metadata.snapshots] == ["append", "overwrite", "append"]
