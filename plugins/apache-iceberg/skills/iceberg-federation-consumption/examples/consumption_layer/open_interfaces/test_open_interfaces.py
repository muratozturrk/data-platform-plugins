"""The same governed data through three open interfaces: SQL endpoint (DB-API), Arrow Flight, MCP."""
import asyncio
import json
import time

import pyarrow as pa
import pyarrow.flight as flight
import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from common.lakehouse import trino_connect, trino_query
from common.sales import load_sales
from common.semantic import compile_query, drop_semantic_schema, publish_governed_view
from open_interfaces.flight_service import IcebergFlightServer, fetch, start
from open_interfaces.mcp_service import build_server

ROWS = 300_000
TOKENS = {"tok-admin": "admin", "tok-m02": "m02"}
COLUMNS = ["order_id", "merchant_id", "quantity", "unit_price", "order_ts"]


@pytest.fixture(scope="module")
def table(spark, ns):
    t = f"{ns}.sales"
    load_sales(spark, t, ROWS)
    return t


@pytest.fixture(scope="module")
def semantic(ns, table):
    schema = f"{ns}_semantic"
    publish_governed_view(table, schema)
    yield schema
    drop_semantic_schema(schema)


@pytest.fixture(scope="module")
def flight_port(catalog):
    server = IcebergFlightServer(catalog, TOKENS)
    start(server)
    yield server.port
    server.shutdown()


def dbapi_rows(sql: str, user="admin") -> tuple[list[tuple], list[tuple]]:
    with trino_connect(user) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        return [tuple(r) for r in cur.fetchall()], cur.description


def call_tool(server, name: str, args: dict):
    async def run():
        async with create_connected_server_and_client_session(server._mcp_server) as client:
            return await client.list_tools(), await client.call_tool(name, args)
    return asyncio.run(run())


def test_sql_endpoint_and_flight_return_identical_rows(table, flight_port):
    where_sql = "order_ts >= TIMESTAMP '2025-03-05 00:00:00 UTC' AND order_ts < TIMESTAMP '2025-03-06 00:00:00 UTC'"
    rows, _ = dbapi_rows(f"SELECT {', '.join(COLUMNS)} FROM {table} WHERE {where_sql} ORDER BY order_id")
    schema, arrow = fetch(flight_port, "tok-admin", table, COLUMNS,
                          where="order_ts >= '2025-03-05T00:00:00+00:00' and order_ts < '2025-03-06T00:00:00+00:00'")
    flight_rows = [tuple(r.values()) for r in arrow.sort_by("order_id").to_pylist()]
    assert len(rows) == 10_000 and flight_rows == rows
    assert schema.names == COLUMNS                         # schema is known before any data moves


def test_flight_keeps_columnar_types_and_moves_bulk_data_faster(table, flight_port):
    start_ = time.perf_counter()
    rows, description = dbapi_rows(f"SELECT {', '.join(COLUMNS)} FROM {table}")
    dbapi_s = time.perf_counter() - start_
    start_ = time.perf_counter()
    _, arrow = fetch(flight_port, "tok-admin", table, COLUMNS)
    flight_s = time.perf_counter() - start_
    print(f"{ROWS} rows x {len(COLUMNS)} cols: DB-API {dbapi_s:.2f} s ({ROWS / dbapi_s:,.0f} rows/s), "
          f"Flight {flight_s:.2f} s ({ROWS / flight_s:,.0f} rows/s), speedup {dbapi_s / flight_s:.1f}x")
    assert len(rows) == arrow.num_rows == ROWS
    assert isinstance(rows[0], tuple)                                  # row-oriented Python objects
    assert [d[1] for d in description][:3] == ["bigint", "varchar", "integer"]
    assert arrow.schema.field("quantity").type == pa.int32()           # typed columnar buffers
    assert pa.types.is_timestamp(arrow.schema.field("order_ts").type) and arrow.schema.field("order_ts").type.tz
    assert flight_s < dbapi_s


def test_flight_authenticates_and_enforces_the_tenant_filter(table, flight_port):
    with pytest.raises(flight.FlightUnauthenticatedError):
        fetch(flight_port, "wrong-token", table, COLUMNS)
    _, mine = fetch(flight_port, "tok-m02", table, ["merchant_id", "quantity"])
    expected = trino_query(f"SELECT count(*) FROM {table} WHERE merchant_id = 'm02'")[0][0]
    assert set(mine.column("merchant_id").to_pylist()) == {"m02"} and mine.num_rows == expected


def test_mcp_server_exposes_governed_tools_not_raw_sql(table, semantic):
    tools, result = call_tool(build_server("analyst", semantic), "query_metrics",
                              {"metrics": ["net_revenue", "completed_orders"], "dimensions": ["region"]})
    assert {t.name for t in tools.tools} == {"list_datasets", "describe_metrics", "query_metrics"}
    assert all("sql" not in json.dumps(t.inputSchema).lower() for t in tools.tools)
    rows = json.loads(result.content[0].text)["rows"]
    direct = trino_query(compile_query(["net_revenue", "completed_orders"], ["region"], table))
    assert [(r["region"], r["completed_orders"]) for r in rows] == [(d[0], d[2]) for d in direct]
    assert all(abs(r["net_revenue"] - d[1]) < 1e-6 * d[1] for r, d in zip(rows, direct))


def test_mcp_calls_run_with_the_callers_permissions(semantic):
    _, result = call_tool(build_server("m02", semantic), "query_metrics",
                          {"metrics": ["completed_orders"], "dimensions": ["merchant_id"]})
    assert [r["merchant_id"] for r in json.loads(result.content[0].text)["rows"]] == ["m02"]

    _, datasets = call_tool(build_server("m02", semantic), "list_datasets", {})
    assert json.loads(datasets.content[0].text)["datasets"] == ["orders_governed"]

    _, bad = call_tool(build_server("analyst", semantic), "query_metrics",
                       {"metrics": ["net_revenue"], "dimensions": ["customer_email"]})
    assert bad.isError and "not in the semantic model" in bad.content[0].text
