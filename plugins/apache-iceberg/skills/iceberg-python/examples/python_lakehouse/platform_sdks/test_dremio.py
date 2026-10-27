"""Dremio: OSS server, Arrow Flight from Python, Iceberg DDL/DML, interop with PyIceberg."""
import time

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from pyiceberg.table import StaticTable

from common.lakehouse import S3_PROPS
from common.trips import create_trips_table, make_trips
from platform_sdks.dremio_client import (FlightSql, ensure_s3_source, login, promote_as_iceberg,
                                                 publish_hadoop_pointer, s3fs, wait_until_up)


# Unique names per run: Dremio caches metadata by path, so a table re-created at the same
# location would be served from the stale cache on reruns.
RUN = int(time.time())


@pytest.fixture(scope="module")
def dremio():
    wait_until_up()
    headers = login()
    ensure_s3_source(headers)
    return headers


@pytest.fixture(scope="module")
def sql(dremio):
    return FlightSql()


@pytest.fixture(scope="module")
def trips(catalog, ns, dremio):
    name = f"trips_{RUN}"
    t = create_trips_table(catalog, f"{ns}.{name}")
    publish_hadoop_pointer(t)
    promote_as_iceberg(dremio, ["lake", "warehouse", ns, name])
    t.dremio_path = f"lake.warehouse.{ns}.{name}"
    return t


def test_rest_catalog_source_is_not_in_oss(dremio):
    import requests
    from platform_sdks.dremio_client import DREMIO
    r = requests.post(f"{DREMIO}/api/v3/catalog", headers=dremio, timeout=30, json={
        "entityType": "source", "name": "rest", "type": "RESTCATALOG", "config": {}})
    assert r.status_code == 400 and "RESTCATALOG" in r.text


def test_rest_table_is_invisible_without_a_hadoop_pointer(catalog, ns, dremio):
    import requests
    t = create_trips_table(catalog, f"{ns}.no_pointer_{RUN}", rows=10)
    with pytest.raises(requests.HTTPError, match="400"):
        promote_as_iceberg(dremio, ["lake", "warehouse", ns, f"no_pointer_{RUN}"])
    assert t.metadata_location.endswith(".metadata.json") and "/v" not in t.metadata_location.rsplit("/", 1)[1]


def test_flight_query(sql, ns, trips):
    got = sql.query(f"SELECT trip_distance, total_amount FROM {trips.dremio_path} "
                    f"WHERE total_amount > 100")
    expected = trips.scan(row_filter="total_amount > 100").to_arrow()
    assert got.num_rows == expected.num_rows
    assert abs(pc.sum(got["total_amount"]).as_py() - pc.sum(expected["total_amount"]).as_py()) < 1e-6
    assert isinstance(got, pa.Table)                    # Flight returns Arrow, no row conversion


def test_ctas_and_insert_produce_iceberg_that_pyiceberg_reads(catalog, sql, ns, trips):
    out = f"lake.warehouse.{ns}_dremio_{RUN}.daily_summary"
    sql.query(f"""CREATE TABLE {out} AS
                  SELECT passenger_count, AVG(total_amount) AS avg_total
                  FROM {trips.dremio_path} GROUP BY passenger_count""")
    sql.query(f"INSERT INTO {out} VALUES (5, 45.12), (6, 32.75)")
    # Dremio writes a Hadoop-layout table: v<N>.metadata.json + version-hint.text.
    meta_dir = f"warehouse/{ns}_dremio_{RUN}/daily_summary/metadata"
    with s3fs().open_input_stream(f"{meta_dir}/version-hint.text") as f:
        version = int(f.read())
    assert version == 2                                  # CTAS, INSERT
    static = StaticTable.from_metadata(f"s3://{meta_dir}/v{version}.metadata.json", properties=S3_PROPS)
    rows = dict(zip(*static.scan().to_arrow().select(["passenger_count", "avg_total"]).to_pydict().values()))
    assert rows[5] == 45.12 and rows[6] == 32.75 and set(rows) == {1, 2, 3, 4, 5, 6}
    # Registering the file in the REST catalog makes it a first-class table for every engine.
    ident = f"{ns}.dremio_summary"
    if catalog.table_exists(ident):
        catalog.drop_table(ident)
    registered = catalog.register_table(ident, static.metadata_location)
    assert registered.scan().to_arrow().num_rows == 6


def test_decimal_literal_ctas_silently_zeroes_wider_values(sql, ns, trips):
    out = f"lake.warehouse.{ns}_dremio_{RUN}.typed_by_literals"
    sql.query(f"CREATE TABLE {out} AS SELECT * FROM (VALUES (1, 2.5)) AS t(k, v)")   # v: DECIMAL(2,1)
    sql.query(f"INSERT INTO {out} VALUES (2, 32.75)")
    got = {r["k"]: float(r["v"]) for r in sql.query(f"SELECT k, v FROM {out}").to_pylist()}
    assert got == {1: 2.5, 2: 0.0}                       # no error, the value is lost


def test_file_system_source_is_stale_until_the_pointer_moves(dremio, sql, ns, trips):
    table = trips.dremio_path
    count = lambda: sql.query(f"SELECT count(*) AS n FROM {table}").to_pylist()[0]["n"]
    assert count() == 5000
    trips.append(make_trips(10, first_id=90_001, seed=9))          # commit through the REST catalog
    sql.query(f"ALTER TABLE {table} REFRESH METADATA")
    assert count() == 5000                                          # Dremio follows version-hint.text
    publish_hadoop_pointer(trips)
    sql.query(f"ALTER TABLE {table} REFRESH METADATA")
    assert count() == 5010
