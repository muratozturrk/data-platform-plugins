"""The REST catalog ecosystem: one Polaris table, several clients: PyIceberg, Trino
(Iceberg REST connector with vended credentials), Polars and DuckDB (on PyIceberg's scan)."""
import duckdb
import polars as pl

from conftest import trino

EXPECTED = (100, 5625.0)          # rows, sum(amount) of eco.orders


def test_trino_reads_the_polaris_table(lake, measure):
    rows = trino("SELECT count(*), sum(amount) FROM lake.eco.orders")
    measure("11.1_trino_read", rows)
    assert tuple(rows[0]) == EXPECTED


def test_trino_writes_and_pyiceberg_reads(lake, measure):
    trino("CREATE TABLE lake.eco.big_orders AS SELECT order_id, amount FROM lake.eco.orders WHERE amount > 50")
    t = lake.load_table("eco.big_orders")
    arrow = t.scan().to_arrow()
    measure("11.1_trino_ctas", {"rows_via_pyiceberg": arrow.num_rows, "location": t.location().split("/")[-1][:40]})
    assert arrow.num_rows == 50                     # amount in 62.5 .. 112.5


def test_polars_and_duckdb_on_the_same_table(lake, measure):
    table = lake.load_table("eco.orders")
    by_polars = pl.scan_iceberg(table).select(pl.len(), pl.col("amount").sum()).collect().row(0)
    arrow = table.scan().to_arrow()
    by_duckdb = duckdb.sql("SELECT count(*), sum(amount) FROM arrow").fetchone()
    measure("11.1_polars_duckdb", {"polars": by_polars, "duckdb": by_duckdb})
    assert tuple(by_polars) == EXPECTED and tuple(by_duckdb) == EXPECTED


def test_commit_from_one_engine_is_visible_to_the_others(lake, measure):
    trino("INSERT INTO lake.eco.orders VALUES (100, 1000.0)")
    after_pyiceberg = lake.load_table("eco.orders").scan().to_arrow().num_rows
    scan = pl.scan_iceberg(lake.load_table("eco.orders"))
    after_polars = scan.collect().height
    polars_len = scan.select(pl.len()).collect().item()      # observed: 0 with polars 1.31.0 once Trino added a file
    snapshots = len(lake.load_table("eco.orders").snapshots())
    measure("11.1_cross_engine_commit", {"pyiceberg_rows": after_pyiceberg, "polars_rows": after_polars,
                                         "polars_select_len": polars_len, "snapshots": snapshots})
    assert after_pyiceberg == after_polars == 101 and snapshots == 2
