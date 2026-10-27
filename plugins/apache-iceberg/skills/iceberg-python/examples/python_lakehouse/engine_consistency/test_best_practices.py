from datetime import date

import pytest
from pyiceberg.expressions import EqualTo

from common.seed_spice import NS as SPICE_NS
from common.trips import make_trips
from duckdb_engine.duckdb_ops import attach_rest_catalog, connect
from platform_sdks import spice_client
from engine_consistency.capabilities import (OPERATIONS, probe_daft, probe_duckdb, probe_polars,
                                                 probe_pyiceberg)
from engine_consistency.consistency import (same, via_daft, via_duckdb_arrow, via_duckdb_catalog,
                                                via_duckdb_iceberg_scan, via_pyiceberg, via_polars, via_spice)


@pytest.fixture(scope="module")
def con():
    c = connect()
    attach_rest_catalog(c)
    yield c
    c.close()


@pytest.fixture(scope="module")
def shared(catalog):
    """The table Spice also serves, so all six engines can read the same snapshots."""
    t = catalog.load_table(f"{SPICE_NS}.trips")
    t.overwrite(make_trips(5000, seed=21))
    v1 = t.current_snapshot().snapshot_id
    t.overwrite(make_trips(300, days=1, first_id=80_001, seed=22, start=date(2023, 1, 3)),
                overwrite_filter=EqualTo("pickup_date", "2023-01-03"))
    spice_client.wait_ready({"trips"})
    return t, v1, t.current_snapshot().snapshot_id


def test_every_engine_returns_the_same_kpis_for_the_same_snapshot(shared, con):
    t, v1, v2 = shared
    ident = f"{SPICE_NS}.trips"
    for snap in (v1, v2):
        reference = via_pyiceberg(t, snap)
        results = {
            "polars": via_polars(t, snap),
            "duckdb_arrow": via_duckdb_arrow(t, snap),
            "duckdb_iceberg_scan": via_duckdb_iceberg_scan(con, t, snap),
            "duckdb_catalog": via_duckdb_catalog(con, ident, snap),
            "daft": via_daft(t, snap),
        }
        assert all(same(reference, r) for r in results.values()), results
    assert sum(r[1] for r in via_pyiceberg(t, v1)) == 5000
    assert sum(r[1] for r in via_pyiceberg(t, v2)) == 5000 - 1000 + 300
    assert not same(via_pyiceberg(t, v1), via_pyiceberg(t, v2))


def test_spice_matches_at_the_current_snapshot(shared):
    t, _, v2 = shared
    reference = via_pyiceberg(t, v2)
    for _ in range(20):                                # results cache TTL is 1 s
        if same(reference, via_spice(spice_client.query)):
            break
        __import__("time").sleep(0.5)
    assert same(reference, via_spice(spice_client.query))


@pytest.fixture(scope="module")
def eq_table(catalog):
    """A table with an equality delete, written by Spice (the only writer here that makes one)."""
    rw = catalog.load_table(f"{SPICE_NS}.trips_rw")
    rw.overwrite(make_trips(10, days=1, first_id=1))
    spice_client.wait_ready({"trips_rw"})
    spice_client.query("DELETE FROM trips_rw WHERE trip_id = 1")
    rw.refresh()
    assert int(rw.current_snapshot().summary["added-equality-deletes"]) == 1
    return rw


def test_capability_matrix(catalog, ns, con, eq_table):
    matrix = {
        "pyiceberg": probe_pyiceberg(catalog, ns, eq_table),
        "polars": probe_polars(catalog, ns, eq_table),
        "duckdb": probe_duckdb(catalog, ns, eq_table, con),
        "daft": probe_daft(catalog, ns, eq_table),
    }
    print()
    print("| Library | " + " | ".join(OPERATIONS) + " |")
    for lib, caps in matrix.items():
        print(f"| {lib} | " + " | ".join("yes" if caps[o] else "no" for o in OPERATIONS) + " |")
    assert matrix == {
        "pyiceberg": dict(time_travel_read=True, append=True, partition_overwrite=True, row_delete=True,
                          schema_evolution=True, read_equality_deletes=False),
        "polars": dict(time_travel_read=True, append=True, partition_overwrite=False, row_delete=False,
                       schema_evolution=False, read_equality_deletes=False),
        "duckdb": dict(time_travel_read=True, append=True, partition_overwrite=True, row_delete=True,
                       schema_evolution=True, read_equality_deletes=True),
        "daft": dict(time_travel_read=True, append=True, partition_overwrite=True, row_delete=False,
                     schema_evolution=False, read_equality_deletes=False),
    }
