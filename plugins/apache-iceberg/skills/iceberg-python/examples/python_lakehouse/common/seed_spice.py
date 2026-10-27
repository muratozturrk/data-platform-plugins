"""Create the tables the Spicepod points at before Spice starts (docker compose `seed` service).

Spice resolves an Iceberg dataset at startup and retries with a growing backoff if the table is
missing, so creating the tables first keeps start-up deterministic.
"""
from pyiceberg.exceptions import NamespaceAlreadyExistsError

from common.lakehouse import get_catalog
from common.trips import TRIPS_SCHEMA, create_trips_table

NS = "spice_demo"

if __name__ == "__main__":
    catalog = get_catalog()
    try:
        catalog.create_namespace(NS)
    except NamespaceAlreadyExistsError:
        pass
    if not catalog.table_exists(f"{NS}.trips"):
        create_trips_table(catalog, f"{NS}.trips", rows=0)
    if not catalog.table_exists(f"{NS}.trips_rw"):
        catalog.create_table(f"{NS}.trips_rw", schema=TRIPS_SCHEMA)
    print("spice tables ready")
