"""What an Iceberg catalog really stores, and how a commit swaps it.

Uses PyIceberg's SqlCatalog, which keeps the same `iceberg_tables` layout as Iceberg's Java
JdbcCatalog (the REST reference server in this stack runs a JdbcCatalog behind its API).
"""
from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass

import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException


@dataclass
class CatalogRow:
    catalog_name: str
    table_namespace: str
    table_name: str
    metadata_location: str
    previous_metadata_location: str | None


def catalog_columns(db_path: str) -> list[str]:
    with sqlite3.connect(db_path) as con:
        return [r[1] for r in con.execute("PRAGMA table_info(iceberg_tables)")]


def catalog_row(db_path: str, namespace: str, table: str) -> CatalogRow:
    """The complete catalog entry of one table: a name and two file pointers."""
    with sqlite3.connect(db_path) as con:
        row = con.execute(
            "SELECT catalog_name, table_namespace, table_name, metadata_location, "
            "previous_metadata_location FROM iceberg_tables "
            "WHERE table_namespace = ? AND table_name = ?", (namespace, table)).fetchone()
    return CatalogRow(*row)


@dataclass
class Resolution:
    """The path an engine walks from a table name to the data files."""
    metadata_location: str
    snapshot_id: int | None
    manifest_list: str | None
    manifests: list[str]
    data_files: list[str]


def resolve(catalog, identifier: str) -> Resolution:
    table = catalog.load_table(identifier)            # 1. name -> metadata.json (the catalog's job)
    snap = table.current_snapshot()                   # 2. metadata.json -> current snapshot
    if snap is None:
        return Resolution(table.metadata_location, None, None, [], [])
    manifests = snap.manifests(table.io)              # 3. snapshot -> manifest list -> manifests
    files = [e.data_file.file_path                    # 4. manifests -> data files
             for m in manifests for e in m.fetch_manifest_entry(table.io)]
    return Resolution(table.metadata_location, snap.snapshot_id, snap.manifest_list,
                      [m.manifest_path for m in manifests], files)


@dataclass
class AppendResult:
    attempts: int
    conflicts: list[str]


def append_with_retry(catalog, identifier: str, rows: pa.Table, stale_table=None,
                      max_attempts: int = 5) -> AppendResult:
    """Optimistic concurrency the way engines do it: build the commit on the version you read,
    let the catalog compare-and-swap, and on conflict reload and rebuild.

    stale_table: a Table object loaded earlier (simulates a writer that read before someone else
    committed). The first attempt uses it, later attempts always reload from the catalog.
    """
    conflicts = []
    table = stale_table
    for attempt in range(1, max_attempts + 1):
        if table is None:
            table = catalog.load_table(identifier)
        try:
            table.append(rows)
            return AppendResult(attempt, conflicts)
        except CommitFailedException as e:
            conflicts.append(str(e))
            table = None                                # discard the stale view, re-read
            time.sleep(0.05 * attempt)
    raise RuntimeError(f"gave up after {max_attempts} attempts: {conflicts}")
