"""Gradual migration from another catalog to Polaris.

Phase 1  federate: the legacy catalog is reachable through Polaris; nothing moves.
Phase 2  cut over: each table is registered in an internal Polaris catalog from its current
         metadata.json (zero data copied); the federation is removed.
Phase 3  writes go to Polaris; the legacy catalog must be frozen, or history splits.
"""
import uuid

import boto3
import pyarrow as pa
import pytest

from common.polaris import ROOT, S3_ENDPOINT, CatalogClient
from common.remotes import REFERENCE_URI, federate, reference, unfederate

LOCATION = "s3://warehouse/rest/"


def ids(*values):
    return pa.table({"id": pa.array(list(values), pa.int64())})


def object_count(prefix: str) -> int:
    s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT, aws_access_key_id="admin",
                      aws_secret_access_key="password", region_name="us-east-1")
    bucket, key = prefix.removeprefix("s3://").split("/", 1)
    return sum(len(p.get("Contents", [])) for p in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key))


@pytest.fixture(scope="module")
def legacy():
    ns = f"legacy_{uuid.uuid4().hex[:6]}"
    cat = reference()
    cat.create_namespace(ns)
    for t, rows in (("customers", (1, 2, 3)), ("orders", (10, 11, 12, 13))):
        cat.create_table(f"{ns}.{t}", schema=ids(1).schema).append(ids(*rows))
    return {"ns": ns, "catalog": cat}


def test_gradual_migration_with_zero_copy_cutover(admin, legacy, root, measure):
    ns, legacy_cat = legacy["ns"], legacy["catalog"]
    fed, target = f"fed_legacy_{uuid.uuid4().hex[:4]}", f"lakehouse_{uuid.uuid4().hex[:4]}"

    # Phase 1: users already go through Polaris while the legacy catalog stays the owner.
    assert federate(admin, fed, REFERENCE_URI, "default", LOCATION).status_code == 201
    via_polaris = {t: root.pyiceberg(fed).load_table(f"{ns}.{t}").scan().to_arrow().num_rows
                   for t in ("customers", "orders")}
    assert via_polaris == {"customers": 3, "orders": 4}

    # Phase 2: cut over. Pointers are taken from the legacy catalog, then the federation goes away
    # (Polaris forbids two catalogs over the same location).
    pointers = {t: legacy_cat.load_table(f"{ns}.{t}").metadata_location for t in ("customers", "orders")}
    objects_before = object_count(f"{LOCATION}{ns}/")
    assert unfederate(admin, fed) == 204
    admin.create_catalog(target, f"{LOCATION}{ns}", allowed=[LOCATION],
                         properties={"polaris.config.allow.unstructured.table.location": "true"})
    admin.grant(target, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
    rc = CatalogClient(target, *ROOT, vended=False)
    rc.create_namespace(ns)
    for t, loc in pointers.items():
        rc.c("POST", f"/namespaces/{ns}/register", json={"name": t, "metadata-location": loc})
    try:
        engine = root.pyiceberg(target)
        after = {t: engine.load_table(f"{ns}.{t}").scan().to_arrow().num_rows for t in pointers}
        assert after == via_polaris
        objects_after_register = object_count(f"{LOCATION}{ns}/")
        assert objects_after_register == objects_before                    # nothing copied

        # Phase 3: new writes go to Polaris. The legacy catalog still points at the old metadata.
        engine.load_table(f"{ns}.orders").append(ids(14))
        assert engine.load_table(f"{ns}.orders").scan().to_arrow().num_rows == 5
        assert legacy_cat.load_table(f"{ns}.orders").scan().to_arrow().num_rows == 4   # stale

        # A writer that was not switched off keeps committing to the legacy catalog: split history.
        legacy_cat.load_table(f"{ns}.orders").append(ids(99))
        polaris_ids = set(engine.load_table(f"{ns}.orders").scan().to_arrow()["id"].to_pylist())
        legacy_ids = set(legacy_cat.load_table(f"{ns}.orders").scan().to_arrow()["id"].to_pylist())
        assert 14 in polaris_ids and 99 not in polaris_ids
        assert 99 in legacy_ids and 14 not in legacy_ids
        measure("migration", {"rows_via_federation": via_polaris, "rows_after_register": after,
                                  "objects_before_cutover": objects_before,
                                  "objects_after_register": objects_after_register,
                                  "split_brain": {"polaris_only": [14], "legacy_only": [99]}})
    finally:
        # Drop only the Polaris entries (purge=False): the files belong to the legacy catalog too.
        for t in pointers:
            rc.drop_table(ns, t, purge=False)
        rc.drop_namespace(ns)
        admin.m("DELETE", f"/catalogs/{target}")
