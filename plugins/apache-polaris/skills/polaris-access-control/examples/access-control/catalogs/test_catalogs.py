"""Catalogs as isolated governance units.

Two business-unit catalogs (marketing, finance) on different storage prefixes/buckets, each with
its own roles and namespaces. Checks isolation of metadata, of grants and of storage access.
"""
import pytest

from common.polaris import ApiError, CatalogClient, split_uri, vended_s3
from conftest import seed_table


@pytest.fixture(scope="module")
def units(admin, root):
    import uuid
    u = uuid.uuid4().hex[:6]
    mkt, fin = f"marketing_{u}", f"finance_{u}"
    admin.create_catalog(mkt, f"s3://warehouse/{mkt}")
    admin.create_catalog(fin, f"s3://finance-bucket/{fin}")      # separate bucket
    for cat in (mkt, fin):
        admin.grant(cat, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
        seed_table(root, cat, "campaigns" if cat == mkt else "ledger", "events")
    yield mkt, fin
    for cat in (mkt, fin):
        admin.drop_catalog(cat)


def test_each_catalog_is_its_own_namespace_tree(units, root, measure):
    mkt, fin = units
    m, f = CatalogClient(mkt, root.client_id, root.client_secret), CatalogClient(fin, root.client_id, root.client_secret)
    assert m.list_namespaces() == ["campaigns"] and f.list_namespaces() == ["ledger"]
    # Same table name in both catalogs: two unrelated tables with their own metadata locations.
    seed_table(root, fin, "campaigns", "events")
    loc_m = m.load_table("campaigns", "events")["metadata-location"]
    loc_f = f.load_table("campaigns", "events")["metadata-location"]
    assert split_uri(loc_m)[0] == "warehouse" and split_uri(loc_f)[0] == "finance-bucket"
    measure("metadata_locations", {"marketing": loc_m.split("/metadata/")[0], "finance": loc_f.split("/metadata/")[0]})


def test_every_catalog_gets_its_own_catalog_admin(units, admin):
    mkt, fin = units
    # Polaris creates a catalog_admin role per catalog and binds it to service_admin.
    assert admin.catalog_roles(mkt) == ["catalog_admin"] and admin.catalog_roles(fin) == ["catalog_admin"]
    assert admin.catalog_roles_of("service_admin", mkt) == ["catalog_admin"]


def test_new_catalog_admin_manages_access_and_metadata_but_not_data(admin, root, uid, measure):
    """The built-in catalog_admin starts with CATALOG_MANAGE_ACCESS + CATALOG_MANAGE_METADATA only.
    Root can create namespaces and metadata-only tables, but not a table with vended credentials
    (what every engine requests) until CATALOG_MANAGE_CONTENT is granted."""
    cat = f"fresh_{uid}"
    admin.create_catalog(cat, f"s3://warehouse/{cat}")
    try:
        before = sorted(g["privilege"] for g in admin.grants(cat, "catalog_admin"))
        assert before == ["CATALOG_MANAGE_ACCESS", "CATALOG_MANAGE_METADATA"]
        plain = CatalogClient(cat, root.client_id, root.client_secret, vended=False)
        engine_like = CatalogClient(cat, root.client_id, root.client_secret, vended=True)
        plain.create_namespace("first")
        plain.create_table("first", "metadata_only")                 # no storage credentials asked
        with pytest.raises(ApiError) as e:
            engine_like.create_table("first", "with_credentials")     # credentials = data privilege
        assert e.value.status == 403
        admin.grant(cat, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
        engine_like.create_table("first", "with_credentials")
        measure("catalog_admin_default_privileges", before)
    finally:
        admin.drop_catalog(cat)


def test_grants_do_not_cross_catalog_boundaries(units, admin, uid, measure):
    mkt, fin = units
    p = admin.create_principal(f"mkt_analyst_{uid}")
    admin.create_principal_role(f"mkt_analyst_{uid}")
    admin.assign_principal_role(p.name, f"mkt_analyst_{uid}")
    admin.create_catalog_role(mkt, "mkt_reader")
    for priv in ("NAMESPACE_LIST", "TABLE_LIST", "TABLE_READ_PROPERTIES", "TABLE_READ_DATA"):
        admin.grant(mkt, "mkt_reader", {"type": "catalog", "privilege": priv})
    admin.assign_catalog_role(f"mkt_analyst_{uid}", mkt, "mkt_reader")
    try:
        assert p.pyiceberg(mkt).load_table("campaigns.events").scan().to_arrow().num_rows == 3
        fin_view = p.catalog(fin)
        with pytest.raises(ApiError) as e:
            fin_view.list_namespaces()
        assert e.value.status == 403
        with pytest.raises(ApiError) as e2:
            fin_view.load_table("ledger", "events")
        assert e2.value.status == 403
        measure("cross_catalog_denial", e2.value.text[:160])
    finally:
        admin.drop_principal(p.name)
        admin.drop_principal_role(f"mkt_analyst_{uid}")
        admin.drop_catalog_role(mkt, "mkt_reader")


def test_vended_credentials_never_reach_another_catalogs_storage(units, root):
    mkt, fin = units
    m = CatalogClient(mkt, root.client_id, root.client_secret)
    f = CatalogClient(fin, root.client_id, root.client_secret)
    s3_mkt = vended_s3(m.load_table("campaigns", "events"))
    fin_loc = f.load_table("ledger", "events")["metadata-location"]
    bucket, key = split_uri(fin_loc)
    from botocore.exceptions import ClientError
    with pytest.raises(ClientError) as e:
        s3_mkt.get_object(Bucket=bucket, Key=key)
    assert e.value.response["Error"]["Code"] in ("AccessDenied", "403")


def test_storage_locations_are_enforced(units, root, admin, uid, measure):
    mkt, _ = units
    m = CatalogClient(mkt, root.client_id, root.client_secret)
    # A table location outside the catalog's allowedLocations is rejected, even for root.
    with pytest.raises(ApiError) as e:
        m.create_table("campaigns", f"escape_{uid}", location="s3://finance-bucket/stolen")
    assert e.value.status in (400, 403)
    # A catalog whose base location is outside its own allowed locations cannot be created.
    with pytest.raises(ApiError) as e2:
        admin.create_catalog(f"bad_{uid}", "s3://warehouse/elsewhere", allowed=["s3://warehouse/only_here"])
    assert e2.value.status == 400
    measure("location_errors", {"table": f"{e.value.status} {e.value.text[:140]}",
                                    "catalog": f"{e2.value.status} {e2.value.text[:140]}"})
