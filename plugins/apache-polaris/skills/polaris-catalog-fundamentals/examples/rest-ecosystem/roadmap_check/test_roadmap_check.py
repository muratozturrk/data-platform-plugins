"""Polaris roadmap items as announced in mid-2025, checked against the Polaris 1.8.0 release.
Each test records whether the item exists and whether it behaves as announced."""
import json

import pytest
import requests

from conftest import POLARIS_EXT

ICEBERG_API = "http://polaris:8181/api/catalog/v1/lake"


def claim(rows, item, verdict, evidence):
    rows.append({"item": item, "verdict": verdict, "evidence": evidence})


def test_generic_tables_lifecycle(lake, h, claims, measure):
    """CreateGenericTable, LoadGenericTable, ListGenericTables, DropGenericTable on /generic-tables."""
    gt = f"{POLARIS_EXT}/namespaces/eco/generic-tables"
    created = requests.post(gt, headers=h, timeout=30, json={
        "name": "delta_events", "format": "delta", "base-location": "s3://warehouse/lake/eco/delta_events",
        "doc": "Delta table registered in Polaris", "properties": {"owner": "analytics"}})
    loaded = requests.get(f"{gt}/delta_events", headers=h, timeout=30)
    listed = requests.get(gt, headers=h, timeout=30).json()["identifiers"]
    iceberg_list = requests.get(f"{ICEBERG_API}/namespaces/eco/tables", headers=h, timeout=30).json()["identifiers"]
    as_iceberg = requests.get(f"{ICEBERG_API}/namespaces/eco/tables/delta_events", headers=h, timeout=30)
    clash = requests.post(gt, headers=h, timeout=30, json={"name": "orders", "format": "delta",
                                                          "base-location": "s3://warehouse/lake/eco/orders_delta"})
    dropped = requests.delete(f"{gt}/delta_events", headers=h, timeout=30)
    after = requests.get(f"{gt}/delta_events", headers=h, timeout=30)
    result = {"create": created.status_code, "load_format": loaded.json()["table"]["format"],
              "list": [i["name"] for i in listed], "in_iceberg_list": "delta_events" in [i["name"] for i in iceberg_list],
              "load_via_iceberg_api": as_iceberg.status_code, "same_name_as_iceberg_table": clash.status_code,
              "drop": dropped.status_code, "load_after_drop": after.status_code}
    measure("11.2_generic_tables", result)
    assert result["create"] == 200 and result["load_format"] == "delta" and result["list"] == ["delta_events"]
    assert not result["in_iceberg_list"] and result["load_via_iceberg_api"] == 404
    assert result["same_name_as_iceberg_table"] == 409 and result["drop"] == 204 and result["load_after_drop"] == 404
    claim(claims, "Generic tables: create / load / list / drop on /generic-tables", "Shipped (1.8.0)",
          "test_generic_tables_lifecycle")
    claim(claims, "Separate endpoints from Iceberg; name unique across entity types", "Holds",
          "not in Iceberg listTables; Iceberg loadTable 404; same name 409")


@pytest.mark.parametrize("policy_type", ["system.compaction", "system.snapshot_retention", "system.row_access_policy",
                                         "custom.myorg.data_masking", "system.data-compaction", "system.snapshot-expiry",
                                         "system.metadata-compaction", "system.orphan-file-removal"])
def test_policy_types(lake, h, policy_type, measure):
    r = requests.post(f"{POLARIS_EXT}/namespaces/eco/policies", headers=h, timeout=30, json={
        "name": "p_" + policy_type.replace(".", "_").replace("-", "_"), "type": policy_type,
        "content": json.dumps({"enable": True})})
    measure(f"11.2_policy_type[{policy_type}]", r.status_code)
    assert r.status_code == (200 if policy_type in ("system.data-compaction", "system.snapshot-expiry",
                                                    "system.metadata-compaction", "system.orphan-file-removal") else 400)


def test_policy_inheritance_override_and_versions(lake, h, claims, measure):
    p = f"{POLARIS_EXT}/namespaces/eco/policies"

    def create(name, ptype, enable):
        return requests.post(p, headers=h, timeout=30, json={"name": name, "type": ptype, "content": json.dumps({"enable": enable})})

    def attach(name, target):
        return requests.put(f"{p}/{name}/mappings", headers=h, timeout=30, json={"target": target}).status_code

    create("ns_compaction", "system.data-compaction", True)
    create("ns_retention", "system.snapshot-expiry", True)
    create("orders_compaction", "system.data-compaction", False)
    attaches = [attach("ns_compaction", {"type": "namespace", "path": ["eco"]}),
                attach("ns_retention", {"type": "namespace", "path": ["eco"]}),
                attach("orders_compaction", {"type": "table-like", "path": ["eco", "orders"]})]
    second_same_type = attach("p_system_data_compaction", {"type": "table-like", "path": ["eco", "orders"]})
    applicable = requests.get(f"{POLARIS_EXT}/applicable-policies", headers=h, timeout=30,
                              params={"namespace": "eco", "target-name": "orders"}).json()["applicable-policies"]
    effective = {a["policy-type"]: (a["name"], a["inherited"]) for a in applicable}
    invalid = create("bad", "system.data-compaction", True)
    bad_content = requests.post(p, headers=h, timeout=30, json={"name": "bad2", "type": "system.data-compaction", "content": "not json"})
    upd = requests.put(f"{p}/ns_retention", headers=h, timeout=30, json={
        "current-policy-version": 0, "content": json.dumps({"enable": True, "config": {"max_snapshot_age_days": 7}})})
    stale = requests.put(f"{p}/ns_retention", headers=h, timeout=30, json={"current-policy-version": 0, "content": json.dumps({"enable": False})})
    result = {"attach": attaches, "second_policy_same_type_on_table": second_same_type, "effective_for_orders": effective,
              "invalid_content": bad_content.status_code, "update_version": upd.json()["policy"]["version"],
              "update_with_stale_version": stale.status_code}
    measure("11.2_policies", result)
    assert attaches == [204, 204, 204] and second_same_type == 409
    assert effective == {"system.data-compaction": ("orders_compaction", False), "system.snapshot-expiry": ("ns_retention", True)}
    assert bad_content.status_code == 400 and result["update_version"] == 1 and stale.status_code == 409
    del invalid
    claim(claims, "Policy store: CRUD, attach, inheritance with table-level override", "Shipped (1.8.0)",
          "test_policy_inheritance_override_and_versions")
    claim(claims, "Policy types system.compaction, system.snapshot_retention, system.row_access_policy; custom types",
          "Does not hold: 4 other system types, custom types rejected", "test_policy_types")
    claim(claims, "Policy versioning", "Partly: version counter with optimistic concurrency; rollback not observed",
          "update -> version 1; stale version 409")


def test_items_that_are_not_in_1_8(lake, h, claims, measure):
    s3_compatible = requests.post("http://polaris:8181/api/management/v1/catalogs", headers=h, timeout=30, json={"catalog": {
        "name": "my-s3compatible-catalog", "type": "INTERNAL", "properties": {"default-base-location": "s3://other/x"},
        "storageConfigInfo": {"storageType": "S3_COMPATIBLE", "region": "eu-central-1", "s3.endpoint": "https://localhost:9000",
                              "s3.stsEndpoint": "https://localhost:9000", "s3.pathStyleAccess": True}}})
    volume = requests.post(f"{ICEBERG_API}/namespaces/eco/volume/", headers=h, timeout=30, json={"name": "images"})
    result = {"S3_COMPATIBLE catalog (roadmap JSON)": s3_compatible.status_code, "create volume": volume.status_code}
    measure("11.2_not_shipped", {**result, "s3_compatible_error": s3_compatible.text[:200]})
    assert s3_compatible.status_code == 400 and volume.status_code == 404
    claim(claims, "S3_COMPATIBLE storage type", "Not shipped as designed; S3 with endpoint / stsEndpoint / pathStyleAccess covers it",
          "S3_COMPATIBLE 400; S3 + SeaweedFS STS used by every lab")
    claim(claims, "Volumes for unstructured data (/volume)", "Not in 1.8.0", "POST .../volume/ 404")
