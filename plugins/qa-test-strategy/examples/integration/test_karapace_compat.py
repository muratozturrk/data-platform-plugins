"""T-sr-001: incompatible schema rejected at the gate (BACKWARD compatibility)."""
import json, pytest, httpx

V1 = {"type": "record", "name": "Order", "fields": [{"name": "order_id", "type": "string"}, {"name": "amount", "type": "double"}]}
V2_BAD = {"type": "record", "name": "Order", "fields": [{"name": "order_id", "type": "string"}, {"name": "amount", "type": "double"}, {"name": "currency", "type": "string"}]}   # no default → breaks BACKWARD
V2_OK = {"type": "record", "name": "Order", "fields": [{"name": "order_id", "type": "string"}, {"name": "amount", "type": "double"}, {"name": "currency", "type": "string", "default": "TRY"}]}

@pytest.mark.integration
def test_backward_compatibility_enforced(karapace, unique):
    subject = f"t.acme.sales.order.created.v1-value-{unique}"
    httpx.put(f"{karapace}/config/{subject}", json={"compatibility": "BACKWARD"}).raise_for_status()
    httpx.post(f"{karapace}/subjects/{subject}/versions", json={"schema": json.dumps(V1)}).raise_for_status()
    bad = httpx.post(f"{karapace}/subjects/{subject}/versions", json={"schema": json.dumps(V2_BAD)})
    assert bad.status_code == 409
    ok = httpx.post(f"{karapace}/subjects/{subject}/versions", json={"schema": json.dumps(V2_OK)})
    assert ok.status_code == 200 and ok.json()["id"]
