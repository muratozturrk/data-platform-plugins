#!/usr/bin/env python3
"""Push OpenMetadata classifications to Ranger as tags (ServiceTags import).
Env: OM_URL, OM_TOKEN, RANGER_URL, RANGER_USER, RANGER_PASSWORD, RANGER_SERVICE (e.g. trino-interactive), OM_SERVICE (OpenMetadata database service name)"""
import os, httpx

OM, OMH = os.environ["OM_URL"].rstrip("/"), {"Authorization": f"Bearer {os.environ['OM_TOKEN']}"}
RANGER, AUTH = os.environ["RANGER_URL"].rstrip("/"), (os.environ["RANGER_USER"], os.environ["RANGER_PASSWORD"])
# Infotypes are attributes of PII, never separate tags (D10); RESTRICTED carries level=table|column (D1).
MAP = {"PII.Sensitive": ("PII", {}), "PII.NationalId": ("PII", {"type": "tckn"}), "PII.IBAN": ("PII", {"type": "iban"}),
       "PII.Email": ("PII", {"type": "email"}), "Tier.Restricted": ("RESTRICTED", {}), "Purpose.Fraud": ("PURPOSE_FRAUD", {})}
ATTR_DEFS = {"PII": ["type"], "RESTRICTED": ["level"]}

def tables():
    after = None
    while True:
        r = httpx.get(f"{OM}/api/v1/tables", headers=OMH, params={"service": os.environ["OM_SERVICE"], "fields": "columns,tags", "limit": 100, **({"after": after} if after else {})}, timeout=60)
        r.raise_for_status(); body = r.json(); yield from body["data"]
        after = body.get("paging", {}).get("after")
        if not after: break

def main():
    tag_defs, tags, resources, mapping = {}, {}, [], {}
    def tag_id(name, attrs):
        key = (name, tuple(sorted(attrs.items())))
        if key not in tags:
            tags[key] = len(tags) + 1
            tag_defs[name] = {"name": name, "source": "openmetadata", "attributeDefs": [{"name": a, "type": "string"} for a in ATTR_DEFS.get(name, [])]}
        return tags[key]
    rid = 0
    for t in tables():
        _, catalog, schema, table = t["fullyQualifiedName"].split(".", 3)
        def add(elements, om_tags):
            nonlocal rid
            level = {"level": "column" if "column" in elements else "table"}
            ids = [tag_id(n, {**a, **(level if n == "RESTRICTED" else {})}) for n, a in (MAP[x["tagFQN"]] for x in om_tags if x["tagFQN"] in MAP)]
            if ids:
                rid += 1; resources.append({"id": rid, "serviceName": os.environ["RANGER_SERVICE"], "resourceElements": {k: {"values": [v]} for k, v in elements.items()}})
                mapping[str(rid)] = ids
        add({"catalog": catalog, "schema": schema, "table": table}, t.get("tags", []))
        for c in t.get("columns", []):
            add({"catalog": catalog, "schema": schema, "table": table, "column": c["name"]}, c.get("tags", []))
    doc = {"op": "replace", "serviceName": os.environ["RANGER_SERVICE"], "tagDefinitions": {str(i + 1): d for i, d in enumerate(tag_defs.values())},
           "tags": {str(i): {"type": n, "attributes": dict(a)} for (n, a), i in tags.items()}, "serviceResources": resources, "resourceToTagIds": mapping}
    r = httpx.put(f"{RANGER}/service/tags/importservicetags", auth=AUTH, json=doc, timeout=120); r.raise_for_status()
    print(f"pushed {len(resources)} tagged resources, {len(tags)} tags")

if __name__ == "__main__":
    main()
