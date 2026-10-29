# Gradual Migration into Polaris

## Concept

Moving tables from another catalog into Polaris without a big-bang switch. Federation decouples
two changes that are risky to do together:

1. **User-facing switch**: every engine points at Polaris (one URI, one identity, Polaris RBAC),
   while the legacy catalog still owns the tables (federated `EXTERNAL` catalog).
2. **Ownership switch**: each table is registered in an internal Polaris catalog from its current
   `metadata.json`. No data or metadata files are copied; only the pointer moves.

The same path applies to any Iceberg REST source, including Nessie "seamless migration"
scenarios (Nessie federation itself needs a release with an Iceberg REST endpoint, see
[external-catalog-products.md](external-catalog-products.md)). `register_table` and the catalog
pointer model in general: `iceberg-catalogs` skill of the apache-iceberg plugin.

## Phases

| Phase | Action | Risk | Mitigation |
|---|---|---|---|
| 1 Federate | Polaris `EXTERNAL` catalog -> legacy catalog; repoint engines | Remote outages surface in Polaris | Monitor the remote |
| 2 Cut over | Freeze writers, read each table's current `metadata-location`, delete the federation, create the internal catalog, register tables | Commits during the switch | Freeze writers for the cutover window |
| 3 Run on Polaris | Writes go to Polaris | A forgotten legacy writer keeps committing | Make the legacy catalog read-only or shut it down |

The federation must be deleted before the internal catalog is created when both would cover the
same storage prefix: Polaris rejects overlapping catalog locations (400).

## API Reference

| Step | Call |
|---|---|
| Read pointer (legacy) | Iceberg `loadTable` -> `metadata-location` (PyIceberg: `table.metadata_location`) |
| Delete federation | `DELETE /api/management/v1/catalogs/{fed}` (entity only) |
| Internal catalog | `POST /api/management/v1/catalogs` with `type: INTERNAL`, `allowedLocations` covering the old table prefixes |
| Allow foreign table layout | Catalog property `polaris.config.allow.unstructured.table.location: "true"` |
| Register | `POST /api/catalog/v1/{prefix}/namespaces/{ns}/register` with `{"name": t, "metadata-location": loc}` |
| Drop Polaris entry only | `DELETE .../tables/{t}?purgeRequested=false` |

`polaris.config.allow.unstructured.table.location=true` is needed when registered tables live
outside the namespace locations Polaris would generate itself (the normal case after a migration).

## Worked Example: Zero-Copy Cutover

```python
# Phase 1: users go through Polaris; legacy still owns the tables
federate(admin, fed, REFERENCE_URI, "default", "s3://warehouse/rest/")
rows_via_fed = {t: root.pyiceberg(fed).load_table(f"{ns}.{t}").scan().to_arrow().num_rows
                for t in ("customers", "orders")}

# Phase 2: take pointers right before the cutover, then switch ownership
pointers = {t: legacy_cat.load_table(f"{ns}.{t}").metadata_location for t in ("customers", "orders")}
assert unfederate(admin, fed) == 204
admin.create_catalog(target, f"s3://warehouse/rest/{ns}", allowed=["s3://warehouse/rest/"],
                     properties={"polaris.config.allow.unstructured.table.location": "true"})
admin.grant(target, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
rc = CatalogClient(target, *ROOT, vended=False)
rc.create_namespace(ns)
for t, loc in pointers.items():
    rc.c("POST", f"/namespaces/{ns}/register", json={"name": t, "metadata-location": loc})
```

Measured: `customers` 3 rows and `orders` 4 rows both through the federation and after
registration; 10 storage objects under the table prefix before and after registration (nothing
copied).

## Split History: Why the Legacy Writer Must Stop

After the cutover the suite appends id 14 through Polaris and, as a forgotten writer would,
id 99 directly to the legacy catalog. Result: Polaris has 14 but not 99, the legacy catalog has
99 but not 14. Both catalogs now point to different `metadata.json` files that share an
ancestor; neither sees the other's commit. Re-pointing engines is not enough; the old catalog
must stop accepting writes.

## Cleanup Rules

- Drop Polaris entries with `purge=false` while the files are still shared with the legacy catalog;
  `purge=true` would delete data the legacy catalog still references.
- The examples' `drop_catalog()` helper empties a catalog with purge; never use it on a federated
  catalog (it would drop remote tables) or on registered tables whose files are shared.

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| 400 overlap when creating the target catalog | Federation still exists on the same prefix | Delete the federation entity first |
| Register fails on location checks | Table paths do not follow Polaris' namespace layout | `polaris.config.allow.unstructured.table.location=true` on the target catalog |
| Registered table misses the last commits | Pointer read too early | Freeze writers, read pointers right before registering |
| Two diverging histories | Legacy writer still active | Make the legacy catalog read-only or shut it down |
| Data loss after cleanup | Dropped with purge while files are shared | `purgeRequested=false` |

## Related

- [federation-mechanics.md](federation-mechanics.md): the `EXTERNAL` catalog used in phase 1.
- `polaris-deployment` skill: catalog storage configuration (`allowedLocations`, STS).
- `iceberg-catalogs` skill (apache-iceberg plugin): `register_table` across catalogs in general.
