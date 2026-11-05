---
name: nessie-authentication-authorization
description: Securing Nessie: OIDC authentication with Keycloak (users and service principals), metadata authorization with CEL rules over references, paths, content types and Iceberg actions (including CATALOG_S3_SIGN), role patterns per institution, protecting main and tags, the AccessChecker SPI, and how Nessie's metadata authorization combines with Apache Ranger data authorization and object-store credentials. Use when writing or reviewing Nessie security configuration.
---

# Authentication and Authorization

## Authentication
`nessie.server.authentication.enabled=true`, `quarkus.oidc.auth-server-url=https://keycloak.platform.svc/realms/platform`, `quarkus.oidc.client-id=nessie`, `quarkus.oidc.credentials.secret=${ENV:NESSIE_OIDC_SECRET}`; bearer tokens from Keycloak for humans (CLI/UI) and client-credentials service principals for engines and the control plane (`svc-trino-t-acme`, `svc-spark-t-acme`, `svc-control-plane`). Roles come from token claims (`roles`), mapped from Keycloak groups `{client}_{institution}_{role}`.

## Authorization model (metadata only)
`nessie.server.authorization.enabled=true`; rules `nessie.server.authorization.rules.<id>=<CEL>`; deny by default — a request passes only if some rule evaluates true. Nessie controls metadata (references, content keys); **data files are protected by Ranger (Trino/Spark) and by object-store credentials/signing**; historical data stays readable via old hashes unless GC removes it.
Variables: `op`, `role`, `roles`, `ref`, `path`, `contentType`, `api` (`apiName` Nessie|Iceberg, `apiVersion`), `actions` (Iceberg REST: `CATALOG_CREATE_ENTITY`, `CATALOG_UPDATE_ENTITY`, `CATALOG_DROP_ENTITY`, `CATALOG_RENAME_ENTITY_*`, `CATALOG_UPDATE_MULTIPLE`, `CATALOG_S3_SIGN`, `META_*` metadata updates, `SNAP_ADD_DATA_FILES` …), `type` (repository config).
Ops: `VIEW_REFERENCE`, `CREATE_REFERENCE`, `ASSIGN_REFERENCE_TO_HASH`, `DELETE_REFERENCE`, `LIST_COMMIT_LOG`, `VIEW_REFLOG`, `READ_ENTRIES`, `READ_CONTENT_KEY`, `READ_ENTITY_VALUE`, `CREATE_ENTITY`, `UPDATE_ENTITY`, `DELETE_ENTITY`, `COMMIT_CHANGE_AGAINST_REFERENCE`, `READ_REPOSITORY_CONFIG`, `UPDATE_REPOSITORY_CONFIG`.

## Platform rule set (pattern)
```properties
# platform admins: everything
nessie.server.authorization.rules.admin=role=='platform_admins'
# control plane: manage references and repository config
nessie.server.authorization.rules.cp=role=='svc-control-plane' && op in ['VIEW_REFERENCE','CREATE_REFERENCE','DELETE_REFERENCE','ASSIGN_REFERENCE_TO_HASH','READ_REPOSITORY_CONFIG','UPDATE_REPOSITORY_CONFIG']
# every institution role may read its own branch and release tags
nessie.server.authorization.rules.inst_read=role.matches('^platform_(t-[a-z0-9]+)_.*') && ref.startsWith(role.replace('platform_','').split('_')[0]) && op in ['VIEW_REFERENCE','LIST_COMMIT_LOG','READ_ENTRIES','READ_CONTENT_KEY','READ_ENTITY_VALUE']
nessie.server.authorization.rules.rel_read=role.startsWith('platform_t-') && ref.startsWith('rel-') && op in ['VIEW_REFERENCE','READ_ENTRIES','READ_ENTITY_VALUE']
# engineers may commit on their institution branch and WAP branches; S3 signing allowed for the institution's own paths
nessie.server.authorization.rules.inst_write=role.matches('^platform_t-[a-z0-9]+_engineer$') && (ref.startsWith(role.substring(9, role.indexOf('_engineer'))) || ref.startsWith('wap-')) && op in ['COMMIT_CHANGE_AGAINST_REFERENCE','CREATE_ENTITY','UPDATE_ENTITY','DELETE_ENTITY']
nessie.server.authorization.rules.sign=role.startsWith('platform_t-') && api.apiName=='Iceberg' && actions.exists(a, a=='CATALOG_S3_SIGN') && path.startsWith(role.substring(9, role.indexOf('_', 9)).replace('-','_'))
# nobody but the promotion job merges to main
nessie.server.authorization.rules.main_merge=role=='svc-promotion' && ref=='main' && op in ['VIEW_REFERENCE','COMMIT_CHANGE_AGAINST_REFERENCE','ASSIGN_REFERENCE_TO_HASH']
```
Generate these from the `Policy`/`Tenant` manifests; test with a two-institution matrix.

## Combining with Ranger
| Concern | Nessie CEL | Ranger |
|---|---|---|
| Which branches/tags a role sees | ✔ | — |
| Create/commit/merge/delete references | ✔ | — |
| Table/namespace create/drop/rename (metadata) | ✔ | ✔ (DDL permissions at the engine) |
| SELECT on columns, row filters, masking | — | ✔ |
| S3 signing / vending scope | ✔ (path) | — (object-store policy) |
Both are compiled from the same source; an engineer blocked in Ranger must also be unable to read the table's metadata location directly — hence S3 signing/vending plus CEL path rules.

## Custom `AccessChecker`
Implement the SPI only if CEL cannot express a need (e.g. lookups in Ranger); keep rules in Git either way.

## Anti-patterns
- `authorization.enabled=false` in shared environments
- A catch-all allow rule for `op=='VIEW_REFERENCE'` on all refs (leaks catalog structure across institutions)
- Granting `ASSIGN_REFERENCE_TO_HASH` on `main` to pipelines (history rewrite)
- Relying on Nessie rules alone for row/column security

Sources: Nessie docs — Authentication, Authorization (model, CEL rules, variables, ops, Iceberg actions incl. CATALOG_S3_SIGN, AccessChecker SPI, stories); Helm chart authentication/authorization values; Apache Ranger Trino/Spark plugin docs.
