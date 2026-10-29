---
name: polaris-access-control
description: "Apache Polaris access control as measured on Polaris 1.8.0 - catalogs as isolated governance units (allowedLocations, per-catalog catalog_admin), principals and their credential lifecycle (secret shown once, OAuth2 client credentials, admin reset vs self rotate, revocation), catalog roles and the 25 documented privileges with the measured privilege x operation matrix (data vs metadata, vended read vs read+write credentials, namespace inheritance), principal roles and token scopes (PRINCIPAL_ROLE:ALL vs one role), and practices: least privilege, namespace grants, naming, roles defined in YAML and applied idempotently, automated access reviews. Use when designing Polaris RBAC, granting a team or service access, debugging 401/403 from Polaris or AccessDenied on vended keys, rotating or revoking credentials, or auditing who can reach sensitive catalogs."
---

# Polaris Access Control

Polaris evaluates every request for one **principal** through three layers: the principal holds
**principal roles** (global job functions), principal roles are granted **catalog roles** (local
to one catalog), and catalog roles hold **privileges** on a catalog, namespace, table or view.
Data access is separate from metadata access: `TABLE_READ_DATA` / `TABLE_WRITE_DATA` are what make
Polaris vend short-lived, table-scoped storage credentials. Everything below was measured against
Polaris 1.8.0 with real reads and writes through vended keys. The raw endpoints behind these
operations are in the `polaris-rest-api` skill; credential vending itself is explained in
`polaris-catalog-fundamentals`; generic catalog RBAC concepts are in the `iceberg-catalogs` skill
of the apache-iceberg plugin.

## When to Use This Skill

- Designing catalogs, catalog roles and principal roles for teams, services and sensitive data.
- Giving an analyst, a pipeline or a BI tool exactly the access it needs.
- Debugging `401`, `403`, `Invalid locations`, `not authorized for op ...` or `AccessDenied` on vended keys.
- Onboarding, rotating, resetting or retiring principals.
- Running a job with less than its owner's access (token scope).
- Keeping RBAC in code, detecting drift and reviewing access automatically.

## Decision Table

| Need | Technique / API | Reference |
|---|---|---|
| Separate teams, regions or sensitivity levels | One catalog each, own bucket/prefix in `allowedLocations` | [access_model.md](references/access_model.md) |
| Why a new catalog's admin cannot create tables | `catalog_admin` lacks `CATALOG_MANAGE_CONTENT` by default | [access_model.md](references/access_model.md) |
| New service identity | One principal per job; store the secret at creation | [principals.md](references/principals.md) |
| Leaked secret vs planned rotation | Admin `reset` vs self `rotate` | [principals.md](references/principals.md) |
| Read every table in a namespace, incl. future ones | `TABLE_READ_DATA` on the namespace | [privileges_reference.md](references/privileges_reference.md) |
| Manage structure without seeing rows | `TABLE_FULL_METADATA` / `CATALOG_MANAGE_METADATA` | [privileges_reference.md](references/privileges_reference.md) |
| One person, several duties | Several principal roles; token scope picks one | [access_model.md](references/access_model.md) |
| Roles in Git, drift reverted | YAML model + plan/apply | [roles_as_code_and_review.md](references/roles_as_code_and_review.md) |
| Periodic access review | 7-rule audit + roster diff | [roles_as_code_and_review.md](references/roles_as_code_and_review.md) |

## Catalogs as Governance Units

A catalog has its own namespace tree, its own catalog roles and its own storage configuration.
Nothing granted in one catalog applies in another, and vended keys for a table in one catalog get
`AccessDenied` on another catalog's files.

```json
{"catalog": {"name": "finance", "type": "INTERNAL",
  "properties": {"default-base-location": "s3://finance-bucket/finance"},
  "storageConfigInfo": {"storageType": "S3", "allowedLocations": ["s3://finance-bucket/finance"],
                        "roleArn": "arn:aws:iam::<account>:role/<vendor-role>"}}}
```

Gotchas (measured):
- Every new catalog gets a `catalog_admin` catalog role bound to `service_admin`, holding only
  `CATALOG_MANAGE_ACCESS` + `CATALOG_MANAGE_METADATA`. Creating a table with vended credentials
  (what engines request) is 403 until you grant `CATALOG_MANAGE_CONTENT`.
- A table location outside `allowedLocations` is 403 `Invalid locations`, even for root; a catalog
  whose base location is outside its own allowed list is 400.

## Principals

One identity per user or service; OAuth2 client credentials give a JWT (`sub` = principal name,
lifetime 3600 s). The secret is returned once at creation and never listed again.

| Situation | Action | Effect (measured) |
|---|---|---|
| Secret leaked / person left | Admin `POST /principals/{name}/reset` | Old secret 401 at once |
| Planned rotation | The principal itself calls `/rotate` | Old secret still valid after 1 rotation, 401 after 2 |
| Retire access | Revoke principal role, then delete principal | Same live token: 403 after revoke, 401 after delete |

Access is evaluated per request, not per token: revocation does not wait for token expiry.
`service_admin` cannot rotate another principal's secret (403 `not authorized for op ROTATE_CREDENTIALS`).

## Catalog Roles and Privileges

25 documented privileges in four families (table, view, namespace, catalog). Key measured facts:

- `TABLE_READ_DATA` vends read-only keys; `TABLE_WRITE_DATA` vends read+write and implies read.
- `TABLE_FULL_METADATA` = all table metadata operations, no data.
- `CATALOG_MANAGE_CONTENT` = all content including data, but **no** grant management;
  `CATALOG_MANAGE_ACCESS` = grant management only, no content.
- `NAMESPACE_LIST` lists namespaces only; listing tables needs `TABLE_LIST`, views `VIEW_LIST`.
- A namespace grant covers current, future and nested tables; a table grant reaches one table.
- Grants from several catalog roles add up; a revoke takes effect on the next request.

```python
admin.grant("sales_cat", "sales_reader",
            {"type": "namespace", "namespace": ["sales"], "privilege": "TABLE_READ_DATA"})
```

## Principal Roles and Token Scope

Principal roles are global and bind catalog roles from several catalogs. The token's `scope`
decides what is active: `PRINCIPAL_ROLE:ALL` activates every held role, `PRINCIPAL_ROLE:<name>`
only that one. A token for a role the principal does not hold is issued but rejected on use (401),
so it cannot escalate. Catalog-role names are local: `eng_reader` in two catalogs shares nothing.

## Practices

Least privilege, grants on namespaces, one convention for role names (`<domain>_<function>`),
separate catalogs (and buckets) for sensitive data, regular reviews, and testing role changes in
non-production. Encode them: a YAML model applied idempotently, drift shown as removals, and an
audit whose every rule is tested against an injected violation (`roles_as_code/`).

## Run the Examples

```bash
cd skills/polaris-access-control/examples/access-control
docker compose -p polaris-access-control up -d --build
docker compose -p polaris-access-control exec runner python -m pytest -v
docker compose -p polaris-access-control down -v
```

On Git Bash for Windows prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`. State is in
memory: restarting Polaris wipes catalogs, principals and roles.

## Anti-Patterns

| Anti-pattern | Why it hurts | Instead |
|---|---|---|
| One principal shared by several jobs | No attribution, revoking one breaks all | One principal per job |
| Catalog-level broad privileges for non-admin roles | Covers every namespace, now and later | Namespace grants with the narrowest privilege |
| Table-by-table grants | Grows with tables, misses new ones | One namespace grant |
| Assuming `NAMESPACE_LIST` shows tables | Users see empty namespaces | Add `TABLE_LIST` / `VIEW_LIST` |
| Expecting offboarding to wait for token expiry | Access is checked per request, so nothing changes until you revoke | Revoke role / delete principal: effective on the next request |
| Admin "rotating" a leaked secret | 403; and rotation keeps the old secret valid | Admin `reset` |
| Sensitive data in a shared catalog or bucket | One wrong grant exposes it | Own catalog + own storage prefix, allow-listed roles |
| Manual grants in production | Invisible drift | YAML model, plan/apply, scheduled audit |

## References

- [references/access_model.md](references/access_model.md): catalogs, isolation, storage locations, principal roles, token scopes, use-case blueprints.
- [references/principals.md](references/principals.md): creation, tokens, reset vs rotate, revocation, lifecycle review.
- [references/privileges_reference.md](references/privileges_reference.md): all privileges, measured matrix, inheritance, documented claims checked.
- [references/roles_as_code_and_review.md](references/roles_as_code_and_review.md): YAML model, plan/apply, drift, access matrix, 7 audit rules.
- Examples: `examples/access-control/` (5 packages, 39 tests).
- Related skills: `polaris-rest-api`, `polaris-catalog-fundamentals`, `polaris-configuration-and-cli` (CLI for roles and grants), `polaris-spark` (RBAC errors seen from Spark).
