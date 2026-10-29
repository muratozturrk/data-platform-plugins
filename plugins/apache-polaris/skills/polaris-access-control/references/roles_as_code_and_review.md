# Roles as Code and Automated Access Review

## Concept

Access models drift: ad-hoc grants, leftover principals and copied roles accumulate, and manual
reviews do not scale. Treat the access model like infrastructure:

```
rbac_model.yaml (desired)  ─┐
                            ├─> plan (adds + removals of access edges) ─> apply (idempotent) ─> Polaris
Polaris (actual state)     ─┘
Polaris ─> audit (7 rules)      Polaris ─> access matrix (persona x catalog x read/write)
```

| Approach | Use when | Trade-off |
|---|---|---|
| Manual grants in a UI or CLI | Very small teams | No history, drift invisible |
| Roles as code (this pattern or a Terraform provider) | More than a handful of roles | Needs a pipeline and review |
| Scheduled automated audit | Always | Rules must match your real naming convention |

## The Model

```yaml
read: &read [NAMESPACE_LIST, TABLE_LIST, TABLE_READ_PROPERTIES, TABLE_READ_DATA, VIEW_LIST, VIEW_READ_PROPERTIES]
write: &write [NAMESPACE_LIST, TABLE_LIST, TABLE_READ_PROPERTIES, TABLE_READ_DATA, TABLE_CREATE,
               TABLE_WRITE_PROPERTIES, TABLE_WRITE_DATA, VIEW_LIST, VIEW_READ_PROPERTIES]
catalogs:
  marketing:       {namespaces: [campaigns], sensitive: false}
  sensitive_data:  {namespaces: [customers], sensitive: true}
catalog_roles:
  marketing:
    marketing_admin:   [{catalog: [CATALOG_MANAGE_CONTENT]}]
    marketing_analyst: [{namespace: campaigns, privileges: *read}]
  sensitive_data:
    sensitive_data_viewer: [{namespace: customers, privileges: *read}]
principal_roles:
  marketing_analyst:  {marketing: [marketing_analyst]}
  compliance_officer: {sensitive_data: [sensitive_data_viewer]}
principals:
  max_mkt_bi: [marketing_analyst]
sensitive_access_roles: [compliance_officer, privacy_engineer]
```

The full model (`examples/access-control/roles_as_code/rbac_model.yaml`) combines multi-tenant,
cross-team and sensitive-data setups: 5 catalogs, 9 catalog roles, 6 principal roles, 6 principals.

## Plan and Apply

```python
def plan(desired: State, actual: State) -> list[Change]:
    """Adds for everything missing; removes only for access edges (grants, bindings, memberships).
    Identities and containers are never deleted automatically: that is a human decision."""
    changes = []
    for kind, attr in (("catalog", "catalogs"), ("namespace", "namespaces"), ("catalog_role", "catalog_roles"),
                       ("grant", "grants"), ("principal_role", "principal_roles"), ("binding", "bindings"),
                       ("principal", "principals"), ("membership", "memberships")):
        want, have = getattr(desired, attr), getattr(actual, attr)
        changes += [Change("add", kind, k) for k in sorted(want - have, key=str)]
        if kind in ("grant", "binding", "membership"):
            changes += [Change("remove", kind, k) for k in sorted(have - want, key=str)]
    return sorted(changes, key=lambda c: (c.action == "add", ORDER_ADD.index(c.kind)))
```

`apply` executes removals first, then adds in dependency order (catalog, namespace, catalog role,
grant, principal role, binding, principal, membership). New catalogs also get
`CATALOG_MANAGE_CONTENT` on `catalog_admin` so tables can be created with vended credentials.

Measured:

| Step | Result |
|---|---|
| First plan on an empty server | 5 catalogs, 5 namespaces, 9 catalog roles, 42 grants, 6 principal roles, 15 bindings, 6 principals, 6 memberships |
| Second plan | 0 changes (idempotent) |
| A manual grant and a manual membership added | Plan: remove 1 grant (`TABLE_WRITE_DATA` on `marketing_analyst`), remove 1 membership; empty after apply |

## Verifying Access, Not Configuration

The access-matrix test computes the expected access from the YAML only and compares it with real
reads and appends (through vended keys) for every persona x catalog. It does not trust Polaris'
own view of the grants. Measured: 30 combinations, 5 write, 10 read, 15 none, all equal to the
model. Only the two allow-listed personas reached the sensitive catalog.

## Audit Rules

| Rule | Severity | Practice | Check |
|---|---|---|---|
| R1 | high | Least privilege | Broad privileges (`MANAGE_*`, `*_FULL_METADATA`) on a role not named `*_admin` |
| R2 | low | Grant on namespaces | Table-level grants |
| R3 | low | Naming convention | Role names not snake_case `<domain>_<function>` |
| R4 | medium | Lifecycle review | Principal without principal role |
| R5 | medium | Regular review | Catalog role bound to nothing / principal role granting nothing |
| R6 | high | Sensitive-data governance | Sensitive catalog reachable by a role outside the allow-list |
| R7 | high | Admin separation | Non-root principal holding `service_admin` |

Measured: 0 findings on the model; each rule fired on an injected violation and the audit was
clean again after cleanup. A rule that never fires in a test is a rule that can silently stop
working, so keep one injected violation per rule.

## Practices Encoded

- Align roles with business functions; one principal role per job function, not per person.
- Least privilege; broad privileges only on `*_admin` roles.
- Hierarchical namespaces and namespace grants.
- One naming convention, enforced (snake_case here).
- Separate catalogs for sensitive data with an explicit allow-list; the rule that matters most in GDPR/HIPAA reviews.
- Test role changes against a non-production server (the examples use a throwaway stack).

## Pitfalls and Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Plan wants to delete a catalog | Not possible here by design | Delete containers and identities by hand after review |
| Drift reappears after apply | Someone grants manually in a loop (script, UI) | Find the source; make the model the only writer |
| Audit floods with R3 findings | Convention in the rule differs from yours | Adapt the regex to your convention, then enforce it |
| Access matrix differs from the model | A grant gives more than expected (e.g. `WRITE_DATA` implies read) | Check [privileges_reference.md](privileges_reference.md) |
| Sensitive catalog reachable by a new role | Binding added outside the allow-list | R6 flags it; fix the model or the allow-list |

## Related

- [access_model.md](access_model.md), [principals.md](principals.md), [privileges_reference.md](privileges_reference.md)
- `polaris-configuration-and-cli` skill: CLI-based management.
- `polaris-rest-api` skill: endpoints used by `apply`.
