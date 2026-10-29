# Principals and Their Credential Lifecycle

## Concept

A principal is the identity of a user (a person acting through an application) or a service (a
Spark job, a BI tool, an ingestion pipeline). It authenticates with a client id and client secret
using the OAuth2 client-credentials flow and gets access only through principal roles. Every
request is attributed to exactly one principal, which is what makes access auditable and lets
you revoke one job without touching another.

## Lifecycle

```
admin:   POST /principals {name, properties}        -> clientId + clientSecret (shown once)
admin:   PUT  /principals/{p}/principal-roles       -> role assigned
service: POST /api/catalog/v1/oauth/tokens          -> JWT, 3600 s
service: catalog / management calls with Bearer token
admin:   DELETE /principals/{p}/principal-roles/{r} -> next request 403
admin:   DELETE /principals/{p}                     -> next request 401
```

Three questions to answer in every review:

1. Does every principal that should exist exist?
2. Does each one hold exactly the roles it needs (no missing, no extra)?
3. Did every retired principal lose access?

## API Reference (Management API v1)

| Operation | Call | Notes |
|---|---|---|
| Create | `POST /principals` `{"principal": {"name", "properties"}}` | Response holds `credentials.clientId` / `clientSecret`; a `clientId` you send is ignored |
| Get / list | `GET /principals/{name}`, `GET /principals` | Never contains secrets |
| Update | `PUT /principals/{name}` with `currentEntityVersion` | Properties such as owner, kind |
| Delete | `DELETE /principals/{name}` | Live tokens fail with 401 next request |
| Reset (admin) | `POST /principals/{name}/reset` `{}` | Same client id, new secret, old one invalid |
| Rotate (self) | `POST /principals/{self}/rotate` | Only the principal itself |
| Assign role | `PUT /principals/{name}/principal-roles` `{"principalRole": {"name"}}` | Role must exist (404 otherwise) |
| Revoke role | `DELETE /principals/{name}/principal-roles/{role}` | Effective next request |

Token request:

```bash
curl -s -X POST "$POLARIS/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials -d client_id="$ID" -d client_secret="$SECRET" \
  -d scope=PRINCIPAL_ROLE:ALL
```

## Measured Behaviour (Polaris 1.8.0)

| Test | Result |
|---|---|
| Stored principal record | Keys: clientId, createTimestamp, entityVersion, lastUpdateTimestamp, name, properties (no secret) |
| Client-credentials token | JWT `sub` = principal name, `iss` = polaris, lifetime 3600 s; wrong secret 401 |
| Admin reset | Same client id, new secret; old secret 401 |
| Admin calls `/rotate` for another principal | 403 `not authorized for op ROTATE_CREDENTIALS` |
| Self rotation | Original secret still 200 after one rotation, 401 after the second (previous secret kept as secondary) |
| Lifecycle | 403 before assignment; access after; 403 after role revoke; 401 after principal delete |
| `credentialRotationRequired: true` at creation | Varies by image: on one 1.8.0 test the principal was blocked (403) until it rotated; another stack observed no blocking. Test it on your build before relying on it |

## Reset or Rotate?

| Situation | Use | Why |
|---|---|---|
| Secret leaked, person left in a hurry | Admin `reset` | Old secret stops working at once |
| Planned rotation of a running service | Self `rotate` | Old secret keeps working until the next rotation: no downtime |
| Hand a secret to a person who must change it | Create, then let them `rotate` | The admin never knows the final secret |

## Worked Example: Lifecycle Review Against a Roster

The IdP or HR system says what should exist; Polaris says what does. The review is the diff.

```python
def audit_lifecycle(admin, roster: dict[str, list[str]], retired: list[str], scope_prefix=""):
    present = {p["name"] for p in admin.principals() if p["name"].startswith(scope_prefix)}
    report = LifecycleReport()
    for name, expected in roster.items():
        if name not in present:
            report.missing.append(name)
            continue
        actual = set(admin.principal_roles_of(name))
        if lacking := sorted(set(expected) - actual):
            report.missing_roles[name] = lacking
        if extra := sorted(actual - set(expected)):
            report.excess_roles[name] = extra
    report.not_revoked = sorted(p for p in retired if p in present)
    return report
```

Measured on a seeded server: 1 missing principal, 1 principal without its role, 1 excess role and
1 retired principal not revoked, all reported. Report excess roles as well as missing ones: least
privilege fails silently when extra roles pile up.

## Practices

- Store the secret in a secrets manager the moment the principal is created.
- One principal per job or tool; never share one between services.
- Keep identity metadata (owner, kind, ticket) in principal properties so reviews have context.
- Offboarding: revoke principal roles, then delete the principal. Both act on tokens already issued.
- Pass secrets to engines through environment variables or a secret store, never in code or notebooks.

## Pitfalls and Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Secret lost right after creation | Returned once, never listed | Admin `reset` to issue a new one |
| Requested `clientId` not used | Polaris generates its own | Read it from the create response |
| 403 on `/rotate` as admin | Rotation is self-service | Use `/reset` |
| Old secret still works after rotation | Previous secret kept as secondary | Rotate twice, or `reset` |
| 400 `invalid_scope` | No `scope` in the token request | Send `PRINCIPAL_ROLE:ALL` or one role |
| 401 right after deleting and re-creating a principal | Old token belongs to the deleted entity | Get a new token with the new secret |

## Related

- [access_model.md](access_model.md): principal roles and scopes.
- [roles_as_code_and_review.md](roles_as_code_and_review.md): principals synced from a model.
- `polaris-rest-api` skill: request bodies and status codes for every principal call.
- `polaris-configuration-and-cli` skill: the same operations with the `polaris` CLI.
