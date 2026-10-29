# Tokens and Scopes

## Concept

Both Polaris APIs take a bearer token. In the default setup Polaris issues tokens itself through
the Iceberg REST OAuth2 endpoint, using the client-credentials grant with a principal's client id
and secret. The token's `scope` decides which of the principal's roles are active for the
session. Data access additionally depends on the `X-Iceberg-Access-Delegation` header, which asks
Polaris to vend storage credentials with each table load.

## Token Endpoint

```
POST /api/catalog/v1/oauth/tokens
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials&client_id=<id>&client_secret=<secret>&scope=PRINCIPAL_ROLE:ALL
```

Response (measured on 1.8.0):

```json
{"access_token": "<JWT>", "token_type": "bearer", "expires_in": 3600, ...}
```

| Case | Result |
|---|---|
| Correct id, secret and scope | 200, `expires_in` 3600 |
| No `scope` | 400 `invalid_scope` |
| Wrong secret | 401 |
| Scope names a role the principal does not hold | 200, but every call with the token is 401 |

JWT claims observed: `sub` = principal name, `iss` = `polaris`, expiry one hour after issue.

## Scopes

| Scope | Effect |
|---|---|
| `PRINCIPAL_ROLE:ALL` | Every principal role the principal holds is active |
| `PRINCIPAL_ROLE:<name>` | Only that principal role is active |

Use a single-role scope to run automation with less than its owner's access. Engines take the
scope as a catalog property (for example `scope` in PyIceberg or
`spark.sql.catalog.<name>.scope` in Spark).

## Token Lifetime and Revocation

Access is evaluated per request against the current roles and grants. A still-valid token stops
working immediately when its principal role is revoked (403) or the principal is deleted (401);
it does not keep the access it had when issued. Clients refresh before `expires_in` runs out;
PyIceberg and the Iceberg Java client do this themselves when given `credential` and the token
endpoint.

## Engine Configuration

```python
from pyiceberg.catalog.rest import RestCatalog
catalog = RestCatalog("polaris", **{
    "uri": "http://polaris:8181/api/catalog",
    "warehouse": "finance",                                    # catalog name -> prefix
    "credential": f"{client_id}:{client_secret}",
    "oauth2-server-uri": "http://polaris:8181/api/catalog/v1/oauth/tokens",
    "scope": "PRINCIPAL_ROLE:ALL",
    "header.X-Iceberg-Access-Delegation": "vended-credentials",
})
```

Without the delegation header the engine gets metadata only and must bring its own storage keys.
With it, every table load returns short-lived keys scoped to that table's location: read-only for
`TABLE_READ_DATA`, read+write for `TABLE_WRITE_DATA`. Creating a table with this header also needs
a data privilege, because Polaris must vend keys for the new location.

## curl Pattern

```bash
P=http://localhost:8181
TOKEN=$(curl -s -X POST "$P/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials -d client_id="$ID" -d client_secret="$SECRET" \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
curl -s -H "Authorization: Bearer $TOKEN" "$P/api/catalog/v1/config?warehouse=finance" | jq .overrides
curl -s -H "Authorization: Bearer $TOKEN" "$P/api/management/v1/catalogs" | jq '.catalogs[].name'
```

Keep `$SECRET` in the environment or a secret store; do not put it on the command line in shared
shells (shell history, process lists).

## External Identity Providers

The Iceberg REST specification deprecates the catalog's own token endpoint in favour of an
external OAuth2 server (`oauth2-server-uri` pointing at the IdP). Polaris 1.8.0 still serves the
built-in endpoint, and every example here uses it. Federated identity settings exist in 1.8.0 but
were not verified here; see the `polaris-configuration-and-cli` skill (token broker) and the
roadmap notes in `polaris-catalog-fundamentals`.

## Worked Example: Least-Privilege Session

```python
def test_token_scope_activates_only_the_requested_role(org):
    dana = org["people"]["dana"]
    viewer_only = dana.pyiceberg(org["comp"], scope="PRINCIPAL_ROLE:comp_viewer")
    assert viewer_only.load_table("audits.events").scan().to_arrow().num_rows == 3
    eng_with_viewer_scope = dana.pyiceberg(org["eng"], scope="PRINCIPAL_ROLE:comp_viewer")
    assert is_denied(lambda: eng_with_viewer_scope.load_table("pipelines.events"))
    eng_all = dana.pyiceberg(org["eng"], scope="PRINCIPAL_ROLE:ALL")
    append_one(eng_all, "pipelines.events")
```

(From `polaris-access-control/examples/access-control/principal_roles/`.)

## Pitfalls and Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| 400 `invalid_scope` | `scope` missing | Add `PRINCIPAL_ROLE:ALL` or a role |
| 401 with a fresh token | Scope names a role not held, or principal deleted | Check roles; new token |
| 403 on everything after onboarding | No principal role, or rotation required on that image | Assign a role; rotate the secret |
| Table loads, scan fails with storage 403 | No delegation header or no data privilege | Send `X-Iceberg-Access-Delegation: vended-credentials`; grant `TABLE_READ_DATA` |
| Long jobs fail after an hour | Static token pasted into config | Give the client `credential` + token endpoint so it refreshes |

## Related

- [management_api.md](management_api.md), [catalog_api.md](catalog_api.md)
- `polaris-access-control` skill: principal roles, scopes and revocation semantics.
- `polaris-spark` skill: the same settings as Spark catalog properties.
