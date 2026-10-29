# Realms

## Concept

A realm is a tenant inside one Polaris deployment: its own root principal, principals, roles,
catalogs and metadata. Realms are **configured**, not created through the API. Each request is
resolved to a realm from a header; tokens are bound to the realm they were issued in.

| Boundary | Use for | Isolates |
|---|---|---|
| Realm | Customers, business units, environments on shared infrastructure | Principals, roles, catalogs, metastore content |
| Catalog | Teams or domains inside a tenant | Storage config and grants |
| Namespace | Grouping tables | Grants only |

## Configuration Reference (Polaris 1.8.0)

| Setting | Default | Meaning |
|---|---|---|
| `polaris.realm-context.realms` | `POLARIS` | Allowed realms; the **first** is the default |
| `polaris.realm-context.header-name` | `Polaris-Realm` | Header carrying the realm |
| `polaris.realm-context.require-header` | `false` | `true` rejects requests without the header |
| `POLARIS_BOOTSTRAP_CREDENTIALS` | none | `realm,client_id,secret;realm2,client_id,secret` root credentials at startup (in-memory setups) |
| admin tool `bootstrap -r <realm> -c <realm>,<id>,<secret>` | | Root principal in the database (relational-jdbc) |

```yaml
  polaris-realms:
    image: apache/polaris:latest
    environment:
      polaris.realm-context.realms: POLARIS,my_team_realm
      POLARIS_BOOTSTRAP_CREDENTIALS: POLARIS,root,s3cr3t;my_team_realm,root,t3amsecret
```

## Request Flow

```
POST /api/catalog/v1/oauth/tokens   Polaris-Realm: my_team_realm   (root secret of that realm)
  -> token bound to my_team_realm
GET  /api/management/v1/catalogs    Polaris-Realm: my_team_realm   -> only my_team_realm's catalogs
same token with                     Polaris-Realm: POLARIS         -> 401
```

## Measured Behavior

| Check | Result |
|---|---|
| Catalog created in `my_team_realm` | 201; visible there (`team_cat`), `POLARIS` sees none |
| `POLARIS` token used with `Polaris-Realm: my_team_realm` | 401 |
| `POLARIS` root secret against `my_team_realm` | 401 (same client ID `root`, different realm) |
| Token request **without** header, `POLARIS` secret | 200: falls back to the first realm |
| Token request without header, `my_team_realm` secret | 401 |
| Header named `realm: my_team_realm` (older docs) | Ignored: 401; `Polaris-Realm:` 200 |
| `POLARIS_BOOTSTRAP_MY_REALM_ROOT_CLIENT_ID` / `_SECRET` (older docs) | Not read; realm bootstrapped with generated credentials printed in the log (`root principal credentials:`); `my-client-id` 401 |
| Admin tool `-r my_realm -c my_realm,client-id,my-secret` with JDBC URL `.../{realm}` | Exit 3: `FATAL: database "{realm}" does not exist` |
| Same flags with `jdbc:postgresql://postgres:5432/my_realm` | Exit 0; server on that DB issued a token for `client-id` (200) |
| Fresh realm | No catalog exists until you create one |

Requests without the header silently landing in the default realm is the important trap: for
strict tenancy set `polaris.realm-context.require-header=true` (Polaris' own readiness checks
flag `require-header=false`, see the `polaris-deployment` skill).

## Worked Example: Per-Realm Durable Bootstrap

PostgreSQL does not get databases from Polaris; create them yourself:

```sql
CREATE DATABASE my_realm;
\c my_realm
CREATE SCHEMA polaris_schema;
```

```yaml
  bootstrap-my-realm:
    image: apache/polaris-admin-tool:latest
    command: ["bootstrap", "-r", "my_realm", "-c", "my_realm,client-id,my-secret"]
    environment: &durable_env
      polaris.persistence.type: relational-jdbc
      quarkus.datasource.db-kind: postgresql
      quarkus.datasource.jdbc.url: jdbc:postgresql://postgres:5432/my_realm   # concrete name
      quarkus.datasource.username: polaris
      quarkus.datasource.password: polaris
  polaris-durable:
    image: apache/polaris:latest
    environment:
      <<: *durable_env
      polaris.realm-context.realms: my_realm
```

Pre-1.0 commands such as `java -jar polaris-service-all.jar bootstrap polaris-server.yml` or
`bin/polaris-service bootstrap config/polaris-server.yml` do not apply to 1.x; the admin-tool
image bootstraps.

## Using Realms from Clients

- HTTP: send `Polaris-Realm: <realm>` on the token request **and** on every API call.
- CLI: `--realm <realm>`, or store realm and header name in a profile (`polaris profiles create`).
- Engines (Spark, PyIceberg): pass the header as a catalog header property; the engine-specific
  form is in the engine skills of this plugin.

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| Request lands in the default realm | Header missing or misnamed | `Polaris-Realm` header; `require-header=true` |
| 401 with a valid secret | Secret belongs to another realm, or token from another realm | Request the token with the right realm header |
| Custom bootstrap variables ignored | Per-realm `POLARIS_BOOTSTRAP_<REALM>_...` names | `POLARIS_BOOTSTRAP_CREDENTIALS=realm,id,secret` |
| Admin tool exit 3, database `{realm}` missing | Placeholder in JDBC URL is not substituted | One concrete database per URL |
| Root credentials unknown after start | Realm bootstrapped with generated credentials | Read them from the log once, then rotate, or bootstrap explicitly |

## Related

- [polaris-cli.md](polaris-cli.md): `--realm`, profiles.
- [feature-flags-and-hardening.md](feature-flags-and-hardening.md): per-realm feature overrides, per-realm rate limits.
- `polaris-deployment` skill: relational-jdbc and admin-tool basics.
