---
name: polaris-configuration-and-cli
description: "Tune and manage Apache Polaris (1.8.0) day to day - the polaris CLI (PyPI apache-polaris) for catalogs, principals, principal roles, catalog roles, namespaces and privileges, its auth options (--host/--port, --base-url, CLIENT_ID/CLIENT_SECRET, --access-token, profiles), the 1.8.0 forms of commonly misdocumented commands (--set-property, principals reset, no --cascade); realms (polaris.realm-context.realms, the Polaris-Realm header, require-header, per-realm root bootstrap); feature flags globally and per realm (polaris.features.\"FLAG\", realm-overrides, storage-type flags), catalog polaris.config.* properties, rate limiting (filter.type default + token bucket), the RSA token broker and task limits. Use when scripting Polaris governance in CI, adding tenants, hardening settings, or debugging CLI exit 2 / 403 ROTATE_CREDENTIALS / 501 on revoke, requests landing in the wrong realm, ignored .defaults. flags, or every request returning 500 after enabling rate limiting."
---

# Polaris Configuration and CLI

Day-2 management of a running Polaris: the `polaris` CLI for catalogs, principals, roles and
grants; realms for tenant isolation; feature flags and catalog properties; and the service
settings that harden a shared server (rate limiting, token broker, task limits). All behavior is
measured on Polaris 1.8.0 with CLI 1.8.0. Much published syntax comes from older releases and
fails or is silently ignored on 1.8.0; this skill lists the working forms. Getting the server
running (images, persistence, storage, observability, readiness checks) is the
`polaris-deployment` skill.

## When to Use This Skill

- Scripting catalog, principal, role and grant changes for operations scripts or CI.
- A CLI command from documentation exits 1 or 2, or the server answers 403 / 501.
- Hosting several tenants or environments on one Polaris (realms).
- Requests or tokens end up in the wrong realm, or a realm's secret returns 401.
- Allowing or restricting storage types globally or for one realm.
- Turning on rate limiting, choosing a token broker, setting task limits.
- A configuration property seems to have no effect.

## Decision Table

| Need | Technique / setting | Reference |
|---|---|---|
| Admin from a shell or CI | `polaris` CLI with env credentials or a profile | [polaris-cli.md](references/polaris-cli.md) |
| Several environments / realms from one shell | `polaris profiles create`, `--profile`, `CLIENT_PROFILE` | [polaris-cli.md](references/polaris-cli.md) |
| New secret for a principal | `principals reset` (admin) / `rotate-credentials` (self) | [polaris-cli.md](references/polaris-cli.md) |
| Tenant isolation in one deployment | `polaris.realm-context.realms` + `Polaris-Realm` header | [realms.md](references/realms.md) |
| Reject requests without a realm | `polaris.realm-context.require-header=true` | [realms.md](references/realms.md) |
| Root principal for a new realm | `POLARIS_BOOTSTRAP_CREDENTIALS` or admin tool `-r/-c` | [realms.md](references/realms.md) |
| Server-wide behavior switch | `polaris.features."FLAG"` | [feature-flags-and-hardening.md](references/feature-flags-and-hardening.md) |
| Stricter rules for one tenant | `polaris.features.realm-overrides."realm"."FLAG"` | [feature-flags-and-hardening.md](references/feature-flags-and-hardening.md) |
| Protect from overload | `polaris.rate-limiter.filter.type=default` + token bucket | [feature-flags-and-hardening.md](references/feature-flags-and-hardening.md) |
| Tokens valid across replicas | `token-broker.type=rsa-key-pair` with key files | [feature-flags-and-hardening.md](references/feature-flags-and-hardening.md) |

## The `polaris` CLI

**What it is.** `polaris [global-options] <command> <subcommand> [options] [arguments]` over
the management and catalog APIs. Commands: `catalogs`, `principals`, `principal-roles`,
`catalog-roles`, `namespaces`, `privileges`, `profiles`.

```bash
export CLIENT_ID=root CLIENT_SECRET=s3cr3t
polaris --host polaris --port 8181 catalogs update my_catalog --set-property environment=dev
polaris --base-url http://polaris:8181 principals reset data_engineer_1
polaris --host polaris --port 8181 privileges table revoke --catalog my_catalog \
  --catalog-role limited_reader --namespace analytics.q1 --table sales_data TABLE_READ_DATA
```

**Gotchas (measured)**

- 14 of 18 commonly documented commands work unchanged. Fails: `--property` on `catalogs update`
  (exit 2; use `--set-property`), `rotate-credentials` by root (403; use `principals reset`),
  `namespaces create analytics.q1` without the parent (create `analytics` first),
  `revoke --cascade` (501; drop it).
- Default target is `localhost:8181`; `--base-url` takes the server root, not `/api/catalog/v1`.
- Client ID plus `--access-token` together: exit 2 (`... but not both`).
- Table grants need an existing table; a missing namespace gives a misleading `CatalogRole not found`.
- FILE catalogs fail on default servers (`Unsupported storage type: FILE`).

## Realms

**What it is.** A tenant with its own root principal, catalogs and roles, configured on the
server and selected per request by the `Polaris-Realm` header.

```yaml
polaris.realm-context.realms: POLARIS,my_team_realm        # first = default
POLARIS_BOOTSTRAP_CREDENTIALS: POLARIS,root,s3cr3t;my_team_realm,root,t3amsecret
polaris.realm-context.require-header: "true"               # strict tenancy
```

**Gotchas (measured)**

- Isolation holds: a team-realm catalog is invisible in `POLARIS`; a `POLARIS` token or secret
  used against the team realm gets 401.
- Without the header, requests fall back to the first realm (the `POLARIS` secret got 200).
- Only `Polaris-Realm` is read; a header named `realm` is ignored.
- `POLARIS_BOOTSTRAP_<REALM>_ROOT_CLIENT_ID/_SECRET` are not read; the realm gets generated
  credentials (printed in the log).
- Admin tool `bootstrap -r my_realm -c my_realm,client-id,my-secret` works, but a
  `jdbc:postgresql://.../{realm}` URL is not substituted (exit 3, database `{realm}` missing).

## Feature Flags and Hardening

**What it is.** Quarkus properties: global flags, realm overrides, catalog `polaris.config.*`
properties, and service settings.

```yaml
polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES": '["S3","GCS","AZURE"]'
polaris.features.realm-overrides."analytics"."SUPPORTED_CATALOG_STORAGE_TYPES": '["S3"]'
polaris.rate-limiter.filter.type: default
polaris.rate-limiter.token-bucket.requests-per-second: "5"
polaris.rate-limiter.token-bucket.window: PT1S
polaris.authentication.token-broker.type: rsa-key-pair
polaris.authentication.token-broker.rsa-key-pair.public-key-file: /secrets/public.key
polaris.authentication.token-broker.rsa-key-pair.private-key-file: /secrets/private.key
```

**Gotchas (measured)**

- `polaris.features.defaults."FLAG"` is ignored; the realm override works (GCS 400 in
  `analytics`, 201 in `POLARIS`).
- `rate-limiter.filter.type=token-bucket` makes every request 500 while health stays green;
  valid values are `default` and `no-op`. With `default` at 5/s: 7 x 200, 23 x 429 out of 30,
  bucket per realm.
- RSA key files: tokens are RS256 and verify with the mounted public key.
- 1.8.0 default storage types are already `S3, GCS, AZURE`; FILE needs `ALLOW_INSECURE_STORAGE_TYPES` too.

## Run the Examples

`examples/config-and-cli/` (one Polaris service per group of settings, PostgreSQL, admin tool,
CLI runner; 14 tests):

```bash
cd examples/config-and-cli
docker compose -p polaris-config up -d --build
docker compose -p polaris-config exec runner python -m pytest -v
docker compose -p polaris-config down -v
```

Keep the project name `polaris-config` (containers are found by compose label). On Git Bash for
Windows prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`. Details:
[examples/config-and-cli/README.md](examples/config-and-cli/README.md).

## Anti-Patterns

| Anti-pattern | Why it hurts | Instead |
|---|---|---|
| Copying pre-1.x CLI syntax into CI | Exit 2 / 403 / 501 break the pipeline | Use the 1.8.0 forms above |
| Secrets as CLI flags in scripts | Shell history, process list | `CLIENT_ID`/`CLIENT_SECRET` from a secret store, or profiles |
| Relying on the default realm for isolation | Headerless requests land in it silently | `require-header=true` |
| `polaris.features.defaults."..."` | Silently ignored | `polaris.features."..."` |
| `rate-limiter.filter.type=token-bucket` | Every request 500 | `filter.type=default` + `token-bucket.*` |
| Per-process signing keys on several replicas | Tokens from one replica rejected by another | Shared RSA key files |
| One realm per team when catalogs would do | Separate roots, duplicated admin | Realms for tenants, catalogs for teams |

## References

- [references/polaris-cli.md](references/polaris-cli.md): global options, auth methods, command reference, failing older syntax, onboarding example.
- [references/realms.md](references/realms.md): realm settings, request flow, measured isolation, per-realm bootstrap.
- [references/feature-flags-and-hardening.md](references/feature-flags-and-hardening.md): flags, realm overrides, catalog properties, rate limiting, token broker, task limits.
- Related skills: `polaris-deployment` (persistence, storage, observability, readiness), `polaris-catalog-federation` (federation flags), `polaris-catalog-fundamentals`.
