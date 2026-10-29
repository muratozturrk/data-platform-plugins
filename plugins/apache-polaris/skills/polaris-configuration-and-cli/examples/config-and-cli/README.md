# Polaris Configuration and CLI Examples

The `polaris` CLI (PyPI `apache-polaris` 1.8.0), realms, feature flags, rate limiting and the
token broker, each on a Polaris 1.8.0 server configured for exactly that behavior. These 14 tests
are part of a suite that passed in full on a fresh stack.

| Test package | What it proves |
|---|---|
| `cli/` | A full CLI session (catalogs, principals, principal roles, catalog roles, namespaces, privileges): 14 of 18 commands work as commonly documented, 4 need the 1.8.0 form (`--set-property`, `principals reset`, parent namespace first, no `--cascade`); end state checked; FILE storage refused on a default server; connection and auth options (`--host/--port`, `--base-url` = server root, `CLIENT_ID`/`CLIENT_SECRET` env, not client ID plus access token); profiles via `--profile` and `CLIENT_PROFILE` |
| `realms/` | Two realms on one server are isolated (catalogs, principals, tokens); no header falls back to the first realm; only the `Polaris-Realm` header is read; `POLARIS_BOOTSTRAP_<REALM>_ROOT_CLIENT_ID/_SECRET` are ignored; the admin tool's `-r/-c` flags work but a `{realm}` placeholder in the JDBC URL is not substituted |
| `hardening/` | `rate-limiter.filter.type=token-bucket` breaks every request (500); `filter.type=default` with 5 req/s gives 7 x 200 and 23 x 429 out of 30, per realm; tokens are RS256 and verify with the mounted public key; `polaris.features.defaults."..."` is ignored while `realm-overrides` works |

## Stack

| Service | Settings it carries |
|---|---|
| `polaris` | CLI target; FILE storage enabled (`ALLOW_INSECURE_STORAGE_TYPES`, `SUPPORTED_CATALOG_STORAGE_TYPES`) |
| `polaris-realms` | `polaris.realm-context.realms: POLARIS,my_team_realm`, root credentials for both realms |
| `polaris-legacy-env` | Per-realm bootstrap variables that 1.8.0 does not read |
| `postgres` + `bootstrap-templated` + `bootstrap-fixed` | Admin tool with a `{realm}` JDBC URL (fails) and with a concrete database (works) |
| `polaris-durable` | relational-jdbc on database `my_realm`, defaults otherwise |
| `polaris-hard` | RSA token broker from key files, rate limit 5/s, task limits, `.defaults.` flag form, realm override for `analytics`, JSON logs |
| `polaris-ratelimit-invalid` | `polaris.rate-limiter.filter.type: token-bucket` |
| `keys` | One-shot RSA key pair into the `secrets` volume |
| `runner` | `python:3.11-slim` + `apache-polaris==1.8.0`, requests, Docker SDK, cryptography, PyIceberg 0.9.1 |

All Polaris services use `apache/polaris:latest` (1.8.0 at the time of the run) and the admin tool
`apache/polaris-admin-tool:latest`.

## How to Run

```bash
docker compose -p polaris-config up -d --build
docker compose -p polaris-config exec runner python -m pytest -v
docker compose -p polaris-config down -v
```

Use the project name `polaris-config`: `conftest.py` finds containers by that compose project
label. On Git Bash for Windows prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`. The CLI
tests build on each other (`my_catalog`, `limited_reader`, ...) and the profile test writes
`~/.polaris` inside the runner, so run on a fresh stack.

## Layout

```
docker-compose.yml           one Polaris service per group of settings, PostgreSQL, admin tool, runner
config/make_keys.py          RSA key pair for the rsa-key-pair token broker
config/postgres-init.sql     database my_realm with polaris_schema
common/polaris.py            Management API + Iceberg REST client (used to create the granted table)
conftest.py                  cli() runs 'polaris ...' lines with connection options; token(); container()
cli/                         test_cli.py
realms/                      test_realms.py
hardening/                   test_production_hardening.py
runner/Dockerfile            test runner image
```

Metrics, tracing, logs and the startup readiness checks are in the `polaris-deployment` skill's
`examples/observability`.
