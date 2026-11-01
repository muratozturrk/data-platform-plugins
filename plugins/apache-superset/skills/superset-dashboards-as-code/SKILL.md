---
name: superset-dashboards-as-code
description: Managing Superset databases, datasets, charts, dashboards, RLS, roles and ownership as versioned YAML with native export/import and superset-cli/preset-cli "sync native" in CI, Jinja templating per tenant/environment, drift detection and promotion. Use when setting up GitOps for Superset or reviewing asset YAML.
---

# Dashboards as Code

## Core Concepts
- Superset's native export produces a ZIP with `metadata.yaml` and `databases/`, `datasets/`, `charts/`, `dashboards/` YAML keyed by UUID; import via UI, REST (`/api/v1/assets/import/`, per-type import endpoints) or CLI.
- `superset-cli` (open-source part of Preset's backend-sdk; `preset-cli` for Preset workspaces) adds: `export-assets`, `sync native` (import a directory of templated YAML, `--overwrite`, `--disallow-edits` to lock synced assets in the UI, `--option key=value` for templates), `sync dbt-core/dbt-cloud` (datasets+metrics from dbt models/exposures), `export-rls`/`import-rls`, `export-roles`, `export-users`, `export-ownership`/`import-ownership`, `delete-assets` with dry-run.
- Export through the CLI (not the UI) when assets contain Jinja: the CLI escapes Superset macros so files round-trip.
- UUIDs are identity: keep them stable across edits; a new UUID means a new asset and a possible duplicate.

## Repo layout
```
superset-assets/
  metadata.yaml                 # version, type
  shared/{databases,datasets,charts,dashboards}/
  tenants/t-acme/{databases,datasets,charts,dashboards}/
  policies/rls.yaml  roles.yaml  ownership.yaml
  templates/database.yaml.j2    # URI/catalog per env/tenant
  tests/                        # schema, naming, security checks
```
Each tenant bundle is generated or templated from `shared/` plus tenant options (catalog, schema, theme, dashboards enabled).

## Templating
```yaml
# databases/trino.yaml
database_name: "{{ tenant }}-trino"
sqlalchemy_uri: "trino://{{ env.TRINO_USER }}@trino.platform:8443/{{ tenant_catalog }}?protocol=https"
extra: '{"engine_params": {"connect_args": {"http_scheme": "https"}}, "allows_virtual_table_explore": true}'
allow_dml: false
expose_in_sqllab: {{ expose_sqllab | default(false) }}
impersonate_user: true
```
Run: `superset-cli https://superset.platform sync native tenants/t-acme --option tenant=t-acme --option tenant_catalog=t_acme --disallow-edits --overwrite`.

## CI pipeline
1. Validate: YAML parse, required keys, UUID uniqueness/stability vs main, naming check (`naming-conventions`), no credentials, dataset metrics defined, `cache_timeout` present.
2. Staging sync + smoke: load each dashboard via API (`/api/v1/dashboard/{id}/charts`, chart data) as a test user.
3. Prod sync on tag; then `import-rls`, `import-ownership`; register embedded dashboards and allowed domains.
4. Nightly drift: `export-assets` from prod → `git diff` → open an issue listing UI edits.
5. Rollback: re-sync previous tag; metadata DB backup before bulk changes.

## Patterns
- dbt as source of datasets: `sync dbt-core` maps models (with `meta.superset` configs) to datasets and metrics; dashboards reference those datasets → one semantic definition.
- Control-plane generated bundles: the `Tenant` controller renders the tenant folder and triggers the sync job; humans edit `shared/` only.
- Certification and `is_managed_externally` flags set on synced assets so the UI shows them as code-managed.

## Anti-patterns
- Editing prod in the UI and re-exporting "sometimes"
- Secrets in `sqlalchemy_uri` inside Git
- Copying dashboards per tenant by hand
- Importing with UI when files contain Jinja

Sources: Superset docs (import/export, REST API); preset-io/backend-sdk (`preset-cli`/`superset-cli`) README and Preset docs "Workspace operations"; Preset blog "Managing Apache Superset Assets as Code" and "Creating Custom Apache Superset Assets Programmatically"; Preset Agent Skills (`preset-cli-skills`, `preset-api-skills`) for command-level mechanics.
