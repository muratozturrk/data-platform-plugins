---
name: infra-resource-naming
description: Naming for Kubernetes resources, namespaces, labels and annotations, Helm releases, operators' CRs (Strimzi, Spark, ClickHouse), Vault paths, Keycloak realms/clients/roles, Azure resources (CAF), storage containers and Git repositories. Use when writing IaC, Helm charts, CRs or tenant onboarding code.
---

# Infrastructure Resource Naming

## When to Use This Skill
- Writing Kubernetes manifests, Helm charts, Crossplane compositions, Bicep/Terraform
- Implementing the `Tenant` controller (it creates names in every system)
- Reviewing IaC PRs

## Core Concepts

### Kubernetes
- Names: DNS-1123 subdomain/label (`[a-z0-9-]`, ≤ 63 for labels/services, ≤ 253 for most objects); never underscores.
- Namespaces: `{tenant}-{component}` (`t-acme-spark`, `t-acme-airflow`) or platform namespaces `platform-{component}` (`platform-keycloak`). Prefix `t-` marks tenant namespaces for policies.
- Recommended labels on everything (Kubernetes convention): `app.kubernetes.io/name`, `app.kubernetes.io/instance`, `app.kubernetes.io/version`, `app.kubernetes.io/component`, `app.kubernetes.io/part-of=data-platform`, `app.kubernetes.io/managed-by`. Platform labels under your own prefix: `platform.example/tenant`, `platform.example/workspace`, `platform.example/tier`, `platform.example/cost-center`.
- Annotations for non-identifying metadata (`platform.example/owner`, `platform.example/manifest-hash`).
- Operator CRs: `{tenant}-{purpose}` (`t-acme-main` Kafka, `t-acme-orders-rollup-01hx…` SparkApplication, `t-acme-serving` ClickHouseInstallation).
- Helm releases: `{component}` in platform namespaces, `{tenant}-{component}` in tenant namespaces; chart names = component names.
- Secrets: `{tenant}-{system}-{purpose}` (`t-acme-erp-postgres-credentials`); ConfigMaps `{component}-config`.

### Vault / OpenBao
Paths `tenants/{tenant}/{system}/{purpose}` (`tenants/t-acme/erp-postgres/credentials`); policies `tenant-{tenant}-{role}`; Kubernetes auth roles `{tenant}-{component}`.

### Keycloak
Realm `{platform}` (organizations mode) or `{tenant}` (realm mode); organizations = tenant id; clients `{component}` (`airflow`, `superset`, `trino`, `platform-api`); roles `{scope}:{action}` (`workspace:admin`, `pipeline:run`, `dataset:read`); groups `{tenant}/{workspace}/{role}`.

### Azure (Cloud Adoption Framework)
Components in order: `{resource-type-abbr}-{workload}-{environment}-{region}-{instance}`; CAF abbreviations (`rg`, `aks`, `st` storage account, `kv`, `evhns`, `synw`, `adf`, `dbw`); respect per-resource limits (storage accounts: 3–24 lowercase alphanumerics, globally unique, no hyphens). Examples: `rg-dataplatform-prod-westeurope-001`, `aks-dataplatform-prod-weu-001`, `stdpprodweu001`, `kv-dataplatform-prod-weu-001`. Tags carry tenant/cost-center, not the name.

### Storage containers / buckets
`{org}-{platform}-{environment}-{purpose}` (`contoso-dp-prod-lake`); tenant isolation inside via prefix (pool) or `{org}-{platform}-{environment}-{tenant}` (silo). Bucket names are global and lowercase; keep them short.

### Git repositories
`{platform}-{component}` (`dp-control-plane`, `dp-plugins`, `dp-infra`), tenant config repos `dp-tenant-{tenant}`; no team names.

## Patterns
- The `Tenant` controller derives every name from `tenant_id` + a fixed pattern table; names are never typed by hand.
- Length budget: compute the worst case (`t-{tenant}-{component}-{purpose}` ≤ 63) and constrain `tenant_id` accordingly (e.g. ≤ 20 chars, `[a-z0-9-]`).
- Environment appears in infra names (it is a deployment dimension) but never in data or topic names.

## Anti-patterns
- Underscores or uppercase in Kubernetes names
- Person or team names in namespaces or releases
- Random suffixes with no instance counter (`-x7k2`) where a stable name is needed
- Azure names without type abbreviation or environment

See `references/sources.md`.
