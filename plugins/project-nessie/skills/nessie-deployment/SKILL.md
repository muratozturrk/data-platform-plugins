---
name: nessie-deployment
description: Running Nessie in production on Kubernetes: Helm chart values, Quarkus configuration, version store choice (JDBC/PostgreSQL default, RocksDB, MongoDB, BigTable), HA and scaling, TLS/ingress, OIDC wiring, OpenTelemetry/Prometheus, backups and restore, upgrades and export/import. Use when installing, sizing, upgrading or troubleshooting the Nessie server.
---

# Nessie Deployment

## Core Concepts
- Image `ghcr.io/projectnessie/nessie:<version>` (Quarkus, Java); Helm chart `nessie-helm/nessie` from `https://charts.projectnessie.org`; configuration via Helm values → `application.properties`/env (`NESSIE_*`, `QUARKUS_*`), `advancedConfig` for any property.
- **Version store** (`nessie.version.store.type`): IN_MEMORY (dev), ROCKSDB (single node, PVC), **JDBC** with `nessie.version.store.persist.jdbc.datasource=postgresql` and `quarkus.datasource.postgresql.*` (production, platform default), MONGODB, BIGTABLE, CASSANDRA (LWT), DYNAMODB (beta). Legacy types must be migrated with `nessie-quarkus-cli` export/import.
- Stateless server → HA with `replicaCount ≥ 2` behind a Service/Ingress; PostgreSQL is the state (HA via the platform's PostgreSQL operator; pgBackRest backups).
- Ports: 19120 HTTP (REST v1/v2, Iceberg REST, UI), 9000 management (health, metrics). Probes: `/q/health/live`, `/q/health/ready`.
- Security wiring: `authentication.enabled=true`, `authentication.oidcAuthServerUrl`, `oidcClientId` (Keycloak realm/client); `authorization.enabled=true` + `authorization.rules` (CEL); TLS at ingress or Quarkus; secrets via Kubernetes Secrets populated by External Secrets Operator from OpenBao.
- Catalog settings: warehouses and S3 bucket options (see `nessie-iceberg-rest-catalog`); `nessie.catalog.service.s3.default-options.*`.
- Observability: `quarkus.otel.*` exporter to the platform collector; Prometheus scrape on management port; key metrics: commit latency/rate, reference count, HTTP 4xx/5xx, datasource pool usage, JVM.

## Sizing (starting points)
2 replicas × (1 vCPU, 2 GiB; JVM heap 1 GiB) serve thousands of commits/minute; scale replicas for request throughput; PostgreSQL small (metadata only, tens of GB over years); connection pool `quarkus.datasource.postgresql.jdbc.max-size` ≈ 20 per replica.

## Operations
- Backups: PostgreSQL (pgBackRest) + periodic `nessie-quarkus-cli export` to object storage (portable, version-independent); restore drill quarterly.
- Upgrades: pin version; read release notes; upgrade staging; rolling restart (schema migrations run automatically on JDBC store); keep engine client versions compatible (Spark extensions artifact version = Nessie version).
- Migration between stores or repositories: export → import; between catalogs (Polaris → Nessie): see `nessie-cli-api-and-migration`.
- Repository config: `nessie.server.default-branch=main`; UI at `/` for inspection (disable or protect in prod).

## Anti-patterns
- IN_MEMORY or ROCKSDB in production (data loss / single node)
- Single replica with no PDB
- Copying the PostgreSQL DB between versions instead of export/import for migrations
- Management port exposed through the public ingress

Sources: Nessie docs — Server configuration (version stores, datasource, authentication, authorization, catalog), Helm chart README, Tools (export/import), Releases; Bitnami/Tanzu Nessie chart docs (JDBC PostgreSQL, OIDC values).
