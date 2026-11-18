---
name: keycloak-deployment-kubernetes
description: Running Keycloak 26.x on Kubernetes for the HKM platform - Keycloak Operator and the Keycloak CR (k8s.keycloak.org/v2beta1), optimized container image with providers and themes, PostgreSQL via CloudNativePG, hostname/reverse proxy/TLS, management interface (health, metrics), sizing from login and token rates, single-cluster HA across zones, multi-cluster v2 (stateless), rolling vs recreate updates, network policies and resource settings. Use when deploying, sizing or reviewing a Keycloak installation.
---

# Keycloak on Kubernetes

## Shape of the deployment
- **Operator** (OLM, kustomize or Helm chart for the operator — the Helm chart is experimental in 26.8) manages the `Keycloak` CR; CRDs `Keycloak` and `KeycloakRealmImport` are `v2beta1` since 26.6. The legacy Realm Operator is end-of-life (26.8).
- **Database**: PostgreSQL 14–18 supported (13 removed in 26.5); use CloudNativePG with 3 instances, synchronous replica, PITR backups. Keycloak must always talk to the primary (JDBC `targetServerType=primary`; PgBouncer needs `targetServerType=any`). Create the database with UTF8 encoding (startup check since 26.6).
- **Image**: build an optimized image (`kc.sh build` with `db=postgres`, features, health/metrics) that bakes in providers and themes; run with `startOptimized: true`. Pin the exact Keycloak version.
- **Network**: central ingress terminates TLS and re-encrypts to Keycloak (no plain internal hop; HKM D21); TLS 1.2 minimum with forward-secret AEAD suites, TLS 1.3 only once every client is confirmed; public certificate only for `sso.<domain>`, private CA for internal traffic. Set `proxy.headers: forwarded` with `proxy-trusted-addresses` limited to the ingress, and make the ingress overwrite forwarding headers. One public hostname (`sso.<domain>`); expose `/admin` only on an internal hostname or network (`hostname.admin`) and block `/admin/` and `/realms/master/` at the public ingress.
- **Management interface** on port 9000: `/health/ready`, `/health/live`, `/metrics` — never expose publicly; scrape with a ServiceMonitor (labels configurable in the CR since 26.6).

## Sizing (Keycloak HA guide and 26.4 benchmark)
| Load driver | 1 vCPU handles |
|---|---|
| Password logins (hashing dominates) | ~15 logins/s |
| Client-credentials grants | ~120 grants/s |
| Refresh-token requests | ~120 refreshes/s |

- Memory: ~1250 MB per pod including realm caches and 10 000 cached sessions; pod limit ≈ (1250 − 300) / 0.7 ≈ 1360 MB with default heap settings.
- Add **150 % CPU headroom** over measured steady load for spikes, start-up and failover; never let pods be CPU-throttled.
- Database: ~1400 write IOPS and 0.35–0.7 vCPU per 100 login/logout/refresh per second (sessions persisted).
- More than ~2500 concurrently used clients: raise `users` cache to 2× and `realms` cache to 4× the client count.
- Benchmark (26.4, 3 pods, Aurora PostgreSQL): 500 logins + 2500 refreshes/s with 24 vCPU limit per pod; scaling is near-linear; keep all nodes in one region (latency kills p99).
- HKM estimate (starting point, validate with Keycloak Benchmark): 30 institutions × ~200 concurrent staff → peak ~10 logins/s, ~80 refreshes/s, ~200 client-credentials/s from jobs → 3 pods × 2 vCPU request / 5 vCPU limit, 2 GiB memory; DB 2–4 vCPU.

## High availability
- **Per cluster, multi-zone**: ≥3 pods spread with `whenUnsatisfiable: DoNotSchedule` topology constraints, PodDisruptionBudget `minAvailable: 2`, embedded Infinispan with the `jdbc-ping` stack; sessions are persisted in the database (default since 26), so a restart does not log users out; no sticky sessions.
- **Multi-cluster v2** (`--features=stateless`, supported in 26.8, disabled by default) is the platform model (HKM D21): clusters without external Infinispan, all inside one region and on premises (HKM D25), synchronous DB replication with commit latency below 10 ms, clock skew within 10 s, a load balancer probing `/lb-check`. Multi-cluster v1 (`multi-site`) is deprecated.
- Updates: `spec.update.strategy: Auto` (HKM D21) lets the operator run rolling updates for compatible patch releases (zero-downtime patch updates supported since 26.6) and choose a recreate otherwise; minor upgrades (26.7 → 26.8) require a full shutdown — plan a window, take a DB backup first; rollback only by restoring it (HKM D42).

## Must-set options
| Option | Value | Why |
|---|---|---|
| `hostname` | `https://sso.<domain>` | stable issuer; tokens are bound to it |
| `hostname-admin` | internal URL | keep admin console off the internet |
| `proxy-headers`, `proxy-trusted-addresses` | `forwarded`; ingress IPs/CIDRs only | correct client IP for brute force and events; spoofed headers ignored |
| `http-enabled` | false | re-encrypt only (HKM D21) |
| `https-protocols` | `TLSv1.3,TLSv1.2` | TLS 1.2 minimum, AEAD and forward secrecy |
| `db-tls-mode` | `verify-server` | database credentials and data in transit |
| `health-enabled`, `metrics-enabled` | true | probes and Prometheus |
| `cache-embedded-cluster-name` | `hkm-<env>` | 26.8: name required for `stateless`; avoids accidental joins |
| `log-console-output` | json | shipping to OpenSearch |
| `truststore-paths` / CR `truststores` | institution IdP and LDAP CAs | brokering and LDAPS |

## Anti-patterns
- Running `start-dev` or the H2 database anywhere but a laptop.
- Exposing port 9000 or `/admin` through the public ingress.
- Sticky sessions based on `AUTH_SESSION_ID` (deprecated 26.8) instead of letting Keycloak route.
- Treating Keycloak as stateless and skipping DB backups.

## Examples and references
- `examples/deploy/keycloak-cr.yaml`, `examples/deploy/cnpg-cluster.yaml`, `examples/deploy/Dockerfile`
- `references/sizing-and-ha.md` — merged rules: production configuration, database and caches, clustering/HA/DR, reverse proxy/hostname/TLS, performance and load testing.
