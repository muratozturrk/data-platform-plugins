---
name: multi-tenancy-patterns
description: Design tenant isolation, tenant context propagation, per-tenant cost and noisy-neighbour control for a data platform using silo/pool/bridge models and warehouse/lakehouse-specific patterns. Use when adding any stateful component, storage layout, compute pool, secret, queue or API that serves more than one customer, project or environment.
---

# Multi-Tenancy Patterns for a Data Platform

Even if the platform is first deployed single-tenant per customer, design it tenant-aware from day one: the AWS SaaS Lens notes that silo deployments still rely on a shared identity, onboarding and operations layer, so tenant context is needed regardless of the isolation model.

## When to Use This Skill

- Designing storage layout, catalogs, schemas, compute pools, queues, secrets
- Deciding silo vs pool vs bridge for a component
- Propagating tenant context through APIs, jobs, logs, metrics
- Cost attribution, quotas, throttling, noisy-neighbour prevention
- Compliance-driven isolation (data residency, KVKK/GDPR, SOC2)

## Core Concepts

### Isolation models (AWS SaaS Lens)
| Model | Description | Isolation | Cost/ops | Typical use |
|---|---|---|---|---|
| Silo | Dedicated resources per tenant (account/instance/db) | Strongest | Highest (3–5× pooled) | Regulated, premium tiers |
| Pool | Shared resources, logical isolation (row-level security, tenant column) | Weakest | Lowest | Default for most tenants |
| Bridge | Mix per component (e.g. shared ingestion, per-tenant storage/schema) | Medium | Medium | Most mature platforms land here |

Decide per component, not per platform. A microservice-decomposed system has many small silo/pool decisions.

### Data-platform specifics
- **Snowflake** (Snowflake design-patterns whitepaper): Multi-Tenant Table (MTT, tenant column + row access policies), Object Per Tenant (OPT, schema/database per tenant), Account Per Tenant (APT). Hybrids are common (MTT storage + per-tenant warehouse for compute isolation).
- **Databricks / Unity Catalog**: catalog per tenant bound to a tenant workspace, external locations + storage credentials per tenant, SCIM-provisioned groups; tenant identification must be established at auth time or catalog isolation can be bypassed.
- **Lake storage**: prefix-per-tenant (`s3://lake/tenants/{tenant}/...`) with IAM/ABAC session tags; never a shared prefix with tenant only in file names.
- **Catalog/metadata tools** (e.g. DataHub): domain-per-tenant for logical separation, platform-instance per tenant for physical separation, separate deployment for strict isolation.
- **Orchestration**: per-tenant pools/queues and concurrency limits; tenant tag on every task.

### Tenant context
A `TenantContext {tenant_id, project_id, environment, plan, region, tz, locale, principal}` object is created at the edge (auth), validated, and propagated explicitly through every API, job submission, plugin call, storage path resolution, secret lookup, log line and metric label. No implicit globals, no default tenant.

## Patterns

### Tenant-aware resource resolver
Core never builds a path, connection string or secret key directly; it asks the provider plugin: `storage.resolve(logical_path, ctx)`, `secrets.get(ref, ctx)`. Provider plugins implement silo/pool/bridge rules from tenant configuration.

### Onboarding as a workflow
Creating a tenant = identity + config layer + isolation resources (catalog, prefix, pool, policies) + quotas + sample data + smoke pipeline, idempotent and automated. Tier upgrade (pool → silo) is a migration workflow, not a rebuild.

### Cost and limits
Every job emits usage events with tenant, project, dimension (compute-seconds, bytes scanned, rows written, storage-bytes-days). Quotas live in plan configuration and are enforced at submission, not after the bill.

### Noisy neighbour controls
Per-tenant concurrency limits, queue fairness, separate compute pools for premium tiers, rate limits on APIs, circuit breakers on shared services.

### Testing tenancy
Every integration test runs with at least two tenants and asserts cross-tenant reads fail. Add a fitness function that scans for storage/secret access without a tenant context parameter.

## Anti-patterns
- Tenant id only as a column with no row-level policy
- Shared service accounts for all tenants
- Tenant resolved from a hostname deep inside a job instead of at the edge
- "We only have one customer now" as a reason to skip context propagation

See `references/sources.md`.
