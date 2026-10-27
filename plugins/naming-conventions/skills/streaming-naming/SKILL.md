---
name: streaming-naming
description: Naming for Kafka topics, schema-registry subjects, consumer groups, connectors, Flink/Kafka Streams jobs and dead-letter topics in a multi-tenant platform. Use when creating a topic or stream job, reviewing a Strimzi/Connect manifest, or designing topic ACL prefixes.
---

# Streaming Naming (Kafka and friends)

## When to Use This Skill
- Creating or reviewing a topic, subject, consumer group, connector or stream job
- Designing prefix ACLs and quotas for a tenant
- Deciding where version, environment and tenant live in a topic name

## Core Concepts

### Hard limits
Kafka topic names: ≤ 249 characters, `[a-zA-Z0-9._-]`; `.` and `_` collide in metric names, so **use one separator for hierarchy (`.`) and never mix `.` and `_` in the same position**. Names are case-sensitive; use lowercase only.

### Pattern
```
{tenant}.{domain}.{entity}.{event-or-kind}.v{n}
t-acme.sales.order.created.v1
t-acme.sales.order.cdc.v1            # Debezium CDC stream
t-acme.sales.order.created.dlq.v1    # dead-letter for the same contract
```
- `tenant` first: Kafka's multi-tenancy model is prefix = user space; a prefix ACL (`KafkaUser` with `patternType: prefix`) and quotas then cover the whole tenant with one rule.
- `domain` from the glossary (bounded context), `entity` singular, `event` past-tense verb for domain events, `cdc`/`snapshot`/`cmd`/`log` for kinds.
- `v{n}` is the **contract** version: bump on incompatible schema change; compatible evolution stays on the same topic (schema registry compatibility mode).
- **No environment in topic names**: environments are separate clusters or virtual clusters; mixing `prod`/`dev` into names forces code to know the environment.

### Related names
| Object | Pattern | Example |
|---|---|---|
| Schema subject | `{topic}-value` / `{topic}-key` (TopicNameStrategy) | `t-acme.sales.order.created.v1-value` |
| Consumer group | `{tenant}.{consumer-app}.{purpose}` | `t-acme.fraud-scorer.order-created` |
| Connector | `{tenant}.{source|sink}.{system}.{entity}` | `t-acme.source.erp-postgres.order` |
| Stream job | `{tenant}.{domain}.{job-purpose}` | `t-acme.sales.order-enrichment` |
| DLQ | `{topic-without-version}.dlq.v{n}` | as above |
| Compacted state topic | `{tenant}.{domain}.{entity}.state.v{n}` | |
| Internal/changelog | leave to the framework (`-changelog`, `-repartition`) | |

### Headers and keys
Message key = entity id (`order_id`) for ordering; standard headers `tenant_id`, `schema_version`, `trace_id`, `produced_at` with these exact names.

## Patterns
- **Tenant prefix is enforced at creation**: an admission policy or topic operator rejects topics outside the tenant's prefix.
- **Per-tenant monitoring** rolls up by prefix; names that break the pattern break the dashboards — make the pattern a fitness function.
- **Rename = new topic**: plan with `/rename-plan` (mirror, migrate groups, retire).

## Anti-patterns
- `UserEvents`, `ORDER_QUEUE`, `Payment.Events` (mixed case/separators)
- Team names (`team-blue.orders`) — re-orgs break ownership encoded in names; use domain
- `prod-orders`, `orders-final`, `orders-new`
- Version omitted on topics that carry a schema contract

See the Kafka multi-tenancy chapter and Confluent/Conduktor naming guides in the plugin README sources.
