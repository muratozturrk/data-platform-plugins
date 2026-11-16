# apache-ranger (v1)

Apache Ranger (minimum 2.8.0, target 2.9.0) as the access-policy and audit plane of the HKM open-source data platform. v1 is built from project documentation and release notes, engine docs (Trino, Strimzi/Kafka, Kyuubi), standards (NIST ABAC, ISO 27001, KVKK), reports and community deployments; v2 folds in the book summaries kept in the surrounding workspace (`summaries/` → `synthesis/` → skills).

## Layout
```
agents/       ranger-platform-engineer · ranger-policy-author · ranger-config-reviewer
commands/     /ranger-platform-design · /ranger-policy-compile · /ranger-service-onboard · /ranger-audit-review · /ranger-config-review
skills/       8 skills (below)
examples/     deploy/ · plugins/{trino,kafka,spark}/ · policies/ · automation/ · tests/
references/   sources.md · standards.md · books.md
skills/*/references/   rule files filled from book summaries (Azure-style RBAC/ABAC model, role engineering, attributes, audit, KVKK, engine notes)
```

| Skill | Purpose |
|---|---|
| `ranger-architecture-policy-model` | Components, PAP/PDP/PEP/PIP view, service-defs, policy types and items, evaluation order, tags, zones, where each rule belongs |
| `ranger-deployment-kubernetes` | Images and chart reality, topology, install.properties, TLS, HA, secrets, sizing, environments |
| `ranger-identity-usersync` | Keycloak/LDAP → Ranger users, groups, roles, attributes; Admin UI login; debug checklist |
| `ranger-policy-design-abac-masking` | Layered policy set, RBAC vs ABAC, masking and row-filter patterns, purpose/time-bound access, naming, decision tables |
| `ranger-engine-plugins` | Coverage map for the HKM stack; Trino, Kafka on Strimzi, Spark via Kyuubi AuthZ; native compilation for engines without plugins; tags from OpenMetadata |
| `ranger-audit-compliance` | Access/admin audits, destinations and 2.7 packaging, spooling, OpenSearch + immudb pipeline, retention, KVKK/ISO/NIST mapping, reports and alerts |
| `ranger-api-policy-as-code` | REST API v2, Python client, compile → validate → test → plan → apply → verify → drift, tag push, policy testing |
| `ranger-operations-upgrades` | SLIs/alerts, failure modes, performance, backup, advisories, upgrade procedure 2.6→2.8, runbooks |

## Authorization model
The model follows Azure RBAC/ABAC concepts (role definition, scope, role assignment, condition, deny as guardrail, time-bound elevation, access reviews) with Ranger as the runtime; see `skills/ranger-policy-design-abac-masking/references/azure-style-rbac-abac.md`.

## How it fits the other plugins
`trino` (engine side of the Ranger config), `project-nessie` (catalog CEL rules compiled from the same Policy), `platform-engineering` (Policy manifest and Tenant controller), `qa-test-strategy` (isolation, mask-consistency and audit tests; this plugin's `examples/tests` implement T-ranger-* and T-trino-00x), `apache-superset` (impersonation path and RLS as defence in depth), `naming-conventions` (policy, zone, service names).
