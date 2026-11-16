---
name: ranger-architecture-policy-model
description: How Apache Ranger works: Admin, usersync, tagsync, plugins and the policy download/cache/enforce/audit loop; service definitions and services; the policy model (access, data-masking and row-filter policies; allow/deny items and exceptions; conditions; validity schedules; priority; roles; delegate admin; labels), tag-based policies, security zones, evaluation order, and Ranger's place next to OPA, engine-native ACLs and catalog authorization (PAP/PDP/PEP/PIP view). Use when explaining Ranger behaviour or deciding where a rule belongs.
---

# Ranger Architecture and Policy Model

## Architecture (XACML/NIST vocabulary)
| Role | Ranger component | Notes |
|---|---|---|
| PAP (policy administration) | Ranger Admin (UI + REST), backed by an RDBMS | on the platform the real PAP is Git + control plane; Admin is the runtime store |
| PDP + PEP (decision + enforcement) | plugin inside each engine | in-process, no network hop per decision; policies downloaded every `policy.pollIntervalMs` (default 30 s) and cached on disk |
| PIP (attributes) | usersync (users/groups/attributes), tag store (tags on resources), request context from the engine (user, groups, client IP, query) | |
| Audit | plugin audit providers → Solr/Elasticsearch-compatible/HDFS-S3/Kafka/Log4j; Admin audit for policy changes | |
Consequences: enforcement continues when Admin is down (cached policies); policy changes take up to the poll interval; a plugin decides only for its own engine.

## Service definitions and services
A **service-def** describes an engine type: resource hierarchy (Trino: catalog → schema → table → column, plus `trinouser`, `systemproperty`, `sessionproperty`, `function`, `procedure`, `queryid`…; Kafka: `topic`, `consumergroup`, `transactionalid`, `cluster`, `delegationtoken`), access types (select/insert/… ; publish/consume/…), mask types, row-filter support, condition evaluators, context enrichers. A **service** is one engine cluster's policy set (`trino-interactive`, `kafka-main`). A **tag service** holds tag policies and is linked to resource services.

## Policy model
- **Policy types**: 0 access, 1 data-masking, 2 row-filter.
- **Policy items**: `policyItems` (allow), `allowExceptions`, `denyPolicyItems`, `denyExceptions`; each item = users/groups/roles + accesses (+ conditions, `delegateAdmin`).
- **Evaluation** for a request: tag policies first, then resource policies; in each: matching **deny** (unless a deny-exception matches) → denied; else matching **allow** (unless an allow-exception matches) → allowed; else undetermined → next policy; nothing allows → denied (or the engine's native fallback if enabled — disable it).
- **Priority**: `override` policies beat `normal`; `isDenyAllElse` turns a policy into a whitelist for its resource.
- **Conditions**: custom evaluators from the service-def (IP range, expression over `USER`/`TAG`/`REQ` attributes, tag expiry `accessed-after-expiry`, time validity via `validitySchedules`).
- **Macros**: `{USER}`, `{OWNER}` in resources; `${{USER.attr}}`, `${{TAG.attr}}`-style expressions in conditions/row filters where supported by the plugin version.
- **Masking**: mask types (`MASK`, `MASK_SHOW_LAST_4`, `MASK_SHOW_FIRST_4`, `MASK_HASH`, `MASK_NULL`, `MASK_NONE`, `MASK_DATE_SHOW_YEAR`, `CUSTOM` expression); items are ordered, first match wins.
- **Row filter**: SQL predicate per item; first match wins; an empty filter for a privileged group means "all rows".
- **Roles**: Ranger roles group users/groups/roles; policies can target roles (use for function-level roles; keep institution membership in groups).
- **Labels, descriptions, `guid`**: stable ids for policy-as-code.

## Tag-based policies (ABAC)
Tags (classification + attributes) are attached to resources in the tag store (from Atlas via tagsync, or pushed through the REST API — the platform pushes OpenMetadata classifications). A tag policy (`tag = PII`) applies to every resource carrying the tag across all linked services: one deny/mask rule instead of one per column. Tag attributes enable conditions (expiry date, sensitivity level, legal basis).

## Security zones
A zone = named set of resources per service (e.g. catalog `t_acme_lake`, topic prefix `t.acme.*`) with its own admins and policies; a resource belongs to at most one zone; zone policies apply only inside it; unzoned policies do not leak into zones. This is the platform's institution boundary for administration.

## Where a rule belongs
| Rule | Mechanism |
|---|---|
| Institution may read its gold schema | resource policy in the institution zone |
| Nobody but `pii-reader` sees IBAN anywhere | tag-based masking policy on `PII` |
| Shared table rows fenced per institution | row-filter policy in `shared` zone |
| Purpose limitation (fraud analysis only) | tag policy with condition on `purpose` attribute / dedicated role |
| Retention, archive, purge | OPA retention engine (not Ranger) |
| Branch/merge rights on catalog | Nessie CEL rules (compiled from the same Policy) |
| Metadata visibility in catalog UI | OpenMetadata RBAC |

## Anti-patterns
- Treating the Admin UI as the source of truth
- Per-user policies; one policy per table when a wildcard or tag would do
- Relying on native ACL fallback
- Assuming Ranger protects files in object storage (it protects the engine path only)

Sources: see `references/sources.md` (Ranger wiki: architecture, policy model, tag-based policies, security zones; NIST SP 800-162; XACML 3.0).

Details and book-derived rules: `references/access-control-models.md`.
