---
name: ranger-operations-upgrades
description: Operating Ranger: health and SLIs (policy download, cache age, usersync lag, audit lag, decision latency), HA and failure modes (Admin down, DB down, audit store down), performance tuning for large policy sets, backups and restore, security advisories and patching, upgrades (2.6→2.7→2.8: audit module split, JWT plugin auth, service-def updates, engine compatibility matrix), runbooks. Use when setting up monitoring, writing runbooks or planning an upgrade.
---

# Operations and Upgrades

## SLIs and alerts
| Signal | Source | Alert |
|---|---|---|
| Plugin policy version vs latest | `/service/plugins/plugins/info`; plugin metrics | any plugin behind > 5 min |
| Policy cache age | timestamp of `policy.cache.dir/*.json` in engine pods | > 15 min |
| Admin availability/latency | probes, HTTP metrics (JMX/Prometheus exporter) | 5xx, p95 > 1 s on policy download |
| Usersync / control-plane identity sync lag | last successful sync | > 30 min |
| Audit lag and spool backlog | index time − event time; spool dir size | lag > 5 min; backlog growing |
| Deny rate by service/institution | audit store | spike vs baseline |
| DB health | PostgreSQL exporter | pool saturation, replication lag |
Engine-side decision latency is in-process (microseconds–milliseconds); regressions come from huge policy sets, heavy condition scripts or tag enrichers — watch engine planning time after large policy changes.

## Failure modes
| Failure | Behaviour | Action |
|---|---|---|
| Admin down | plugins keep enforcing cached policies; no changes propagate | restore Admin; revocations pending — treat as incident if > 15 min |
| DB down | Admin fails; same as above | DB failover |
| Audit store down | plugins spool to disk and replay | watch backlog; extend spool volume |
| Plugin cannot load cache at engine start and Admin unreachable | engine denies (fail closed) or fails to start | keep cache on persistent volume; start order |
| Identity sync stale | new users denied; removed users still in Ranger (engine token still gates) | reconcile; revoke at Keycloak first |

## Performance at scale
Prefer wildcard/tag policies (fewer, broader); avoid thousands of per-table policies; keep condition expressions simple; limit super-users; use zones to shrink evaluation sets; policy deltas (`ranger.plugin.<svc>.policy.delta` support where available) reduce download size; size Admin heap for policy export of the largest service.

## Backup and restore
State = PostgreSQL (policies, roles, zones, tags, users) + Git (desired state). pgBackRest PITR; quarterly restore drill into staging and re-point one plugin; export bundle per release as secondary backup. Audit stores have their own backup/ISM.

## Security hygiene
Track Ranger security advisories and release notes (each release lists security fixes, e.g. XXE and dependency CVEs); patch Admin within the platform's SLA; rotate admin/service passwords and keystores; restrict Admin network exposure; scan images (Trivy); keep plugin client libraries aligned with Admin's minor version.

## Upgrade procedure
1. **Triage**: release notes for breaking changes — 2.7: audit module refactor, plugin packages bundle only Solr/HDFS audit libs, usersync default `ldap.referral=follow`; 2.8: `authz-api`/`authz-embedded`, inline policies, hadoop-common removed from plugins, JWT plugin↔Admin auth, Kafka 3.9.x. Check engine compatibility: Trino version's Ranger authorizer vs Admin service-def; Kafka broker version vs plugin; Kyuubi AuthZ vs Ranger client version.
2. **Rehearse** on a DB copy in staging: run the new Admin's DB patches (Job), compare policy export before/after (no semantic drift), run the decision-table suite through each engine with new plugin libs.
3. **Roll**: Admin first (plugins are backward compatible within supported ranges), then plugins engine by engine (rebuild images with matching audit-destination modules), then enable new features (JWT auth) deliberately.
4. **Verify**: plugin info versions, audit flow, deny rate, two-institution tests.
5. **Rollback**: previous Admin image + DB restore (DB patches are forward-only); plugins can stay if compatible.

## Runbooks (index)
Access wrongly denied (debug checklist in identity skill) · Access wrongly allowed (BLOCKER: revoke at Keycloak, fix policy, prove propagation, audit search for exposure window) · Plugin not syncing · Audit backlog · Admin DB restore · Emergency read-only mode (override deny policy for writes).

## Anti-patterns
- Upgrading plugins and Admin in one step without rehearsal
- No monitoring of plugin policy versions (revokes assumed, not proven)
- Policy cache on ephemeral storage
- Skipping release notes because "it's only a minor"

Sources: `references/sources.md` (Ranger 2.7 and 2.8 release notes and news; platform baseline minimum 2.8.0, target 2.9.0, conflicts #41; plugin info API, operations notes from community deployments).

Details and book-derived rules: `references/security-operations.md`.
