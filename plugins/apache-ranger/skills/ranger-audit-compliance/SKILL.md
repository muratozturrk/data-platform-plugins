---
name: ranger-audit-compliance
description: Ranger auditing for compliance on the platform: access audits from plugins and admin audits from Ranger Admin, audit destinations (Elasticsearch-compatible/OpenSearch, Solr, HDFS/S3, Kafka, Log4j) and the 2.7+ packaging change, spooling and loss prevention, audit filters, pipeline to OpenSearch for search and immudb for tamper evidence, retention, KVKK/GDPR evidence reports, access reviews and alerting. Use when designing the audit pipeline, answering auditors, or investigating access.
---

# Audit and Compliance

## What Ranger records
- **Access audit** (per decision, from the plugin): event time, service/repo, `reqUser`, resource path and type, access type, result (allowed/denied), **policy id and version** that decided, tags matched, client IP, cluster/zone name, session/request id (Trino query id), enforcer (`ranger-acl`), additional info.
- **Admin audit** (from Ranger Admin): who created/updated/deleted policies, roles, users, services, zones; logins; export/import; plugin policy-download status (which plugin has which policy version — the proof that a revoke propagated).

## Destinations and packaging
Plugins can write to Solr, Elasticsearch-compatible stores, HDFS/S3-compatible files (JSON, ORC), Kafka, Log4j, CloudWatch. From 2.7 the audit module is split into per-destination modules and plugin packages include only Solr and HDFS libraries by default — add the module for your destination (e.g. Elasticsearch, Kafka) to each plugin image. Trino's built-in Ranger plugin documents Solr/Elasticsearch/S3/HDFS audit stores; confirm which destination libraries the Trino version bundles.
Platform design:
```
plugins ──(primary)──> OpenSearch index ranger-audit-{service}-{yyyy.MM}   (Admin audit UI, dashboards, alerts)
        └─(secondary)─> Kafka topic platform.audit.ranger.v1 ──> immudb ledger (hash-chained evidence) + long-term WORM archive
Ranger Admin admin-audit (DB) ──> exporter job ──> same Kafka topic
```
If the Ranger Elasticsearch client is incompatible with the platform's OpenSearch version, use Kafka as the plugin destination and index into OpenSearch with Kafka Connect, keeping a small Solr only if the Admin audit UI is required.

## Never lose audits
`xasecure.audit.destination.<dest>.batch.filespool.dir` on persistent storage (plugin spools to disk when the destination is down and replays); queue/batch sizes tuned for peak; alert on spool backlog, on audit lag (event time vs index time), and on **gaps**: daily reconciliation of Trino query-completed events (event listener) against Ranger access audits by query id.

## Audit filters and volume
Ranger audit filters (per service) can drop noise (e.g. metadata listing by service accounts) — use sparingly and never for denied events or PII-tagged resources. Kafka can generate very high volumes: audit `deny` always, `allow` for produce/consume at topic-open granularity if supported, otherwise sample via filter rules agreed with compliance.

## Retention
OpenSearch ISM: hot 30–90 days, warm to 1 year, delete per KVKK retention schedule; immudb/WORM archive retains the legally required period; the OPA retention engine owns the schedule; purge of audit data itself is audited.

## Compliance mapping
| Requirement | Evidence from Ranger |
|---|---|
| KVKK art. 12 technical measures: access control, logging | policy set in Git + Ranger; access/admin audits; plugin status (propagation) |
| Purpose limitation, data minimisation | tag policies and masks; audit shows mask/row-filter policy ids applied |
| Access review (periodic) | report: grants per group per dataset, unused policies, last access per subject |
| Data-subject access/erasure support | who accessed subject-related datasets (dataset-level; row-level needs engine query text) |
| Segregation of duties | zone admins vs platform admins vs auditors (read-only audit role) |
| ISO/IEC 27001 A.5.15–5.18, A.8.2–8.5, A.8.15 | same artefacts |
| NIST SP 800-53 AC-2/3/6, AU-2/3/6/9/12 | same; immudb for AU-9 (protection of audit information) |

## Reports and alerts
Dashboards: denies by policy/institution, PII access by subject, impersonation events, policy changes timeline, plugin policy-version lag. Alerts: deny spike, access to `RESTRICTED` outside allowed role, policy change outside CI principal, audit pipeline lag/backlog, plugin not downloading policies > 5 min.

## Anti-patterns
- Audit only to local Log4j files in pods
- No spool directory (silent loss during outages)
- Filtering out allowed events on sensitive data to save space
- Auditors given Admin rights instead of a read-only audit view

Sources: `references/sources.md` and `references/standards.md` (Ranger audit configuration docs, 2.7 release notes, Trino Ranger docs, KVKK technical-measures guide, ISO/IEC 27001:2022, NIST SP 800-53r5).

Details and book-derived rules: `references/audit-design.md`, `references/compliance-kvkk.md`.
