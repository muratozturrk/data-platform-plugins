# Audit design and access review

Status: starter rules; access review process filled from the Azure RBAC and ABAC documentation (2026-11-14), NIST SP 800-162 (2026-11-15) and Authorization in Action (2026-11-15); retention partly filled from NIST; record fields, monitoring, governance metrics and compliance evidence filled from Authorization in Action (2026-11-15); audit fields, filters and exclusions from the Apache Ranger wiki and lab V5 (merged 2026-11-15); what to audit per engine, policy-change audit, engine record fields and alert rules from Hadoop Security (merged 2026-11-15); separate retention of allowed and denied decisions and authentication-failure alerts from Kafka: The Definitive Guide, 2nd ed. (merged 2026-11-15); authenticated principal and query-ID uniqueness from Trino: The Definitive Guide, 2nd ed. (merged 2026-11-16); access-request audit, regulator evidence and lineage evidence, monitoring independence, actionable alerts and the compliance-monitoring baseline from Data Governance: The Definitive Guide (merged 2026-11-16); owner-answered access reviews including cross-institution agreements from Data Management at Scale, 2nd ed. (merged 2026-11-16); incident evidence snapshots, recorded alert handling and KVKK markers from the KVKK Personal Data Security Guide (merged 2026-11-16); dataset address, timestamps and access endpoint in the decision schema and audit traced to the golden source from Data Mesh (merged 2026-11-16).

## Starter rules

1. Audit both allow and deny with the deciding policy ID and version, the zone, the requesting user or service-client ID, its groups, the action, the resource and the tags evaluated, also when the decision used only attributes; a decision without a deciding policy (default deny, Ranger super user) carries policy ID -1. Why: evidence needs the reason, an explanation needs the deciding policy rather than every matching one, the failed attempt next to the successful one is what makes the log useful, and accountability is lost when a decision cannot be traced to a principal. (plugin v1; NIST SP 800-162 §2.3, §3.2.2.2; Authorization in Action ch. 1.4.3, 4.3.2, 4.4.1, 10.2.3, 13.5.8; Apache Ranger wiki p. 10; Hadoop Security ch. 1, 8; Kafka DG ch. 11; DMaS ch. 8; KVKK guide §3.2; trial: verification lab V5, 2026-11-14; decision 2026-11-15, conflicts #25; conflicts #18.)
2. Spool to disk when the destination is down. Why: no silent loss.
3. Auditors get a read-only audit role, never admin. Why: separation of duties.

### Compliance evidence from policy work

Source: Authorization in Action, ch. 14 (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 2, 6, 9 (merged 2026-11-16).

1. Tag every policy in the manifest with compliance metadata (for example `kvkk:personal-data`), link the controls it implements in its ADR, and block deployment when a compliance test on a tagged path fails. Why: compliance becomes a continuous check instead of a separate exercise. (Authorization in Action ch. 14.5.)
2. Build audit evidence from normal work (ADRs, tagged policies, Git history with reviews, CI results, decision logs and review-board minutes), show it to Auditors on a read-only dashboard, and keep it continuously, so that a regulator can be shown per data class who may access it together with audit-log proof that this was enforced, and an external audit finds nothing unexpected. Why: evidence produced by the process needs no separate collection, and regulators ask for both the rule and the proof of its enforcement. (Authorization in Action ch. 14 p. 282–283; Data Governance DG ch. 2, 9.)
3. Answer from OpenMetadata lineage together with the Ranger audit which systems and datasets process data tagged `PII`, and show the unbroken chain from approved sources to each regulated output; use the same view for continual internal audit. Why: regulators require explainable outputs and a list of every system that processes personal data. (Data Governance DG ch. 6.)

### Audit filters and exclusions

Source: Apache Ranger wiki, p. 9 Ranger Audit Filters (2025), Blacklist for Ranger Audits (2020), service-user UI exclusion (2019), p. 14 Release notes 2.8.0 (merged 2026-11-15); lab V5; Hadoop Security, ch. 8 (merged 2026-11-15).

1. Keep each service's default audit filters, and express any further reduction only as audit filters in that service's configuration, generated from Git together with the service; never edit filters in the UI. Why: filters are simplified policy JSON in each service config, and the defaults write allowed and denied decisions (Trino's default drops only allowed `execute` on `queryid`). (Apache Ranger wiki p. 9; trial: verification lab V5, 2026-11-14; conflicts #18.)
2. Start every service's filter list with a rule that audits denied results. Why: later exclusions then cannot hide a denial, and volume filtering must never drop denied events. (Apache Ranger wiki p. 9; Hadoop Security ch. 8.)
3. Let a filter exclude only a declared service principal together with a named access type or action (today only the KRaft controller's `cluster_action`); never a human user or group, a Ranger super user, a governed resource, or a `PII` or `RESTRICTED` tag, and put tag-based filters only in the tag service's configuration. Why: a broad filter silently removes accountability, super-user allows with policy -1 are the alert signal, the resource exclusions added in 2.8 hide allows and denies alike, and sensitive access must stay traceable. (Apache Ranger wiki p. 9, p. 14; trial: verification lab V5, 2026-11-14; conflicts #18.)
4. Leave the plugin exclusion lists (`ranger.plugin.audit.exclude.users`, `ranger.plugin.audit.exclude.groups`) empty or limited to declared service principals. Why: an excluded principal produces no audit record at all in that plugin instance. (Apache Ranger wiki p. 9.)
5. Treat the Ranger Admin UI's service-user exclusion only as a viewing aid, never as evidence that records are absent. Why: the records are still written to the store; only the view hides them. (Apache Ranger wiki p. 9.)
6. Review every filter and exclusion per service, with its written reason, in the periodic audit review. Why: an exclusion applies only to the plugin instance whose configuration holds it. (Apache Ranger wiki p. 9.)

Example (rewritten, Kafka service audit filter list):
```json
[
  { "accessResult": "DENIED", "isAudited": true },
  { "users": ["kraft-controller"], "accessTypes": ["cluster_action"], "isAudited": false,
    "description": "KRaft controller heartbeat noise, conflicts #18" }
]
```

### What to audit per engine and why

Source: Hadoop Security, ch. 8, Afterword (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 1 (merged 2026-11-16); Data Mesh, ch. 14 (merged 2026-11-16)

1. Design audit for three uses and check each: active alerts on selected events (Security monitoring, rule 5), a queryable history for investigation, and compliance evidence kept under the retention for personal data (Retention, rule 1). Why: these are the three reasons accounting exists, and without accounting breaches go unnoticed. (Hadoop Security ch. 8.)
2. Audit Trino per statement and per object checked, and Kafka per topic and consumer-group operation, through each engine's Ranger plugin, and treat the plugin audit as the record of every authorization decision; Trino Gateway logs and the object store's access log are not authorization audit, because they see only routing or the engines' own credentials, never the end user's decision. Why: query engines and storage produce different auditable events, and only the plugin sees the end user and the deciding policy. (Hadoop Security ch. 8.)
3. Keep two audit streams, distinguishable in the store and both included in the access review: data-access decisions from the plugins, and changes to authorization (access requests with their use case, approver and approval time from Flowable; Git pull requests with author and approver; Ranger Admin's audit of policy, role, zone and service changes, refused change attempts included), each change record carrying user, client IP, operation, operation text, target and outcome. Why: policy changes show who widened access, a refused change attempt is a signal of its own, and compliance has to prove who asked for each grant, who approved it and every access that followed. (Hadoop Security ch. 8; Data Governance DG ch. 1; conflicts #6.)
4. Write audit as structured events to the dedicated audit destination (OpenSearch and immudb), never only into engine service logs, and audit metadata operations (listing catalogs, schemas and tables, describing a table) as well as data reads. Why: audit buried in service logs is hard to extract, and listings reveal which objects exist. (Hadoop Security ch. 8.)
5. Collect audit from every engine instance into the central store, add alert rules there where an engine has none, and link audit records to the governance metadata in OpenMetadata (tags, lineage, retention class) and, through lineage, to the golden source of the data (the operational system where it was first recorded), so that an investigation sees what kind of data was touched and where it came from. Why: decisions are spread over nodes and engines, audit without governance context is the gap the book names, and root-cause analysis has to reach beyond the datasets into the systems that produced the data. (Hadoop Security ch. 8, Afterword; Data Mesh ch. 14.) Auditor query interface: Retention and protection of audit data, rule 4 (conflicts #47).

(The rewrite also replaces the stale note "the query interface for Auditors waits for Conflict H2" with the pointer to decided #47; no new content.)

### Audit record fields and correlation

Source: Authorization in Action, ch. 4, 6, 12, 13 (merged 2026-11-15); Apache Ranger wiki, p. 4, p. 9, p. 10 (merged 2026-11-15); Hadoop Security, ch. 8 (merged 2026-11-15); Trino: The Definitive Guide, 2nd ed., ch. 12 (merged 2026-11-16); Data Mesh, ch. 14 (merged 2026-11-16)

1. Make each decision traceable to its inputs: from the audit record's user, groups, tags and time, the attribute values used and their source and age must be reconstructable from Keycloak admin events, OpenMetadata tag history and the usersync and tag-sync run times. Why: multi-input decisions cannot be explained otherwise, and audits need the provenance of every attribute. (Authorization in Action ch. 6.2.3, 12.6.2.)
2. Use one decision-log schema for every engine, mapped onto the Ranger audit schema with `result`, `policy`, `policy version`, `zone_name` and `tags` as mandatory fields, plus the dataset's canonical scope path (its logical address from the catalog, azure-style-rbac-abac.md › Scope design and inheritance, rule 3), the event time and the time the store received the record, and the access endpoint (engine and service), and send all decision logs through the same pipeline to OpenSearch and immudb. Why: audit across engines needs consistent logs, hand-built per-engine logging drifts, these fields say which policy, zone and tag decided, and structured records carrying the dataset address and both timestamps allow analysis across datasets. (Authorization in Action ch. 4 p. 84, 13.5.4; Apache Ranger wiki p. 10; Data Mesh ch. 14.)
3. Match Ranger access types in audit queries and SIEM rules on the `action` field, never on `access`. Why: the Trino plugin writes its own operation name (for example `SelectFromColumns`) to `access` and the Ranger access type to `action`, the reverse of the wiki's description, while Kafka writes the access type to both. (Apache Ranger wiki p. 9, p. 10; trial: verification lab V5, 2026-11-14; conflicts #18.)
4. Scope each institution Auditor's audit view and each zone's access review by `zone_name`. Why: the field names the zone whose policy authorized the request, and zone auditors see only their zone's audits. (Apache Ranger wiki p. 4, p. 10.)
5. Let every engine decision record carry the end user taken from the authenticated principal or an authorized impersonation (trino-security.md › Paths beside the Trino plugin, rule 2), never from a client-set name such as Trino's `--user` or `--source`, the impersonating service when one acted for the user (Superset), client IP, session and query ID, with a Trino query ID always kept together with the coordinator that issued it and the time, statement type and statement text (`reqData`, held only in OpenSearch under Retention, rule 3), the objects touched with the access type checked, and the result; keep end user and impersonating service in separate fields and never record an impersonated query only under the service's name. Why: these fields reconstruct who accessed what, losing either identity breaks traceability, client-set names can be anything, and a Trino query ID's counter and coordinator part reset on restart. (Hadoop Security ch. 8; Trino DG ch. 12; conflicts #30, #39.)

Example (rewritten, normalised decision record):
```json
{
  "evtTime": "2027-02-03T10:15:22Z",
  "ingestTime": "2027-02-03T10:15:24Z",
  "endpoint": "trino:trino-shared",
  "user": "alex",
  "groups": ["platform_t-acme_analyst"],
  "resource": "t_acme_lake.gold_sales.customers.email",
  "scopePath": "/institutions/t-acme/domains/sales/datasets/customers",
  "action": "select",
  "result": "ALLOWED",
  "policy": "tag-mask-pii",
  "policyVersion": 7,
  "zone_name": "t-acme",
  "tags": ["PII"]
}
```

### Retention and protection of audit data

Source: NIST SP 800-162, §3.2.1.6 (merged 2026-11-15); Apache Ranger wiki, p. 10 (merged 2026-11-15); Kafka: The Definitive Guide, 2nd ed., ch. 11 (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 1, 8 (merged 2026-11-16); KVKK Personal Data Security Guide, §3.2 (merged 2026-11-16).

1. Treat the audit store as personal data: define its retention period, and restrict read access to the Auditor role and the platform security team. Why: grant and deny patterns reveal principal attributes, and the Trino plugin writes the full SQL statement, literals included, to `reqData`. (NIST SP 800-162 §3.2.1.6; Apache Ranger wiki p. 10; Data Governance DG ch. 1; trial: verification lab V5, 2026-11-15; decision 2026-11-15, conflicts #30.)
2. Set audit retention in the platform's retention policy (OPA) and not through the audit sink's time-to-live fields, and let that policy give allowed and denied decisions their own retention periods where they differ. Why: retention is the one policy the platform gives to OPA, a TTL in the sink configuration escapes that review, and the book keeps allowed and denied decisions under separate retention because both serve attack detection and audits. (Apache Ranger wiki p. 10; Kafka DG ch. 11.)
3. Send to immudb the decision fields (event ID, time, user, groups, resource, access type, result, policy ID and version, zone, tags), never the statement text; `reqData` stays only in OpenSearch under the retention and access limits of rule 1. Why: immudb keeps tamper-evident evidence of the decision, while the statement text is personal data with its own retention. (Apache Ranger wiki p. 10; decision 2026-11-15, conflicts #39; conflicts #30.)
4. Keep OpenSearch as the audit store of record and immudb outside the lake, and run both, together with the alerting, independently of the engines they watch, with failover; give Auditors and the platform security team a read-only SQL interface over the OpenSearch audit index, and never load audit records into the lake or expose them through Trino. Why: audit stored in the system it audits can be read or altered by the principals it records, and an outage of a production system must not blind its monitoring. (Hadoop Security ch. 8; Data Governance DG ch. 8; decision 2026-11-15, conflicts #47; conflicts #30.)
5. When an incident opens, snapshot the audit records of the affected scope and time window together with the policy state (Git commit, Ranger policy versions), anchor the snapshot's hashes in immudb with an ID recorded in the incident ticket, keep the statement text in OpenSearch only (rule 3), and never prune, filter or edit the audit of that scope while the investigation runs. Why: KVKK requires incident evidence to be collected and stored securely, and evidence that can change during an investigation proves nothing. (KVKK guide §3.2; conflicts #39.)
6. Define two audit retention classes in the OPA retention policy: OpenSearch records, `reqData` included, are kept 1 year, and the immudb decision fields 2 years; identity records are kept at least as long (conflicts #11), and the values pass a legal review before production. Why: the statement text is personal data and should go first, while the tamper-evident decision record must outlive it to answer later reviews and incidents; no source gives a period, so these are platform values. (decision 2026-11-16; conflicts #11, #30, #39, #50.)

### Access review process and cadence

Source: Azure RBAC and Azure ABAC documentation (Microsoft Learn), pages 04, 09, 16 (merged 2026-11-14); NIST SP 800-162, §3.1.2.3 (merged 2026-11-15); Authorization in Action, ch. 4, 14 (merged 2026-11-15); Attribute-Based Access Control, ch. 3.10 (merged 2026-11-15); Data Management at Scale, 2nd ed., ch. 2, 8 (merged 2026-11-16); KVKK Personal Data Security Guide, §2.3 (merged 2026-11-16).

1. Run recurring reviews (weekly to annually) for privileged roles, each instance shorter than its period; a monthly review lasts at most 27 days. Why: Azure prevents overlapping instances this way. (Azure docs 16.)
2. Review against a snapshot of the manifest and Ranger state taken at the start; changes made later go to the next cycle. Why: Azure freezes the review scope when each instance begins. (Azure docs 16.)
3. Apply denials automatically as Git changes when the review completes, and define in advance what happens to unanswered items: for privileged roles (Institution Owner, Access Administrator, PII Reader) no answer removes the assignment; for other roles the item carries over to the next cycle. Why: Azure changes nothing until completion and then removes denied access within minutes, and unreviewed privileged access must not persist. (Azure docs 16; KVKK guide §2.3; decision 2026-11-15.)
4. Show a usage-based recommendation from the Ranger audit and certify entitlements only with that usage in view: approve if the principal accessed the scope within the last 30 days, otherwise deny. Why: Azure recommends from a 30-day sign-in window, and in the book's case stale contractor entitlements kept granting access through inheritance. (Azure docs 16; Authorization in Action ch. 14.4, 14.6, 14 Do/Don't.)
5. Require a reason for every approval and send reminders to reviewers. Why: approvals become auditable and reviews finish. (Azure docs 16.)
6. Include eligible and active assignments, service principals and assignments made by delegates in the review scope. Why: Azure scopes reviews by assignment type and principal type, and delegated assignments must be reviewed. (Azure docs 09, 16.)
7. Expand group assignments to their members for reviewers, and when a member is denied, fix the Keycloak membership or the group assignment explicitly. Why: Azure shows transitive members but does not remove anyone from a shared group. (Azure docs 16.)
8. Before each review, remove assignments whose principal no longer exists and check that each description still holds. Why: orphaned assignments and stale justifications distort the review. (Azure docs 04.)
9. Produce an effective-access report from the manifest: principal by dataset, after group expansion, scope inheritance, guardrails and conditions, so it shows effective entitlements rather than role lists. Compute it for every attribute value a principal can reach (for example every `project` a group's members can hold), not only today's values, so that "who can access this dataset?" is answered before any request. Where a dataset requires such before-the-fact review and the report cannot resolve its conditions, keep that dataset on plain role assignments. Why: ABAC does not naturally answer who can access an object; the cost grows with principals times resources, and grouping users into roles and resources into scopes cuts it; identical roles can yield different access at different times; and the maximum risk depends on the attribute states that can be reached. (NIST SP 800-162 §3.1.2.3; Authorization in Action ch. 4.3.2; Attribute-Based Access Control ch. 3.10.)
10. Let the owning institution answer the access review of its own data: the Institution Owner, or the data owners it names in the RACI (role-engineering.md › Governance roles, rule 6), reviews every assignment on its zone's datasets, scoped by `zone_name` (Audit record fields and correlation, rule 4), including each cross-institution share, which is checked against its recorded agreement (purpose, environment, validity, approver; azure-style-rbac-abac.md › Role assignments, rule 12) and removed when it no longer matches; platform-scope assignments are reviewed by the platform team, and the Auditor checks that every review was done (role-engineering.md › Governance roles, rule 3). Why: the owner judges who may see its data, and reviewing agreements together with the audit log proves they are respected. (DMaS ch. 2, 8.)
11. Review every zone that holds personal data, the authorization matrix included, and every privileged role (Institution Owner, Access Administrator, PII Reader, platform administrators) every quarter, and run at least one unscheduled review per institution between two scheduled ones (compliance-kvkk.md › Periodic review and removal of access, rules 1 and 2). Why: KVKK asks for regular and random controls without a number, and one quarterly cycle keeps the matrix and the privileged roles under the same rhythm; the quarter is a platform value. (KVKK guide §2.3, §4.2; decision 2026-11-16.)
12. Review every cross-institution sharing agreement each quarter together with the zone review (rule 11), and again when the agreement expires. Why: an agreement is the reason for a share, and an agreement that is never checked cannot prove it is still respected. (DMaS ch. 8; decision 2026-11-16.)

### Security monitoring indicators and alerts

Source: Authorization in Action, ch. 14, 15, 17 (merged 2026-11-15); Apache Ranger wiki, p. 9, p. 10 (merged 2026-11-15); Hadoop Security, ch. 2, 8 (merged 2026-11-15); Kafka: The Definitive Guide, 2nd ed., ch. 11 (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 7, 8 (merged 2026-11-16); KVKK Personal Data Security Guide, §3.2 (merged 2026-11-16); Data Mesh, ch. 14 (merged 2026-11-16)

1. Define freshness and completeness indicators for every signal that feeds decisions (usersync, tag sync, plugin policy and user-store versions) and alert when one exceeds its documented interval. Why: bad signals undermine correct policies. (Authorization in Action ch. 15.5.2.)
2. Run a fixed set of canonical requests against every engine on a schedule and alert when outcomes differ. Why: inconsistent decisions for the same request undermine correct policies. (Authorization in Action ch. 15.5.3.)
3. Analyse decision logs for anomalies, drift, conflicts and unused policies; establish each finding with deterministic queries or policy replay, use AI only to summarise or cluster, and feed findings into the update stage and its tests. Why: decisions are evidence of how intent was interpreted, overlapping policies silently widened access, and a finding must not depend on a model. (Authorization in Action ch. 14.2.4, 14 p. 274, 17.3, 17 p. 337; Data Mesh ch. 14.)
4. Alert on every allowed audit event with policy ID -1. Why: allowed access by a Ranger super user is audited without a policy ID, so these events are the only trace of a bypass of all policies, deny guardrails included. (Apache Ranger wiki p. 9, p. 10; trial: verification lab V5, 2026-11-14; conflicts #18.)
5. Alert on rule-matched events, not on every deny: denies on resources tagged `PII` or `RESTRICTED`; any Ranger Admin policy, role, zone or service change made by an account other than the deployment pipeline; administrative actions; membership changes of groups that hold assignments (Keycloak admin events) across each user's life cycle; and use of a privileged role outside an approved activation. Review the alert rules together with the incident procedure. Why: an alert on every deny drowns the signal, these events show attempted access to sensitive data and changes outside the Git path, and access and role changes and privileged-user violations are core security monitoring items. (Hadoop Security ch. 8; Data Governance DG ch. 8; conflicts #6.)
6. Alert on runs of failed access per principal, authentication failures as well as authorization denials (the book treats about 20 consecutive failures as abnormal and a few as typos; tune the threshold on the platform's own audit), and on access from unexpected networks or outside working hours, which needs client IP and time in every record; apply a lower threshold to principals whose departure is already known from the identity source. Why: credential guessing leaves failures in the logs, a stolen valid credential differs only in where and when it is used, and users near the end of their employment are a known exfiltration risk. (Hadoop Security ch. 2; Kafka DG ch. 11; Data Governance DG ch. 7.) (Hadoop Security ch. 2; Kafka DG ch. 11; Data Governance DG ch. 7; KVKK guide §3.2.)
7. Give every alert rule a unique name, a purpose and a threshold, and make every alert state what is wrong and where to look; route it to the on-call group, require acknowledgement, reroute it when nobody acknowledges, repeat it while the value stays outside the threshold, and record the action taken on it with a ticket link. Why: alerts must be actionable, an alert nobody acknowledges is not remedied in time, and KVKK requires action on monitoring warnings, not only their collection. (Data Governance DG ch. 8; KVKK guide §3.2.)
8. Keep the account lockout at 5 failed sign-ins and the alert at about 20, and set the threshold for spikes in denied decisions from a baseline taken over the first 30 days of audit. Why: a deny threshold set before the platform has traffic would be a guess. (decision 2026-11-16)

Example (rewritten, platform alert manifest):
```yaml
kind: AuditAlert
metadata: { id: alert-pii-deny }
spec:
  stream: decision
  match: { result: DENIED, tags: [PII], zone_name: t-acme }
  notify: platform-security
---
kind: AuditAlert
metadata: { id: alert-change-outside-pipeline }
spec:
  stream: policy-change
  match: { userNot: svc-policy-deployer }     # only the deployment pipeline writes Ranger (conflicts #6)
  notify: platform-security
```

Example (rewritten, platform alert manifest):
```yaml
kind: AuditAlert
metadata: { id: alert-assigned-group-change }
spec:
  purpose: "Membership change of a group that holds an assignment in zone t-acme"
  stream: identity-change                      # Keycloak admin events
  match: { event: group-membership, groupHasAssignment: true, zone_name: t-acme }
  threshold: 1
  message: "Group {group} changed by {actor}; check the request ticket and ra-IDs of the group"
  notify: platform-oncall
  acknowledgement: { required: true, rerouteTo: platform-security, repeatWhileOpen: true }
```

### Governance monitoring metrics

Source: Authorization in Action, ch. 14, 15 (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 8 (merged 2026-11-16).

1. Track overdue policy reviews, open audit findings, policy incidents, failed tests, time to resolve and the alert history with its trends, and report them to the monthly policy review board (role-engineering.md › Governance roles, rule 5). Why: these give early warning of drift, and the alert history is evidence for audits and decisions. (Authorization in Action ch. 14.6, 14 p. 280; Data Governance DG ch. 8.)
2. Report zero-trust progress on separate dimensions, not as one maturity level: enforcement coverage (share of decisions made by Ranger, against the path inventory in trino-security.md › Enforcement points on every path, rule 1), signal freshness and completeness, consistency across engines, and the number of remaining implicit-trust paths (security-operations.md › Network zoning and closing bypass paths, rule 1). Why: the dimensions move independently, and coverage shows bypass paths. (Authorization in Action ch. 15.5.)
3. Start compliance monitoring with a baseline audit of the platform's governance against the regulations that apply (KVKK), then write a monitoring plan that addresses the largest risks first and names who does what. Why: monitoring needs a baseline to measure against and priorities to stay affordable. (Data Governance DG ch. 8.)

