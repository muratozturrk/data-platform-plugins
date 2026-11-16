# Attribute and tag design (ABAC)

Status: starter rules; attribute engineering, classification to policy chain, environment conditions, lineage and the written/digital/metapolicy layers filled from NIST SP 800-162 (2026-11-15); attribute schema and typing, value confidence, practice statements and assurance levels, delivery between decisions, enumerated vs formula grants and confinement of restricted reads from Attribute-Based Access Control (merged 2026-11-15); context freshness, normalization and the role/scope/attribute choice from Authorization in Action (merged 2026-11-15); tag services, tag policies and condition hooks from the Apache Ranger wiki (merged 2026-11-15); differentiated access, masking and tokenization principles from Hadoop Security (merged 2026-11-15); policy book per data class, purpose groups, classification scheme, protection levels, class policies, auto-classification, partial access and lineage capture from Data Governance: The Definitive Guide (merged 2026-11-16); provider metadata at registration, golden source and application owner, short central class and purpose lists, catalog ownership per institution, de-identification options, logical products with several physical forms and combining datasets from Data Management at Scale, 2nd ed. (merged 2026-11-16); special-category status, KVKK risk factors, the processing inventory and data minimisation (archive scope, lapsed purpose) from the KVKK Personal Data Security Guide (merged 2026-11-16); local policies declared in the dataset manifest, institution-declared classes with platform techniques, consent recorded with purpose assignments and lineage-linked policy refresh from Data Mesh (merged 2026-11-16).

## Starter rules

1. Take resource attributes only as Ranger tags pushed from OpenMetadata classifications by one synchronizer through the Ranger tag API, and principal attributes only from Keycloak; keep the number of attribute sources to these two and document any local copy. Why: one authoritative source per attribute, fewer sources make performance and security management simpler, and the shipped tag synchronizer reads Apache Atlas, so OpenMetadata needs its own. (plugin v1; NIST SP 800-162 §3.2.2.6–3.2.2.7, §3.3.2; Apache Ranger wiki p. 2; Attribute-Based Access Control ch. 7.4.)
2. Use attributes and tags for sensitivity, purpose, region, clearance and other changing context (time, location, project), roles for stable job functions, and scope for structure. Why: this keeps the policy count flat as datasets grow, one reusable attribute policy replaces many roles, and the split between variable and stable characteristics also recurs in RBAC constraints. (plugin v1; Authorization in Action ch. 5.3, 6.2, 6.2.4; Attribute-Based Access Control ch. 3.8.)
3. Give every attribute one named authority (owner), one authoritative source (for example HR for department, the security office for clearance, the data steward for classification) and a freshness bound, and record these as metaattributes together with creation date and assurance. Record per value how it was established (self-asserted or taken from a record), its last update and a confidence level, and let a condition that guards sensitive data accept only values at the confidence its data class requires. Set the freshness bound per attribute from its change rate and the cost of a stale value, never one interval for an entity that mixes fast- and slow-changing attributes. Why: authorities are the root of trust, a stale attribute is a wrong decision, confidence in an attribute can itself be a decision input, a self-reported value may be enough for public data but not for sensitive access, and one shared interval either overloads the source or keeps fast-changing facts stale. (plugin v1; NIST SP 800-162 §1.4, §2.4.2, §3.3.1; Authorization in Action ch. 6.2.2, 12.4.2, 12.4.4; Attribute-Based Access Control ch. 7.4.1, 7.4.4, 7.7.1.)

### Written policy, digital policy and metapolicy

Source: NIST SP 800-162, §2.4.1, §3.2.1.1 (merged 2026-11-15); Attribute-Based Access Control, ch. 4.4.3 (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 1, 2, 4, Appendix A (merged 2026-11-16).

1. Write every access requirement first as a written policy per resource type and per data class: who may create, read, modify, delete or share it and under which conditions, and for a data class also the processing it allows and where the data may reside (the policy book); use cases are acceptable. Give both halves of a class definition the same class name: the access part compiles through the manifest to Ranger, the retention part goes to the OPA retention policy. Why: written rules replace undocumented owner judgement and are the input for digital policy, regulators ask per class who may access the data and what may be done with it, and the platform splits access (Ranger) from retention (OPA), so only a shared class name keeps the two consistent. (NIST SP 800-162 §2.4.1, §3.2.1.1; Data Governance DG ch. 2.)
2. Turn written policies into digital policy only through the manifest and the compiler to Ranger policy JSON, never by editing Ranger directly, and prefer automated enforcement (compiled policy, CI checks) to manual procedures so that the compliant path is also the easiest one. Why: written policies are not machine-enforceable, one compile path keeps every digital policy traceable to its source, and automated enforcement deploys faster and removes bureaucracy and human error. (NIST SP 800-162 §2.4.1; Data Governance DG ch. 4, Appendix A; conflicts #6.)
3. Document the metapolicy: priority and conflict resolution, including the combining algorithm (deny-overrides: access-control-models.md › Policy combining, rule 2; azure-style-rbac-abac.md › Evaluation), authorship (repository folders and code owners, conflicts #7), storage (Git), the update process, and the default for business data, which on the platform is closed (deny by default). Why: several digital policies need deconfliction and controlled authorship, different combining algorithms give different results for the same rules, and the book leaves the open-or-closed default to a governance decision that must be written down. (NIST SP 800-162 §2.4.1; Attribute-Based Access Control ch. 4.4.3; Data Governance DG ch. 1; conflicts #7.)

Example: the `requirement` field in azure-style-rbac-abac.md › Role assignments links an assignment to its written policy.

### Tag services and tag policies

Source: Apache Ranger wiki, p. 2 Tag Based Policies and requirements (2015), Prohibition Policy (2016) (merged 2026-11-15).

1. Keep one tag service per environment (for example `platform-tags` in production, `platform-tags-dev` in development) and link every resource service of that environment (Trino, Kafka) to it. Why: a plugin enforces tag policies only for a resource service that refers to a tag service, and the wiki separates tag policies per cluster by tag service. (Apache Ranger wiki p. 2.)
2. Write tag policies only for tags with a security meaning (`PII`, `RESTRICTED`) and leave lineage, quality or analytics tags without policies. Why: a tag with a policy changes access decisions, and tags without security meaning must not. (Apache Ranger wiki p. 2.)
3. Sync only resource-level tags (catalog, schema, table, column, topic) into the Ranger tag store, never row- or cell-level labels. Why: the tag store is designed for resource-level tags. (Apache Ranger wiki p. 2.)
4. Review the default tag policy that every new tag service creates (`EXPIRES_ON` on attribute `expiry_date`), and either remove it or keep it on purpose, recorded in Git. Why: it appears without a manifest entry, and its script condition fails to initialise in the Kafka plugin on JDK 17 (kafka-security.md › Authorizer model, rule 5). (Apache Ranger wiki p. 2.)

Example (rewritten, platform manifest for the link):
```yaml
kind: TagServiceLink
metadata: { id: tag-link-prod }
spec:
  tagService: platform-tags
  resourceServices: [trino, kafka]     # every resource service of the environment
  zones: [t-acme, t-beta]              # see access-control-models.md › Security zones, rule 4
  defaultPolicies: { EXPIRES_ON: removed }
```

### Attribute engineering: naming, authoritative source, assurance, freshness

Source: NIST SP 800-162, §2.2, §2.4.2, §3.1.3.2, §3.1.3.4, §3.2.1.3–3.2.1.4, §3.2.2.6–3.2.2.12, §3.2.3, §3.3.1 (merged 2026-11-15); Authorization in Action, ch. 6, 9, 12 (merged 2026-11-15); Attribute-Based Access Control, ch. 4.5–4.6, 7, 8.2.2 (merged 2026-11-15).

1. Model every attribute, principal attribute or tag, as a name-value pair with a published schema: family (principal, resource, request or environment), data type, allowed values, meaning, issuing authority (owner), the companion attributes it needs (for example a `role` value is valid only together with an `institution` value) and the policies that use it. Keep one attribute per concept (never `dept`, `department` and `org_unit` for the same fact), and let CI reject attribute and tag names that are not in the schema. Why: ABAC fails without a defined attribute set, owners need the schema to tag correctly, an attribute read out of its context or with the wrong type gives a wrong decision or an evaluation error, and unregistered near-duplicate names are the attribute version of role explosion. Keycloak specifics for principal attributes: identity-and-principals.md › Custom security attributes, rules 4–6. (NIST SP 800-162 §2.2, §2.4.2, §3.2.1.3; Authorization in Action ch. 6.2.2, 6.2.5, 9.2.3; Attribute-Based Access Control ch. 4.5.1, 4.6, 7.4, 7.4.2.)
2. Use the same attribute names and values in written policies, manifests, generated Ranger policies, the metapolicy and any engine-local configuration that reads attributes. Why: policies are expressed in attributes, central and local rules must read them in the same way, and a name that differs between layers silently stops matching. (NIST SP 800-162 §3.1.3.2; Attribute-Based Access Control ch. 8.2.2.)
3. Let the institution that provisions an attribute value own its life cycle: creation, change and retirement. Why: only the provisioning side knows when a value stops being true. (NIST SP 800-162 §3.2.1.4.)
4. Normalize attribute names, values, types and units across institutions, or keep one central mapping table in the platform folder (for example `dept_code` in one institution = `department` in another), and write cross-institution policies only on attributes listed there. Translate a regulation (for example KVKK) once into rules over generic platform tags such as `PII`, so that each institution only maps its local fields to those tags. Normalize once, at the source or in that table, never in per-engine code, so that `institution` has one value set (`t-acme`, `t-beta`) in Trino, Kafka and Spark policies. Why: a policy that spans institutions needs attributes that mean the same everywhere and are comparable and accurate, a rule translated once stays consistent across institutions, and per-engine normalization made identical policies decide differently. (NIST SP 800-162 §2.4.2, §3.2.2.12, §3.2.3.2; Authorization in Action ch. 12.2, 12.6.1, 12 Do/Don't; Attribute-Based Access Control ch. 7.4.1, 7.5.2, 7.8; decision 2026-11-15, conflicts #35: no separate normalisation service.)
5. Check every attribute feed for completeness, allowed values, integrity and change history, and accept a feed only with verifiable origin (mutually authenticated connections inside the platform, issuer-signed tokens otherwise). Accept an attribute asserted by another institution only through a verifiable chain of trust back to that institution's authority (its identity provider brokered by Keycloak). Keep provenance with issuer and timestamp: OpenMetadata tag history for tags, Keycloak admin events for principal attributes. Protect the attribute stores (Keycloak, OpenMetadata, the Ranger user and tag stores) with change logs and tamper detection, and move attributes only over encrypted, authenticated channels with short-lived tokens against replay. Why: decisions need authoritative, trustworthy attribute data, a remote attribute is only as trustworthy as the chain that verified it, and a falsified or tampered signal corrupts every decision. (NIST SP 800-162 §3.2.2.10; Authorization in Action ch. 12.5.2; Attribute-Based Access Control ch. 7.5.1, 7.6.1, 7.6.2.)
6. Prefer one shared repository per attribute over local copies; document every local copy with its refresh interval, and allow an offline bulk copy only for a disconnected site with stable users, after a recorded risk analysis. Define per attribute its refresh, the cache that covers a source outage and its fail-over, and back up Keycloak and OpenMetadata as attribute stores. Why: local and offline copies may still hold stale or revoked values, freshness decides whether a value still applies, and attributes must stay available. (NIST SP 800-162 §3.2.2.6–3.2.2.7, §3.3.1; Attribute-Based Access Control ch. 7.7.1, 7.7.3, 7.7.4.)
7. Record, for every attribute that drives decisions, its timeliness, relevance, authority, quality, reliability, completeness and the authentication strength behind it. Why: a decision is only as trustworthy as these properties of its inputs. (NIST SP 800-162 §3.2.3.1.)
8. Publish the attribute schema to policy authors, but show a principal only the values of a restricted attribute that apply to it. Why: the value list of a restricted attribute must not leak through the schema. (NIST SP 800-162 §3.1.3.4.)
9. Deliver attribute, group and tag changes to the plugins only through their refresh paths (usersync, tag sync, policy and user-store download), never by patching a running plugin's state, so that every decision is evaluated against one consistent set of attributes. Why: updates applied in the middle of a decision cause faulty decisions. (Attribute-Based Access Control ch. 7.7.2; conflicts #17.)
10. Keep an attribute practice statement for every attribute provider, recorded beside the trust model (security-operations.md › Introducing attribute-based policy, rule 2). The platform writes it for OpenMetadata tags, and each institution writes it for the Keycloak attributes or the brokered identity provider it supplies. The statement covers attributes, sources, establishment and update processes, security and service levels. Assign each provider an assurance level from a four-level cumulative scheme (a lighter one for internal attributes), and rate a decision that combines attributes of different levels at the lowest of them unless a documented exception exists. Why: the statement is the basis for trusting a provider, as a certificate practice statement is for a certificate authority, and a decision is only as strong as its weakest attribute. (Attribute-Based Access Control ch. 7.9, 7.9.2.)

Example (rewritten, platform attribute catalog entry):
```yaml
kind: AttributeDefinition
metadata: { id: attr-institution }
spec:
  family: principal
  type: string
  allowedValues: [t-acme, t-beta]
  authority: t-acme identity administrator (Keycloak realm t-acme)
  source: keycloak -> usersync
  establishedBy: record              # not self-asserted
  refresh: PT60S                     # usersync cycle (identity-and-principals.md › Usersync, rule 2)
  onOutage: plugin user-store cache  # conflicts #34
  provider: { practiceStatement: aps-t-acme, assuranceLevel: 2 }
  usedBy: [ra-0051]
```

### When to decide by role, by scope, or by attribute

Source: Authorization in Action, ch. 1, 4, 5, 6, 7 (merged 2026-11-15). Axis choice: starter rule 2; when to add a condition: azure-style-rbac-abac.md › Conditions, rule 2; Apache Ranger wiki, p. 2 (merged 2026-11-15); Attribute-Based Access Control, ch. 3.3 (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 2 (merged 2026-11-16); Data Management at Scale, 2nd ed., ch. 8 (merged 2026-11-16); Data Mesh, ch. 14 (merged 2026-11-16)

1. Never put context (region, segment, institution, time window, on-call duty, temporary assignment, device, expiry date) into a group, role, resource or tag name; express it as a principal attribute, a tag attribute (for example `expiry_date` on tag `RETENTION`), the scope or a time-bound condition (`validitySchedules`). Why: context in names causes role explosion and hides intent, while stable rules over changing attributes stay readable, and tag attribute values reach policy conditions through the request context. (Authorization in Action ch. 1.2.3, 4 Do/Don't, 5.2.4, 5.2.5, 6.1.5, 7.2.3; Apache Ranger wiki p. 2.)
2. Prefer enumerated grants (a group's role assignment on a scope) wherever access must be reviewable per principal and per resource. Use a formula condition (for example principal `institution` equals the dataset's institution tag) only where one rule replaces many enumerated assignments, and document every formula for reviewers in the assignment's description or its ADR (audit-design.md › Access review process and cadence, rule 9). Why: reviewing logical-formula policies is computationally hard (NP-complete), enumerated relations are easy to review, and a formula stays one rule as data grows. (Attribute-Based Access Control ch. 3.3.)
3. Where the same person may use the same data for one purpose but not for another (consent-bound data, people who hold several roles), express the purpose as membership in a purpose group with its own assignment, take the purpose from one short list of usage purposes kept in the platform folder, combine it with the data's classification through a condition on the class tag (azure-style-rbac-abac.md › Conditions, rule 14), make the assignment time-bound when the purpose ends, and name the purpose in the assignment's description; record the consent scope a purpose assignment relies on in its description and in the dataset's OpenMetadata entry, so consumers can see it, and remove the assignment through Git when that consent is withdrawn; never copy the data into a purpose-specific container outside a governed dataset. Why: a mapping from data class to role cannot tell two uses of the same data apart, a short central purpose list limits use without a role per scenario, a purpose container is a copy that escapes the source policy, and consent kept apart from the data cannot be tracked once the data is shared. (Data Governance DG ch. 2; DMaS ch. 8; Data Mesh ch. 14; conflicts #10, #12.)
4. Start the platform list of usage purposes with reporting, analytics, operations, ML training, regulatory reporting and research, managed by the governance committee; record the purpose in the assignment and the sharing agreement, never at query time. Why: a fixed list keeps purpose groups comparable across institutions, and a purpose typed at query time cannot be verified. (decision 2026-11-16)
5. Mark datasets whose use depends on consent with the tag `CONSENT_REQUIRED`, and enforce record-level consent with a row filter on a consent column that matches the reader's purpose group. Why: consent is per data subject and per purpose, which a dataset-level grant cannot express. (Data Mesh ch. 14; decision 2026-11-16.)

Example (manifest): on-call access as a time-bound assignment, not a permanent `t-acme_oncall` group:
```yaml
kind: RoleAssignment
metadata: { id: ra-0120 }
spec:
  principal: { type: group, id: kc-grp-5512, name: platform_t-acme_analyst }
  roleDefinition: data-reader
  scope: /institutions/t-acme/domains/ops
  description: "On-call week 2027-W03, ticket OPS-77"
  validity: { start: "2027-01-18", end: "2027-01-25" }
```

Example (rewritten, manifest):
```yaml
kind: RoleAssignment
metadata: { id: ra-0230 }
spec:
  principal: { type: group, id: kc-grp-5910, name: platform_t-acme_churn-study }   # purpose group
  roleDefinition: data-reader
  scope: /institutions/t-acme/domains/sales/datasets/customers
  description: "Purpose: churn study under consent scope C-12, owner t-acme marketing lead, ticket ACC-410"
  validity: { end: "2027-03-31" }
```

### Classification to policy chain

Source: NIST SP 800-162, §1.4, §2.4.2, §3.1.3.1, §3.1.3.3–3.1.3.4, §3.2.2.8, §3.4.1; Azure RBAC and Azure ABAC documentation, page 12 (merged 2026-11-15); Attribute-Based Access Control, ch. 7.4.1–7.4.2 (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 1, 2, 3, 4, 7, 9 (merged 2026-11-16); Data Management at Scale, 2nd ed., ch. 2, 4, 8, 9 (merged 2026-11-16); KVKK Personal Data Security Guide, §1.3, §2.1, §2.3, §4.2 (merged 2026-11-16); Data Mesh, ch. 3, 5, 9, 14 (merged 2026-11-16)

1. Publish the allowed tag set and, at creation and before publication, give every new dataset its minimum attribute set and its policy, registered through the OpenMetadata API by the institution's CI pipeline: owning institution, data owner, the application owner of the system that holds it, its golden source, the sensitivity classification of every column (at column level wherever a record mixes sensitive and non-sensitive fields), the columns that row filters use (including a reserved owner column where rows belong to different owners, role-engineering.md › Governance roles, rule 8), links to business glossary terms, lineage to its sources, and its retention class (values from the OPA retention policy, conflicts #50), where the data may reside and the regulations that apply, all declared in the dataset's manifest next to its address and output ports; keep owner, data class, sensitivity and environment as catalog fields in OpenMetadata; the catalog or CI gate rejects a dataset without owner, classification or glossary links. Tag personal and critical data on ingestion; until a dataset is classified it stays in the raw or staging layer, readable only by the ETL group (azure-style-rbac-abac.md › Scope design and inheritance, rule 7), and is never opened through broad role grants to compensate. Why: an untagged or unassigned dataset escapes tag policies, protection must exist from the first read, fields in one record need different protection, fine-grained filters and masks need provider metadata that must exist when the dataset is created, no data is shared without an owner, unknown data combined with role grants leads to oversharing, and the manifest is where a dataset declares the concerns its policy carries. (Azure docs 12; NIST SP 800-162 §1.4, §2.4.2, §3.1.3.1, §3.2.2.8; Attribute-Based Access Control ch. 7.4.2; Data Governance DG ch. 1, 2, 3, 9; DMaS ch. 2, 4, 8, 9; Data Mesh ch. 3, 14; conflicts #48, #50.)
2. Let only the designated tag authority set or remove decision-driving tags (`PII`, `RESTRICTED`); a dataset owner or data user never removes or changes a tag to make a request succeed, and every tag change is audited through OpenMetadata tag history. Why: a self-modifiable attribute lets a subject grant itself access, and decision quality equals attribute quality. (NIST SP 800-162 §2.4.2, §3.1.3.3–3.1.3.4, §3.4.1; Attribute-Based Access Control ch. 7.4.1, 7.4.2.)
3. Define the classification scheme before choosing tools, and keep it as one short enterprise dictionary in OpenMetadata with a named owner (the legal or data-protection office), controlled centrally and used unchanged by every institution: infotypes (for example phone number, IP address) grouped into data classes (for example `PII`), and a small ordered set of sensitivity levels (for example public, external, internal, restricted); mark in the dictionary every personal-data infotype that is special-category data (özel nitelikli kişisel veri) as defined in Law No. 6698, and treat those infotypes as usually restricted for the CI flag below; `PII` and `RESTRICTED` stay independent tags, and CI only flags a column whose class is usually restricted but lacks `RESTRICTED`, for the tag authority to decide (decision 2026-11-16, conflicts #55). Why: a policy can only target data that has a name, class definitions drive compliance, tools implement the taxonomy rather than define it, long lists are applied inconsistently, automatic enforcement depends on labels that mean the same everywhere, and special-category status is set by law and is the first KVKK risk factor. (Data Governance DG ch. 1, 2, 4; DMaS ch. 8, 9; KVKK guide §1.3, §2.1.)
4. Record for every data class, and for every source the written governance policy covers, its required protection level, chosen by weighing the cost of a breach (including unavailability, legal and reputational cost) against its likelihood, on a high, medium or low scale where an exact cost is impractical; for personal data, rate it by its special-category status, the confidentiality its nature requires and the harm a breach would cause the data subjects, and select tag policies by this level, never by the team that requests access. Why: protection has its own cost, so the level must follow the value and risk of the data, and KVKK rates risk by these three factors so that protection follows the data's nature. (Data Governance DG ch. 4, 7; KVKK guide §2.1.)
5. Make the data class the unit of policy: write sensitivity rules once against the class or sensitivity tag (the tag mask policy for `PII`, the platform guardrail for `RESTRICTED`), never as resource policies naming individual tables or columns, and keep resource policies for one-off grants; the owning institution declares in its dataset manifest which columns carry which class, and the platform team writes the class policy and its technique (mask, tokenization, row filter) once, so no institution writes its own rule for a class. Why: one class policy covers every matching field in every engine, stays understandable, scales with the data volume, and a tightened class rule reaches every labelled object without relabelling; the owner knows which of its columns are sensitive, while the technique has to be the same for every dataset. (Data Governance DG ch. 1, 2; DMaS ch. 8; Data Mesh ch. 5, 9; conflicts #37.)
6. Classify automatically when a new source is onboarded (OpenMetadata auto-classification), and scan existing stores on a schedule for sensitive patterns, free-text fields and images included; every finding passes the tag authority (rule 2) before its tag reaches Ranger. Why: unclassified data escapes class policies, and sensitive data turns up where nobody expects it. (Data Governance DG ch. 2, 7.)
7. Show catalog metadata of published datasets (name, description, owner, class, sensitivity, environment) in OpenMetadata only inside the owning institution, hide domains marked sensitive, and keep Trino listings hidden as today; a request to discover data across institutions goes through the sharing path of azure-style-rbac-abac.md › Role assignments, rule 12. Why: metadata reveals what data exists and who holds it, and discovery must not open a second path around access control. (Data Governance DG ch. 2, 7; decision 2026-11-16, conflicts #52.)
8. Delegate catalog ownership in OpenMetadata per institution and domain: the institution's stewards maintain its technical metadata, its branch of the business glossary and the links from glossary terms to data, with OpenMetadata roles mapped from the same Keycloak groups as the platform roles (data owner = Institution Owner, steward = Access Administrator, conflicts #13); decision-driving tags stay with the tag authority (rule 2) and the class list with the dictionary owner (rule 3). Why: domain-scoped ownership makes metadata transparent, the catalog itself needs a permission model aligned with the governance roles, and enforcement depends on centrally controlled labels. (DMaS ch. 9; conflicts #13, #53.)
9. Keep the personal-data processing inventory (kişisel veri işleme envanteri) in OpenMetadata as the root of personal-data classification: list every dataset that holds personal data in it, with its data recording system (catalog, schema or topic prefix) and owning institution, before any access policy is written for that dataset or any access to it is reviewed, and let every `PII` tag and special-category mark trace to an inventory entry. Why: data that is not listed cannot be risk-rated, protected or reviewed, and KVKK derives what must be protected from the inventory. (KVKK guide §2.1, §2.3, §4.2.)
10. Use the sensitivity labels `PUBLIC`, `INTERNAL`, `CONFIDENTIAL` and `RESTRICTED`, and keep `PII` as an independent tag (conflicts #55); classify on every schema change and run a full scan once a week. Why: a short, fixed label set is applied consistently, and a new column must not wait long unclassified. (decision 2026-11-16)

Example (rewritten, enterprise dictionary entry):
```yaml
kind: DataClass
metadata: { id: dc-pii }
spec:
  tag: PII                                   # synced to the Ranger tag store
  owner: t-acme data-protection office
  infotypes: [phone_number, ip_address]
  protectionLevel: high                      # breach cost x likelihood
  autoClassify: { onIngest: true, scheduledScan: true }   # scan interval: Unclear
  policies: { access: tag-mask-pii, retention: "opa:class-pii" }
```

Example (rewritten, platform manifest):
```yaml
kind: Dataset
metadata: { id: ds-t-acme-customers }
spec:
  institution: t-acme
  resource: t_acme_lake.gold_sales.customers          # logical address, never a storage path
  outputPorts: [ "trino:t_acme_lake.gold_sales.customers", "kafka:t.acme.sales.pub.customers" ]
  columns:
    national_id: { class: PII }                        # declared by t-acme; the platform tag policy masks it
    email:       { class: PII }
  retentionClass: "opa:class-pii"                     # value owned by the OPA retention policy
  locality: on-premises-tr                             # example value
  regulations: [kvkk]
```

### Condition patterns (principal attribute equals resource tag, path prefix, time, network)

Source: NIST SP 800-162, §3.1.3.5 (merged 2026-11-15). Pattern list: azure-style-rbac-abac.md › Conditions, rule 14; Authorization in Action, ch. 12 (merged 2026-11-15); Apache Ranger wiki, p. 2, p. 6, p. 13 (merged 2026-11-15); Attribute-Based Access Control, ch. 7.4.3 (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 2, 7 (merged 2026-11-16).

1. Use an environment attribute (time, client network, threat level, system status) in a condition only when its value is read at decision time from a source that every enforcement point can reach and that the requester cannot alter. Why: environment values are detected, not administered in advance, so a value the client can set is not a control. (NIST SP 800-162 §3.1.3.5; Attribute-Based Access Control ch. 7.4.3.)
2. Add environment and operational signals (time window, network origin, approval or on-call state, a stated use case, and device state once a signal exists that meets rule 1) only to high-risk decisions, so that one principal may write from one context and only read from another, and document per engine and data class which signals a policy uses, how each is delivered (token claim, sync, request) and how fresh it must be; weigh every added check against the friction that pushes users toward unmanaged copies. Why: a read tolerates hour-old claims while a high-risk change needs live data, every extra signal adds latency and dependencies, a stolen identity otherwise carries all its roles into every context, and too much friction produces shadow IT. (Authorization in Action ch. 12 p. 230–231, 12.3.1, 12.7; Data Governance DG ch. 2, 7.)
3. Write a context enricher only when a condition needs data that the request does not carry (for example the projects a user belongs to), never for time, user, groups or client IP, which every request already holds; load its data into memory at plugin start, keep it stateless and safe for concurrent calls, and register it in the service definition in Git, where enrichers run in the listed order. Why: enrichers and evaluators run on every authorization call. (Apache Ranger wiki p. 6.)
4. Prefer the shipped condition evaluators (time of day, IP range) and `validitySchedules` to custom evaluators and script conditions; deploy a custom evaluator only with its jar on Ranger Admin and every plugin host, and test a script condition on the engine's own JDK before rollout. Why: custom code needs deployment and restarts everywhere, script conditions failed to initialise in the Kafka plugin on JDK 17, and the script engines had remote-code-execution flaws fixed in 2.8.0 (Nashorn) and 2.9.0 (GraalJS). (Apache Ranger wiki p. 2, p. 6, p. 13.)
5. Do not use device posture as a condition: the platform has no trustworthy device signal. Why: a condition on a value nobody can verify gives a false sense of control. (Authorization in Action ch. 4, 12; decision 2026-11-16.)

### Differentiated access, masking and tokenization principles

Source: Hadoop Security, ch. 6, Afterword (merged 2026-11-15). Mechanics on the SQL path: trino-security.md › Row filters, masks and views on the SQL path; Data Governance: The Definitive Guide, ch. 2, 3, 6, 7, Appendix A (merged 2026-11-16); Data Management at Scale, 2nd ed., ch. 4 (merged 2026-11-16); Data Mesh, ch. 3 (merged 2026-11-16)

1. Choose the mechanism by where sensitivity varies: per audience for whole tables, separate tables in the product's design, each with its own dataset scope (a split at design time, never a copy, rule 2); per audience for whole columns, column scope in the assignment or a column tag with a mask; per row, a row filter driven by the filter columns the provider registered (Classification to policy chain, rule 1); per cell, a row filter or mask driven by resource-level tags and principal attributes, never row- or cell-level labels in the tag store (Tag services and tag policies, rule 3); and decide per dataset, recorded in its ADR, whether unauthorized values are hidden (row filter) or returned masked (mask). Why: coarse security is simplest as separate tables, group grants handle column-level differences, labels matched to user authorizations handle mixed sensitivity, and filtering and masking give different answers to the same query. (Hadoop Security ch. 6; DMaS ch. 4.)
2. Protect a sensitive column for exploration, data science and third parties with a tag and a mask policy applied on the fly by default; de-identify before landing (in the ingest pipeline) only where no consumer ever needs the clear value, and make a per-project de-identified copy only as a governed dataset under conflicts #10, choosing the method by the column's classification and recording it in the dataset's ADR; never make ungoverned per-audience copies, hand-maintained sanitized copies, special views or custom masking functions in queries (trino-security.md › Row filters, masks and views, rules 1, 5). Why: classifications drive the choice of method, policy-based masking is more maintainable than per-view or per-copy workarounds, every copy is one more access path, and a sanitized copy needs a builder with full access, goes stale and multiplies into versions. (Hadoop Security ch. 6, Afterword; Data Governance DG ch. 3, 6; DMaS ch. 4; KVKK guide §2.4, §4.1; conflicts #10.)
3. Offer partial access to data tagged `PII` as an authorized level of its own between deny and full read: a masked form for display (characters starred out or only the format kept) and a tokenized form (a consistent replacement value, such as a hash mask, that keeps joins, grouping and counts) where analysts need linkage and statistics but not the real values; full read stays with PII Reader through the unmasked item of the zone's tag mask policy; columns tagged `RESTRICTED` are masked (`MASK_NULL`) and unmasked only through Restricted Reader, and tables tagged `RESTRICTED` stay under the deny guardrail (azure-style-rbac-abac.md › Deny assignments and guardrails, rules 9 and 10). Why: users often need to group or join on a value without seeing it, masking breaks joins and statistical properties while tokenization keeps them, and an all-or-nothing choice pushes requests toward full access. (Hadoop Security Afterword; Data Governance DG ch. 2, 7; Data Mesh ch. 3; conflicts #37; trial: ranger-policy-compile, 2026-11-16; decision 2026-11-16, conflicts #62, #72.)
4. Before granting full access to personal data, consider the weaker forms and record the choice in the dataset's ADR: not storing the field at all, storing only aggregates or a subset, or tokenizing it. Why: each form lowers breach risk in a different way, and data that is not there cannot leak. (Data Governance DG ch. 7.)
5. Where automated processing needs personal data, let the processing job read it while people see it only masked or tokenized; a person sees individual records only through an activated PII Reader assignment (azure-style-rbac-abac.md › Privileged access, rule 1). Why: the value of the processing does not require people to view individual records. (Data Governance DG Appendix A.) The job runs as its own workload identity without a mask exemption unless it holds a documented PII Reader assignment (identity-and-principals.md › Service accounts and ingest identities, rule 6).
6. Enforce a minimum aggregate size (for example at least 50 people per group) only through a governed aggregate dataset (conflicts #10) whose pipeline applies the minimum and is tested in CI; give consumers Data Reader on that dataset only, never on the source, and do not claim protection against differencing between releases. Why: a view or row filter cannot guarantee group size, and only the published dataset can be tested and reviewed. (Data Governance DG ch. 7; decision 2026-11-16, conflicts #54.)
7. Before relying on a hash mask as deterministic tokenization, verify in the lab that it returns the same token for the same value on every engine; if a keyed token is needed, keep the key in OpenBao. Why: grouping on tokens works only if they are deterministic, and a key outside OpenBao would escape its controls. (decision 2026-11-16)
8. Keep one infotype-to-mask table in the platform folder and generate every zone's tag mask policy from it: express an infotype as the `type` attribute of the `PII` tag (`PII` with `type=iban`), never as a separate tag such as `PII_IBAN`, and give a `PII` column whose type has no row the default `MASK_HASH`. Why: one table gives every engine and zone the same mask for the same kind of value, and sub-tags would multiply tag policies. (trial: ranger-policy-compile, 2026-11-16; decision 2026-11-16, conflicts #71.)

| Tag and attribute | Mask for readers without an activated PII Reader or Restricted Reader |
| --- | --- |
| `PII`, `type=tckn` (national ID) | `MASK_HASH` |
| `PII`, `type=iban` | `MASK_SHOW_LAST_4` |
| `PII`, any other or no `type` | `MASK_HASH` |
| `RESTRICTED` on a column | `MASK_NULL` |

Example (rewritten, platform manifest):
```yaml
kind: DatasetProtection
metadata: { id: dp-t-acme-customers, adr: ADR-031 }
spec:
  dataset: t_acme_lake.gold_sales.customers
  columns:
    tckn:  { tag: PII, unauthorized: masked }      # display only
    email: { tag: PII, unauthorized: tokenized }   # joins and counts still work (mechanism: Unclear U4)
  rows: { unauthorized: hidden, rowFilter: ra-0131-rowfilter }
```

Example (rewritten, Ranger tag mask policy JSON):
```json
{
  "service": "platform-tags",
  "zoneName": "t-acme",
  "name": "t-acme-tag-mask",
  "description": "Generated from the infotype-to-mask table; zone-scoped tag policy is a lab check",
  "policyType": 1,
  "resources": { "tag": { "values": ["PII"] } },
  "dataMaskPolicyItems": [
    { "groups": ["platform_t-acme_pii-reader"], "accesses": [ { "type": "trino:select", "isAllowed": true }, { "type": "hive:select", "isAllowed": true } ],
      "dataMaskInfo": { "dataMaskType": "MASK_NONE" },
      "validitySchedules": [ { "startTime": "2026/11/20 09:00:00", "endTime": "2026/11/20 17:00:00", "timeZone": "Europe/Istanbul" } ] },
    { "groups": ["public"], "accesses": [ { "type": "trino:select", "isAllowed": true }, { "type": "hive:select", "isAllowed": true } ],
      "conditions": [ { "type": "expression", "values": ["TAG.type == 'iban'"] } ],
      "dataMaskInfo": { "dataMaskType": "MASK_SHOW_LAST_4" } },
    { "groups": ["public"], "accesses": [ { "type": "trino:select", "isAllowed": true }, { "type": "hive:select", "isAllowed": true } ],
      "dataMaskInfo": { "dataMaskType": "MASK_HASH" } }
  ]
}
```

### Data minimisation: archive scope and lapsed purpose

Source: KVKK Personal Data Security Guide (Kişisel Veri Güvenliği Rehberi), KVKK Publications No. 72, April 2025, §1.3, §2.4 (merged 2026-11-16). Narrow scope and default masks: azure-style-rbac-abac.md › The four building blocks, rule 3; Differentiated access, masking and tokenization principles, rules 1–3.

1. Move personal data that is rarely accessed out of the published layers into an archive-layer schema of its domain (`archive_<domain>`, for example `t_acme_lake.archive_sales`), listed as a non-published layer in the platform folder and granted only at dataset scope through its own assignments, so that no domain-scope grant reaches it. Why: KVKK asks for rarely used data to be kept in a more secure place, and domain scopes compile only to the published layers (azure-style-rbac-abac.md › Scope design and inheritance, rule 7). (KVKK guide §2.4; conflicts #48.)
2. When the owner or an access review finds that the purpose for keeping a dataset has lapsed, remove its access assignments and hand it to the OPA retention policy for deletion or anonymisation in the same reviewed change (compliance-kvkk.md › Privacy and regulatory use of roles, rule 3). Why: KVKK requires data that is no longer needed to be destroyed securely, and access left on such data serves no purpose. (KVKK guide §1.3, §2.4; conflicts #50.)
3. Keep archived data in an `archive_<domain>` schema of the same catalog. Why: one naming rule lets policies and retention address the archive by pattern. (decision 2026-11-16)

Example (rewritten, manifest):
```yaml
kind: RoleAssignment
metadata: { id: ra-0182 }
spec:
  principal: { type: group, id: kc-grp-5520, name: platform_t-acme_records }
  roleDefinition: data-reader
  scope: /institutions/t-acme/domains/sales/datasets/orders_2019   # compiles to t_acme_lake.archive_sales.orders_2019
  description: "Archive lookups for legal claims, ticket ACC-402; archive layer, dataset scope only"
```

### Policy inheritance through lineage (derived data)

Source: NIST SP 800-162, §3.1.3.7 (merged 2026-11-15); Attribute-Based Access Control, ch. 5.3.2–5.3.3 (merged 2026-11-15); Hadoop Security, Afterword (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 2, 6 (merged 2026-11-16); Data Management at Scale, 2nd ed., ch. 4, 8, 9 (merged 2026-11-16); Data Mesh, ch. 3, 14 (merged 2026-11-16)

1. Apply the same policy, generated from one policy source, to every copy and every access path of the same data (SQL, API, stream, file), never parallel storage permissions beside Ranger: model each data product in OpenMetadata as one logical entity linked to its domain, owner and every physical form (Iceberg table, topic, ClickHouse copy), all carrying the same tags; a persistent copy exists only as a governed dataset created under an ADR that inherits tags through lineage (conflicts #10), and a copy in an engine without a Ranger plugin, such as ClickHouse, needs equivalent controls before it holds governed data. Why: the weakest copy becomes the access path, protection must not depend on the access point, one product can have several physical forms that need the same tags, unmanaged copies escape compliance, and separately maintained controls per component drift apart. (NIST SP 800-162 §3.1.3.7; Hadoop Security Afterword; DMaS ch. 4, 8, 9; Data Mesh ch. 3; conflicts #10.)
2. Confine reads of `RESTRICTED` data: CI rejects an assignment set that lets one principal both read a dataset tagged `RESTRICTED` and write to a dataset that does not carry the same tag, so a job that processed restricted data writes only inside the restricted container; negative tests cover the confinement (policy-testing-and-verification.md › Verification properties, rule 4). Why: a legitimate reader can still leak data by copying it, and grants alone do not stop the copy. (Attribute-Based Access Control ch. 5.3.2, 5.3.3; Data Governance DG ch. 2; conflicts #10.)
3. Capture lineage in OpenMetadata for every pipeline engine (Spark, Flink, Airflow, Trino, Kafka Connect) under written capture criteria per engine (scanner, runtime API or manual upload), including the datasets consumers derive, recorded centrally rather than only in the consuming tool; capture it at least at table level, and at column level for every column tagged `PII` or `RESTRICTED`; propagate a source column's tags, and with them its mask and access policy, to every derived column that passes the values through unchanged, and propagate later tag changes of the source in the same way, so that a derived column follows the source's current classification; let the tag authority re-evaluate a derived column whose content is transformed or nulled, and give a new classification to a derived dataset whose transformation removes the link to the original data (for example an aggregation that drops identifiers); flag a derived dataset that inherits a sensitive tag but lands in a schema or container that does not carry it. Why: lineage shows where classified data flows, shared derived data must stay traceable, column lineage answers which tables hold personal data, derivatives must not escape the source policy, transformed data may need a different one, and data keeps its link to the policy that governs it until a transformation removes the link to the original data. (Data Governance DG ch. 2, 6; DMaS ch. 4, 9; Data Mesh ch. 14; conflicts #10.)
4. Classify every end product (Superset dashboard, extract, report) and every dataset that combines several sources at the most sensitive tag of any source column it reads (column-level lineage; columns of the same table that it does not read do not count), and let only principals cleared for that tag view it; the owner of a combining dataset keeps every source's restrictions and adds its own for the combination and for new data it creates. Why: personal data in any input makes the output sensitive, a composite owns only the rules on what it combines or creates, and a table-level reading would hide every dashboard built on a table with one `RESTRICTED` column. (Data Governance DG ch. 6; DMaS ch. 8; trial: ranger-policy-compile, 2026-11-16; decision 2026-11-16, conflicts #70.)
5. Give every derived dataset that adds no new facts a `sourceOwners` field in its manifest and OpenMetadata entry; its creation and every further share need the approval of those source owners, and only a dataset with new facts may be shared on its own owner's authority. Why: a copy without new facts is still the source owner's data, while the governed copy of conflicts #10 still needs one accountable owner. (DMaS ch. 2, 8; decision 2026-11-16, conflicts #56; conflicts #10.)

Example (rewritten, CI decision-table cases):
```yaml
# wi-t-acme-fraud-scoring reads the RESTRICTED columns of t_acme_lake.gold_fraud.cases through a time-bound
# Restricted Reader assignment (azure-style-rbac-abac.md › Deny assignments and guardrails, rule 10); writes go to trino-etl
- { id: confine-1, expect: allow, principal: wi-t-acme-fraud-scoring, service: trino-etl, access: "trino:insert",
    resource: t_acme_lake.gold_fraud.case_scores, tags: [RESTRICTED] }
- { id: confine-2, expect: deny,  principal: wi-t-acme-fraud-scoring, service: trino-etl, access: "trino:insert",
    resource: t_acme_lake.gold_sales.orders, tags: [] }
```

