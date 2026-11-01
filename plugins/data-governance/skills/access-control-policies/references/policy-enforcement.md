# Policy Enforcement: RBAC/ABAC, Row/Column Security, Masking, Bridge, Audit

Where an access rule is **written**, **decided** and **enforced** are three different questions.
Getting them right is most of access-control design. Code blocks in this file are
**illustrative** — syntax varies by engine and version; check your engine's documentation before
use. Product facts carry evidence tags as in [SKILL.md](../SKILL.md).

## 1. Vocabulary (XACML / NIST SP 800-162)

| Term | Role | Typical component on a data platform |
|---|---|---|
| PAP — policy administration point | Where policies are authored and stored | Ranger Admin, OPA bundle repo, warehouse `CREATE ... POLICY` DDL, Git |
| PDP — policy decision point | Evaluates a request against policies | Ranger plugin inside the engine, OPA sidecar, warehouse optimiser |
| PEP — policy enforcement point | Intercepts the request and applies the decision (deny, rewrite with filter/mask) | Query engine, warehouse, object-store gateway |
| PIP — policy information point | Supplies attributes (user groups, resource tags) | Identity provider/LDAP, **catalog classification**, Ranger tag store |

The catalog's natural role is **PIP** (resource attributes = classification) plus a **registry of
policy metadata** (which policy protects what, why, owner). The rubric measures delegation of the
catalog's *own* decisions to an external PDP separately (D6.7).

## 2. Two planes — do not mix them

| Plane | Question | Who enforces | Measured catalogs |
|---|---|---|---|
| **Metadata plane** | May Ana see/edit the catalog entry of `sales.customers`? | The catalog | OM D6.2 = 4, DataHub 3, Atlas 2 |
| **Data plane** | May Ana `SELECT national_id FROM sales.customers`? | Engine / Ranger-style tool | No catalog enforces it (D6.3 ≤ 1, D6.4 ≤ 1) |

OpenMetadata's documented access control is metadata visibility; its `MetadataOperation` list
(`ViewAll`, `ViewSampleData`, `EditTags`, ...) has no column- or row-scoped operation
(`openmetadata-spec/.../policies/accessControl/resourceDescriptor.json:13-71`) (K). A strong
catalog ABAC score therefore says nothing about data security.

## 3. RBAC vs ABAC

| | RBAC | ABAC |
|---|---|---|
| Grant unit | Role -> permission on resource | Rule over subject, resource, environment attributes |
| Good for | Coarse, stable duties (admin, engineer, analyst) | Sensitivity, domain, region, purpose, time |
| Failure mode | Role explosion (`analyst_eu_pii_readonly_finance`) | Opaque rules; wrong attribute = wrong decision |
| New sensitive column | Needs a policy change | Covered once tagged |
| Audit question "why allowed?" | Role membership | Rule + attribute values at decision time |

Use RBAC for *who you are* and ABAC for *what the data is*: roles grant base access to a domain;
tag-based rules subtract (deny/mask) for sensitive classes. Make ABAC explainable: OpenMetadata's
`/v1/permissions/debug/evaluate` returns which policy and rule decided and why
(`openmetadata-service/.../resources/permissions/PermissionsResource.java:296-345`) (K); its
evaluator applies **deny rules first** (`.../security/policyevaluator/PolicyEvaluator.java:47-67`)
(K) and offers 13 attribute functions such as `isOwner`, `hasDomain`, `matchAnyTag`, `matchTeam`,
`hasAnyRole` (`.../policyevaluator/RuleEvaluator.java:54-405`) (K). Require the same of your data
plane PDP. Note deny-first also means a more specific allow cannot override a broad deny — design
deny rules narrowly.

## 4. Row- and column-level security

| Pattern | Enforced at | Pros | Cons |
|---|---|---|---|
| Engine-native column policy / column grant | Engine | No copies, consistent across tools | Engine-specific syntax |
| Engine-native row access policy | Engine | Dynamic per user attribute | Can hurt pruning; test performance |
| Ranger row-filter / column-deny policy | Ranger plugin inside engine | Central across Hive, Trino, Spark-on-Hive etc. | Plugin per engine; version coupling |
| Secure (authorised) view | Engine | Works on any SQL engine | Base table must be locked down; views multiply |
| Physical filtered copy | Pipeline | Simple for external sharing | Copies drift; more data to protect |

Rubric target (D6.4): level 3 = row filter bound **dynamically to user attributes**, consistent in UI
and API; level 4 = composite conditions, performance monitoring, and a **"what would user X see"
simulation**. Build the simulation yourself if the engine lacks it: run the policy expression with
the target user's attributes against a sample.

Illustrative secure view — mask plus dynamic row filter on a generic SQL engine:

```sql
-- ILLUSTRATIVE: function names (current_user, is_member) differ per engine.
CREATE VIEW sales.customers_secure AS
SELECT
  c.customer_id,
  CASE WHEN is_member('pii_readers') THEN c.national_id
       ELSE CONCAT('*******', RIGHT(c.national_id, 4)) END        AS national_id,
  CASE WHEN is_member('pii_readers') THEN c.email
       ELSE CONCAT('***@', SPLIT_PART(c.email, '@', 2)) END       AS email,
  CASE WHEN is_member('pii_readers') THEN c.birth_date
       ELSE DATE_TRUNC('year', c.birth_date) END                 AS birth_date,
  c.region_id,
  c.segment
FROM sales.customers c
WHERE EXISTS (                       -- row filter bound to a user attribute table
  SELECT 1 FROM sec.user_region ur
  WHERE ur.user_name = current_user AND ur.region_id = c.region_id);
-- Then: REVOKE SELECT ON sales.customers FROM analysts;
--       GRANT  SELECT ON sales.customers_secure TO analysts;
```

Illustrative tag-driven engine-native masking (warehouse style; one policy per data type, bound
to a tag, so new tagged columns are covered automatically):

```sql
-- ILLUSTRATIVE warehouse-style syntax.
CREATE MASKING POLICY mask_national_id AS (val STRING) RETURNS STRING ->
  CASE WHEN CURRENT_ROLE() IN ('PII_READER') THEN val
       ELSE '*******' || RIGHT(val, 4) END;
ALTER TAG governance.pii_type SET MASKING POLICY mask_national_id;  -- tag-based binding
ALTER TABLE sales.customers MODIFY COLUMN national_id
  SET TAG governance.pii_type = 'NATIONAL_ID';                     -- written by the catalog sync
```

DataHub's only route to masking is exactly this: a DataHub Action writes the tag to Snowflake and
Snowflake's tag-based masking does the work (`datahub-actions/.../action/snowflake/snowflake_util.py:80-116`) (K).

## 5. Masking, tokenisation, pseudonymisation

| Technique | Reversible | Joinable | Where enforced | Typical use |
|---|---|---|---|---|
| Dynamic masking (redact, partial, null, year-only) | Not by the reader | No | Engine at read | Analysts on shared tables |
| Deterministic hash (salted/keyed) | No (but dictionary-attackable without a secret key) | Yes | Pipeline or engine | Join keys across datasets |
| Format-preserving encryption | Yes, with key | Yes | Pipeline; decrypt UDF | Systems needing valid formats |
| Vault tokenisation | Yes, via token service | Yes (deterministic tokens) | Ingestion; detokenise via service | Payment data, national IDs |
| Static masking / test data | No | Optional | Copy job | Non-production environments |

Rules:

- Rubric D6.3 level 3 = method selectable **and different per role/attribute**; level 4 adds
  **reversible pseudonymisation where every re-identification is audited**. Implement
  re-identification as a request to a token service that logs requester, reason and record count.
- Pseudonymised data is still personal data (GDPR Art. 4(5)); it lowers risk, it does not exit
  scope. Whether a dataset is *anonymous* is a re-identification-risk question — see the
  `data-anonymization` skill.
- Record in the catalog, per column: technique, key domain/vault path, who can reverse it. The
  catalog does not execute any of it.
- The catalog's own sample masking is not data masking: OpenMetadata replaces stored samples with
  a fixed `********` only for `PII.Sensitive` (`.../security/mask/PIIMasker.java:45-48`) (K).

## 6. Ranger-style policies (illustrative shape)

Ranger stores policies as JSON with `policyType` 0 = access, 1 = data mask, 2 = row filter. A
**tag-based** masking policy keyed on the catalog's classification (field names follow Ranger's
public REST model; mask type names come from the engine's service definition — Hive-style shown):

```json
{
  "service": "platform_tag",
  "name": "mask-national-id",
  "policyType": 1,
  "resources": { "tag": { "values": ["PII_NATIONAL_ID"] } },
  "dataMaskPolicyItems": [
    { "groups": ["analysts"], "accesses": [{ "type": "hive:select", "isAllowed": true }],
      "dataMaskInfo": { "dataMaskType": "hive:MASK_SHOW_LAST_4" } },
    { "groups": ["pii_readers"], "accesses": [{ "type": "hive:select", "isAllowed": true }],
      "dataMaskInfo": { "dataMaskType": "hive:MASK_NONE" } }
  ],
  "description": "Generated from catalog tag PII.NationalId; owner: data-protection"
}
```

A resource-based **row filter** (keep row filters resource-based unless your Ranger version
documents tag-based row filtering):

```json
{
  "service": "platform_trino",
  "name": "customers-by-region",
  "policyType": 2,
  "resources": { "catalog": {"values": ["lake"]}, "schema": {"values": ["sales"]},
                 "table": {"values": ["customers"]} },
  "rowFilterPolicyItems": [
    { "groups": ["regional_analysts"], "accesses": [{ "type": "select", "isAllowed": true }],
      "rowFilterInfo": { "filterExpr": "region_id IN (SELECT region_id FROM sec.user_region WHERE user_name = current_user)" } }
  ]
}
```

Put a generator, not humans, in front of these: policies derived from catalog tags carry the tag
name and catalog link in `description`, which the drift check below relies on.

## 7. The catalog <-> enforcer bridge

Three directions, in increasing difficulty and decreasing marginal value:

| Direction | What it does | Difficulty | State of the art (measured) |
|---|---|---|---|
| **1. Forward sync** catalog -> tag store | Push column tags to the enforcer's tag store; tag-based policies fire | Easy: catalog search API + enforcer REST API + change webhook | Exists: Atlas -> Ranger tagsync is mature (D); OpenMetadata's commercial distribution documents a one-way, tags-only Ranger sync that creates no policies (D); OSS OpenMetadata 1.13.3 ships only an empty scaffold (K) |
| **2. Reverse reconciliation** enforcer -> catalog | Read enforcer policies, compare with tags, report drift | Medium: map two data models; schedule it | **Found in no product** (Atlas, DataHub, OM commercial, Privacera, Immuta per vendor docs) |
| **3. Delegation** catalog's own decisions -> external PDP (D6.7) | The catalog becomes a PEP for its own API | Hardest; least added value for data protection | Atlas = 3 (`atlas.authorizer.impl=RANGER`); DataHub = 2; OM = 1 |

Evidence for the bridge findings:

- OpenMetadata 1.13.3 has a Ranger-shaped but empty extension point: `securityService` entity whose
  service-type enum has the single value `Ranger` (`openmetadata-spec/.../entity/services/securityService.json:20-25`),
  `rangerConnection.json`, `securityServiceMetadataPipeline.json` (one `type` field), and a base
  source whose topology is `children=[]  # ... don't have child entities like policies, roles`
  (`ingestion/src/metadata/ingestion/source/security/security_service.py:68`); no Ranger connector
  under `ingestion/.../source/security/` (K). This represents Ranger as a catalog *service*; it does
  not delegate decisions, so D6.7 stays 1.
- A Ranger-side attempt to pull OpenMetadata tags (apache/ranger PR #544, RANGER-4978) was closed
  by its author in March 2026 (JDK incompatibility), table-level only, no drift check (İ).
- DataHub's Ranger integration runs the other way: Ranger policies authorise DataHub's own UI/API;
  catalog tags do not reach Ranger (D).

Why direction 2 matters: the silent failure mode is a column the catalog shows as `Sensitive`
while the enforcer has no policy for it — nobody sees an error. The inverse (masked in Ranger,
untagged in the catalog) gets "cleaned up" as unexplained one day.

Illustrative drift check (run on a schedule, e.g. in the orchestrator; open a steward task per
finding):

```python
# ILLUSTRATIVE: resolve both sides to fully qualified column names, then diff.
tagged    = {c.fqn for c in catalog.columns(tag_prefix="PII.") if c.level == "Sensitive"}
by_tag    = {fqn for t in enforcer.tag_policies(policy_type="mask")      # tag-based coverage
                 for fqn in enforcer.columns_with_tag(t.tag)}
by_res    = {fqn for p in enforcer.resource_policies(types=("mask", "deny"))
                 for fqn in p.columns()}
protected = by_tag | by_res
report = {
    "tagged_unprotected":  sorted(tagged - protected),   # highest severity
    "protected_untagged":  sorted(protected - tagged),   # document or remove
    "dead_tag_policies":   sorted(t.tag for t in enforcer.tag_policies()
                                  if not enforcer.columns_with_tag(t.tag)),
}
```

Add one more join that no product offers: enforcer audit log x `tagged_unprotected` = "how many
reads hit sensitive columns while unprotected" — the number a breach assessment needs.

## 8. Audit logging

| Log | Source | Must contain | Measured |
|---|---|---|---|
| Data access (reads) | Engine / enforcer audit | user, time, resource down to column, policy id applied, mask/filter applied, rows | Not a catalog function (all three) |
| Access decisions in the catalog | Catalog | subject, operation, resource, decision, deciding rule | Not written to audit in OM or DataHub (D6.2 notes) |
| Metadata changes | Catalog | who, when, before/after | OM: append-only `audit_log_event` table, filter + async export (`bootstrap/sql/migrations/native/1.12.0/mysql/schemaChanges.sql:9-32`) = 3; DataHub: `/openapi/v1/events/audit/search` = 3; Atlas: listing + plain-text `audit.log`, aged out by schedule = 2 (K) |
| Policy changes | PAP (Ranger/Git) | diff, author, approver | Use Git history for policy-as-code |

Rubric D6.6 level 4 asks for immutable (append-only or signed) storage, real-time export to a
SIEM and independently verifiable integrity; none of the three reaches it (no cryptographic
integrity). Ship catalog and enforcer logs to a write-once store and the SIEM; join them on
column FQN with the classification snapshot of that day.

## 9. Access requests and reviews

Rubric D9.4 levels: 2 = request routed to owner and approved in-product; 3 = **time-boxed** grant
with automatic expiry and **mandatory justification**; 4 = approval chain by sensitivity, queryable
history, and **periodic recertification campaigns**. In OSS, OpenMetadata = 1 and DataHub = 1;
DataHub's level-3 workflow is documented only for its commercial cloud (D).

Minimum viable process:

1. Request names dataset, columns/level, purpose, duration; purpose and duration mandatory.
2. Approver = data owner; `Restricted` adds data-protection officer.
3. Grant = group membership with expiry in the IdP, so the enforcer picks it up — not a
   per-user policy.
4. Quarterly recertification: owners confirm or revoke every grant on `Restricted` data; unanswered
   = revoked. KVKK Board Decision 2018/10 expects defined scope and duration of access and periodic
   authorisation checks for special-category data (see `kvkk-compliance`).
5. Evidence: request, approval, grant, expiry, and the engine's access log for the grant window.
