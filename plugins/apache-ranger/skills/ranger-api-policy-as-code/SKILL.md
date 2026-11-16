---
name: ranger-api-policy-as-code
description: Automating Ranger: REST API v2 (services, service-defs, policies, roles, zones, tags, users/groups, plugin info), the apache-ranger Python client, export/import, idempotent policy-as-code compiled from platform Policy manifests, stable GUIDs and labels, drift detection against Git, CI validation and decision-table tests, tag push from OpenMetadata, and safe rollout/rollback of policy changes. Use when building or reviewing Ranger automation.
---

# REST API and Policy-as-Code

## API surface (public v2 unless noted)
| Area | Endpoints |
|---|---|
| Service-defs / services | `/service/public/v2/api/servicedef[/name/{n}]`, `/service/public/v2/api/service[/name/{n}]` |
| Policies | `GET/POST /service/public/v2/api/policy`, `PUT/DELETE /policy/{id}`, `GET /service/{svc}/policy[?policyName=…]`, `GET /policy/guid/{guid}`, apply (create-or-merge) `POST /policy/apply`, bulk export/import `/service/plugins/policies/exportJson`, `/importPoliciesFromFile` |
| Roles | `/service/public/v2/api/roles[/name/{n}]`, add/remove users/groups |
| Security zones | `/service/public/v2/api/zones[/name/{n}]` |
| Tags | `/service/tags/tagdefs`, `/tags`, `/resources`, `/tagresourcemaps`; bulk `PUT /service/tags/importservicetags` (ServiceTags document — what tagsync uses) |
| Users / groups | `/service/xusers/users`, `/groups`, `/groupusers` (xusers API), secure variants |
| Plugin status | `/service/plugins/plugins/info` (policy/tag versions downloaded per plugin host) |
| Admin audit | `/service/assets/report`, `/service/xaudit/…` |
Auth: basic (service account), Kerberos, or JWT where configured; use a dedicated `svc-control-plane` user with admin role limited by zones where possible. Python: `pip install apache-ranger` (`RangerClient`, `RangerPolicy`, `RangerService`, `RangerSecurityZone` models).

## Policy-as-code pipeline
```
Policy manifests (Git) ──compile──> desired state per Ranger service/zone (JSON with stable guid = uuid5(manifest path + rule id))
   ──validate (CI): schema, naming, no users, labels, mask default item, no public wildcard allow
   ──decision-table tests against an ephemeral Ranger + Trino (Testcontainers / PR namespace)
   ──plan: diff desired vs live (by guid) → create / update / delete list, reviewed in PR
   ──apply: zones → roles → tag defs → policies (create/update) → deletes last
   ──verify: plugin info shows new policy version on all plugins; smoke decisions
   ──nightly drift: export live → diff → alert on UI edits or unknown guids
```
Rules: idempotent (re-apply = no change); never match by display name alone; deletes only for policies carrying label `source:policy-manifest`; a `--dry-run` plan in every PR; revokes are prioritised and verified by plugin policy version (propagation proof ≤ poll interval).

## Rollback
Policies are versioned in Ranger (`/policy/{id}/versionList`), but the rollback mechanism is Git revert → re-apply; keep the last-good export artefact per release for emergency import.

## Tags from OpenMetadata
Job: list tagged columns/tables from OpenMetadata API → map classifications to Ranger tag defs (`PII` with attributes `type`, `level`; `PURPOSE` with `name`) → build a `ServiceTags` document per Ranger resource service (service resources with `resourceElements` catalog/schema/table/column and tag associations) → `importservicetags` → reconcile deletions. Trigger on OpenMetadata change events and nightly.

## Testing policies
- Unit: compiler golden files; schema validation.
- Integration: decision table executed through the engine (Trino JDBC as each test principal): allow, deny (`Access Denied`), masked value pattern, filtered row set; Kafka produce/consume attempts per principal.
- Regression: every revoked grant keeps a negative test.
- Audit assertions: each test finds its audit record with the expected policy id.

## Anti-patterns
- Screen-scraping or UI-only changes
- Import files overwriting hand-made policies without guid discipline
- Applying grants and revokes in one unordered batch
- Service account with unrestricted admin used by many tools

Sources: `references/sources.md` (Ranger REST API docs / Swagger, apache-ranger Python client, tag REST API, export/import docs).

Details and book-derived rules: `references/policy-testing-and-verification.md`.
