# Defect severity × priority

| Severity \ Priority | Low | Medium | High | Critical |
|---|---|---|---|---|
| Blocker | — | — | rare (unreachable path) | default |
| Critical | — | rare | default | release-visible / data wrong |
| Major | with workaround | default | SLA risk | launch-blocking |
| Minor | default | release-visible | compliance text | — |
| Trivial | default | before launch | — | — |

Data-exposure (cross-tenant, masking) ⇒ Blocker + security incident runbook, always.

## Defect template
- Title (behaviour, component)
- Class: defect / error / failure / change request
- Environment, versions, tenant, run_id / query_id
- Steps, expected, actual, evidence (logs, screenshots)
- Severity, priority, component, tenant impact, data impact, rollback available
- Linked test case id, requirement id
