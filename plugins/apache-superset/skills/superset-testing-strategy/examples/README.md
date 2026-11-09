# superset-testing-strategy – examples
- `assets_validate.py` – static bundle validator (schema, UUID stability vs Git baseline, credentials, RLS presence, naming); CI exit code
- `test_api_contract.py` – schemathesis run against the pinned OpenAPI spec on the harness + chart-data response contract
- `test_rls_mask_consistency.py` – same policy → same output across chart-data JSON, CSV export, embedded guest token
- `test_guest_token_scope.py` – token for dashboard/chart A cannot read B; expiry; RLS from token applied
- `test_security_headers.py` – CSP frame-ancestors, cookie flags, CSRF, public role, guest-token permission
- `k6_dashboard_load.js` – chart-data load test per dashboard with thresholds
- `playwright/dashboard-journey.spec.ts` – Keycloak login → dashboard → filter → cross-filter → drill → masked CSV export
- `playwright/visual-regression.spec.ts` – screenshot baselines, light/dark, 3 viewports, masked dynamic regions
- `superset_test_inventory.yaml` – T-bi-* tests (existing + new ids) in qa-test-strategy schema
