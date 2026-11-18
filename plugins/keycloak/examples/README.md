# Examples

| Folder | What | Checked |
|---|---|---|
| `deploy/` | Optimized image (`Dockerfile`), Keycloak CR v2beta1 with PDB (`keycloak-cr.yaml`), CloudNativePG cluster (`cnpg-cluster.yaml`) | YAML parses; field names from the Keycloak Operator guides (main, 2026-11) |
| `terraform/` | Realm baseline, claims-contract scopes, web-app clients, workload clients with federated Kubernetes credentials, Airflow and Kafka clients, institution module (organization, IdP, allow-listed group mappers, FGAP v2 delegation) | `tofu fmt` clean; argument names from terraform-provider-keycloak v5.10 docs. `tofu validate` not run here (provider registry not reachable from the build sandbox) — run it in your pipeline first |
| `integrations/` | Trino, Superset, Airflow 3, Strimzi Kafka, OpenBao, MinIO, Grafana, Kubernetes API configuration fragments | syntax only; property names from each component's documentation — marked "verify" where version-dependent |
| `automation/` | `tenant_to_tfvars.py` (Tenant manifest → Terraform variables), `claims_contract_check.py` (token vs claims contract), `realm_audit.py` (realm export vs platform rules) | runnable; covered by `tests/` |
| `tests/` | pytest suite for the automation scripts (T-iam-010…032 in the qa-test-strategy taxonomy) | `python -m pytest examples/tests` → 16 passed |

Host names (`sso.hkm.example`, `*.hkm.example`), tenant `t-acme` and registry paths are placeholders.
