# control-plane examples

ADF-inspired resource kinds for the control plane. All files omit tenant/environment: they come from `TenantContext`.

- `tenant.crd.yaml` – `Tenant` CRD (what the Tenant controller reconciles across every component)
- `linkedservice.yaml`, `integrationruntime.yaml`, `trigger.yaml` – the kinds missing from the basic manifests
- `policy.yaml` – one policy definition compiled to OPA data + engine grants
