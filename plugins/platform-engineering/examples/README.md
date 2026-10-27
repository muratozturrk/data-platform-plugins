# platform-engineering – examples

Concrete starting points referenced by the skills. Copy and adapt; nothing here is customer-specific.

- `manifests/` – declarative pipeline, registry and tenant manifests with the `apiVersion/kind/metadata/spec` shape
- `spi/` – a minimal Python SPI (SourceConnector v1), a default plugin, entry-point discovery and the contract test
- `tenancy/` – `TenantContext` and a tenant-aware storage resolver (silo/pool/bridge by configuration)
- `fitness-functions/` – import-linter rules, a forbidden-token scan and a GitHub Actions job wiring them together
