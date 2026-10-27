# Sources

- AWS Well-Architected Framework, *SaaS Lens*: Silo, Pool and Bridge models; tenant isolation; SaaS identity. https://docs.aws.amazon.com/wellarchitected/latest/saas-lens/
- AWS Solutions Library, *Guidance for Multi-Tenant Architectures on AWS* (database tier: silo = instance per tenant, bridge = schema per tenant, pool = row-level security).
- Snowflake, *Design Patterns for Building Multi-Tenant Applications on Snowflake* (whitepaper): MTT / OPT / APT patterns and hybrids.
- Databricks Community, *Building Multi-Tenant Architecture on Databricks Platform*: catalog per tenant, workspace binding, external locations and storage credentials per tenant.
- DataHub docs, *Multi-Tenancy and Data Isolation*: domain, platform-instance and separate-deployment approaches.
