# Sources

## Verification basis
- A shared lab environment on `apache/superset:4.1.2` with Postgres 16 metadata DB and warehouse, driven through the REST API and the served frontend bundle (about 120 chapter labs). Not re-verified on 6.x.

## Books whose rules the labs implemented (rules are paraphrased in the sibling skills, not quoted)
- Nicholas Desbarats, *Practical Charts* (2023).
- Cole Nussbaumer Knaflic, *Storytelling with Data* (2015).
- Stephen Few, *Information Dashboard Design* (2006).
- Andy Kirk, *Data Visualisation: A Handbook for Data Driven Design*, 2nd ed. (2019).

## Official documentation
- Superset REST API: https://superset.apache.org/docs/api
- Configuring Superset (feature flags, `EXTRA_*_COLOR_SCHEMES`, `D3_FORMAT`): https://superset.apache.org/docs/configuration/configuring-superset
- Chart data / QueryObject schema: Swagger UI at `/swagger/v1` on a running instance.
- Importing and exporting assets: https://superset.apache.org/docs/configuration/importing-exporting-datasources
