# Sources

- Azure Data Factory documentation: concepts of pipeline, activity, dataset, linked service (`connectVia` integration runtime), trigger; JSON definitions; publish-from-Git. https://learn.microsoft.com/azure/data-factory/
- Azure Databricks / Databricks architecture overview: control plane (web app, orchestration, scheduling, cluster management) vs classic and serverless compute planes. https://learn.microsoft.com/azure/databricks/getting-started/overview
- Kubernetes operator pattern and CRD conventions; Crossplane compositions; Stakater Cloud Orchestrator as an example of Organization → Keycloak realm/groups reconciled by Crossplane. https://docs.stakater.com/stakater-cloud-orchestrator/latest/integrations/keycloak.html
- Zhamak Dehghani, *Data Mesh*: data product developer experience plane, infrastructure provisioning plane, supervision plane; data product manifest.
- Fournier & Nowland, *Platform Engineering*: platform APIs, migrations, operating platforms at scale.
- Config-driven ETL prior art (Samara, dpone, Drune, factl) for manifest/registry/validate/render CLI shapes.
- Strimzi `KafkaUser` ACL/quota CRs; Spark operator `SparkApplication`; Altinity `ClickHouseInstallation`; OpenMetadata REST API; OpenLineage specification.
