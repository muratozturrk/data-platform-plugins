# Sources

- Internal capability/gap research on OpenMetadata 1.13.3 (`1.13.3-release`, commit
  `255f6694913b84797064a42859cda3f2a3425dc6`), compared with DataHub v1.7.0 and Apache Atlas 2.5.0,
  2026: candidate pool (19 capability candidates with gap links, platform integration, on-prem
  LLM rationale, KVKK effect, verified extension points, risks), novelty verification (four
  sources per candidate, verdicts, cross-cutting "empty extension points" finding, eliminated
  ideas), gap analysis (five-class partition, Class 1 issue/roadmap verification, theme mapping),
  and the research design and plan (T8 scoring criteria and weights, technical design contents,
  KVKK impact assessment requirement, on-prem model-realism risk). The scoring and design steps
  were specified but not executed; no scores or rankings are reproduced. Organisation-specific
  requirements, domain-specific systems and closed-source vendor material omitted; candidate C05
  generalised to domain reference data.
- OpenMetadata source at tag `1.13.3-release`: every `path:line` in this skill was either
  verified by the research or spot-checked for this skill (NLQ service/factory, DJL embedding
  client and `conf/openmetadata.yaml` keys, RDF config, `securityServiceMetadataPipeline.json`,
  `slaValidation.json` with no code references, `aiPlatformConfiguration.json`, `app.json`
  `agentType`, `SinkProviderRegistry` registered only from tests, `nodeSubType.json`,
  `NodeFactory`, `customProperty.json`, `TypeResource`, `importer.py`, `processor_factory.py`,
  `document.json`, `EntityLifecycleEventHandler`/`Dispatcher`, `IndexMappingLoader`,
  `DataRetention`, `PIIMasker`, `eventSubscription.json`, `table.json` retention field).
- OpenMetadata GitHub issues/PRs cited (`İ`): #8271, #12558, #22134, #23135, #24538, #26202,
  #27173, #27396, #28477, #28922 / PR #28984, #29392, #29734, #30099, #32048.
- Collate (commercial OpenMetadata) documentation and comparison page: Ranger connector
  ("Reverse Metadata (Collate Only)"), Data Marketplace / Data Access Requests, AI SDK licensing
  (Collate Community License 1.0), OpenMetadata 2.0 AgentExecution docs (`D`).
- DataHub documentation: `docs/managed-datahub/` `<FeatureAvailability saasOnly />` markers,
  Ranger authorization guide, model-card aspects; RFC #17242, PR #15762, PR #18503 (`D`/`İ`).
- Apache Atlas / Ranger: Ranger tag-based policies (cwiki), RANGER-2000 (policy validity
  schedule), RANGER-4978 and apache/ranger PR #544, ATLAS-872, ATLAS-941 (`D`/`İ`).
- Standards and literature named in novelty verdicts: W3C Data Privacy Vocabulary (DPV) incl.
  its issue on temporal validity, SKOS, PROV-O, SHACL; Mitchell et al. 2019 "Model Cards";
  arXiv 2506.22305 (LLM-based contextual PII classification); PRIVEE; systematic reviews of
  data-catalog tools and data lakes (cited generically).
- Commercial products named only as "closest existing solution" (vendor claims, `B`/`D`):
  Collibra, Alation, Microsoft Purview, Databricks Unity Catalog, Snowflake, BigID, OneTrust,
  Relyance AI, Securiti, Immuta, Privacera, Monte Carlo, PoolParty, TopBraid EDG, ARX,
  Google Cloud DLP, IBM watsonx.governance.
- KVKK (Law No. 6698) and secondary regulation as referenced in the research; this skill is not
  legal advice - verify against the official text in force.
