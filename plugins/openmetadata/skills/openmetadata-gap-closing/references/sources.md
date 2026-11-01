# Sources

- Internal capability and gap research on OpenMetadata 1.13.3 (`1.13.3-release`, commit
  `255f6694913b84797064a42859cda3f2a3425dc6`), August–September 2026. It used a frozen
  90-capability rubric and per-capability evidence records (score, evidence type, `path:line`,
  note). Material used here:
  - the gap implementation guide (24 gaps, impact × ease ranking, Parts A–C, appendix of
    limitations), including the later correction that raised gap 4.3 ease from 3 to 4 after the
    `securityService` scaffold was found;
  - the prioritised gap table (45 gaps, selection rule, competitive vs absolute split);
  - the gap analysis (method, class rules S1–S5, issue-tracker validation, impact assessment,
    LLM/agentic/ontology theme marks).
  Organisation-specific context (named systems, domain examples, the target platform report) and
  the closed-source vendor comparison are omitted.
- OpenMetadata source at `1.13.3-release`: all `path:line` citations. Items marked
  "[verified here]" were checked directly in the source while writing this skill:
  - `entityRulesSettings` write-time enforcement (`RuleEngine.java`, `EntityRepository.java`);
  - `periodicBatchEntityTrigger` cron scheduling;
  - `NLQServiceFactory` / `naturalLanguageSearch.providerClass`;
  - `conceptMappingType` values, including `SAME_AS`;
  - no `extension` field on team, user and policy schemas;
  - UI entity-reference targets;
  - glossary CSV columns;
  - absence of `policyAgentPipeline.json` and `supportsMaskedAccess`.
- Comparator sources cited by the research: DataHub v1.7.0
  (`StructuredPropertyDefinition.pdl`, `saasOnly` feature gating) and Apache Atlas 2.5.0
  (`AtlasStructDefStoreV2.java`, `AtlasAuthorizerFactory.java`).
- Official OpenMetadata documentation: docs.open-metadata.org/latest/how-to-guides/data-governance
  and /latest/connectors (evidence type D), plus the data-quality anomaly-detection page (the
  ambiguity for D5.6).
- Issues and PRs (evidence type İ): OpenMetadata #27396 (policy agent, merged, targets 2.0.0),
  #28595 (masked access), #23135 (preview column shuffling), #12558 (MDM, closed); DataHub
  RFC #17242 (bilateral data contracts, open), #4716 (Delta Sharing, closed unmerged); ATLAS-3874.
- KVKK references (Law No. 6698; by-law on erasure, destruction or anonymisation of personal
  data; KVKK Board principle decision 2026/1301) are as cited in the research. This skill is
  not legal advice.
