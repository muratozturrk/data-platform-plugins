# Sources

- Internal evidence-based governance research, August 2026: frozen Capability Reference Model
  v1.0 (14 domains, 90 capabilities, 48 sources, frozen 2026-08-11) with its README, changelog and
  regulation mapping; per-product evidence files (score, evidence type, `path:line` or URL, note)
  for OpenMetadata `1.13.3-release` (commit `255f6694913b84797064a42859cda3f2a3425dc6`), DataHub
  `v1.7.0` (`7f81ccbfe27b9acc947f5f600fcf9ddb72138a80`) and Apache Atlas `release-2.5.0`
  (`234608315f29b643c2dd7632cad0e8ef2a15c696`), all inspected 2026-08-11; executive summary,
  full matrix, community sustainability and DMBOK coverage tables; cross-check report with manual
  verification (2026-08-12); research design document (rubric structure, scoring, honesty rules,
  parallel-assessor consistency safeguards, risks); validation and cross-check scripts (checks
  described, code not copied). Organisation-specific context omitted.
- Spot-checked in the OpenMetadata source clone at the pinned commit: `conf/openmetadata.yaml:717`
  (`RDF_ENABLED` default false), `conf/openmetadata.yaml:330-342` (`authorizerConfiguration.className`),
  `ingestion/src/metadata/cmd.py` command enum (no backup/restore command) with
  `ingestion/src/metadata/cli/restore.py` present, `AbstractEventConsumer.java:144-200`
  (failed-event handling). All consistent with the research.
- Framework sources behind the rubric (public): DAMA-DMBOK2 knowledge-area context diagrams
  (KA1-KA11, DAMA International chapter publications, 2017); EDM Council DCAM
  (https://edmcouncil.org/frameworks/dcam/); ISO/IEC 38505-1:2017 governance of data
  (https://www.iso.org/standard/56639.html); ISO 8000-61:2016 data quality management
  (https://www.iso.org/standard/63086.html); DGI Data Governance Framework
  (https://datagovernance.com/the-dgi-data-governance-framework/); ISO/IEC 11179-3:2023 metadata
  registries; ISO/IEC 20889:2018 de-identification techniques; ISO 22301:2019 business continuity;
  Forrester Wave methodology page; Data Mesh Principles and Logical Architecture
  (martinfowler.com, 2020); OpenLineage object model spec; Delta Sharing protocol spec; EU AI Act
  (Regulation (EU) 2024/1689) Article 10; NIST AI RMF 1.0 and its Generative AI Profile; FinOps
  Framework reporting & analytics; Unicode CLDR; academic papers on data context services (CIDR
  2017), provenance extraction (KDD 2020), ontology-based governance (IEEE CSCloud 2016), data
  quality dimensions (JMIS 1996), LLM dataset documentation (arXiv 2404.15320), personalised data
  lake exploration (KAIS 2023), k-anonymity (2002), differential privacy (ICALP 2006), RAG threat
  model (arXiv 2509.20324).
- KVKK sources behind D14 (public, verified against official texts in the research): Law No. 6698
  (as amended by Law No. 7499), the VERBİS (Data Controllers' Registry) Regulation, the
  Regulation on Erasure, Destruction or Anonymisation of Personal Data, the communiqués on the
  duty to inform and on applications to the data controller, Board decisions 2018/10, 2019/10,
  2019/271 and principle decision 2026/1301, the 2024 cross-border transfer regulation, the
  Presidential Circular 2019/12 on information and communication security, and the KVKK public
  sector compliance guide. Detail in the `openmetadata-kvkk-compliance` skill. Not legal advice.
- Community figures: GitHub REST API v3 (unauthenticated) and Apache JIRA REST search, pulled
  2026-08-11; endpoints as described in `openmetadata-results.md`.

## What was changed relative to the research

- **A fourth product was removed.** The study also scored a closed-source commercial product,
  almost entirely from vendor claims. Its column, rows and commentary are omitted everywhere.
  Per-product means for the three open-source products are unaffected (each is computed from that
  product's own evidence file). Recomputed here from the evidence files: the domain **spread**
  column (max - min of the unrounded means over three products; changes D1 1.3→0.6, D9 2.3→1.5,
  D11 1.8→1.3, D14 1.1→0.4 from removing the fourth product, and D6 0.6→0.5 because the research
  subtracted rounded means; others unchanged), the DMBOK leader marks, the overall unweighted means, the score
  distributions and evidence-type counts. All domain and DMBOK means were re-derived from the
  evidence files and match the research tables.
- **Cross-check re-run for three products** (D6.8 and D8.4 all-zero) is this skill's computation;
  the study's run included the fourth product, whose unknown scores suppressed the all-zero rule.
  The notes for those six cells were read and each documents two search places.
- **Capability count.** The design document planned "~72" capabilities; the frozen rubric has
  **90** (D14 grew to 11 after legal review). This skill uses 90.
- **DMBOK numbering.** The design document used DMBOK2 book chapter-style numbers (e.g. KA11
  Metadata); the frozen rubric and the coverage table use their own `dmbok2-ka1..ka11` keys (KA10
  Metadata). This skill follows the frozen rubric.
- **Adjective/product-name scan.** The rubric README and changelog say subjective adjectives and
  product names in level texts are scanned by the validation script; the validator inspected
  (`validate.py`) does not implement such a scan. The procedure here tells you to add your own grep.
- **Null marker.** The study's tooling requires null-score notes to start with the Turkish
  `arandı:` ("searched:"); this skill writes "searched:" and notes the original.
- **Level anchors condensed.** `rubric.md` gives level-2 and level-4 anchors per capability in
  shortened English; level 0, 1 and 3 follow the general scale. Write full level texts into your
  own rubric before scoring borderline cases.
- Organisation-specific motivation for D14 (a named public-sector data domain) and author names
  of framework and academic sources are omitted.
