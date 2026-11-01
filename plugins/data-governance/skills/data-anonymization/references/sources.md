# Sources

## Research files used (paths inside the research repository)

| File | Used for |
|---|---|
| `research/analysis/arx-kod-analizi.md` | ARX algorithm, data structures, memory model, 21 privacy criteria, metric decomposability, bottlenecks D1–D9, parallelisability, code-cited papers |
| `research/analysis/dagitik-anonimlestirme-literatur.md` | Distributed/parallel algorithms, verified scales, approximate methods, DP tools, streaming, PB assessment |
| `research/analysis/musteri-ortami-ve-risk-raporu.md` | Risk report anatomy, identifier matchers and US-only locale, QI discovery (Motwani & Xu), per-engine statistics and parallelism, sample-row rules |
| `research/analysis/dokuman-anonimlestirme.md` | Document pipeline, fake redaction, incremental save, metadata, OCR, Turkish NER, i2b2 ceiling, Presidio |
| `research/analysis/anonimlestirme-mimarisi.md` (§2, §3, §6.1–6.4, §7, §8.1–8.3, §9, §10, §11.4, §12 — technical parts) | One-sided NDV test, Iceberg metadata traps, exact-measurement SQL, metric choice, ARX semantics vs code, shuffle/bucketing facts, PostgreSQL/Citus limits, KVKK link, evidence limits |
| `research/analysis/anonimlestirme-mimarisi-v2.md` (§5, §6, §8, §9 — technical parts) | Discovery layer, risk report rules, evidence-ledger fields, document mode |
| `report/Anonimlestirme-Motoru-Raporu.md` (§8 technical parts) | Legal framing and evidence limits |
| `research/analysis/1pb-sistem-muhendisligi.md` (via the architecture document's citations) | Generic shuffle volume, Spark/Iceberg flags, Iceberg metadata fields |
| `research/rubric/kvkk-mevzuat.md` | Verified KVKK article numbers and durations |

Excluded: organisation-specific platform sizing and hardware, sector examples, product strategy
(positioning, licensing, phasing as a product roadmap, deployment business choices) and all
closed-source vendor material.

## Source code

- **ARX** — `https://github.com/arx-deidentifier/arx`, commit `4e0a5f5` ("Experimental mvn build
  (#468)"), shallow clone, inspected 2026-09-08. Core Apache 2.0; some `package-info` files GPLv3.
  Paths in this skill are relative to `src/main/org/deidentifier/arx/`. Lattice library JHPL
  (`lib/ant/jhpl/jhpl-0.0.1.jar`) is binary-only.

Spot-checked for this skill against the clone (all confirmed unless noted): `DataMatrix.java:57-66`;
`ARXConfiguration.java:288-294,387-393`; `ARXAnonymizer.java:154-166,654-664`;
`FLASHAlgorithmImpl.java:92-99`; `SolutionSpace.java:78-85,374-381`; `PrivacyCriterion.java:54-58,181`;
`KAnonymity.java:103-106`; `SampleBasedCriterion.java:55-66`; `EDDifferentialPrivacy.java:210-228,259-272`;
`MetricMDNUEntropyPrecomputed.java:256-292`; `MetricMDNMLoss.java:427-434`;
`MetricSDClassification.java:430-439`; `AbstractTransformer.java:37`; `Transformer.java:405`;
`TransformationCheckerStateMachine.java:56-66`; `HashGroupifyDistribution.java:230`;
`AverageReidentificationRisk.java:165`; `RiskModelAttributes.java:38-41,176-187,246-273,317`;
`RiskModelSampleSummary.java:30-36,44,64,90,128-151`; `RiskModelSampleRisks.java:55-59,128-133`;
`HIPAAConstants.java:40-50`; `HIPAAMatcherAttributeName.java:20-24,38,106`;
`HIPAAMatcherAttributeValue.java:199-206`; `risk/resources/us/*` line counts; `data/adult.csv`;
no executor/thread use in `src/main`. Corrected line numbers: initial groupify capacity is
`TransformationChecker.java:121` (research: `:120`); the output buffer is `Transformer.java:107-108`
(research: `:110-111`).

## Literature

**Privacy models and metrics**
- Sweeney L. (2002). Achieving k-anonymity privacy protection using generalization and suppression. IJUFKS 10(5).
- Machanavajjhala A., Kifer D., Gehrke J. (2007). ℓ-diversity: Privacy beyond k-anonymity. ACM TKDD 1(1).
- Li N., Li T., Venkatasubramanian S. (2007). t-Closeness: Privacy beyond k-anonymity and ℓ-diversity. ICDE.
- Nergiz M., Atzori M., Clifton C. δ-presence. · Brickell J., Shmatikov V. δ-disclosure. · Cao J., Karras P. β-likeness (cited in ARX javadoc).
- El Emam K., Dankar F. (2008). Protecting privacy using k-anonymity. JAMIA 15(5):627-637. DOI 10.1197/jamia.M2716 (citation only).
- El Emam K. Guide to the De-Identification of Personal Health Information; Measuring the Probability of Re-Identification (cited in ARX, not read).
- Dankar F., Zayatz L., Pannekoek J. — population uniqueness estimators (cited in ARX).
- Bild R., Kuhn K.A., Prasser F. (2018). SafePub: A truthful data anonymization algorithm with strong privacy guarantees. PoPETs 2018(1):67-87.
- Li N., Qardaji W., Su D. (2012). On sampling, anonymization, and differential privacy. ASIACCS.
- McSherry F., Talwar K. (2007). Mechanism design via differential privacy. FOCS.
- LeFevre K., DeWitt D.J., Ramakrishnan R. (2006). Mondrian multidimensional k-anonymity. ICDE. DOI 10.1109/ICDE.2006.101.
- De Waal A., Willenborg L. (1999). Information loss through global recoding and local suppression. Netherlands Official Statistics 14.
- Goldberger J., Tassa T. (2010). Efficient anonymizations with enhanced utility. Transactions on Data Privacy 3:149-175.
- Wan Z., Vorobeychik Y., Xia W., Clayton E.W., Kantarcioglu M., Ganta R., Heatherly R., Malin B.A. (2015). A game theoretic framework for analyzing re-identification risk. PLOS ONE.

**Search algorithms and tools**
- Kohlmayer F., Prasser F., Eckert C., Kemper A., Kuhn K.A. (2012). Flash: Efficient, stable and optimal k-anonymity. PASSAT/SocialCom.
- Prasser F., Bild R., Eicher J., Spengler H., Kohlmayer F., Kuhn K.A. (2016). Lightning: Utility-driven anonymization of high-dimensional data. Transactions on Data Privacy 9(2):161-185.
- Prasser F., Eicher J., Spengler H., Bild R., Kuhn K.A. (2020). Flexible data anonymization using ARX — Current status and challenges ahead. Software: Practice and Experience 50(7):1277-1304. DOI 10.1002/spe.2812.
- LeFevre K., DeWitt D.J., Ramakrishnan R. (2005). Incognito: Efficient full-domain k-anonymity. SIGMOD. DOI 10.1145/1066157.1066164.
- Motwani R., Xu Y. (2007). Efficient algorithms for masking and finding quasi-identifiers. VLDB (full text checked).

**Distributed, parallel, approximate, streaming**
- De Capitani di Vimercati S., Facchinetti D., Foresti S., Livraga G., Oldani G., Paraboschi S., Rossi M., Samarati P. (2023). Scalable distributed data anonymization for large datasets. IEEE TBD 9(3):818-831. DOI 10.1109/TBDATA.2022.3207521. Code: `github.com/mosaicrown/mondrian`.
- Bazai S.U., Jang-Jaccard J., Alavizadeh H. (2021). Scalable, high-performance, and generalized subtree data anonymization approach for Apache Spark. Electronics 10(5):589. DOI 10.3390/electronics10050589.
- Halilovic M., Meurers T., Otte K., Prasser F. (2025). Parallel privacy preservation through partitioning (P4). BMC Medical Informatics and Decision Making. DOI 10.1186/s12911-025-02959-z.
- Reddy K., Chakraborty N., Dharmavaram A., Tandon A. (2025). SKALD: Scalable k-anonymisation for large datasets. arXiv:2505.03529.
- Ashkouti F., Khamforoosh K. (2023). A distributed computing model for big data anonymization in the networks. PLOS ONE. DOI 10.1371/journal.pone.0285212.
- Zhang X., Yang L.T., Liu C., Chen J. (2014). Two-phase top-down specialization using MapReduce. IEEE TPDS 25(2):363-373 (citation only).
- Zhang X., Liu C., Nepal S., Yang C., Dou W., Chen J. (2014). Hybrid sub-tree anonymization using MapReduce. JCSS 80(5):1008-1020 (citation only).
- Chia P.H., Desfontaines D., Perera I.M., Simmons-Marengo D., Li C., Day W.-Y., Wang Q., Guevara M. (2019). KHyperLogLog. IEEE S&P, 350-364.
- Desfontaines D., Lochbihler A., Basin D. (2019). Cardinality estimators do not preserve privacy. PoPETs 2019(2):26-46.
- Cormode G., Muthukrishnan S. (2005). The Count-Min sketch. J. Algorithms 55(1):58-75.
- Charikar M., Chaudhuri S., Motwani R., Narasayya V. (2000). Towards estimation error guarantees for distinct values. PODS.
- Harmouch H., Naumann F. (2018). Cardinality estimation: An experimental survey. PVLDB 11(4).
- Nanayakkara P. et al. Lessons learned: Surveying the practicality of differential privacy in the industry. arXiv:2211.03898.
- Cao J., Carminati B., Ferrari E., Tan K.-L. (2011). CASTLE: Continuously anonymizing data streams. IEEE TDSC 8(3) (citation only).
- Groneberg P., Nuñez von Voigt S., Janke T., Loechel L., Wolf K., Grünewald E., Pallas F. (2025). Prink: k_s-anonymization for streaming data in Apache Flink. arXiv:2505.13153.

**Documents**
- NSA (2005). Redacting with Confidence: How to Safely Publish Sanitized Reports Converted From Word to PDF.
- i2b2 2014 de-identification overview: `https://www.sciencedirect.com/science/article/pii/S1532046415001173`; Beyond Accuracy (arXiv:2312.08495).
- BERTurk: `https://github.com/stefan-it/turkish-bert`; Turkish NER survey: `https://openreview.net/pdf?id=xCcOlhiNyu` (primary PDF unreachable — figures not verified against full text).
- Turkish Tesseract accuracy 86.41 %: single study, citation not verified.

## Tools and documentation

PipelineDP (`github.com/OpenMined/PipelineDP`), PipelineDP4j (`github.com/google/differential-privacy`),
Tumult Analytics (`github.com/opendp/tumult-analytics`), OpenDP (`github.com/opendp/opendp`),
Microsoft Presidio (`microsoft.github.io/presidio`), AWS Glue DataBrew PII docs, Google Cloud
Sensitive Data Protection k-anonymity docs, PostgreSQL `pg_stats`, MySQL 8.0 §10.9.6 / §28.3.11,
Oracle 18c Histograms and 19c Extended Statistics / Parallel Execution, SQL Server
`DBCC SHOW_STATISTICS` and cost threshold for parallelism, Apache Hive Column Statistics, Spark
`ANALYZE TABLE`, Flink execution mode, Iceberg table spec and Spark configuration. Repository facts
(stars, last push, "not for production") as recorded by the research on 2026-09-08.

## Legal texts (not legal advice)

| Instrument | Use here |
|---|---|
| Law No. 6698 (consolidated) | Art. 4/2(ç), Art. 28/1(b); Art. 3/1(b) definition (not in the research's verified table) |
| Regulation on Erasure, Destruction or Anonymisation of Personal Data, Official Gazette 28/10/2017-30224 | Art. 4/1(c), 7/3, 10, 11/2 |
| Board Principle Decision 2026/1301 (public-law controllers publishing on the internet) | Masking/anonymisation for published data |
| KVKK Board guide on erasure, destruction and anonymisation | Mentioned only; not verified by the research |
| Article 29 Working Party Opinion 05/2014 on Anonymisation Techniques (WP216) | k-anonymity not sufficient alone; singling out / linkability / inference (via secondary sources) |
| ICO anonymisation guidance; ISO/IEC 20889:2018 | Background |
| EDPB draft anonymisation guidelines | Reported by a secondary source; primary text not accessed |
