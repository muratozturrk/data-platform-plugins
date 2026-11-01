# Document Anonymisation

Free-text documents (PDF, DOCX, XLSX, scans) are a different assurance class from tables. In a table
k is a number you compute over every row; in a document there are no groups to count — only
**recall and precision**, which are performance estimates, not guarantees. Every accuracy figure
below is someone else's measurement; none was reproduced on Turkish business documents.
Tags: **D** docs/primary publication, **L** literature, **(derived)** this skill's reasoning,
**(not verified)** searched without a confirming primary source.

## 1. Pipeline

```
1. METADATA STRIPPING     → the only step with an exact guarantee
2. FORMAT NORMALISATION   → is there a text layer?
3. OCR (if needed)        → weak link for Turkish
4. DETECTION              → two engines: pattern+validation and model
5. REGENERATION           → build a clean document from scratch
```

Order matters: metadata stripping does not depend on detection, and regeneration (not editing) is
what makes removal real.

## 2. Format failures — not NER problems, and exactly solvable

### 2.1 Fake redaction (D)

PDF has separate visual and text layers. A black rectangle drawn over text hides the visual layer;
the text layer stays and is extractable with basic tools. The NSA guide *Redacting with
Confidence: How to Safely Publish Sanitized Reports Converted From Word to PDF* (2005) names two
common errors — drawing an image over text without removing it, and setting the background colour
to the text colour — and requires that sensitive content be **actually removed**. A 2019 US court
filing redacted this way exposed the hidden text.

### 2.2 Incremental save (D)

When a PDF is edited and saved incrementally, new content is appended and the **original content
stays in the byte stream**; tools such as QPDF recover earlier revisions.

Consequence (derived): the anonymised output must never be an edited copy of the source. Generate a
new document; carry no source bytes. This is the document equivalent of "write the anonymised table
to a new table, never overwrite the source".

### 2.3 Metadata (D)

Even with clean visible text a file keeps: author, creation/modification dates, software version,
revision history, comments, tracked changes, total editing time, internal network paths, sometimes
GPS coordinates.

- **Turning off tracked changes does not delete history**; traces can survive even "accept all".
- **A file path alone leaks**: `S:\Clients\Acme Corp\Matter 17 — Regulatory Inquiry\draft.docx`
  reveals client, matter number and nature of the work.

Metadata fields are enumerable and can all be removed — there is no probability here. This is the
only place on the document side where an exact statement is possible, so the field inventory per
format (PDF, DOCX, XLSX) must itself be complete and checked at byte level after cleaning.

## 3. OCR — first link, weak in Turkish

| Setting | Tesseract accuracy | Source |
|---|---|---|
| Scanned Turkish documents | **86.41 %** | Single study; dataset not identified (not verified) |
| English documents | 98–99 % | Same line of literature |

Tesseract still beat CuneiForm and GOCR on Turkish, so the gap is attributed to the language
(vowel harmony, agglutinative morphology, diacritics where a missing dot changes the word: ı/i,
ş/s, ğ/g) and partly to model quality.

**Errors chain (derived).** A national ID (T.C. Kimlik No) carries check digits; a validating
detector gets high precision from them. If OCR misreads one digit, validation fails, the number is
**not** classified as an ID, it stays in the output, and nothing reports an error. Measure
end-to-end on your own scans; never multiply OCR accuracy by NER F1 — the errors are not
independent and some are silent. Record OCR quality per document as an input-quality indicator and
route low-quality documents to human review.

## 4. Turkish NER — published numbers vs reality

| Model | Dataset | F1 |
|---|---|---|
| TURNA-Encoder | WikiANN (tr) | 95.59 |
| BERTurk | WikiANN (tr) | 94.17 |
| BERTurk (cased, 128k) | WikiANN (tr), 5-run mean | 93.92 (test) |
| BERTurk | Milliyet news | 94.17 |

The same survey reports BERTurk on historical Turkish: **91.21 in-domain dev → 61.91 out-of-domain
test**, about a 30-point fall, despite fine-tuning on modern and historical text. Figures come
from search-result abstracts of *Turkish Named Entity Recognition: A Survey and Comparative
Analysis*; the primary PDF was unreachable (not verified against the full text).

WikiANN is encyclopaedia text. Contracts, petitions, medical abbreviations, addresses inside tables,
names in capitals and OCR debris are all out of domain. Quoting ~94 for such documents is not
defensible; measure on the actual document types.

### 4.1 NER labels are not personal-data labels

WikiANN tags three types: Person, Location, Organisation (D). Most identifiers need other methods:

| Entity | Classic NER? | Method |
|---|---|---|
| Person name | ✅ | Model (+ dictionary) |
| Address | Partly (Location) | Model + dictionary |
| Organisation | ✅ | Model |
| T.C. Kimlik No, VKN, IBAN | ❌ | Pattern + check-digit validation |
| Phone, e-mail, URL, IP | ❌ | Pattern |
| Date of birth | Partly | Pattern + context |
| Social-security, land-registry, case numbers | ❌ | Institution-specific pattern |

Hence **two engines** with different assurance: pattern+validation is deterministic and
explainable (report the rule name); the model is probabilistic (report the confidence). Show them
separately — the same split ARX uses for tables (name vs value matchers, IBAN validation).

### 4.2 Presidio as a skeleton (D)

Microsoft Presidio combines regex, NER and custom logic per recogniser on spaCy, Stanza or
HuggingFace transformers. Its language docs mention English and Spanish and give no complete list;
Turkish is not listed (neither "supported" nor "unsupported" is stated). Adding a language needs
(1) an NLP model for it and (2) recogniser changes — *"context words used to increase the PII
detection confidence aren't"* language-independent. Usable as a skeleton; the Turkish model,
context words and TR recognisers are all extra work. Behaviour on Turkish text was not tested.

## 5. What guarantee can be given

### 5.1 The ceiling even in the best case (L)

i2b2 2014 clinical de-identification (English, one narrow domain, years of annotated data): one of
the best systems reached F1 0.9185, recall 0.9409, precision 0.8972; coarser 7-label sets reach
micro precision ≈ 0.955, with dates and streets near 0.995. The literature: *"95% has been suggested
as a rule-of-thumb"* and *"perfect de-identification may be unachievable, with the best performances
expected being around 0.95 or slightly higher."* Turkish, mixed document types and OCR meet none of
those conditions.

### 5.2 Asymmetry: you can show what you found, not what you missed

| | Structured data | Documents |
|---|---|---|
| Measured quantity | min k — a number | Recall/precision — an estimate |
| Computed on | All data, exactly | A hand-annotated gold sample |
| Reproducible | Yes — same snapshot, same result | Only if model version, threshold and gold set are fixed |
| "Nothing was missed"? | Question is meaningless — every group was counted | **Never** |

Precision is measurable (inspect what was flagged). Recall needs to know what is really in the
document — a gold set, which is a sample; error types absent from the sample are invisible.

### 5.3 Statements you can and cannot make

Can (derived):
- "N identifiers of types … were found in this document set and removed by method …".
- "All metadata fields were removed" — exact, given a complete field inventory.
- "The output was generated from scratch; no source bytes were carried over" — exact.
- "Measured recall on your document sample is X %, on [date], model [version], threshold [t],
  gold set of [n] documents."

Cannot:
- "This document is anonymous." / "All personal data was removed."
- "The model is 94 % accurate" — unless measured on these documents.

### 5.4 Managing the uncertainty

1. Every detection carries a confidence and a reason (engine, rule, context).
2. **Below-threshold detections are not dropped silently** — they go to a "review" list.
3. Sample-based human review is part of the process: reviewers check a share of documents, misses
   join the gold set, the recall estimate is updated.
4. The evidence record stores the **conditions** of measurement (model name/version, threshold,
   gold-set size and date, measured recall/precision, OCR quality) — never document content or the
   detected identifier values.

## 6. Tool landscape

| Tool | Free-text anonymisation | Turkish |
|---|---|---|
| ARX | No — structured data only; matchers work on column names/values | — |
| Microsoft Presidio | Yes | Not listed |
| Commercial tools | Not surveyed | Not surveyed |

Legal treatment of free-text anonymisation under KVKK or EU rules was out of the research's scope.

## 7. Still to measure before relying on numbers

| # | Measurement | Method |
|---|---|---|
| 1 | End-to-end recall and precision on your own documents | Annotate a representative gold set; run OCR'd and non-OCR'd separately |
| 2 | Turkish OCR accuracy on your scans | Character error rate **and** identifier-level error rate |
| 3 | Turkish NER survey figures | Confirm from the primary paper |
| 4 | T.C. Kimlik No / VKN check-digit rules | From the issuing authority's official publication |
| 5 | Presidio on Turkish | Trial run on a small Turkish sample |
| 6 | Metadata field inventory per format | Full list per format; byte-level audit after cleaning |
