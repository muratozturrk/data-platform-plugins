# Şablon kataloğu

✅ hazır ve test edildi · 📝 planlı (yoksa artist + animator `scenes/` altında özel sahne olarak üretir)

Her şablon: tek HTML dosyası, `pm-data` JSON bloğuyla sürülür, tek paused GSAP timeline, ~29–33 KB gzip JS.
Kullanım (`demo.yaml`): `{ type: html, template: <ad>, data: {...} }` veya `source: dosya.yaml`.

## Genel
| Şablon | Durum | Veri alanları | Hareket |
|---|---|---|---|
| `title` | ✅ | eyebrow, title, subtitle, author, date | harf harf maske, çizgi, alt başlık |
| `section-cover` | ✅ | number, title, hint | Codrops tarzı dairesel clip-path açılışı, sayaç |
| `bullets` | ✅ | eyebrow, title, items[{text, detail}], highlight | bar + maske, vurgu: diğerleri solar, el çizimi alt çizgi |
| `chart` | 📝 | type(bar/line), labels, series, source | eksen → bar/çizgi |
| `formula` | 📝 | latex, terms | KaTeX parça parça |
| `diagram-flow` | 📝 | steps | kutu + ok çizimi |
| `timeline` | 📝 | events[{date, label}] | çizgi + noktalar |
| `comparison` | 📝 | left, right, rows | iki kolon, kazanan vurgusu |
| `quote` | 📝 | text, author | kelime kelime |
| `code-diff` | 📝 | steps[code] | magic move |
| `closing` | 📝 | message, contact | sade kapanış |

## Veri ve mimari
| Şablon | Durum | Not |
|---|---|---|
| `architecture-layers` | ✅ | `architecture.yaml`; katman katman açılış + stream/batch partikülleri |
| `data-flow-particles` | 📝 | tek akış hattı, yoğunluk = hacim |
| `dag` | 📝 | Airflow/dbt; görev durum renkleri, retry |
| `lineage-graph` | 📝 | etki analizi, BFS katmanları |
| `medallion` | 📝 | bronze/silver/gold detay |
| `schema-erd` | 📝 | tablo ve ilişkiler |
| `streaming-vs-batch` | 📝 | yan yana karşılaştırma |
| `cluster-scale` | 📝 | düğüm sayısı artışı |

## Kod
| Şablon | Durum | Not |
|---|---|---|
| `terminal` | ✅ | lines[{cmd} \| {out, cls: ok/warn/err/dim}], typeSpeed |
| `sql-result-table` | 📝 | sorgu → tablo → grafik |
| `api-request-response` | 📝 | istek/yanıt balonları |
| `log-stream` | 📝 | akan log, eşleşen satır vurgusu |
| `before-after-perf` | 📝 | iki bar, çarpan etiketi |

## Ürün
| Şablon | Durum |
|---|---|
| `ui-mock-frame`, `cursor-path`, `zoom-callout`, `feature-card`, `pricing-tier`, `integration-logos-grid` | 📝 |

## Yönetici
| Şablon | Durum | Veri alanları |
|---|---|---|
| `bluf-headline` | ✅ | kicker, headline, number, suffix, label, source |
| `kpi-counter-row` | ✅ | eyebrow, title, items[{label, from, to, digits, unit, prefix, delta, tone}], source |
| `decision-ask` | ✅ | eyebrow, title, ask, items[], deadline |
| `waterfall-roi`, `risk-matrix`, `roadmap-gantt`, `scenario-comparison`, `metric-tree` | 📝 | |

## Kamu
| Şablon | Durum |
|---|---|
| `turkey-province-map` (81 il choropleth), `org-protocol`, `compliance-checklist`, `maturity-model`, `stakeholder-flow` | 📝 |

## Yeni şablon ekleme kontrol listesi
1. `_base/tokens.css` + `_base/stage.css` + `_base/timeline-runtime.js` kullan; ham renk yok.
2. Varsayılan `pm-data` gerçekçi Türkçe örnek veriyle dolu olsun (şablon tek başına açılınca çalışsın).
3. `node ../scripts/lint-determinism.mjs <dosya>` temiz, `size-budget` ≤ 60 KB.
4. Global değişken adlarında `top`, `name`, `status`, `parent` gibi `window` özelliklerini kullanma.
5. Bu tabloya ekle.
