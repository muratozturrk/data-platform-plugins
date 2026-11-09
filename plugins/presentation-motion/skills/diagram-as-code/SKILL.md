---
name: diagram-as-code
description: Diyagramları veri/koddan üretme - architecture.yaml şeması, Airflow DAG / dbt manifest / OpenLineage ayrıştırma, Mermaid veya D2'yi önceden SVG'ye çevirip GSAP ile adım adım açma, yerleşim kuralları. Mimari, akış, pipeline veya lineage diyagramı gereken her sahnede kullan.
---

# Diagram-as-code

## architecture.yaml şeması (architecture-layers şablonu)
```yaml
eyebrow: "Mimari"
title: "Kaynaktan rapora veri akışı"
layers: [{ id, label }]                     # soldan sağa sütunlar (≤ 5)
nodes:  [{ id, label, sub?, layer }]        # katman başına ≤ 5
edges:  [{ from, to, flow: stream|batch }]  # stream = eşit aralıklı partikül, batch = paket
```
Aynı katmandaki düğümler arası kenar dikey çizilir (bronze → silver → gold).

## Kaynaklardan grafik çıkarma
| Kaynak | Yöntem |
|---|---|
| Airflow DAG (py) | Statik okuma: `a >> b`, `[a, b] >> c`, `a.set_downstream(b)`, TaskFlow çağrı zinciri. Kodu çalıştırma. |
| dbt `manifest.json` | `nodes[k].depends_on.nodes` → kenar; `resource_type` → grup; `sources`, `exposures` uçlar |
| OpenLineage | `RunEvent.inputs[]/outputs[]` → dataset ↔ job iki parçalı grafik |
| Mermaid / D2 | **Önceden** SVG'ye çevir (`mmdc -i x.mmd -o x.svg` / `d2 x.d2 x.svg`), SVG'yi sahneye göm, `g.node` ve `path.edge` öğelerini GSAP ile sırayla aç. Mermaid'i çalışma anında yükleme (~960 KB). |

## Yerleşim kuralları
- Akış soldan sağa (Türkçe okuma yönü). Geri besleme kenarları alttan kavisli.
- Katman/düğüm sınırını aşınca grupla: "3 operasyonel kaynak".
- Kenar kesişmelerini azaltmak için katman içi sırayı bağlı olduğu düğümlerin ortalama sırasına göre diz (barycenter).
- Açılış sırası anlatım sırasıdır: `reveal` alanı yoksa katman sırası.

## Detay seviyesi
`executive`: işlev adları, hacim yok · `technical`: teknoloji adı + hacim + SLA.
