---
description: POC/MVP demo videosu iskeleti kurar - Problem → Mimari → Pipeline → Canlı sonuç → Metrikler → İstek
argument-hint: "<POC adı>" [--app superset|airflow|jupyter|generic] [--url http://...]
---

Girdi: `$ARGUMENTS`. Referans örnek: `${CLAUDE_PLUGIN_ROOT}/examples/26-poc-realtime-pipeline/`.

Kullanıcıdan topla (tek mesajda sor): problem (tek cümle + mevcut durum sayısı), mimari bileşenler, demo edilecek ekran/komut, ölçülen sonuçlar (önce/sonra), istenen karar, kitle.

Sonra `demo.yaml` üret (2–3 dk):
| # | Segment | Şablon/araç |
|---|---|---|
| 1 | Problem / özet | `bluf-headline` |
| 2 | Mimari | `architecture-layers` + `data/architecture.yaml` |
| 3 | Pipeline çalışıyor | `terminal` (veya VHS tape) |
| 4 | Canlı sonuç | `browser` segmenti (demo-scripter) |
| 5 | Metrikler | `kpi-counter-row` |
| 6 | Karar / sonraki adım | `decision-ask` |

Eksik rakamları `[VERİ GEREKLİ]` olarak bırak ve listele. Sonra `/presentation-motion:demo`.
