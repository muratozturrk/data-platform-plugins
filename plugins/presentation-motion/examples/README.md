# Örnek projeler

✅ çalışır · 🧩 iskelet (deck.yaml + demo.yaml hazır; eksik şablonlar 📝) · 📝 planlı

| # | Örnek | Kitle | Durum | İçerik |
|---|---|---|---|---|
| 01 | thesis-defense | akademik | 📝 | formül, grafik, atıf |
| 02 | conference-talk | teknik | 📝 | kod diff, diyagram, el çizimi vurgu |
| 03 | quarterly-report | yönetici | 📝 | KPI sayaçları, bar/çizgi |
| 04 | product-intro-30s | genel | 📝 | kinetik tipografi, maske açılışı |
| 05 | codrops-inspired | genel | 📝 | SVG maske, kinetik SVG tipografi, shader geçişi (videoya uyarlanmış) |
| 06 | lecture-explainer | akademik | 📝 | adım adım süreç, path üzerinde hareket |
| 07 | data-platform-architecture | teknik/ürün | 📝 | architecture.yaml → akışlı mimari |
| 08 | pipeline-dag-walkthrough | teknik | 📝 | Airflow/dbt DAG çalışması, retry |
| 09 | sql-to-insight | teknik/ürün | 📝 | sorgu → tablo → grafik |
| 10 | code-talk-live-coding | teknik | 📝 | terminal, diff, test çıktısı |
| 11 | product-launch-90s | ürün/yönetici | 📝 | yeniden çizilmiş UI, sanal imleç |
| 12 | product-feature-tour | ürün | 📝 | 5 özellik × 20 sn |
| 13 | migration-story | teknik/yönetici | 📝 | eski → yeni, maliyet eğrileri |
| 14 | data-quality-observability | teknik/ürün | 📝 | freshness, SLA, anomali |
| 15 | lineage-impact-analysis | teknik/ürün | 📝 | zincirleme etki |
| 16 | benchmark-results | teknik | 📝 | yarışan barlar, log ölçek |
| 17 | incident-postmortem | teknik/yönetici | 📝 | olay zaman çizelgesi |
| 18 | executive-briefing-5min | yönetici | 🧩 | BLUF: istek → 3 kanıt → karar |
| 19 | board-roi-business-case | yönetici | 📝 | waterfall ROI, senaryolar |
| 20 | government-briefing | kamu | 📝 | resmi üslup, il haritası, KVKK |
| 21 | public-sector-digital-transformation | kamu | 📝 | olgunluk modeli, kurumlar arası akış |
| 22 | one-deck-three-audiences | hepsi | 📝 | aynı içerik → 3 varyant |
| 23 | superset-dashboard-tour | ürün/teknik | 📝 | filtre → grafik → drill-down (bkz. 26'daki demo.superset.yaml) |
| 24 | superset-sqllab-to-chart | teknik | 📝 | SQL Lab → grafik oluşturma |
| 25 | superset-embedded-in-product | ürün | 📝 | gömülü dashboard |
| 26 | **poc-realtime-pipeline** | yönetici/teknik | ✅ | HTML sahneler + gerçek tarayıcı kaydı + KPI + karar; 57 sn, 2,1 MB |
| 27 | mvp-release-demo | ürün | 📝 | sürüm notları + kayıt |
| 28 | airflow-run-and-monitor | teknik | 📝 | Airflow UI kaydı |
| 29 | jupyter-analysis-walkthrough | teknik/akademik | 📝 | hücre hücre çalıştırma |
| 30 | test-suite-to-demo | teknik | 📝 | mevcut Playwright testlerinden demo |
| 31 | **poc-superset-selfservice** | yönetici | ✅ | `/poc` iskeleti: BLUF + mimari + terminal + mock Superset kaydı + KPI + karar; gerçek Superset için `demo.superset.yaml` (bkz. klasör README) |
| 32 | **paper-talk-ijepa** | akademik | ✅ | makale sunumu: yeniden çizilmiş mimari diyagramlar, maskeleme animasyonu, formül, nokta grafiği, tablo, ablasyon, APA kaynakça |

## Çalıştırma
```bash
cd examples/26-poc-realtime-pipeline
node ../../capture/compose/compose.mjs demo.yaml        # → out/26-poc-realtime-pipeline-pptx.mp4
```
Örnek klasörü yapısı: `deck.yaml`, `storyboard.md`, `demo.yaml`, (varsa) `architecture.yaml`, `mock-app/`, `capture/`, `out/`.
