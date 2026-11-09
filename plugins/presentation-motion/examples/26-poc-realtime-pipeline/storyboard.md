# Gerçek Zamanlı Satış Analitiği – POC

**Ana mesaj:** Raporlar 24 saat yerine 5 dakikada hazır; pilot için onay istiyoruz.
**Kitle:** executive (+ teknik ekip) · **Süre hedefi:** ~1:00 · **Tema:** corporate-dark

| # | Sahne | Mesaj (tek cümle) | Şablon | Süre | Görsel / hareket notu | Veri kaynağı |
|---|---|---|---|---|---|---|
| 1 | Açılış | POC'nin adı ve kapsamı | title | 5 sn | başlık harf harf | — |
| 2 | Özet (BLUF) | 288× daha hızlı karar verisi | bluf-headline | 6 sn | sayaç | POC ölçümleri |
| 3 | Mimari | Akış + toplu alım tek lakehouse'ta birleşiyor | architecture-layers | 15 sn | katman katman, partiküller | architecture.yaml |
| 4 | Pipeline | Dönüşümler tek komutla, testlerle | terminal | 7 sn | yazılıyor efekti | dbt çıktısı (örnek) |
| 5 | Canlı sonuç | Filtre değişince dashboard anında güncelleniyor | browser (mock-app) | ~10 sn | caption, zoom, spotlight | demo veritabanı |
| 6 | Etki | Gecikme, maliyet, kalite | kpi-counter-row | 6 sn | sayaçlar | POC ölçümleri |
| 7 | Karar | Pilot onayı | decision-ask | 7 sn | çerçeve çizimi, tikler | — |

## Açık sorular
- KPI rakamları örnektir → gerçek POC ölçümleriyle değiştirilmeli `[VERİ GEREKLİ]`.
- Gerçek Superset kaydı için `demo.superset.yaml` selector'ları doğrulanmalı.
