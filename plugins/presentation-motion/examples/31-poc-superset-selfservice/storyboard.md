# Self-Servis Raporlama – Superset POC

**Ana mesaj:** Bölge müdürleri raporlarını artık kendileri alıyor; yaygınlaştırma pilotu için onay istiyoruz.
**Kitle:** executive · **Süre hedefi:** 2–3 dk (seslendirmeyle) · **Tema:** corporate-light

| # | Sahne | Mesaj (tek cümle) | Şablon | Görsel / hareket notu | Veri kaynağı |
|---|---|---|---|---|---|
| 0 | Açılış | POC'nin adı ve kapsamı | title | başlık harf harf | — |
| 1 | Özet (BLUF) | Raporlar 72× daha hızlı (3 gün → 1 saat) | bluf-headline | sayaç | POC ölçümleri `[VERİ GEREKLİ]` |
| 2 | Mimari | Kaynaklar saatlik olarak tek ambarda, Superset'ten self-servis | architecture-layers | katman katman, partiküller | data/architecture.yaml |
| 3 | Pipeline | Modeller testlerle tek komutla güncelleniyor | terminal | yazılıyor efekti | dbt/superset çıktısı (örnek) |
| 4 | Canlı sonuç | Filtre değişince tüm grafikler güncelleniyor | browser (mock-app) | caption, zoom, spotlight | sentetik veri |
| 5 | Etki | Süre, talep sayısı, kullanıcı | kpi-counter-row | sayaçlar | POC ölçümleri `[VERİ GEREKLİ]` |
| 6 | Karar | Yaygınlaştırma pilotu onayı | decision-ask | çerçeve, tikler | — |

## [VERİ GEREKLİ] — doldurulacak rakamlar
- Mevcut durum: aylık rapor talebi (örnek **120**), ortalama bekleme (örnek **3 gün / 72 saat**)
- POC sonrası: rapor süresi (örnek **1 saat**), aylık talep (örnek **18**), self-servis kullanıcı (örnek **85**), azalma oranı (örnek **%85**)
- Pilot maliyeti / sunucu sayısı (örnek **2 sunucu**), pilot süresi (örnek **2 ay**)
- Karar tarihi
- KPI başlığındaki POC süresi (örnek **6 hafta**)

## Notlar
- Sayaç alanları sayı istediği için videoda örnek değerler görünür; `[VERİ GEREKLİ]` kaynak satırında ve karar tarihinde durur. Dış paylaşımdan önce gerçek değerlerle değiştirin.
- Gerçek Superset kaydı için `demo.superset.yaml` selector'ları doğrulanmalı.
