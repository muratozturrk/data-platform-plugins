---
name: technical-storytelling
description: Teknik sunum kurgusu - problem/kısıt/seçenek/karar (ADR tarzı), mimari anlatımı, canlı demo riskini azaltma (kayıtlı demo), benchmark metodolojisi, kod sahneleri, konferans ve ekip içi sunum ritmi. Mühendislik ekibine, konferansa veya teknik değerlendirme kuruluna sunum hazırlarken kullan.
---

# Teknik hikâye anlatımı

## Yapı
1. **Problem** (somut, ölçülmüş): "Gece batch'i 6 saat sürüyor, sabah raporları eksik"
2. **Kısıtlar**: bütçe, mevcut sistemler, ekip yetkinliği, mevzuat
3. **Seçenekler** (2–3) ve değerlendirme kriterleri → `comparison`
4. **Karar** ve gerekçe (ADR) → mimari sahnesi (`architecture-layers`)
5. **Kanıt**: benchmark (`before-after-perf`), demo kaydı (browser/terminal segmenti)
6. **Öğrenilenler ve ödünleşimler** (trade-off): dürüstlük güven yaratır
7. **Sonraki adımlar** ve açık sorular

## Benchmark kuralları
Donanım, veri hacmi, tekrar sayısı, ısınma (warm-up), medyan ve p95. "3× daha hızlı" yerine "medyan 42 sn → 14 sn (n=10, 1 TB, 8 vCPU)". Log ölçek varsa eksende yaz.

## Demo
Canlı demo yerine **kayıtlı demo** (Playwright/VHS): ağ, kimlik doğrulama, veri değişkenliği riskleri sıfırlanır. Canlı sunumda da videoyu oynat, soru gelirse canlı sisteme geç.

## Kod sahneleri
- Ekranda ≤ 15 satır; ilgisiz satırları gizle ("…").
- Değişen satırı vurgula, diğerlerini soldur; diff için magic move.
- Font ≥ 28 px (mono), koyu tema.

## Ritim
Konferans: 20–30 dk, her 5–7 dk'da bir görsel değişim (demo, diyagram, kod). İç ekip: daha yoğun detay kabul edilir ama ana mesaj yine ilk 1 dk'da.
