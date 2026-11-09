---
name: government-protocol
description: Kamu kurumları ve üst düzey devlet yöneticilerine sunum kuralları - resmi hitap ve unvan sırası, kurum logo yerleşimi, resmi dil, KVKK ve kamu bilgi güvenliği atıf kalıpları, veri egemenliği/yerinde kurulum anlatımı, kamu yararı odaklı kurgu, çevrimdışı teslim. Bakanlık, kamu kurumu, belediye veya kamu ihalesi sunumlarında kullan.
---

# Kamu sunumu protokolü

## Kurgu
1. **Açılış:** sunumun konusu ve sunan kurum; hitap gerekiyorsa en üst makamdan başlayarak ("Sayın Bakan Yardımcım, Sayın Genel Müdürüm, değerli katılımcılar")
2. **Kamu yararı:** vatandaşa / kuruma etkisi (süre, maliyet, hizmet kalitesi)
3. **Mevcut durum ve ihtiyaç** (resmi verilerle, kaynak gösterilerek)
4. **Çözüm** (işlev odaklı mimari, yerinde kurulum / veri egemenliği)
5. **Güvenlik ve uyum:** KVKK, kurumun bilgi güvenliği politikaları, erişim yetkilendirme, kayıt (log) tutma
6. **Ölçeklenebilirlik:** il/kurum bazında yaygınlaştırma (`turkey-province-map`, `maturity-model`)
7. **Takvim ve talep** (`roadmap-gantt`, `decision-ask`)

## Dil ve üslup
- Resmi ve saygılı; argo, abartı ve pazarlama dili yok ("devrim niteliğinde" ❌).
- Kısaltmaların ilk kullanımında açılımı. Yabancı terim yerine Türkçe karşılık (veri ambarı, akış işleme).
- Siyasi yorum, kurumlar arası olumsuz karşılaştırma, doğrulanmamış istatistik yok.
- Mevzuat atfı: adıyla ve numarasıyla doğru yaz (ör. "6698 sayılı Kişisel Verilerin Korunması Kanunu"). Emin değilsen `[DOĞRULA]`.

## Görsel
- `government` teması; sade, yüksek kontrast, az hareket.
- Kurum logosu: sunulan kurum ve sunan kurum logoları kurumun belirlediği sırayla; logo oranı ve renkleri değiştirilmez. Logo dosyası kullanıcıdan alınır.
- Harita ve istatistiklerde resmi kaynak ve tarih.

## Teslim
Kapalı ağ olasılığına karşı: çevrimdışı HTML + MP4 + PDF; tüm kütüphaneler ve fontlar yerel. USB'de teslim için dosya adlarında Türkçe karakter ve boşluk kullanma.
