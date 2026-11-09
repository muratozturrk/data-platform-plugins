---
name: product-ui-recreation
description: Ürün arayüzünü ekran kaydı yerine HTML/CSS ile sadeleştirerek yeniden çizme - ui-mock-frame, sanal imleç koreografisi, zoom callout, özellik turu; vektör olduğu için keskin ve küçük video. Ürün lansmanı, özellik tanıtımı veya henüz bitmemiş (MVP) arayüzler için kullan.
---

# Ürün arayüzünü yeniden çizme

## Ne zaman kayıt, ne zaman yeniden çizim?
| Durum | Seçim |
|---|---|
| Gerçek veriyle çalışan sistem, POC kanıtı | **Kayıt** (Playwright) — inandırıcılık |
| Lansman videosu, pazarlama, sabit görünüm | **Yeniden çizim** — tam kontrol, keskinlik |
| Henüz bitmemiş MVP ekranı | **Yeniden çizim** — "temsilidir" notuyla |
| Arayüz sık değişiyor | **Kayıt** + `/rerecord` |

## Yöntem
1. Gerçek ekranın ekran görüntüsünü referans al, **sadeleştir**: menüleri azalt, yalnızca anlatılan akışın öğeleri.
2. `ui-mock-frame`: tarayıcı/uygulama çerçevesi (pencere çubuğu, adres alanı opsiyonel) içinde HTML.
3. Gerçek veri yerine `synthetic-data`.
4. Sanal imleç: SVG ok, `follow-path` tarifiyle bezier yol; tıklamada halka (`scale .3→1.4, opacity 1→0`).
5. Zoom callout: `.world` kapsayıcıya `camera-zoom`, ya da ayrı büyüteç balonu (orijinal küçük kalır, detay balonda).
6. Durum değişimleri (filtre → grafik güncellenir) GSAP ile; gerçek uygulamadaki gecikmeleri gösterme.

## Kurallar
- Ürün dışı markaları ve gerçek müşteri verisini gösterme.
- Arayüzü abartma: olmayan özelliği çizme; MVP'de "temsilidir" etiketi.
- 1080p'de en küçük UI metni ≥ 20 px (gerekirse 1,25× ölçekle).
