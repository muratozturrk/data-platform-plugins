---
name: brand-theme
description: Tasarım token sistemi ve tema kuralları - tokens.css değişkenleri, hazır temalar (corporate-light/dark, academic, projection, government), kurum renginden tema türetme, kontrast doğrulama (WCAG), tipografi ölçeği ve 1920×1080 yerleşim kuralları. Tema oluştururken veya sahnelerde renk/font seçerken kullan.
---

# Tema ve token'lar

## Değişkenler (`_base/tokens.css`)
Renk: `--bg --surface --ink --ink-2 --muted --line --accent --accent-2 --good --warn --bad --flow`
Font: `--font-sans --font-mono` · Ölçek: `--fs-hero(120) --fs-h1(84) --fs-h2(56) --fs-body(36) --fs-small(28)` · `--radius --safe(120) --scale`
**Kural:** sahnelerde ham hex/font adı yok; yalnızca `var(--...)`.

## Hazır temalar
| Tema | Kullanım |
|---|---|
| `corporate-dark` | teknik sunum, ürün demosu, video |
| `corporate-light` | yönetici sunumu, yazdırma, aydınlık salon |
| `academic` | konferans, tez savunması (serif başlık) |
| `projection` | zayıf projektörler; kontrast ≥ 7:1, `--scale: 1.12` |
| `government` | kamu; sade, lacivert + kırmızı vurgu |

## Kurum renginden tema türetme
1. Ana renk → `--accent`. Açık temada metin üzerinde kontrast ≥ 4.5:1 olana kadar koyulaştır; koyu temada açılaştır.
2. `--accent-2`: ana rengin ±30° ton kayması ya da kurumun ikincil rengi.
3. Nötrler: ana rengin tonunda çok düşük doygunluklu gri skala (`--bg`, `--surface`, `--line`).
4. Durum renkleri sabit kalır (yeşil/kehribar/kırmızı); kurum rengi kırmızıysa `--bad` için farklı ton seç.
5. Kontrast hesabı: WCAG göreli parlaklık `(L1+0.05)/(L2+0.05)`; tüm ink/bg çiftlerini raporla.

## Yerleşim
- Sahne 1920×1080, `.pm-stage` padding `--safe`. Alt 140 px altyazı için boş.
- En küçük font 28 px. Satır uzunluğu ≤ 60 karakter.
- Bir sahnede en fazla 1 vurgu rengi; boşluk ≥ %40.
- Fontlar yerel (çevrimdışı): Inter / Segoe UI / system-ui. Kurum fontu varsa `vendor/fonts/` + `@font-face`.
