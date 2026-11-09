---
name: tr-number-format
description: Türkçe sayı, para, yüzde, tarih ve birim yazım kuralları - Intl.NumberFormat('tr-TR'), yüzde işaretinin başta olması (%97,4), binlik nokta/ondalık virgül, ₺, milyon/milyar kısaltmaları, sayaç animasyonlarında biçimin korunması. Sahnelere sayı yazarken ve veri doğrularken kullan.
---

# Türkçe sayı biçimi

| Tür | Doğru | Yanlış |
|---|---|---|
| Binlik / ondalık | 1.234.567,8 | 1,234,567.8 |
| Yüzde | **%97,4** · %38 artış | 97.4% · 97,4 % |
| Yüzde puan | +21 puan | +%21 (farklı anlam!) |
| Para | 182.500 ₺ · 1,2 milyon ₺ | ₺182,500 · 1.2M TL |
| Büyük sayı | 3,4 milyar · 12 bin | 3.4B · 12K |
| Tarih | 7 Kasım 2026 · 07.11.2026 | 11/07/2026 |
| Saat / süre | 14.30 · 5 dk · 2 sa 15 dk | 2:30 PM |
| Aralık | Ağustos–Ekim 2026 (en dash) | Ağustos-Ekim |
| Çarpan | 288× | 288x |

## Kod
```js
PM.fmt(182500)        // "182.500"
PM.fmt(97.4, 1)       // "97,4"
new Intl.NumberFormat('tr-TR', { style:'currency', currency:'TRY', maximumFractionDigits:0 }).format(182500) // "₺182.500" → sahnede "182.500 ₺" tercih
new Intl.NumberFormat('tr-TR', { notation:'compact' }).format(3400000000) // "3,4 Mr" → sahnede "3,4 milyar" yaz
```
- KPI şablonunda yüzde için `prefix: "%"`, birimler için `unit: "dk"`.
- Sayaç animasyonunda ondalık basamak sayısı sabit kalmalı (`digits`), yoksa metin genişliği titrer; `font-variant-numeric: tabular-nums`.
- Negatif: "−%38" (eksi işareti U+2212) veya "%38 azalış".
