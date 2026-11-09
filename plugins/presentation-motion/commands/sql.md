---
description: SQL sorgusu → sonuç tablosu → grafik akışını tek sahnede anlatır
argument-hint: <query.sql> <result.csv> [--chart bar|line] [--x kolon] [--y kolon]
---

**data-scientist** + **animator**: `$ARGUMENTS`

1. Sorguyu biçimlendir (anahtar kelimeler büyük harf, satır başına bir yan tümce).
2. Sahne akışı (tek timeline):
   1. Sorgu yazılıyor (terminal şablonu mantığı veya kod bloğu), önemli satırlar vurgulanır
   2. Sonuç tablosu satır satır belirir (en fazla 8 satır; fazlası "…")
   3. Tablo, seçilen kolonlarla bar/çizgi grafiğe **morph** olur (FLIP: hücre → bar)
3. Sayılar `tr-number-format` ile. Kaynak satırı: "Sorgu: gold.daily_sales, demo veritabanı".
4. CSV gerçek veri içeriyorsa önce `/presentation-motion:redact`.
