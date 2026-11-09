---
description: Hassas veriyi tespit edip sentetik/maskeli veriyle değiştirir (compliance-guard raporuna göre)
argument-hint: [--dry-run]
---

1. **compliance-guard**'ı çalıştır (yoksa) → `out/compliance.md`.
2. Her bulgu için düzeltme öner, `--dry-run` değilse kullanıcı onayıyla uygula:
   - Metin/veri: `synthetic-data` skill'i ile tutarlı takma ad (aynı gerçek değer → hep aynı takma ad), sayıları oranı koruyarak ölçekle.
   - Tablo/şema adları: anlamlı jenerik adlar (`gold.daily_sales` → `gold.gunluk_satis_ornek`).
   - Browser segmentleri: `mask` / `hide` listelerine seçici ekle; gerekirse demo veritabanına geç.
3. Değişiklik eşlemesini `out/redaction-map.md`'ye yaz (**git'e girmemeli**; .gitignore'a ekle).
4. Tekrar compliance-guard → GEÇTİ olana kadar.
