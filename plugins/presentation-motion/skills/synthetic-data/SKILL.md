---
name: synthetic-data
description: Demo ve sunumlar için gerçekçi ama sahte veri - tutarlı takma ad üretimi, oranları koruyarak sayı ölçekleme, Türkçe ad/şehir/kurum üretimi, demo veritabanı tohumlama (seed), gerçek şema/tablo adlarının maskelenmesi. Demo kaydı hazırlarken, /redact sırasında ve örnek veri gerektiğinde kullan.
---

# Sentetik veri

## İlkeler
- **Tutarlılık:** aynı gerçek değer her yerde aynı takma ada dönüşür (deterministik hash → sözlük indeksi). Grafik, tablo ve anlatım birbiriyle çelişmez.
- **Oranı koru:** gerçek sayıları sabit bir katsayıyla ölçekle (ör. ×0,73); trend ve oranlar doğru kalır, mutlak değer gizlenir. Katsayıyı sunumda belirtme, ama `out/redaction-map.md`'de tut (git dışı).
- **Gerçekçi dağılım:** düzgün rastgele değil; satışlarda haftalık mevsimsellik, uzun kuyruk müşteri dağılımı.
- **Sabit tohum:** her kayıtta aynı veri (demo tekrarlanabilir olmalı).

## Türkçe sözlükler
Şehirler (81 il), bölgeler (7), Türkçe ad-soyad listeleri, sektör adları, "Örnek A.Ş." tarzı kurum adları. Gerçek kişi/kurum adına benzeyen birleşimlerden kaçın.

## Demo veritabanı
```sql
-- seed.sql: her kayıttan önce
TRUNCATE gold.daily_sales;
INSERT INTO gold.daily_sales SELECT ... FROM generate_series('2026-08-01'::date, '2026-10-31', '1 day') d, regions r;
```
- Tarih filtreleri sabit tarihlere göre ("son 7 gün" değil "25–31 Ekim 2026").
- Python: Faker (`Faker('tr_TR')`) + numpy (seed).

## Maskeleme sözlüğü örneği
| Gerçek | Sunumda |
|---|---|
| `prod-dwh-01.kurum.local` | `veri-ambari` |
| `crm.customer_pii` | `crm.musteri` |
| Müşteri "X Holding" | "Müşteri A" |
