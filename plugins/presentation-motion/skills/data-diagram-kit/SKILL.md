---
name: data-diagram-kit
description: Veri platformu diyagramları için görsel dil - jenerik bileşen ikonları (kaynak, kuyruk, depolama, compute, katman, dashboard, API), renk anlamları, durum renkleri, akış gösterimi ve üçüncü taraf logo/marka kullanım kuralları. Mimari, pipeline veya ürün diyagramı çizerken kullan.
---

# Veri diyagramı kiti

## Jenerik ikonlar (inline SVG, 24×24, stroke 1.75, `currentColor`)
| Bileşen | Şekil |
|---|---|
| Veritabanı / tablo | silindir |
| Olay kuyruğu / stream | yatay 3 çizgi + ok |
| Nesne depolama / lake | kutu yığını |
| Compute / iş | dişli |
| Zamanlayıcı / orkestrasyon | saat + düğüm |
| Dashboard | 2×2 kart ızgarası |
| API | `{ }` |
| Kullanıcı / tüketici | kişi silueti |
| Güvenlik / KVKK | kalkan |

## Renk anlamları
- `--flow` (camgöbeği/mavi): akış verisi · `--warn` (kehribar): batch paketleri
- Durum: gri (bekliyor) → `--accent` (çalışıyor) → `--good` (başarılı) / `--bad` (hata)
- Katman kartları `--surface`, kenarlıklar `--line`; vurgulanan düğümde kenarlık `--accent`.

## Logo ve marka
- Varsayılan: **logo yok**, işlev adı ("Akış katmanı"). Teknik kitlede ürün adı metin olarak yazılabilir.
- Üçüncü taraf logosu yalnızca kullanıcı isterse ve markanın kullanım kurallarına uygunsa (oran, renk, boşluk; logoyu değiştirme).
- Kamu sunumlarında yabancı ürün logoları yerine işlev adları tercih edilir (yerli çözüm vurgusu varsa özellikle).
