---
description: Superset için hazır demo akışı üretir - dashboard turu, SQL Lab'den grafiğe veya gömülü dashboard
argument-hint: <dashboard-id|slug> [--flow tour|sqllab|chart|embedded] [--base-url http://localhost:8088]
---

**demo-scripter** (adapter: `superset`): `$ARGUMENTS`

1. Oturum yoksa: `SUPERSET_USER`/`SUPERSET_PASS` ortam değişkenleriyle `--login superset` (şifreyi dosyaya yazma).
2. Akış şablonları:
   - **tour**: `?standalone=1` → `wait: ready` → filtre seç → uygula → `wait: ready` → ilgili grafiğe zoom → spotlight KPI
   - **sqllab**: `/sqllab/` → sorgu yaz → çalıştır → sonuç tablosu spotlight → "grafik oluştur"
   - **chart**: veri seti → grafik tipi → metrik/boyut → çalıştır → kaydet → dashboard'a ekle
   - **embedded**: kendi ürününüzün sayfası içinde gömülü dashboard'a odak
3. Seçicileri canlı DOM'dan doğrula (`adapters/superset.mjs` → `selectors`); farklıysa adapter'ı güncelle.
4. Önbelleği ısıt (dashboard'u kayıttan önce bir kez aç); tarih filtrelerini sabit tarihe çek.
5. Referans: `${CLAUDE_PLUGIN_ROOT}/examples/26-poc-realtime-pipeline/demo.superset.yaml`.
