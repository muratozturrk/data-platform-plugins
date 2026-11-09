---
name: ui-capture
description: Gerçek uygulama kaydı (Playwright) - recorder.mjs adım dili (wait/click/type/select/zoom/spotlight/caption/badge), CDP screencast ile yüksek kalite, adapter'lar (Superset, Airflow, Jupyter, generic), oturum/storageState, maskeleme, insan temposu, kararlılık kuralları ve sık sorunlar. Superset/POC/MVP demo kaydı yaparken kullan.
---

# UI kaydı

## Neden yerleşik recordVideo değil?
Playwright'ın Chromium kaydı ~1 Mbit/s sabit VP8 → metin bulanık. `recorder.mjs` CDP `Page.startScreencast` (JPEG q92, `deviceScaleFactor: 2`) karelerini zaman damgalarıyla alır, ffmpeg ile H.264 CRF 18'e çevirir (değişmeyen anlarda kare üretilmez, süreler damgadan hesaplanır).

## Akış dosyası
```yaml
app: superset            # adapters/<app>.mjs
baseUrl: http://localhost:8088
url: /superset/dashboard/sales/?standalone=1
auth: .auth/superset.json
scale: 2
mask: [".user-email"]    # blur
hide: [".promo-banner"]  # display:none
steps:
  - wait: ready                         # adapter.waitReady
  - caption: "Bölge filtresi seçiliyor"
  - click: { selector: "...", zoom: 1.5 }
  - select: { selector: "...", option: "Marmara" }
  - type: { selector: "...", text: "SELECT ...", delay: 55 }
  - spotlight: { selector: "...", hold: 2 }
  - zoom: false
  - badge: "⏩ 4×"
  - hold: 1.5
```
Overlay: sentetik imleç + tıklama halkası, caption bandı, spotlight, kamera (hedefi ortalayan, kenarlarda sınırlandırılmış zoom).

## Komutlar
```bash
R="${CLAUDE_PLUGIN_ROOT}/capture/playwright"
node $R/recorder.mjs --login superset --base-url http://localhost:8088 --save .auth/superset.json
node $R/recorder.mjs capture/tour.yaml -o out/tour.mp4
```

## Kararlılık
- Selector önceliği: `data-test(id)` → rol+ad → metin → CSS sınıfı.
- Her durum değişikliğinden sonra `wait: ready` veya `wait: {selector}`; zaman aşımı kaydı başarısız yapar.
- Önbelleği ısıt; sabit tarih filtreleri; demo veritabanı + seed.
- Pop-up/çerez/bildirim → `hide`. Kaydırma çubukları otomatik gizlenir.
- `locale: tr-TR`, `timezone: Europe/Istanbul` (varsayılan).

## Superset notları
`?standalone=1` (menü yok) / `2` (başlık da yok). Hazır olma: `.chart-container` görünür + `.loading` = 0. Seçiciler sürüme göre değişir → `adapters/superset.mjs`.

## Sık sorunlar
| Sorun | Çözüm |
|---|---|
| Yükleme spinner'ı videoda | kayıttan önce `url` + `waitReady` (recorder bunu screencast'ten önce yapar) |
| Zoom'da bulanıklık | `scale: 2` |
| İmleç zoom sonrası yanlış yerde | zoom'u tıklamadan önce yap (`click: {zoom}`), tıklamadan sonra `zoom: false` |
| Çok uzun segment | böl; araya HTML sahnesi; `speed`/`trim` |
