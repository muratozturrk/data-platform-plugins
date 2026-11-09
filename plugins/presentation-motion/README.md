# presentation-motion

HTML/CSS/GSAP ile **animasyonlu sunum ve demo videoları** üreten Claude Code plugin'i.
İş, akademik, teknik, yönetici ve kamu sunumları; Superset ve POC/MVP demo kayıtları.

- **Kodla tasarım:** her sahne tek HTML dosyası + tek paused GSAP timeline + `pm-data` JSON
- **Deterministik video:** kare kare `seek` → ffmpeg (kare atlamaz, önizleme = video)
- **Gerçek uygulama kaydı:** Playwright + CDP screencast, sentetik imleç, zoom, spotlight, caption, maskeleme
- **Küçük dosyalar:** 6 sn 1080p sahne ≈ 0,3 MB; 57 sn POC demosu ≈ 2,1 MB
- **Çevrimdışı:** tüm kütüphaneler `vendor/` içinde (kapalı ağ / kamu ortamı)

## Kurulum
Gereksinimler: Node.js 22+, ffmpeg (PATH'te), (isteğe bağlı) VHS.
```bash
cd presentation-motion
npm install
npx playwright install chromium-headless-shell
node scripts/fetch-vendor.mjs
```
Claude Code'da yerel olarak denemek için:
```bash
claude --plugin-dir ./presentation-motion
```
Komutlar `/presentation-motion:<komut>` olarak görünür (ör. `/presentation-motion:poc`).

## Hızlı başlangıç
```bash
# Hazır örnek: HTML sahneler + tarayıcı kaydı → tek video
cd examples/26-poc-realtime-pipeline
node ../../capture/compose/compose.mjs demo.yaml

# Tek sahne render
node scripts/render-scene.mjs templates/kpi-counter-row.html --profile pptx -o out/kpi.mp4

# Gerçek uygulama kaydı
node capture/playwright/recorder.mjs --login superset --base-url http://localhost:8088 --save .auth/superset.json
node capture/playwright/recorder.mjs capture/tour.yaml -o out/tour.mp4
```

## Tipik akış (Claude Code içinde)
```
/presentation-motion:new  →  /storyboard  →  /build  →  /review  →  /render
                     └── POC/demo:  /poc  →  /record | /superset | /tape  →  /demo
```

## Yapı
| Klasör | İçerik |
|---|---|
| `commands/` (24) | new, storyboard, build, animate, theme, preview, inspire, audience, variants, handout, subtitles, arch, dag, lineage, sql, record, demo, poc, superset, tape, rerecord, review, redact, render |
| `agents/` (12) | director, artist, animator, data-scientist, solution-architect, audience-adapter, demo-scripter, camera-operator, narrator, render-engineer, compliance-guard, reviewer |
| `skills/` (14) | motion-recipes, deterministic-render, video-export, brand-theme, academic-blocks, diagram-as-code, data-diagram-kit, tr-number-format, executive-storytelling, technical-storytelling, government-protocol, synthetic-data, product-ui-recreation, ui-capture |
| `templates/` | `_base/` runtime + 8 hazır şablon (bkz. `CATALOG.md`) |
| `capture/` | `playwright/` recorder + overlay + adapter'lar (superset, airflow, jupyter, generic) · `terminal/` VHS tape'leri · `compose/` demo.yaml birleştirici |
| `examples/` | 30 örnek kataloğu; `26-poc-realtime-pipeline` uçtan uca çalışır |
| `scripts/` | render-scene, lint-determinism, size-budget, probe, fetch-vendor |
| `hooks/` | sahne/şablon dosyası yazılınca determinism lint'i (PostToolUse) |

## Agent rolleri
**Üretenler:** director (hikâye) → artist (görsel dil) → animator (hareket) · data-scientist (veri/formül) · solution-architect (mimari/DAG/lineage) · audience-adapter (kitle varyantları) · demo-scripter (Playwright akışı) → camera-operator (zoom/caption/kurgu) · narrator (altyazı/ses) · render-engineer (video)
**Denetleyenler (salt okur):** compliance-guard (gizlilik/KVKK/marka) · reviewer (bağımsız kalite kapısı)

## Doğrulanmış durum
- ✅ 8 şablon render ve lint testi; tümü ≤ 33 KB gzip JS
- ✅ recorder: caption, sentetik imleç, maskeleme, ortalayan zoom, spotlight (yerel sayfa ile test)
- ✅ compose: 7 segment (6 HTML + 1 browser) → 57 sn, 2,1 MB, H.264 yuv420p
- ⚠️ Superset/Airflow/Jupyter adapter seçicileri gerçek bir kurulumda doğrulanmadı; sürümünüze göre güncelleyin
- ⚠️ terminal segmenti (VHS) test edilmedi (VHS kurulu değildi); `terminal` HTML şablonu çalışıyor
- 📝 Katalogdaki planlı şablonlar ve örnekler

## Lisanslar
Plugin kodu: MIT. Üçüncü taraf kütüphaneler: `vendor/README.md` (GSAP No-Charge lisansı, diğerleri MIT/ISC).
