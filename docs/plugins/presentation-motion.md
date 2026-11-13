# presentation-motion

> HTML/CSS/GSAP ile animasyonlu sunum ve demo videoları üretir: storyboard, sahne şablonları, Playwright ile gerçek uygulama kaydı (Superset, POC/MVP), deterministik MP4 render, kalite ve gizlilik denetimi.

| | |
| --- | --- |
| Sürüm | 0.1.0 |
| Kategori | presentation |
| Kaynak | `plugins/presentation-motion/` |
| Kaynak materyal | Plugin bu repoda elle yazıldı. Üçüncü taraf kütüphaneler `vendor/` altında: GSAP 3.13 (No-Charge lisansı), KaTeX 0.16, D3 7.9, rough-notation (`vendor/README.md`) |

## 1. Ne işe yarar?

Sunumu slayt yerine kısa bir video olarak üretir. Her sahne tek bir HTML dosyasıdır. Sahnenin içeriği `pm-data` JSON bloğundan gelir, hareketi de duraklatılmış (paused) tek bir GSAP timeline'ı yönetir. Render sırasında timeline kare kare ilerletilir, her karenin ekran görüntüsü alınır ve ffmpeg bunları videoya çevirir. Bu yüzden kare atlanmaz ve tarayıcıdaki önizleme ile video birebir aynıdır. Bir 1080p sahne 6 saniyede yaklaşık 0,3 MB tutuyor. 57 saniyelik POC demosu ise 2,1 MB.

HTML sahnelerin yanına gerçek uygulama kaydı eklenebilir. Playwright, Superset, Airflow, Jupyter ya da kendi web uygulamanızı `demo.yaml` içindeki adımlarla gezer. Kayda sentetik imleç, zoom, spotlight, altyazı ve maskeleme eklenir. Terminal komutları VHS tape'leri ya da `terminal` şablonuyla gösterilir. `compose.mjs` bütün segmentleri tek bir videoda birleştirir.

Plugin dört kitle için kurgu kuralları içerir: teknik ekip, özel sektör yöneticisi, kamu yöneticisi ve akademik kitle. Yönetici sunumunda BLUF ve Minto, kamuda resmi hitap, KVKK atıfları ve çevrimdışı teslim, akademide formül, grafik ve atıf ön plandadır. Türkçe sayı biçimi (`%97,4`, `1.250.000 ₺`) ve sentetik demo verisi için ayrı skill'ler var. Teslimden önce iki salt okur agent (`compliance-guard`, `reviewer`) gizlilik ve kalite denetimi yapar.

Repodaki diğer plugin'ler veri platformunu kurmak ve işletmek içindir. Bu plugin ise o platformu anlatmak içindir: mimari, DAG, lineage ve SQL sahneleri `architecture.yaml`, Airflow DAG, dbt `manifest.json` ve OpenLineage olaylarından üretilir.

## 2. Kurulum

```text
/plugin marketplace add muratozturrk/data-platform-plugins
/plugin install presentation-motion@data-platform-plugins
```

Yerel geliştirme: `claude --plugin-dir ./plugins/presentation-motion`

Gereksinimler:
- Node.js 22+
- `ffmpeg` (PATH'te olmalı). `web` profili için ffmpeg'in `libsvtav1` ile derlenmiş olması gerekir.
- Playwright ve Chromium. `node_modules` repoda olmadığı için kurulumdan sonra plugin klasöründe bir kez çalıştırın:

```bash
cd <plugin klasörü>
npm install
npx playwright install chromium-headless-shell
```

- İsteğe bağlı: [VHS](https://github.com/charmbracelet/vhs) (terminal kaydı için; yoksa `terminal` HTML şablonu kullanılır).

`vendor/` altındaki JS kütüphaneleri repoda olduğu için `node scripts/fetch-vendor.mjs` çalıştırmak gerekmez. Bu script yalnızca kütüphaneleri yeniden indirmek ya da KaTeX fontlarını almak için gerekir (bkz. bölüm 7).

Komutlar `/presentation-motion:<komut>` olarak görünür, ör. `/presentation-motion:poc`.

## 3. İçerik

### Skill'ler

| Skill | Ne zaman devreye girer | İçerdiği konular |
| --- | --- | --- |
| `deterministic-render` | Sahne yazarken, efekt uyarlarken, render sorunlarında | Sahne sözleşmesi (PM runtime, paused timeline, `pm-data`), yasak API'ler, scroll/fare/fizik efektlerini zamana bağlama, `render-scene.mjs` çalışma mantığı |
| `motion-recipes` | Sahneye hareket eklerken | Kinetik tipografi, maske açılışı, sayaç, SVG çizgi çizimi, morph, motion path, stagger, FLIP, kamera zoom, parallax; Codrops efektlerinin seek edilebilir GSAP karşılıkları |
| `video-export` | Render profili seçerken, boyut/kalite sorunlarında | PowerPoint H.264, web AV1, 4K, alfa kanallı WebM; CRF/bitrate, boyut hedefi, bantlanma, ffmpeg komutları, teslim paketi |
| `brand-theme` | Tema oluştururken, renk/font seçerken | `tokens.css` değişkenleri, hazır temalar (corporate-light/dark, academic, projection, government), kurum renginden tema, WCAG kontrastı, 1920×1080 yerleşim |
| `academic-blocks` | Formül, grafik, kod veya akademik sahne yazarken | KaTeX parça parça formül, D3 grafik animasyonları, Shiki Magic Move kod geçişi, el çizimi vurgu, atıf gösterimi |
| `diagram-as-code` | Mimari, akış, pipeline, lineage diyagramlarında | `architecture.yaml` şeması, Airflow DAG / dbt manifest / OpenLineage ayrıştırma, Mermaid/D2 → SVG → GSAP ile adım adım açma |
| `data-diagram-kit` | Mimari veya ürün diyagramı çizerken | Jenerik bileşen ikonları (kaynak, kuyruk, depolama, compute, dashboard, API), renk ve durum anlamları, üçüncü taraf logo kuralları |
| `tr-number-format` | Sahnelere sayı yazarken | `Intl.NumberFormat('tr-TR')`, yüzde işaretinin başta olması, binlik nokta / ondalık virgül, ₺, milyon/milyar kısaltmaları, sayaçlarda biçimin korunması |
| `executive-storytelling` | Yönetici sunumu, business case, ROI, onay sunumu | BLUF, Minto piramidi, SCQA, so-what testi, karar odaklı kapanış, tek sahne/tek mesaj/tek sayı |
| `technical-storytelling` | Mühendislik ekibine veya konferansa sunumda | Problem/kısıt/seçenek/karar (ADR), mimari anlatımı, kayıtlı demo ile risk azaltma, benchmark metodolojisi |
| `government-protocol` | Bakanlık, kamu kurumu, belediye, ihale sunumlarında | Resmi hitap ve unvan sırası, logo yerleşimi, resmi dil, KVKK ve kamu bilgi güvenliği atıfları, veri egemenliği, çevrimdışı teslim |
| `synthetic-data` | Demo kaydı hazırlarken, `/redact` sırasında | Tutarlı takma ad, oranları koruyarak ölçekleme, Türkçe ad/şehir/kurum üretimi, demo DB seed, şema adlarının maskelenmesi |
| `product-ui-recreation` | Ürün lansmanı, özellik turu, henüz bitmemiş MVP arayüzleri | Arayüzü kayıt yerine HTML/CSS ile yeniden çizme: `ui-mock-frame`, sanal imleç, zoom callout |
| `ui-capture` | Gerçek uygulama kaydında | `recorder.mjs` adım dili (wait/click/type/select/zoom/spotlight/caption/badge), CDP screencast, adapter'lar, `storageState`, maskeleme, insan temposu |

### Agent'lar

Hepsi `model: inherit`.

| Agent | Ne yapar | Ne zaman kullanılır |
| --- | --- | --- |
| `director` | Konu, kaynak, kitle ve süreden ana mesajı çıkarır; sahne sırası, süre ve görsel notlarla `storyboard.md` yazar | Yeni sunumun ilk adımı, yeniden kurgu |
| `artist` | Izgara, palet, tipografi, ikon/SVG ve her sahnenin durağan son karesini tasarlar; hareket eklemez | Tema, yeni sahne, görsel tutarlılık |
| `animator` | Statik sahneye tek paused GSAP timeline ile koreografi ekler; scroll/fare efektlerini zamana bağlar | Animasyon ekleme, `/animate` |
| `data-scientist` | CSV/SQL sonucundan D3 grafik, LaTeX'ten KaTeX sahnesi üretir; eksen, birim, yuvarlama ve atıf doğruluğunu kontrol eder | Grafik, formül, benchmark sahneleri |
| `solution-architect` | `architecture.yaml`, DAG, dbt manifest ve OpenLineage'dan doğru diyagram verisi üretir | `/arch`, `/dag`, `/lineage` |
| `audience-adapter` | Aynı içeriği başka kitleye uyarlar; 60 sn / 5 dk / 15 dk varyantları üretir | `/audience`, `/variants` |
| `demo-scripter` | Gerçek uygulamayı inceleyip stabil selector'larla Playwright akışı (`demo.yaml` browser segmenti) yazar | `/record`, `/superset`, `/rerecord` |
| `camera-operator` | Kayda zoom, spotlight, caption ve imleç ritmi ekler; beklemeleri kırpar/hızlandırır | Kayıttan sonra demo videosunu parlatma |
| `narrator` | Sahne süreleriyle eşzamanlı seslendirme metni ve SRT üretir; isterse Piper TTS adımlarını hazırlar | `/subtitles` |
| `render-engineer` | Determinism lint'i, boyut bütçesi, render, birleştirme, codec profili ve ffprobe doğrulaması | Önizleme ve final video |
| `compliance-guard` | **Salt okur.** Sahne, veri ve kayıt karelerinde kişisel veri, kurum adı, gerçek host/IP/token, iç maliyet ve logo kullanımını denetler | Render öncesi, `/redact` |
| `reviewer` | **Salt okur.** Mesaj, okunabilirlik, hareket, veri doğruluğu, teknik kalite ve lisans açısından puanlar | `/review`, final öncesi kalite kapısı |

### Komutlar

| Komut | Argüman | Ne yapar |
| --- | --- | --- |
| `/new` | `<klasör> [konu] [--audience ...] [--minutes 5] [--theme ...]` | Yeni proje klasörü kurar (deck.yaml, tema, vendor ve `_base` kopyaları) |
| `/storyboard` | `<kaynak-dosya(lar)> [--minutes 5]` | Kaynak metinden sahne sahne `storyboard.md` (director) |
| `/build` | `[sahne-numaraları]` | Storyboard'dan sahneleri ve `demo.yaml`'ı üretir, önizleme render'ı alır |
| `/animate` | `<sahne.html> <tarif> [seçici]` | Tek sahneye bir `motion-recipes` tarifi uygular |
| `/theme` | `<ana-renk> [ikincil] [font] [--logo] [--base ...]` | Kurum renginden tema token dosyası üretir, kontrastı doğrular |
| `/preview` | `[sahne.html \| scenes/]` | Tarayıcıda canlı önizleme (Space oynat/duraklat, R baştan, ←/→ sar) |
| `/inspire` | `<url \| efekt-adı>` | Codrops vb. bir demodan tekniği çıkarıp videoya uygun tarife çevirir |
| `/audience` | `<technical \| executive \| government \| academic>` | Mevcut sunumu başka kitleye uyarlar |
| `/variants` | `[--only teaser,executive,technical]` | 60 sn, 5 dk ve 15 dk varyantları |
| `/handout` | `[--onepager] [--slides]` | Tek sayfalık yönetici özeti PDF ve son karelerden PDF yedek |
| `/subtitles` | `[--burn] [--tts piper]` | Seslendirme metni ve SRT, isterse videoya gömme |
| `/arch` | `<architecture.yaml \| "anlatım"> [--detail ...]` | Katman katman açılan, veri akışlı mimari sahnesi |
| `/dag` | `<dag.py \| manifest.json> [--fail task_id]` | Airflow/dbt pipeline çalışma animasyonu, isteğe bağlı retry |
| `/lineage` | `<openlineage.json \| manifest.json> <dataset> [--direction ...]` | Etki analizi animasyonu |
| `/sql` | `<query.sql> <result.csv> [--chart ...] [--x] [--y]` | Sorgu → tablo → grafik akışı tek sahnede |
| `/record` | `<url> "<senaryo>"` | Doğal dille anlatılan akışı Playwright ile kaydeder |
| `/superset` | `<dashboard-id> [--flow tour\|sqllab\|chart\|embedded] [--base-url]` | Superset için hazır demo akışı |
| `/tape` | `"<komutlar>" [--speed 2] [--simulate]` | Terminal komutlarından CLI demo segmenti |
| `/poc` | `"<POC adı>" [--app ...] [--url]` | POC/MVP demo iskeleti: Problem → Mimari → Pipeline → Canlı sonuç → Metrikler → İstek |
| `/demo` | `[demo.yaml] [--only 2,4] [-o çıktı.mp4]` | Tüm segmentleri render edip tek videoda birleştirir |
| `/rerecord` | `[demo.yaml] [--check]` | Arayüz değişince kayıtları yeniden çeker; `--check` yalnızca selector'ları doğrular |
| `/review` | `[--audience ...] [--strict]` | Bağımsız kalite incelemesi, `review.md` |
| `/redact` | `[--dry-run]` | Hassas veriyi sentetik/maskeli veriyle değiştirir |
| `/render` | `[pptx\|web\|4k\|alpha] [--max-mb 25] [--skip-review]` | Final video, boyut hedefi ve doğrulama |

### Araçlar (scripts/, capture/)

| Dosya | Ne yapar | Ek bağımlılık |
| --- | --- | --- |
| `scripts/render-scene.mjs` | Sahne veya klasörü kare kare render eder. Profiller: `pptx` (H.264 CRF 18), `web` (AV1 CRF 30), `4k` (3840×2160), `alpha` (WebM VP9 + şeffaflık) | Playwright, ffmpeg |
| `scripts/lint-determinism.mjs` | Sahnelerde videoyu bozan kalıpları yakalar: `Date.now`, `Math.random`, `setTimeout`, `requestAnimationFrame`, CSS `transition`, `tl.play()`, CDN'den uzak kaynak. Satıra `pm-lint-ignore` yazılırsa atlanır. `--hook` ile PostToolUse hook'u olarak çalışır | Node |
| `scripts/size-budget.mjs` | Sahnenin yüklediği yerel JS dosyalarının gzip toplamını ölçer (varsayılan bütçe 60 KB, `--budget`) | Node |
| `scripts/probe.mjs` | Çıktı videosunu ffprobe ile doğrular (`--max-mb`, `--fps`) | ffprobe |
| `scripts/fetch-vendor.mjs` | `vendor/` kütüphanelerini jsDelivr'dan yeniden indirir | İnternet |
| `capture/playwright/recorder.mjs` | Tarayıcı kaydı; `--login <adapter> --save .auth/x.json` ile oturum kaydeder | Playwright |
| `capture/playwright/adapters/` | superset, airflow, jupyter, generic | — |
| `capture/terminal/*.tape` | dbt run, kubectl, psql, spark-submit VHS tape örnekleri | VHS |
| `capture/compose/compose.mjs` | `demo.yaml` segmentlerini (html, browser, terminal, video) tek videoda birleştirir; şema: `demo.schema.json` | Playwright, ffmpeg |

### Şablonlar (templates/)

Hazır ve test edilmiş 8 şablon: `title`, `section-cover`, `bullets`, `bluf-headline`, `kpi-counter-row`, `decision-ask`, `architecture-layers`, `terminal`. Ortak runtime `templates/_base/` altında (`stage.css`, `tokens.css`, `timeline-runtime.js`). Planlı şablonlar (`chart`, `formula`, `dag`, `lineage-graph` vb.) `templates/CATALOG.md`'de 📝 ile işaretli; bunlar istendiğinde artist ve animator tarafından projeye özel sahne olarak üretilir.

## 4. Hızlı başlangıç

1. Bağımlılıkları kurun (bölüm 2) ve çalışan örneği render edin:
   ```bash
   cd plugins/presentation-motion/examples/26-poc-realtime-pipeline
   node ../../capture/compose/compose.mjs demo.yaml   # → out/26-poc-realtime-pipeline-pptx.mp4
   ```
2. Claude Code'da yeni proje açın: `/presentation-motion:new q3-yonetim "Q3 platform durumu" --audience executive --minutes 5`
3. Kaynak dokümandan storyboard çıkarın: `/presentation-motion:storyboard rapor.md`
4. Sahneleri üretin ve önizleyin: `/presentation-motion:build`, ardından `/presentation-motion:preview`
5. İnceleyip final videoyu alın: `/presentation-motion:review`, ardından `/presentation-motion:render pptx --max-mb 25`

Tipik akış:

```text
/new → /storyboard → /build → /review → /render
POC/demo: /poc → /record | /superset | /tape → /demo
```

## 5. Kullanım senaryoları

### Senaryo 1: Yönetim kuruluna 5 dakikalık karar sunumu
- **Durum:** Lakehouse geçişi için bütçe onayı gerekiyor.
- **Ne yazarsınız:** "`business-case.md`'den yönetim kurulu için 5 dakikalık sunum hazırla. BLUF ile başla, 3 kanıt, sonunda karar isteği olsun. Tema kurum rengimiz #0B3D91."
- **Ne olur:** `director` `executive-storytelling` kurallarıyla storyboard yazar, `/theme` kurum renginden tema üretir. `bluf-headline`, `kpi-counter-row` ve `decision-ask` şablonları kullanılır. Sayılar `tr-number-format` ile yazılır.
- **Çıktı:** `storyboard.md`, `scenes/`, `out/*.mp4` (PowerPoint'e gömülebilir H.264). İsterseniz `/handout --onepager` ile tek sayfalık PDF.

### Senaryo 2: Superset self-servis POC demosu
- **Durum:** Bir kuruma Superset ile self-servis raporlama POC'u gösterilecek.
- **Ne yazarsınız:** `/presentation-motion:poc "Superset self-servis" --app superset --url http://localhost:8088`, sonra "dashboard 5'te bölge filtresini Marmara yap, satış grafiğine yakınlaş" ile `/record`.
- **Ne olur:** Problem → Mimari → Pipeline → Canlı sonuç → Metrikler → İstek iskeleti kurulur. `demo-scripter` Playwright akışını yazar, `camera-operator` zoom ve altyazı ekler. `/demo` hepsini birleştirir.
- **Çıktı:** Tek MP4. Hazır örnek: `examples/31-poc-superset-selfservice` (mock Superset ile çalışır; gerçek kurulum için `demo.superset.yaml`).

### Senaryo 3: dbt lineage etki analizi
- **Durum:** Bir kolonun değişmesinin hangi tabloları ve dashboard'ları etkilediğini ekibe göstermek istiyorsunuz.
- **Ne yazarsınız:** `/presentation-motion:lineage target/manifest.json stg_orders --direction downstream`
- **Ne olur:** `solution-architect` manifest'i ayrıştırır, etkilenen düğümler zincirleme yanan bir sahne olarak üretilir.
- **Çıktı:** Lineage sahnesi (`scenes/`), `/render` ile video.

### Senaryo 4: Makale sunumu (akademik)
- **Durum:** Bir makaleyi seminerde anlatacaksınız.
- **Ne yazarsınız:** "`paper.pdf` için 10 dakikalık akademik sunum: mimari diyagramı yeniden çiz, ana formülü parça parça aç, sonuç tablosunu grafiğe çevir, APA kaynakça."
- **Ne olur:** `academic-blocks` ve `data-scientist` formül, grafik ve atıfları üretir; `reviewer --audience academic` doğruluk kontrolü yapar.
- **Çıktı:** Örnek: `examples/32-paper-talk-ijepa`.

### Senaryo 5: Kamu kurumuna sunum ve gizlilik denetimi
- **Durum:** Bir bakanlığa sunulacak demo kaydında gerçek kurum adları ve host adları görünüyor.
- **Ne yazarsınız:** `/presentation-motion:audience government`, ardından `/presentation-motion:redact --dry-run` ve `/presentation-motion:redact`.
- **Ne olur:** `government-protocol` kurallarıyla hitap, logo ve KVKK atıfları düzenlenir. `compliance-guard` kişisel veri, gerçek host/IP/token ve kurum adlarını raporlar; `synthetic-data` bunları tutarlı takma adlarla değiştirir.
- **Çıktı:** Maskelenmiş proje, compliance raporu, çevrimdışı teslim edilebilir video.

## 6. Dosya yapısı

```text
plugins/presentation-motion/
├── .claude-plugin/plugin.json
├── agents/        12 agent
├── commands/      24 komut
├── skills/        14 skill (her biri SKILL.md)
├── hooks/hooks.json            Write/Edit sonrası determinism lint'i
├── templates/                  8 hazır şablon + _base/ runtime + CATALOG.md
├── capture/
│   ├── playwright/             recorder.mjs, overlay.js, mask.css, adapters/
│   ├── terminal/               VHS tape örnekleri
│   └── compose/                compose.mjs, demo.schema.json
├── scripts/                    render-scene, lint-determinism, size-budget, probe, fetch-vendor
├── vendor/                     GSAP, KaTeX, D3, rough-notation (çevrimdışı)
├── examples/                   30+ örnek kataloğu (README.md)
├── package.json                playwright, yaml
└── .gitignore                  node_modules/, out/, .auth/
```

Nereye bakılır:
- Sahne yazma kuralları: `skills/deterministic-render/SKILL.md`
- Hangi şablon hazır, hangi veri alanlarını alıyor: `templates/CATALOG.md`
- `demo.yaml` yapısı: `capture/compose/demo.schema.json` ve `examples/26-poc-realtime-pipeline/demo.yaml`
- Kayıt adım dili: `skills/ui-capture/SKILL.md`

## 7. İpuçları ve sınırlamalar

- **Hook tüm projelerde çalışır.** Plugin her Write/Edit sonrası `lint-determinism.mjs --hook` çalıştırır. Sahne/şablon dışındaki dosyalarda hiçbir şey yapmadan çıkar ve ek paket gerektirmez, ama Node.js'in kurulu olması gerekir.
- **Örneklerin çoğu planlı.** `examples/README.md`'deki 32 örnekten yalnızca 26, 31 ve 32 çalışıyor; 18 iskelet, diğerleri 📝 planlı.
- **Adapter seçicileri doğrulanmadı.** Superset, Airflow ve Jupyter adapter'ları gerçek bir kurulumda denenmedi. Sürümünüze göre selector'ları güncelleyin; `/rerecord --check` kaydetmeden selector'ları doğrular.
- **VHS terminal segmenti test edilmedi.** VHS yoksa `terminal` HTML şablonu kullanılır, o çalışıyor.
- **KaTeX fontları repoda yok.** Formül sahneleri için `node_modules` veya jsDelivr'dan `katex@0.16.11/dist/fonts/` içeriğini `vendor/katex/fonts/` altına kopyalayın; yoksa formüller yedek fontla çizilir.
- **`web` profili AV1 ister.** ffmpeg'iniz `libsvtav1` içermiyorsa `pptx` profilini kullanın.
- **Determinism kuralı.** Sahnede gerçek zamana bağlı hiçbir şey olmamalı (`Date.now`, `Math.random`, CSS transition, scroll tetikleyicisi). Zaman için `PM.time()`, rastgelelik için `PM.random()` kullanılır ve her hareket timeline'a bağlanır, yoksa önizleme ile video farklı olur.
- **GSAP lisansı.** GSAP "No-Charge" lisansıyla gelir; ticari bir üründe yeniden dağıtmadan önce lisans koşullarını okuyun (`vendor/README.md`).
- **Boyut.** `npm install` ve render'dan sonra klasör 70 MB'ı geçebilir. `node_modules/` ve `out/` `.gitignore`'da; repoya yalnızca kaynak gider.

## 8. Bakım

- Plugin bu repoda elle yazılmıştır; upstream'den çekilmez. `build-platform-plugins.ps1` ve `build-platform-plugins_linux.sh` onu `presentation` kategorisiyle `marketplace.json`'a ekler.
- Değişiklikten sonra `plugin.json` sürümünü artırın; `marketplace.json`'daki sürüm de aynı olmalı, yoksa kurulu kullanıcılara güncelleme gitmez.
- Şablon kontrolü (plugin klasöründe): `node scripts/lint-determinism.mjs templates`, `node scripts/size-budget.mjs templates`, `node scripts/render-scene.mjs templates/kpi-counter-row.html --profile pptx -o out/kpi.mp4`
- Uçtan uca kontrol: `examples/26-poc-realtime-pipeline` içinde `node ../../capture/compose/compose.mjs demo.yaml`, ardından `node ../../scripts/probe.mjs out/26-poc-realtime-pipeline-pptx.mp4 --max-mb 25`.
- Kütüphane güncellemesi: `scripts/fetch-vendor.mjs` içindeki sürümleri değiştirip çalıştırın, `vendor/README.md`'yi güncelleyin.
- İlgili dokümanlar: [apache-superset.md](apache-superset.md) (Superset demoları), [openmetadata.md](openmetadata.md) (lineage), [data-engineering-design-patterns.md](data-engineering-design-patterns.md).
