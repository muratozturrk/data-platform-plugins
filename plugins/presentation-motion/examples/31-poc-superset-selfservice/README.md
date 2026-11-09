# 31 · Self-Servis Raporlama – Superset POC

Bu klasör, yönetici kitlesine yönelik yaklaşık 1 dakikalık bir POC demo videosu üretir. Senaryo örnektir: bölge müdürleri satış raporlarını BI ekibine talep açmadan Superset üzerinden kendileri alıyor. Rakamların hepsi yer tutucudur (bkz. [Doldurulacak veriler](#doldurulacak-veriler-veri-gerekli)).

| Çıktı | Değer |
|---|---|
| Video | `out/31-poc-superset-selfservice-pptx.mp4` |
| Süre / boyut | 62,9 sn · 2,6 MB |
| Format | 1920×1080 · H.264 · yuv420p · 30 fps (PowerPoint uyumlu) |

İki sürümü var:

- **`demo.yaml`**: Superset yerine yerel bir sahte dashboard'u (`mock-app/dashboard.html`) kaydeder. Superset kurulu olmayan her makinede çalışır. Yukarıdaki video bununla üretildi.
- **`demo.superset.yaml`**: Aynı akışı çalışan gerçek bir Superset'ten kaydeder ve ayrıca bir SQL Lab segmenti ekler. Kullanmadan önce [hazırlık](#gerçek-superset-ile-kullanım) gerekir.

---

## Klasör yapısı

```
31-poc-superset-selfservice/
├─ deck.yaml               # başlık, kitle (executive), tema (corporate-light), çıktı profili
├─ storyboard.md           # sahne sırası, tek cümlelik mesajlar, eksik veri listesi
├─ demo.yaml               # video tarifi: sahte dashboard ile
├─ demo.superset.yaml      # video tarifi: gerçek Superset ile
├─ data/architecture.yaml  # mimari diyagramın verisi (katmanlar, düğümler, oklar)
├─ mock-app/dashboard.html # Superset görünümlü sahte dashboard (sentetik veri, logo yok)
├─ scenes/                 # render sırasında otomatik üretilir (_gen-*, _base) → git'e girmez
├─ vendor/                 # render sırasında otomatik kopyalanır → git'e girmez
├─ .auth/                  # Superset oturum dosyası → git'e girmez
└─ out/                    # videolar → git'e girmez
```

## Videonun akışı

| # | Segment | Şablon / araç | Mesaj | Süre |
|---|---|---|---|---|
| 0 | Açılış | `title` | POC'nin adı ve kapsamı | 5,2 sn |
| 1 | Özet | `bluf-headline` | Raporlar 72× daha hızlı (3 gün → 1 saat) | 6,1 sn |
| 2 | Mimari | `architecture-layers` + `data/architecture.yaml` | ERP/CRM/hedef dosyası → Airflow + dbt → veri ambarı → Superset | 15,5 sn |
| 3 | Pipeline | `terminal` | `dbt build` (6/6 başarılı) → `superset import-dashboards` | 12,5 sn |
| 4 | Canlı sonuç | `browser` (Playwright kaydı) | Bölge filtresi değişince tüm grafikler güncelleniyor | 10,6 sn |
| 5 | Etki | `kpi-counter-row` | 72 → 1 saat · ayda 120 → 18 talep · 85 kullanıcı | 6,3 sn |
| 6 | Karar | `decision-ask` | Tüm bölgelere yaygınlaştırma için 2 aylık pilot onayı | 6,8 sn |

Segment 3'teki terminal çıktısı canlı çalıştırılmadı, gerçekçi bir örnek olarak yazıldı. Gerçek çıktıyı kullanmak isterseniz `demo.yaml` içindeki `lines` listesini güncelleyin.

---

## Hızlı başlangıç (sahte dashboard)

Gereksinimler: Node.js, ffmpeg/ffprobe (PATH'te) ve eklenti kökünde `npm install` (Playwright Chromium dahil).

```bash
cd presentation-motion/examples/31-poc-superset-selfservice
node ../../capture/compose/compose.mjs demo.yaml
```

- Render 3–4 dakika sürer. HTML sahneleri kare kare çizilir, tarayıcı segmenti ise gerçek zamanlı kaydedilir.
- Komutu **bu klasörden** çalıştırın. `mock-app/...` ve `.auth/...` yolları bu klasöre göre çözülür.
- Bir şey değiştirdiyseniz yalnızca aynı komutu tekrar çalıştırmanız yeterli.

### Sahnelerin tek tek önizlemesi

```bash
# Claude Code içinde
/presentation-motion:preview
```

Önizlemede Space oynatır/duraklatır, R baştan başlatır, ←/→ ileri-geri sarar.

---

## Doldurulacak veriler (`[VERİ GEREKLİ]`)

Videodaki rakamların hepsi **örnektir**. Dışarıya göstermeden önce gerçek POC ölçümleriyle değiştirin. Değerler hem `demo.yaml` hem `demo.superset.yaml` içinde ayrı ayrı yazılı; ikisini birlikte güncelleyin.

| Değer | Örnek | Nerede |
|---|---|---|
| Mevcut bekleme süresi | 3 gün / 72 saat | bluf `headline`, `label`; kpi 1. kart `from` |
| POC sonrası rapor süresi | 1 saat | kpi 1. kart `to`, bluf `label` |
| Hızlanma çarpanı | 72× | bluf `number` (= önce ÷ sonra) |
| Aylık rapor talebi (önce → sonra) | 120 → 18 (−%85) | kpi 2. kart `from`, `to`, `delta` |
| Self-servis kullanıcı | 85 (5 bölge) | kpi 3. kart |
| POC süresi | 6 hafta | kpi `title` |
| Pilot kapsamı / maliyeti | 2 sunucu, 2 ay | decision `ask`, `items` |
| Karar tarihi | — | decision `deadline` |

Notlar:

- Sayaç alanları (`number`, `from`, `to`) yalnızca sayı kabul eder. Bu yüzden videoda örnek sayılar görünür; `[VERİ GEREKLİ]` notu kaynak satırında ve karar tarihinde durur. Gerçek veriyi girince `source` satırlarından bu notu silin.
- `bluf-headline` şablonu sayının önüne işaret koyamaz, yalnızca sonuna ek koyabilir. Bu yüzden özet sahnesinde "%85" yerine "72×" kullanıldı. Yüzde göstermek isterseniz `kpi-counter-row` şablonundaki `prefix: "%"` alanını kullanın.
- Rakamların tutarlı kalmasına dikkat edin. Örneğin `number` değeri, bekleme süresinin önce ÷ sonra oranına eşit olmalı.

---

## Gerçek Superset ile kullanım

`demo.superset.yaml` dosyası, sahte dashboard yerine çalışan bir Superset'i Playwright ile kaydeder. Akış şöyledir:

1. Özet ve mimari sahneleri
2. **Dashboard turu**: bölge filtresi → uygula → "Hedef gerçekleşme" grafiğine yakınlaşma
3. **SQL Lab**: sorgu yazılır → çalıştırılır → sonuç tablosu vurgulanır
4. KPI ve karar sahneleri

### 1. Superset tarafında gerekenler

Akıştaki seçiciler aşağıdaki adlarla eşleşmelidir. Ya Superset'i bu adlarla hazırlayın ya da YAML'daki adları kendi kurulumunuza göre değiştirin.

| Gereken | Değer (`demo.superset.yaml`) |
|---|---|
| Dashboard slug'ı | `satis-performansi` → `/superset/dashboard/satis-performansi/` |
| Native filter adı | `Bölge`; seçeneklerinden biri `Marmara` |
| Grafik başlığı | `Hedef gerçekleşme` |
| SQL Lab'de sorgulanacak tablo | `marts.fct_sales` (`region`, `amount` kolonları) |
| Demo kullanıcısı | Dashboard ve SQL Lab yetkisi olan, **yalnızca sentetik veriyi** görebilen bir hesap |

Slug, dashboard'un *Properties → URL slug* alanından verilir. Slug yoksa sayısal id de kullanılabilir (`/superset/dashboard/12/`).

Yerelde deneme ortamı kurmak için resmi Docker kurulumunu kullanabilirsiniz (`docker compose -f docker-compose-image-tag.yml up`). Bu kurulum varsayılan olarak `http://localhost:8088` adresinde çalışır.

### 2. Oturum dosyasını oluşturun (bir kez)

Kullanıcı adı ve parolayı **dosyaya yazmayın**, ortam değişkeni olarak verin:

```powershell
# Windows PowerShell
$env:SUPERSET_USER = "demo"; $env:SUPERSET_PASS = "..."
node ../../capture/playwright/recorder.mjs --login superset --base-url http://localhost:8088 --save .auth/superset.json
```

```bash
# Linux / macOS / Git Bash
export SUPERSET_USER=demo SUPERSET_PASS=...
node ../../capture/playwright/recorder.mjs --login superset --base-url http://localhost:8088 --save .auth/superset.json
```

Başarılı olursa `✓ Oturum kaydedildi: .auth/superset.json` yazar. `.auth/` klasörü `.gitignore` içinde olduğu için repoya girmez.

> **Dikkat:** `.auth/superset.json` dosyası yoksa kayıt hata vermeden **oturumsuz** başlar. Bu durumda giriş sayfası açılır ve `wait: ready` adımı zaman aşımına uğrar. Oturum süresi dolduğunda bu adımı tekrarlayın.

### 3. Adresi ve seçicileri kontrol edin

`demo.superset.yaml` içinde kontrol edilecek yerler:

```yaml
baseUrl: http://localhost:8088                                   # Superset adresi
url: "/superset/dashboard/satis-performansi/?standalone=1"       # 1 = üst menü gizli, 2 = başlık da gizli
mask: ["[data-pii]"]                                             # bulanıklaştırılacak öğeler
```

Seçiciler `capture/playwright/adapters/superset.mjs` dosyasında Superset 4.x/5.x için tanımlandı. Sürümünüzde farklı olabilecek noktalar:

| Adım | Seçici | Ne zaman değiştirmeli |
|---|---|---|
| Filtre aç | `[data-test="filter-control-name"]:has-text("Bölge")` | Tıklama açılır listeyi açmıyorsa, filtrenin `.ant-select` kutusunu hedefleyin |
| Seçenek seç | `select: { option: "Marmara" }` | Ekranda `text="Marmara"` metnine tıklar; aynı metin sayfada başka yerde de varsa yanlış öğeye tıklayabilir |
| Uygula | `[data-test="filter-bar__apply-button"]` | — |
| Grafik zoom | `.chart-slice:has(.header-title:has-text("Hedef gerçekleşme"))` | Grafik başlığı farklıysa |
| SQL editörü | `.ace_text-input` | Superset sürümünüz Monaco editör kullanıyorsa `.monaco-editor textarea` |
| Sorguyu çalıştır | `[data-test="run-query-action"]` | — |
| Sonuç tablosu | `[data-test="table-container"]` | — |

Seçicileri tarayıcı DevTools'ta (`document.querySelector(...)`) ya da `npx playwright codegen http://localhost:8088` ile hızlıca doğrulayabilirsiniz.

**Maskeleme:** `[data-pii]` varsayılan Superset sayfalarında hiçbir öğeyle eşleşmez. Kişisel veri görünen öğeleri, örneğin kullanıcı menüsü, e-posta veya müşteri adı kolonlarını, kendi seçicilerinizle ekleyin. `standalone=1` üst menüyü zaten gizler. Dış paylaşımdan önce `/presentation-motion:review` (compliance-guard) çalıştırın.

### 4. Önce tek segment deneyin

Tüm videoyu render etmeden önce yalnızca tarayıcı akışını test etmek daha hızlıdır. `demo.superset.yaml` içindeki browser segmentini `baseUrl` ile birlikte ayrı bir dosyaya kopyalayın (örn. `capture/superset-tour.yaml`) ve çalıştırın:

```bash
node ../../capture/playwright/recorder.mjs capture/superset-tour.yaml -o out/test-tour.mp4
```

### 5. Tam videoyu üretin

```bash
node ../../capture/compose/compose.mjs demo.superset.yaml
```

### Sık karşılaşılan sorunlar

| Belirti | Neden / çözüm |
|---|---|
| `wait: ready` zaman aşımı (60 sn) | Oturum yok ya da süresi dolmuş (2. adımı tekrarlayın) veya bir grafik hata veriyor / çok yavaş. Yarım yüklenmiş dashboard bilerek kaydedilmez. |
| `SUPERSET_USER / SUPERSET_PASS ... tanımlı değil` | Ortam değişkenleri aynı terminal oturumunda tanımlanmamış. |
| Seçici bulunamadı / yanlış öğeye tıklandı | 3. adımdaki tabloya göre seçiciyi sürümünüze uyarlayın. |
| Kayıtta bildirim/toast görünüyor | Adaptör `.ant-notification` ve toast'ları gizler; yeni bir öğe çıkıyorsa YAML'a `hide: [...]` ekleyin. |
| Arayüz güncellendi, video eskidi | `/presentation-motion:rerecord` tüm tarayıcı ve terminal segmentlerini yeniden çeker. |

---

## Sonraki adımlar

| Ne | Komut |
|---|---|
| Seslendirme ve altyazı (süreyi 2–3 dk'ya uzatır) | `/presentation-motion:subtitles` |
| Kalite ve uyum incelemesi | `/presentation-motion:review` |
| Teknik ekip veya kamu kitlesi için uyarlama | `/presentation-motion:audience` |
| Yöneticiler için tek sayfa PDF özet | `/presentation-motion:handout` |
| Farklı çıktı profili (web, 4k) | `/presentation-motion:render` |
