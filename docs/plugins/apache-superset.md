# apache-superset

> Apache Superset 6.x'i çok kiracılı, açık kaynak bir veri platformunun BI katmanı olarak işletmek ve okunabilir dashboard'lar tasarlamak için skill, agent ve komutlar.

| | |
| --- | --- |
| Sürüm | 0.3.0 |
| Kategori | data |
| Kaynak | `plugins/apache-superset/` |
| Kaynak materyal | Superset ve Preset dokümanları, Stackable operator, 7 dashboard/görselleştirme kitabının bölüm özetleri ve Superset 4.1.2 lab'ları, iç araştırma dokümanları (genelleştirilmiş) |

## 1. Ne işe yarar?

Superset'i platformun sunum katmanı olarak kurarken, güvenliğini sağlarken ve dashboard'ları kod olarak yönetirken ihtiyaç duyulan kararları ve politikaları taşır: dağıtım (Stackable/Helm), Keycloak ve rol eşleme, RLS, guest token ile gömme, dashboards-as-code, dataset/semantik katman, önbellek ve performans.

v0.2 ile üç alan eklendi. Birincisi dashboard kalitesi: tasarım standardı, yaşam döngüsü, storytelling/declutter, grafik seçimi ve renk erişilebilirliği. İkincisi gerçek Superset davranışı: lab'larda doğrulanmış viz_type ve API sınırları. Üçüncüsü kurumdan bağımsız yönetişim: gizlilik/uyum, AI katmanı ve Superset iç yapısı.

v0.3 test katmanını ekler: Superset'i platform bileşeni olarak test etme stratejisi (statik, unit, contract, integration, E2E, performans, görsel, güvenlik, upgrade; `qa-test-strategy` envanterindeki `T-bi-*` id'leriyle eşlenmiş), tek kullanımlık test ortamı (harness) ve sürüm yükseltme regresyonu.

Çağrı sırası (API/MCP/CLI mekaniği) Preset'in Agent Skills'ine bırakılmıştır; bu plugin politika ve standart taşır.

## 2. Kurulum

```text
/plugin marketplace add muratozturrk/data-platform-plugins
/plugin install apache-superset@data-platform-plugins
```

Yerel geliştirme: `claude --plugin-dir ./plugins/apache-superset`
Gereksinimler: örnek script'ler için Python 3 ve `pyyaml`; `chartdata.py` için pandas. Test örnekleri için ek olarak `pytest`, `requests`, Docker (compose), isteğe bağlı schemathesis, Playwright ve k6.

## 3. İçerik

### Skill'ler
| Skill | Ne zaman devreye girer | İçerdiği konular |
| --- | --- | --- |
| `superset-architecture-deployment` | Kurulum, ölçekleme, yükseltme | Bileşenler, operator ve Helm, imaj, config, upgrade |
| `superset-security-model` | Rol, yetki, RLS, SSO | FAB rolleri, DAR, dashboard RBAC, Keycloak OIDC eşleme |
| `superset-multi-tenancy-embedding` | Çok kiracılık, gömme | Pool/silo/bridge, embedded SDK, guest token |
| `superset-dashboards-as-code` | Asset'leri Git'te yönetme | Export/import, `superset-cli sync native`, CI, drift |
| `superset-dataset-semantic-layer` | Dataset ve metrik tasarımı | Dataset sözleşmesi, metrikler, dbt, Trino/ClickHouse/Iceberg |
| `superset-performance-caching` | Yavaş dashboard, ambar koruma | Cache katmanları, async, limitler, bütçeler |
| `superset-dashboard-design` | Dashboard kurma veya gözden geçirme | Bilgi tasarımı standardı, düzen, filtreler, tamamlanma ölçütü |
| `superset-extensions-mcp` | 6.1 uzantıları, MCP | Extensions framework, MCP servisi, API/CLI/MCP seçimi |
| `superset-chart-api-limits` | viz_type veya API davranışı tartışılırken | Doğrulanmış viz_type bilgisi, olmayan kontroller ve çözümleri, chart-data tuzakları (4.1.2) |
| `superset-chart-selection` | Grafik tipi seçimi | Grafik, ölçek ve sayı biçimi kuralları, Superset karşılıkları |
| `superset-dashboard-lifecycle` | Brief'ten bakıma kadar süreç | Keşif, prototip, kullanıcı testi, benimsenme ölçümü, senaryo kalıpları |
| `superset-storytelling-declutter` | Dashboard'u sadeleştirme, hikâye kurma | Knaflic dersleri, Big Idea, storyboard, declutter kuralları |
| `superset-color-accessibility` | Palet ve kontrast kararları | Palet türleri, kontrast, renk körlüğü, `label_colors` |
| `superset-privacy-compliance` | Gizlilik ve veri koruma | Küçük hücre baskılama, saklama/imha, maskeli dışa aktarım, SQL Lab korkulukları, denetim kaydı |
| `superset-ai-layer` | Superset üzerinde AI yardımı | Yönetişimli MCP geçidi, NL2SQL, sorgu doğrulama, değerlendirme |
| `superset-internals-contributing` | Superset'i genişletme veya hata ayıklama | DAO/Command, superset-core, WebSocket sunucusu, Extensions CLI, CI/test |
| `superset-testing-strategy` | Superset'e dokunan her testi planlarken veya yazarken | Test piramidi, OpenAPI/schemathesis contract, RLS/maske tutarlılığı (JSON/CSV/embedded), guest token kapsamı, k6, Playwright, görsel regresyon, güvenlik başlıkları; `T-bi-*` eşleme |
| `superset-test-harness` | Integration, E2E veya performans testi çalıştırırken | Compose/Testcontainers ile tek kullanımlık stack (PostgreSQL, Redis, Trino, Keycloak, Mailpit), test config, iki kurumlu seed, API istemcisi fixture'ları, reset/teardown |
| `superset-upgrade-regression` | Sürüm yükseltme veya imaj yeniden derleme | UPDATING.md triyajı, metadata DB provası, asset re-export diff, SDK/extension uyumu, parite katmanları, geri alma |

### Agent'lar
| Agent | Model | Ne yapar | Ne zaman kullanılır |
| --- | --- | --- | --- |
| `superset-platform-engineer` | opus | Dağıtım, Keycloak, RLS, gömme, performans, yükseltme | Platform kurulumu ve işletimi |
| `superset-dashboard-designer` | sonnet | Dashboard ve grafik tasarımı | Yeni dashboard veya yeniden tasarım |
| `superset-asset-reviewer` | sonnet | Export edilmiş asset YAML'ının PR incelemesi | Asset değişiklikleri |
| `superset-compliance-reviewer` | sonnet | Config, asset ve dashboard'larda gizlilik/yönetişim incelemesi | Yayın öncesi uyum kontrolü |

### Komutlar
| Komut | Argüman | Ne yapar |
| --- | --- | --- |
| `/superset-tenant-onboard` | kiracı bilgisi | Tenant controller'ın Superset'te oluşturacağı her şeyi çıkarır |
| `/superset-embed-plan` | gömme senaryosu | Guest token servisi, RLS, SDK, sıkılaştırma ve test planı |
| `/superset-dashboard-review` | dashboard | On üç hata ve Superset kontrolleriyle inceleme, yeni düzen |
| `/superset-assets-sync` | repo/ortam | Dashboards-as-code hattı (şablonlama, CI sync, drift, geri alma) |
| `/superset-dashboard-lint` | export klasörü | Few'in on üç hatası ve declutter kuralları için sezgisel lint |
| `/superset-dashboard-brief` | dashboard fikri | Kitle, karar, KPI, veri, güvenlik ve başarı ölçütlerini içeren brief |
| `/superset-chart-pick` | soru/veri tarifi | viz_type ve ayar önerir, kaçınılacakları söyler |
| `/superset-test-plan` | değişiklik/özellik/sürüm | `T-bi-*` id'li test listesi, araç, harness boyutu, takvim, kabul ölçütleri ve envanter delta YAML'ı |
| `/superset-assets-validate` | asset paketi | Şema, UUID kararlılığı, kimlik bilgisi, RLS varlığı ve isimlendirme için statik doğrulama; CI çıkış kodu |
| `/superset-upgrade-regression` | hedef sürüm | Triyaj, config/flag diff, DB provası, parite katmanları, rollout/rollback ve kanıt kaydı |

### Araçlar (örnek script'ler)
| Dosya | Ne yapar | Ek bağımlılık |
| --- | --- | --- |
| `skills/superset-dashboard-design/examples/dashboard_lint.py` | Export klasörünü Few'in on üç hatasına göre tarar (çevrimdışı) | `pyyaml` |
| `skills/superset-storytelling-declutter/examples/declutter_lint.py` | Grafik YAML'ında declutter kontrolleri | `pyyaml` |
| `skills/superset-chart-api-limits/examples/superset_client.py` | Lab Superset'ine API istemcisi | `requests` |
| `skills/superset-chart-selection/examples/rules.py`, `chartdata.py` | Ölçek kuralları ve zaman ekseni sorgu yardımcıları | `chartdata.py` için pandas |
| `skills/superset-color-accessibility/examples/checks.py` | Kontrast ve palet kontrolleri | yok |
| `skills/superset-testing-strategy/examples/assets_validate.py` | Asset paketini statik doğrular (`--baseline`, `--superset-version`) | `pyyaml` |
| `skills/superset-testing-strategy/examples/test_*.py`, `k6_dashboard_load.js`, `playwright/*.spec.ts` | Contract, RLS/maske, guest token, güvenlik başlığı testleri; yük ve E2E/görsel testler | `pytest`, `requests`; k6; Playwright |
| `skills/superset-test-harness/examples/` | `docker-compose.test.yml`, `conftest.py`, `seed_institutions.sh`, idempotent import / Alerts & Reports / cache izolasyonu testleri | Docker, `pytest` |
| `skills/superset-upgrade-regression/examples/upgrade_check.py` | Config ve flag'leri hedef sürüme göre kontrol eder (`--config --current --target`) | yok |

## 4. Hızlı başlangıç

1. Planlama: `/superset-dashboard-brief` ile kitle ve KPI'ları yazın.
2. Grafik seçimi: `/superset-chart-pick` ile her soru için viz_type belirleyin.
3. Uygulama: `superset-dashboard-designer` agent'ı ile düzeni kurun.
4. Kontrol: Export alıp `/superset-dashboard-lint` çalıştırın.
5. Teslim: `/superset-assets-sync` ile CI hattına bağlayın.

## 5. Kullanım senaryoları

**Yeni tenant ekleme**
Durum: Platforma yeni bir kiracı geliyor.
Prompt: `/superset-tenant-onboard acme, silo`
Ne olur: Komut, rol, RLS, veritabanı bağlantısı ve dataset adımlarını çıkarır.

**Dashboard incelemesi**
Durum: Hazır dashboard kalabalık ve okunmuyor.
Prompt: `/superset-dashboard-lint ./export/sales` ardından `/superset-dashboard-review`
Ne olur: Lint bulguları ("bak" işareti, hüküm değil) ve düzen önerisi gelir.

**Gizlilik incelemesi**
Durum: Dashboard dışarıya veya geniş bir kitleye açılacak.
Prompt: `superset-compliance-reviewer ile bu asset klasörünü incele`
Ne olur: Agent, gizlilik ve güvenlik skill'lerine göre bulguları sıralar.

**Değişiklik için test planı**
Durum: Yeni bir gömülü dashboard paketi yayına çıkacak.
Prompt: `/superset-test-plan yeni embedded satış dashboard'u`
Ne olur: `T-bi-*` id'li test tablosu ve `qa-test-strategy` şemasında envanter delta YAML'ı gelir.

**Sürüm yükseltme**
Durum: Superset bir üst sürüme geçecek.
Prompt: `/superset-upgrade-regression 6.0 -> 6.1`
Ne olur: Kontrol listesi, komutlar ve upgrade kanıt kaydı şablonu çıkar.

## 6. Dosya yapısı

```text
plugins/apache-superset/
├── .claude-plugin/plugin.json
├── README.md
├── agents/      # 4 agent
├── commands/    # 10 komut
└── skills/<skill>/
    ├── SKILL.md
    ├── references/sources.md
    └── examples/        # skill'e ait örnekler (config, CI, lint, istemci)
```

## 7. İpuçları ve sınırlamalar

- "Doğrulanmış (4.1.2)" etiketli bilgiler Superset 4.1.2 lab'larından gelir; 6.x'te farklı olabilir. Kullanmadan önce `registered_viz_keys` ile kontrol edin.
- Lab'larda tarayıcıda render doğrulaması yapılmadı; bazı bilgiler yalnızca frontend bundle'ının okunmasına dayanır.
- Lint çıktıları sezgiseldir. `dashboard_lint.py` çevrimdışı modda pie toplamı kuralını atlar.
- `dashboard_lint.py` export klasörü okuyucusu (`from_export`) örnek bir sahte export ile denendi; gerçek export paketlerinde alan adları farklıysa uyarlama gerekebilir.
- Guest token, gömme, alerts & reports ve `logs` saklama davranışları lab'larda doğrulanmadı.
- Test örnekleri şablondur; çalışan bir harness'a karşı koşulmadı. `conftest.py` içindeki `test-guest-secret` yalnızca test ortamı içindir.
- Gizlilik ve AI skill'leri kurumdan bağımsız yazıldı; mevzuata özgü maddeler (ör. KVKK/GDPR madde numaraları) içermez, hukuki görüş yerine geçmez.

## 8. Bakım

- Yeni Superset sürümünde "doğrulanmış" etiketli tabloları lab ile yeniden doğrulayın.
- Plugin'in kendi `README.md` dosyası elle tutulur; `build-platform-plugins.ps1` yalnızca marketplace ve kök README'yi üretir (kategori `$cats["apache-superset"] = "data"`).
- İlgili: [apache-polaris.md](apache-polaris.md) (Iceberg REST katalog).
