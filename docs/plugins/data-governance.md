# data-governance

> Bir veri platformunda veri sözleşmesi, katalog ve soyağacı, sınıflandırma ve erişim kontrolü, kalite SLA'ları, KVKK uyumu, anonimleştirme ve yönetişim aracı değerlendirmesi için ürüne bağlı olmayan, kanıta dayalı skill, agent ve komutlar.

| | |
| --- | --- |
| Sürüm | 0.2.0 |
| Kategori | governance |
| Kaynak | `plugins/data-governance/` |
| Kaynak materyal | Veri yönetişimi yazılımları üzerine kod seviyesinde kanıtlı karşılaştırmalı araştırma (OpenMetadata 1.13.3, DataHub 1.7.0, Apache Atlas 2.5.0); 14 alan / 90 yetenekli dondurulmuş rubrik; KVKK mevzuatının resmî metinlerden doğrulanmış eşlemesi; anonimleştirme literatürü ve ARX kaynak kodu analizi |

## 1. Ne işe yarar?

Veri yönetişimi, platformdaki her veri setinin bir sahibi, sürümlü bir sözleşmesi, bir sınıflandırması, bir erişim politikası, bir saklama süresi ve ölçülen kalite hedefleri olmasını sağlamaktır. Bu plugin bunları belirli bir ürüne bağlı kalmadan anlatır. OpenMetadata, DataHub ve Apache Atlas yalnız ölçülmüş örnek olarak geçer. Kurulu platformun bunlardan biri olduğu varsayılmaz.

Plugin'in dayanağı genel tavsiyeler değil, ölçümdür. Üç açık kaynak katalog aynı rubrikle, kaynak kodu okunarak puanlandı. Skill'ler bu puanları `dosya:satır` kanıtıyla taşır ve her iddianın kanıt tipini belirtir (`K` kod, `D` resmî doküman, `İ` issue/yol haritası, `B` üretici beyanı). Bir özellik bulunamadığında "yok" denmez, "doğrulanamadı" denir. Araştırmanın ölçmediği konular (ör. sözleşme uyumluluk kuralları, semver) "genel uygulama" diye ayrıca işaretlenir.

Ölçümden çıkan ana sonuç şudur: açık kaynak kataloglar kaliteyi, sahipliği ve soyağacını **kaydetmede ve göstermede** iyidir, ama neredeyse hiçbir şeyi **zorlamaz**. Sorgu anında maskeleme ve satır/kolon güvenliği katalogda değil, sorgu motorunda ya da Ranger benzeri bir yetkilendirme aracında uygulanır. KVKK yükümlülüklerinin hepsi kaydedilebilir, ama "en zayıf halka" kuralıyla 49 yükümlülükten OpenMetadata 0'ını, DataHub 4'ünü, Atlas 0'ını tam karşılar. Plugin bu yüzden her kontrol için "zorlanan" ve "yalnız uzlaşma" ayrımı yapar.

`openmetadata` plugin'i aynı araştırmanın OpenMetadata'ya özgü kısmını (kurulum, connector'lar, boşluk kapatma) taşır. Bu plugin ise ürün seçimi yapılmadan ya da hangi ürün seçilirse seçilsin geçerli olanı anlatır.

## 2. Kurulum

```text
/plugin marketplace add muratozturrk/data-platform-plugins
/plugin install data-governance@data-platform-plugins
```

Yerel geliştirme: `claude --plugin-dir ./plugins/data-governance`

Gereksinimler: ek bir şey gerekmez. Skill'ler bilgi ve şablon içerir, çalıştırılacak kod yoktur.

## 3. İçerik

### Skill'ler

| Skill | Ne zaman devreye girer | İçerdiği konular |
| --- | --- | --- |
| `data-contracts` | Yeni veri seti yayınlanırken, şema değişirken, veri ürünü tanımlanırken | Sözleşme biçimi (şema, anlam, kalite, SLA, güvenlik, değişim kuralları), ODCS tarzı örnek YAML, uyumluluk kuralları ve semver, kullanımdan kaldırma süresi, CI kontrolü; veri ürünü ve sahiplik; üç kataloğun sözleşmeyi ne kadar sakladığı ve doğruladığı |
| `data-catalog-lineage` | Katalog seçimi, soyağacı tasarımı, şema değişikliğinin etki analizi | D1/D2/D3/D11/D12 yetenek seviyeleri; soyağacı çıkarım yöntemleri (çalışma zamanı, sorgu ayrıştırma, tanım ayrıştırma, elle) ve kör noktaları; kolon seviyesi soyağacı; OpenLineage; 32 yetenekli kanıtlı karşılaştırma, bağlayıcı tablosu, topluluk sağlığı, "hangi durumda hangi katalog" |
| `access-control-policies` | Erişim verilirken, hassas veri sınıflandırılırken, maskeleme tasarlanırken | İki eksenli sınıflandırma, otomatik PII tespiti ve sınırları; politika deposu / karar noktası / uygulama noktası ayrımı; RBAC/ABAC, satır/kolon güvenliği, maskeleme ve tokenizasyon; katalog → Ranger köprüsü; denetim kaydı ve erişim gözden geçirme; D6 karşılaştırması |
| `data-quality-slas` | Veri ürünü için kalite hedefi koyarken, kalite olayı süreci kurarken | Tazelik/tamlık/doğruluk/teklik SLO'ları; olay yaşam döngüsü, önem ve sahiplik; D5/D10 seviyeleri; üç kataloğun test, profil, olay ve anomali yetenekleri |
| `kvkk-compliance` | Kişisel veri envanteri, VERBİS, ilgili kişi başvurusu, ihlal kapsamı, periyodik imha, denetime hazırlık | Doğrulanmış 49 yükümlülüğün platform yeteneğine eşlenmesi; doğrulanmış süreler; m.28 istisnaları; 7499 sayılı Kanun ile yurt dışı aktarım; ürüne bağlı olmayan varlık başına metadata modeli; süreçler ve denetçiye sunulacak kanıt; D14 rubriği. Hukuki görüş değildir. |
| `data-anonymization` | Veri test ortamına, analize ya da dış tarafa giderken; amacı bitmiş veri saklanacaksa | Tanımlayıcı / yarı-tanımlayıcı / hassas öznitelik ayrımı ve otomatik tespiti (Türkçe sorunları dahil); maskeleme, takma ad, k-anonimlik, l-çeşitlilik, t-yakınlık, diferansiyel gizlilik; yeniden tanımlama riski ve ARX tarzı risk raporu; bilgi kaybı ölçütleri; ARX iç yapısı ve tek makine sınırları; dağıtık anonimleştirme literatürü; doküman anonimleştirme (Türkçe NER'in gerçek başarımı) |
| `governance-assessment` | Katalog/yönetişim aracı seçerken, üretici iddiasını sınarken, olgunluk ölçerken | 90 yeteneğin tamamı ve 0–4 seviye çapaları; puanlama ve kanıt kuralları, `null` ile 0 ayrımı; çapraz kontroller; DMBOK bilgi alanı kapsaması (Doküman ve İçerik boşluğu dahil); üç ürünün ölçülmüş sonuçları; kanıt YAML şablonu |

### Agent'lar

| Agent | Model | Ne yapar | Ne zaman kullanılır |
| --- | --- | --- | --- |
| `data-governance-expert` | opus | Sahiplik, sözleşme, sınıflandırma, erişim, saklama ve SLA'yı birlikte tasarlar. Her kontrol için zorlanıyor mu yoksa uzlaşma mı olduğunu söyler. Gerekli skill'i kendisi yükler. | Yeni veri seti, boru hattı, şema değişikliği, veri ürünü ya da tüketici eklenirken |
| `governance-assessor` | opus | Rubrikle puanlama yapar: sürümü sabitler, her puana kanıt bağlar, kanıtlanmamış yokluğu `null` bırakır, çapraz kontrolleri çalıştırır. | Araç seçiminde, üretici iddiası sorgulanırken, olgunluk ölçümünde |
| `kvkk-compliance-reviewer` | opus | Platformu, metadata modelini ya da süreci KVKK yükümlülüklerine göre inceler. Her yükümlülük için "zorlanan / uzlaşma / eksik / uygulanamaz" der. | Denetim öncesi; kişisel veri platforma alınırken; sınıflandırma veya saklama modeli değişirken |
| `privacy-engineer` | opus | Anonimleştirme tasarlar: tanımlayıcıları bulur, gizlilik modelini ve parametreyi seçer, risk ve bilgi kaybını ölçtürür, ölçek planını ve kanıt izini yazar. | Veri korunan alanın dışına çıkarken ya da biri bir veri setine "anonim" dediğinde |

### Komutlar

| Komut | Argüman | Ne yapar |
| --- | --- | --- |
| `/governance-audit` | Dosya yolları, şema, boru hattı kodu, katalog dışa aktarımı ya da tasarım | 9 maddelik kontrol (sahiplik, sözleşme, sınıflandırma, erişim, saklama, kalite, soyağacı, KVKK, anonimleştirme). Her madde ilgili skill'in ölçütüyle değerlendirilir, risk sırasıyla düzeltme listesi çıkar. |
| `/governance-assess` | Ürün adı ve sürümü, depo yolu, ya da "bize hangi katalog?" sorusu | Değerlendirmeyi assessor'a yaptırır. Seçim sorusunda alanları ağırlıklandırır ve ölçülmüş sonuçlardan başlar. |
| `/kvkk-audit` | Taksonomi, metadata modeli, saklama tablosu, süreç dokümanı ya da bir soru | İncelemeyi KVKK reviewer'a yaptırır. Anonimleştirme kullanılıyorsa riski privacy engineer'a kontrol ettirir. |
| `/anonymization-plan` | Şema, kolon adları, satır sayısı, motor, çıktının amacı ve alıcısı (gerçek kişisel veri değil) | Saldırgan modeli, öznitelik sınıfları, model ve parametre, risk ve kayıp ölçütleri, çalıştırma planı ve saklanacak kanıtı verir. |

## 4. Hızlı başlangıç

1. Plugin'i kurun.
2. Bir veri setini denetletin: `/governance-audit ./schemas/customer_orders.json ./dags/orders.py`.
3. Dönen tabloda ⚠️ (yalnız uzlaşma) ve ❌ (eksik) satırlarına bakın. Düzeltme listesi risk sırasıyla gelir.
4. Kişisel veri varsa: `/kvkk-audit ./governance/classifications.yaml`.
5. Veri test ortamına ya da dışarıya gidecekse: `/anonymization-plan` ile kolon adlarını ve amacı verin.

## 5. Kullanım senaryoları

**Katalog seçimi.**
Ne yazarsınız: `/governance-assess OpenMetadata, DataHub ve Atlas; bizim için soyağacı, erişim ve KVKK önemli`.
Ne olur: Assessor, `governance-assessment` ve `data-catalog-lineage` skill'lerindeki ölçülmüş sonuçlardan başlar ve ağırlıkları uygular. Sizin sürümünüzde yeniden doğrulanması gereken puanları ayrıca işaretler.
Çıktı: Alan bazında skor tablosu, belirleyici puanların kanıtı ve doğrulanmamış kısımlar.

**Şema değişikliği.**
Ne yazarsınız: "`orders` tablosunda `customer_id` kolonunu yeniden adlandıracağız, ne bozulur?"
Ne olur: `data-contracts` yeniden adlandırmanın kırıcı değişiklik olduğunu söyler. Bazı araçlar bunu "uyumlu" diye etiketler, ölçülen bir örneği de bu yüzden uyarı olarak verir. `data-catalog-lineage` etkilenen aşağı akış varlıklarının nasıl bulunacağını anlatır.
Çıktı: Kullanımdan kaldırma planı, sözleşme sürüm artışı ve CI kontrolü.

**Maskeleme nereye konur.**
Ne yazarsınız: "TCKN kolonunu analistlerden gizlemek istiyoruz, katalogda politika yazsak yeter mi?"
Ne olur: `access-control-policies` ölçülen kataloglardan hiçbirinin veri yolunda olmadığını, maskelemenin sorgu motorunda ya da Ranger benzeri bir araçta uygulanması gerektiğini anlatır. Katalog etiketinden politika üretme yolunu verir.
Çıktı: Örnek maskeleme görünümü ve politika şekli (açıklayıcı örnek olarak işaretli) ve sapma kontrolü.

**"Bu veri anonim" iddiası.**
Ne yazarsınız: "TCKN'yi sildik, veri artık anonim mi?"
Ne olur: `privacy-engineer` maskelemenin anonimleştirme olmadığını söyler. Yaş, ilçe ve tarih gibi yarı-tanımlayıcıların birlikte kişiyi tekilleştirebileceğini gösterir ve riski ölçen sorguyu verir.
Çıktı: Saldırgan modeli, ölçülecek risk ve bilgi kaybı, önerilen model ve parametre.

**İhlal anında kapsam.**
Ne yazarsınız: "Üç tablo sızdı, hangi kişi kategorileri etkilendi?"
Ne olur: `kvkk-compliance` soyağacı ve sınıflandırmayla kapsam çıkarma adımlarını verir. 72 saatlik sürenin ihlalin öğrenildiği andan başladığını hatırlatır.
Çıktı: Kapsam çıkarma adımları ve kayda geçirilecek kanıt.

## 6. Dosya yapısı

```text
plugins/data-governance/
├── .claude-plugin/plugin.json
├── agents/
│   ├── data-governance-expert.md
│   ├── governance-assessor.md
│   ├── kvkk-compliance-reviewer.md
│   └── privacy-engineer.md
├── commands/
│   ├── governance-audit.md
│   ├── governance-assess.md
│   ├── kvkk-audit.md
│   └── anonymization-plan.md
├── examples/README.md
└── skills/
    ├── data-contracts/          contract-spec, change-management, data-products-and-ownership
    ├── data-catalog-lineage/    catalog-capabilities, lineage-methods, catalog-comparison
    ├── access-control-policies/ classification, policy-enforcement, d6-comparison
    ├── data-quality-slas/       quality-capabilities, slo-and-incidents, d5-d10-comparison
    ├── kvkk-compliance/         obligations-map, d14-rubric, metadata-model, processes-and-evidence
    ├── data-anonymization/      privacy-models-and-risk, discovery-and-risk-report, arx-and-scaling, document-anonymization
    └── governance-assessment/   rubric, assessment-procedure, dmbok-coverage, measured-results (+ assets/evidence-template.yaml)
```

Her skill'de `SKILL.md` özet ve karar tablosunu, `references/` ise ayrıntıyı taşır. `references/sources.md` kaynakları, araştırmadan farklılaşan düzeltmeleri ve bilerek dışarıda bırakılanları listeler.

## 7. İpuçları ve sınırlamalar

- **Sürümler sabittir.** Ölçümler OpenMetadata 1.13.3, DataHub 1.7.0 ve Atlas 2.5.0 içindir. Yeni sürümde karar vermeden önce ilgili puanı yeniden doğrulayın.
- **Kaydetmek, zorlamak değildir.** Bir alanın modelde bulunması doldurulmasının zorunlu olduğu anlamına gelmez. Skill'ler bu ayrımı her yerde yapar.
- **Anonimleştirme ölçekte kanıtlanmamıştır.** k değerini ölçmek büyük veride ucuzdur. İyi bir genelleştirmeyi aramak ise petabayt ölçeğinde hakemli bir çalışmayla gösterilmemiştir. Skill bunu açıkça yazar.
- **Doküman anonimleştirme olasılıksaldır.** Türkçe NER için garanti verilmez, beklenen başarım verilir.
- **Bazı içerik ölçüm değil, genel uygulamadır.** Sözleşme uyumluluk kuralları, semver ve CI örnekleri böyledir. Skill'lerde bu şekilde işaretlidir.
- **Kapsam dışı.** Kapalı kaynak ürünler ve kuruma ya da sektöre özgü gereksinimler plugin'e alınmadı.
- **KVKK içeriği hukuki görüş değildir.** Madde ve süreler resmî metinlerden doğrulandı. Uygulamayı kurumunuzun hukuk birimiyle teyit edin.

## 8. Bakım

- Bir ürünün yeni sürümüne geçerken `governance-assessment` prosedürüyle değişen yetenekleri yeniden ölçün, sonra karşılaştırma tablolarını güncelleyin.
- Kanıt referansları (`dosya:satır`) sürüme bağlıdır. Referans tutmuyorsa iddia yeniden doğrulanmalıdır.
- Plugin `build-platform-plugins.ps1` / `.sh` tarafından ezilmez ("data-governance zaten var, korunuyor"). Kategori kaydı iki script'te `data-governance` → `governance` olarak durur.
- İlgili dokümanlar: [README.md](README.md), [openmetadata.md](openmetadata.md), [_TEMPLATE.md](_TEMPLATE.md).
