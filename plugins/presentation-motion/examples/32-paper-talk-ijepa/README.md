# 32 · Makale Sunumu Örneği – I-JEPA

Bir akademik makaleyi (Assran vd., 2023, *Self-Supervised Learning from Images with a Joint-Embedding Predictive Architecture*, arXiv:2301.08243) anlatan ~2 dakikalık animasyonlu sunum.

| Çıktı | Değer |
|---|---|
| Video | `out/32-paper-talk-ijepa-pptx.mp4` |
| Süre / boyut | 118,5 sn · 4,9 MB · 1920×1080 · H.264 · 30 fps |
| Tema | `academic` |

## Çalıştırma
```bash
cd presentation-motion/examples/32-paper-talk-ijepa
node ../../capture/compose/compose.mjs demo.yaml            # tümü (~6 dk)
node ../../capture/compose/compose.mjs demo.yaml --only 7,9  # yalnız değişen sahneler
```
Tek sahneyi tarayıcıda önizlemek için `scenes/NN-*.html` dosyasını açın (Space oynat/duraklat, R baştan, ←/→ sar).

## Yapı
| # | Sahne | Dosya / şablon | Makaledeki karşılığı |
|---|---|---|---|
| 0 | Açılış | `title` | — |
| 1 | Problem | `bullets` | Bölüm 1 |
| 2 | Üç mimari (JEA / Üretken / JEPA) | `scenes/02-architectures.html` | Şekil 2 |
| 3 | Çoklu blok maskeleme | `scenes/03-masking.html` | Bölüm 3, Şekil 4 |
| 4 | Yöntem akışı (EMA hedef kodlayıcı) | `scenes/04-pipeline.html` | Şekil 3, Ek A |
| 5 | Kayıp formülü | `scenes/05-loss.html` | Bölüm 3 |
| 6 | Bölüm geçişi | `section-cover` | — |
| 7 | ImageNet nokta grafiği | `scenes/07-imagenet.html` | Tablo 1 |
| 8 | Verimlilik | `kpi-counter-row` | Özet, Bölüm 1, 7 |
| 9 | Aktarım tablosu | `scenes/09-transfer.html` | Tablo 3, 4 |
| 10 | Ablasyon çubukları | `scenes/10-ablations.html` | Tablo 6, 7 |
| 11 | Çıkarımlar ve sınırlılıklar | `bullets` | Bölüm 7–10 |
| 12 | Kaynakça (APA 7) | `scenes/12-references.html` | — |

## Kendi makaleniz için uyarlama
- Hazır şablonların verisi `demo.yaml` içinde; özel sahnelerin verisi her HTML dosyasındaki `<script id="pm-data">` bloğunda. Çoğu değişiklik yalnızca bu JSON'u düzenlemekle yapılır.
- Özel sahneler `scenes/_base` sözleşmesini izler: tek paused GSAP timeline, `PM.start()`, `data-hold`. Değişiklikten sonra `node ../../scripts/lint-determinism.mjs scenes/<dosya>` çalıştırın.
- GSAP CSS değişkenine (`var(--accent)`) geçiş yapamaz; renk tween'lerinde tema rengini sabit değer olarak verin (academic: `#7c2d12`).
- Formül KaTeX yerine HTML ile dizildi, çünkü `vendor/katex/fonts` yok. KaTeX kullanmak için önce fontları ekleyin.

## Doğruluk notları
Bkz. `storyboard.md`. Kısaca: tüm rakamlar PDF tablolarından doğrulandı; diyagramlar yeniden çizildi (görsel kopyalanmadı); künye arXiv sürümüne göre. Makale CVPR 2023'te yayımlandı; yayın künyesi kullanılacaksa sayfa numaraları doğrulanmalı.

## Sunum dosyası (PPTX)
| Dosya | Davranış |
|---|---|
| `out/ijepa-sunum.pptx` | Canlı sunum: her slayt açılınca videosu kendiliğinden oynar, sonraki slayda **tıklayınca** geçilir |
| `out/ijepa-sunum-otomatik.pptx` | Kiosk / kayıt: video bitince kendiliğinden ilerler |

- 13 slayt = 13 sahne; her slayt tam ekran gömülü H.264 video (videodaki tüm animasyonlar korunur).
- Slaytlar arası hızlı **soldurma** geçişi.
- Kapak resmi videonun **ilk karesi**. Video, geçiş ve yükleme yüzünden slayt açıldıktan ~0,6 sn sonra başlar; kapak ilk kare olduğu için bu sırada atlama görünmez.
- Konuşmacı notları her slaytta (Sunucu Görünümü).
- PowerPoint 365’te slayt gösterisi çalıştırılarak ölçüldü: ileri gidildiğinde 13/13 slaytta video 0’dan başlıyor; geri dönülen slaytta da baştan oynuyor.

**Sunum sırasında bilinmesi gereken:** Bir slayttayken **geri (←) tuşuna ilk basış** önceki slayda gitmez, o slaytın video adımını geri alır (PowerPoint’in standart davranışı). Önceki slayda dönmek için **iki kez ← basın** ya da **slayt numarasını yazıp Enter**’a basın.

PDF’e dışa aktarılırsa slaytlarda videonun ilk karesi (çoğunlukla boş başlangıç) görünür; dağıtım için `/presentation-motion:handout` ile son karelerden PDF üretin.

Yeniden üretmek (sahneleri değiştirip render aldıktan sonra):
```bash
npm install --prefix /tmp/pptx-tools pptxgenjs@3.12.0 jszip@3.10.1   # bir kez
NODE_PATH=/tmp/pptx-tools/node_modules node tools/build-pptx.cjs          # tıklamalı
NODE_PATH=/tmp/pptx-tools/node_modules node tools/build-pptx.cjs --auto   # otomatik
```
Konuşmacı notları `tools/build-pptx.cjs` içindeki `NOTES` dizisindedir.

### Tasarım kararları ve sorun giderme
Bu ayarlar PowerPoint 365’te slayt gösterisi çalıştırılıp her slaytta video konumu ölçülerek seçildi.

| Belirti | Neden | Çözüm (betikte uygulandı) |
|---|---|---|
| Slayt açılınca önce **bitmiş sahne** görünüp sonra video başa atlıyor (3. slaytta belirgin) | Video, geçiş + yükleme yüzünden ~1 sn geç başlıyor; kapak resmi son kare olunca bu sürede bitmiş hâl görünüyordu | Kapak resmi **ilk kare** yapıldı; geçiş `fast` yapıldı (gecikme ~1 sn → ~0,6 sn) |
| Video hiç başlamıyor / PowerPoint efekti tanımıyor | Zamanlama XML’i PowerPoint’in ana animasyon dizisi (`mainSeq`) içinde değildi | Zamanlama PowerPoint’in “Başlangıç: Otomatik” için yazdığı yapıyla birebir üretiliyor; PowerPoint’te her slaytta “Medya oynat (83), öncekinden sonra” olarak görünüyor |
| Otomatik sürümde son kare kesiliyor | İlerleme süresi video süresi + 0,5 sn idi, ama video ~0,6 sn geç başlıyor | Pay **1,2 sn**’ye çıkarıldı; ilk 4 slaytta video sonuna kadar oynayıp geçtiği ölçüldü |
| Geri tuşu önceki slayda gitmiyor | PowerPoint’te ilk geri basış, slayttaki son animasyon adımını (burada video) geri alır | Değiştirilemez (standart davranış): **iki kez ←** ya da **slayt no + Enter** |
| OneDrive/Teams’te açılmıyor | ZIP içinde klasör girdileri | Paket klasör girdisi olmadan yazılıyor |

Doğrulama adımları (yeniden üretimden sonra önerilir):
1. `python <pptx skill>/scripts/office/validate.py out/ijepa-sunum.pptx` → “All validations PASSED!”
2. PowerPoint’te **F5** ile gösteriyi başlatıp 3. slaytta videonun boş başlangıçtan çizildiğini kontrol edin.
3. Sunum bilgisayarında da bir kez deneyin: eski PowerPoint sürümleri ve Mac’te video başlangıç gecikmesi farklı olabilir.

Bilinen sınırlar:
- Slayt içerikleri video olduğu için PowerPoint içinde metin/grafik düzenlenemez; değişiklik sahne HTML’inde yapılıp yeniden render alınır.
- PDF dışa aktarımında slaytlar boş başlangıç karesiyle çıkar (yukarıya bakın).
