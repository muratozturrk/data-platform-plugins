# I-JEPA – Makale Sunumu (örnek)

**Kaynak:** Assran, M. vd. (2023). *Self-Supervised Learning from Images with a Joint-Embedding Predictive Architecture.* arXiv:2301.08243v3.
**Ana mesaj:** Görüntüdeki eksik bölgeleri piksel değil temsil uzayında tahmin etmek, el yapımı veri artırma olmadan anlamsal ve verimli temsil öğrenmeyi sağlıyor.
**Kitle:** academic · **Süre hedefi:** ~2,5 dk · **Tema:** academic

| # | Sahne | Mesaj (tek cümle) | Şablon / dosya | Veri kaynağı |
|---|---|---|---|---|
| 0 | Açılış | Makale, yazarlar, kurum | title | — |
| 1 | Problem | Artırma bağımlılığı vs. piksel ayrıntısı | bullets | Bölüm 1 |
| 2 | Arka plan | JEA / Üretken / JEPA farkı: kaybın uzayı | scenes/02-architectures.html | Şekil 2 |
| 3 | Maskeleme | 4 hedef blok + 1 bağlam bloğu | scenes/03-masking.html | Bölüm 3, Şekil 4 |
| 4 | Akış | Bağlam → tahminci → L2 ← hedef kodlayıcı (EMA) | scenes/04-pipeline.html | Şekil 3, Ek A |
| 5 | Kayıp | Ortalama L2, temsil uzayında | scenes/05-loss.html | Bölüm 3, Denklem |
| 6 | Bölüm | Bulgular | section-cover | — |
| 7 | ImageNet | Artırmasız 81,1 ≈ iBOT 81,0 | scenes/07-imagenet.html | Tablo 1 |
| 8 | Verimlilik | <1.200 GPU saat; >2,5× / >10× | kpi-counter-row | Özet, Bölüm 1, 7 |
| 9 | Aktarım | Anlamsal + düşük seviye görevler | scenes/09-transfer.html | Tablo 3, 4 |
| 10 | Ablasyon | Çoklu blok 54,2 vs ≤20,2; temsil 66,9 vs piksel 40,7 | scenes/10-ablations.html | Tablo 6, 7 |
| 11 | Sonuç | Çıkarımlar ve sınırlılıklar | bullets | Bölüm 7–10 |
| 12 | Kaynakça | APA 7 | scenes/12-references.html | — |

## Doğruluk notları
- Tüm sayılar PDF'teki tablolardan (sayfa görüntüsü üzerinden) kontrol edildi.
- Diyagramlar makaledeki şekillerden **yeniden çizildi**; görsel kopyalanmadı.
- Sahne 3'teki yama ızgarası temsilîdir (14 × 14, ViT-B/16 örneği); blok boyutları makaledeki aralıklara uyar.
- Sahne 5'teki EMA formülü makalede açık yazılmaz; standart EMA gösterimi kullanıldı (momentum değeri Ek A'dan).
- Künye arXiv sürümüne göre verildi. Makale CVPR 2023'te yayımlandı; yayın künyesi (sayfa no.) kullanılacaksa doğrulanmalı.
- Sahne 7 nokta grafiğidir; eksen 76'dan başlar (dipnotta belirtildi). Sahne 10 çubuk grafiği sıfırdan başlar.
