---
name: academic-blocks
description: Akademik ve veri sahnesi blokları - KaTeX formül (parça parça açılma), D3 grafik animasyonları (eksen, bar, çizgi, dağılım), Shiki Magic Move ile kod geçişleri, el çizimi vurgular, atıf ve kaynak gösterimi. Formül, grafik, kod veya akademik içerik sahnesi yazarken kullan.
---

# Akademik bloklar

## KaTeX formül
```html
<link rel="stylesheet" href="../vendor/katex/katex.min.css">
<script src="../vendor/katex/katex.min.js"></script>
<div id="eq"></div>
<script>
katex.render(String.raw`\hat{y} = \sigma\left(\htmlClass{t-w}{W}x + \htmlClass{t-b}{b}\right)`, eq, { trust: true, strict: false, displayMode: true });
tl.from('#eq .katex', {opacity:0, y:20})
  .from('.t-w', {color:'var(--accent)', scale:1.3, duration:.5}, '+=.4');   // terimleri ayrı vurgula
</script>
```
- `\htmlClass{}` ile terimlere sınıf ver; parçaları ayrı ayrı animasyonla (Manim'in TransformMatchingTex mantığı).
- Fontlar: `vendor/katex/fonts/` (fetch-vendor notuna bak).
- Formül numarası sağda `(1)`; metinde "Denklem (1)".

## D3 grafikleri (deterministik)
D3'ü **yalnızca yerleşim/ölçek** için kullan; animasyonu GSAP yapar (D3 transition gerçek zamanlıdır).
```js
const x = d3.scaleBand(D.labels, [0, 1400]).padding(.25);
const y = d3.scaleLinear([0, d3.max(D.values)], [0, 600]).nice();
// barları SVG rect olarak çiz, sonra:
tl.from('.axis', {opacity:0}).from('.bar', {scaleY:0, transformOrigin:'bottom', stagger:.08, duration:.7});
```
- Bar ekseni 0'dan başlar. Çizgi: path draw + son değer etiketi. Dağılım: noktalar stagger + regresyon çizgisi draw.
- Etiketler `PM.fmt()`. Renk: `--accent` ana seri, `--muted` karşılaştırma.

## Kod (Shiki Magic Move)
Kodu build zamanında Shiki ile token'lara çevirip HTML olarak göm; adımlar arası geçişte aynı token'ları FLIP ile taşı. Basit durumda: satır vurgusu (diğer satırlar `opacity:.35`) + yeni satırların maske ile açılması.

## Vurgu ve atıf
- El çizimi vurgu: SVG path + `draw-path` tarifi (rough-notation'ın kendi animasyonu seek edilemez; yalnızca şekil üretimi için kullan).
- Her veri sahnesinde `.pm-source`: "Kaynak: Yazar (Yıl), tablo 3; n = 1.240".
- Son sahne: APA 7 kaynakça (en fazla 6 kaynak, gerisi QR/bağlantı).
