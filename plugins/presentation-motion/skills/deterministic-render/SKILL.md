---
name: deterministic-render
description: HTML animasyonunu kare atlamadan videoya dökmenin kuralları - sahne sözleşmesi (PM runtime, paused timeline, pm-data), yasak API'ler, scroll/fare/fizik efektlerini zamana bağlama, render-scene.mjs'in çalışma mantığı ve hata teşhisi. Sahne yazarken, Codrops tarzı efekt uyarlarken ve render sorunlarında kullan.
---

# Deterministik render

## İlke
Video, gerçek zamanlı kayıt değil **kare kare hesaplama**dır: her kare için `PM.seek(frame/fps)` → ekran görüntüsü → ffmpeg. Bu yüzden sahnedeki her görsel durum **yalnızca timeline zamanının fonksiyonu** olmalı.

## Sahne sözleşmesi
```html
<html data-theme="corporate-dark">
<link rel="stylesheet" href="_base/tokens.css"><link rel="stylesheet" href="_base/stage.css">
<body data-hold="2">                      <!-- animasyon sonrası durağan süre -->
<div class="pm-stage"> ... 1920×1080 ... </div>
<script type="application/json" id="pm-data">{ ... }</script>
<script src="../vendor/gsap/gsap.min.js"></script>
<script src="_base/timeline-runtime.js"></script>
<script>
  const D = PM.data;
  const tl = PM.timeline();   // paused
  // ...
  PM.start();                 // fontları bekler, ready=true; önizlemede oynatır
</script>
```
Runtime API: `PM.data`, `PM.timeline()`, `PM.seek(t)`, `PM.duration()`, `PM.time()`, `PM.random()`, `PM.fmt(v,d)`, `PM.render`.

## Yasaklar (lint + hook yakalar)
| Yasak | Yerine |
|---|---|
| `Date.now()`, `new Date()` | `PM.time()` |
| `Math.random()` | `PM.random()` (seed'li) |
| `setTimeout/setInterval` | `tl.call(fn, null, t)` / `tl.to` |
| `requestAnimationFrame` döngüsü | `tl` `onUpdate` |
| CSS `transition`, `@keyframes` | GSAP tween |
| `tl.play()` sahne içinde | runtime yönetir |
| CDN / Google Fonts | `vendor/`, yerel font |
Bilinçli istisna: satıra `// pm-lint-ignore`.

## Sürekli hareketler
Proxy nesneyi `ease:'none'` ile tween'le, görsel durumu proxy'den hesapla (partikül, marquee, imleç yanıp sönmesi). Geri sarmada (seek geriye) durum yeniden hesaplanabilmeli: **artımlı (incremental) durum tutma**, her onUpdate'te baştan hesapla.

## Fizik / simülasyon
Kapalı form (konum = f(t)) tercih et. Mümkün değilse sabit adımlı simülasyonu t=0'dan hedef kareye kadar yeniden koş (kısa sahnelerde kabul edilebilir) ya da kareleri önceden hesapla.

## WebGL (Three.js/OGL)
`preserveDrawingBuffer: true`, render'ı onUpdate'te elle çağır, `uTime` uniform'unu `PM.time()`'dan ver. Render süresi artar; yalnızca açılış/geçiş sahnelerinde kullan.

## Teşhis
| Belirti | Neden |
|---|---|
| `PM.ready zaman aşımı` | JS hatası, bozuk `pm-data` JSON, global değişken çakışması (`top`, `name`, `status` gibi window özelliklerini kullanma!) |
| Videoda takılma/atlama | seek edilemeyen animasyon |
| İlk karede font değişimi | font yüklenmeden ölçüm; `PM.start()` fontları bekler, SplitText'i fontlar hazır olduktan sonra çağır |
| Önizleme ile video farklı | gerçek zamana bağlı kod |
