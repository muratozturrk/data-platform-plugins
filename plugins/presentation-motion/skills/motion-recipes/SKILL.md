---
name: motion-recipes
description: Sunum animasyonu tarif kütüphanesi - kinetik tipografi, maske açılışı, sayaç, SVG çizgi çizimi, morph, path üzerinde hareket, stagger, FLIP/magic move, kamera zoom, parallax, Codrops efekt ailelerinin videoya uygun (seek edilebilir) GSAP karşılıkları. Sahneye hareket eklerken veya bir efekti adlandırıp uygularken kullan.
---

# Hareket tarifleri

Tüm tarifler `const tl = PM.timeline()` üzerinde çalışır ve **seek edilebilir**. Zaman `t` ile gösterilen yerler timeline konum parametresidir.

## Metin
| Tarif | Kod |
|---|---|
| `split-text-rise` | `const s = SplitText.create(el,{type:'lines,words,chars',mask:'lines'}); tl.from(s.chars,{yPercent:110,stagger:.022,duration:.9,ease:'expo.out'},t)` |
| `word-mask-rise` | `SplitText.create(el,{type:'words',mask:'words'})` → `from(words,{yPercent:100,stagger:.05})` |
| `counter` | `const o={v:0}; tl.to(o,{v:N,duration:1.6,ease:'power2.out',onUpdate:()=>el.textContent=PM.fmt(o.v,d)},t)` |
| `typewriter` | proxy `{n:0}` → `to(proxy,{n:text.length,ease:'none',duration:len/18,onUpdate:()=>el.textContent=text.slice(0,Math.round(proxy.n))})` |
| `highlight-underline` | SVG path: `gsap.set(p,{strokeDasharray:L,strokeDashoffset:L}); tl.to(p,{strokeDashoffset:0,duration:.7})` |
| `dim-others` | `tl.to(others,{opacity:.35,duration:.6})` + vurgu |

## Şekil / SVG
| Tarif | Kod |
|---|---|
| `draw-path` | `getTotalLength()` → dasharray/dashoffset tween (DrawSVGPlugin opsiyonel) |
| `morph` | `MorphSVGPlugin`: `tl.to('#a',{morphSVG:'#b',duration:1})` (açık kaynak alternatif: flubber) |
| `follow-path` | `MotionPathPlugin`: `tl.to(dot,{motionPath:{path:'#p',align:'#p',alignOrigin:[.5,.5]},ease:'none'})` |
| `flow-particles` | proxy `{t:0}` `ease:'none'`; onUpdate'te `path.getPointAtLength(((t*speed+i/n)%1)*L)` (bkz. architecture-layers) |
| `circle-reveal` (Codrops) | `tl.to(panel,{clipPath:'circle(150% at 50% 50%)',duration:1.2,ease:'power3.inOut'})` başlangıç `circle(0%)` |
| `mask-wipe` | `clipPath: 'inset(0 100% 0 0)'` → `'inset(0 0% 0 0)'` |

## Yerleşim / sahne
| Tarif | Kod |
|---|---|
| `stagger-cards` | `tl.from(cards,{y:60,opacity:0,stagger:.12,duration:.8})` |
| `flip-move` | GSAP Flip: `const st=Flip.getState(items); /*DOM değiştir*/ tl.add(Flip.from(st,{duration:.8,ease:'power2.inOut'}))` |
| `camera-zoom` | sahne içi `.world` kapsayıcıya `tl.to(world,{scale:1.8,x:..,y:..,duration:1.2,ease:'power2.inOut'})` |
| `parallax-layers` | katmanlara farklı `x` mesafeleri, aynı süre |
| `ken-burns` | `tl.fromTo(img,{scale:1},{scale:1.08,duration:6,ease:'none'})` |
| `blink` (imleç vb.) | `opacity = Math.floor(PM.time()*2)%2 ? .2 : 1` (onUpdate içinde) |

## Codrops efekt aileleri → video karşılığı
| Codrops efekti | Video karşılığı |
|---|---|
| ScrollTrigger scrub | `tl.to(proxy,{p:1,ease:'none'})` + onUpdate'te ilerlemeyi uygula |
| Fare takipli lens/parallax | bezier yol üzerinde sanal imleç (`follow-path`) |
| Shader görsel geçişi | Three.js/OGL uniform `uProgress`'i timeline'dan sür; render'ı onUpdate'te çağır |
| Fizik ile dağılan harfler | `PM.random()` ile sabit başlangıç hızları, `t` ile kapalı form konum (x=v·t, y=v·t+½g·t²) |
| Sonsuz marquee | `x = -((PM.time()*speed) % width)` |

## Zamanlama varsayılanları
Giriş 0.7–1.0 sn `power3.out`/`expo.out` · çıkış `power2.in` · stagger harf 0.02–0.04, öğe 0.08–0.15 · okuma payı `data-hold`.
