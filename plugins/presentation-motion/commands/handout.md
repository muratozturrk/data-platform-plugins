---
description: Yöneticiler için tek sayfalık özet PDF ve sahnelerin son karelerinden PDF yedek üretir
argument-hint: [--onepager] [--slides]
---

Çıktılar (`$ARGUMENTS` boşsa ikisi de):

1. **Sahne PDF'i** (projeksiyon arızası ve e-posta için yedek): her sahnenin SON karesini PNG olarak al (`?render=1` + `PM.seek(PM.duration())`), sonra PDF'te birleştir:
```bash
ffmpeg -i out/<deck>-pptx.mp4 -vf "select='gt(scene,0.4)'" -vsync vfr out/_frames/%03d.png   # alternatif: sahne başına son kare
```
   Tercih edilen: Playwright ile her `scenes/*.html` ve `_gen-*.html` için son kare → `page.pdf()` veya PNG → PDF.
2. **Tek sayfa özet** (`out/onepager.html` → PDF, A4): başlık, ana mesaj, 3 kanıt (sayılarla), risk, istenen karar, iletişim. `tokens.css` ile aynı tema; yazdırma için açık tema.
3. Gizlilik: önce **compliance-guard** çalıştır; KALDI ise PDF üretme.
