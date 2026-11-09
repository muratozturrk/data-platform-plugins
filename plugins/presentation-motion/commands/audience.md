---
description: Mevcut storyboard ve demo'yu başka bir hedef kitleye uyarlar (audience-adapter agent)
argument-hint: <technical | executive | government | academic>
---

**audience-adapter** agent'ını çalıştır. Hedef kitle: `$ARGUMENTS`

- Çıktılar: `storyboard.<kitle>.md`, `demo.<kitle>.yaml` (asıl dosyalar korunur).
- Rakamlar değişmez; dil, sıra, detay seviyesi, hareket dozu ve tema değişir.
- Bitince farkları tablo halinde özetle ve render komutunu ver:
```bash
node "${CLAUDE_PLUGIN_ROOT}/capture/compose/compose.mjs" demo.<kitle>.yaml
```
