---
description: Terminal komutlarından VHS tape (veya VHS yoksa terminal şablonu) ile CLI demo segmenti üretir
argument-hint: "<komut1; komut2>" [--speed 2] [--simulate]
---

Girdi: `$ARGUMENTS`

- **VHS kuruluysa** (`vhs --version`): `capture/<ad>.tape` yaz (örnekler: `${CLAUDE_PLUGIN_ROOT}/capture/terminal/*.tape`), `demo.yaml`'a `type: terminal, tape, speed` ekle. Komutlar GERÇEKTEN çalışır; zararsız ve demo ortamında olduklarından emin ol, kullanıcıya onaylat.
- **VHS yoksa veya `--simulate`**: komutları ve beklenen çıktıyı kullanıcıdan/loglardan al, `terminal` şablonuyla `type: html` segmenti üret (çalıştırma yok, deterministik).
- Uzun çıktıları kırp; önemli satırları `cls: ok|warn|err` ile renklendir.
