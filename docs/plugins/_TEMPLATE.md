<!--
Plugin kullanım dokümanı şablonu.

Kurallar:
- Dosya: docs/plugins/<plugin-adı>.md (plugin klasör adıyla birebir aynı).
- docs/ build script'leri tarafından ezilmez. plugins/<plugin>/README.md ve kök README.md
  build-platform-plugins.ps1/.sh tarafından yeniden yazılabildiği için doküman oraya konmaz.
- Yeni doküman eklenince docs/plugins/README.md içindeki tabloya bir satır eklenir.
- İçerik plugin'in gerçek dosyalarından çıkarılır (plugin.json, agents/, commands/, skills/*/SKILL.md,
  scripts/). Uydurma özellik yazılmaz; komut ve isimler dosyalardaki gibi yazılır.
- Dil: Türkçe anlatım; skill/agent/komut adları, kod ve hata metinleri orijinal haliyle.
- Bölüm sırası aşağıdaki gibidir. Plugin'de karşılığı olmayan bölüm silinir (ör. scripts yoksa
  "Araçlar" bölümü).
-->

# <plugin-adı>

> <plugin.json description'ının Türkçe tek cümlelik özeti>

| | |
| --- | --- |
| Sürüm | <plugin.json version> |
| Kategori | <marketplace.json category> |
| Kaynak | `plugins/<plugin-adı>/` |
| Kaynak materyal | <ör. iç kütüphane / wshobson/agents upstream> |

## 1. Ne işe yarar?

2-4 paragraf: hangi problemi çözer, kimin için, diğer plugin'lerden farkı.

## 2. Kurulum

```text
/plugin marketplace add muratozturrk/data-platform-plugins
/plugin install <plugin-adı>@data-platform-plugins
```

Yerel geliştirme: `claude --plugin-dir ./plugins/<plugin-adı>`
Gereksinimler: <Python sürümü, opsiyonel bağımlılıklar>

## 3. İçerik

### Skill'ler
| Skill | Ne zaman devreye girer | İçerdiği konular |

### Agent'lar
| Agent | Model | Ne yapar | Ne zaman kullanılır |

### Komutlar
| Komut | Argüman | Ne yapar |

### Araçlar (scripts/)
| Dosya | Ne yapar | Spark/ek bağımlılık gerekir mi |

## 4. Hızlı başlangıç

En sık kullanım için 3-5 adımlık yol.

## 5. Kullanım senaryoları

Her senaryo: **Durum → Ne yazarsınız (örnek prompt) → Ne olur → Çıktı**.

## 6. Dosya yapısı

Plugin klasör ağacı ve hangi dosyaya ne zaman bakılır.

## 7. İpuçları ve sınırlamalar

Bilinen kısıtlar, doğrulanmamış kısımlar, sık hatalar.

## 8. Bakım

Plugin nasıl güncellenir, testler nasıl çalıştırılır, ilgili dokümanlar.
