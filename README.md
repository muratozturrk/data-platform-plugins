# data-platform-plugins

Data engineering platformu inşası için derlenmiş Claude Code plugin marketplace'i.
Plugin'lerin çoğu [wshobson/agents](https://github.com/wshobson/agents) (MIT) reposundan alınmıştır;
`data-governance` bu repoda sıfırdan geliştirilmektedir.

## Kurulum

```
/plugin marketplace add muratozturrk/data-platform-plugins
/plugin install data-engineering@data-platform-plugins
/plugin install data-governance@data-platform-plugins
```

Yerel geliştirme için: `claude --plugin-dir .\plugins\data-governance`

## Plugin yapısı

```
plugins/<plugin>/
├── .claude-plugin/plugin.json
├── agents/      # subagent tanımları (*.md, YAML frontmatter)
├── commands/    # slash komutları (*.md)
├── skills/      # <skill>/SKILL.md (+ references/)
└── examples/    # gerçek proje örnekleri (kod, config)
```

## Güncelleme

Upstream'den yeniden çekmek için `.\build-platform-plugins.ps1 -Target .` çalıştırın.
`data-governance` ve `examples/` içerikleri script tarafından ezilmez.