#!/usr/bin/env node
// Sahne dosyalarında videoya dökmeyi bozan (deterministik olmayan) kalıpları bulur.
// Kullanım:  node lint-determinism.mjs <dosya|klasör>...
//            node lint-determinism.mjs --hook   (Claude Code PostToolUse; stdin JSON)
import { readFileSync, statSync, readdirSync } from 'node:fs';
import { join, extname } from 'node:path';

const RULES = [
  { re: /\bDate\.now\s*\(|\bnew Date\s*\(\s*\)/, msg: "Gerçek saat kullanımı: zamanı timeline'dan alın (PM.time())." },
  { re: /\bMath\.random\s*\(/, msg: "Math.random: sabit seed'li PM.random() kullanın." },
  { re: /\bset(Timeout|Interval)\s*\(/, msg: "setTimeout/setInterval: animasyonu timeline'a ekleyin (tl.call / tl.to)." },
  { re: /\brequestAnimationFrame\s*\(/, msg: 'requestAnimationFrame döngüsü seek edilemez; tl onUpdate kullanın.' },
  { re: /(^|[;{\s"'])transition\s*:/, msg: 'CSS transition seek edilemez; GSAP tween kullanın.' },
  { re: /\btl\.play\s*\(|\.restart\s*\(/, msg: 'Sahne timeline\'ı paused kalmalı; oynatmayı runtime yönetir.' },
  { re: /https?:\/\/(cdn\.|unpkg\.|fonts\.googleapis)/, msg: 'Uzak kaynak: çevrimdışı çalışma için vendor/ kullanın.' },
];
const EXT = new Set(['.html', '.js', '.mjs', '.css']);
const SKIP = /[\\/](vendor|node_modules|out|capture)[\\/]|timeline-runtime\.js$/;

function files(p) {
  const s = statSync(p);
  if (s.isFile()) return EXT.has(extname(p)) ? [p] : [];
  return readdirSync(p).flatMap(f => files(join(p, f)));
}

function lint(file) {
  if (SKIP.test(file)) return [];
  const out = [];
  readFileSync(file, 'utf8').split(/\r?\n/).forEach((line, i) => {
    if (line.includes('pm-lint-ignore')) return;
    for (const r of RULES) if (r.re.test(line)) out.push(`${file}:${i + 1}  ${r.msg}`);
  });
  return out;
}

if (process.argv.includes('--hook')) {
  let raw = '';
  for await (const c of process.stdin) raw += c;
  let fp = '';
  try { fp = JSON.parse(raw || '{}')?.tool_input?.file_path ?? ''; } catch { process.exit(0); }
  const norm = fp.replace(/\\/g, '/');
  // Yalnızca sahne ve şablon dosyalarını denetle
  if (!/\/(scenes|templates)\/.*\.(html|js|css)$/.test(norm)) process.exit(0);
  const issues = lint(fp);
  if (issues.length) {
    console.error('presentation-motion determinism lint:\n' + issues.join('\n') +
      '\n(Bilinçli kullanım ise satıra "pm-lint-ignore" yorumu ekleyin.)');
    process.exit(2); // exit 2: stderr Claude'a geri bildirilir
  }
  process.exit(0);
}

const targets = process.argv.slice(2);
if (!targets.length) { console.log('Kullanım: lint-determinism.mjs <dosya|klasör>...'); process.exit(1); }
const all = targets.flatMap(files).flatMap(lint);
console.log(all.length ? all.join('\n') : '✓ Determinism lint temiz');
process.exit(all.length ? 1 : 0);
