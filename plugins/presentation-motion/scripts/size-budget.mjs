#!/usr/bin/env node
// Her sahnenin yüklediği yerel JS dosyalarının gzip toplamını ölçer.
// Kullanım: node size-budget.mjs <scenes-klasörü|sahne.html> [--budget 60]
import { readFileSync, statSync, readdirSync, existsSync } from 'node:fs';
import { gzipSync } from 'node:zlib';
import { join, dirname, resolve, extname } from 'node:path';

const args = process.argv.slice(2);
const bi = args.indexOf('--budget');
const budgetKB = bi >= 0 ? Number(args.splice(bi, 2)[1]) : 60;
const target = args[0];
if (!target) { console.log('Kullanım: size-budget.mjs <klasör|dosya> [--budget KB]'); process.exit(1); }

const scenes = statSync(target).isDirectory()
  ? readdirSync(target).filter(f => extname(f) === '.html').map(f => join(target, f))
  : [target];

let fail = false;
for (const scene of scenes) {
  const html = readFileSync(scene, 'utf8');
  const refs = [...html.matchAll(/<script[^>]+src="([^"]+)"/g)].map(m => m[1]).filter(u => !/^https?:/.test(u));
  let total = 0;
  const rows = [];
  for (const r of refs) {
    const p = resolve(dirname(scene), r);
    if (!existsSync(p)) { rows.push(`        ?  ${r} (bulunamadı — scripts/fetch-vendor.mjs çalıştırıldı mı?)`); continue; }
    const g = gzipSync(readFileSync(p), { level: 9 }).length;
    total += g;
    rows.push(`   ${(g / 1024).toFixed(1).padStart(6)} KB  ${r}`);
  }
  const ok = total / 1024 <= budgetKB;
  if (!ok) fail = true;
  console.log(`${ok ? '✓' : '✗'} ${scene}  ${(total / 1024).toFixed(1)} KB gzip (bütçe ${budgetKB} KB)`);
  rows.forEach(r => console.log(r));
}
process.exit(fail ? 1 : 0);
