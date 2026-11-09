#!/usr/bin/env node
// Çevrimdışı çalışma için kütüphaneleri SABİT sürümlerle vendor/ klasörüne indirir.
// Sürüm yükseltmek için aşağıdaki tabloyu güncelleyin ve tekrar çalıştırın.
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..', 'vendor');
const CDN = 'https://cdn.jsdelivr.net/npm/';
const LIBS = {
  'gsap/gsap.min.js':                      'gsap@3.13.0/dist/gsap.min.js',
  'gsap/SplitText.min.js':                 'gsap@3.13.0/dist/SplitText.min.js',
  'gsap/DrawSVGPlugin.min.js':             'gsap@3.13.0/dist/DrawSVGPlugin.min.js',
  'gsap/MorphSVGPlugin.min.js':            'gsap@3.13.0/dist/MorphSVGPlugin.min.js',
  'gsap/MotionPathPlugin.min.js':          'gsap@3.13.0/dist/MotionPathPlugin.min.js',
  'gsap/TextPlugin.min.js':                'gsap@3.13.0/dist/TextPlugin.min.js',
  'katex/katex.min.js':                    'katex@0.16.11/dist/katex.min.js',
  'katex/katex.min.css':                   'katex@0.16.11/dist/katex.min.css',
  'rough-notation/rough-notation.iife.js': 'rough-notation@0.5.1/lib/rough-notation.iife.js',
  'd3/d3.min.js':                          'd3@7.9.0/dist/d3.min.js',
};

let failed = 0;
for (const [dest, src] of Object.entries(LIBS)) {
  const res = await fetch(CDN + src);
  if (!res.ok) { console.log(`✗ ${src} (${res.status})`); failed++; continue; }
  const out = join(root, dest);
  mkdirSync(dirname(out), { recursive: true });
  writeFileSync(out, Buffer.from(await res.arrayBuffer()));
  console.log(`✓ ${dest}`);
}
console.log('\nNot: KaTeX fontları için katex@0.16.11/dist/fonts/ içeriğini vendor/katex/fonts/ altına kopyalayın.');
process.exit(failed ? 1 : 0);
