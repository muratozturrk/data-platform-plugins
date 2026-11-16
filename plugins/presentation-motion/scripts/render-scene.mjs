#!/usr/bin/env node
/*
 * Bir veya daha fazla sahneyi kare kare (deterministik) render edip videoya çevirir.
 * Her kare için PM.seek(t) → ekran görüntüsü → ffmpeg (image2pipe).
 *
 * Kullanım:
 *   node render-scene.mjs <sahne.html | scenes-klasörü> [-o out/deck.mp4] [--profile pptx|web|4k|alpha] [--fps 30]
 *   node render-scene.mjs <sahne.html | scenes-klasörü> --steps [-o out/steps]
 *     → her sahneyi step-* label'larından parçalara böler: out/steps/<sahne>-01.mp4, -02.mp4 ...
 *       (tıklamalı PPTX için: scripts/build-pptx.mjs)
 *
 * Profiller:
 *   pptx  → 1920×1080, H.264 CRF 18, yuv420p, +faststart (PowerPoint/Keynote uyumlu)
 *   web   → 1920×1080, AV1 (SVT-AV1) CRF 30 — en küçük dosya
 *   4k    → 3840×2160 (deviceScaleFactor 2), H.264 CRF 18
 *   alpha → WebM VP9 + şeffaflık (sahnede background: transparent olmalı)
 */
import { chromium } from 'playwright';
import { spawn } from 'node:child_process';
import { statSync, readdirSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { join, resolve, dirname, extname, basename } from 'node:path';
import { pathToFileURL } from 'node:url';
import { tmpdir } from 'node:os';

const PROFILES = {
  pptx:  { scale: 1, args: ['-c:v', 'libx264', '-preset', 'slow', '-crf', '18', '-pix_fmt', 'yuv420p', '-movflags', '+faststart'], ext: '.mp4' },
  web:   { scale: 1, args: ['-c:v', 'libsvtav1', '-preset', '6', '-crf', '30', '-pix_fmt', 'yuv420p'], ext: '.mp4' },
  '4k':  { scale: 2, args: ['-c:v', 'libx264', '-preset', 'slow', '-crf', '18', '-pix_fmt', 'yuv420p', '-movflags', '+faststart'], ext: '.mp4' },
  alpha: { scale: 1, args: ['-c:v', 'libvpx-vp9', '-pix_fmt', 'yuva420p', '-b:v', '0', '-crf', '30'], ext: '.webm', omitBackground: true },
};

const a = process.argv.slice(2);
const flag = (k, d) => { const i = a.indexOf(k); return i >= 0 ? a.splice(i, 2)[1] : d; };
const profileName = flag('--profile', 'pptx');
const fps = Number(flag('--fps', 30));
let outFile = flag('-o', null);
const si = a.indexOf('--steps');
const STEPS = si >= 0 && !!a.splice(si, 1);
const input = a[0];
if (!input) { console.log('Kullanım: render-scene.mjs <sahne.html|klasör> [-o çıktı] [--profile pptx|web|4k|alpha] [--fps 30]'); process.exit(1); }
const profile = PROFILES[profileName];
if (!profile) { console.error(`Bilinmeyen profil: ${profileName}`); process.exit(1); }

const scenes = statSync(input).isDirectory()
  ? readdirSync(input).filter(f => extname(f) === '.html').sort().map(f => resolve(input, f))
  : [resolve(input)];
if (STEPS) {
  // --steps: -o bir KLASÖR; her sahne step-* label'larından bölünür → <sahne>-01.mp4, <sahne>-02.mp4 ...
  outFile = resolve(outFile || join(dirname(scenes[0]), '..', 'out', 'steps'));
  mkdirSync(outFile, { recursive: true });
} else {
  outFile = resolve(outFile || join(dirname(scenes[0]), '..', 'out', `${basename(input, '.html')}-${profileName}${profile.ext}`));
  mkdirSync(dirname(outFile), { recursive: true });
}

function encode(file) {
  const ff = spawn('ffmpeg', ['-y', '-loglevel', 'error', '-f', 'image2pipe', '-framerate', String(fps), '-c:v', 'png', '-i', '-', ...profile.args, file],
    { stdio: ['pipe', 'inherit', 'inherit'] });
  const done = new Promise((res, rej) => ff.on('close', c => (c === 0 ? res() : rej(new Error(`ffmpeg çıkış kodu ${c}`)))));
  return { ff, done };
}

const browser = await chromium.launch();
const ctx = await browser.newContext({ viewport: { width: 1920, height: 1080 }, deviceScaleFactor: profile.scale });
const page = await ctx.newPage();
page.on('console', m => { if (m.type() === 'error') console.error('  [sahne]', m.text()); });

const parts = [];
const tmp = join(tmpdir(), `pm-render-${process.pid}`);
mkdirSync(tmp, { recursive: true });

for (const [i, scene] of scenes.entries()) {
  await page.goto(`${pathToFileURL(scene).href}?render=1&fps=${fps}`);
  await page.waitForFunction(() => window.PM && window.PM.ready, null, { timeout: 30000 });
  const duration = await page.evaluate(() => PM.duration());
  const frames = Math.max(1, Math.round(duration * fps));
  // Kare aralıkları: normalde tek parça; --steps'te her durak bir parça sınırı.
  // Ara parçalar sınır karesini de içerir → parçanın son karesi = sonraki parçanın ilk karesi (PPTX'te dikişsiz).
  const cuts = STEPS ? [...new Set((await page.evaluate(() => PM.steps())).map(t => Math.round(t * fps)))].filter(f => f > 0 && f < frames - 1) : [];
  const bounds = [0, ...cuts, frames - 1];
  const t0 = performance.now();
  for (let k = 0; k < bounds.length - 1; k++) {
    const part = STEPS ? join(outFile, `${basename(scene, '.html')}-${String(k + 1).padStart(2, '0')}${profile.ext}`)
      : scenes.length === 1 ? outFile : join(tmp, `${String(i).padStart(3, '0')}${profile.ext}`);
    const { ff, done } = encode(part);
    for (let f = bounds[k]; f <= bounds[k + 1]; f++) {
      await page.evaluate(t => PM.seek(t), f / fps);
      const png = await page.screenshot({ type: 'png', omitBackground: !!profile.omitBackground, clip: { x: 0, y: 0, width: 1920, height: 1080 } });
      if (!ff.stdin.write(png)) await new Promise(r => ff.stdin.once('drain', r));
    }
    ff.stdin.end();
    await done;
    parts.push(part);
  }
  const info = STEPS ? `, ${bounds.length - 1} parça` : '';
  console.log(`✓ ${basename(scene)}  ${duration.toFixed(2)} sn, ${frames} kare${info} (${((performance.now() - t0) / 1000).toFixed(1)} sn'de)`);
}
await browser.close();

if (scenes.length > 1 && !STEPS) {
  const list = join(tmp, 'list.txt');
  writeFileSync(list, parts.map(p => `file '${p.replace(/\\/g, '/')}'`).join('\n'));
  await new Promise((res, rej) => spawn('ffmpeg', ['-y', '-loglevel', 'error', '-f', 'concat', '-safe', '0', '-i', list, '-c', 'copy', outFile], { stdio: 'inherit' })
    .on('close', c => (c === 0 ? res() : rej(new Error('concat başarısız')))));
}
rmSync(tmp, { recursive: true, force: true });
console.log(`→ ${outFile}`);
