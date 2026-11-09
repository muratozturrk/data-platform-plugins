#!/usr/bin/env node
/*
 * demo.yaml → segmentleri üret → normalize et → tek video.
 *
 * Kullanım:  node compose.mjs <deck>/demo.yaml [-o <deck>/out/demo.mp4] [--only 2,3]
 *
 * Segment tipleri:
 *   html     → { template, data?, source?, scene? }   şablon + veri ya da hazır sahne (kare kare render)
 *   browser  → { app, url, auth, steps, mask, ... }    Playwright gerçek uygulama kaydı (recorder.mjs)
 *   terminal → { tape, speed? }                        VHS ile terminal kaydı (vhs kurulu olmalı)
 *   video    → { file, speed? }                        hazır video dosyası
 * Ortak alanlar: speed (hızlandırma, ör. 2.0), trim: [başlangıç, bitiş] (sn)
 */
import { readFileSync, writeFileSync, mkdirSync, existsSync, cpSync, statSync } from 'node:fs';
import { join, dirname, resolve, basename, isAbsolute } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { spawn, execFileSync } from 'node:child_process';
import YAML from 'yaml';
import { record } from '../playwright/recorder.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const run = (cmd, args, opts = {}) => new Promise((res, rej) =>
  spawn(cmd, args, { stdio: 'inherit', ...opts }).on('close', c => (c === 0 ? res() : rej(new Error(`${cmd} çıkış kodu ${c}`)))));

const a = process.argv.slice(2);
const flag = k => { const i = a.indexOf(k); return i >= 0 ? a.splice(i, 2)[1] : undefined; };
const outArg = flag('-o');
const only = flag('--only')?.split(',').map(Number);
const demoFile = resolve(a[0] || 'demo.yaml');
const deck = dirname(demoFile);
const demo = YAML.parse(readFileSync(demoFile, 'utf8'));
const P = { profile: 'pptx', resolution: '1920x1080', fps: 30, ...(demo.output || {}) };
const [W, H] = P.resolution.split('x').map(Number);
const segDir = join(deck, 'out', '_segments');
mkdirSync(segDir, { recursive: true });
const rel = p => (isAbsolute(p) ? p : join(deck, p));

/** Sahnelerin çevrimdışı çalışması için _base ve vendor'ı deste kopyala */
function ensureAssets() {
  const base = join(deck, 'scenes', '_base');
  if (!existsSync(base)) cpSync(join(ROOT, 'templates', '_base'), base, { recursive: true });
  const vendor = join(deck, 'vendor');
  if (!existsSync(vendor)) cpSync(join(ROOT, 'vendor'), vendor, { recursive: true });
}

/** Şablonu veriyle doldurup deck/scenes/_gen-NN-<şablon>.html olarak yazar */
function materialize(seg, i) {
  ensureAssets();
  if (seg.scene) return rel(seg.scene);
  const tpl = join(ROOT, 'templates', `${seg.template}.html`);
  if (!existsSync(tpl)) throw new Error(`Şablon yok: ${seg.template}`);
  let data = seg.data || {};
  if (seg.source) data = { ...YAML.parse(readFileSync(rel(seg.source), 'utf8')), ...data };
  let html = readFileSync(tpl, 'utf8');
  if (Object.keys(data).length) {
    html = html.replace(/(<script type="application\/json" id="pm-data">)[\s\S]*?(<\/script>)/, `$1\n${JSON.stringify(data, null, 2)}\n$2`);
  }
  if (demo.theme) html = html.replace(/data-theme="[^"]*"/, `data-theme="${demo.theme}"`);
  const out = join(deck, 'scenes', `_gen-${String(i).padStart(2, '0')}-${seg.template}.html`);
  writeFileSync(out, html);
  return out;
}

/** Her segmenti aynı çözünürlük/fps/pix_fmt'ye getirir (concat için şart) */
async function normalize(input, out, seg) {
  const vf = [];
  if (seg.trim) vf.push(`trim=${seg.trim[0]}:${seg.trim[1]},setpts=PTS-STARTPTS`);
  if (seg.speed && seg.speed !== 1) vf.push(`setpts=PTS/${seg.speed}`);
  vf.push(`scale=${W}:${H}:force_original_aspect_ratio=decrease:flags=lanczos`, `pad=${W}:${H}:(ow-iw)/2:(oh-ih)/2`, `fps=${P.fps}`, 'format=yuv420p');
  await run('ffmpeg', ['-y', '-loglevel', 'error', '-i', input, '-vf', vf.join(','), '-an',
    '-c:v', 'libx264', '-preset', 'medium', '-crf', '17', '-pix_fmt', 'yuv420p', out]);
}

const parts = [];
for (const [i, seg] of demo.segments.entries()) {
  const raw = join(segDir, `${String(i).padStart(2, '0')}-raw.mp4`);
  const norm = join(segDir, `${String(i).padStart(2, '0')}.mp4`);
  if (only && !only.includes(i)) { if (existsSync(norm)) parts.push(norm); continue; }
  console.log(`▶ [${i}] ${seg.type}${seg.template ? ' · ' + seg.template : ''}${seg.app ? ' · ' + seg.app : ''}`);
  switch (seg.type) {
    case 'html': {
      const scene = materialize(seg, i);
      await run('node', [join(ROOT, 'scripts', 'render-scene.mjs'), scene, '-o', raw, '--profile', 'pptx', '--fps', String(P.fps)]);
      break;
    }
    case 'browser': {
      // Deste içindeki yerel dosya (ör. mock-app/dashboard.html) → file:// URL
      let url = seg.url;
      if (url && !/^(https?|file):/.test(url) && !url.startsWith('/') && existsSync(rel(url))) url = pathToFileURL(rel(url)).href;
      const flow = { ...seg, url, baseUrl: seg.baseUrl || demo.baseUrl, auth: seg.auth ? rel(seg.auth) : undefined, fps: P.fps };
      await record(flow, raw);
      break;
    }
    case 'terminal': {
      // VHS tape'ini çıktı yolu ve çözünürlükle sarmalayıp çalıştırır
      const body = readFileSync(rel(seg.tape), 'utf8').split(/\r?\n/).filter(l => !/^\s*(Output|Set (Width|Height))\b/.test(l));
      const tape = join(segDir, `${String(i).padStart(2, '0')}.tape`);
      writeFileSync(tape, [`Output "${raw.replace(/\\/g, '/')}"`, `Set Width ${W}`, `Set Height ${H}`, ...body].join('\n'));
      try { await run('vhs', [tape], { cwd: deck }); }
      catch { throw new Error('VHS çalıştırılamadı. Kurulum: https://github.com/charmbracelet/vhs (ya da html/terminal şablonunu kullanın)'); }
      break;
    }
    case 'video': {
      await normalize(rel(seg.file), norm, seg); parts.push(norm); continue;
    }
    default: throw new Error(`Bilinmeyen segment tipi: ${seg.type}`);
  }
  await normalize(raw, norm, seg);
  parts.push(norm);
}

// --- Birleştir ---
const list = join(segDir, 'list.txt');
writeFileSync(list, parts.map(p => `file '${p.replace(/\\/g, '/')}'`).join('\n'));
const joined = join(segDir, 'joined.mp4');
await run('ffmpeg', ['-y', '-loglevel', 'error', '-f', 'concat', '-safe', '0', '-i', list, '-c', 'copy', joined]);

// --- Çıktı profili ---
const ext = P.profile === 'alpha' ? '.webm' : '.mp4';
const out = resolve(outArg || join(deck, 'out', `${basename(deck)}-${P.profile}${ext}`));
const dur = Number(execFileSync('ffprobe', ['-v', 'error', '-show_entries', 'format=duration', '-of', 'csv=p=0', joined]).toString());
const enc = {
  pptx: ['-c:v', 'libx264', '-preset', 'slow', '-crf', '18', '-pix_fmt', 'yuv420p', '-movflags', '+faststart'],
  web:  ['-c:v', 'libsvtav1', '-preset', '6', '-crf', '30', '-pix_fmt', 'yuv420p'],
  '4k': ['-c:v', 'libx264', '-preset', 'slow', '-crf', '18', '-pix_fmt', 'yuv420p', '-movflags', '+faststart'],
}[P.profile] || [];
await run('ffmpeg', ['-y', '-loglevel', 'error', '-i', joined, ...enc, out]);

// --- Boyut hedefi (ör. e-posta için 25 MB) ---
if (P.max_size_mb && statSync(out).size / 1048576 > P.max_size_mb) {
  const kbps = Math.floor((P.max_size_mb * 8192 * 0.95) / dur);
  console.log(`↺ Boyut hedefi aşıldı, ${kbps} kbit/s ile yeniden kodlanıyor`);
  await run('ffmpeg', ['-y', '-loglevel', 'error', '-i', joined, '-c:v', 'libx264', '-preset', 'slow', '-b:v', `${kbps}k`,
    '-maxrate', `${Math.floor(kbps * 1.5)}k`, '-bufsize', `${kbps * 2}k`, '-pix_fmt', 'yuv420p', '-movflags', '+faststart', out]);
}
console.log(`\n→ ${out}  (${dur.toFixed(1)} sn)`);
await run('node', [join(ROOT, 'scripts', 'probe.mjs'), out, ...(P.max_size_mb ? ['--max-mb', String(P.max_size_mb)] : []), '--fps', String(P.fps)]);
