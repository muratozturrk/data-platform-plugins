#!/usr/bin/env node
// Render edilen videoyu ffprobe ile doğrular.
// Kullanım: node probe.mjs out/deck.mp4 [--max-mb 25] [--fps 30]
import { execFileSync } from 'node:child_process';
import { statSync } from 'node:fs';

const a = process.argv.slice(2);
const file = a[0];
const opt = k => { const i = a.indexOf(k); return i >= 0 ? Number(a[i + 1]) : undefined; };
if (!file) { console.log('Kullanım: probe.mjs <video> [--max-mb N] [--fps N]'); process.exit(1); }

const j = JSON.parse(execFileSync('ffprobe', ['-v', 'error', '-print_format', 'json', '-show_format', '-show_streams', file]));
const v = j.streams.find(s => s.codec_type === 'video');
const [n, d] = v.r_frame_rate.split('/').map(Number);
const fps = n / d;
const sizeMB = statSync(file).size / 1048576;
const dur = Number(j.format.duration);

console.log(`dosya      ${file}`);
console.log(`çözünürlük ${v.width}x${v.height}  codec ${v.codec_name}  pix_fmt ${v.pix_fmt}`);
console.log(`fps        ${fps.toFixed(2)}   süre ${dur.toFixed(2)} sn`);
console.log(`boyut      ${sizeMB.toFixed(2)} MB   ortalama ${(Number(j.format.bit_rate) / 1e6).toFixed(2)} Mbit/s`);

let fail = false;
const maxMb = opt('--max-mb');
const wantFps = opt('--fps');
if (maxMb && sizeMB > maxMb) { console.log(`✗ Boyut hedefi aşıldı (${maxMb} MB)`); fail = true; }
if (wantFps && Math.abs(fps - wantFps) > 0.01) { console.log(`✗ ${wantFps} fps bekleniyordu`); fail = true; }
if (v.codec_name === 'h264' && v.pix_fmt !== 'yuv420p') { console.log('✗ H.264 için yuv420p gerekli (PowerPoint uyumu)'); fail = true; }
console.log(fail ? '✗ Probe başarısız' : '✓ Probe temiz');
process.exit(fail ? 1 : 0);
