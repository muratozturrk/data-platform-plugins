#!/usr/bin/env node
/*
 * Gerçek bir web uygulamasını script ile kullanıp yüksek kaliteli video kaydeder.
 *
 * Neden recordVideo değil? Playwright'ın yerleşik kaydı Chromium'da ~1 Mbit/s sabit VP8 kullanır (metin bulanık).
 * Burada Chrome DevTools Protocol screencast (JPEG kareler + zaman damgası) → ffmpeg H.264 kullanılır.
 * Not: Bu gerçek zamanlı bir kayıttır (uygulama gerçek hızında çalışır). Sahne şablonları ise
 *      render-scene.mjs ile kare kare (deterministik) render edilir.
 *
 * Kullanım:
 *   node recorder.mjs flow.yaml [-o out/segment.mp4]
 *   node recorder.mjs --login superset --base-url http://localhost:8088 --save .auth/superset.json
 *
 * flow.yaml:
 *   app: superset                 # adapters/<app>.mjs (varsayılan generic)
 *   baseUrl: http://localhost:8088
 *   url: /superset/dashboard/sales/?standalone=1
 *   auth: .auth/superset.json     # storageState (isteğe bağlı)
 *   scale: 2                      # deviceScaleFactor (zoom'da keskinlik)
 *   mask: [".user-email"]
 *   steps: [...]                  # aşağıdaki "Adımlar" bölümüne bakın
 *
 * Adımlar:
 *   - wait: ready | 1.5 | { selector: "..." }
 *   - caption: "Metin" | null           - badge: "⏩ 4×" | null
 *   - click: "sel" | { selector, zoom: 1.6 }
 *   - type: { selector, text, delay: 55 }
 *   - select: { selector?, option: "Marmara" }
 *   - press: "Enter"                     - hover: "sel"
 *   - scroll: { y: 600 } | { selector }
 *   - zoom: { selector, scale: 1.6, ms: 800 } | false
 *   - spotlight: { selector, hold: 2 } | false
 *   - hold: 2                            - goto: "/yol"
 */
import { chromium } from 'playwright';
import { spawn } from 'node:child_process';
import { readFileSync, writeFileSync, mkdirSync, rmSync, existsSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { tmpdir } from 'node:os';

const HERE = dirname(fileURLToPath(import.meta.url));
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function loadAdapter(name = 'generic') {
  const p = join(HERE, 'adapters', `${name}.mjs`);
  if (!existsSync(p)) throw new Error(`Adapter bulunamadı: ${name}`);
  return (await import(pathToFileURL(p).href)).default;
}

// ---------- İnsan benzeri imleç hareketi ----------
let mouse = { x: 960, y: 540 };
async function moveTo(page, x, y) {
  const dist = Math.hypot(x - mouse.x, y - mouse.y);
  const ms = Math.min(900, Math.max(320, dist * 0.9));
  const steps = Math.round(ms / 16);
  // Hafif kavisli yol (kontrol noktası çizgiye dik kaydırılır)
  const cx = (mouse.x + x) / 2 + (y - mouse.y) * 0.15, cy = (mouse.y + y) / 2 - (x - mouse.x) * 0.15;
  const from = { ...mouse };
  for (let i = 1; i <= steps; i++) {
    const t = i / steps, e = t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2; // easeInOutCubic
    const px = (1 - e) ** 2 * from.x + 2 * (1 - e) * e * cx + e * e * x;
    const py = (1 - e) ** 2 * from.y + 2 * (1 - e) * e * cy + e * e * y;
    await page.mouse.move(px, py);
    await sleep(16);
  }
  mouse = { x, y };
}
async function center(page, selector) {
  const loc = page.locator(selector).first();
  await loc.scrollIntoViewIfNeeded();
  const b = await loc.boundingBox();
  if (!b) throw new Error(`Öğe görünür değil: ${selector}`);
  return { x: b.x + b.width / 2, y: b.y + b.height / 2, loc };
}

// ---------- Adım çalıştırıcı ----------
async function runSteps(page, steps, adapter, baseUrl) {
  for (const step of steps) {
    const [k, v] = Object.entries(step)[0];
    switch (k) {
      case 'goto': await page.goto(v.startsWith('http') ? v : baseUrl + v); break;
      case 'wait':
        if (v === 'ready') await adapter.waitReady(page);
        else if (typeof v === 'number') await sleep(v * 1000);
        else await page.locator(v.selector).first().waitFor({ timeout: v.timeout || 30000 });
        break;
      case 'caption': await page.evaluate(t => window.__pm.caption(t), v); await sleep(350); break;
      case 'badge': await page.evaluate(t => window.__pm.badge(t), v); break;
      case 'hover': { const c = await center(page, v); await moveTo(page, c.x, c.y); await sleep(250); break; }
      case 'click': {
        const sel = typeof v === 'string' ? v : v.selector;
        if (v.zoom) { await page.evaluate(([s, z]) => window.__pm.zoom(s, z, 800), [sel, v.zoom]); await sleep(850); }
        const c = await center(page, sel);
        await moveTo(page, c.x, c.y); await sleep(120);
        await page.mouse.down(); await sleep(70); await page.mouse.up();
        await sleep(350);
        break;
      }
      case 'type': {
        const c = await center(page, v.selector);
        await moveTo(page, c.x, c.y); await page.mouse.click(c.x, c.y);
        await c.loc.pressSequentially(v.text, { delay: v.delay ?? 55 });
        await sleep(300);
        break;
      }
      case 'select': {
        if (v.selector && (await page.locator(v.selector).first().evaluate(e => e.tagName)) === 'SELECT') {
          await page.selectOption(v.selector, { label: v.option });
        } else {
          if (v.selector) { const c = await center(page, v.selector); await moveTo(page, c.x, c.y); await page.mouse.click(c.x, c.y); await sleep(400); }
          const o = await center(page, `text="${v.option}"`);
          await moveTo(page, o.x, o.y); await page.mouse.click(o.x, o.y);
        }
        await sleep(400);
        break;
      }
      case 'press': await page.keyboard.press(v); await sleep(250); break;
      case 'scroll':
        if (v.selector) await page.locator(v.selector).first().scrollIntoViewIfNeeded();
        else await page.mouse.wheel(0, v.y || 600);
        await sleep(700);
        break;
      case 'zoom':
        if (v === false) await page.evaluate(() => window.__pm.zoom(null));
        else await page.evaluate(([s, z, ms]) => window.__pm.zoom(s, z, ms), [v.selector, v.scale || 1.6, v.ms || 800]);
        await sleep((v && v.ms) || 850);
        break;
      case 'spotlight':
        if (v === false) { await page.evaluate(() => window.__pm.spotlight(null)); await sleep(400); break; }
        await page.evaluate(s => window.__pm.spotlight(s), v.selector);
        await sleep((v.hold || 2) * 1000);
        if (v.hold) await page.evaluate(() => window.__pm.spotlight(null));
        break;
      case 'hold': await sleep(v * 1000); break;
      default: throw new Error(`Bilinmeyen adım: ${k}`);
    }
  }
}

// ---------- Kare kodlama ----------
async function encode(frames, stopTs, out, { width = 1920, height = 1080, fps = 30, crf = 18 } = {}) {
  const dir = join(tmpdir(), `pm-rec-${process.pid}`);
  mkdirSync(dir, { recursive: true });
  let list = 'ffconcat version 1.0\n';
  frames.forEach((f, i) => {
    const name = `f${String(i).padStart(6, '0')}.jpg`;
    writeFileSync(join(dir, name), f.buf);
    const next = i + 1 < frames.length ? frames[i + 1].ts : stopTs;
    list += `file ${name}\nduration ${Math.max(0.001, next - f.ts).toFixed(4)}\n`;
  });
  list += `file f${String(frames.length - 1).padStart(6, '0')}.jpg\n`; // concat demuxer son kareyi tekrar ister
  writeFileSync(join(dir, 'list.txt'), list);
  mkdirSync(dirname(out), { recursive: true });
  await new Promise((res, rej) => spawn('ffmpeg', ['-y', '-loglevel', 'error', '-f', 'concat', '-safe', '0', '-i', join(dir, 'list.txt'),
    // JPEG kareler tam aralık (yuvj420p) gelir; PowerPoint uyumu için sınırlı aralık yuv420p'ye çevrilir
    '-vf', `fps=${fps},scale=${width}:${height}:flags=lanczos:out_range=tv,format=yuv420p`,
    '-c:v', 'libx264', '-preset', 'slow', '-crf', String(crf), '-pix_fmt', 'yuv420p', '-color_range', 'tv',
    '-movflags', '+faststart', out], { stdio: 'inherit' })
    .on('close', c => (c === 0 ? res() : rej(new Error(`ffmpeg çıkış kodu ${c}`)))));
  rmSync(dir, { recursive: true, force: true });
}

/** Programatik API (compose.mjs bunu kullanır) */
export async function record(flow, out) {
  const adapter = await loadAdapter(flow.app);
  const vp = flow.viewport || { width: 1920, height: 1080 };
  const scale = flow.scale || 2;
  const browser = await chromium.launch();
  const context = await browser.newContext({
    viewport: vp, deviceScaleFactor: scale,
    storageState: flow.auth && existsSync(flow.auth) ? flow.auth : undefined,
    locale: flow.locale || 'tr-TR', timezoneId: flow.timezone || 'Europe/Istanbul',
  });
  const extraCss = readFileSync(join(HERE, 'mask.css'), 'utf8') +
    [...(adapter.hide || []), ...(flow.hide || [])].map(s => `${s}{display:none!important}`).join('\n') +
    (flow.mask || []).map(s => `${s}{filter:blur(9px)!important}`).join('\n');
  await context.addInitScript({ path: join(HERE, 'overlay.js') });
  await context.addInitScript(css => {
    const add = () => { const s = document.createElement('style'); s.textContent = css; document.documentElement.appendChild(s); };
    if (document.documentElement) add(); else addEventListener('DOMContentLoaded', add);
  }, extraCss);

  const page = await context.newPage();
  const baseUrl = flow.baseUrl || '';
  // Önce sayfayı aç ve hazır olmasını bekle (yükleme anı kayda girmesin)
  if (flow.url) { await page.goto(flow.url.startsWith('http') || flow.url.startsWith('file:') ? flow.url : baseUrl + flow.url); await adapter.waitReady(page); }
  await page.mouse.move(mouse.x, mouse.y);

  const cdp = await context.newCDPSession(page);
  const frames = [];
  cdp.on('Page.screencastFrame', async ({ data, metadata, sessionId }) => {
    frames.push({ buf: Buffer.from(data, 'base64'), ts: metadata.timestamp });
    try { await cdp.send('Page.screencastFrameAck', { sessionId }); } catch {}
  });
  await cdp.send('Page.startScreencast', { format: 'jpeg', quality: 92, maxWidth: vp.width * scale, maxHeight: vp.height * scale, everyNthFrame: 1 });
  await page.evaluate(() => window.__pm && window.__pm.mount());
  await sleep(flow.leadIn ?? 600);

  try {
    await runSteps(page, flow.steps || [], adapter, baseUrl);
    await sleep((flow.tail ?? 0.8) * 1000);
  } finally {
    const stopTs = Date.now() / 1000;
    await cdp.send('Page.stopScreencast').catch(() => {});
    await browser.close();
    if (!frames.length) throw new Error('Hiç kare yakalanmadı');
    await encode(frames, stopTs, out, { width: vp.width, height: vp.height, fps: flow.fps || 30 });
  }
  return out;
}

// ---------- CLI ----------
if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  const a = process.argv.slice(2);
  const flag = k => { const i = a.indexOf(k); return i >= 0 ? a.splice(i, 2)[1] : undefined; };
  const login = flag('--login');
  if (login) {
    const adapter = await loadAdapter(login);
    const baseUrl = flag('--base-url');
    const save = resolve(flag('--save') || `.auth/${login}.json`);
    mkdirSync(dirname(save), { recursive: true });
    const browser = await chromium.launch();
    const page = await browser.newPage();
    await adapter.login(page, { baseUrl, save });
    await browser.close();
    console.log(`✓ Oturum kaydedildi: ${save}`);
  } else {
    const out = resolve(flag('-o') || 'out/recording.mp4');
    const file = a[0];
    if (!file) { console.log('Kullanım: recorder.mjs flow.yaml [-o çıktı.mp4]  |  --login <app> --base-url URL --save yol'); process.exit(1); }
    const text = readFileSync(file, 'utf8');
    const flow = file.endsWith('.json') ? JSON.parse(text) : (await import('yaml')).parse(text);
    await record(flow, out);
    console.log(`✓ ${out}`);
  }
}
