#!/usr/bin/env node
/*
 * Render edilmiş sahne videolarından PowerPoint sunumu üretir. Her sahne = bir slayt, tam ekran video.
 *
 * Kullanım:
 *   node build-pptx.mjs <video-klasörü> [-o out/sunum.pptx] [--notes notlar.json] [--auto]
 *
 * Klasördeki dosyalar sahne adına göre slaytlara gruplanır (ada göre sıralı):
 *   02-problem.mp4                         → tek videolu slayt: açılınca oynar
 *   03-bullets-01.mp4, -02.mp4, -03.mp4    → TIKLAMALI slayt: -01 açılınca oynar; her tıklama bir
 *                                            sonraki parçayı üstte gösterip oynatır (render-scene --steps üretir)
 * Sonraki slayta, slayttaki son parçadan sonraki tıklamayla geçilir.
 *
 *   --notes : {"<sahne-adı>": "konuşmacı notu", ...}
 *   --auto  : her slayt video bitince kendiliğinden ilerler (kiosk). Tıklamalı slaytlarla birlikte kullanılamaz.
 *
 * Zamanlama XML'i PowerPoint 365'in "Başlangıç: Otomatik" (Medya oynat) ve "Tıklatıldığında + Belir"
 * için yazdığı yapıyla aynıdır. Parçanın kapak resmi kendi ilk karesidir = önceki parçanın son karesi,
 * bu yüzden parça değişimi görünmez.
 */
import { readdirSync, readFileSync, writeFileSync, statSync, mkdirSync } from 'node:fs';
import { join, resolve, dirname, basename } from 'node:path';
import { execFileSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import PptxGenJS from 'pptxgenjs';
import JSZip from 'jszip';

const a = process.argv.slice(2);
const flag = (k, d) => { const i = a.indexOf(k); return i >= 0 ? a.splice(i, 2)[1] : d; };
const AUTO = a.includes('--auto') && !!a.splice(a.indexOf('--auto'), 1);
const out = resolve(flag('-o', 'out/sunum.pptx'));
const notesFile = flag('--notes', null);
const dir = a[0];
if (!dir) { console.log('Kullanım: build-pptx.mjs <video-klasörü> [-o çıktı.pptx] [--notes notlar.json] [--auto]'); process.exit(1); }
const notes = notesFile ? JSON.parse(readFileSync(notesFile, 'utf8')) : {};

// Sahne adına göre grupla: <ad>-NN.mp4 → parçalar, <ad>.mp4 → tek parça
const slides = new Map();
for (const f of readdirSync(dir).filter(f => f.endsWith('.mp4')).sort()) {
  const m = f.match(/^(.*)-(\d{2})\.mp4$/);
  const name = m ? m[1] : f.slice(0, -4);
  if (!slides.has(name)) slides.set(name, []);
  slides.get(name).push(resolve(dir, f));
}
if (!slides.size) { console.error(`Video yok: ${dir}`); process.exit(1); }
if (AUTO && [...slides.values()].some(c => c.length > 1)) { console.error('--auto tıklamalı (parçalı) slaytlarla kullanılamaz'); process.exit(1); }

const dur = f => Number(execFileSync('ffprobe', ['-v', 'error', '-show_entries', 'format=duration', '-of', 'csv=p=0', f]).toString().trim());
const posters = join(tmpdir(), `pm-pptx-${process.pid}`);
mkdirSync(posters, { recursive: true });

const pres = new PptxGenJS();
pres.layout = 'LAYOUT_WIDE';            // 13,333 × 7,5 inç = 16:9, video ile birebir
pres.author = 'presentation-motion';
const plan = [];
let n = 0;
for (const [name, clips] of slides) {
  const s = pres.addSlide();
  s.background = { color: '000000' };
  const ms = clips.map(c => {
    const poster = join(posters, `${n++}.png`);
    execFileSync('ffmpeg', ['-v', 'error', '-y', '-i', c, '-frames:v', '1', poster]);
    s.addMedia({ type: 'video', path: c, x: 0, y: 0, w: 13.333, h: 7.5, cover: 'data:image/png;base64,' + readFileSync(poster).toString('base64') });
    return Math.round(dur(c) * 1000);
  });
  if (notes[name]) s.addNotes(notes[name]);
  plan.push({ name, ms });
}
mkdirSync(dirname(out), { recursive: true });
await pres.writeFile({ fileName: out });

// --- Sonradan işleme: geçiş + zamanlama (pptxgenjs bunları yazmıyor) ---
const zip = await JSZip.loadAsync(readFileSync(out));
for (const [i, { ms }] of plan.entries()) {
  const p = `ppt/slides/slide${i + 1}.xml`;
  let xml = await zip.file(p).async('string');
  // Video şekillerinin kimlikleri, eklenme (= üst üste binme) sırasıyla
  const spids = (xml.match(/<p:pic>[\s\S]*?<\/p:pic>/g) || []).filter(x => x.includes('videoFile')).map(x => x.match(/<p:cNvPr id="(\d+)"/)[1]);
  if (spids.length !== ms.length) throw new Error(`${p}: ${ms.length} video beklendi, ${spids.length} bulundu`);
  let id = 3;
  const tgt = s => `<p:tgtEl><p:spTgt spid="${s}"/></p:tgtEl>`;
  const play = (k, node) => `<p:par><p:cTn id="${id++}" presetID="1" presetClass="mediacall" presetSubtype="0" fill="hold" nodeType="${node}"><p:stCondLst><p:cond delay="0"/></p:stCondLst><p:childTnLst>` +
    `<p:cmd type="call" cmd="playFrom(0.0)"><p:cBhvr><p:cTn id="${id++}" dur="${ms[k]}" fill="hold"/>${tgt(spids[k])}</p:cBhvr></p:cmd></p:childTnLst></p:cTn></p:par>`;
  const appear = k => `<p:par><p:cTn id="${id++}" presetID="1" presetClass="entr" presetSubtype="0" fill="hold" nodeType="clickEffect"><p:stCondLst><p:cond delay="0"/></p:stCondLst><p:childTnLst>` +
    `<p:set><p:cBhvr><p:cTn id="${id++}" dur="1" fill="hold"><p:stCondLst><p:cond delay="0"/></p:stCondLst></p:cTn>${tgt(spids[k])}<p:attrNameLst><p:attrName>style.visibility</p:attrName></p:attrNameLst></p:cBhvr><p:to><p:strVal val="visible"/></p:to></p:set></p:childTnLst></p:cTn></p:par>`;
  // Grup = PowerPoint'in bir "adım"ı: dış par (tetik) > iç par (gecikme 0) > efektler
  const group = (cond, effects) => { const o = id++, inner = id++; return `<p:par><p:cTn id="${o}" fill="hold"><p:stCondLst>${cond}</p:stCondLst><p:childTnLst><p:par><p:cTn id="${inner}" fill="hold"><p:stCondLst><p:cond delay="0"/></p:stCondLst><p:childTnLst>${effects()}</p:childTnLst></p:cTn></p:par></p:childTnLst></p:cTn></p:par>`; };
  let steps = group('<p:cond delay="indefinite"/><p:cond evt="onBegin" delay="0"><p:tn val="2"/></p:cond>', () => play(0, 'afterEffect'));
  for (let k = 1; k < ms.length; k++) steps += group('<p:cond delay="indefinite"/>', () => appear(k) + play(k, 'withEffect'));
  const media = spids.map(s => `<p:video><p:cMediaNode vol="80000"><p:cTn id="${id++}" fill="hold" display="0"><p:stCondLst><p:cond delay="indefinite"/></p:stCondLst></p:cTn>${tgt(s)}</p:cMediaNode></p:video>`).join('');
  const timing = '<p:timing><p:tnLst><p:par><p:cTn id="1" dur="indefinite" restart="never" nodeType="tmRoot"><p:childTnLst>' +
    `<p:seq concurrent="1" nextAc="seek"><p:cTn id="2" dur="indefinite" nodeType="mainSeq"><p:childTnLst>${steps}</p:childTnLst></p:cTn>` +
    '<p:prevCondLst><p:cond evt="onPrev" delay="0"><p:tgtEl><p:sldTgt/></p:tgtEl></p:cond></p:prevCondLst>' +
    '<p:nextCondLst><p:cond evt="onNext" delay="0"><p:tgtEl><p:sldTgt/></p:tgtEl></p:cond></p:nextCondLst></p:seq>' +
    `${media}</p:childTnLst></p:cTn></p:par></p:tnLst></p:timing>`;
  // Video geçiş + yükleme yüzünden ~0,6 sn geç başlar; --auto'da ilerleme süresine 1,2 sn pay
  const adv = AUTO ? ` advTm="${ms[0] + 1200}"` : '';
  const transition = `<p:transition spd="fast"${adv}><p:fade/></p:transition>`;
  xml = xml.replace(/<p:transition[\s\S]*?<\/p:transition>|<p:transition[^>]*\/>/g, '').replace(/<p:timing>[\s\S]*?<\/p:timing>/g, '');
  // Şema sırası: cSld, clrMapOvr, transition, timing
  xml = xml.includes('</p:clrMapOvr>') ? xml.replace('</p:clrMapOvr>', '</p:clrMapOvr>' + transition + timing) : xml.replace('</p:cSld>', '</p:cSld>' + transition + timing);
  zip.file(p, xml);
}
// Klasör girdileri olmadan yeniden paketle (OneDrive/Teams bunlarda takılabiliyor)
const clean = new JSZip();
for (const [name, entry] of Object.entries(zip.files)) if (!entry.dir) clean.file(name, await entry.async('nodebuffer'), { createFolders: false });
writeFileSync(out, await clean.generateAsync({ type: 'nodebuffer', compression: 'DEFLATE' }));

const clicks = plan.reduce((s, x) => s + x.ms.length - 1, 0);
console.log(`✓ ${out}  (${plan.length} slayt, ${clicks} slayt içi tıklama, ${(statSync(out).size / 1048576).toFixed(1)} MB)`);
for (const { name, ms } of plan) console.log(`  ${name}: ${ms.length} parça (${ms.map(x => (x / 1000).toFixed(1)).join(' + ')} sn)`);
