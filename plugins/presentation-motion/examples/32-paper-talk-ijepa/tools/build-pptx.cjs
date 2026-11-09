#!/usr/bin/env node
/*
 * Render edilmiş sahne videolarından (out/_segments/NN.mp4) sunum dosyası üretir.
 *   - Her sahne = bir slayt, tam ekran gömülü video, slayt açılınca OTOMATİK oynar.
 *   - Kapak resmi = videonun İLK karesi. Video geçiş + yükleme nedeniyle ~1 sn geç başlar;
 *     son kare kapak olursa önce bitmiş sahne görünüp başa atlıyordu (3. slaytta belirgin).
 *   - Slaytlar arası soldurma geçişi; ilerleme tıklamayla (sunucu kontrolünde).
 *   - Konuşmacı notları aşağıdaki NOTES dizisinden.
 *
 * Kullanım (önce compose ile render alın):
 *   NODE_PATH=<pptxgenjs + jszip kurulu node_modules> node tools/build-pptx.cjs [--auto]
 *   --auto : her slayt video bitince kendiliğinden ilerler (kiosk / sunucusuz oynatma);
 *            video ~0,6 sn geç başladığı için süreye 1,2 sn pay eklenir
 */
const fs = require("fs");
const path = require("path");
const { execFileSync } = require("child_process");
const PptxGenJS = require("pptxgenjs");
const JSZip = require("jszip");

const DECK = path.resolve(__dirname, "..");
const SEG = path.join(DECK, "out", "_segments");
const POSTERS = path.join(DECK, "out", "pptx-posters");
const AUTO = process.argv.includes("--auto");
const OUT = path.join(DECK, "out", AUTO ? "ijepa-sunum-otomatik.pptx" : "ijepa-sunum.pptx");

// Slayt başlıkları ve konuşmacı notları — demo.yaml segment sırasıyla aynı
const NOTES = [
  ["I-JEPA", "Bugün Meta AI’dan Assran ve arkadaşlarının 2023 tarihli I-JEPA makalesini anlatacağım: görüntülerden, veri artırma kullanmadan öz-denetimli temsil öğrenme."],
  ["Problem", "Değişmezlik tabanlı yöntemler (DINO, iBOT) anlamsal temsil öğrenir ama el yapımı veri artırmaya bağımlıdır. MAE gibi üretken yöntemler artırma gerektirmez ama piksel ayrıntısına odaklandığı için anlamsal düzey düşer. Makalenin sorusu: ikisinin iyi yanlarını birleştirebilir miyiz?"],
  ["Üç mimari ailesi", "Şekil 2: ortak gömme mimarisi iki girdinin gömmelerini benzetir; üretken mimari y’yi piksel uzayında yeniden kurar; JEPA ise y’nin temsilini tahmin eder. Kritik fark, kaybın hesaplandığı uzay: JEPA’da temsil uzayı."],
  ["Çoklu blok maskeleme", "Görüntü yamalara bölünür. Ölçeği 0,15–0,2 olan 4 hedef blok ve ölçeği 0,85–1,0 olan bir bağlam bloğu örneklenir; çakışan yamalar bağlamdan çıkarılır. Önemli ayrıntı: hedefler girdide değil, hedef kodlayıcının çıktısında maskelenir."],
  ["Yöntem akışı", "Bağlam kodlayıcı yalnız görünen yamaları işler. Dar bir ViT olan tahminci, konum maske belirteçleriyle her hedef bloğun temsilini tahmin eder. Hedef kodlayıcı tüm görüntüyü işler; ağırlıkları gradyanla değil, bağlam kodlayıcının üstel hareketli ortalamasıyla güncellenir (momentum 0,996 → 1,0)."],
  ["Kayıp fonksiyonu", "Kayıp, tahmin edilen ve hedef yama temsilleri arasındaki ortalama L2 uzaklığıdır; M = 4 blok üzerinden ortalanır. Yalnız bağlam kodlayıcı ve tahminci AdamW ile eğitilir."],
  ["Bulgular", "Şimdi sonuçlara geçelim: sınıflandırma, aktarım, verimlilik ve ablasyon."],
  ["ImageNet doğrusal değerlendirme", "Tablo 1: veri artırma kullanmayan yöntemler arasında I-JEPA en iyisi. 448 piksel çözünürlükte ViT-H/16 ile 81,1 Top-1; bu, veri artırma kullanan iBOT’un 81,0’ına eşit, üstelik 300 epoch ile. Not: eksen 76’dan başlıyor."],
  ["Verimlilik", "ViT-H/14 ön-eğitimi 1.200 GPU saatten az sürüyor (16 A100, 72 saatten kısa). iBOT ViT-S/16’dan 2,5 kattan, MAE ViT-H/14’ten 10 kattan fazla verimli. İterasyon başına yaklaşık yüzde 7 ek maliyet var ama yaklaşık 5 kat daha az iterasyonda yakınsıyor."],
  ["Aktarım", "Tablo 3 ve 4: artırma kullanmayanlar arasında anlamsal görevlerin hepsinde birinci; CIFAR100 ve Places205’te DINO’yu da geçiyor. Clevr uzaklık tahmininde artırma tabanlı yöntemlerden belirgin şekilde önde: 72,4."],
  ["Ablasyon", "Tablo 6: önerilen çoklu blok maskeleme 54,2; diğer stratejiler 20,2 ve altı. Tablo 7: aynı kurgu piksel uzayında tahmin edilince doğruluk 66,9’dan 40,7’ye düşüyor. Yani iki tasarım kararı da belirleyici."],
  ["Çıkarımlar ve sınırlılıklar", "Artırmasız anlamsal temsil mümkün; maskeleme stratejisi belirleyici; yöntem daha az hesapla ölçekleniyor. Sınırlılık: aktarım görevlerinde iBOT hâlâ önde ve ViT-G/16’nın büyük yamaları yerel görevlerde fayda sağlamıyor."],
  ["Kaynakça", "Teşekkürler. Sorulara geçebiliriz."],
];

const dur = f => Number(execFileSync("ffprobe", ["-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", f]).toString().trim());

(async () => {
  fs.mkdirSync(POSTERS, { recursive: true });
  const pres = new PptxGenJS();
  pres.layout = "LAYOUT_WIDE";           // 13,333 × 7,5 inç = 16:9, video ile birebir
  pres.title = "I-JEPA – Makale Sunumu";
  pres.author = "presentation-motion";
  pres.subject = "Assran vd. (2023), arXiv:2301.08243";

  const durations = [];
  NOTES.forEach(([title, note], i) => {
    const nn = String(i).padStart(2, "0");
    const video = path.join(SEG, nn + ".mp4");
    if (!fs.existsSync(video)) throw new Error("Segment yok: " + video + " — önce compose ile render alın");
    const poster = path.join(POSTERS, nn + ".png");
    execFileSync("ffmpeg", ["-v", "error", "-y", "-i", video, "-frames:v", "1", poster]);
    durations.push(dur(video));
    const s = pres.addSlide();
    s.background = { color: "FBFAF7" };
    s.addMedia({ type: "video", path: video, x: 0, y: 0, w: 13.333, h: 7.5,
      cover: "data:image/png;base64," + fs.readFileSync(poster).toString("base64") });
    s.addNotes(title + "\n\n" + note);
  });
  await pres.writeFile({ fileName: OUT });

  // --- Sonradan işleme: otomatik oynatma + geçiş (pptxgenjs bunları yazmıyor) ---
  const zip = await JSZip.loadAsync(fs.readFileSync(OUT));
  for (let i = 0; i < NOTES.length; i++) {
    const p = "ppt/slides/slide" + (i + 1) + ".xml";
    let xml = await zip.file(p).async("string");
    // Video şeklinin kimliği: videoFile içeren p:pic öğesinin cNvPr id değeri
    const pic = (xml.match(/<p:pic>[\s\S]*?<\/p:pic>/g) || []).find(x => x.includes("videoFile"));
    if (!pic) throw new Error(p + ": video şekli bulunamadı");
    const spid = pic.match(/<p:cNvPr id="(\d+)"/)[1];
    const ms = Math.round(durations[i] * 1000);
    const adv = AUTO ? " advTm=\"" + (ms + 1200) + "\"" : "";
    const transition = "<p:transition spd=\"fast\"" + adv + "><p:fade/></p:transition>";
    const tgt = "<p:tgtEl><p:spTgt spid=\"" + spid + "\"/></p:tgtEl>";
    // PowerPoint’in “Başlangıç: Otomatik” için yazdığı yapı: tmRoot > mainSeq > (onBegin) > mediacall playFrom
    const timing = "<p:timing><p:tnLst><p:par><p:cTn id=\"1\" dur=\"indefinite\" restart=\"never\" nodeType=\"tmRoot\"><p:childTnLst>" +
      "<p:seq concurrent=\"1\" nextAc=\"seek\"><p:cTn id=\"2\" dur=\"indefinite\" nodeType=\"mainSeq\"><p:childTnLst>" +
      "<p:par><p:cTn id=\"3\" fill=\"hold\"><p:stCondLst><p:cond delay=\"indefinite\"/><p:cond evt=\"onBegin\" delay=\"0\"><p:tn val=\"2\"/></p:cond></p:stCondLst><p:childTnLst>" +
      "<p:par><p:cTn id=\"4\" fill=\"hold\"><p:stCondLst><p:cond delay=\"0\"/></p:stCondLst><p:childTnLst>" +
      "<p:par><p:cTn id=\"5\" presetID=\"1\" presetClass=\"mediacall\" presetSubtype=\"0\" fill=\"hold\" nodeType=\"afterEffect\"><p:stCondLst><p:cond delay=\"0\"/></p:stCondLst><p:childTnLst>" +
      "<p:cmd type=\"call\" cmd=\"playFrom(0.0)\"><p:cBhvr><p:cTn id=\"6\" dur=\"" + ms + "\" fill=\"hold\"/>" + tgt + "</p:cBhvr></p:cmd>" +
      "</p:childTnLst></p:cTn></p:par></p:childTnLst></p:cTn></p:par></p:childTnLst></p:cTn></p:par>" +
      "</p:childTnLst></p:cTn><p:prevCondLst><p:cond evt=\"onPrev\" delay=\"0\"><p:tgtEl><p:sldTgt/></p:tgtEl></p:cond></p:prevCondLst>" +
      "<p:nextCondLst><p:cond evt=\"onNext\" delay=\"0\"><p:tgtEl><p:sldTgt/></p:tgtEl></p:cond></p:nextCondLst></p:seq>" +
      "<p:video><p:cMediaNode vol=\"80000\"><p:cTn id=\"7\" fill=\"hold\" display=\"0\"><p:stCondLst><p:cond delay=\"indefinite\"/></p:stCondLst></p:cTn>" + tgt + "</p:cMediaNode></p:video>" +
      "</p:childTnLst></p:cTn></p:par></p:tnLst></p:timing>";
    xml = xml.replace(/<p:transition[\s\S]*?<\/p:transition>|<p:transition[^>]*\/>/g, "")
             .replace(/<p:timing>[\s\S]*?<\/p:timing>/g, "");
    // Şema sırası: cSld, clrMapOvr, transition, timing
    xml = xml.includes("</p:clrMapOvr>")
      ? xml.replace("</p:clrMapOvr>", "</p:clrMapOvr>" + transition + timing)
      : xml.replace("</p:cSld>", "</p:cSld>" + transition + timing);
    zip.file(p, xml);
  }
  // Klasör girdileri olmadan yeniden paketle (OneDrive/Teams gibi bulut servisleri bunlarda takılabiliyor)
  const clean = new JSZip();
  for (const [name, entry] of Object.entries(zip.files)) {
    if (!entry.dir) clean.file(name, await entry.async("nodebuffer"), { createFolders: false });
  }
  fs.writeFileSync(OUT, await clean.generateAsync({ type: "nodebuffer", compression: "DEFLATE" }));
  const mb = (fs.statSync(OUT).size / 1048576).toFixed(1);
  const total = durations.reduce((a, b) => a + b, 0).toFixed(1);
  console.log("✓ " + OUT + "  (" + NOTES.length + " slayt, " + mb + " MB, toplam " + total + " sn video)");
})().catch(e => { console.error(e); process.exit(1); });
