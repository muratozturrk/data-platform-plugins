/*
 * presentation-motion sahne runtime'ı
 * --------------------------------------------------------------
 * Sözleşme (her sahne bunu izler):
 *   1. Sahne <div class="pm-stage"> içinde 1920×1080 tasarlanır.
 *   2. Veriler <script type="application/json" id="pm-data"> içindedir → PM.data
 *   3. Tüm animasyon tek bir paused timeline'dadır:  const tl = PM.timeline();
 *   4. Sahne kurulunca PM.start() çağrılır.
 *   5. <body data-hold="2"> → animasyon bittikten sonra 2 sn durağan son kare.
 *
 * Modlar:
 *   ?render=1  → render script'i her kare için PM.seek(t) çağırır (oynatma yok).
 *   (varsayılan) önizleme: otomatik oynatır. Space = oynat/duraklat, R = baştan,
 *                ←/→ = 0.5 sn geri/ileri.
 */
(function () {
  const q = new URLSearchParams(location.search);
  const RENDER = q.has('render');
  let seed = Number(q.get('seed') || 1) || 1;

  const PM = (window.PM = {
    render: RENDER,
    fps: Number(q.get('fps') || 30),
    tl: null,
    ready: false,

    /** pm-data JSON bloğu */
    data: (() => {
      const el = document.getElementById('pm-data');
      try { return el ? JSON.parse(el.textContent) : {}; } catch (e) { console.error('pm-data JSON hatalı', e); return {}; }
    })(),

    /** Sahnenin tek timeline'ı (her zaman paused) */
    timeline(opts = {}) {
      PM.tl = gsap.timeline({ paused: true, defaults: { ease: 'power3.out', duration: 0.8 }, ...opts });
      return PM.tl;
    },

    /** Deterministik rastgele sayı (Math.random yerine) */
    random() { seed = (seed * 16807) % 2147483647; return (seed - 1) / 2147483646; },

    /** Timeline zamanı (Date.now yerine) */
    time() { return PM.tl ? PM.tl.time() : 0; },

    hold() { return Number(document.body.dataset.hold || 1.5); },
    duration() { return PM.tl ? PM.tl.duration() + PM.hold() : 0; },
    seek(t) { if (PM.tl) PM.tl.seek(Math.min(t, PM.tl.duration()), false); },

    /** Türkçe sayı biçimi */
    fmt(v, digits = 0) {
      return new Intl.NumberFormat('tr-TR', { minimumFractionDigits: digits, maximumFractionDigits: digits }).format(v);
    },

    async start() {
      if (document.fonts && document.fonts.ready) await document.fonts.ready;
      fit();
      PM.seek(0);
      PM.ready = true;
      if (!RENDER && PM.tl) PM.tl.play(0);
    },
  });

  function fit() {
    const s = document.querySelector('.pm-stage');
    if (!s) return;
    if (RENDER) { s.style.transform = 'none'; return; }
    const k = Math.min(innerWidth / 1920, innerHeight / 1080);
    s.style.transform = `translate(${(innerWidth - 1920 * k) / 2}px, ${(innerHeight - 1080 * k) / 2}px) scale(${k})`;
  }
  addEventListener('resize', fit);

  addEventListener('keydown', (e) => {
    if (RENDER || !PM.tl) return;
    if (e.code === 'Space') { e.preventDefault(); PM.tl.paused() ? PM.tl.play() : PM.tl.pause(); }
    if (e.key === 'r' || e.key === 'R') PM.tl.play(0);
    if (e.key === 'ArrowRight') PM.tl.seek(PM.tl.time() + 0.5);
    if (e.key === 'ArrowLeft') PM.tl.seek(Math.max(0, PM.tl.time() - 0.5));
  });

  if (RENDER) document.documentElement.classList.add('pm-render');
})();
