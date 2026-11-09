/*
 * Kayıt sırasında sayfaya enjekte edilen overlay (addInitScript ile).
 * - Sentetik imleç: Playwright gerçek imleci kaydetmez; mousemove olaylarını izleyen SVG imleç çizer.
 * - Tıklama halkası, caption (alt yazı bandı), spotlight (hedef dışını karartma).
 * - Kamera: body'ye transform uygulayarak hedefe yakınlaşma.
 * Overlay <html>'in doğrudan çocuğudur (body'nin değil), böylece kamera zoom'u overlay'i etkilemez.
 */
(() => {
  if (window.__pm) return;
  const css = `
    #__pm_overlay{position:fixed;inset:0;pointer-events:none;z-index:2147483647;font-family:Inter,"Segoe UI",system-ui,sans-serif}
    #__pm_cursor{position:fixed;left:0;top:0;width:34px;height:34px;transform:translate(-100px,-100px);will-change:transform;filter:drop-shadow(0 2px 3px rgba(0,0,0,.35))}
    .__pm_ripple{position:fixed;width:56px;height:56px;margin:-28px 0 0 -28px;border-radius:50%;border:3px solid rgba(37,99,235,.9);background:rgba(37,99,235,.18)}
    #__pm_caption{position:fixed;left:50%;bottom:64px;transform:translateX(-50%);max-width:70%;padding:18px 34px;border-radius:16px;
      background:rgba(15,23,42,.88);color:#fff;font-size:30px;font-weight:600;line-height:1.3;text-align:center;opacity:0;transition:opacity .35s, transform .35s}
    #__pm_caption.on{opacity:1}
    #__pm_spot{position:fixed;border-radius:14px;box-shadow:0 0 0 9999px rgba(2,6,23,.55);outline:3px solid rgba(96,165,250,.95);opacity:0;transition:opacity .4s, all .5s cubic-bezier(.65,0,.35,1)}
    #__pm_spot.on{opacity:1}
    #__pm_badge{position:fixed;right:40px;top:36px;padding:8px 18px;border-radius:999px;background:rgba(15,23,42,.85);color:#fff;font-size:24px;font-weight:700;opacity:0;transition:opacity .3s}
    #__pm_badge.on{opacity:1}
    html::-webkit-scrollbar,body::-webkit-scrollbar,*::-webkit-scrollbar{width:0!important;height:0!important}
  `;
  const svg = `<svg id="__pm_cursor" viewBox="0 0 24 24"><path d="M4 2 L4 20 L9 15.5 L12.5 22.5 L15.5 21 L12 14 L19 14 Z" fill="#111" stroke="#fff" stroke-width="1.6" stroke-linejoin="round"/></svg>`;

  function mount() {
    if (document.getElementById('__pm_overlay')) return;
    const st = document.createElement('style'); st.id = '__pm_style'; st.textContent = css;
    document.documentElement.appendChild(st);
    const o = document.createElement('div'); o.id = '__pm_overlay';
    o.innerHTML = svg + '<div id="__pm_caption"></div><div id="__pm_spot"></div><div id="__pm_badge"></div>';
    document.documentElement.appendChild(o);
  }

  let cam = null; // { s, tx, ty, bx, by } — aktif kamera durumu
  window.__pm = {
    mount,
    caption(text) { mount(); const c = document.getElementById('__pm_caption'); if (!text) { c.classList.remove('on'); return; } c.textContent = text; c.classList.add('on'); },
    badge(text) { mount(); const b = document.getElementById('__pm_badge'); if (!text) { b.classList.remove('on'); return; } b.textContent = text; b.classList.add('on'); },
    spotlight(sel, pad = 12) {
      mount(); const s = document.getElementById('__pm_spot');
      if (!sel) { s.classList.remove('on'); return; }
      const el = document.querySelector(sel); if (!el) return false;
      const r = el.getBoundingClientRect();
      Object.assign(s.style, { left: r.left - pad + 'px', top: r.top - pad + 'px', width: r.width + 2 * pad + 'px', height: r.height + 2 * pad + 'px' });
      s.classList.add('on'); return true;
    },
    /**
     * Kamera: hedef öğeyi ekran ortasına getirerek yakınlaş (Screen Studio tarzı).
     * Ekran dışına boşluk düşmemesi için kaydırma sınırlandırılır.
     * Ekran koordinatı: f(x) = b0 + t + s·(x − b0)   (b0: body'nin zoomsuz konumu)
     */
    zoom(sel, scale = 1.6, ms = 800) {
      const b = document.body, W = innerWidth, H = innerHeight;
      b.style.transformOrigin = '0 0';
      b.style.transition = `transform ${ms}ms cubic-bezier(.65,0,.35,1)`;
      if (!sel || scale <= 1) { b.style.transform = 'none'; cam = null; return true; }
      const el = document.querySelector(sel); if (!el) return false;
      if (!cam) { const br = b.getBoundingClientRect(); cam = { s: 1, tx: 0, ty: 0, bx: br.left, by: br.top }; }
      // Mevcut zoom'u tersine çevirip öğenin zoomsuz merkezini bul
      const r = el.getBoundingClientRect();
      const cx = cam.bx + (r.left + r.width / 2 - cam.bx - cam.tx) / cam.s;
      const cy = cam.by + (r.top + r.height / 2 - cam.by - cam.ty) / cam.s;
      const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));
      // Hedef merkezi ekran ortasına; f(0) ≤ 0 ve f(W) ≥ W koşuluyla sınırla
      const tx = clamp(W / 2 - cam.bx - scale * (cx - cam.bx), W - cam.bx - scale * (W - cam.bx), -cam.bx + scale * cam.bx);
      const ty = clamp(H / 2 - cam.by - scale * (cy - cam.by), H - cam.by - scale * (H - cam.by), -cam.by + scale * cam.by);
      b.style.transform = `translate(${tx}px, ${ty}px) scale(${scale})`;
      cam = { ...cam, s: scale, tx, ty };
      return true;
    },
  };

  addEventListener('mousemove', e => {
    const c = document.getElementById('__pm_cursor');
    if (c) c.style.transform = `translate(${e.clientX - 4}px, ${e.clientY - 2}px)`;
  }, true);
  addEventListener('mousedown', e => {
    mount();
    const r = document.createElement('div'); r.className = '__pm_ripple';
    r.style.left = e.clientX + 'px'; r.style.top = e.clientY + 'px';
    document.getElementById('__pm_overlay').appendChild(r);
    r.animate([{ transform: 'scale(.3)', opacity: 1 }, { transform: 'scale(1.4)', opacity: 0 }], { duration: 650, easing: 'cubic-bezier(.2,.7,.3,1)' }).onfinish = () => r.remove();
  }, true);

  if (document.readyState === 'loading') addEventListener('DOMContentLoaded', mount); else mount();
})();
