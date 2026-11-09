// Genel web uygulaması adapter'ı. Yeni bir uygulama için bunu kopyalayıp özelleştirin.
export default {
  name: 'generic',
  /** Sayfa "çekime hazır" olana kadar bekle */
  async waitReady(page, { timeout = 30000 } = {}) {
    await page.waitForLoadState('networkidle', { timeout });
    await page.evaluate(() => document.fonts && document.fonts.ready);
  },
  /** Kayıtta gizlenecek ek seçiciler */
  hide: [],
};
