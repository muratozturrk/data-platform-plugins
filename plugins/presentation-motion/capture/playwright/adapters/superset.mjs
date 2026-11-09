/*
 * Apache Superset adapter'ı.
 * Seçiciler Superset 4.x/5.x için yazıldı; sürümünüzde DOM farklıysa burada güncelleyin.
 * Kimlik bilgileri: SUPERSET_USER / SUPERSET_PASS ortam değişkenleri (dosyaya yazmayın).
 */
export default {
  name: 'superset',

  /** Form ile giriş yapar ve oturumu storageState dosyasına kaydeder (bir kez çalıştırılır). */
  async login(page, { baseUrl, username = process.env.SUPERSET_USER, password = process.env.SUPERSET_PASS, save }) {
    if (!username || !password) throw new Error('SUPERSET_USER / SUPERSET_PASS ortam değişkenleri tanımlı değil');
    await page.goto(`${baseUrl}/login/`);
    await page.locator('input[name="username"], #username').first().fill(username);
    await page.locator('input[name="password"], #password').first().fill(password);
    await page.locator('button[type="submit"], input[type="submit"]').first().click();
    await page.waitForURL(u => !u.pathname.startsWith('/login'), { timeout: 30000 });
    if (save) await page.context().storageState({ path: save });
  },

  /** standalone=1 üst menüyü, 2 ayrıca dashboard başlığını gizler */
  dashboardUrl(baseUrl, idOrSlug, standalone = 1) {
    return `${baseUrl}/superset/dashboard/${idOrSlug}/?standalone=${standalone}`;
  },

  sqlLabUrl(baseUrl) {
    return `${baseUrl}/sqllab/`;
  },

  /**
   * Tüm grafikler çizilene kadar bekler: .chart-container görünür ve .loading göstergeleri sıfır.
   * Zaman aşımı SESSİZCE YUTULMAZ: kayıt başarısız sayılır (yarım yüklenmiş dashboard'u kaydetmemek için).
   */
  async waitReady(page, { timeout = 60000 } = {}) {
    await page.locator('.chart-container, .dashboard-chart, [data-test="chart-container"]').first().waitFor({ timeout });
    await page.waitForFunction(
      () => document.querySelectorAll('.loading, [data-test="loading-indicator"], .ant-skeleton').length === 0,
      null, { timeout }
    );
    await page.waitForTimeout(500); // grafik giriş animasyonları (ECharts) bitsin
  },

  hide: ['.ant-notification', '[data-test="toast-container"]', '.navbar-right .ant-badge'],

  /** Sık kullanılan hedefler (sürüme göre doğrulayın) */
  selectors: {
    nativeFilter: name => `[data-test="filter-control-name"]:has-text("${name}")`,
    applyFilters: '[data-test="filter-bar__apply-button"]',
    chartByTitle: title => `.chart-slice:has(.header-title:has-text("${title}"))`,
    sqlEditor: '.ace_editor, .monaco-editor',
    runQuery: '[data-test="run-query-action"]',
    resultTable: '[data-test="table-container"], .filterable-table-container',
  },
};
