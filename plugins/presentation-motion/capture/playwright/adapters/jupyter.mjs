/*
 * JupyterLab adapter'ı (taslak). Hücre hücre çalıştırma demoları için.
 * Seçicileri kendi JupyterLab sürümünüzde doğrulayın.
 */
export default {
  name: 'jupyter',
  async waitReady(page, { timeout = 60000 } = {}) {
    await page.locator('.jp-Notebook').first().waitFor({ timeout });
    await page.waitForLoadState('networkidle', { timeout });
  },
  /** n'inci hücreye tıklayıp Shift+Enter ile çalıştırır, çıktı gelene kadar bekler */
  async runCell(page, n, { timeout = 120000 } = {}) {
    const cell = page.locator('.jp-Notebook .jp-Cell').nth(n);
    await cell.click();
    await page.keyboard.press('Shift+Enter');
    await cell.locator('.jp-InputPrompt').filter({ hasNotText: '[*]' }).waitFor({ timeout });
  },
  hide: ['.jp-Notification-Toast'],
};
