// T-bi-011: screenshot baselines per dashboard, theme and viewport; dynamic regions masked
import { test, expect } from '@playwright/test';
const dashboards = ['sales-overview', 'operations-monitoring'];
const themes = ['light', 'dark'];
const viewports = [{ width: 1440, height: 900 }, { width: 1024, height: 768 }, { width: 390, height: 844 }];

test.use({ storageState: 'playwright/.auth/t-acme-analyst.json' });
for (const slug of dashboards) for (const theme of themes) for (const vp of viewports) {
  test(`${slug} ${theme} ${vp.width}x${vp.height}`, async ({ page }) => {
    await page.setViewportSize(vp);
    await page.emulateMedia({ colorScheme: theme as 'light' | 'dark' });
    await page.goto(`/superset/dashboard/${slug}/?standalone=2`);
    await page.waitForLoadState('networkidle');
    await expect(page.getByTestId('dashboard-content')).toHaveScreenshot(`${slug}-${theme}-${vp.width}.png`, {
      maxDiffPixelRatio: 0.01,
      mask: [page.getByTestId('dashboard-header-last-modified'), page.locator('.chart-last-refresh'), page.locator('[data-test="big-number-trendline"] .time')],
    });
  });
}
