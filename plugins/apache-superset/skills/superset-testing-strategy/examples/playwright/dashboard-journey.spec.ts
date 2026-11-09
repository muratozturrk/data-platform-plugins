// T-bi-004 / E2E-10: Keycloak login → dashboard → native filter → cross-filter → drill-to-detail → masked CSV export
import { test, expect } from '@playwright/test';

test.use({ storageState: 'playwright/.auth/t-acme-analyst.json' });   // created by auth.setup.ts via Keycloak

test('analyst journey on sales overview', async ({ page }) => {
  await page.goto(`/superset/dashboard/sales-overview/?standalone=0`);
  await expect(page.getByTestId('dashboard-header')).toBeVisible();
  await page.getByRole('button', { name: /filters|Filtreler/i }).click();
  await page.getByLabel('Region').click(); await page.getByRole('option', { name: 'Marmara' }).click();
  await page.getByRole('button', { name: /Apply filters|Uygula/i }).click();
  const kpi = page.getByTestId('chart-id-revenue-today');
  await expect(kpi).toContainText(/₺|TRY/);
  await page.getByTestId('chart-id-orders-by-channel').locator('.echarts-for-react').click({ position: { x: 120, y: 80 } });   // cross-filter
  await expect(page.getByText(/Cross-filter applied|Çapraz filtre/i)).toBeVisible();
  await page.getByTestId('chart-id-orders-by-channel').getByLabel('More Options').click();
  await page.getByRole('menuitem', { name: /Drill to detail/i }).click();
  await expect(page.getByRole('dialog')).toContainText('order_id');
  const [download] = await Promise.all([
    page.waitForEvent('download'),
    page.getByRole('dialog').getByRole('button', { name: /Download|İndir/i }).click(),
  ]);
  const content = await (await download.createReadStream())!.toArray();
  const csv = Buffer.concat(content).toString();
  expect(csv).toMatch(/XXXX\d{4}/);                 // iban masked in export
  expect(csv).not.toMatch(/TR\d{24}/);              // no raw IBAN
});
