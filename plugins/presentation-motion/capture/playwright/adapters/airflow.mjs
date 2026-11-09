/*
 * Apache Airflow UI adapter'ı (taslak).
 * Airflow 2.x ve 3.x arayüzleri farklıdır; seçicileri kendi sürümünüzde doğrulayın.
 */
export default {
  name: 'airflow',
  dagUrl(baseUrl, dagId) {
    return `${baseUrl}/dags/${dagId}/grid`;
  },
  async waitReady(page, { timeout = 30000 } = {}) {
    await page.waitForLoadState('networkidle', { timeout });
  },
  /** Görev durumu değişimini beklemek için: hedef görev kutusu "success" olana kadar */
  async waitTaskState(page, taskId, state = 'success', { timeout = 120000 } = {}) {
    await page.waitForSelector(`[data-testid="task-instance"][data-task-id="${taskId}"][data-state="${state}"]`, { timeout });
  },
  hide: ['.alert-info'],
};
