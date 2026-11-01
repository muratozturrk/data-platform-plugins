import { embedDashboard } from '@superset-ui/embedded-sdk';

export async function mountDashboard(container: HTMLElement, dashboardUuid: string): Promise<void> {
  await embedDashboard({
    id: dashboardUuid,
    supersetDomain: 'https://t-acme-bi.platform.example',
    mountPoint: container,
    fetchGuestToken: async () => {
      // host backend -> control plane -> token; the browser never calls Superset's guest_token endpoint
      const res = await fetch(`/api/embed-tokens?dashboard=${dashboardUuid}`, { credentials: 'include' });
      if (!res.ok) throw new Error(`guest token failed: ${res.status}`);
      return (await res.json()).token;        // SDK calls this again before expiry
    },
    dashboardUiConfig: {
      hideTitle: true,
      hideChartControls: false,
      filters: { visible: true, expanded: false },
      urlParams: { tenant_theme: 'light' },
    },
    iframeSandboxExtras: [],
    referrerPolicy: 'strict-origin-when-cross-origin',
  });
}
