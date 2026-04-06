import { defaultWorkspaceSettings, type WorkspaceSettingsResponse } from "@/lib/workspace-config";
import { dashboardData, marketData, ordersData, riskData, strategyData } from "@/lib/mock-data";

const apiBaseUrl = process.env.SOPOTEK_API_BASE_URL ?? process.env.NEXT_PUBLIC_SOPOTEK_API_BASE_URL ?? "http://127.0.0.1:8000";
const envApiToken = process.env.SOPOTEK_API_TOKEN ?? process.env.NEXT_PUBLIC_SOPOTEK_API_TOKEN ?? "";
export const wsBaseUrl = process.env.NEXT_PUBLIC_SOPOTEK_WS_BASE_URL ?? "ws://127.0.0.1:8000";
export const publicApiToken = process.env.NEXT_PUBLIC_SOPOTEK_API_TOKEN ?? "";

async function request<T>(path: string, fallback: T, apiToken?: string): Promise<{ data: T; live: boolean }> {
  try {
    const resolvedApiToken = apiToken ?? envApiToken;
    if (!resolvedApiToken) {
      return { data: fallback, live: false };
    }
    const response = await fetch(`${apiBaseUrl}${path}`, {
      headers: {
        Authorization: `Bearer ${resolvedApiToken}`
      },
      cache: "no-store"
    });
    if (!response.ok) {
      return { data: fallback, live: false };
    }
    return { data: (await response.json()) as T, live: true };
  } catch {
    return { data: fallback, live: false };
  }
}

export async function loadDashboardData(apiToken?: string) {
  const portfolio = await request("/portfolio", {
    account_id: "primary",
    broker: "paper",
    total_equity: dashboardData.totalEquity,
    cash: dashboardData.totalEquity * 0.28,
    buying_power: dashboardData.totalEquity * 0.41,
    daily_pnl: dashboardData.dailyPnl,
    weekly_pnl: dashboardData.weeklyPnl,
    monthly_pnl: dashboardData.monthlyPnl,
    gross_exposure: dashboardData.grossExposure,
    net_exposure: dashboardData.netExposure,
    max_drawdown: dashboardData.drawdown,
    var_95: dashboardData.var95,
    margin_usage: 0.34,
    active_positions: dashboardData.activePositions,
    selected_symbols: dashboardData.selectedSymbols,
    risk_limits: riskData.risk_limits,
    positions: dashboardData.positions,
    history: []
  }, apiToken);
  const strategies = await request("/strategies", strategyData, apiToken);
  const risk = await request("/risk", riskData, apiToken);
  return {
    portfolio: portfolio.data,
    strategies: strategies.data,
    risk: risk.data,
    alerts: risk.data.alerts ?? dashboardData.alerts,
    source: portfolio.live || strategies.live || risk.live ? "live" : "demo"
  };
}

export async function loadWorkspaceSettings(apiToken?: string) {
  const result = await request<WorkspaceSettingsResponse>(
    "/workspace/settings",
    {
      ...defaultWorkspaceSettings(),
      created_at: null,
      updated_at: null
    },
    apiToken
  );
  return result.data;
}

export async function loadMarketData() {
  return {
    ...marketData,
    source: envApiToken ? "live" : "demo"
  };
}

export async function loadStrategies(apiToken?: string) {
  const result = await request("/strategies", strategyData, apiToken);
  return result.data;
}

export async function loadOrders(apiToken?: string) {
  const result = await request("/orders/trades", ordersData, apiToken);
  return result.data;
}

export async function loadRisk(apiToken?: string) {
  const result = await request("/risk", riskData, apiToken);
  return result.data;
}
