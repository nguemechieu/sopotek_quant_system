import { WorkspaceSettingsForm } from "@/components/control-panel/workspace-settings-form";
import { MetricCard } from "@/components/panels/metric-card";
import { SectionCard } from "@/components/panels/section-card";
import { StatusPill } from "@/components/panels/status-pill";
import { loadDashboardData, loadWorkspaceSettings } from "@/lib/api";
import { formatCompactCurrency, formatCurrency, formatPercent } from "@/lib/format";
import { requireServerSession } from "@/lib/server-session";

export default async function DashboardPage() {
  const session = await requireServerSession();
  const { portfolio, strategies, risk, alerts, source } = await loadDashboardData(session.accessToken);
  const workspaceSettings = await loadWorkspaceSettings(session.accessToken);

  return (
    <div className="grid gap-6 xl:grid-cols-[1.25fr_0.75fr]">
      <SectionCard
        eyebrow="Control Panel"
        title="Set the same launch parameters the desktop dashboard uses before the workspace goes live."
        rightSlot={<StatusPill value={session.user.role} />}
      >
        <WorkspaceSettingsForm initialSettings={workspaceSettings} userRole={session.user.role} />
      </SectionCard>

      <div className="space-y-6">
        <SectionCard
          eyebrow="Account Snapshot"
          title="Capital, broker binding, and desk posture after sign-in."
          rightSlot={<StatusPill value={risk.trading_enabled ? "enabled" : "paused"} />}
        >
          <div className="grid gap-4 sm:grid-cols-2">
            <MetricCard label="Total Equity" value={formatCurrency(portfolio.total_equity)} hint={`${source.toUpperCase()} feed`} />
            <MetricCard label="Daily PnL" value={formatCompactCurrency(portfolio.daily_pnl)} tone={portfolio.daily_pnl >= 0 ? "good" : "warn"} hint="Current session change" />
            <MetricCard label="Gross Exposure" value={formatCompactCurrency(portfolio.gross_exposure)} hint={`${portfolio.active_positions} active positions`} />
            <MetricCard label="VaR 95" value={formatPercent(portfolio.var_95)} tone="warn" hint="Modeled one-day loss" />
          </div>

          <div className="mt-5 rounded-[24px] border border-white/10 bg-black/10 p-5">
            <p className="text-xs uppercase tracking-[0.3em] text-mist/45">Selected Symbols</p>
            <div className="mt-4 flex flex-wrap gap-2">
              {portfolio.selected_symbols.length ? (
                portfolio.selected_symbols.map((symbol: string) => (
                  <span key={symbol} className="rounded-full border border-white/10 bg-white/5 px-3 py-1 font-[var(--font-mono)] text-sm">
                    {symbol}
                  </span>
                ))
              ) : (
                <span className="text-sm text-mist/55">No watchlist has been saved for this account yet.</span>
              )}
            </div>
          </div>
        </SectionCard>

        <SectionCard eyebrow="Live Context" title="Strategy temperature and risk alerts around the current account.">
          <div className="space-y-4">
            {strategies.slice(0, 2).map((strategy: any) => (
              <div key={strategy.id} className="rounded-[22px] border border-white/10 bg-white/5 p-4">
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <p className="text-lg font-semibold text-sand">{strategy.name}</p>
                    <p className="mt-1 text-sm text-mist/60">{strategy.code}</p>
                  </div>
                  <StatusPill value={strategy.status} />
                </div>
                <div className="mt-4 grid grid-cols-3 gap-3 text-sm">
                  <div>
                    <p className="text-xs uppercase tracking-[0.25em] text-mist/45">Sharpe</p>
                    <p className="mt-1 font-semibold text-mist">{strategy.performance.sharpe}</p>
                  </div>
                  <div>
                    <p className="text-xs uppercase tracking-[0.25em] text-mist/45">PnL</p>
                    <p className="mt-1 font-semibold text-mist">{formatCompactCurrency(strategy.performance.pnl)}</p>
                  </div>
                  <div>
                    <p className="text-xs uppercase tracking-[0.25em] text-mist/45">Universe</p>
                    <p className="mt-1 font-semibold text-mist">{strategy.assigned_symbols.length} names</p>
                  </div>
                </div>
              </div>
            ))}

            {alerts.map((alert: any, index: number) => (
              <div key={`${alert.category}-${index}`} className="rounded-[22px] border border-white/10 bg-black/10 p-4">
                <div className="flex items-center justify-between gap-3">
                  <p className="font-semibold text-sand">{alert.message}</p>
                  <StatusPill value={alert.severity} />
                </div>
                <p className="mt-2 text-sm uppercase tracking-[0.24em] text-mist/45">{alert.category}</p>
              </div>
            ))}
          </div>
        </SectionCard>
      </div>
    </div>
  );
}
