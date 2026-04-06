import { SectionCard } from "@/components/panels/section-card";
import { StatusPill } from "@/components/panels/status-pill";
import { loadRisk } from "@/lib/api";
import { formatCompactCurrency, formatPercent } from "@/lib/format";
import { requireServerSession } from "@/lib/server-session";

export default async function RiskPage() {
  const session = await requireServerSession();
  const risk = await loadRisk(session.accessToken);

  return (
    <div className="grid gap-6 xl:grid-cols-[1.15fr_0.85fr]">
      <SectionCard eyebrow="Risk Dashboard" title="Drawdown, exposure, and limit posture across the book.">
        <div className="grid gap-4 md:grid-cols-3">
          <div className="rounded-[24px] border border-white/10 bg-white/5 p-5">
            <p className="text-xs uppercase tracking-[0.25em] text-mist/45">Drawdown</p>
            <p className="mt-3 text-3xl font-semibold text-amber-300">{formatPercent(risk.drawdown)}</p>
          </div>
          <div className="rounded-[24px] border border-white/10 bg-white/5 p-5">
            <p className="text-xs uppercase tracking-[0.25em] text-mist/45">Gross Exposure</p>
            <p className="mt-3 text-3xl font-semibold text-sand">{formatCompactCurrency(risk.gross_exposure)}</p>
          </div>
          <div className="rounded-[24px] border border-white/10 bg-white/5 p-5">
            <p className="text-xs uppercase tracking-[0.25em] text-mist/45">Trading State</p>
            <div className="mt-3">
              <StatusPill value={risk.trading_enabled ? "enabled" : "paused"} />
            </div>
          </div>
        </div>

        <div className="mt-6 space-y-4">
          {Object.entries(risk.exposure_by_asset).map(([asset, exposure]) => (
            <div key={asset} className="rounded-[22px] border border-white/10 bg-black/10 p-4">
              <div className="flex items-center justify-between">
                <p className="text-sm uppercase tracking-[0.22em] text-mist/55">{asset}</p>
                <p className="font-semibold text-sand">{formatCompactCurrency(exposure as number)}</p>
              </div>
              <div className="mt-3 h-2 rounded-full bg-white/5">
                <div
                  className="h-2 rounded-full bg-amber-300/80"
                  style={{ width: `${Math.min(((exposure as number) / risk.gross_exposure) * 100, 100)}%` }}
                />
              </div>
            </div>
          ))}
        </div>
      </SectionCard>

      <SectionCard eyebrow="Limits & Alerts" title="Configured guardrails and the alerts stream behind them.">
        <div className="rounded-[24px] border border-white/10 bg-white/5 p-5">
          <p className="text-xs uppercase tracking-[0.25em] text-mist/45">Current Limits</p>
          <div className="mt-4 space-y-3 text-sm">
            {Object.entries(risk.risk_limits).map(([key, value]) => (
              <div key={key} className="flex items-center justify-between rounded-2xl bg-black/15 px-4 py-3">
                <span className="text-mist/70">{key}</span>
                <span className="font-[var(--font-mono)] text-sand">{String(value)}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="mt-5 space-y-3">
          {risk.alerts.map((alert: any, index: number) => (
            <div key={`${alert.category}-${index}`} className="rounded-[22px] border border-white/10 bg-white/5 p-4">
              <div className="flex items-start justify-between gap-4">
                <div>
                  <p className="font-semibold text-sand">{alert.message}</p>
                  <p className="mt-2 text-xs uppercase tracking-[0.24em] text-mist/45">{alert.category}</p>
                </div>
                <StatusPill value={alert.severity} />
              </div>
            </div>
          ))}
        </div>
      </SectionCard>
    </div>
  );
}
