import { DataTable } from "@/components/panels/data-table";
import { SectionCard } from "@/components/panels/section-card";
import { StatusPill } from "@/components/panels/status-pill";
import { loadStrategies } from "@/lib/api";
import { formatCompactCurrency } from "@/lib/format";
import { requireServerSession } from "@/lib/server-session";

export default async function StrategiesPage() {
  const session = await requireServerSession();
  const strategies = await loadStrategies(session.accessToken);

  return (
    <SectionCard eyebrow="Strategy Control" title="Enable, tune, and inspect the live alpha stack.">
      <DataTable
        rows={strategies}
        columns={[
          {
            key: "name",
            header: "Strategy",
            render: (row: any) => (
              <div>
                <p className="font-semibold text-sand">{row.name}</p>
                <p className="mt-1 text-xs uppercase tracking-[0.22em] text-mist/45">{row.code}</p>
              </div>
            )
          },
          {
            key: "status",
            header: "Status",
            render: (row: any) => <StatusPill value={row.status} />
          },
          {
            key: "performance",
            header: "Performance",
            render: (row: any) => (
              <div className="space-y-1">
                <p>Sharpe {row.performance.sharpe}</p>
                <p>PnL {formatCompactCurrency(row.performance.pnl)}</p>
                <p>Win rate {row.performance.win_rate}%</p>
              </div>
            )
          },
          {
            key: "parameters",
            header: "Parameters",
            render: (row: any) => (
              <div className="font-[var(--font-mono)] text-xs text-mist/75">
                {Object.entries(row.parameters).map(([key, value]) => (
                  <p key={key}>
                    {key}: {String(value)}
                  </p>
                ))}
              </div>
            )
          },
          {
            key: "symbols",
            header: "Assigned Symbols",
            render: (row: any) => (
              <div className="flex flex-wrap gap-2">
                {row.assigned_symbols.map((symbol: string) => (
                  <span key={symbol} className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs">
                    {symbol}
                  </span>
                ))}
              </div>
            )
          }
        ]}
      />
    </SectionCard>
  );
}
