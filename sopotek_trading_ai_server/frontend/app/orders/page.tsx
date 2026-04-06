import { DataTable } from "@/components/panels/data-table";
import { SectionCard } from "@/components/panels/section-card";
import { StatusPill } from "@/components/panels/status-pill";
import { loadOrders } from "@/lib/api";
import { formatCompactCurrency } from "@/lib/format";
import { requireServerSession } from "@/lib/server-session";

export default async function OrdersPage() {
  const session = await requireServerSession();
  const orders = await loadOrders(session.accessToken);

  return (
    <SectionCard eyebrow="Orders & Trades" title="Execution inventory, fills, and operator audit trail.">
      <DataTable
        rows={orders}
        columns={[
          {
            key: "order",
            header: "Order",
            render: (row: any) => (
              <div>
                <p className="font-semibold text-sand">{row.symbol}</p>
                <p className="mt-1 font-[var(--font-mono)] text-xs text-mist/45">{row.order_id}</p>
              </div>
            )
          },
          {
            key: "side",
            header: "Side",
            render: (row: any) => <span className="uppercase">{row.side}</span>
          },
          {
            key: "status",
            header: "Status",
            render: (row: any) => <StatusPill value={row.status} />
          },
          {
            key: "price",
            header: "Average Price",
            render: (row: any) => <span className="font-[var(--font-mono)]">{row.average_price ?? "Pending"}</span>
          },
          {
            key: "pnl",
            header: "PnL",
            render: (row: any) => (
              <span className={row.pnl >= 0 ? "text-lime-300" : "text-rose-300"}>
                {formatCompactCurrency(row.pnl)}
              </span>
            )
          },
          {
            key: "venue",
            header: "Venue",
            render: (row: any) => row.venue
          }
        ]}
      />
    </SectionCard>
  );
}
