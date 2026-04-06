import { CandlestickCard } from "@/components/charts/candlestick-card";
import { OrderBookCard } from "@/components/charts/order-book-card";
import { SectionCard } from "@/components/panels/section-card";
import { LiveStrip } from "@/components/panels/live-strip";
import { loadMarketData } from "@/lib/api";
import { formatPercent } from "@/lib/format";
import { requireServerSession } from "@/lib/server-session";

export default async function MarketPage() {
  await requireServerSession();
  const market = await loadMarketData();

  return (
    <div className="space-y-6">
      <LiveStrip items={market.watchlist} />

      <div className="grid gap-6 xl:grid-cols-[1.45fr_0.95fr]">
        <SectionCard
          eyebrow="Market View"
          title={`${market.symbol} candlestick tape and live microstructure.`}
          rightSlot={
            <div className="rounded-full border border-white/10 bg-white/5 px-4 py-2 text-sm">
              <span className="font-[var(--font-mono)] text-mist">{market.last}</span>
              <span className={`ml-3 ${market.changePct >= 0 ? "text-lime-300" : "text-rose-300"}`}>
                {formatPercent(market.changePct)}
              </span>
            </div>
          }
        >
          <CandlestickCard candles={market.candles} />
        </SectionCard>

        <SectionCard eyebrow="Depth" title="Top of book, spread, and liquidity balance.">
          <div className="grid gap-4">
            <div className="grid grid-cols-3 gap-3 rounded-[22px] border border-white/10 bg-white/5 p-4 text-sm">
              <div>
                <p className="text-xs uppercase tracking-[0.25em] text-mist/45">Bid</p>
                <p className="mt-2 font-[var(--font-mono)] text-lime-300">{market.bid}</p>
              </div>
              <div>
                <p className="text-xs uppercase tracking-[0.25em] text-mist/45">Ask</p>
                <p className="mt-2 font-[var(--font-mono)] text-rose-300">{market.ask}</p>
              </div>
              <div>
                <p className="text-xs uppercase tracking-[0.25em] text-mist/45">Volume</p>
                <p className="mt-2 font-[var(--font-mono)] text-sand">{market.volume.toLocaleString()}</p>
              </div>
            </div>
            <OrderBookCard bids={market.orderBook.bids} asks={market.orderBook.asks} />
          </div>
        </SectionCard>
      </div>
    </div>
  );
}
