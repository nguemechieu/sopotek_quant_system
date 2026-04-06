type Level = {
  price: number;
  size: number;
};

export function OrderBookCard({
  bids,
  asks
}: {
  bids: Level[];
  asks: Level[];
}) {
  const maxSize = Math.max(
    ...bids.map((level) => level.size),
    ...asks.map((level) => level.size),
    1
  );

  return (
    <div className="grid gap-4 md:grid-cols-2">
      <div className="rounded-[22px] border border-lime-400/20 bg-lime-400/5 p-4">
        <p className="text-xs uppercase tracking-[0.28em] text-lime-300/80">Bids</p>
        <div className="mt-4 space-y-3">
          {bids.map((level) => (
            <div key={`bid-${level.price}`} className="space-y-1">
              <div className="flex items-center justify-between font-[var(--font-mono)] text-sm text-mist">
                <span>{level.price.toFixed(4)}</span>
                <span>{level.size.toFixed(1)}m</span>
              </div>
              <div className="h-2 rounded-full bg-white/5">
                <div
                  className="h-2 rounded-full bg-lime-300/70"
                  style={{ width: `${(level.size / maxSize) * 100}%` }}
                />
              </div>
            </div>
          ))}
        </div>
      </div>
      <div className="rounded-[22px] border border-rose-400/20 bg-rose-400/5 p-4">
        <p className="text-xs uppercase tracking-[0.28em] text-rose-300/80">Asks</p>
        <div className="mt-4 space-y-3">
          {asks.map((level) => (
            <div key={`ask-${level.price}`} className="space-y-1">
              <div className="flex items-center justify-between font-[var(--font-mono)] text-sm text-mist">
                <span>{level.price.toFixed(4)}</span>
                <span>{level.size.toFixed(1)}m</span>
              </div>
              <div className="h-2 rounded-full bg-white/5">
                <div
                  className="ml-auto h-2 rounded-full bg-rose-300/70"
                  style={{ width: `${(level.size / maxSize) * 100}%` }}
                />
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
