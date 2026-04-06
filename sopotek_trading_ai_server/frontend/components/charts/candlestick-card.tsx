type Candle = {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
};

export function CandlestickCard({ candles }: { candles: Candle[] }) {
  const highs = candles.map((candle) => candle.high);
  const lows = candles.map((candle) => candle.low);
  const maxHigh = Math.max(...highs);
  const minLow = Math.min(...lows);
  const range = Math.max(maxHigh - minLow, 0.0001);

  return (
    <div className="grid-lines relative overflow-hidden rounded-[24px] border border-white/10 bg-black/15 p-4">
      <div className="absolute inset-x-0 top-0 h-24 bg-gradient-to-b from-amber-500/10 to-transparent" />
      <svg viewBox="0 0 760 320" className="relative z-10 h-[320px] w-full">
        {candles.map((candle, index) => {
          const x = 60 + index * 82;
          const width = 34;
          const openY = 280 - ((candle.open - minLow) / range) * 230;
          const closeY = 280 - ((candle.close - minLow) / range) * 230;
          const highY = 280 - ((candle.high - minLow) / range) * 230;
          const lowY = 280 - ((candle.low - minLow) / range) * 230;
          const bullish = candle.close >= candle.open;
          const color = bullish ? "#7ee787" : "#fb7185";
          return (
            <g key={candle.time}>
              <line x1={x + width / 2} x2={x + width / 2} y1={highY} y2={lowY} stroke={color} strokeWidth="3" />
              <rect
                x={x}
                y={Math.min(openY, closeY)}
                width={width}
                height={Math.max(Math.abs(closeY - openY), 6)}
                rx="4"
                fill={color}
                fillOpacity={bullish ? 0.88 : 0.74}
              />
              <text x={x + width / 2} y="308" textAnchor="middle" fill="#9fb4c5" fontSize="12">
                {candle.time}
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}
