"use client";

import { useEffect, useMemo, useState } from "react";

import { publicApiToken, wsBaseUrl } from "@/lib/api";
import { formatPercent } from "@/lib/format";

type Watch = {
  symbol: string;
  last: number;
  changePct: number;
};

export function LiveStrip({ items }: { items: Watch[] }) {
  const [watchlist, setWatchlist] = useState(items);
  const symbols = useMemo(() => items.map((item) => item.symbol), [items]);

  useEffect(() => {
    if (!publicApiToken || !wsBaseUrl || symbols.length === 0) {
      return;
    }
    const socket = new WebSocket(
      `${wsBaseUrl.replace(/^http/, "ws")}/ws/market?token=${publicApiToken}&symbols=${symbols.join(",")}`
    );
    socket.onmessage = (event) => {
      const payload = JSON.parse(event.data);
      if (!payload?.data?.symbol) {
        return;
      }
      setWatchlist((current) =>
        current.map((item) =>
          item.symbol === payload.data.symbol
            ? {
                ...item,
                last: Number(payload.data.last ?? item.last),
                changePct: Number(payload.data.change_pct ?? item.changePct)
              }
            : item
        )
      );
    };
    return () => socket.close();
  }, [symbols]);

  return (
    <div className="panel rounded-[24px] px-4 py-3">
      <div className="flex flex-wrap gap-3">
        {watchlist.map((item) => (
          <div key={item.symbol} className="rounded-full border border-white/10 bg-white/5 px-4 py-2 text-sm">
            <span className="font-[var(--font-mono)] text-mist">{item.symbol}</span>
            <span className="ml-3 font-semibold text-sand">{item.last}</span>
            <span className={`ml-2 ${item.changePct >= 0 ? "text-lime-300" : "text-rose-300"}`}>
              {formatPercent(item.changePct)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
