from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from sopotek.broker.base import BaseBroker
from sopotek.core.models import OrderBookSnapshot, OrderIntent


class LegacyBrokerAdapter(BaseBroker):
    """Adapter that lets the new runtime use the existing broker layer."""

    def __init__(self, broker: Any, *, poll_interval: float = 0.25) -> None:
        self.broker = broker
        self.poll_interval = max(0.05, float(poll_interval))

    async def fetch_ohlcv(self, symbol: str, timeframe: str = "1m", limit: int = 200):
        return await self.broker.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)

    async def place_order(self, order: OrderIntent):
        if hasattr(self.broker, "place_order"):
            return await self.broker.place_order(order)
        return await self.broker.create_order(
            symbol=order.symbol,
            side=order.side,
            amount=order.quantity,
            type=order.order_type,
            price=order.price,
            stop_loss=order.stop_price,
            take_profit=order.take_profit,
            params=dict(order.metadata),
        )

    async def stream_ticks(self, symbol: str) -> AsyncIterator[dict]:
        if hasattr(self.broker, "stream_ticks"):
            async for tick in self.broker.stream_ticks(symbol):
                yield tick
            return

        while True:
            ticker = await self.broker.fetch_ticker(symbol)
            payload = dict(ticker or {})
            payload.setdefault("symbol", symbol)
            payload.setdefault("price", payload.get("last") or payload.get("close") or payload.get("bid") or payload.get("ask"))
            yield payload
            await asyncio.sleep(self.poll_interval)

    async def stream_order_book(self, symbol: str) -> AsyncIterator[OrderBookSnapshot]:
        if hasattr(self.broker, "stream_order_book"):
            async for snapshot in self.broker.stream_order_book(symbol):
                if isinstance(snapshot, OrderBookSnapshot):
                    yield snapshot
                else:
                    payload = dict(snapshot or {})
                    yield OrderBookSnapshot(
                        symbol=payload.get("symbol", symbol),
                        bids=list(payload.get("bids") or []),
                        asks=list(payload.get("asks") or []),
                    )
            return

        if not hasattr(self.broker, "fetch_order_book"):
            return
        while True:
            payload = await self.broker.fetch_order_book(symbol)
            yield OrderBookSnapshot(
                symbol=symbol,
                bids=list((payload or {}).get("bids") or []),
                asks=list((payload or {}).get("asks") or []),
            )
            await asyncio.sleep(self.poll_interval)
