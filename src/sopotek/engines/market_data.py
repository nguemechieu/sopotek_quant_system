from __future__ import annotations

import asyncio
import contextlib
import math
from datetime import datetime, timedelta, timezone
from typing import Any

from sopotek.broker.base import BaseBroker
from sopotek.core.event_bus import AsyncEventBus
from sopotek.core.event_types import EventType
from sopotek.core.models import Candle, OrderBookSnapshot


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    return _utc_now()


def timeframe_to_seconds(timeframe: str) -> int:
    text = str(timeframe or "1m").strip().lower()
    value = int(text[:-1] or 1)
    suffix = text[-1]
    if suffix == "s":
        return value
    if suffix == "m":
        return value * 60
    if suffix == "h":
        return value * 3600
    if suffix == "d":
        return value * 86400
    return 60


class LiveFeedManager:
    def __init__(self, broker: BaseBroker, event_bus: AsyncEventBus) -> None:
        self.broker = broker
        self.bus = event_bus
        self._tasks: dict[str, asyncio.Task[Any]] = {}

    async def start_symbol(self, symbol: str) -> None:
        if symbol in self._tasks and not self._tasks[symbol].done():
            return
        self._tasks[symbol] = asyncio.create_task(self._stream_symbol(symbol), name=f"live_feed:{symbol}")

    async def stop(self) -> None:
        tasks = list(self._tasks.values())
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _stream_symbol(self, symbol: str) -> None:
        async for tick in self.broker.stream_ticks(symbol):
            payload = dict(tick or {})
            payload["symbol"] = payload.get("symbol", symbol)
            await self.bus.publish(EventType.MARKET_TICK, payload, priority=20, source="live_feed")


class CandleAggregator:
    def __init__(self, event_bus: AsyncEventBus, *, timeframe: str = "1m") -> None:
        self.bus = event_bus
        self.timeframe = timeframe
        self._seconds = timeframe_to_seconds(timeframe)
        self._candles: dict[tuple[str, str], Candle] = {}
        self.bus.subscribe(EventType.MARKET_TICK, self.on_tick)

    async def on_tick(self, event) -> None:
        payload = dict(getattr(event, "data", {}) or {})
        symbol = str(payload.get("symbol") or "").strip()
        if not symbol:
            return
        price = float(payload.get("price") or payload.get("last") or payload.get("close") or 0.0)
        if price <= 0:
            return
        volume = float(payload.get("volume") or 0.0)
        timestamp = _normalize_timestamp(payload.get("timestamp"))

        start_epoch = math.floor(timestamp.timestamp() / self._seconds) * self._seconds
        start = datetime.fromtimestamp(start_epoch, tz=timezone.utc)
        end = start + timedelta(seconds=self._seconds)

        key = (symbol, self.timeframe)
        current = self._candles.get(key)
        if current is None or current.start != start:
            if current is not None:
                await self.bus.publish(EventType.CANDLE, current, priority=40, source="candle_aggregator")
            current = Candle(
                symbol=symbol,
                timeframe=self.timeframe,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=volume,
                start=start,
                end=end,
            )
            self._candles[key] = current
            return

        current.high = max(current.high, price)
        current.low = min(current.low, price)
        current.close = price
        current.volume += volume

    async def flush(self) -> None:
        pending = list(self._candles.values())
        self._candles.clear()
        for candle in pending:
            await self.bus.publish(EventType.CANDLE, candle, priority=40, source="candle_aggregator")


class OrderBookEngine:
    def __init__(self, event_bus: AsyncEventBus) -> None:
        self.bus = event_bus
        self.snapshots: dict[str, OrderBookSnapshot] = {}

    async def publish_snapshot(self, snapshot: OrderBookSnapshot | dict[str, Any]) -> OrderBookSnapshot:
        if not isinstance(snapshot, OrderBookSnapshot):
            payload = dict(snapshot or {})
            snapshot = OrderBookSnapshot(
                symbol=str(payload.get("symbol") or ""),
                bids=list(payload.get("bids") or []),
                asks=list(payload.get("asks") or []),
            )
        self.snapshots[snapshot.symbol] = snapshot
        await self.bus.publish(EventType.ORDER_BOOK, snapshot, priority=30, source="order_book_engine")
        return snapshot


class MarketDataEngine:
    def __init__(
        self,
        broker: BaseBroker,
        event_bus: AsyncEventBus,
        *,
        candle_timeframes: list[str] | None = None,
    ) -> None:
        self.broker = broker
        self.bus = event_bus
        self.live_feed = LiveFeedManager(broker=broker, event_bus=event_bus)
        self.order_book = OrderBookEngine(event_bus)
        self.aggregators = [CandleAggregator(event_bus, timeframe=timeframe) for timeframe in (candle_timeframes or ["1m"])]

    async def start(self, symbols: list[str]) -> None:
        for symbol in symbols:
            await self.live_feed.start_symbol(symbol)

    async def stop(self) -> None:
        await self.live_feed.stop()
        for aggregator in self.aggregators:
            await aggregator.flush()

    async def publish_tick(self, symbol: str, tick: dict[str, Any]) -> None:
        payload = dict(tick or {})
        payload["symbol"] = payload.get("symbol", symbol)
        await self.bus.publish(EventType.MARKET_TICK, payload, priority=20, source="market_data_engine")
