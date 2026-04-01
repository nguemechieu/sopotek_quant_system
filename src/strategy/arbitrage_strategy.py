"""Arbitrage strategy that watches market ticks and emits cross-exchange orders."""

from typing import Any

from strategy.base_strategy import BaseStrategy
from event_bus.event_types import EventType


class ArbitrageStrategy(BaseStrategy):
    """A simple arbitrage strategy that compares prices across exchanges.

    This strategy listens for market ticks from multiple venues and stores the
    most recent quoted price for each symbol by exchange. When two or more quotes
    exist for the same symbol, it compares the lowest and highest quote and
    emits a buy or sell signal if the observed spread exceeds the configured
    threshold.
    """

    def __init__(self, event_bus: Any):
        super().__init__(event_bus)
        self.prices: dict[str, dict[str, float]] = {}
        self.bus.subscribe(EventType.MARKET_TICK, self.on_tick)

    async def on_tick(self, event: Any) -> None:
        """Handle market tick events and publish a signal when arbitrage is detected."""
        tick = getattr(event, "data", {}) or {}
        if not isinstance(tick, dict):
            return

        exchange = tick.get("exchange")
        symbol = tick.get("symbol")
        price = tick.get("price")

        if not exchange or not symbol or price is None:
            return

        symbol_prices = self.prices.setdefault(symbol, {})
        try:
            symbol_prices[exchange] = float(price)
        except (TypeError, ValueError):
            return

        if len(symbol_prices) < 2:
            return

        sorted_prices = sorted(symbol_prices.items(), key=lambda item: item[1])
        low_exchange, low_price = sorted_prices[0]
        high_exchange, high_price = sorted_prices[-1]

        if low_price <= 0:
            return

        spread = (high_price - low_price) / low_price
        if spread <= 0.01:
            return

        if low_price < high_price:
            await self.signal(symbol, "BUY", 0.01)
        else:
            await self.signal(symbol, "SELL", 0.01)
