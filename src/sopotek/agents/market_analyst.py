from __future__ import annotations

from sopotek.agents.base import BaseAgent
from sopotek.core.event_bus import AsyncEventBus
from sopotek.core.event_types import EventType
from sopotek.core.models import AnalystInsight, Candle


class MarketAnalystAgent(BaseAgent):
    name = "market_analyst"

    def __init__(self) -> None:
        self.bus: AsyncEventBus | None = None
        self.latest_insights: dict[str, AnalystInsight] = {}

    def attach(self, event_bus: AsyncEventBus) -> None:
        self.bus = event_bus
        event_bus.subscribe(EventType.CANDLE, self._on_candle)

    async def _on_candle(self, event) -> None:
        candle = getattr(event, "data", None)
        if candle is None or self.bus is None:
            return
        if not isinstance(candle, Candle):
            candle = Candle(**dict(candle))
        delta = candle.close - candle.open
        regime = "bullish" if delta > 0 else "bearish" if delta < 0 else "neutral"
        range_size = max(0.0, candle.high - candle.low)
        volatility = 1.0 + ((range_size / candle.close) if candle.close else 0.0)
        preferred_strategy = "trend" if regime == "bullish" else "mean_reversion" if regime == "neutral" else "defensive"
        insight = AnalystInsight(
            symbol=candle.symbol,
            regime=regime,
            momentum=delta,
            volatility=volatility,
            preferred_strategy=preferred_strategy,
        )
        self.latest_insights[candle.symbol] = insight
        await self.bus.publish(EventType.ANALYST_INSIGHT, insight, priority=50, source=self.name)
