from __future__ import annotations

from sopotek.agents.base import BaseAgent
from sopotek.core.event_bus import AsyncEventBus
from sopotek.core.event_types import EventType
from sopotek.core.models import AnalystInsight


class StrategySelectorAgent(BaseAgent):
    name = "strategy_selector"

    def __init__(self, strategy_map: dict[str, list[str]] | None = None) -> None:
        self.bus: AsyncEventBus | None = None
        self.strategy_map = strategy_map or {
            "bullish": ["trend", "breakout"],
            "bearish": ["defensive", "mean_reversion"],
            "neutral": ["mean_reversion"],
        }

    def attach(self, event_bus: AsyncEventBus) -> None:
        self.bus = event_bus
        event_bus.subscribe(EventType.ANALYST_INSIGHT, self._on_insight)

    async def _on_insight(self, event) -> None:
        insight = getattr(event, "data", None)
        if insight is None or self.bus is None:
            return
        if not isinstance(insight, AnalystInsight):
            insight = AnalystInsight(**dict(insight))
        strategies = self.strategy_map.get(insight.regime, [])
        if insight.preferred_strategy and insight.preferred_strategy not in strategies:
            strategies = [insight.preferred_strategy] + strategies
        if not strategies:
            return
        await self.bus.publish(
            EventType.STRATEGY_SELECTION,
            {"symbol": insight.symbol, "strategies": strategies},
            priority=55,
            source=self.name,
        )
