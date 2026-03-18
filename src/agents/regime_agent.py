from event_bus.event import Event
from event_bus.event_types import EventType

from agents.base_agent import BaseAgent


class RegimeAgent(BaseAgent):
    def __init__(self, snapshot_builder, memory=None, event_bus=None):
        super().__init__("RegimeAgent", memory=memory, event_bus=event_bus)
        self.snapshot_builder = snapshot_builder

    async def process(self, context):
        working = dict(context or {})
        symbol = str(working.get("symbol") or "").strip().upper()
        decision_id = working.get("decision_id")
        signal = working.get("signal")
        snapshot = self.snapshot_builder(
            symbol=symbol,
            signal=signal,
            candles=working.get("candles") or [],
            dataset=working.get("dataset"),
            timeframe=working.get("timeframe"),
        )
        working["regime_snapshot"] = snapshot
        if isinstance(signal, dict) and snapshot:
            enriched = dict(signal)
            enriched.setdefault("regime", snapshot.get("regime"))
            enriched["regime_snapshot"] = dict(snapshot)
            working["signal"] = enriched
            signal = enriched
        if self.event_bus is not None:
            await self.event_bus.publish(Event(EventType.REGIME, dict(snapshot or {})))
        self.remember(
            "classified",
            {
                "regime": (snapshot or {}).get("regime"),
                "volatility": (snapshot or {}).get("volatility"),
                "timeframe": (snapshot or {}).get("timeframe"),
                "strategy_name": signal.get("strategy_name") if isinstance(signal, dict) else None,
            },
            symbol=symbol,
            decision_id=decision_id,
        )
        return working
