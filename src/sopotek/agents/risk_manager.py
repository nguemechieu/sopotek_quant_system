from __future__ import annotations

from sopotek.agents.base import BaseAgent
from sopotek.core.event_bus import AsyncEventBus
from sopotek.core.event_types import EventType
from sopotek.engines.risk import RiskEngine


class RiskManagerAgent(BaseAgent):
    name = "risk_manager"

    def __init__(self, risk_engine: RiskEngine) -> None:
        self.risk_engine = risk_engine
        self.alerts: list[dict] = []

    def attach(self, event_bus: AsyncEventBus) -> None:
        event_bus.subscribe(EventType.RISK_ALERT, self._on_alert)

    async def _on_alert(self, event) -> None:
        payload = dict(getattr(event, "data", {}) or {})
        self.alerts.append(payload)
        if payload.get("kill_switch_active"):
            self.risk_engine.activate_kill_switch(str(payload.get("reason") or "Kill switch activated"))
