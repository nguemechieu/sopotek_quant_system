from __future__ import annotations

from sopotek.agents.base import BaseAgent
from sopotek.core.event_bus import AsyncEventBus
from sopotek.core.event_types import EventType
from sopotek.core.models import ExecutionReport


class ExecutionMonitorAgent(BaseAgent):
    name = "execution_monitor"

    def __init__(self) -> None:
        self.reports: list[ExecutionReport] = []

    def attach(self, event_bus: AsyncEventBus) -> None:
        event_bus.subscribe(EventType.EXECUTION_REPORT, self._on_execution_report)

    async def _on_execution_report(self, event) -> None:
        report = getattr(event, "data", None)
        if report is None:
            return
        if not isinstance(report, ExecutionReport):
            report = ExecutionReport(**dict(report))
        self.reports.append(report)
