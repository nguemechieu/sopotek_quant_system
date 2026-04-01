from __future__ import annotations

from abc import ABC, abstractmethod

from sopotek.core.event_bus import AsyncEventBus


class BaseAgent(ABC):
    name = "agent"

    @abstractmethod
    def attach(self, event_bus: AsyncEventBus) -> None:
        ...
