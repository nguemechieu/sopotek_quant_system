from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from sopotek.core.models import OrderBookSnapshot, OrderIntent


class BaseBroker(ABC):
    @abstractmethod
    async def fetch_ohlcv(self, symbol: str, timeframe: str = "1m", limit: int = 200):
        ...

    @abstractmethod
    async def place_order(self, order: OrderIntent):
        ...

    @abstractmethod
    async def stream_ticks(self, symbol: str) -> AsyncIterator[dict]:
        ...

    async def stream_order_book(self, symbol: str) -> AsyncIterator[OrderBookSnapshot]:
        raise NotImplementedError("stream_order_book is not implemented for this broker")
