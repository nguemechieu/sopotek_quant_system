from __future__ import annotations

import inspect
from abc import ABC
from collections import defaultdict
from typing import Any

from sopotek.core.event_bus import AsyncEventBus
from sopotek.core.event_types import EventType
from sopotek.core.models import Signal


class BaseStrategy(ABC):
    name = "base"

    async def on_tick(self, event) -> None:
        return None

    async def on_candle(self, event) -> None:
        return None

    async def generate_signal(self, *, symbol: str, trigger: str, payload: Any) -> Signal | None:
        return None


class StrategyRegistry:
    def __init__(self) -> None:
        self._strategies: dict[str, BaseStrategy] = {}
        self._global_active: set[str] = set()
        self._symbol_active: dict[str, list[str]] = defaultdict(list)

    def register(self, strategy: BaseStrategy, *, active: bool = True, symbols: list[str] | None = None) -> BaseStrategy:
        self._strategies[strategy.name] = strategy
        if active:
            self._global_active.add(strategy.name)
        if symbols:
            for symbol in symbols:
                self._symbol_active[symbol] = [strategy.name]
        return strategy

    def set_active(self, symbol: str, strategy_names: list[str]) -> None:
        self._symbol_active[str(symbol)] = [name for name in strategy_names if name in self._strategies]

    def activate(self, strategy_name: str) -> None:
        if strategy_name in self._strategies:
            self._global_active.add(strategy_name)

    def deactivate(self, strategy_name: str) -> None:
        self._global_active.discard(strategy_name)
        for symbol, names in list(self._symbol_active.items()):
            self._symbol_active[symbol] = [name for name in names if name != strategy_name]

    def get_active(self, symbol: str | None = None) -> list[BaseStrategy]:
        if symbol and self._symbol_active.get(symbol):
            names = self._symbol_active[symbol]
        else:
            names = list(self._global_active)
        return [self._strategies[name] for name in names if name in self._strategies]


class StrategyEngine:
    def __init__(self, event_bus: AsyncEventBus, registry: StrategyRegistry) -> None:
        self.bus = event_bus
        self.registry = registry
        self.bus.subscribe(EventType.MARKET_TICK, self._on_tick)
        self.bus.subscribe(EventType.CANDLE, self._on_candle)
        self.bus.subscribe(EventType.STRATEGY_SELECTION, self._on_strategy_selection)

    async def _on_tick(self, event) -> None:
        await self._process_event(event, EventType.MARKET_TICK)

    async def _on_candle(self, event) -> None:
        await self._process_event(event, EventType.CANDLE)

    async def _process_event(self, event, trigger: str) -> None:
        payload = getattr(event, "data", None)
        symbol = getattr(payload, "symbol", None)
        if symbol is None and isinstance(payload, dict):
            symbol = payload.get("symbol")
        symbol = str(symbol or "").strip()
        if not symbol:
            return

        for strategy in self.registry.get_active(symbol):
            if trigger == EventType.MARKET_TICK:
                await strategy.on_tick(event)
            elif trigger == EventType.CANDLE:
                await strategy.on_candle(event)

            generated = strategy.generate_signal(symbol=symbol, trigger=trigger, payload=payload)
            signal = await generated if inspect.isawaitable(generated) else generated
            if signal is None:
                continue
            if not isinstance(signal, Signal):
                signal = Signal(**dict(signal))
            await self.bus.publish(EventType.SIGNAL, signal, priority=60, source=strategy.name)

    async def _on_strategy_selection(self, event) -> None:
        payload = dict(getattr(event, "data", {}) or {})
        symbol = str(payload.get("symbol") or "").strip()
        strategies = list(payload.get("strategies") or [])
        if symbol and strategies:
            self.registry.set_active(symbol, strategies)
