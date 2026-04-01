from __future__ import annotations

import logging

from sopotek.agents import ExecutionMonitorAgent, MarketAnalystAgent, RiskManagerAgent, StrategySelectorAgent
from sopotek.broker.base import BaseBroker
from sopotek.core.event_bus import AsyncEventBus, InMemoryEventStore
from sopotek.engines import ExecutionEngine, MarketDataEngine, PortfolioEngine, RiskEngine, StrategyEngine, StrategyRegistry


class SopotekRuntime:
    """Composable v2 runtime for the desktop trading system."""

    def __init__(
        self,
        broker: BaseBroker,
        *,
        event_bus: AsyncEventBus | None = None,
        starting_equity: float = 100000.0,
        candle_timeframes: list[str] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.logger = logger or logging.getLogger("SopotekRuntime")
        self.bus = event_bus or AsyncEventBus(store=InMemoryEventStore(), enable_persistence=True, logger=self.logger)
        self.broker = broker
        self.registry = StrategyRegistry()

        self.market_data = MarketDataEngine(broker, self.bus, candle_timeframes=candle_timeframes or ["1m"])
        self.strategy_engine = StrategyEngine(self.bus, self.registry)
        self.portfolio_engine = PortfolioEngine(self.bus, starting_cash=starting_equity)
        self.risk_engine = RiskEngine(self.bus, starting_equity=starting_equity)
        self.execution_engine = ExecutionEngine(broker, self.bus)

        self.market_analyst = MarketAnalystAgent()
        self.strategy_selector = StrategySelectorAgent()
        self.risk_manager = RiskManagerAgent(self.risk_engine)
        self.execution_monitor = ExecutionMonitorAgent()

        for agent in (
            self.market_analyst,
            self.strategy_selector,
            self.risk_manager,
            self.execution_monitor,
        ):
            agent.attach(self.bus)

    def register_strategy(self, strategy, *, active: bool = True, symbols: list[str] | None = None):
        return self.registry.register(strategy, active=active, symbols=symbols)

    async def start(self, symbols: list[str]) -> None:
        self.bus.run_in_background()
        await self.market_data.start(symbols)

    async def stop(self) -> None:
        await self.market_data.stop()
        await self.bus.shutdown()
