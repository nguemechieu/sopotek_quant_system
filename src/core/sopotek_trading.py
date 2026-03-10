import asyncio
import logging

from manager.portfolio_manager import PortfolioManager
from execution.execution_manager import ExecutionManager
from strategy.strategy_registry import StrategyRegistry
from engines.risk_engine import RiskEngine
from execution.order_router import OrderRouter
from event_bus.event_bus import EventBus
from core.multi_symbol_orchestrator import MultiSymbolOrchestrator


class SopotekTrading:

    def __init__(self, controller=None):

        self.controller = controller
        self.logger = logging.getLogger(__name__)

        # =========================
        # BROKER
        # =========================

        self.broker = getattr(controller, "broker", None)

        if self.broker is None:
            raise RuntimeError("Broker not initialized")

        required_methods = ("fetch_ohlcv", "fetch_balance", "create_order")
        missing = [name for name in required_methods if not hasattr(self.broker, name)]
        if missing:
            raise RuntimeError(
                "Controller broker is missing required capabilities: " + ", ".join(missing)
            )

        self.symbols = getattr(controller, "symbols", ["BTC/USDT", "ETH/USDT"])

        # =========================
        # CORE COMPONENTS
        # =========================

        self.strategy = StrategyRegistry()
        self._apply_strategy_preferences()

        self.event_bus = EventBus()

        self.portfolio = PortfolioManager(event_bus=self.event_bus)

        self.router = OrderRouter(broker=self.broker)

        self.execution_manager = ExecutionManager(
            broker=self.broker,
            event_bus=self.event_bus,
            router=self.router,
            trade_repository=getattr(controller, "trade_repository", None),
            trade_notifier=getattr(controller, "handle_trade_execution", None),
        )

        self.risk_engine = None
        self.orchestrator = None

        # =========================
        # SYSTEM SETTINGS
        # =========================

        self.time_frame = getattr(controller, "time_frame", "1h")
        self.limit = getattr(controller, "limit", 50000)
        self.running = False

        self.logger.info("Sopotek Trading System initialized")

    def _apply_strategy_preferences(self):
        strategy_name = getattr(self.controller, "strategy_name", None)
        strategy_params = getattr(self.controller, "strategy_params", None)
        self.strategy.configure(strategy_name=strategy_name, params=strategy_params)

    # ==========================================
    # START SYSTEM
    # ==========================================

    async def start(self):

        if self.broker is None:
            raise RuntimeError("Broker not initialized")



        balance = getattr(self.controller, "balances", {}) or {}
        equity = float(getattr(self.controller, "initial_capital", 10000) or 10000)
        if isinstance(balance, dict):
            total = balance.get("total")
            if isinstance(total, dict):
                for currency in ("USDT", "USD", "USDC", "BUSD"):
                    value = total.get(currency)
                    if value is None:
                        continue
                    try:
                        equity = float(value)
                        break
                    except Exception:
                        continue



        self.risk_engine = RiskEngine(
            account_equity=equity,
            max_portfolio_risk=getattr(self.controller, "max_portfolio_risk", 100),
            max_risk_per_trade=getattr(self.controller, "max_risk_per_trade", 50),
            max_position_size_pct=getattr(self.controller, "max_position_size_pct", 25),
            max_gross_exposure_pct=getattr(self.controller, "max_gross_exposure_pct", 30),
        )

        self.orchestrator = MultiSymbolOrchestrator(controller=self.controller,
            broker=self.broker,
            strategy=self.strategy,
            execution_manager=self.execution_manager,
            risk_engine=self.risk_engine
        )


        await self.orchestrator.start(symbols=self.symbols)

        self.logger.info(f"Loaded {len(self.symbols)} symbols")

        self.running = True

        await self.run()

    # ==========================================
    # MAIN TRADING LOOP
    # ==========================================

    async def run(self):

        self.logger.info("Trading loop started")

        while self.running:

            try:
                active_symbols = self.symbols[:100]
                if self.controller and hasattr(self.controller, "get_active_autotrade_symbols"):
                    try:
                        resolved = self.controller.get_active_autotrade_symbols()
                    except Exception:
                        resolved = []
                    if resolved:
                        active_symbols = resolved[:100]

                for symbol in active_symbols:

                    if self.controller and hasattr(self.controller, "_safe_fetch_ohlcv"):
                        candles = await self.controller._safe_fetch_ohlcv(
                            symbol,
                            timeframe=self.time_frame,
                            limit=self.limit,
                        )
                    else:
                        candles = await self.broker.fetch_ohlcv(
                            symbol,
                            timeframe=self.time_frame,
                            limit=self.limit
                        )

                    signal = self.strategy.generate_ai_signal(candles)

                    if signal:
                        await self.process_signal(symbol, signal)

                await asyncio.sleep(5)

            except Exception:
                self.logger.exception("Trading loop error")

    # ==========================================
    # PROCESS SIGNAL
    # ==========================================

    async def process_signal(self, symbol, signal):

        side = signal["side"]
        price = signal.get("price")
        amount = signal["amount"]

        allowed = self.risk_engine.validate_trade(symbol, amount)

        if not allowed:

            self.logger.warning("Trade rejected by risk engine")

            return

        order = await self.execution_manager.execute(
            symbol=symbol,
            side=side,
            amount=amount,
            price=price
        )

        self.portfolio.update(order)

    # ==========================================
    # STOP SYSTEM
    # ==========================================

    async def stop(self):

        self.logger.info("Stopping trading system")

        self.running = False

        if self.broker:

            await self.broker.close()
