import traceback

from sopotek_trading.backend.strategy.breakout_strategy import BreakoutStrategy
from sopotek_trading.backend.strategy.macd_strategy import MACDStrategy
from sopotek_trading.backend.strategy.mean_reversion_strategy import MeanReversionStrategy
from sopotek_trading.backend.strategy.ml_strategy import MLStrategy
from sopotek_trading.backend.strategy.trend_strategy import TrendStrategy


class StrategyEngine:

    def __init__(self, controller):

        self.model = "models"
        self.controller = controller
        self.logger = controller.logger

        self.strategies = [
            MeanReversionStrategy(),
            BreakoutStrategy(),
            MLStrategy(self.model),
            MACDStrategy(),
            TrendStrategy()
        ]

        self.symbols = getattr(controller, "symbols", [])
        self.running = False

        if self.logger:
            self.logger.info("StrategyEngine initialized")

    # =========================================
    # REGISTER STRATEGY
    # =========================================

    def register_strategy(self, strategy):

        self.strategies.append(strategy)

        if self.logger:
            self.logger.info(
                f"Strategy registered: {strategy.__class__.__name__}"
            )

    # =========================================
    # GENERATE SIGNALS (CANDLES)
    # =========================================

    async def generate_signals(self, symbol, df):

        signals = []

        if not self.running:
            return signals

        for strategy in self.strategies:

            try:

                if hasattr(strategy, "generate_signal"):

                    signal = await strategy.generate_signal(symbol, df)

                    if signal:
                        signals.append(signal)

            except Exception as e:

                if self.logger:
                    self.logger.error(
                        f"Strategy error {strategy.__class__.__name__}: {e}"
                    )

                traceback.print_exc()

        return signals

    # =========================================
    # PROCESS CANDLE DATA
    # =========================================

    async def on_market_data(self, symbol, df):

        signals = await self.generate_signals(symbol, df)

        if not signals:
            return

        for signal in signals:

            if hasattr(self.controller, "signal_generated"):

                self.controller.signal_generated.emit(signal)

    # =========================================
    # PROCESS TICKER DATA
    # =========================================

    async def on_ticker_data(self, symbol, bid, ask):

        if not self.running:
            return

        price = (bid + ask) / 2

        ticker_data = {
            "symbol": symbol,
            "bid": bid,
            "ask": ask,
            "price": price
        }

        for strategy in self.strategies:

            try:

                if hasattr(strategy, "on_ticker"):

                    signal = await strategy.on_ticker(ticker_data)

                    if signal:

                        if hasattr(self.controller, "signal_generated"):

                            self.controller.signal_generated.emit(signal)

            except Exception as e:

                if self.logger:
                    self.logger.error(
                        f"Ticker strategy error {strategy.__class__.__name__}: {e}"
                    )

                traceback.print_exc()

    # =========================================
    # START ENGINE
    # =========================================

    async def start(self):

        self.running = True

        if self.logger:
            self.logger.info("StrategyEngine started")

    # =========================================
    # STOP ENGINE
    # =========================================

    async def stop(self):

        self.running = False

        if self.logger:
            self.logger.info("StrategyEngine stopped")