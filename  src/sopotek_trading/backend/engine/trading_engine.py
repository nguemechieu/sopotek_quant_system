
import traceback
import pandas as pd

from sopotek_trading.backend.analytics.performance_engine import PerformanceEngine
from sopotek_trading.backend.broker.broker_factory import BrokerFactory
from sopotek_trading.backend.engine.strategy_engine import StrategyEngine
from sopotek_trading.backend.portfolio.portfolio import Portfolio
from sopotek_trading.backend.portfolio.portfolio_manager import PortfolioManager
from sopotek_trading.backend.quant.regime_detector import MarkovRegimeDetector
from sopotek_trading.backend.risk.institutional_risk import InstitutionalRiskEngine
from sopotek_trading.backend.strategy.strategy import Strategy


class TradingEngine:

    # ==========================================================
    # INIT
    # ==========================================================

    def __init__(self, controller):

        self.controller = controller
        self.logger = controller.logger

        self.execution_manager = controller.execution_manager
        self.trade_signal = getattr(controller, "trade_signal", None)

        self.symbols = controller.symbols
        self.timeframe = controller.timeframe

        self.order_type = controller.order_type
        self.slippage = controller.slippage

        self.running = False
        self.tasks = []

        # Models
        self.ml_models = controller.ml_models
        self.regime_detector = MarkovRegimeDetector(n_regimes=4)

        # Infrastructure
        self.strategy = Strategy(controller)
        self.strategy_engine = StrategyEngine(controller)

        self.risk_engine = InstitutionalRiskEngine(
            account_equity=controller.account_equity,
            max_portfolio_risk=controller.max_portfolio_risk,
            max_risk_per_trade=controller.max_risk_per_trade,
            max_daily_drawdown=controller.max_daily_drawdown,
            max_position_size_pct=controller.max_position_size_pct,
            max_gross_exposure_pct=controller.max_gross_exposure_pct
        )

        self.broker = BrokerFactory.create(self.controller)

        self.portfolio = Portfolio(broker=self.broker)
        self.portfolio_manager = PortfolioManager(
            self.broker,
            self.portfolio,
            self.risk_engine
        )

        self.performance_engine = PerformanceEngine(controller)

        self.logger.info("TradingEngine initialized")

    # ==========================================================
    # START
    # ==========================================================

    async def start(self):

        if self.running:
            return

        self.running = True
        self.logger.info("TradingEngine starting...")

        await self.broker.connect()

        self.logger.info("TradingEngine started")

    # ==========================================================
    # STOP
    # ==========================================================

    async def stop(self):

        self.running = False

        for task in self.tasks:
            task.cancel()

        self.tasks.clear()

        await self.broker.close()

        self.logger.info("TradingEngine stopped")

    # ======================================================
    # MAIN TRADE EXECUTION
    # ======================================================

    async def run_trade(self, symbol: str, df: pd.DataFrame):

        try:

            if df is None or df.empty:
                return

            model = self.ml_models.get(symbol)
            if not model:
                return

            # ===============================
            # ML Prediction
            # ===============================
            analysis = model.predict(df)

            signal = analysis.get("signal", "HOLD")
            if signal not in ["BUY", "SELL"]:
                return

            entry_price = float(analysis.get("current_price", 0))
            confidence = float(analysis.get("confidence", 0.5))

            if entry_price <= 0:
                return

            # ===============================
            # Regime Detection (fit once)
            # ===============================
            if not self.regime_detector.fitted:
                self.regime_detector.fit(df)

            regime_data = self.regime_detector.predict(df)

            regime = regime_data["regime"]
            prob = regime_data["probability"]

            if prob < 0.55:
                return

            if regime == "HIGH_VOL":
                confidence *= 0.5

            if regime == "TREND_UP" and signal == "SELL":
                return

            if regime == "TREND_DOWN" and signal == "BUY":
                return

            # ===============================
            # Risk Calculation
            # ===============================
            volatility = (
                df["close"].pct_change().rolling(20).std().iloc[-1]
            )

            volatility = float(volatility) if not pd.isna(volatility) else 0.01

            stop_distance = entry_price * volatility * 2

            stop_price = (
                entry_price - stop_distance
                if signal == "BUY"
                else entry_price + stop_distance
            )

            size = self.risk_engine.position_size(
                entry_price=entry_price,
                stop_price=stop_price,
                confidence=confidence,
                volatility=volatility
            )

            if size <= 0:
                return

            if self.portfolio.has_position(symbol):
                return

            # ===============================
            # Execute Order
            # ===============================
            result = await self.execution_manager.execute_trade(
                symbol=symbol,
                side=signal.lower(),
                amount=size,
                order_type=self.order_type,
                price=entry_price,
                stop_loss=stop_price,
                take_profit=entry_price + stop_distance,
                slippage=self.slippage,
            )

            if not result:
                return

            # ===============================
            # Portfolio Update
            # ===============================
            self.portfolio_manager.update_fill(
                symbol=symbol,
                side=signal,
                quantity=size,
                price=entry_price
            )

            trade_data = {
                "symbol": symbol,
                "side": signal,
                "price": entry_price,
                "size": size,
                "confidence": confidence,
                "volatility": volatility,
                "regime": regime
            }

            if self.trade_signal:
                self.trade_signal.emit(trade_data)

            self.performance_engine.record_trade(trade_data)

            self.logger.info(
                f"TRADE EXECUTED | {symbol} | {signal} | {size}"
            )

        except Exception as e:
            self.logger.error(f"run_trade error {symbol}: {e}")
            traceback.print_exc()