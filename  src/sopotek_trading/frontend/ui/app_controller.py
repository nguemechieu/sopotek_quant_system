import asyncio
import logging
import os
import sys
import traceback

import pandas as pd
from PySide6.QtCore import Signal
from PySide6.QtWidgets import QMainWindow, QStackedWidget, QMessageBox

from sopotek_trading.backend.analytics.performance_engine import PerformanceEngine
from sopotek_trading.backend.broker.rate_limiter import RateLimiter
from sopotek_trading.backend.sopotek_trading import SopotekTrading
from sopotek_trading.backend.strategy.breakout_strategy import BreakoutStrategy
from sopotek_trading.backend.strategy.mean_reversion_strategy import MeanReversionStrategy
from sopotek_trading.backend.strategy.strategy_orchestrator import StrategyOrchestrator
from sopotek_trading.backend.strategy.trend_strategy import TrendStrategy
from sopotek_trading.backend.websocket.candle_buffer import CandleBuffer
from sopotek_trading.frontend.ui.dashboard import Dashboard
from sopotek_trading.frontend.ui.terminal import Terminal


class AppController(QMainWindow):
    # ================================
    # Signals
    # ================================

    candle_signal = Signal(str, object)
    equity_signal = Signal(float)
    symbols_signal = Signal(str, list)
    trade_signal = Signal(dict)
    ticker_signal = Signal(str, float, float)
    connection_signal = Signal(str)
    orderbook_signal = Signal(str, list, list)
    strategy_debug_signal = Signal(dict)
    autotrade_toggle = Signal(bool)

    logout_requested = Signal(str)
    training_status_signal = Signal(str, str)

    # ================================
    # INIT
    # ================================

    def __init__(self):

        super().__init__()
        self.controller = self

        self.type = "crypto"
        self.trading_app = None
        self.terminal = None
        self.initial_capital = 10000
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(logging.StreamHandler(sys.stdout))
        self.logger.addHandler(logging.FileHandler("logs/app.log"))
        self.rate_limiter = RateLimiter()
        self.rate_limit = 5
        self.trading_engine = None
        self.ws_manager = None
        self.current_equity = 0.0

        self.ml_models= None

        self.slippage = 3
        self.order_type = "market"
        self.stop_loss = 100
        self.take_profit = 100
        self.amount = 0.01
        self.price = 1.2
        self.commission = 0.003
        self.account_equity = 0.0


        self.orderbook = []
        self.candles = []
        self.tickers = []

        self.limit = 1000
        self.candles_buffer=CandleBuffer(self.limit)




        self.regime_detector=None


        self.risk_engine = None
        self.portfolio = None
        self.execution_manager = None

        self.symbols = ["BTC/USDT", "ETH/USDT", "XLM/USDT"]
        self.timeframe = "1h"
        self.time_frame=self.timeframe
        self.running = False
        self.account_equity = 0.0
        self.max_portfolio_risk = 1000
        self.max_risk_per_trade = 3
        self.max_daily_drawdown = 13
        self.max_position_size_pct = 1
        self.max_gross_exposure_pct = 500
        self.paper_balance = 10000
        self.paper_positions = {}
        self.paper_order_id = 1
        self.account_id = "wer"
        self.config = None

        try:

            self.strategy = None

            self._setup_paths()
            self._setup_config()
            self._setup_data()
            self._setup_strategies()
            self._setup_ui()
            self.strategy_engine = None

        except Exception as e:
            traceback.print_exc()
            self.logger.error(e)

    # ================================
    # Setup Paths
    # ================================

    def _setup_paths(self):

        self.data_dir = "data"
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir, exist_ok=True)




    # ================================
    # Setup Config
    # ================================

    def _setup_config(self):

        self.exchange_name = "binanceus"

        self.api_key = "jhk"
        self.secret = "jkl"

        self.mode = "live"
        self.order_type = "market"

        self.time_frame = "1d"
        self.limit = 1000

        self.balance = 0.0
        self.equity = 0.0

        self.daily_loss = 0.0
        self.daily_gains = 0.0
        self.account_equity = 0
        self.max_risk_per_trade = 10
        self.max_position_size_pct = 12
        self.max_gross_exposure_pct = 4
        self.max_daily_drawdown = 50
        self.max_portfolio_risk = 500

        self.time_frame = "1d"
        self.metrics = {'total_trades': 120, 'win_rate': 0.55, 'profit': 2400, 'balance': 12400}
        self.strategies = [TrendStrategy(), MeanReversionStrategy(), BreakoutStrategy()]
        self.weights = {"trend": 0.5, "mean_reversion": 0.2, "breakout": 0.2, "ml": 0.1}
        self.strategy_weight = 1
        self.orchestrator = StrategyOrchestrator(self.strategies, self.metrics)
        self.broker=None
        self.symbols_list=[]

    # ================================
    # Setup Data
    # ================================

    def _setup_data(self):

        self.historical_data = pd.DataFrame(
            columns=[
                "symbol",
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume"
            ]
        )

        self.historical_returns = pd.DataFrame(
            columns=[
                "symbol",
                "trades",
                "returns"
            ]
        )

        self.historical_data.to_csv(
            f"{self.data_dir}\\historical_data.csv",
            index=False
        )

        self.historical_returns.to_csv(
            f"{self.data_dir}\\historical_returns.csv",
            index=False
        )

    # ================================
    # Setup Strategies
    # ================================

    def _setup_strategies(self):

        self.metrics = {
            "total_trades": 0,
            "win_rate": 0,
            "profit": 0,
            "balance": 0
        }

        self.strategies = None

        self.weights = {
            "trend": 0.5,
            "mean_reversion": 0.2,
            "breakout": 0.2,
            "ml": 0.1
        }



        self.performance_engine = PerformanceEngine(
            controller=self.controller
        )
        self.orchestrator = StrategyOrchestrator(
            self.strategies,
            self.metrics
        )

    # ================================
    # Setup UI
    # ================================

    def _setup_ui(self):

        self.setWindowTitle("Sopotek Trading Platform")
        self.resize(1600, 900)

        self.stack = QStackedWidget()
        self.setCentralWidget(self.stack)

        self.dashboard = Dashboard(self.controller)
        self.stack.addWidget(self.dashboard)

        self.terminal = None

        self.dashboard.login_success.connect(self._handle_login)

    # ================================
    # Login Handler
    # ================================

    def _handle_login(self):

        asyncio.create_task(self.initialize_trading())

    # ================================
    # Initialize Trading
    # ================================

    async def initialize_trading(self):

        try:

            self.dashboard.show_loading()

            await self._cleanup_session()

            self.trading_app = SopotekTrading(

                controller=self.controller

            )
            await  self.trading_app.initialize()
            self.terminal = Terminal(controller=self.controller)
            self.terminal.update()

            self.stack.addWidget(self.terminal)

            self.terminal.autotrade_toggle.connect(
                lambda enabled: asyncio.create_task(
                    self.trading_app.start() if enabled else self.trading_app.stop()
                )
            )

            self.terminal.logout_requested.connect(
                lambda: asyncio.create_task(self.logout())
            )
            self.stack.addWidget(self.terminal)
            self.stack.setCurrentWidget(self.terminal)

        except Exception as e:

            QMessageBox.critical(self, "Initialization Failed", str(e))

            self.dashboard.hide_loading()

    # ================================
    # Cleanup Session
    # ================================

    async def _cleanup_session(self):

        if self.trading_app:
            await self.trading_app.stop()
            self.trading_app = None

        if self.terminal:
            self.stack.removeWidget(self.terminal)
            self.terminal.deleteLater()
            self.terminal = None

    # ================================
    # Logout
    # ================================

    async def logout(self):

        try:

            await self._cleanup_session()

        finally:

            self.stack.setCurrentWidget(self.dashboard)
            self.dashboard.setEnabled(True)
            self.dashboard.connect_button.setText("Connect")
