import asyncio
import traceback

from PySide6.QtCore import QObject

from sopotek_trading.backend.broker.broker_factory import BrokerFactory
from sopotek_trading.backend.core.orchestrator import Orchestrator
from sopotek_trading.backend.managers.execution_manager import ExecutionManager
from sopotek_trading.backend.managers.web_socket_manager import WebSocketManager
from sopotek_trading.backend.risk.institutional_risk import InstitutionalRiskEngine


class SopotekTrading(QObject):

    def __init__(self, controller):
        super().__init__()

        self.symbols_list = controller.symbols_list
        self.tickers = {}
        self.ws_manager = controller.ws_manager
        self.controller = controller
        self.logger = controller.logger

        # UI Signals
        self.ticker_signal = controller.ticker_signal
        self.equity_signal = controller.equity_signal
        self.candle_signal = controller.candle_signal
        self.trade_signal = controller.trade_signal
        self.connection_signal = controller.connection_signal
        self.orderbook_signal = controller.orderbook_signal
        self.strategy_debug_signal = controller.strategy_debug_signal
        # ----------------------------
        # Orchestrator
        # ----------------------------

        self.orchestrator = Orchestrator(controller=self.controller)

        self.limit = controller.limit
        self.time_frame = controller.time_frame

        self.symbols = controller.symbols

        self.running = False
        self.autotrading_enabled = False

        self.model_trained = {}
        self.ml_models = {}

        self.spread_pct = 0
        self.current_equity = 0
        self.controller.risk_engine=InstitutionalRiskEngine(
            account_equity=controller.account_equity,
            max_portfolio_risk= controller.max_portfolio_risk,
            max_risk_per_trade= controller.max_risk_per_trade,
            max_position_size_pct= controller.max_position_size_pct,
            max_gross_exposure_pct= controller.max_gross_exposure_pct
        )













        self.logger.info("Sopotek Trading System Ready")

    # ======================================================
    # INITIALIZATION
    # ======================================================

    async def initialize(self):

        try:
            self.controller.broker= BrokerFactory().create(controller=self.controller)
            self.controller.execution_manager = ExecutionManager(controller=self.controller)

            self.connection_signal.emit("connecting")

            await self.controller.broker.connect()

            # Fetch symbols
            exchange_symbols = await self.controller.broker.fetch_symbols()

            if not exchange_symbols:
                raise Exception("No symbols received from exchange")

            if not self.symbols:
                self.controller.symbols = exchange_symbols

            # Send to UI
            self.controller.symbols_signal.emit(
                self.controller.broker.exchange_name,
                self.symbols
            )

            self.logger.info(f"{len(self.symbols)} symbols loaded")

            self.controller.execution_manager.start()


            self.running = True

            asyncio.create_task(self._balance_scheduler())
            self.symbols_list= await self.get_symbols()
            self.symbols= self.symbols_list
            self.logger.info(f"Symbols loaded: {self.symbols_list}")


            # Websocket
            self.ws_manager = WebSocketManager(
                controller=self.controller,
                symbols=["BTC/USDT","XLM/USDT","ETC/USDT"],
                timeframe=self.time_frame,
                candle_callback=self._on_ws_candle,
                ticker_callback=self._on_ticker_callback
            )

            asyncio.create_task(self.ws_manager.start())
            if self.ws_manager.running:
             self.connection_signal.emit("connected")

        except Exception as e:

            self.connection_signal.emit("disconnected")
            self.logger.error(e)
            traceback.print_exc()

    # ======================================================
    # SHUTDOWN
    # ======================================================

    async def shutdown(self):

        self.logger.info("Shutting down system...")

        self.running = False
        self.autotrading_enabled = False

        if self.ws_manager:
           await self.ws_manager.stop()

        await self.controller.execution_manager.stop()
        await self.controller.broker.close()

        self.connection_signal.emit("disconnected")

        self.logger.info("Shutdown complete.")

    # ======================================================
    # BALANCE SCHEDULER
    # ======================================================

    async def _balance_scheduler(self):

        while self.running:

            try:

                balance = await self.controller.broker.fetch_balance()
                self.logger.info(f"Balance: {balance}")

                self.current_equity = float(balance.get("equity", 0))
                self.controller.risk_engine.update_equity(self.current_equity)


                self.equity_signal.emit(self.current_equity)

            except Exception as e:

                self.logger.error(f"Balance scheduler error: {e}")

            await asyncio.sleep(30)

    # ======================================================
    # WEBSOCKET CANDLE
    # ======================================================

    async def _on_ws_candle(self, symbol: str, candle):

        try:

            if "/" not in symbol and symbol.endswith("USDT"):
                symbol = symbol[:-4] + "/USDT"

            required = {"timestamp", "open", "high", "low", "close", "volume"}

            if not required.issubset(candle.keys()):
                self.logger.error(
                    "Required symbol {} not found in candle".format(symbol)
                )
                return
            if self.autotrading_enabled:
                asyncio.create_task(
                self.orchestrator.trading_engine.run_trade(symbol, candle)
    )
            self.controller.candles_buffer.set(symbol, candle)

            self.candle_signal.emit(symbol, candle)





        except Exception as e:

            self.logger.error(f"Candle error {symbol}: {e}")
            traceback.print_exc()

    # ======================================================
    # TICKER
    # ======================================================

    async def _on_ticker_callback(self, symbol: str, bid: float, ask: float):

        try:

            if "/" not in symbol and symbol.endswith("USDT"):
                symbol = symbol[:-4] + "/USDT"

            bid = float(bid)
            ask = float(ask)
            self.tickers[symbol] = {"bid": bid, "ask": ask}

            if bid <= 0 or ask <= 0:
                return

            mid = (bid + ask) / 2
            spread = ask - bid
            self.logger.info(f"Spread {symbol}: {spread}")

            self.spread_pct = (spread / mid) * 100 if mid else 0
            self.logger.info(f"Spread pct {symbol}: {self.spread_pct}")

            self.ticker_signal.emit(symbol, bid, ask)

        except Exception as e:

            self.logger.error(f"Ticker error {symbol}: {e}")

    # ======================================================
    # AUTOTRADING
    # ======================================================

    async def start(self):

        self.autotrading_enabled = True


        self.logger.info(f"AutoTrading enabled for {self.symbols}")

        if not self.orchestrator.running:

            asyncio.create_task(self.orchestrator.start())

    async def stop(self):

        self.autotrading_enabled = False

        await self.orchestrator.shutdown()

        self.logger.info("AutoTrading disabled")

    async def get_symbols(self):

     try:

        symbols = await self.controller.broker.fetch_symbols()

        self.controller.symbols = symbols

        self.controller.symbols_signal.emit(
            self.controller.broker.exchange_name,
            symbols
        )

        return symbols

     except Exception as e:

        self.logger.error(f"Symbol fetch failed: {e}")