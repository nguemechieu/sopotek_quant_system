import asyncio
import json
import logging
import os
import re
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import pandas as pd
from PySide6.QtCore import QSettings, Signal
from PySide6.QtWidgets import QMainWindow, QMessageBox, QStackedWidget

from broker.broker_factory import BrokerFactory
from broker.rate_limiter import RateLimiter
from core.sopotek_trading import SopotekTrading
from event_bus.event_bus import EventBus
from event_bus.event_types import EventType
from frontend.ui.dashboard import Dashboard
from frontend.ui.i18n import DEFAULT_LANGUAGE, normalize_language_code, translate
from frontend.ui.terminal import Terminal
from integrations.telegram_service import TelegramService
from engines.performance_engine import PerformanceEngine
from manager.broker_manager import BrokerManager
from market_data.candle_buffer import CandleBuffer
from market_data.orderbook_buffer import OrderBookBuffer
from market_data.ticker_buffer import TickerBuffer
from market_data.ticker_stream import TickerStream
from market_data.websocket.alpaca_web_socket import AlpacaWebSocket
from market_data.websocket.binanceus_web_socket import BinanceUsWebSocket
from market_data.websocket.coinbase_web_socket import CoinbaseWebSocket
from market_data.websocket.paper_web_socket import PaperWebSocket
from storage.database import init_database
from storage.market_data_repository import MarketDataRepository
from storage.trade_repository import TradeRepository
from strategy.strategy import Strategy


class AppController(QMainWindow):

    symbols_signal = Signal(str, list)
    candle_signal = Signal(str, object)
    equity_signal = Signal(float)

    trade_signal = Signal(dict)
    ticker_signal = Signal(str, float, float)
    connection_signal = Signal(str)
    orderbook_signal = Signal(str, list, list)
    ai_signal_monitor = Signal(dict)

    strategy_debug_signal = Signal(dict)
    autotrade_toggle = Signal(bool)

    logout_requested = Signal(str)
    training_status_signal = Signal(str, str)
    language_changed = Signal(str)

    ALLOWED_CRYPTO_QUOTES = {"USDT", "USD", "USDC", "BUSD", "BTC", "ETH"}
    BANNED_BASE_TOKENS = {"USD4", "FAKE", "TEST"}
    BANNED_BASE_SUFFIXES = {"UP", "DOWN", "BULL", "BEAR", "3L", "3S", "5L", "5S"}
    PREFERRED_BASES = [
        "BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "DOGE", "AVAX",
        "DOT", "LINK", "LTC", "ATOM", "AAVE", "NEAR", "UNI", "MKR",
    ]
    QUOTE_PRIORITY = {"USDT": 0, "USD": 1, "USDC": 2, "BUSD": 3, "BTC": 4, "ETH": 5}

    def __init__(self):
        super().__init__()

        self.controller = self
        self._login_lock = asyncio.Lock()

        self._ticker_task = None
        self._ws_task = None
        self._ws_bus_task = None
        self.ws_bus = None
        self.ws_manager = None
        self.settings = QSettings("Sopotek", "TradingPlatform")
        self.language_code = normalize_language_code(
            self.settings.value("ui/language", DEFAULT_LANGUAGE)
        )

        self.logger = logging.getLogger("AppController")
        self.logger.setLevel(logging.INFO)

        os.makedirs("logs", exist_ok=True)
        if not self.logger.handlers:
            self.logger.addHandler(logging.StreamHandler(sys.stdout))
            self.logger.addHandler(logging.FileHandler("logs/app.log"))

        self.broker_manager = BrokerManager()
        self.rate_limiter = RateLimiter()

        self.broker = None
        self.trading_system = None
        self.terminal = None
        self.telegram_service = None

        self.max_portfolio_risk = 1700
        self.max_risk_per_trade = 20
        self.max_position_size_pct = 100
        self.max_gross_exposure_pct = 34
        self.confidence = 0
        self.volatility = 0
        self.order_type = "limit"
        self.time_frame = "1h"
        self.strategy_name = Strategy.normalize_strategy_name(
            self.settings.value("strategy/name", "Trend Following")
        )
        self.telegram_enabled = str(self.settings.value("integrations/telegram_enabled", "false")).lower() in {"1", "true", "yes", "on"}
        self.telegram_bot_token = str(self.settings.value("integrations/telegram_bot_token", "") or "").strip()
        self.telegram_chat_id = str(self.settings.value("integrations/telegram_chat_id", "") or "").strip()
        self.openai_api_key = str(self.settings.value("integrations/openai_api_key", "") or "").strip()
        self.openai_model = str(self.settings.value("integrations/openai_model", "gpt-5-mini") or "gpt-5-mini").strip()
        self.autotrade_scope = str(self.settings.value("autotrade/scope", "all") or "all").strip().lower()
        if self.autotrade_scope not in {"all", "selected", "watchlist"}:
            self.autotrade_scope = "all"
        raw_watchlist = self.settings.value("autotrade/watchlist", "[]")
        try:
            parsed_watchlist = json.loads(raw_watchlist or "[]")
        except Exception:
            parsed_watchlist = []
        self.autotrade_watchlist = {
            str(symbol).upper().strip()
            for symbol in parsed_watchlist
            if str(symbol).strip()
        }
        self.strategy_params = {
            "rsi_period": int(self.settings.value("strategy/rsi_period", 14)),
            "ema_fast": int(self.settings.value("strategy/ema_fast", 20)),
            "ema_slow": int(self.settings.value("strategy/ema_slow", 50)),
            "atr_period": int(self.settings.value("strategy/atr_period", 14)),
            "oversold_threshold": float(self.settings.value("strategy/oversold_threshold", 35.0)),
            "overbought_threshold": float(self.settings.value("strategy/overbought_threshold", 65.0)),
            "breakout_lookback": int(self.settings.value("strategy/breakout_lookback", 20)),
            "min_confidence": float(self.settings.value("strategy/min_confidence", 0.55)),
            "signal_amount": float(self.settings.value("strategy/signal_amount", 1.0)),
        }

        self.portfolio = None
        self.ai_signal = None
        self.balances = {}
        self.balance = {}

        self.ticker_stream = TickerStream()
        self._performance_recorded_orders = set()

        self.limit = 50000
        self.initial_capital = 10000

        self.candle_buffer = CandleBuffer(max_length=self.limit)
        self.candle_buffers = {}
        self.orderbook_buffer = OrderBookBuffer()
        self.ticker_buffer = TickerBuffer(max_length=self.limit)
        self._orderbook_tasks = {}
        self._orderbook_last_request_at = {}

        self.symbols = ["BTC/USDT", "ETH/USDT", "XLM/USDT"]

        self.connected = False
        self.config = None

        try:
            self._setup_paths()
            self._setup_data()
            self._setup_ui(self.controller)
            self.setWindowTitle(self.tr("app.window_title"))
        except Exception:
            traceback.print_exc()

    def tr(self, key, **kwargs):
        return translate(self.language_code, key, **kwargs)

    def set_language(self, language_code):
        normalized = normalize_language_code(language_code)
        if normalized == self.language_code:
            return

        self.language_code = normalized
        self.settings.setValue("ui/language", normalized)
        self.setWindowTitle(self.tr("app.window_title"))
        self.language_changed.emit(normalized)

    def _setup_paths(self):
        self.data_dir = "data"
        os.makedirs(self.data_dir, exist_ok=True)

    def _setup_data(self):
        init_database()
        self.historical_data = pd.DataFrame(
            columns=["symbol", "timestamp", "open", "high", "low", "close", "volume"]
        )
        self.market_data_repository = MarketDataRepository()
        self.trade_repository = TradeRepository()
        self.performance_engine = PerformanceEngine()

    def _setup_ui(self, controller):
        self.setWindowTitle("Sopotek Trading AI Platform")
        self.resize(1600, 900)

        self.stack = QStackedWidget()
        self.setCentralWidget(self.stack)

        self.dashboard = Dashboard(controller)
        self.stack.addWidget(self.dashboard)

        self.dashboard.login_requested.connect(self._on_login_requested)

    def _on_login_requested(self, config):
        self._create_task(self.handle_login(config), "handle_login")

    def _on_logout_requested(self):
        self._create_task(self.logout(), "logout")

    def _create_task(self, coro, name):
        task = asyncio.create_task(coro)

        def _done(t):
            try:
                exc = t.exception()
                if exc:
                    self.logger.error("Task %s failed: %s", name, exc)
            except asyncio.CancelledError:
                pass

        task.add_done_callback(_done)
        return task

    async def handle_login(self, config):
        async with self._login_lock:
            try:
                if config is None:
                    raise RuntimeError("Invalid configuration received")
                if config.broker is None:
                    raise RuntimeError("Broker configuration missing")

                self.dashboard.show_loading()
                self.config = config

                broker_type = config.broker.type
                exchange = config.broker.exchange or "unknown"
                if not broker_type:
                    raise RuntimeError("Broker type missing")

                await self._cleanup_session(stop_trading=True, close_broker=True)

                self.logger.info("Initializing broker %s", exchange)
                broker = BrokerFactory.create(config)

                if broker is None:
                    raise RuntimeError("Broker creation failed")

                if hasattr(broker, "controller"):
                    broker.controller = self
                if hasattr(broker, "logger"):
                    broker.logger = self.logger

                self.broker = broker
                await self.broker.connect()

                raw_symbols = await self._fetch_symbols(self.broker)
                filtered_symbols = self._filter_symbols_for_trading(raw_symbols, broker_type, exchange)
                self.symbols = await self._select_trade_symbols(filtered_symbols, broker_type, exchange)

                self.balances = await self._fetch_balances(self.broker)
                self.balance = self.balances

                self.logger.info(
                    "Broker ready exchange=%s type=%s symbols=%s (raw=%s filtered=%s)",
                    exchange,
                    broker_type,
                    len(self.symbols),
                    len(raw_symbols),
                    len(filtered_symbols),
                )

                self.trading_system = SopotekTrading(self)
                portfolio_manager = getattr(self.trading_system, "portfolio", None)
                self.portfolio = getattr(portfolio_manager, "portfolio", None)
                self._performance_recorded_orders.clear()
                self.connected = True

                self.connection_signal.emit("connected")
                self.symbols_signal.emit(exchange, self.symbols)

                await self.initialize_trading()
                self.symbols_signal.emit(exchange, self.symbols)
                await self._restart_telegram_service()

                await self._start_market_stream()
                await self._warmup_visible_candles()

            except Exception as e:
                self.connected = False
                self.connection_signal.emit("disconnected")
                self.logger.exception("Initialization failed")
                QMessageBox.critical(self, "Initialization Failed", str(e))
            finally:
                self.dashboard.hide_loading()

    async def _fetch_symbols(self, broker):
        symbols = None

        if hasattr(broker, "fetch_symbol"):
            symbols = await broker.fetch_symbol()
        elif hasattr(broker, "fetch_symbols"):
            symbols = await broker.fetch_symbols()

        if isinstance(symbols, dict):
            instruments = symbols.get("instruments", [])
            normalized = []
            for item in instruments:
                if isinstance(item, dict):
                    name = item.get("name") or item.get("displayName")
                    if name:
                        normalized.append(name)
            symbols = normalized

        if not symbols:
            return list(self.symbols)

        return [s for s in symbols if s]

    def _filter_symbols_for_trading(self, symbols, broker_type, exchange=None):
        if str(exchange or "").lower() == "stellar":
            filtered = []
            for symbol in symbols:
                if not isinstance(symbol, str) or "/" not in symbol:
                    continue

                base, quote = symbol.upper().split("/", 1)
                if not re.fullmatch(r"[A-Z]{2,12}", base):
                    continue
                if not re.fullmatch(r"[A-Z]{2,12}", quote):
                    continue
                filtered.append(f"{base}/{quote}")

            return list(dict.fromkeys(filtered))

        if broker_type != "crypto":
            return list(dict.fromkeys(symbols))

        filtered = []
        for symbol in symbols:
            if not isinstance(symbol, str) or "/" not in symbol:
                continue

            base, quote = symbol.upper().split("/", 1)

            if quote not in self.ALLOWED_CRYPTO_QUOTES:
                continue

            if not re.fullmatch(r"[A-Z]{2,12}", base):
                continue
            if base in self.BANNED_BASE_TOKENS or quote in self.BANNED_BASE_TOKENS:
                continue
            if any(base.endswith(sfx) for sfx in self.BANNED_BASE_SUFFIXES):
                continue

            filtered.append(f"{base}/{quote}")

        return list(dict.fromkeys(filtered))

    async def _select_trade_symbols(self, symbols, broker_type, exchange=None):
        if str(exchange or "").lower() == "stellar":
            prioritized = []
            preferred_quotes = ("USDC", "USDT", "XLM", "EURC")
            account_assets = {
                str(code).upper()
                for code in (getattr(getattr(self, "broker", None), "_account_asset_codes", []) or [])
            }
            unique_symbols = list(dict.fromkeys(str(symbol).upper() for symbol in symbols if symbol))

            def stellar_sort_key(symbol):
                if "/" not in symbol:
                    return (99, 1, 1, symbol)
                base, quote = symbol.split("/", 1)
                quote_rank = preferred_quotes.index(quote) if quote in preferred_quotes else len(preferred_quotes)
                account_rank = 0 if base in account_assets or quote in account_assets else 1
                length_rank = 0 if len(base) <= 5 else 1
                return (quote_rank, account_rank, length_rank, base, quote)

            ordered_symbols = sorted(unique_symbols, key=stellar_sort_key)
            for quote in preferred_quotes:
                for symbol in ordered_symbols:
                    if "/" not in symbol:
                        continue
                    base, current_quote = symbol.split("/", 1)
                    if current_quote != quote:
                        continue
                    normalized = f"{base}/{current_quote}"
                    if normalized not in prioritized:
                        prioritized.append(normalized)
            for symbol in ordered_symbols:
                normalized = str(symbol).upper()
                if normalized not in prioritized:
                    prioritized.append(normalized)

            validated = []
            max_xlm_pairs = 4
            xlm_pairs = 0
            for symbol in prioritized[:40]:
                book = await self._safe_fetch_orderbook(symbol, limit=1)
                bids = (book or {}).get("bids") or []
                asks = (book or {}).get("asks") or []
                if not bids and not asks:
                    continue
                if symbol.endswith("/XLM"):
                    if xlm_pairs >= max_xlm_pairs:
                        continue
                    xlm_pairs += 1
                validated.append(symbol)
                if len(validated) >= 12:
                    break

            return validated if validated else prioritized[:12]

        if broker_type != "crypto":
            return symbols[:50]

        prioritized = self._prioritize_symbols_for_trading(symbols, top_n=30)
        return prioritized if prioritized else symbols[:30]

    def _prioritize_symbols_for_trading(self, symbols, top_n=30):
        def sort_key(symbol):
            if not isinstance(symbol, str) or "/" not in symbol:
                return (99, 99, symbol)

            base, quote = symbol.upper().split("/", 1)
            preferred_rank = self.PREFERRED_BASES.index(base) if base in self.PREFERRED_BASES else len(self.PREFERRED_BASES)
            quote_rank = self.QUOTE_PRIORITY.get(quote, 99)
            return (quote_rank, preferred_rank, base, quote)

        ordered = sorted(dict.fromkeys(symbols), key=sort_key)
        return ordered[:top_n]

    async def _rank_symbols_by_risk_return(self, symbols, max_candidates=120, top_n=30):
        candidates = symbols[:max_candidates]
        semaphore = asyncio.Semaphore(8)
        scored = []

        async def score_symbol(symbol):
            async with semaphore:
                try:
                    candles = await self._safe_fetch_ohlcv(symbol, timeframe="1h", limit=120)
                    if not candles or len(candles) < 40:
                        return

                    closes = []
                    for row in candles:
                        if isinstance(row, (list, tuple)) and len(row) >= 5:
                            closes.append(float(row[4]))

                    if len(closes) < 30:
                        return

                    rets = []
                    for i in range(1, len(closes)):
                        prev = closes[i - 1]
                        cur = closes[i]
                        if prev > 0:
                            rets.append((cur - prev) / prev)

                    if len(rets) < 20:
                        return

                    mean_ret = sum(rets) / len(rets)
                    var = sum((r - mean_ret) ** 2 for r in rets) / max(len(rets) - 1, 1)
                    vol = var ** 0.5
                    if vol <= 1e-9:
                        return

                    total_return = (closes[-1] - closes[0]) / closes[0] if closes[0] else 0.0
                    sharpe_like = mean_ret / vol
                    score = (0.7 * sharpe_like) + (0.3 * total_return)

                    scored.append((symbol, score))

                except Exception:
                    return

        await asyncio.gather(*(score_symbol(sym) for sym in candidates))

        scored.sort(key=lambda x: x[1], reverse=True)
        return [s for s, _ in scored[:top_n]]

    async def _fetch_balances(self, broker):
        balances = await broker.fetch_balance()
        if balances is None:
            return {}
        if isinstance(balances, dict):
            return balances
        return {"raw": balances}

    async def update_balance(self):
        if not self.broker:
            return

        self.balances = await self._fetch_balances(self.broker)
        self.balance = self.balances

    async def initialize_trading(self):
        try:
            if self.terminal:
                await self._cleanup_session(stop_trading=False, close_broker=False)

            self.terminal = Terminal(self)
            self.stack.addWidget(self.terminal)
            self.stack.setCurrentWidget(self.terminal)
            self.terminal.logout_requested.connect(self._on_logout_requested)
            if hasattr(self.terminal, "load_persisted_runtime_data"):
                await self.terminal.load_persisted_runtime_data()

        except Exception as e:
            self.logger.exception("Terminal initialization failed")
            QMessageBox.critical(self, "Initialization Failed", str(e))

    def update_integration_settings(
        self,
        telegram_enabled=None,
        telegram_bot_token=None,
        telegram_chat_id=None,
        openai_api_key=None,
        openai_model=None,
    ):
        if telegram_enabled is not None:
            self.telegram_enabled = bool(telegram_enabled)
        if telegram_bot_token is not None:
            self.telegram_bot_token = str(telegram_bot_token or "").strip()
        if telegram_chat_id is not None:
            self.telegram_chat_id = str(telegram_chat_id or "").strip()
        if openai_api_key is not None:
            self.openai_api_key = str(openai_api_key or "").strip()
        if openai_model is not None:
            self.openai_model = str(openai_model or "gpt-5-mini").strip() or "gpt-5-mini"

        self.settings.setValue("integrations/telegram_enabled", self.telegram_enabled)
        self.settings.setValue("integrations/telegram_bot_token", self.telegram_bot_token)
        self.settings.setValue("integrations/telegram_chat_id", self.telegram_chat_id)
        self.settings.setValue("integrations/openai_api_key", self.openai_api_key)
        self.settings.setValue("integrations/openai_model", self.openai_model)

        asyncio.get_event_loop().create_task(self._restart_telegram_service())

    async def _restart_telegram_service(self):
        if self.telegram_service is not None:
            try:
                await self.telegram_service.stop()
            except Exception as exc:
                self.logger.debug("Telegram restart stop failed: %s", exc)

        self.telegram_service = TelegramService(
            controller=self,
            logger=self.logger,
            bot_token=self.telegram_bot_token,
            chat_id=self.telegram_chat_id,
            enabled=self.telegram_enabled,
        )
        if self.telegram_enabled and self.telegram_bot_token:
            await self.telegram_service.start()

    async def _stop_telegram_service(self):
        if self.telegram_service is None:
            return
        try:
            await self.telegram_service.stop()
        finally:
            self.telegram_service = None

    def set_autotrade_scope(self, scope):
        normalized = str(scope or "all").strip().lower()
        if normalized not in {"all", "selected", "watchlist"}:
            normalized = "all"
        self.autotrade_scope = normalized
        self.settings.setValue("autotrade/scope", normalized)

    def set_autotrade_watchlist(self, symbols):
        normalized = sorted(
            {
                str(symbol).upper().strip()
                for symbol in (symbols or [])
                if str(symbol).strip()
            }
        )
        self.autotrade_watchlist = set(normalized)
        self.settings.setValue("autotrade/watchlist", json.dumps(normalized))

    def _current_autotrade_selected_symbol(self):
        terminal = getattr(self, "terminal", None)
        if terminal is not None:
            current_chart_symbol = None
            if hasattr(terminal, "_current_chart_symbol"):
                try:
                    current_chart_symbol = terminal._current_chart_symbol()
                except Exception:
                    current_chart_symbol = None
            if current_chart_symbol:
                return str(current_chart_symbol).upper().strip()

            picker = getattr(terminal, "symbol_picker", None)
            if picker is not None:
                try:
                    symbol = picker.currentText()
                except Exception:
                    symbol = ""
                if symbol:
                    return str(symbol).upper().strip()

        for symbol in getattr(self, "symbols", []) or []:
            if symbol:
                return str(symbol).upper().strip()
        return ""

    def is_symbol_enabled_for_autotrade(self, symbol):
        normalized = str(symbol or "").upper().strip()
        if not normalized:
            return False

        scope = str(getattr(self, "autotrade_scope", "all") or "all").lower()
        if scope == "selected":
            return normalized == self._current_autotrade_selected_symbol()
        if scope == "watchlist":
            return normalized in set(getattr(self, "autotrade_watchlist", set()) or set())
        return normalized in {str(item).upper().strip() for item in (getattr(self, "symbols", []) or [])}

    def get_active_autotrade_symbols(self):
        available = [
            str(symbol).upper().strip()
            for symbol in (getattr(self, "symbols", []) or [])
            if str(symbol).strip()
        ]
        if not available:
            return []

        scope = str(getattr(self, "autotrade_scope", "all") or "all").lower()
        if scope == "selected":
            selected = self._current_autotrade_selected_symbol()
            return [selected] if selected else []
        if scope == "watchlist":
            watchlist = set(getattr(self, "autotrade_watchlist", set()) or set())
            return [symbol for symbol in available if symbol in watchlist]
        return available

    async def _start_market_stream(self):
        exchange = (self.config.broker.exchange or "").lower() if self.config and self.config.broker else ""

        # Oanda stays on polling as requested.
        if exchange == "oanda":
            self.ws_manager = None
            self.logger.info("Using polling market data for Oanda")
            await self._start_ticker_polling()
            return

        if exchange == "stellar":
            self.ws_manager = None
            self.logger.info("Using polling market data for Stellar Horizon")
            await self._start_ticker_polling()
            return

        try:
            self.ws_bus = EventBus()
            self.ws_bus.subscribe(EventType.MARKET_TICK, self._on_ws_market_tick)

            ws_client = self._build_ws_client(exchange)
            if ws_client is None:
                self.ws_manager = None
                await self._start_ticker_polling()
                return

            self.ws_manager = ws_client
            self._ws_bus_task = self._create_task(self.ws_bus.start(), "ws_event_bus")
            self._ws_task = self._create_task(ws_client.connect(), "ws_connect")

            def _ws_done(t):
                try:
                    exc = t.exception()
                    if exc:
                        self.logger.error("WebSocket stream failed: %s", exc)
                        self._create_task(self._start_ticker_polling(), "ticker_poll_fallback")
                except asyncio.CancelledError:
                    pass

            self._ws_task.add_done_callback(_ws_done)

            if self._ticker_task and not self._ticker_task.done():
                self._ticker_task.cancel()
                self._ticker_task = None

            self.logger.info("WebSocket market data enabled for %s", exchange)

        except Exception as e:
            self.logger.error("WebSocket init failed for %s: %s. Falling back to polling.", exchange, e)
            await self._start_ticker_polling()

    def _build_ws_client(self, exchange):
        symbols = self.symbols[:50]

        if exchange.startswith("binanceus"):
            return BinanceUsWebSocket(symbols=symbols, event_bus=self.ws_bus)

        if exchange == "coinbase":
            products = [s.replace("/", "-") for s in symbols]
            return CoinbaseWebSocket(symbols=products, event_bus=self.ws_bus)

        if exchange == "alpaca":
            return AlpacaWebSocket(
                api_key=self.config.broker.api_key,
                secret_key=self.config.broker.secret,
                symbols=symbols,
                event_bus=self.ws_bus,
            )

        if exchange == "paper":
            return PaperWebSocket(broker=self.broker, symbols=symbols, event_bus=self.ws_bus, interval=1.0)

        return None

    async def _on_ws_market_tick(self, event):
        try:
            data = event.data if hasattr(event, "data") else None
            if not isinstance(data, dict):
                return

            symbol = data.get("symbol")
            if not symbol:
                return

            bid = float(data.get("bid") or data.get("bp") or 0)
            ask = float(data.get("ask") or data.get("ap") or 0)
            last = float(data.get("price") or data.get("last") or 0)

            if bid == 0 and ask == 0:
                bid = last
                ask = last

            self.ticker_stream.update(symbol, data)
            self.ticker_buffer.update(symbol, data)
            self.ticker_signal.emit(symbol, bid, ask)

        except Exception as e:
            self.logger.error("WS tick handling error: %s", e)

    async def _start_ticker_polling(self):
        if self._ticker_task and not self._ticker_task.done():
            self._ticker_task.cancel()

        self._ticker_task = self._create_task(self._ticker_loop(), "ticker_poll")

    async def _ticker_loop(self):
        while self.connected and self.broker is not None:
            try:
                broker_name = str(getattr(getattr(self, "broker", None), "exchange_name", "") or "").lower()
                max_symbols = 10 if broker_name == "stellar" else 30
                sleep_seconds = 4.0 if broker_name == "stellar" else 1.0

                for symbol in self.symbols[:max_symbols]:
                    ticker = await self._safe_fetch_ticker(symbol)
                    if not isinstance(ticker, dict):
                        continue

                    bid = float(ticker.get("bid") or ticker.get("bidPrice") or ticker.get("bp") or 0)
                    ask = float(ticker.get("ask") or ticker.get("askPrice") or ticker.get("ap") or 0)
                    last = float(ticker.get("last") or ticker.get("price") or 0)

                    if bid == 0 and ask == 0:
                        bid = last
                        ask = last

                    self.ticker_stream.update(symbol, ticker)
                    self.ticker_buffer.update(symbol, ticker)

                    self.ticker_signal.emit(symbol, bid, ask)

                await asyncio.sleep(sleep_seconds)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Ticker polling error: %s", e)
                await asyncio.sleep(4.0 if broker_name == "stellar" else 1.0)

    async def _safe_fetch_ticker(self, symbol):
        if not self.broker:
            return None

        if hasattr(self.broker, "fetch_ticker"):
            try:
                tick = await self.broker.fetch_ticker(symbol)
                if isinstance(tick, dict):
                    return tick
            except Exception as exc:
                self.logger.debug("Ticker fetch failed for %s: %s", symbol, exc)

        if hasattr(self.broker, "fetch_price"):
            try:
                price = await self.broker.fetch_price(symbol)
                if price is None:
                    return None
                price = float(price)
                return {
                    "symbol": symbol,
                    "price": price,
                    "bid": price * 0.9998,
                    "ask": price * 1.0002,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            except Exception as exc:
                self.logger.debug("Price fetch failed for %s: %s", symbol, exc)

        return None

    def _active_exchange_code(self):
        broker = getattr(self, "broker", None)
        if broker is not None:
            name = getattr(broker, "exchange_name", None)
            if name:
                return str(name).lower()

        config = getattr(self, "config", None)
        broker_config = getattr(config, "broker", None)
        if broker_config is not None and getattr(broker_config, "exchange", None):
            return str(broker_config.exchange).lower()

        return None

    async def _persist_candles_to_db(self, symbol, timeframe, candles):
        repository = getattr(self, "market_data_repository", None)
        if repository is None or not candles:
            return 0

        exchange = self._active_exchange_code()
        try:
            return await asyncio.to_thread(
                repository.save_candles,
                symbol,
                timeframe,
                candles,
                exchange,
            )
        except Exception as exc:
            self.logger.debug("Candle persistence failed for %s %s: %s", symbol, timeframe, exc)
            return 0

    async def _load_candles_from_db(self, symbol, timeframe="1h", limit=200):
        repository = getattr(self, "market_data_repository", None)
        if repository is None:
            return []

        exchange = self._active_exchange_code()
        try:
            return await asyncio.to_thread(
                repository.get_candles,
                symbol,
                timeframe,
                limit,
                exchange,
            )
        except Exception as exc:
            self.logger.debug("Candle DB load failed for %s %s: %s", symbol, timeframe, exc)
            return []

    async def _load_recent_trades(self, limit=200):
        repository = getattr(self, "trade_repository", None)
        if repository is None:
            return []

        try:
            trades = await asyncio.to_thread(repository.get_trades, limit)
        except Exception as exc:
            self.logger.debug("Trade DB load failed: %s", exc)
            return []

        normalized = []
        for trade in reversed(trades):
            normalized.append(
                {
                    "symbol": getattr(trade, "symbol", ""),
                    "side": getattr(trade, "side", ""),
                    "price": getattr(trade, "price", ""),
                    "size": getattr(trade, "quantity", ""),
                    "order_type": getattr(trade, "order_type", ""),
                    "status": getattr(trade, "status", ""),
                    "order_id": getattr(trade, "order_id", ""),
                    "timestamp": getattr(trade, "timestamp", ""),
                    "pnl": getattr(trade, "pnl", ""),
                }
            )

        return normalized

    def _resolve_history_limit(self, limit=None):
        value = limit if limit is not None else getattr(self, "limit", 1000)
        try:
            resolved = max(100, int(value))
        except Exception:
            resolved = 1000

        exchange = self._active_exchange_code()
        if exchange == "oanda":
            return min(resolved, 5000)
        return min(resolved, 10000)

    def handle_trade_execution(self, trade):
        if not isinstance(trade, dict):
            return

        status = str(trade.get("status") or "").strip().lower().replace("-", "_")
        order_id = str(trade.get("order_id") or "").strip()
        should_record = status in {"filled", "closed"}
        if should_record and order_id:
            if order_id in self._performance_recorded_orders:
                should_record = False
            else:
                self._performance_recorded_orders.add(order_id)

        if should_record and getattr(self, "performance_engine", None) is not None:
            self.performance_engine.record_trade(trade)

        self.trade_signal.emit(trade)
        telegram_service = getattr(self, "telegram_service", None)
        if telegram_service is not None:
            self._create_task(telegram_service.notify_trade(trade), "telegram_trade_notify")

    async def telegram_status_text(self):
        scope = str(getattr(self, "autotrade_scope", "all") or "all").title()
        return (
            "<b>Sopotek Status</b>\n"
            f"Connected: <b>{'YES' if self.connected else 'NO'}</b>\n"
            f"Exchange: <code>{getattr(getattr(self, 'broker', None), 'exchange_name', '-') or '-'}</code>\n"
            f"AI Scope: <b>{scope}</b>\n"
            f"Symbols Loaded: <code>{len(getattr(self, 'symbols', []) or [])}</code>\n"
            f"Market Data: <code>{self.get_market_stream_status()}</code>\n"
            f"Timeframe: <code>{getattr(self, 'time_frame', '1h')}</code>\n"
            f"Balances: {await self.telegram_balances_text(compact=True)}"
        )

    async def telegram_balances_text(self, compact=False):
        balances = getattr(self, "balances", {}) or {}
        if isinstance(balances, dict) and isinstance(balances.get("total"), dict):
            source = balances.get("total") or {}
        elif isinstance(balances, dict):
            source = {
                key: value for key, value in balances.items()
                if isinstance(value, (int, float)) and key not in {"free", "used", "total", "info"}
            }
        else:
            source = {}

        ranked = []
        for asset, value in source.items():
            try:
                numeric = float(value)
            except Exception:
                continue
            if abs(numeric) <= 1e-12:
                continue
            ranked.append((str(asset).upper(), numeric))
        ranked.sort(key=lambda item: abs(item[1]), reverse=True)

        if not ranked:
            return "<code>-</code>" if compact else "<b>Balances</b>\nNo balance data available."

        lines = [f"<code>{asset}: {amount:,.6f}</code>" for asset, amount in ranked[:8]]
        return " | ".join(lines[:4]) if compact else "<b>Balances</b>\n" + "\n".join(lines)

    async def telegram_positions_text(self):
        positions = list(getattr(getattr(self, "terminal", None), "_latest_positions_snapshot", []) or [])
        if not positions:
            return "<b>Positions</b>\nNo open positions."

        lines = ["<b>Positions</b>"]
        for position in positions[:10]:
            lines.append(
                f"<code>{position.get('symbol', '-')}</code> | {position.get('side', '-')} | "
                f"size {position.get('size', position.get('amount', '-'))} | PnL {position.get('pnl', '-')}"
            )
        return "\n".join(lines)

    async def telegram_open_orders_text(self):
        orders = list(getattr(getattr(self, "terminal", None), "_latest_open_orders_snapshot", []) or [])
        if not orders:
            return "<b>Open Orders</b>\nNo open orders."

        lines = ["<b>Open Orders</b>"]
        for order in orders[:10]:
            lines.append(
                f"<code>{order.get('symbol', '-')}</code> | {order.get('side', '-')} | {order.get('status', '-')} | "
                f"qty {order.get('amount', order.get('size', '-'))} | px {order.get('price', '-')}"
            )
        return "\n".join(lines)

    async def capture_telegram_screenshot(self):
        terminal = getattr(self, "terminal", None)
        if terminal is None:
            return None

        output_dir = Path("output") / "telegram"
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / f"terminal_snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        try:
            pixmap = terminal.grab()
            if pixmap is None or pixmap.isNull():
                return None
            pixmap.save(str(path), "PNG")
            return str(path)
        except Exception as exc:
            self.logger.debug("Telegram screenshot capture failed: %s", exc)
            return None

    async def ask_openai_about_app(self, question):
        api_key = str(getattr(self, "openai_api_key", "") or "").strip()
        if not api_key:
            return "OpenAI API key is not configured in Settings -> Integrations."

        context_parts = [
            f"Connected: {self.connected}",
            f"Exchange: {getattr(getattr(self, 'broker', None), 'exchange_name', '-') or '-'}",
            f"AI Scope: {getattr(self, 'autotrade_scope', 'all')}",
            f"Symbols Loaded: {len(getattr(self, 'symbols', []) or [])}",
            f"Timeframe: {getattr(self, 'time_frame', '1h')}",
            f"Balances: {await self.telegram_balances_text(compact=True)}",
        ]
        payload = {
            "model": self.openai_model or "gpt-5-mini",
            "input": [
                {
                    "role": "system",
                    "content": (
                        "You are an assistant inside Sopotek Trading AI. "
                        "Answer briefly and practically using the provided app and market context."
                    ),
                },
                {
                    "role": "user",
                    "content": "\n".join(context_parts) + f"\nQuestion: {question}",
                },
            ],
        }
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
                async with session.post("https://api.openai.com/v1/responses", json=payload, headers=headers) as response:
                    data = await response.json(content_type=None)
                    if response.status >= 400:
                        message = data.get("error", {}).get("message") or str(data)
                        return f"OpenAI request failed: {message}"
        except Exception as exc:
            return f"OpenAI request failed: {exc}"

        text = data.get("output_text")
        if isinstance(text, str) and text.strip():
            return text.strip()

        parts = []
        for item in data.get("output", []) or []:
            for content in item.get("content", []) or []:
                content_text = content.get("text")
                if isinstance(content_text, str) and content_text.strip():
                    parts.append(content_text.strip())
        if parts:
            return "\n".join(parts)
        return "OpenAI returned no text."

    async def _safe_fetch_ohlcv(self, symbol, timeframe="1h", limit=200):
        limit = self._resolve_history_limit(limit)
        # Preferred native broker OHLCV.
        if self.broker and hasattr(self.broker, "fetch_ohlcv"):
            try:
                data = await self.broker.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
                if data:
                    await self._persist_candles_to_db(symbol, timeframe, data)
                    return data
            except Exception:
                pass

        cached_data = await self._load_candles_from_db(symbol, timeframe=timeframe, limit=limit)
        if cached_data:
            return cached_data

        # Fallback: synthesize tiny OHLCV from latest tick.
        tick = await self._safe_fetch_ticker(symbol)
        if not tick:
            return []

        price = float(tick.get("price") or tick.get("last") or 0)
        if price <= 0:
            return []

        now = datetime.now(timezone.utc).isoformat()
        synthetic = [[now, price, price, price, price, 0.0] for _ in range(min(limit, 50))]
        await self._persist_candles_to_db(symbol, timeframe, synthetic)
        return synthetic

    async def _safe_fetch_orderbook(self, symbol, limit=20):
        if self.broker and hasattr(self.broker, "fetch_orderbook"):
            try:
                book = await self.broker.fetch_orderbook(symbol, limit=limit)
                if isinstance(book, dict):
                    return book
            except Exception as exc:
                self.logger.debug("Orderbook fetch failed for %s: %s", symbol, exc)

        tick = await self._safe_fetch_ticker(symbol)
        if not isinstance(tick, dict):
            return {"bids": [], "asks": []}

        bid = float(tick.get("bid") or tick.get("price") or tick.get("last") or 0)
        ask = float(tick.get("ask") or tick.get("price") or tick.get("last") or 0)
        if bid <= 0 and ask <= 0:
            return {"bids": [], "asks": []}

        if bid <= 0:
            bid = ask * 0.999
        if ask <= 0:
            ask = bid * 1.001

        bids = [[bid * (1 - (i * 0.0005)), max(1.0, 10 - i)] for i in range(min(limit, 10))]
        asks = [[ask * (1 + (i * 0.0005)), max(1.0, 10 - i)] for i in range(min(limit, 10))]
        return {"bids": bids, "asks": asks}

    async def request_orderbook(self, symbol, limit=20):
        if not symbol:
            return

        now = time.monotonic()
        broker_name = str(getattr(getattr(self, "broker", None), "exchange_name", "") or "").lower()
        min_interval = 4.0 if broker_name == "stellar" else 1.0

        in_flight = self._orderbook_tasks.get(symbol)
        if in_flight and not in_flight.done():
            return

        cached = self.orderbook_buffer.get(symbol) or {}
        last_requested = self._orderbook_last_request_at.get(symbol, 0.0)
        if cached and (now - last_requested) < min_interval:
            bids = cached.get("bids") or []
            asks = cached.get("asks") or []
            self.orderbook_signal.emit(symbol, bids, asks)
            return

        self._orderbook_last_request_at[symbol] = now
        current_task = asyncio.current_task()
        if current_task is not None:
            self._orderbook_tasks[symbol] = current_task

        try:
            orderbook = await self._safe_fetch_orderbook(symbol, limit=limit)
            bids = orderbook.get("bids") if isinstance(orderbook, dict) else []
            asks = orderbook.get("asks") if isinstance(orderbook, dict) else []

            bids = bids or []
            asks = asks or []

            self.orderbook_buffer.update(symbol, bids, asks)
            self.orderbook_signal.emit(symbol, bids, asks)
        finally:
            active_task = self._orderbook_tasks.get(symbol)
            if active_task is current_task:
                self._orderbook_tasks.pop(symbol, None)

    def publish_ai_signal(self, symbol, signal, candles=None):
        if not symbol or not isinstance(signal, dict):
            return

        side = str(signal.get("side", "hold")).upper()
        confidence = float(signal.get("confidence", 0.0) or 0.0)

        closes = []
        for row in candles or []:
            if isinstance(row, (list, tuple)) and len(row) >= 5:
                try:
                    closes.append(float(row[4]))
                except Exception:
                    continue

        volatility = 0.0
        if len(closes) >= 2:
            returns = []
            for i in range(1, len(closes)):
                prev = closes[i - 1]
                cur = closes[i]
                if prev:
                    returns.append((cur - prev) / prev)
            if returns:
                mean_ret = sum(returns) / len(returns)
                variance = sum((r - mean_ret) ** 2 for r in returns) / max(len(returns) - 1, 1)
                volatility = variance ** 0.5

        regime = "RANGE"
        if side == "BUY":
            regime = "TREND_UP"
        elif side == "SELL":
            regime = "TREND_DOWN"

        payload = {
            "symbol": symbol,
            "signal": side,
            "confidence": confidence,
            "regime": regime,
            "volatility": round(float(volatility), 6),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        self.ai_signal_monitor.emit(payload)

    def publish_strategy_debug(self, symbol, signal, candles=None, features=None):
        if not symbol or not isinstance(signal, dict):
            return

        feature_row = None
        if features is not None:
            try:
                if not features.empty:
                    feature_row = features.iloc[-1]
            except Exception:
                feature_row = None

        index_value = len(candles or []) - 1
        price_value = 0.0
        if candles:
            last_row = candles[-1]
            if isinstance(last_row, (list, tuple)) and len(last_row) >= 5:
                index_value = last_row[0]
                try:
                    price_value = float(last_row[4])
                except Exception:
                    price_value = 0.0

        payload = {
            "symbol": symbol,
            "index": index_value,
            "price": price_value,
            "signal": str(signal.get("side", "hold")).upper(),
            "rsi": round(float(feature_row["rsi"]), 4) if feature_row is not None and "rsi" in feature_row else 0.0,
            "ema_fast": round(float(feature_row["ema_fast"]), 6) if feature_row is not None and "ema_fast" in feature_row else 0.0,
            "ema_slow": round(float(feature_row["ema_slow"]), 6) if feature_row is not None and "ema_slow" in feature_row else 0.0,
            "ml_probability": round(float(signal.get("confidence", 0.0) or 0.0), 4),
            "reason": str(signal.get("reason", "")),
        }

        self.strategy_debug_signal.emit(payload)

    async def request_candle_data(self, symbol, timeframe="1h", limit=None):
        if not symbol:
            return

        limit = self._resolve_history_limit(limit)
        candles = await self._safe_fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if not candles:
            return

        df = pd.DataFrame(candles)
        if df.shape[1] >= 6:
            df = df.iloc[:, :6]
            df.columns = ["timestamp", "open", "high", "low", "close", "volume"]

        # Keep nested cache for symbol/timeframe lookups.
        symbol_cache = self.candle_buffers.setdefault(symbol, {})
        symbol_cache[timeframe] = df

        # Keep legacy buffer path updated with latest close candle rows.
        for _, row in df.tail(200).iterrows():
            self.candle_buffer.update(
                symbol,
                {
                    "timestamp": row["timestamp"],
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row["volume"]),
                },
            )

        self.candle_signal.emit(symbol, df)

    async def _warmup_visible_candles(self):
        # Preload a very small working set so startup stays responsive.
        warm_symbols = list(dict.fromkeys(self.symbols[:2]))
        tasks = [
            self.request_candle_data(symbol=s, timeframe=self.time_frame, limit=180)
            for s in warm_symbols
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _cleanup_session(self, stop_trading=True, close_broker=False):
        try:
            await self._stop_telegram_service()

            if self._ticker_task and not self._ticker_task.done():
                self._ticker_task.cancel()
            self._ticker_task = None

            if self._ws_task and not self._ws_task.done():
                self._ws_task.cancel()
            self._ws_task = None

            if self._ws_bus_task and not self._ws_bus_task.done():
                self._ws_bus_task.cancel()
            self._ws_bus_task = None
            self.ws_bus = None
            self.ws_manager = None

            if stop_trading and self.trading_system:
                await self.trading_system.stop()
                self.trading_system = None

            if self.terminal:
                self.stack.removeWidget(self.terminal)
                self.terminal.deleteLater()
                self.terminal = None

            if close_broker and self.broker:
                await self.broker.close()
                self.broker = None

        except Exception as e:
            self.logger.error("Cleanup error: %s", e)

    def get_market_stream_status(self):
        if self._ws_task and not self._ws_task.done():
            return "Running"
        if self._ticker_task and not self._ticker_task.done():
            return "Polling"
        return "Stopped"

    async def logout(self):
        try:
            await self._cleanup_session(stop_trading=True, close_broker=True)

            self.connected = False
            self.connection_signal.emit("disconnected")

        finally:
            self.stack.setCurrentWidget(self.dashboard)
            self.dashboard.setEnabled(True)
            self.dashboard.connect_button.setText("CONNECT")

    async def get_price(self, symbol):
        tick = await self._safe_fetch_ticker(symbol)
        if not tick:
            raise RuntimeError("Price unavailable")
        return float(tick.get("price") or tick.get("last") or 0)
