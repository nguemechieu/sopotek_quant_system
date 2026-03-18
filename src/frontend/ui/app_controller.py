from frontend.console.system_console import SystemConsole
import asyncio
import json
import logging
import os
import re
import sys
import tempfile
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import pandas as pd
from PySide6.QtCore import QSettings, QTimer, Signal
from PySide6.QtWidgets import (
    QApplication,
    QDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QStackedWidget,
    QTextBrowser,
    QVBoxLayout,
)

from broker.broker_factory import BrokerFactory
from broker.market_venues import normalize_market_venue, supported_market_venues_for_profile
from broker.rate_limiter import RateLimiter
from core.sopotek_trading import SopotekTrading
from event_bus.event_bus import EventBus
from event_bus.event_types import EventType
from frontend.ui.dashboard import Dashboard
from frontend.ui.i18n import DEFAULT_LANGUAGE, normalize_language_code, translate
from frontend.ui.services.screenshot_service import capture_widget_to_output, sanitize_screenshot_fragment
from frontend.ui.terminal import Terminal
from integrations.news_service import NewsService
from integrations.telegram_service import TelegramService
from integrations.voice_service import VoiceService
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
from licensing.license_manager import LicenseManager
from storage.agent_decision_repository import AgentDecisionRepository
from storage.database import configure_database, get_database_url, init_database
from storage.equity_repository import EquitySnapshotRepository
from storage.market_data_repository import MarketDataRepository
from storage.trade_repository import TradeRepository
from strategy.strategy import Strategy
from frontend.console.system_console import SystemConsole


try:
    import winsound
except Exception:  # pragma: no cover - non-Windows fallback
    winsound = None


def _bounded_window_extent(requested, available, *, margin=24, minimum=640):
    try:
        requested_value = int(requested)
    except Exception:
        requested_value = int(minimum)

    try:
        available_value = int(available)
    except Exception:
        available_value = requested_value

    usable = max(320, available_value - max(0, int(margin)))
    bounded_minimum = min(max(320, int(minimum)), usable)
    bounded_size = max(bounded_minimum, min(requested_value, usable))
    return bounded_size, bounded_minimum




class AppController(QMainWindow):
    MAX_HISTORY_LIMIT = 50000
    FOREX_STANDARD_LOT_UNITS = 100000.0
    OPENAI_TTS_MODEL = "gpt-4o-mini-tts"
    OPENAI_TTS_VOICES = [
        "alloy",
        "ash",
        "ballad",
        "coral",
        "echo",
        "fable",
        "nova",
        "onyx",
        "sage",
        "shimmer",
        "verse",
    ]

    symbols_signal = Signal(str, list)
    candle_signal = Signal(str, object)
    equity_signal = Signal(float)

    trade_signal = Signal(dict)
    ticker_signal = Signal(str, float, float)
    connection_signal = Signal(str)
    orderbook_signal = Signal(str, list, list)
    recent_trades_signal = Signal(str, list)
    news_signal = Signal(str, list)
    ai_signal_monitor = Signal(dict)

    strategy_debug_signal = Signal(dict)
    agent_runtime_signal = Signal(dict)
    autotrade_toggle = Signal(bool)
    license_changed = Signal(dict)

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
    FOREX_SYMBOL_QUOTES = {
        "AED", "AUD", "CAD", "CHF", "CNH", "CZK", "DKK", "EUR", "GBP", "HKD",
        "HUF", "JPY", "MXN", "NOK", "NZD", "PLN", "SEK", "SGD", "THB", "TRY",
        "USD", "ZAR",
    }

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

        self.license_manager = LicenseManager(self.settings, logger=self.logger)
        self.license_status = self.license_manager.status()

        self.broker_manager = BrokerManager()
        self.rate_limiter = RateLimiter()

        self.broker = None
        self.trading_system = None
        self.terminal = None
        self.telegram_service = None
        self.behavior_guard = None
        self.portfolio_allocator = None
        self.institutional_risk_engine = None
        self.quant_allocation_snapshot = {}
        self.quant_risk_snapshot = {}
        self.health_check_report = []
        self.health_check_summary = "Not run"
        self._live_agent_decision_events = {}
        self._live_agent_runtime_feed = []
        self.risk_profile_name = str(self.settings.value("risk/profile_name", "Balanced") or "Balanced").strip() or "Balanced"
        self.max_portfolio_risk = self.settings.value("risk/max_portfolio_risk", 0.10) or 0.10
        self.max_risk_per_trade = self.settings.value("risk/max_risk_per_trade", 0.02) or 0.02
        self.max_position_size_pct = self.settings.value("risk/max_position_size_pct", 0.10) or 0.10
        self.max_gross_exposure_pct = self.settings.value("risk/max_gross_exposure_pct", 2.0) or 2.0
        self.hedging_enabled = str(
            self.settings.value("trading/hedging_enabled", "true")
        ).strip().lower() in {"1", "true", "yes", "on"}
        self.margin_closeout_guard_enabled = str(
            self.settings.value("risk/margin_closeout_guard_enabled", "true")
        ).strip().lower() in {"1", "true", "yes", "on"}
        self.max_margin_closeout_pct = max(0.01,
            min(1.0, float(self.settings.value("risk/max_margin_closeout_pct", 0.50) )))
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
        self.voice_provider = str(self.settings.value("integrations/voice_provider", "windows") or "windows").strip().lower()
        if self.voice_provider not in {"windows", "google"}:
            self.voice_provider = "windows"
        self.voice_output_provider = str(
            self.settings.value("integrations/voice_output_provider", "windows") or "windows"
        ).strip().lower()
        if self.voice_output_provider not in {"windows", "openai"}:
            self.voice_output_provider = "windows"
        legacy_voice_name = str(self.settings.value("integrations/voice_name", "") or "").strip()
        self.voice_windows_name = str(
            self.settings.value("integrations/voice_windows_name", legacy_voice_name) or legacy_voice_name
        ).strip()
        self.voice_openai_name = str(
            self.settings.value("integrations/voice_openai_name", "alloy") or "alloy"
        ).strip().lower() or "alloy"
        if self.voice_openai_name not in self.OPENAI_TTS_VOICES:
            self.voice_openai_name = "alloy"
        self.voice_name = self._current_market_chat_voice_name()
        self.news_enabled = str(self.settings.value("integrations/news_enabled", "true")).lower() in {"1", "true", "yes", "on"}
        self.news_autotrade_enabled = str(self.settings.value("integrations/news_autotrade_enabled", "false")).lower() in {"1", "true", "yes", "on"}
        self.news_draw_on_chart = str(self.settings.value("integrations/news_draw_on_chart", "true")).lower() in {"1", "true", "yes", "on"}
        self.news_feed_url = str(self.settings.value("integrations/news_feed_url", NewsService.DEFAULT_FEED_URL) or NewsService.DEFAULT_FEED_URL).strip()
        self.market_trade_preference = normalize_market_venue(self.settings.value("trading/market_type", "auto"))
        self.database_mode = str(self.settings.value("storage/database_mode", "local") or "local").strip().lower()
        if self.database_mode not in {"local", "remote"}:
            self.database_mode = "local"
        self.database_url = str(self.settings.value("storage/database_url", "") or "").strip()
        self.database_connection_url = ""
        self.autotrade_scope = str(self.settings.value("autotrade/scope", "all") or "all").strip().lower()
        if self.autotrade_scope not in {"all", "selected", "watchlist"}:
            self.autotrade_scope = "all"
        self._market_data_shortfall_notices = {}
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
        self.multi_strategy_enabled = str(
            self.settings.value("strategy/multi_strategy_enabled", "true")
        ).strip().lower() in {"1", "true", "yes", "on"}
        self.max_symbol_strategies = max(
            1,
            min(10, int(self.settings.value("strategy/max_symbol_strategies", 3) or 3)),
        )
        self.symbol_strategy_assignments = self._load_strategy_symbol_payload("strategy/symbol_assignments")
        self.symbol_strategy_rankings = self._load_strategy_symbol_payload("strategy/symbol_rankings")
        self.symbol_strategy_locks = self._load_strategy_symbol_lock_payload(
            "strategy/symbol_assignment_locks",
            fallback_symbols=list(self.symbol_strategy_assignments.keys()),
        )
        self.strategy_auto_assignment_enabled = str(
            self.settings.value("strategy/auto_assignment_enabled", "true")
        ).strip().lower() in {"1", "true", "yes", "on"}
        self.strategy_auto_assignment_ready = not self.strategy_auto_assignment_enabled
        self.strategy_auto_assignment_in_progress = False
        self.strategy_auto_assignment_progress = {
            "completed": 0,
            "total": 0,
            "current_symbol": "",
            "timeframe": self.time_frame,
            "updated_at": "",
            "message": "Waiting to scan symbols.",
            "failed_symbols": [],
        }
        self._strategy_auto_assignment_task = None

        self.portfolio = None
        self.ai_signal = None
        self.balances = {}
        self.balance = {}

        self.ticker_stream = TickerStream()
        self._performance_recorded_orders = set()
        self.news_service = NewsService(
            logger=self.logger,
            enabled=self.news_enabled,
            feed_url_template=self.news_feed_url,
        )
        self.voice_service = VoiceService(
            logger=self.logger,
            voice_name=self.voice_name,
            recognition_provider=self.voice_provider,
        )
        self._news_cache = {}
        self._news_inflight = {}

        self.limit = self.MAX_HISTORY_LIMIT
        self.runtime_history_limit = int(getattr(SopotekTrading, "MAX_RUNTIME_ANALYSIS_BARS", 500) or 500)
        self.initial_capital = 10000

        self.candle_buffer = CandleBuffer(max_length=self.limit)
        self.candle_buffers = {}
        self.orderbook_buffer = OrderBookBuffer()
        self.ticker_buffer = TickerBuffer(max_length=self.limit)
        self._orderbook_tasks = {}
        self._orderbook_last_request_at = {}
        self._recent_trades_cache = {}
        self._recent_trades_tasks = {}
        self._recent_trades_last_request_at = {}

        self.symbols = ["BTC/USDT", "ETH/USDT", "XLM/USDT"]

        self.connected = False
        self.config = None
        self._session_closing = False

        try:
            self._setup_paths()
            self._setup_data()
            self._setup_ui(self.controller)
            self.setWindowTitle(self.tr("app.window_title"))
        except Exception:
            traceback.print_exc()

    def tr(self, key, **kwargs):
        return translate(self.language_code, key, **kwargs)

    def refresh_license_status(self):
        self.license_status = self.license_manager.status()
        snapshot = dict(self.license_status)
        self.license_changed.emit(snapshot)
        return snapshot

    def get_license_status(self):
        self.license_status = self.license_manager.status()
        return dict(self.license_status)

    def license_allows(self, feature):
        return bool(self.license_manager.allows_feature(feature))

    def activate_license_key(self, key):
        success, message, _status = self.license_manager.activate_key(key)
        return success, message, self.refresh_license_status()

    def clear_license(self):
        self.license_manager.clear_paid_license()
        return self.refresh_license_status()

    def show_license_dialog(self, parent=None):
        parent_widget = parent or self.terminal or self.dashboard or self
        dialog = QDialog(parent_widget)
        dialog.setWindowTitle("License")
        dialog.resize(620, 460)

        layout = QVBoxLayout(dialog)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(12)

        status_label = QLabel()
        status_label.setWordWrap(True)
        status_label.setStyleSheet(
            "color: #d9e6f7; background-color: #101a2d; border: 1px solid #20324d; "
            "border-radius: 12px; padding: 12px; font-size: 13px; font-weight: 600;"
        )
        layout.addWidget(status_label)

        details = QTextBrowser()
        details.setStyleSheet(
            "QTextBrowser { background-color: #101a2d; color: #d8e6ff; border: 1px solid #20324d; border-radius: 12px; padding: 12px; }"
        )
        layout.addWidget(details, 1)

        key_input = QLineEdit()
        key_input.setPlaceholderText("Enter license key, for example SOPOTEK-SUB-12M-TEAM-001")
        layout.addWidget(key_input)

        button_row = QHBoxLayout()
        activate_button = QPushButton("Activate License")
        community_button = QPushButton("Use Community Mode")
        close_button = QPushButton("Close")
        button_row.addWidget(activate_button)
        button_row.addWidget(community_button)
        button_row.addStretch(1)
        button_row.addWidget(close_button)
        layout.addLayout(button_row)

        def _refresh_dialog():
            status = self.get_license_status()
            status_label.setText(
                f"{status.get('plan_name', 'License')} | {status.get('summary', 'Unknown status')}"
            )
            example_html = (
                "<h3>License Types</h3>"
                "<ul>"
                "<li><b>Trial:</b> starts automatically on first run and unlocks live trading for a limited period.</li>"
                "<li><b>Subscription:</b> activate keys like <code>SOPOTEK-SUB-12M-TEAM-001</code>.</li>"
                "<li><b>Full License:</b> activate keys like <code>SOPOTEK-FULL-LIFETIME-001</code>.</li>"
                "<li><b>Community:</b> paper trading, charts, analysis, and research remain available.</li>"
                "</ul>"
                f"<p><b>Current status:</b> {status.get('description', '-')}</p>"
                "<p><b>Notes:</b> This is the local licensing foundation. Payment, customer portal, and online validation can be added later without rewriting the UI flow.</p>"
            )
            details.setHtml(example_html)

        def _activate():
            success, message, _status = self.activate_license_key(key_input.text())
            if success:
                QMessageBox.information(dialog, "License Activated", message)
                key_input.clear()
            else:
                QMessageBox.warning(dialog, "License Error", message)
            _refresh_dialog()

        def _community():
            self.clear_license()
            QMessageBox.information(
                dialog,
                "Community Mode",
                "Paid license data was cleared. Community mode remains available for paper trading and analysis.",
            )
            _refresh_dialog()

        activate_button.clicked.connect(_activate)
        community_button.clicked.connect(_community)
        close_button.clicked.connect(dialog.accept)
        _refresh_dialog()
        dialog.exec()

    def _friendly_initialization_error(self, exc):
        message = str(exc or "").strip()
        lowered = message.lower()

        if "could not contact dns servers" in lowered or "dns lookup failed" in lowered:
            return (
                "Broker connection failed because DNS resolution is not working on this machine right now. "
                "Check your internet connection, DNS settings, VPN, proxy, or firewall, then try again."
            )

        if "cannot connect to host" in lowered:
            return (
                "Broker connection failed before login completed. "
                "Check your internet connection, VPN, proxy, or firewall, then try again.\n\n"
                f"Details: {message}"
            )

        if "binance.com is not available for us customers" in lowered:
            return "Binance.com is not available for US customers in Sopotek. Choose Binance US or switch the customer region to Outside US."

        if "binance us is only available for us customers" in lowered:
            return "Binance US is only available for US customers in Sopotek. Choose Binance for non-US customers or switch the customer region to US."

        if "api-key format invalid" in lowered or "\"code\":-2014" in lowered or "code': -2014" in lowered:
            return (
                "The broker rejected the API key format. For Binance US, use a Binance US API key and secret pair, "
                "not Binance.com credentials. Also make sure the key and secret were pasted without spaces or line breaks."
            )

        if "coinbase" in lowered and "passphrase" in lowered:
            return (
                "Coinbase Advanced Trade in Sopotek uses the API key name and private key."
            )

        if "coinbase" in lowered and any(
            token in lowered
            for token in ("unauthorized", "authentication", "invalid signature", "forbidden", "401")
        ):
            return (
                "Coinbase authentication failed. In Sopotek's Coinbase mode, use Coinbase Advanced Trade credentials: "
                "put the API key name like organizations/.../apiKeys/... in the first field and the privateKey PEM in "
                "the second field. If you pasted the private key from JSON, keep the full BEGIN/END block or the "
                "escaped \\n form."
            )

        return message or "Unknown initialization error"

    def _broker_is_connected(self, broker):
        if broker is None:
            return False
        for attr in ("_connected", "connected", "is_connected"):
            value = getattr(broker, attr, None)
            if callable(value):
                try:
                    value = value()
                except Exception:
                    continue
            if isinstance(value, bool):
                return value
        return False

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
        self.configure_storage_database(
            database_mode=getattr(self, "database_mode", "local"),
            database_url=getattr(self, "database_url", ""),
            persist=False,
            raise_on_error=False,
        )
        self.historical_data = pd.DataFrame(
            columns=["symbol", "timestamp", "open", "high", "low", "close", "volume"]
        )
        self.performance_engine = PerformanceEngine()
        self._restore_performance_state()

    def _performance_trade_payload_from_record(self, trade):
        return {
            "symbol": getattr(trade, "symbol", ""),
            "side": getattr(trade, "side", ""),
            "source": getattr(trade, "source", ""),
            "price": getattr(trade, "price", ""),
            "size": getattr(trade, "quantity", ""),
            "order_type": getattr(trade, "order_type", ""),
            "status": getattr(trade, "status", ""),
            "order_id": getattr(trade, "order_id", ""),
            "timestamp": getattr(trade, "timestamp", ""),
            "pnl": getattr(trade, "pnl", ""),
            "strategy_name": getattr(trade, "strategy_name", ""),
            "reason": getattr(trade, "reason", ""),
            "confidence": getattr(trade, "confidence", ""),
            "expected_price": getattr(trade, "expected_price", ""),
            "spread_bps": getattr(trade, "spread_bps", ""),
            "slippage_bps": getattr(trade, "slippage_bps", ""),
            "fee": getattr(trade, "fee", ""),
        }

    def _load_persisted_performance_history(self):
        settings = getattr(self, "settings", None)
        if settings is None:
            return []

        raw_value = settings.value("performance/equity_history", "[]")
        try:
            payload = json.loads(raw_value or "[]")
        except Exception:
            payload = raw_value if isinstance(raw_value, list) else []

        history = []
        for item in list(payload or [])[-2000:]:
            timestamp = None
            value = item
            if isinstance(item, dict):
                timestamp = item.get("timestamp")
                value = item.get("equity", item.get("value"))

            try:
                numeric = float(value)
            except Exception:
                continue
            if not pd.notna(numeric):
                continue

            if timestamp in (None, ""):
                history.append(numeric)
            else:
                history.append({"equity": numeric, "timestamp": timestamp})
        return history

    def _persist_performance_history(self):
        perf = getattr(self, "performance_engine", None)
        settings = getattr(self, "settings", None)
        if perf is None or settings is None:
            return

        equity_values = list(getattr(perf, "equity_curve", []) or [])[-2000:]
        equity_timestamps = list(getattr(perf, "equity_timestamps", []) or [])[-len(equity_values):]

        history = []
        for index, value in enumerate(equity_values):
            try:
                numeric = float(value)
            except Exception:
                continue
            if not pd.notna(numeric):
                continue

            timestamp = equity_timestamps[index] if index < len(equity_timestamps) else None
            if timestamp in (None, ""):
                history.append(numeric)
            else:
                try:
                    history.append({"equity": numeric, "timestamp": float(timestamp)})
                except Exception:
                    history.append({"equity": numeric, "timestamp": timestamp})
        settings.setValue("performance/equity_history", json.dumps(history))

    def _load_persisted_equity_history_from_repository(self, limit=2000):
        repository = getattr(self, "equity_repository", None)
        if repository is None or not hasattr(repository, "get_snapshots"):
            return []

        exchange = self._active_exchange_code() if hasattr(self, "_active_exchange_code") else None
        account_label = self.current_account_label() if hasattr(self, "current_account_label") else None
        if str(account_label or "").strip().lower() == "not set":
            account_label = None

        try:
            snapshots = repository.get_snapshots(limit=limit, exchange=exchange, account_label=account_label)
        except TypeError:
            snapshots = repository.get_snapshots(limit=limit)
        except Exception:
            self.logger.debug("Unable to restore equity snapshot ledger", exc_info=True)
            return []

        history = []
        for item in reversed(list(snapshots or [])):
            equity = getattr(item, "equity", None)
            if equity in (None, ""):
                continue
            timestamp = getattr(item, "timestamp", None)
            if isinstance(timestamp, datetime):
                timestamp = timestamp.replace(tzinfo=timezone.utc).timestamp() if timestamp.tzinfo is None else timestamp.astimezone(timezone.utc).timestamp()
            history.append({"equity": float(equity), "timestamp": timestamp})
        return history

    def _persist_equity_snapshot(self, equity, balances=None):
        repository = getattr(self, "equity_repository", None)
        if repository is None or not hasattr(repository, "save_snapshot"):
            return None

        balance_payload = balances if isinstance(balances, dict) else getattr(self, "balances", {}) or {}
        exchange = self._active_exchange_code() if hasattr(self, "_active_exchange_code") else None
        account_label = self.current_account_label() if hasattr(self, "current_account_label") else None
        if str(account_label or "").strip().lower() == "not set":
            account_label = None

        balance_value = self._balance_metric_value(
            balance_payload,
            "balance",
            "cash",
            "equity",
            "nav",
            "net_liquidation",
            "account_value",
        )
        free_margin = self._safe_balance_metric(balance_payload.get("free")) if isinstance(balance_payload, dict) else None
        used_margin = self._safe_balance_metric(balance_payload.get("used")) if isinstance(balance_payload, dict) else None

        try:
            return repository.save_snapshot(
                equity=float(equity),
                exchange=exchange,
                account_label=account_label,
                balance=balance_value,
                free_margin=free_margin,
                used_margin=used_margin,
                payload=balance_payload,
            )
        except Exception:
            self.logger.debug("Unable to persist equity snapshot ledger", exc_info=True)
            return None

    def _restore_performance_state(self):
        perf = getattr(self, "performance_engine", None)
        if perf is None:
            return

        equity_history = self._load_persisted_equity_history_from_repository(limit=2000)
        if not equity_history:
            equity_history = self._load_persisted_performance_history()

        if hasattr(perf, "load_equity_history"):
            perf.load_equity_history(equity_history)

        repository = getattr(self, "trade_repository", None)
        if repository is None or not hasattr(repository, "get_trades"):
            return

        try:
            stored = list(reversed(repository.get_trades(limit=500) or []))
        except Exception:
            self.logger.debug("Unable to restore persisted trade activity for performance analysis", exc_info=True)
            return

        trades = [self._performance_trade_payload_from_record(item) for item in stored]
        if hasattr(perf, "load_trades"):
            perf.load_trades(trades)
        else:
            perf.trades = list(trades)

    def _agent_decision_record_to_payload(self, item):
        payload = {}
        raw_payload = getattr(item, "payload_json", None)
        if raw_payload:
            try:
                payload = json.loads(raw_payload)
            except Exception:
                payload = {"raw": str(raw_payload)}

        timestamp = getattr(item, "timestamp", None)
        timestamp_value = None
        timestamp_label = ""
        if isinstance(timestamp, datetime):
            normalized = timestamp.replace(tzinfo=timezone.utc) if timestamp.tzinfo is None else timestamp.astimezone(timezone.utc)
            timestamp_value = normalized.timestamp()
            timestamp_label = normalized.strftime("%Y-%m-%d %H:%M:%S UTC")
        elif timestamp not in (None, ""):
            timestamp_label = str(timestamp)

        return {
            "id": getattr(item, "id", None),
            "decision_id": str(getattr(item, "decision_id", "") or "").strip(),
            "exchange": getattr(item, "exchange", None),
            "account_label": getattr(item, "account_label", None),
            "symbol": str(getattr(item, "symbol", "") or "").strip().upper(),
            "agent_name": str(getattr(item, "agent_name", "") or "").strip(),
            "stage": str(getattr(item, "stage", "") or "").strip(),
            "strategy_name": str(getattr(item, "strategy_name", "") or payload.get("strategy_name") or "").strip(),
            "timeframe": str(getattr(item, "timeframe", "") or payload.get("timeframe") or "").strip(),
            "side": str(getattr(item, "side", "") or payload.get("side") or "").strip().lower(),
            "confidence": getattr(item, "confidence", None),
            "approved": getattr(item, "approved", None),
            "reason": str(getattr(item, "reason", "") or payload.get("reason") or "").strip(),
            "timestamp": timestamp_value,
            "timestamp_label": timestamp_label,
            "payload": payload,
        }


    def _normalize_live_agent_timestamp(self, timestamp):
        if isinstance(timestamp, datetime):
            normalized = timestamp.replace(tzinfo=timezone.utc) if timestamp.tzinfo is None else timestamp.astimezone(timezone.utc)
            return normalized.timestamp(), normalized.strftime("%Y-%m-%d %H:%M:%S UTC")
        text = str(timestamp or "").strip()
        if not text:
            return None, ""
        try:
            normalized = datetime.fromisoformat(text.replace("Z", "+00:00"))
            normalized = normalized.replace(tzinfo=timezone.utc) if normalized.tzinfo is None else normalized.astimezone(timezone.utc)
            return normalized.timestamp(), normalized.strftime("%Y-%m-%d %H:%M:%S UTC")
        except Exception:
            return None, text

    def _live_agent_memory_event_to_payload(self, event):
        if not isinstance(event, dict):
            return {}
        symbol = self._normalize_strategy_symbol_key(event.get("symbol"))
        if not symbol:
            return {}
        payload = dict(event.get("payload") or {})
        timestamp_value, timestamp_label = self._normalize_live_agent_timestamp(event.get("timestamp"))
        return {
            "decision_id": str(event.get("decision_id") or "").strip(),
            "symbol": symbol,
            "agent_name": str(event.get("agent") or "").strip(),
            "stage": str(event.get("stage") or "").strip(),
            "strategy_name": str(payload.get("strategy_name") or "").strip(),
            "timeframe": str(payload.get("timeframe") or "").strip(),
            "side": str(payload.get("side") or "").strip().lower(),
            "confidence": payload.get("confidence"),
            "approved": payload.get("approved"),
            "reason": str(payload.get("reason") or "").strip(),
            "timestamp": timestamp_value,
            "timestamp_label": timestamp_label,
            "payload": payload,
            "source": "live",
        }

    def _append_live_agent_decision_event(self, payload):
        if not isinstance(payload, dict):
            return []
        symbol = self._normalize_strategy_symbol_key(payload.get("symbol"))
        if not symbol:
            return []
        events = self._live_agent_decision_events.setdefault(symbol, [])
        events.append(dict(payload, symbol=symbol))
        if len(events) > 250:
            del events[:-250]
        return list(events)

    def _append_live_agent_runtime_feed(self, payload):
        if not isinstance(payload, dict):
            return {}

        row = dict(payload)
        symbol = self._normalize_strategy_symbol_key(row.get("symbol"))
        if symbol:
            row["symbol"] = symbol

        row["kind"] = str(row.get("kind") or "runtime").strip().lower() or "runtime"
        row["message"] = str(row.get("message") or row.get("reason") or "").strip()
        row["stage"] = str(row.get("stage") or "").strip()
        row["agent_name"] = str(row.get("agent_name") or "").strip()
        row["event_type"] = str(row.get("event_type") or "").strip()
        row["strategy_name"] = str(row.get("strategy_name") or "").strip()
        row["timeframe"] = str(row.get("timeframe") or "").strip()
        row["decision_id"] = str(row.get("decision_id") or "").strip()

        timestamp_value, timestamp_label = self._normalize_live_agent_timestamp(
            row.get("timestamp") if row.get("timestamp") not in (None, "") else datetime.now(timezone.utc)
        )
        row["timestamp"] = timestamp_value
        row["timestamp_label"] = str(row.get("timestamp_label") or timestamp_label or "").strip()
        if not row["timestamp_label"] and timestamp_value is not None:
            row["timestamp_label"] = datetime.fromtimestamp(timestamp_value, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        feed = getattr(self, "_live_agent_runtime_feed", None)
        if not isinstance(feed, list):
            feed = []
            self._live_agent_runtime_feed = feed
        feed.append(row)
        if len(feed) > 500:
            del feed[:-500]
        return dict(row)

    def _latest_live_agent_decision_chain_for_symbol(self, symbol, limit=12):
        normalized_symbol = self._normalize_strategy_symbol_key(symbol)
        if not normalized_symbol:
            return []
        rows = list((getattr(self, "_live_agent_decision_events", {}) or {}).get(normalized_symbol, []) or [])
        if not rows:
            return []
        latest_decision_id = str(rows[-1].get("decision_id") or "").strip()
        if latest_decision_id:
            rows = [row for row in rows if str(row.get("decision_id") or "").strip() == latest_decision_id]
        return [dict(row) for row in rows[-max(1, int(limit or 12)):]]

    def live_agent_runtime_feed(self, limit=200, symbol=None, kinds=None):
        rows = list(getattr(self, "_live_agent_runtime_feed", []) or [])

        normalized_symbol = self._normalize_strategy_symbol_key(symbol)
        if normalized_symbol:
            rows = [
                dict(row)
                for row in rows
                if self._normalize_strategy_symbol_key((row or {}).get("symbol")) == normalized_symbol
            ]
        else:
            rows = [dict(row) for row in rows]

        if kinds:
            allowed_kinds = {
                str(kind or "").strip().lower()
                for kind in (kinds if isinstance(kinds, (list, tuple, set)) else [kinds])
                if str(kind or "").strip()
            }
            if allowed_kinds:
                rows = [row for row in rows if str(row.get("kind") or "").strip().lower() in allowed_kinds]

        try:
            limit_value = max(1, int(limit or 200))
        except Exception:
            limit_value = 200
        return list(reversed(rows[-limit_value:]))

    def _emit_agent_runtime_signal(self, payload):
        normalized_payload = self._append_live_agent_runtime_feed(payload)
        signal = getattr(self, "agent_runtime_signal", None)
        if signal is not None:
            try:
                signal.emit(dict(normalized_payload or payload or {}))
            except Exception:
                self.logger.debug("Unable to emit agent runtime signal", exc_info=True)

    def _handle_live_agent_memory_event(self, event):
        payload = self._live_agent_memory_event_to_payload(event)
        if not payload:
            return {}
        self._append_live_agent_decision_event(payload)
        runtime_payload = dict(payload)
        runtime_payload["kind"] = "memory"
        runtime_payload["message"] = (
            f"{payload.get('agent_name') or 'Agent'} {payload.get('stage') or 'updated'}"
            f" for {payload.get('symbol') or 'symbol'}"
        )
        if payload.get("reason"):
            runtime_payload["message"] = f"{runtime_payload['message']} | {payload.get('reason')}"
        self._emit_agent_runtime_signal(runtime_payload)
        return payload

    def _agent_runtime_bus_message(self, event_type, data):
        payload = dict(data or {})
        signal_payload = dict(payload.get("signal") or {}) if isinstance(payload.get("signal"), dict) else {}
        review_payload = dict(payload.get("trade_review") or {}) if isinstance(payload.get("trade_review"), dict) else {}
        symbol = self._normalize_strategy_symbol_key(payload.get("symbol"))
        strategy_name = str(signal_payload.get("strategy_name") or review_payload.get("strategy_name") or payload.get("strategy_name") or "").strip()
        timeframe = str(payload.get("timeframe") or review_payload.get("timeframe") or payload.get("timeframe") or "").strip()
        side = str(signal_payload.get("side") or review_payload.get("side") or payload.get("side") or "").strip().upper()
        reason = str(review_payload.get("reason") or signal_payload.get("reason") or payload.get("reason") or "").strip()
        if event_type == EventType.SIGNAL:
            detail = f"{side or 'HOLD'} via {strategy_name or 'strategy'}"
            if timeframe:
                detail = f"{detail} ({timeframe})"
            return f"Signal selected for {symbol}: {detail}."
        if event_type == EventType.RISK_APPROVED:
            return f"Risk approved {side or 'trade'} for {symbol}."
        if event_type == EventType.EXECUTION_PLAN:
            execution_strategy = str(payload.get("execution_strategy") or "").strip() or "default routing"
            return f"Execution plan ready for {symbol}: {execution_strategy}."
        if event_type == EventType.ORDER_FILLED:
            status = str((payload.get("execution_result") or {}).get("status") or payload.get("status") or "filled").strip().lower()
            return f"Execution {status} for {symbol}."
        if event_type == EventType.RISK_ALERT:
            return reason or f"Risk blocked the trade for {symbol}."
        return reason or f"{event_type} for {symbol}."

    async def _handle_trading_agent_bus_event(self, event):
        data = dict(getattr(event, "data", {}) or {})
        symbol = self._normalize_strategy_symbol_key(data.get("symbol"))
        if not symbol:
            return
        event_type = str(getattr(event, "type", "") or "").strip()
        signal_payload = dict(data.get("signal") or {}) if isinstance(data.get("signal"), dict) else {}
        review_payload = dict(data.get("trade_review") or {}) if isinstance(data.get("trade_review"), dict) else {}
        payload = {
            "kind": "bus",
            "event_type": event_type,
            "symbol": symbol,
            "decision_id": str(data.get("decision_id") or review_payload.get("decision_id") or "").strip(),
            "strategy_name": str(signal_payload.get("strategy_name") or review_payload.get("strategy_name") or data.get("strategy_name") or "").strip(),
            "timeframe": str(data.get("timeframe") or review_payload.get("timeframe") or "").strip(),
            "side": str(signal_payload.get("side") or review_payload.get("side") or data.get("side") or "").strip().lower(),
            "reason": str(review_payload.get("reason") or signal_payload.get("reason") or data.get("reason") or "").strip(),
            "message": self._agent_runtime_bus_message(event_type, data),
            "payload": data,
        }
        self._emit_agent_runtime_signal(payload)

    def _bind_trading_runtime_streams(self):
        trading_system = getattr(self, "trading_system", None)
        if trading_system is None:
            return
        memory = getattr(trading_system, "agent_memory", None)
        if memory is not None and hasattr(memory, "add_sink"):
            memory.add_sink(self._handle_live_agent_memory_event)
        event_bus = getattr(trading_system, "event_bus", None)
        if event_bus is not None and hasattr(event_bus, "subscribe"):
            for event_type in (
                EventType.SIGNAL,
                EventType.RISK_APPROVED,
                EventType.RISK_ALERT,
                EventType.EXECUTION_PLAN,
                EventType.ORDER_FILLED,
            ):
                event_bus.subscribe(event_type, self._handle_trading_agent_bus_event)

    def latest_agent_decision_chain_for_symbol(self, symbol, limit=12):
        normalized_symbol = self._normalize_strategy_symbol_key(symbol)
        if not normalized_symbol:
            return []

        live_rows = self._latest_live_agent_decision_chain_for_symbol(normalized_symbol, limit=limit)
        if live_rows:
            return live_rows

        repository = getattr(self, "agent_decision_repository", None)
        exchange = self._active_exchange_code() if hasattr(self, "_active_exchange_code") else None
        account_label = self.current_account_label() if hasattr(self, "current_account_label") else None
        if str(account_label or "").strip().lower() == "not set":
            account_label = None

        if repository is not None and hasattr(repository, "latest_chain_for_symbol"):
            try:
                rows = repository.latest_chain_for_symbol(
                    normalized_symbol,
                    limit=limit,
                    exchange=exchange,
                    account_label=account_label,
                )
                payloads = [self._agent_decision_record_to_payload(item) for item in list(rows or [])]
                if payloads:
                    return payloads
            except Exception:
                self.logger.debug("Unable to restore agent decision chain from repository", exc_info=True)

        trading_system = getattr(self, "trading_system", None)
        if trading_system is None or not hasattr(trading_system, "agent_memory_snapshot"):
            return []

        try:
            events = list(trading_system.agent_memory_snapshot(limit=200) or [])
        except Exception:
            return []
        filtered = [
            dict(item)
            for item in events
            if str((item or {}).get("symbol") or "").strip().upper() == normalized_symbol
        ]
        if not filtered:
            return []
        latest_decision_id = str(filtered[-1].get("decision_id") or "").strip()
        if latest_decision_id:
            filtered = [item for item in filtered if str(item.get("decision_id") or "").strip() == latest_decision_id]
        filtered = filtered[-max(1, int(limit)):]
        return [
            {
                "decision_id": str(item.get("decision_id") or "").strip(),
                "symbol": str(item.get("symbol") or "").strip().upper(),
                "agent_name": str(item.get("agent") or "").strip(),
                "stage": str(item.get("stage") or "").strip(),
                "strategy_name": str((item.get("payload") or {}).get("strategy_name") or "").strip(),
                "timeframe": str((item.get("payload") or {}).get("timeframe") or "").strip(),
                "side": str((item.get("payload") or {}).get("side") or "").strip().lower(),
                "confidence": (item.get("payload") or {}).get("confidence"),
                "approved": (item.get("payload") or {}).get("approved"),
                "reason": str((item.get("payload") or {}).get("reason") or "").strip(),
                "timestamp": item.get("timestamp"),
                "timestamp_label": str(item.get("timestamp") or "").strip(),
                "payload": dict(item.get("payload") or {}),
            }
            for item in filtered
        ]

    def latest_agent_decision_overview_for_symbol(self, symbol):
        chain = list(self.latest_agent_decision_chain_for_symbol(symbol, limit=20) or [])
        if not chain:
            return {}

        signal_row = next((row for row in chain if row.get("agent_name") == "SignalAgent"), {})
        risk_row = next((row for row in reversed(chain) if row.get("agent_name") == "RiskAgent"), {})
        execution_row = next((row for row in reversed(chain) if row.get("agent_name") == "ExecutionAgent"), {})
        latest = dict(chain[-1])
        strategy_name = str(signal_row.get("strategy_name") or risk_row.get("strategy_name") or execution_row.get("strategy_name") or "").strip()
        timeframe = str(signal_row.get("timeframe") or risk_row.get("timeframe") or execution_row.get("timeframe") or "").strip()
        approved = execution_row.get("approved")
        if approved is None:
            approved = risk_row.get("approved")
        return {
            "decision_id": latest.get("decision_id"),
            "symbol": latest.get("symbol"),
            "strategy_name": strategy_name,
            "timeframe": timeframe,
            "side": str(signal_row.get("side") or execution_row.get("side") or "").strip().lower(),
            "approved": approved,
            "final_stage": latest.get("stage"),
            "final_agent": latest.get("agent_name"),
            "reason": str(latest.get("reason") or risk_row.get("reason") or signal_row.get("reason") or "").strip(),
            "steps": len(chain),
            "timestamp_label": latest.get("timestamp_label"),
        }

    def _rebind_storage_dependencies(self):
        trading_system = getattr(self, "trading_system", None)
        if trading_system is None:
            return

        data_hub = getattr(trading_system, "data_hub", None)
        if data_hub is not None:
            data_hub.market_data_repository = self.market_data_repository

        execution_manager = getattr(trading_system, "execution_manager", None)
        if execution_manager is not None:
            execution_manager.trade_repository = self.trade_repository

        binder = getattr(trading_system, "bind_agent_decision_repository", None)
        if callable(binder):
            try:
                binder(getattr(self, "agent_decision_repository", None))
            except Exception:
                self.logger.debug("Unable to rebind agent decision repository", exc_info=True)

    @staticmethod
    def _masked_database_url(database_url):
        text = str(database_url or "").strip()
        if not text:
            return ""
        return re.sub(r":([^:@/]+)@", ":***@", text, count=1)

    def current_database_label(self):
        mode = str(getattr(self, "database_mode", "local") or "local").strip().lower()
        if mode == "remote":
            masked = self._masked_database_url(getattr(self, "database_url", "") or "")
            return masked or "Remote URL not set"
        return "Local SQLite"

    def configure_storage_database(self, database_mode=None, database_url=None, persist=True, raise_on_error=True):
        mode = str(database_mode or getattr(self, "database_mode", "local") or "local").strip().lower()
        if mode not in {"local", "remote"}:
            mode = "local"
        raw_url = str(database_url if database_url is not None else getattr(self, "database_url", "") or "").strip()

        if mode == "remote" and not raw_url:
            raise ValueError("Remote database URL is required when remote storage is selected.")

        target_url = raw_url if mode == "remote" else None
        try:
            configured_url = configure_database(target_url)
            init_database()
        except Exception:
            if not raise_on_error:
                self.logger.exception("Storage database configuration failed; falling back to local SQLite")
                mode = "local"
                raw_url = ""
                configured_url = configure_database(None)
                init_database()
            else:
                raise

        self.database_mode = mode
        self.database_url = raw_url
        self.database_connection_url = configured_url or get_database_url()
        self.market_data_repository = MarketDataRepository()
        self.trade_repository = TradeRepository()
        self.equity_repository = EquitySnapshotRepository()
        self.agent_decision_repository = AgentDecisionRepository()
        self._rebind_storage_dependencies()

        if persist:
            self.settings.setValue("storage/database_mode", self.database_mode)
            self.settings.setValue("storage/database_url", self.database_url)

        return self.database_connection_url

    def _setup_ui(self, controller):
        self.setWindowTitle("Sopotek Trading AI Platform")
        self.resize(1600, 900)
        self.setMinimumSize(960, 640)
        self._fit_window_to_available_screen(requested_width=1600, requested_height=900)

        self.stack = QStackedWidget()
        self.setCentralWidget(self.stack)

        self.dashboard = Dashboard(controller)
        self.stack.addWidget(self.dashboard)

        self.dashboard.login_requested.connect(self._on_login_requested)

    def _fit_window_to_available_screen(self, requested_width=None, requested_height=None):
        screen = self.screen()
        if screen is None:
            app = QApplication.instance()
            screen = app.primaryScreen() if app is not None else None
        if screen is None:
            return

        available = screen.availableGeometry()
        width, minimum_width = _bounded_window_extent(
            requested_width if requested_width is not None else self.width() or 1600,
            available.width(),
            minimum=960,
        )
        height, minimum_height = _bounded_window_extent(
            requested_height if requested_height is not None else self.height() or 900,
            available.height(),
            minimum=640,
        )
        self.setMinimumSize(minimum_width, minimum_height)
        self.resize(width, height)

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
                broker_mode = str(getattr(config.broker, "mode", "") or "").strip().lower()
                if broker_mode == "live" and not self.license_allows("live_trading"):
                    self.dashboard.hide_loading()
                    QMessageBox.warning(
                        self,
                        "License Required",
                        self.license_manager.feature_message("live_trading"),
                    )
                    self.show_license_dialog(self.dashboard)
                    return
                self.strategy_name = Strategy.normalize_strategy_name(getattr(config, "strategy", self.strategy_name))
                self.settings.setValue("strategy/name", self.strategy_name)
                broker_options = dict(getattr(config.broker, "options", None) or {})
                self.set_market_trade_preference(broker_options.get("market_type", self.market_trade_preference))

                broker_type = config.broker.type
                exchange = config.broker.exchange or "unknown"
                if not broker_type:
                    raise RuntimeError("Broker type missing")

                if self.connected or self.broker is not None or self.terminal is not None:
                    self.logger.info("Resetting existing session state before login")
                    self.connected = False
                    self.connection_signal.emit("disconnected")
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
                if self._broker_is_connected(self.broker):
                    self.logger.info("Broker %s is already connected; reusing existing broker session", exchange)
                else:
                    await self.broker.connect()

                raw_symbols = await self._fetch_symbols(self.broker)
                filtered_symbols = self._filter_symbols_for_trading(raw_symbols, broker_type, exchange)
                self.symbols = await self._select_trade_symbols(filtered_symbols, broker_type, exchange)

                self.balances = await self._fetch_balances(self.broker)
                self.balance = self.balances
                self._update_performance_equity(self.balances)

                self.logger.info(
                    "Broker ready exchange=%s type=%s symbols=%s (raw=%s filtered=%s)",
                    exchange,
                    broker_type,
                    len(self.symbols),
                    len(raw_symbols),
                    len(filtered_symbols),
                )

                self.trading_system = SopotekTrading(self)
                self._live_agent_decision_events = {}
                self._live_agent_runtime_feed = []
                self._bind_trading_runtime_streams()
                portfolio_manager = getattr(self.trading_system, "portfolio", None)
                self.portfolio = getattr(portfolio_manager, "portfolio", None)
                self._performance_recorded_orders.clear()
                self.connected = True

                self.connection_signal.emit("connected")
                self.symbols_signal.emit(exchange, self.symbols)

                await self.initialize_trading()
                self.symbols_signal.emit(exchange, self.symbols)
                self.schedule_strategy_auto_assignment(symbols=self.symbols, timeframe=self.time_frame, force=False)
                await self._restart_telegram_service()

                await self._start_market_stream()
                await self._warmup_visible_candles()

            except Exception as e:
                self.connected = False
                self.connection_signal.emit("disconnected")
                self.logger.exception("Initialization failed")
                await self._cleanup_session(stop_trading=True, close_broker=True)
                QMessageBox.critical(
                    self,
                    "Initialization Failed",
                    self._friendly_initialization_error(e),
                )
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
        equity = self._update_performance_equity(self.balances)
        self._update_behavior_guard_equity(self.balances)
        if equity is not None:
            self.equity_signal.emit(equity)

    async def initialize_trading(self):
        try:
            if self.terminal:
                await self._cleanup_session(stop_trading=False, close_broker=False)

            self.terminal = Terminal(self)
            self.stack.addWidget(self.terminal)
            self.stack.setCurrentWidget(self.terminal)
            self._fit_window_to_available_screen()
            QTimer.singleShot(0, self._fit_window_to_available_screen)
            self.terminal.logout_requested.connect(self._on_logout_requested)
            if hasattr(self.terminal, "load_persisted_runtime_data"):
                await self.terminal.load_persisted_runtime_data()
            equity = self._extract_balance_equity_value(getattr(self, "balances", {}))
            if equity is not None:
                self.equity_signal.emit(equity)
            self._create_task(self.run_startup_health_check(), "startup_health_check")

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
        news_enabled=None,
        news_autotrade_enabled=None,
        news_draw_on_chart=None,
        news_feed_url=None,
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
        if news_enabled is not None:
            self.news_enabled = bool(news_enabled)
        if news_autotrade_enabled is not None:
            self.news_autotrade_enabled = bool(news_autotrade_enabled)
        if news_draw_on_chart is not None:
            self.news_draw_on_chart = bool(news_draw_on_chart)
        if news_feed_url is not None:
            self.news_feed_url = str(news_feed_url or NewsService.DEFAULT_FEED_URL).strip() or NewsService.DEFAULT_FEED_URL

        self.settings.setValue("integrations/telegram_enabled", self.telegram_enabled)
        self.settings.setValue("integrations/telegram_bot_token", self.telegram_bot_token)
        self.settings.setValue("integrations/telegram_chat_id", self.telegram_chat_id)
        self.settings.setValue("integrations/openai_api_key", self.openai_api_key)
        self.settings.setValue("integrations/openai_model", self.openai_model)
        self.settings.setValue("integrations/voice_name", getattr(self, "voice_name", ""))
        self.settings.setValue("integrations/voice_windows_name", getattr(self, "voice_windows_name", ""))
        self.settings.setValue("integrations/voice_openai_name", getattr(self, "voice_openai_name", "alloy"))
        self.settings.setValue("integrations/voice_provider", getattr(self, "voice_provider", "windows"))
        self.settings.setValue("integrations/voice_output_provider", getattr(self, "voice_output_provider", "windows"))
        self.settings.setValue("integrations/news_enabled", self.news_enabled)
        self.settings.setValue("integrations/news_autotrade_enabled", self.news_autotrade_enabled)
        self.settings.setValue("integrations/news_draw_on_chart", self.news_draw_on_chart)
        self.settings.setValue("integrations/news_feed_url", self.news_feed_url)
        self.news_service.enabled = self.news_enabled
        self.news_service.feed_url_template = self.news_feed_url

        asyncio.get_event_loop().create_task(self._restart_telegram_service())

    def supported_market_venues(self):
        broker = getattr(self, "broker", None)
        if broker is not None and hasattr(broker, "supported_market_venues"):
            try:
                venues = [
                    str(item).strip().lower()
                    for item in (broker.supported_market_venues() or [])
                    if str(item).strip()
                ]
            except Exception:
                venues = []
            if venues:
                return list(dict.fromkeys(venues))

        broker_cfg = getattr(getattr(self, "config", None), "broker", None)
        broker_type = getattr(broker_cfg, "type", None)
        exchange = getattr(broker_cfg, "exchange", None)
        return supported_market_venues_for_profile(broker_type, exchange)

    def set_market_trade_preference(self, preference):
        normalized = normalize_market_venue(preference)
        supported = self.supported_market_venues()
        if normalized not in supported:
            normalized = "auto" if "auto" in supported else (supported[0] if supported else "auto")
        self.market_trade_preference = normalized
        self.settings.setValue("trading/market_type", normalized)

        broker_cfg = getattr(getattr(self, "config", None), "broker", None)
        if broker_cfg is not None:
            options = dict(getattr(broker_cfg, "options", None) or {})
            options["market_type"] = normalized
            try:
                broker_cfg.options = options
            except Exception:
                pass

        broker = getattr(self, "broker", None)
        if broker is not None and hasattr(broker, "extra_options"):
            broker.extra_options["market_type"] = normalized
            if hasattr(broker, "market_preference"):
                broker.market_preference = normalized
            if hasattr(broker, "apply_market_preference"):
                try:
                    updated_symbols = broker.apply_market_preference(normalized)
                except Exception:
                    updated_symbols = None
                if updated_symbols:
                    self.symbols = list(updated_symbols)
                    exchange_name = getattr(broker, "exchange_name", getattr(broker_cfg, "exchange", "broker")) or "broker"
                    self.symbols_signal.emit(str(exchange_name), list(self.symbols))
                    if getattr(self, "connected", False):
                        self.schedule_strategy_auto_assignment(symbols=self.symbols, timeframe=self.time_frame, force=False)

    async def request_news(self, symbol, force=False, max_age_seconds=300):
        normalized = str(symbol or "").upper().strip()
        if not normalized or not self.news_enabled:
            return []

        cached = self._news_cache.get(normalized)
        if not force and isinstance(cached, dict):
            cached_at = float(cached.get("fetched_at", 0.0) or 0.0)
            if (time.monotonic() - cached_at) <= max_age_seconds:
                events = list(cached.get("events", []) or [])
                self.news_signal.emit(normalized, events)
                return events

        existing_task = self._news_inflight.get(normalized)
        if existing_task is not None and not existing_task.done():
            try:
                return await existing_task
            except Exception:
                return []

        async def runner():
            broker_type = getattr(getattr(self, "config", None), "broker", None)
            broker_type = getattr(broker_type, "type", None)
            events = await self.news_service.fetch_symbol_news(normalized, broker_type=broker_type, limit=8)
            self._news_cache[normalized] = {
                "fetched_at": time.monotonic(),
                "events": list(events or []),
            }
            self.news_signal.emit(normalized, list(events or []))
            return list(events or [])

        task = asyncio.create_task(runner())
        self._news_inflight[normalized] = task
        try:
            return await task
        finally:
            current = self._news_inflight.get(normalized)
            if current is task:
                self._news_inflight.pop(normalized, None)

    def get_news_bias(self, symbol):
        normalized = str(symbol or "").upper().strip()
        cached = self._news_cache.get(normalized, {})
        events = list(cached.get("events", []) or [])
        return self.news_service.summarize_news_bias(events)

    async def apply_news_bias_to_signal(self, symbol, signal):
        if not isinstance(signal, dict):
            return None
        if not self.news_enabled or not self.news_autotrade_enabled:
            return signal

        events = await self.request_news(symbol)
        bias = self.news_service.summarize_news_bias(events)
        direction = str(bias.get("direction", "neutral") or "neutral").lower()
        score = float(bias.get("score", 0.0) or 0.0)

        updated = dict(signal)
        base_reason = str(updated.get("reason", "") or "").strip()
        news_reason = str(bias.get("reason", "") or "").strip()
        if news_reason:
            updated["reason"] = f"{base_reason} | News: {news_reason}" if base_reason else f"News: {news_reason}"
        updated["news_bias"] = direction
        updated["news_score"] = score

        side = str(updated.get("side", "") or "").lower()
        if direction in {"buy", "sell"} and direction != side and abs(score) >= 0.35:
            self.logger.info("Skipping %s %s due to conflicting news bias (%s %.3f)", symbol, side, direction, score)
            return None

        if direction == side and abs(score) >= 0.2:
            updated["confidence"] = min(float(updated.get("confidence", 0.0) or 0.0) + 0.08, 0.99)

        return updated

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

    def telegram_status_snapshot(self):
        service = getattr(self, "telegram_service", None)
        running = bool(getattr(service, "_running", False)) if service is not None else False
        configured = bool(str(getattr(self, "telegram_bot_token", "") or "").strip())
        chat_id = str(getattr(self, "telegram_chat_id", "") or "").strip()
        masked_chat = "Not set"
        if chat_id:
            masked_chat = chat_id if len(chat_id) <= 6 else f"{chat_id[:3]}...{chat_id[-3:]}"
        return {
            "enabled": bool(getattr(self, "telegram_enabled", False)),
            "configured": configured,
            "running": running,
            "chat_id": masked_chat,
            "can_send": bool(service.can_send()) if service is not None else bool(configured and chat_id),
        }

    def telegram_management_text(self):
        snapshot = self.telegram_status_snapshot()
        return (
            "Telegram Integration\n"
            f"Enabled: {'YES' if snapshot['enabled'] else 'NO'}\n"
            f"Configured: {'YES' if snapshot['configured'] else 'NO'}\n"
            f"Running: {'YES' if snapshot['running'] else 'NO'}\n"
            f"Chat ID: {snapshot['chat_id']}\n"
            f"Can Send Messages: {'YES' if snapshot['can_send'] else 'NO'}"
        )

    async def _set_telegram_enabled_state(self, enabled):
        self.telegram_enabled = bool(enabled)
        self.settings.setValue("integrations/telegram_enabled", self.telegram_enabled)
        if self.telegram_enabled:
            await self._restart_telegram_service()
        else:
            await self._stop_telegram_service()

    async def send_test_telegram_message(self, text=None):
        service = getattr(self, "telegram_service", None)
        if service is None or not service.can_send():
            return False
        message = text or (
            "<b>Sopotek Telegram Test</b>\n"
            "Sopotek Pilot sent this test message successfully."
        )
        return bool(await service.send_message(message))

    def trade_quantity_context(self, symbol):
        normalized_symbol = str(symbol or "").strip().upper()
        broker = getattr(self, "broker", None)
        exchange_name = str(getattr(broker, "exchange_name", "") or "").strip().lower()
        compact = normalized_symbol.replace("_", "/").replace("-", "/")
        parts = compact.split("/", 1) if "/" in compact else []
        supports_lots = False
        if exchange_name == "oanda" and len(parts) == 2:
            base, quote = parts
            supports_lots = (
                len(base) == 3
                and len(quote) == 3
                and base.isalpha()
                and quote.isalpha()
                and base in self.FOREX_SYMBOL_QUOTES
                and quote in self.FOREX_SYMBOL_QUOTES
            )
        return {
            "symbol": normalized_symbol,
            "supports_lots": supports_lots,
            "default_mode": "lots" if supports_lots else "units",
            "lot_units": self.FOREX_STANDARD_LOT_UNITS,
        }

    def normalize_trade_quantity(self, symbol, amount, quantity_mode=None):
        try:
            numeric_amount = abs(float(amount))
        except Exception as exc:
            raise ValueError("Trade amount must be numeric.") from exc
        if numeric_amount <= 0:
            raise ValueError("Trade amount must be positive.")

        context = self.trade_quantity_context(symbol)
        requested_mode = str(quantity_mode or context.get("default_mode") or "units").strip().lower()
        if requested_mode.endswith("s"):
            requested_mode = requested_mode[:-1]
        if requested_mode not in {"unit", "lot"}:
            raise ValueError("Trade quantity mode must be 'units' or 'lots'.")
        if requested_mode == "lot" and not context.get("supports_lots"):
            raise ValueError(f"Lot sizing is only available for supported forex symbols. Use units for {symbol}.")

        normalized_units = (
            numeric_amount * float(context.get("lot_units", self.FOREX_STANDARD_LOT_UNITS))
            if requested_mode == "lot"
            else numeric_amount
        )
        result = dict(context)
        result.update(
            {
                "requested_amount": numeric_amount,
                "requested_mode": "lots" if requested_mode == "lot" else "units",
                "amount_units": float(normalized_units),
            }
        )
        return result

    def _normalize_strategy_symbol_key(self, symbol):
        return str(symbol or "").strip().upper().replace("-", "/").replace("_", "/")

    def _load_strategy_symbol_payload(self, key):
        raw_value = self.settings.value(key, "{}")
        try:
            payload = json.loads(raw_value or "{}")
        except Exception:
            payload = {}
        normalized = {}
        if not isinstance(payload, dict):
            return normalized
        for symbol, rows in payload.items():
            normalized_symbol = self._normalize_strategy_symbol_key(symbol)
            source_rows = list(rows or [])
            cleaned_rows = []
            for row in source_rows:
                if not isinstance(row, dict):
                    continue
                strategy_name = Strategy.normalize_strategy_name(row.get("strategy_name"))
                if not strategy_name:
                    continue
                assignment_mode = str(
                    row.get("assignment_mode") or ("ranked" if len(source_rows) > 1 else "single")
                ).strip().lower()
                assignment_source = str(row.get("assignment_source") or "manual").strip().lower()
                cleaned_rows.append(
                    {
                        "strategy_name": strategy_name,
                        "score": float(row.get("score", 0.0) or 0.0),
                        "weight": float(row.get("weight", 0.0) or 0.0),
                        "symbol": normalized_symbol,
                        "timeframe": str(row.get("timeframe") or "").strip(),
                        "assignment_mode": assignment_mode if assignment_mode in {"single", "ranked"} else "single",
                        "assignment_source": assignment_source if assignment_source in {"manual", "auto"} else "manual",
                        "rank": int(row.get("rank", len(cleaned_rows) + 1) or (len(cleaned_rows) + 1)),
                        "total_profit": float(row.get("total_profit", 0.0) or 0.0),
                        "sharpe_ratio": float(row.get("sharpe_ratio", 0.0) or 0.0),
                        "win_rate": float(row.get("win_rate", 0.0) or 0.0),
                        "final_equity": float(row.get("final_equity", 0.0) or 0.0),
                        "max_drawdown": float(row.get("max_drawdown", 0.0) or 0.0),
                        "closed_trades": int(row.get("closed_trades", 0) or 0),
                    }
                )
            if cleaned_rows:
                normalized[normalized_symbol] = cleaned_rows
        return normalized

    def _persist_strategy_symbol_state(self):
        lock_set = getattr(self, "symbol_strategy_locks", None)
        if not isinstance(lock_set, set):
            lock_set = set(lock_set or [])
            self.symbol_strategy_locks = lock_set
        self.settings.setValue("strategy/multi_strategy_enabled", bool(self.multi_strategy_enabled))
        self.settings.setValue("strategy/max_symbol_strategies", int(self.max_symbol_strategies))
        self.settings.setValue("strategy/symbol_assignments", json.dumps(self.symbol_strategy_assignments))
        self.settings.setValue("strategy/symbol_rankings", json.dumps(self.symbol_strategy_rankings))
        self.settings.setValue("strategy/symbol_assignment_locks", json.dumps(sorted(lock_set)))
        self.settings.setValue("strategy/auto_assignment_enabled", bool(getattr(self, "strategy_auto_assignment_enabled", True)))

    def _load_strategy_symbol_lock_payload(self, key, fallback_symbols=None):
        raw_value = self.settings.value(key, None)
        payload = None
        if raw_value not in (None, ""):
            try:
                payload = json.loads(raw_value)
            except Exception:
                payload = None

        if payload is None:
            source = list(fallback_symbols or [])
        elif isinstance(payload, dict):
            source = [symbol for symbol, locked in payload.items() if locked]
        elif isinstance(payload, (list, tuple, set)):
            source = list(payload)
        else:
            source = []

        normalized = []
        for symbol in source:
            normalized_symbol = self._normalize_strategy_symbol_key(symbol)
            if normalized_symbol and normalized_symbol not in normalized:
                normalized.append(normalized_symbol)
        return set(normalized)

    def _mark_symbol_strategy_assignment_locked(self, symbol, locked=True):
        normalized_symbol = self._normalize_strategy_symbol_key(symbol)
        lock_set = getattr(self, "symbol_strategy_locks", None)
        if not isinstance(lock_set, set):
            lock_set = set(lock_set or [])
            self.symbol_strategy_locks = lock_set
        if not normalized_symbol:
            return False
        if locked:
            lock_set.add(normalized_symbol)
        else:
            lock_set.discard(normalized_symbol)
        return normalized_symbol in lock_set

    def symbol_strategy_assignment_locked(self, symbol):
        normalized_symbol = self._normalize_strategy_symbol_key(symbol)
        lock_set = getattr(self, "symbol_strategy_locks", set()) or set()
        return normalized_symbol in lock_set

    def _strategy_auto_assignment_symbols(self, symbols=None):
        symbol_sources = []
        if symbols is not None:
            symbol_sources.append(list(symbols or []))
        else:
            symbol_sources.extend(
                [
                    list(getattr(self, "symbols", []) or []),
                    list(getattr(self, "symbol_strategy_assignments", {}).keys()),
                    list(getattr(self, "symbol_strategy_rankings", {}).keys()),
                    list(getattr(self, "symbol_strategy_locks", set()) or set()),
                ]
            )

        symbol_candidates = []
        for source in symbol_sources:
            for symbol in list(source or []):
                normalized_symbol = self._normalize_strategy_symbol_key(symbol)
                if normalized_symbol and normalized_symbol not in symbol_candidates:
                    symbol_candidates.append(normalized_symbol)
        return symbol_candidates

    def _symbol_has_saved_strategy_state(self, symbol):
        normalized_symbol = self._normalize_strategy_symbol_key(symbol)
        if not normalized_symbol:
            return False
        if self.symbol_strategy_assignment_locked(normalized_symbol):
            return True
        assigned_rows = list((getattr(self, "symbol_strategy_assignments", {}) or {}).get(normalized_symbol, []) or [])
        return bool(assigned_rows)

    def _partition_strategy_auto_assignment_symbols(self, symbols=None):
        symbol_candidates = self._strategy_auto_assignment_symbols(symbols=symbols)
        restored_symbols = []
        missing_symbols = []
        for symbol in symbol_candidates:
            if self._symbol_has_saved_strategy_state(symbol):
                restored_symbols.append(symbol)
            else:
                missing_symbols.append(symbol)
        return symbol_candidates, missing_symbols, restored_symbols

    def _restore_saved_strategy_assignments(self, restored_symbols, timeframe=None, message=None):
        timeframe_value = str(timeframe or getattr(self, "time_frame", "1h") or "1h").strip() or "1h"
        restored_symbols = list(restored_symbols or [])
        restored_count = len(restored_symbols)
        summary_message = message or (
            f"Loaded saved strategy assignments for {restored_count} symbol{'s' if restored_count != 1 else ''}."
            if restored_count
            else "No saved strategy assignments were available."
        )

        self.strategy_auto_assignment_in_progress = False
        self.strategy_auto_assignment_ready = True
        self._update_strategy_auto_assignment_progress(
            completed=restored_count,
            total=restored_count,
            current_symbol="",
            timeframe=timeframe_value,
            message=summary_message,
            failed_symbols=[],
        )

        trading_system = getattr(self, "trading_system", None)
        if trading_system is not None and hasattr(trading_system, "refresh_strategy_preferences"):
            try:
                trading_system.refresh_strategy_preferences()
            except Exception:
                pass

        return {
            "assigned_symbols": [],
            "restored_symbols": list(restored_symbols),
            "skipped_symbols": [],
            "failed_symbols": [],
            "timeframe": timeframe_value,
        }

    def strategy_auto_assignment_status(self):
        progress = dict(getattr(self, "strategy_auto_assignment_progress", {}) or {})
        progress["failed_symbols"] = list(progress.get("failed_symbols", []) or [])
        progress["enabled"] = bool(getattr(self, "strategy_auto_assignment_enabled", True))
        progress["running"] = bool(getattr(self, "strategy_auto_assignment_in_progress", False))
        progress["ready"] = (not progress["enabled"]) or bool(getattr(self, "strategy_auto_assignment_ready", False))
        progress["locked_symbols"] = sorted(list(getattr(self, "symbol_strategy_locks", set()) or set()))
        progress["assigned_symbols"] = len(getattr(self, "symbol_strategy_assignments", {}) or {})
        return progress

    def _update_strategy_auto_assignment_progress(self, **changes):
        snapshot = dict(getattr(self, "strategy_auto_assignment_progress", {}) or {})
        snapshot.update(changes)
        snapshot["updated_at"] = datetime.now(timezone.utc).isoformat()
        if "failed_symbols" not in snapshot:
            snapshot["failed_symbols"] = []
        else:
            snapshot["failed_symbols"] = list(snapshot.get("failed_symbols", []) or [])
        self.strategy_auto_assignment_progress = snapshot
        terminal = getattr(self, "terminal", None)
        if terminal is None:
            return snapshot
        window = getattr(terminal, "detached_tool_windows", {}).get("strategy_assignments")
        if window is not None and hasattr(terminal, "_refresh_strategy_assignment_window"):
            try:
                terminal._refresh_strategy_assignment_window(window=window, message=snapshot.get("message"))
            except Exception:
                pass
        return snapshot

    def _strategy_registry_for_auto_assignment(self):
        trading_system = getattr(self, "trading_system", None)
        registry = getattr(trading_system, "strategy", None)
        if registry is not None and hasattr(registry, "list"):
            return registry
        from strategy.strategy_registry import StrategyRegistry

        return StrategyRegistry()

    def _build_strategy_ranker(self, strategy_registry):
        from backtesting.strategy_ranker import StrategyRanker

        return StrategyRanker(
            strategy_registry=strategy_registry,
            initial_balance=getattr(self, "initial_capital", 10000),
        )

    def _normalize_strategy_ranking_frame(self, dataset):
        if dataset is None:
            return None
        frame = dataset.copy() if hasattr(dataset, "copy") else pd.DataFrame(dataset)
        if not isinstance(frame, pd.DataFrame):
            frame = pd.DataFrame(frame)
        if frame.empty:
            return None

        lowered = {str(column).strip().lower(): column for column in frame.columns}
        if all(name in lowered for name in ("timestamp", "open", "high", "low", "close", "volume")):
            normalized = frame[[lowered[name] for name in ("timestamp", "open", "high", "low", "close", "volume")]].copy()
            normalized.columns = ["timestamp", "open", "high", "low", "close", "volume"]
        elif frame.shape[1] >= 6:
            normalized = frame.iloc[:, :6].copy()
            normalized.columns = ["timestamp", "open", "high", "low", "close", "volume"]
        else:
            return None

        for column in ("open", "high", "low", "close", "volume"):
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
        normalized.dropna(subset=["open", "high", "low", "close"], inplace=True)
        if normalized.empty:
            return None
        return normalized.reset_index(drop=True)

    def _strategy_auto_assignment_timeframes(self, timeframe=None):
        preferred = str(timeframe or getattr(self, "time_frame", "1h") or "1h").strip().lower() or "1h"
        configured = getattr(self, "strategy_assignment_scan_timeframes", None)
        if isinstance(configured, str):
            raw_timeframes = [configured]
        elif isinstance(configured, (list, tuple, set)) and configured:
            raw_timeframes = list(configured)
        else:
            raw_timeframes = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]

        normalized = []
        for item in [preferred, *raw_timeframes]:
            value = str(item or "").strip().lower()
            if value and value not in normalized:
                normalized.append(value)
        return normalized

    def _best_strategy_rankings_across_timeframes(self, rankings):
        best_by_strategy = {}
        for row in list(rankings or []):
            if not isinstance(row, dict):
                continue
            strategy_name = Strategy.normalize_strategy_name(row.get("strategy_name"))
            if not strategy_name:
                continue
            candidate = dict(row)
            candidate["strategy_name"] = strategy_name
            candidate["timeframe"] = str(candidate.get("timeframe") or getattr(self, "time_frame", "1h") or "1h").strip() or "1h"
            candidate_score = float(candidate.get("score", 0.0) or 0.0)
            existing = best_by_strategy.get(strategy_name)
            existing_score = float(existing.get("score", 0.0) or 0.0) if isinstance(existing, dict) else float("-inf")
            if existing is None or candidate_score > existing_score:
                best_by_strategy[strategy_name] = candidate

        ordered = sorted(
            best_by_strategy.values(),
            key=lambda item: (
                -float(item.get("score", 0.0) or 0.0),
                -float(item.get("total_profit", 0.0) or 0.0),
                -float(item.get("sharpe_ratio", 0.0) or 0.0),
                str(item.get("timeframe") or ""),
            ),
        )
        for index, item in enumerate(ordered, start=1):
            item["rank"] = index
        return ordered

    def save_ranked_strategies_for_symbol(self, symbol, rankings, timeframe=None, assignment_source="manual", persist=True):
        normalized_symbol = self._normalize_strategy_symbol_key(symbol)
        assignment_source = str(assignment_source or "manual").strip().lower()
        if assignment_source not in {"manual", "auto"}:
            assignment_source = "manual"

        cleaned_rows = []
        for index, row in enumerate(list(rankings or []), start=1):
            if not isinstance(row, dict):
                continue
            strategy_name = Strategy.normalize_strategy_name(row.get("strategy_name"))
            if not strategy_name:
                continue
            cleaned_rows.append(
                {
                    "strategy_name": strategy_name,
                    "score": float(row.get("score", 0.0) or 0.0),
                    "weight": float(row.get("weight", 0.0) or 0.0),
                    "symbol": normalized_symbol,
                    "timeframe": str(row.get("timeframe") or timeframe or self.time_frame or "").strip(),
                    "assignment_mode": "ranked",
                    "assignment_source": assignment_source,
                    "rank": int(row.get("rank", index) or index),
                    "total_profit": float(row.get("total_profit", 0.0) or 0.0),
                    "sharpe_ratio": float(row.get("sharpe_ratio", 0.0) or 0.0),
                    "win_rate": float(row.get("win_rate", 0.0) or 0.0),
                    "final_equity": float(row.get("final_equity", 0.0) or 0.0),
                    "max_drawdown": float(row.get("max_drawdown", 0.0) or 0.0),
                    "closed_trades": int(row.get("closed_trades", 0) or 0),
                }
            )
        cleaned_rows.sort(key=lambda item: (-float(item.get("score", 0.0) or 0.0), int(item.get("rank", 0) or 0)))
        if cleaned_rows:
            self.symbol_strategy_rankings[normalized_symbol] = cleaned_rows
        else:
            self.symbol_strategy_rankings.pop(normalized_symbol, None)
        if persist:
            self._persist_strategy_symbol_state()
        return list(cleaned_rows)

    def schedule_strategy_auto_assignment(self, symbols=None, timeframe=None, force=False):
        if not bool(getattr(self, "strategy_auto_assignment_enabled", True)) and not force:
            self.strategy_auto_assignment_ready = True
            return None
        if not force:
            _all_symbols, missing_symbols, restored_symbols = self._partition_strategy_auto_assignment_symbols(symbols=symbols)
            if restored_symbols and not missing_symbols:
                self._strategy_auto_assignment_task = None
                self._restore_saved_strategy_assignments(
                    restored_symbols,
                    timeframe=timeframe,
                )
                return None
            if missing_symbols:
                symbols = list(missing_symbols)
        task = getattr(self, "_strategy_auto_assignment_task", None)
        if task is not None and not task.done():
            return task
        self.strategy_auto_assignment_ready = False
        self._strategy_auto_assignment_task = asyncio.get_event_loop().create_task(
            self.auto_rank_and_assign_strategies(symbols=symbols, timeframe=timeframe, force=force)
        )
        return self._strategy_auto_assignment_task

    async def auto_rank_and_assign_strategies(self,
                                              symbols=None, timeframe=None, force=False, min_candles=120, history_limit=240):
        if not bool(getattr(self, "strategy_auto_assignment_enabled", True)) and not force:
            self.strategy_auto_assignment_ready = True
            return self.strategy_auto_assignment_status()

        timeframe_value = str(timeframe or getattr(self, "time_frame", "1h") or "1h").strip() or "1h"
        symbol_candidates = self._strategy_auto_assignment_symbols(symbols=symbols)
        restored_symbols = []
        if not force:
            _all_symbols, missing_symbols, restored_symbols = self._partition_strategy_auto_assignment_symbols(symbols=symbols)
            symbol_candidates = list(missing_symbols)

        registry = self._strategy_registry_for_auto_assignment()
        strategy_names = list(getattr(registry, "list", lambda: [])() or [])
        if restored_symbols and not symbol_candidates:
            return self._restore_saved_strategy_assignments(restored_symbols, timeframe=timeframe_value)
        if not symbol_candidates or not strategy_names:
            self.strategy_auto_assignment_in_progress = False
            self.strategy_auto_assignment_ready = True
            self._update_strategy_auto_assignment_progress(
                completed=len(symbol_candidates),
                total=len(symbol_candidates),
                current_symbol="",
                timeframe=timeframe_value,
                message="No symbols or strategies available for automatic assignment.",
                failed_symbols=[],
            )
            return self.strategy_auto_assignment_status()

        self.strategy_auto_assignment_in_progress = True
        self.strategy_auto_assignment_ready = False
        timeframe_candidates = self._strategy_auto_assignment_timeframes(timeframe=timeframe_value)
        scan_message = f"Scanning {len(symbol_candidates)} symbols across {len(timeframe_candidates)} timeframes and ranking {len(strategy_names)} strategies."
        if restored_symbols:
            scan_message = (
                f"Loaded saved strategy assignments for {len(restored_symbols)} "
                f"symbol{'s' if len(restored_symbols) != 1 else ''}. "
                f"Scanning {len(symbol_candidates)} new symbol{'s' if len(symbol_candidates) != 1 else ''} "
                f"across {len(timeframe_candidates)} timeframes and ranking {len(strategy_names)} strategies."
            )
        self._update_strategy_auto_assignment_progress(
            completed=0,
            total=len(symbol_candidates),
            current_symbol="",
            timeframe=timeframe_value,
            message=scan_message,
            failed_symbols=[]
        )
        self.system_console= SystemConsole(self.controller)  # Ensure system_console is available for logging

        terminal = getattr(self, "terminal", None)
        system_console = getattr(terminal, "system_console", None) if terminal is not None else None
        if system_console is not None:
            system_console.log(
                f"Scanning {len(symbol_candidates)} symbols across {len(timeframe_candidates)} timeframes and ranking {len(strategy_names)} strategies before manual overrides unlock.",
                "INFO",
            )

        assigned_symbols = []
        skipped_symbols = []
        failed_symbols = []
        refreshed_preferences = False
        ranker = self._build_strategy_ranker(registry)

        try:
            for index, symbol in enumerate(symbol_candidates, start=1):
                self._update_strategy_auto_assignment_progress(
                    completed=index - 1,
                    total=len(symbol_candidates),
                    current_symbol=symbol,
                    timeframe=timeframe_value,
                    message=f"Scanning {symbol} ({index}/{len(symbol_candidates)}) and ranking strategies.",
                    failed_symbols=failed_symbols,
                )

                combined_records = []
                resolved_timeframes = []
                for candidate_timeframe in timeframe_candidates:
                    symbol_cache = getattr(self, "candle_buffers", {}).get(symbol, {})
                    frame = self._normalize_strategy_ranking_frame(symbol_cache.get(candidate_timeframe) if isinstance(symbol_cache, dict) else None)
                    if frame is None or len(frame) < max(20, int(min_candles or 20)):
                        try:
                            fetched = await self.request_candle_data(
                                symbol,
                                timeframe=candidate_timeframe,
                                limit=max(int(history_limit or 240), int(min_candles or 120)),
                            )
                        except Exception as exc:
                            fetched = None
                            if not any(
                                item.get("symbol") == symbol and str(item.get("timeframe") or "") == candidate_timeframe
                                for item in failed_symbols
                            ):
                                failed_symbols.append({"symbol": symbol, "timeframe": candidate_timeframe, "reason": str(exc)})
                        frame = self._normalize_strategy_ranking_frame(fetched)
                        if frame is None:
                            symbol_cache = getattr(self, "candle_buffers", {}).get(symbol, {})
                            frame = self._normalize_strategy_ranking_frame(symbol_cache.get(candidate_timeframe) if isinstance(symbol_cache, dict) else None)

                    if frame is None or len(frame) < max(20, int(min_candles or 20)):
                        continue

                    results = await asyncio.to_thread(
                        ranker.rank,
                        frame.copy(),
                        symbol,
                        candidate_timeframe,
                        strategy_names,
                    )
                    records = results.to_dict("records") if results is not None and not getattr(results, "empty", True) else []
                    for record in records:
                        if isinstance(record, dict):
                            record["timeframe"] = str(record.get("timeframe") or candidate_timeframe).strip() or candidate_timeframe
                    if records:
                        combined_records.extend(records)
                        resolved_timeframes.append(candidate_timeframe)

                records = self._best_strategy_rankings_across_timeframes(combined_records)
                if not records:
                    if not any(item.get("symbol") == symbol for item in failed_symbols):
                        failed_symbols.append(
                            {
                                "symbol": symbol,
                                "reason": f"No ranked strategies were produced for {symbol} across the scanned timeframes.",
                            }
                        )
                    continue

                locked = self.symbol_strategy_assignment_locked(symbol)
                if force or not locked:
                    assigned = self.assign_ranked_strategies_to_symbol(
                        symbol,
                        records,
                        top_n=1,
                        timeframe=timeframe_value,
                        assignment_source="auto",
                        lock_symbol=False,
                        refresh_preferences=False,
                    )
                    if assigned:
                        assigned_symbols.append(symbol)
                        refreshed_preferences = True
                else:
                    self.save_ranked_strategies_for_symbol(
                        symbol,
                        records,
                        timeframe=timeframe_value,
                        assignment_source="auto",
                        persist=True,
                    )
                    skipped_symbols.append(symbol)

                best_label = "no ranked strategy"
                if records:
                    best_row = records[0]
                    best_label = f"{best_row.get('strategy_name', 'Strategy')} @ {best_row.get('timeframe', timeframe_value)}"
                self._update_strategy_auto_assignment_progress(
                    completed=index,
                    total=len(symbol_candidates),
                    current_symbol=symbol,
                    timeframe=str(records[0].get("timeframe") or timeframe_value) if records else timeframe_value,
                    message=f"Scanned {index}/{len(symbol_candidates)} symbols. Best fit for {symbol}: {best_label}.",
                    failed_symbols=failed_symbols,
                    scan_timeframes=list(timeframe_candidates),
                    resolved_timeframes=list(resolved_timeframes),
                )

            if refreshed_preferences:
                trading_system = getattr(self, "trading_system", None)
                if trading_system is not None and hasattr(trading_system, "refresh_strategy_preferences"):
                    try:
                        trading_system.refresh_strategy_preferences()
                    except Exception:
                        pass

            self.strategy_auto_assignment_in_progress = False
            self.strategy_auto_assignment_ready = True
            summary_message = (
                f"Automatic strategy assignment completed: {len(assigned_symbols)} symbols assigned, "
                f"{len(restored_symbols)} saved symbols restored, "
                f"{len(skipped_symbols)} manual overrides preserved, {len(failed_symbols)} symbols skipped."
            )
            self._update_strategy_auto_assignment_progress(
                completed=len(symbol_candidates),
                total=len(symbol_candidates),
                current_symbol="",
                timeframe=timeframe_value,
                message=summary_message,
                failed_symbols=failed_symbols,
                scan_timeframes=list(timeframe_candidates),
            )
            if system_console is not None:
                system_console.log(summary_message, "INFO")
            return {
                "assigned_symbols": list(assigned_symbols),
                "restored_symbols": list(restored_symbols),
                "skipped_symbols": list(skipped_symbols),
                "failed_symbols": list(failed_symbols),
                "timeframe": timeframe_value,
                "scan_timeframes": list(timeframe_candidates),
            }
        except asyncio.CancelledError:
            self.strategy_auto_assignment_in_progress = False
            self.strategy_auto_assignment_ready = False
            self._update_strategy_auto_assignment_progress(
                completed=int((getattr(self, "strategy_auto_assignment_progress", {}) or {}).get("completed", 0) or 0),
                total=len(symbol_candidates),
                current_symbol="",
                timeframe=timeframe_value,
                message="Automatic strategy assignment was cancelled.",
                failed_symbols=failed_symbols,
            )
            raise
        except Exception as exc:
            self.strategy_auto_assignment_in_progress = False
            self.strategy_auto_assignment_ready = False
            failure_message = f"Automatic strategy assignment failed: {exc}"
            self._update_strategy_auto_assignment_progress(
                completed=int((getattr(self, "strategy_auto_assignment_progress", {}) or {}).get("completed", 0) or 0),
                total=len(symbol_candidates),
                current_symbol="",
                timeframe=timeframe_value,
                message=failure_message,
                failed_symbols=failed_symbols,
            )
            if system_console is not None:
                system_console.log(failure_message, "ERROR")
            raise

    def ranked_strategies_for_symbol(self, symbol):
        normalized_symbol = self._normalize_strategy_symbol_key(symbol)
        return list(self.symbol_strategy_rankings.get(normalized_symbol, []) or [])

    def raw_assigned_strategies_for_symbol(self, symbol):
        normalized_symbol = self._normalize_strategy_symbol_key(symbol)
        return list(self.symbol_strategy_assignments.get(normalized_symbol, []) or [])

    def strategy_assignment_state_for_symbol(self, symbol):
        normalized_symbol = self._normalize_strategy_symbol_key(symbol)
        explicit_rows = self.raw_assigned_strategies_for_symbol(normalized_symbol)
        active_rows = self.assigned_strategies_for_symbol(normalized_symbol)
        ranked_rows = self.ranked_strategies_for_symbol(normalized_symbol)
        if explicit_rows:
            mode = str(explicit_rows[0].get("assignment_mode") or "").strip().lower()
            if mode not in {"single", "ranked"}:
                mode = "ranked" if len(explicit_rows) > 1 else "single"
        else:
            mode = "default"
        return {
            "symbol": normalized_symbol,
            "mode": mode,
            "explicit_rows": explicit_rows,
            "active_rows": active_rows,
            "ranked_rows": ranked_rows,
            "locked": self.symbol_strategy_assignment_locked(normalized_symbol),
        }

    def assigned_strategies_for_symbol(self, symbol):
        normalized_symbol = self._normalize_strategy_symbol_key(symbol)
        assigned = list(self.symbol_strategy_assignments.get(normalized_symbol, []) or [])
        if assigned:
            if self.multi_strategy_enabled or len(assigned) <= 1:
                return assigned
            primary = dict(assigned[0])
            primary["weight"] = 1.0
            primary["rank"] = 1
            return [primary]
        fallback_name = Strategy.normalize_strategy_name(getattr(self, "strategy_name", "Trend Following"))
        return [
            {
                "strategy_name": fallback_name,
                "score": 1.0,
                "weight": 1.0,
                "symbol": normalized_symbol,
                "timeframe": str(getattr(self, "time_frame", "") or "").strip(),
                "rank": 1,
            }
        ]

    def adaptive_strategy_profiles_for_symbol(self, symbol):
        normalized_symbol = self._normalize_strategy_symbol_key(symbol)
        if not normalized_symbol:
            return []

        state = self.strategy_assignment_state_for_symbol(normalized_symbol)
        active_rows = list(state.get("active_rows", []) or [])
        ranked_rows = list(state.get("ranked_rows", []) or [])
        source_rows = list(active_rows) + list(ranked_rows)
        if not source_rows:
            source_rows = list(self.assigned_strategies_for_symbol(normalized_symbol) or [])

        active_keys = {
            (
                str(row.get("strategy_name") or "").strip(),
                str(row.get("timeframe") or "").strip(),
            )
            for row in active_rows
            if isinstance(row, dict)
        }
        ranked_keys = {
            (
                str(row.get("strategy_name") or "").strip(),
                str(row.get("timeframe") or "").strip(),
            )
            for row in ranked_rows
            if isinstance(row, dict)
        }

        trading_system = getattr(self, "trading_system", None)
        profile_resolver = getattr(trading_system, "adaptive_profile_for_strategy", None) if trading_system is not None else None
        seen = set()
        profiles = []
        for row in list(source_rows or []):
            if not isinstance(row, dict):
                continue
            strategy_name = str(row.get("strategy_name") or "").strip()
            timeframe = str(row.get("timeframe") or getattr(self, "time_frame", "1h") or "1h").strip() or "1h"
            if not strategy_name:
                continue
            fingerprint = (strategy_name, timeframe)
            if fingerprint in seen:
                continue
            seen.add(fingerprint)

            profile = dict(
                profile_resolver(normalized_symbol, strategy_name, timeframe=timeframe) or {}
            ) if callable(profile_resolver) else {}
            mode = "candidate"
            if fingerprint in active_keys and fingerprint in ranked_keys:
                mode = "active + ranked"
            elif fingerprint in active_keys:
                mode = "active"
            elif fingerprint in ranked_keys:
                mode = "ranked"

            profiles.append(
                {
                    "symbol": normalized_symbol,
                    "strategy_name": strategy_name,
                    "timeframe": timeframe,
                    "mode": mode,
                    "adaptive_weight": float(profile.get("adaptive_weight", 1.0) or 1.0),
                    "sample_size": int(profile.get("sample_size", 0) or 0),
                    "win_rate": profile.get("win_rate"),
                    "average_pnl": profile.get("average_pnl"),
                    "scope": str(profile.get("scope") or "none").strip() or "none",
                    "assignment_score": float(row.get("score", 0.0) or 0.0),
                    "assignment_weight": float(row.get("weight", 0.0) or 0.0),
                }
            )

        profiles.sort(
            key=lambda row: (
                float(row.get("adaptive_weight", 1.0) or 1.0),
                int(row.get("sample_size", 0) or 0),
                float(row.get("assignment_score", 0.0) or 0.0),
            ),
            reverse=True,
        )
        return profiles

    def adaptive_strategy_detail_for_symbol(self, symbol, strategy_name, timeframe=None, limit=8):
        normalized_symbol = self._normalize_strategy_symbol_key(symbol)
        normalized_strategy = str(strategy_name or "").strip()
        timeframe_value = str(timeframe or getattr(self, "time_frame", "1h") or "1h").strip() or "1h"
        if not normalized_symbol or not normalized_strategy:
            return {}

        trading_system = getattr(self, "trading_system", None)
        resolver = getattr(trading_system, "adaptive_trade_samples_for_strategy", None) if trading_system is not None else None
        if not callable(resolver):
            return {}

        try:
            return dict(
                resolver(
                    normalized_symbol,
                    normalized_strategy,
                    timeframe=timeframe_value,
                    limit=limit,
                )
                or {}
            )
        except Exception:
            self.logger.debug(
                "Unable to load adaptive strategy detail for %s / %s",
                normalized_symbol,
                normalized_strategy,
                exc_info=True,
            )
            return {}

    def adaptive_strategy_timeline_for_symbol(self, symbol, strategy_name, timeframe=None, limit=16):
        normalized_symbol = self._normalize_strategy_symbol_key(symbol)
        normalized_strategy = str(strategy_name or "").strip()
        timeframe_value = str(timeframe or getattr(self, "time_frame", "1h") or "1h").strip() or "1h"
        if not normalized_symbol or not normalized_strategy:
            return {}

        trading_system = getattr(self, "trading_system", None)
        resolver = getattr(trading_system, "adaptive_weight_timeline_for_strategy", None) if trading_system is not None else None
        if not callable(resolver):
            return {}

        try:
            return dict(
                resolver(
                    normalized_symbol,
                    normalized_strategy,
                    timeframe=timeframe_value,
                    limit=limit,
                )
                or {}
            )
        except Exception:
            self.logger.debug(
                "Unable to load adaptive strategy timeline for %s / %s",
                normalized_symbol,
                normalized_strategy,
                exc_info=True,
            )
            return {}

    def clear_symbol_strategy_assignment(self, symbol):
        normalized_symbol = self._normalize_strategy_symbol_key(symbol)
        removed = list(self.symbol_strategy_assignments.pop(normalized_symbol, []) or [])
        self._mark_symbol_strategy_assignment_locked(normalized_symbol, True)
        self._persist_strategy_symbol_state()

        trading_system = getattr(self, "trading_system", None)
        if trading_system is not None and hasattr(trading_system, "refresh_strategy_preferences"):
            try:
                trading_system.refresh_strategy_preferences()
            except Exception:
                pass
        return removed

    def assign_strategy_to_symbol(self, symbol, strategy_name, timeframe=None):
        normalized_symbol = self._normalize_strategy_symbol_key(symbol)
        normalized_strategy = Strategy.normalize_strategy_name(strategy_name)
        if not normalized_symbol:
            raise ValueError("Select a symbol before assigning a strategy.")
        if not normalized_strategy:
            raise ValueError("Select a valid strategy before assigning it to a symbol.")

        self.multi_strategy_enabled = True
        assigned = [
            {
                "strategy_name": normalized_strategy,
                "score": 1.0,
                "weight": 1.0,
                "symbol": normalized_symbol,
                "timeframe": str(timeframe or self.time_frame or "").strip(),
                "assignment_mode": "single",
                "assignment_source": "manual",
                "rank": 1,
                "total_profit": 0.0,
                "sharpe_ratio": 0.0,
                "win_rate": 0.0,
                "final_equity": 0.0,
                "max_drawdown": 0.0,
                "closed_trades": 0,
            }
        ]
        self.symbol_strategy_assignments[normalized_symbol] = assigned
        self._mark_symbol_strategy_assignment_locked(normalized_symbol, True)
        self._persist_strategy_symbol_state()

        trading_system = getattr(self, "trading_system", None)
        if trading_system is not None and hasattr(trading_system, "refresh_strategy_preferences"):
            try:
                trading_system.refresh_strategy_preferences()
            except Exception:
                pass
        return list(assigned)

    def active_strategy_weight_map(self):
        if not self.multi_strategy_enabled:
            return {Strategy.normalize_strategy_name(getattr(self, "strategy_name", "Trend Following")): 1.0}

        totals = {}
        for rows in (self.symbol_strategy_assignments or {}).values():
            for row in list(rows or []):
                if not isinstance(row, dict):
                    continue
                strategy_name = Strategy.normalize_strategy_name(row.get("strategy_name"))
                if not strategy_name:
                    continue
                totals[strategy_name] = totals.get(strategy_name, 0.0) + max(0.0001, float(row.get("weight", 0.0) or 0.0))
        if not totals:
            return {Strategy.normalize_strategy_name(getattr(self, "strategy_name", "Trend Following")): 1.0}
        total_weight = sum(totals.values())
        if total_weight <= 0:
            return {name: 1.0 / len(totals) for name in totals}
        return {name: value / total_weight for name, value in totals.items()}

    def broker_supports_hedging(self, broker=None):
        broker = broker or getattr(self, "broker", None)
        if broker is None:
            return False
        resolver = getattr(broker, "supports_hedging", None)
        if callable(resolver):
            try:
                return bool(resolver())
            except Exception:
                return False
        advertised = getattr(broker, "hedging_supported", None)
        if advertised is not None:
            return bool(advertised)
        exchange_name = str(
            getattr(broker, "exchange_name", None)
            or getattr(getattr(broker, "config", None), "exchange", None)
            or ""
        ).strip().lower()
        return exchange_name in {"oanda"}

    def hedging_is_active(self, broker=None):
        return bool(getattr(self, "hedging_enabled", True)) and self.broker_supports_hedging(broker)

    def assign_ranked_strategies_to_symbol(
        self,
        symbol,
        rankings,
        top_n=None,
        timeframe=None,
        assignment_source="manual",
        lock_symbol=None,
        refresh_preferences=True,
    ):
        normalized_symbol = self._normalize_strategy_symbol_key(symbol)
        limit = max(1, int(top_n or self.max_symbol_strategies or 1))
        self.max_symbol_strategies = limit
        assignment_source = str(assignment_source or "manual").strip().lower()
        if assignment_source not in {"manual", "auto"}:
            assignment_source = "manual"
        if lock_symbol is None:
            lock_symbol = assignment_source != "auto"

        cleaned_rows = self.save_ranked_strategies_for_symbol(
            normalized_symbol,
            rankings,
            timeframe=timeframe,
            assignment_source=assignment_source,
            persist=False,
        )
        top_rows = cleaned_rows[:limit]
        if not top_rows:
            self.symbol_strategy_assignments.pop(normalized_symbol, None)
            self._mark_symbol_strategy_assignment_locked(normalized_symbol, bool(lock_symbol))
            self._persist_strategy_symbol_state()
            return []

        self.multi_strategy_enabled = True
        weight_seed = [max(0.0001, float(item.get("score", 0.0) or 0.0)) for item in top_rows]
        total_weight = sum(weight_seed) or float(len(top_rows))
        assigned = []
        for index, item in enumerate(top_rows, start=1):
            assigned_item = dict(item)
            assigned_item["rank"] = index
            assigned_item["weight"] = float(weight_seed[index - 1] / total_weight)
            assigned_item["assignment_source"] = assignment_source
            assigned.append(assigned_item)
        self.symbol_strategy_assignments[normalized_symbol] = assigned
        self._mark_symbol_strategy_assignment_locked(normalized_symbol, bool(lock_symbol))
        self._persist_strategy_symbol_state()

        trading_system = getattr(self, "trading_system", None)
        if refresh_preferences and trading_system is not None and hasattr(trading_system, "refresh_strategy_preferences"):
            try:
                trading_system.refresh_strategy_preferences()
            except Exception:
                pass
        return assigned

    async def submit_market_chat_trade(
        self,
        symbol,
        side,
        amount,
        quantity_mode=None,
        order_type="market",
        price=None,
        stop_price=None,
        stop_loss=None,
        take_profit=None,
    ):
        broker = getattr(self, "broker", None)
        if broker is None:
            raise RuntimeError("Connect a broker before placing a trade from Sopotek Pilot.")

        quantity = self.normalize_trade_quantity(symbol, amount, quantity_mode=quantity_mode)
        amount_units = float(quantity["amount_units"])

        trading_system = getattr(self, "trading_system", None)
        execution_manager = getattr(trading_system, "execution_manager", None)
        if execution_manager is not None:
            order = await execution_manager.execute(
                symbol=symbol,
                side=side,
                amount=amount_units,
                type=order_type,
                price=price,
                stop_price=stop_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                source="chatgpt",
                strategy_name="Sopotek Pilot",
                reason="Sopotek Pilot trade command",
                confidence=1.0,
            )
        else:
            order = await broker.create_order(
                symbol=symbol,
                side=side,
                amount=amount_units,
                type=order_type,
                price=price,
                stop_price=stop_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
            )
            if isinstance(order, dict):
                order.setdefault("source", "chatgpt")
                order.setdefault("strategy_name", "Sopotek Pilot")
                order.setdefault("reason", "Sopotek Pilot trade command")
        if isinstance(order, dict):
            order.setdefault("requested_amount", float(quantity["requested_amount"]))
            order.setdefault("requested_quantity_mode", quantity["requested_mode"])
            order.setdefault("amount_units", amount_units)

        if not order:
            raise RuntimeError("The order was skipped by broker or safety checks.")
        return order

    def market_chat_position_summary(self, open_window=True):
        terminal = getattr(self, "terminal", None)
        if terminal is None or not hasattr(terminal, "_position_analysis_window_payload"):
            return None

        try:
            payload = terminal._position_analysis_window_payload() or {}
        except Exception:
            return None

        if open_window and hasattr(terminal, "_open_position_analysis_window"):
            try:
                terminal._open_position_analysis_window()
            except Exception:
                pass

        if not payload.get("available"):
            exchange = str(payload.get("exchange") or getattr(getattr(self, "broker", None), "exchange_name", "") or "-")
            return f"Position analysis is not available because no broker is currently connected. Last broker context: {exchange}."

        positions = list(payload.get("positions", []) or [])
        broker_label = str(payload.get("exchange") or "-").upper()
        if not positions:
            nav = payload.get("nav")
            balance = payload.get("balance")
            return (
                "Position Analysis window opened.\n"
                f"Broker: {broker_label} | Equity/NAV: {nav if nav is not None else '-'} | Balance/Cash: {balance if balance is not None else '-'}\n"
                "No open positions were found."
            )

        total_unrealized = sum(float(item.get("pnl", 0.0) or 0.0) for item in positions)
        total_realized = sum(float(item.get("realized_pnl", 0.0) or 0.0) for item in positions)
        total_margin = sum(float(item.get("margin_used", 0.0) or 0.0) for item in positions)
        total_value = sum(abs(float(item.get("value", 0.0) or 0.0)) for item in positions)
        winner = max(positions, key=lambda item: float(item.get("pnl", 0.0) or 0.0))
        loser = min(positions, key=lambda item: float(item.get("pnl", 0.0) or 0.0))
        largest = max(positions, key=lambda item: abs(float(item.get("value", 0.0) or 0.0)))
        long_count = sum(1 for item in positions if str(item.get("side", "")).lower() == "long")
        short_count = sum(1 for item in positions if str(item.get("side", "")).lower() == "short")
        closeout = payload.get("margin_closeout_percent")
        closeout_guard = self.margin_closeout_snapshot(payload.get("balances"))

        lines = [
            "Position Analysis window opened.",
            (
                f"Broker: {broker_label}"
                f" | Equity/NAV: {payload.get('nav', '-')}"
                f" | Balance/Cash: {payload.get('balance', '-')}"
                f" | Unrealized P/L: {total_unrealized:.2f}"
                f" | Realized P/L: {total_realized:.2f}"
            ),
            (
                f"Open positions: {len(positions)}"
                f" | Long: {long_count}"
                f" | Short: {short_count}"
                f" | Margin Used: {total_margin:.2f}"
                f" | Total Exposure: {total_value:.2f}"
            ),
            (
                f"Biggest winner: {winner.get('symbol', '-')} {float(winner.get('pnl', 0.0) or 0.0):.2f}"
                f" | Biggest loser: {loser.get('symbol', '-')} {float(loser.get('pnl', 0.0) or 0.0):.2f}"
            ),
            f"Largest exposure: {largest.get('symbol', '-')} value {float(largest.get('value', 0.0) or 0.0):.2f}",
        ]
        if closeout is not None:
            lines.append(f"Margin closeout percent: {closeout}")
        if closeout_guard.get("enabled"):
            lines.append(
                f"Margin closeout guard: {'BLOCKING' if closeout_guard.get('blocked') else 'monitoring'} "
                f"at {float(closeout_guard.get('threshold', 0.0) or 0.0):.2%}."
            )
        lines.append("Use Tools -> Position Analysis for the detailed table.")
        return "\n".join(lines)

    # Backward-compatible alias
    def market_chat_oanda_position_summary(self, open_window=True):
        return self.market_chat_position_summary(open_window=open_window)

    async def market_chat_quant_pm_summary(self, open_window=True):
        terminal = getattr(self, "terminal", None)
        if terminal is None or not hasattr(terminal, "_quant_pm_payload"):
            return None

        if open_window and hasattr(terminal, "_open_quant_pm_window"):
            try:
                terminal._open_quant_pm_window()
            except Exception:
                pass

        try:
            payload = await terminal._quant_pm_payload()
        except Exception:
            payload = {}

        if not payload.get("available"):
            broker_label = str(
                payload.get("exchange")
                or getattr(getattr(self, "broker", None), "exchange_name", "")
                or "-"
            ).upper()
            return (
                "Quant PM is not available yet because the trading system is not fully active. "
                f"Current broker context: {broker_label}."
            )

        def fmt_money(value):
            try:
                numeric = float(value)
            except Exception:
                return "-"
            return f"${numeric:,.2f}"

        def fmt_pct(value):
            try:
                numeric = float(value)
            except Exception:
                return "-"
            return f"{numeric:.2%}"

        strategy_rows = list(payload.get("strategy_rows") or [])
        position_rows = list(payload.get("position_rows") or [])
        allocation = dict(payload.get("allocation_snapshot") or {})
        risk = dict(payload.get("risk_snapshot") or {})
        institutional = dict(payload.get("institutional_status") or {})
        behavior = dict(payload.get("behavior_status") or {})
        health_attention = [self._plain_text(str(item)) for item in (payload.get("health_attention") or []) if str(item).strip()]
        top_strategy = strategy_rows[0] if strategy_rows else {}
        top_position = position_rows[0] if position_rows else {}
        correlation_rows = list(payload.get("correlation_rows") or [])
        account = self._plain_text(str(payload.get("account") or "Profile unavailable"))
        equity_label = fmt_money(payload.get("equity"))

        lines = [
            "Quant PM window opened.",
            (
                f"Broker: {str(payload.get('exchange') or '-').upper()} | "
                f"Account: {account} | "
                f"Mode: {payload.get('mode', 'PAPER')} | "
                f"Equity: {equity_label} | "
                f"Health: {self._plain_text(str(payload.get('health') or 'Not run'))}"
            ),
            (
                f"Allocator: {self._plain_text(str((payload.get('allocator_status') or {}).get('allocation_model') or '-'))} | "
                f"Target Weight: {fmt_pct(allocation.get('target_weight'))} | "
                f"Strategy: {self._plain_text(str(allocation.get('strategy_name') or top_strategy.get('strategy') or '-'))}"
            ),
            (
                f"Institutional Risk: {self._plain_text(str(risk.get('reason') or 'No recent decision'))} | "
                f"Trade VaR: {fmt_pct(risk.get('trade_var_pct'))} | "
                f"Gross Exposure: {fmt_pct(risk.get('gross_exposure_pct'))}"
            ),
            (
                f"Behavior Guard: {self._plain_text(str(behavior.get('summary') or behavior.get('state') or '-'))} | "
                f"Top strategy exposure: {self._plain_text(str(top_strategy.get('strategy') or '-'))} "
                f"{fmt_money(top_strategy.get('exposure'))}"
            ),
        ]
        if health_attention:
            lines.append(f"Health attention: {' | '.join(health_attention[:3])}")
        if top_position:
            lines.append(
                "Largest live position: "
                f"{self._plain_text(str(top_position.get('symbol') or '-'))} "
                f"{self._plain_text(str(top_position.get('direction') or '-'))} | "
                f"Exposure {fmt_money(top_position.get('exposure'))}"
            )
        if correlation_rows:
            anchor_row = correlation_rows[0]
            anchor_symbol = str(anchor_row.get("symbol") or "").upper().strip()
            peers = []
            for key, value in anchor_row.items():
                if key == "symbol":
                    continue
                try:
                    numeric = float(value or 0.0)
                except Exception:
                    numeric = 0.0
                peers.append((str(key), abs(numeric), numeric))
            peers.sort(key=lambda item: item[1], reverse=True)
            if peers:
                peer_symbol, _, corr_value = peers[0]
                lines.append(f"Highest visible correlation: {anchor_symbol} vs {peer_symbol} at {corr_value:.2f}.")
        if institutional:
            lines.append(
                f"Portfolio limits: VaR {fmt_pct(institutional.get('max_portfolio_risk'))}, "
                f"symbol cap {fmt_pct(institutional.get('max_symbol_exposure_pct'))}, "
                f"gross cap {fmt_pct(institutional.get('max_gross_exposure_pct'))}."
            )
        lines.append("Use Tools -> Quant PM for the full allocator, exposure, and correlation view.")
        return "\n".join(lines)

    def market_chat_command_guide(self):
        return (
            "Sopotek Pilot Commands\n"
            "\n"
            "General\n"
            "- help\n"
            "- show commands\n"
            "- show app status\n"
            "- take a screenshot\n"
            "\n"
            "Trading Control\n"
            "- start ai trading\n"
            "- stop ai trading\n"
            "- set ai scope all\n"
            "- set ai scope selected\n"
            "- set ai scope watchlist\n"
            "- activate kill switch\n"
            "- resume trading\n"
            "\n"
            "Windows and Tools\n"
            "- open settings\n"
            "- open system health\n"
            "- open recommendations\n"
            "- open performance\n"
            "- open quant pm\n"
            "- open ml research\n"
            "- open closed journal\n"
            "- open journal review\n"
            "- open logs\n"
            "- open position analysis\n"
            "- open oanda positions\n"
            "- open documentation\n"
            "- open api docs\n"
            "- open license\n"
            "- open about\n"
            "- open manual trade\n"
            "\n"
            "Refresh Actions\n"
            "- refresh markets\n"
            "- reload balances\n"
            "- refresh chart\n"
            "- refresh orderbook\n"
            "\n"
            "Telegram\n"
            "- show telegram status\n"
            "- enable telegram\n"
            "- disable telegram\n"
            "- restart telegram\n"
            "- send telegram test message\n"
            "\n"
            "Trading Commands\n"
            "- trade buy EUR/USD amount 0.01 lots confirm\n"
            "- trade sell GBP/USD amount 2000 type limit price 1.2710 sl 1.2750 tp 1.2620 confirm\n"
            "- trade buy BTC/USDT amount 0.25 type stop_limit trigger 65010 price 64990 confirm\n"
            "- cancel order id 123456 confirm\n"
            "- cancel orders for EUR/USD confirm\n"
            "- close position EUR/USD confirm\n"
            "- close position EUR/USD amount 0.01 lots confirm\n"
            "\n"
            "Analysis\n"
            "- show bug summary\n"
            "- show error log\n"
            "- show quant pm summary\n"
            "- show my broker position analysis with equity, NAV, and P/L\n"
            "- show trade history analysis\n"
            "- summarize current recommendations and why\n"
            "- summarize the latest news affecting my active symbols"
        )

    def _market_chat_log_file_paths(self):
        root_dir = Path(__file__).resolve().parents[2]
        candidates = []
        seen = set()

        for directory in (
            Path("logs"),
            Path("src") / "logs",
            root_dir / "logs",
        ):
            try:
                resolved = directory.resolve()
            except Exception:
                resolved = directory
            if not resolved.exists() or not resolved.is_dir():
                continue
            for name in ("native_crash.log", "errors.log", "system.log", "app.log"):
                path = resolved / name
                if not path.exists():
                    continue
                key = str(path).lower()
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(path)

        return candidates

    def _tail_log_lines(self, path, max_lines=240):
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return []
        lines = [line.rstrip() for line in text.splitlines()]
        lines = [line for line in lines if line.strip()]
        if max_lines <= 0:
            return lines
        return lines[-max_lines:]

    def _format_log_timestamp(self, path):
        try:
            return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return "unknown time"

    def _market_chat_native_crash_summary(self, path):
        lines = self._tail_log_lines(path, max_lines=320)
        if not lines:
            return None

        frame_pattern = re.compile(r'File "([^"]+)", line (\d+) in ([^\s]+)')
        frames = []
        capture = False
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("Current thread "):
                capture = True
                continue
            if not capture:
                continue
            if stripped.startswith("Current thread's C stack"):
                break
            match = frame_pattern.search(stripped)
            if match:
                filename = Path(match.group(1)).name
                frames.append(f"{filename}:{match.group(2)} in {match.group(3)}")

        if frames:
            summary = (
                f"{path.name} updated {self._format_log_timestamp(path)}: native crash trace captured. "
                f"Top frame {frames[0]}."
            )
            if len(frames) > 1:
                summary += f" Next frame {frames[1]}."
            return summary

        last_line = self._plain_text(lines[-1])
        if not last_line:
            return None
        return f"{path.name} updated {self._format_log_timestamp(path)}: {last_line}"

    def _market_chat_regular_log_summary(self, path, max_entries=2):
        lines = self._tail_log_lines(path, max_lines=320)
        if not lines:
            return None

        include_tokens = (
            "uncaught exception",
            "traceback",
            "error calling python override",
            "exception",
            "critical",
            "fatal",
            "cleanup error",
            "task ",
            "native crash",
        )
        ignore_tokens = (
            "trade rejected by portfolio allocator",
            "trade rejected by institutional risk engine",
            "trade rejected by risk engine",
            "skipping ",
            "using polling market data",
            "broker ready",
            "initializing broker",
        )

        matches = []
        seen = set()
        for line in reversed(lines):
            lowered = line.lower()
            if not any(token in lowered for token in include_tokens):
                continue
            if any(token in lowered for token in ignore_tokens):
                continue
            cleaned = self._plain_text(line)
            if not cleaned:
                continue
            key = cleaned.lower()
            if key in seen:
                continue
            seen.add(key)
            matches.append(cleaned)
            if len(matches) >= max_entries:
                break

        if not matches:
            return None

        matches.reverse()
        return (
            f"{path.name} updated {self._format_log_timestamp(path)}: "
            + " | ".join(matches)
        )

    def market_chat_error_log_summary(self, open_window=True, max_entries=4):
        if open_window:
            try:
                self.market_chat_open_window("logs")
            except Exception:
                pass

        paths = self._market_chat_log_file_paths()
        if not paths:
            return "I could not find any local log files yet."

        findings = []
        quiet_files = []
        for path in paths:
            if path.name == "native_crash.log":
                summary = self._market_chat_native_crash_summary(path)
            else:
                summary = self._market_chat_regular_log_summary(path)
            if summary:
                findings.append(summary)
            else:
                quiet_files.append(path.name)

        if not findings:
            quiet_text = ", ".join(quiet_files) if quiet_files else "available logs"
            return (
                "I checked the local logs and did not find recent crash or exception signatures. "
                f"Quiet logs: {quiet_text}."
            )

        lines = ["Bug summary from local logs:"]
        for item in findings[: max(1, int(max_entries or 4))]:
            lines.append(f"- {item}")
        if quiet_files:
            lines.append(f"No recent bug signatures in: {', '.join(quiet_files[:4])}.")
        lines.append("Use Tools -> Logs for the full log view.")
        return "\n".join(lines)

    def market_chat_set_ai_trading(self, enabled):
        terminal = getattr(self, "terminal", None)
        if terminal is None:
            return False, "Open the trading terminal first."

        setter = getattr(terminal, "_set_autotrading_enabled", None)
        if not callable(setter):
            return False, "AI trading controls are not available in this terminal session."

        target = bool(enabled)
        is_active = bool(getattr(terminal, "autotrading_enabled", False))
        scope_label = getattr(terminal, "_autotrade_scope_label", lambda: "All Symbols")()
        scope_value = str(getattr(terminal, "autotrade_scope_value", "all") or "all").lower()

        if target and is_active:
            return True, f"AI trading is already ON. Scope: {scope_label}."
        if (not target) and (not is_active):
            return True, "AI trading is already OFF."

        if target and bool(getattr(self, "is_emergency_stop_active", lambda: False)()):
            return False, "Emergency lock is active. Clear the kill switch before enabling AI trading."

        if target and not getattr(self, "trading_system", None):
            return False, "AI trading cannot start because the trading system is not initialized yet."

        if target:
            active_symbols = []
            resolver = getattr(self, "get_active_autotrade_symbols", None)
            if callable(resolver):
                try:
                    active_symbols = list(resolver() or [])
                except Exception:
                    active_symbols = []
            if not active_symbols:
                if scope_value == "watchlist":
                    return False, "AI trading cannot start because the watchlist scope has no checked symbols."
                if scope_value == "selected":
                    return False, "AI trading cannot start because there is no active selected symbol yet."
                return False, "AI trading cannot start because no symbols are available for the chosen AI scope."

        setter(target)
        is_active = bool(getattr(terminal, "autotrading_enabled", False))
        if target and is_active:
            return True, f"AI trading is ON. Scope: {scope_label}."
        if (not target) and (not is_active):
            return True, "AI trading is OFF."

        return False, "AI trading state did not change. Check the terminal for more details."

    async def market_chat_app_status_summary(self, show_panel=True):
        terminal = getattr(self, "terminal", None)
        if terminal is not None and show_panel:
            dock = getattr(terminal, "system_status_dock", None)
            try:
                if dock is not None and not dock.isVisible():
                    terminal._show_system_status_panel()
                elif dock is not None:
                    dock.raise_()
                    dock.activateWindow()
            except Exception:
                pass

        status_lines = ["System status opened."]
        try:
            status_lines.append(self._plain_text(await self.telegram_status_text()))
        except Exception:
            status_lines.append("Runtime status is available in the System Status panel.")

        behavior = self.get_behavior_guard_status() or {}
        if behavior:
            status_lines.append(
                f"Behavior Guard: {self._plain_text(behavior.get('summary') or 'Active')} | "
                f"Reason: {self._plain_text(behavior.get('reason') or 'No active restriction')}"
            )
        status_lines.append(f"Health Check: {self.get_health_check_summary()}")
        return "\n".join(line for line in status_lines if line)

    def _market_chat_open_orders_snapshot(self):
        terminal = getattr(self, "terminal", None)
        if terminal is None:
            return []
        snapshot = list(getattr(terminal, "_latest_open_orders_snapshot", []) or [])
        normalized = []
        normalizer = getattr(terminal, "_normalize_open_order_entry", None)
        for item in snapshot:
            if callable(normalizer):
                try:
                    entry = normalizer(item)
                except Exception:
                    entry = None
                if entry is not None:
                    payload = dict(entry)
                    payload["_raw"] = item
                    normalized.append(payload)
                    continue
            if isinstance(item, dict):
                payload = dict(item)
                payload.setdefault("_raw", item)
                normalized.append(payload)
        return normalized

    def _market_chat_positions_snapshot(self):
        terminal = getattr(self, "terminal", None)
        if terminal is None:
            return []
        snapshot = list(getattr(terminal, "_latest_positions_snapshot", []) or [])
        normalized = []
        normalizer = getattr(terminal, "_normalize_position_entry", None)
        for item in snapshot:
            if callable(normalizer):
                try:
                    entry = normalizer(item)
                except Exception:
                    entry = None
                if entry is not None:
                    payload = dict(entry)
                    payload["_raw"] = item
                    normalized.append(payload)
                    continue
            if isinstance(item, dict):
                payload = dict(item)
                payload.setdefault("_raw", item)
                normalized.append(payload)
        return normalized

    async def cancel_market_chat_order(self, order_id=None, symbol=None, cancel_all_for_symbol=False):
        broker = getattr(self, "broker", None)
        if broker is None:
            raise RuntimeError("Connect a broker before canceling orders from Sopotek Pilot.")

        normalized_symbol = str(symbol or "").strip().upper()
        normalized_id = str(order_id or "").strip()
        orders = self._market_chat_open_orders_snapshot()
        matches = []
        for order in orders:
            candidate_id = str(order.get("order_id") or order.get("id") or "").strip()
            candidate_symbol = str(order.get("symbol") or "").strip().upper()
            if normalized_id and candidate_id == normalized_id:
                matches.append(order)
            elif normalized_symbol and candidate_symbol == normalized_symbol:
                matches.append(order)

        if normalized_id and not matches:
            raise RuntimeError(f"No open order with id {normalized_id} was found.")
        if normalized_symbol and not matches:
            raise RuntimeError(f"No open orders for {normalized_symbol} were found.")
        if not normalized_id and not normalized_symbol:
            raise RuntimeError("Cancel order command needs an order id or symbol.")
        if normalized_symbol and len(matches) > 1 and not cancel_all_for_symbol and not normalized_id:
            ids = ", ".join(str(item.get("order_id") or item.get("id") or "-") for item in matches[:5])
            raise RuntimeError(
                f"Multiple open orders found for {normalized_symbol}. Use 'cancel orders for {normalized_symbol} confirm' or specify an order id. Matches: {ids}"
            )

        targets = matches if cancel_all_for_symbol or normalized_id else matches[:1]
        results = []
        for order in targets:
            target_id = str(order.get("order_id") or order.get("id") or "").strip()
            target_symbol = str(order.get("symbol") or normalized_symbol or "").strip().upper() or None
            if not target_id:
                continue
            if hasattr(broker, "cancel_order"):
                try:
                    result = await broker.cancel_order(target_id, symbol=target_symbol)
                except TypeError:
                    result = await broker.cancel_order(target_id)
            else:
                raise RuntimeError("Current broker does not support cancel_order.")
            results.append(result if result is not None else {"id": target_id, "symbol": target_symbol})

        terminal = getattr(self, "terminal", None)
        if terminal is not None and hasattr(terminal, "_schedule_open_orders_refresh"):
            terminal._schedule_open_orders_refresh()
        return results

    async def close_market_chat_position(self, symbol, amount=None, quantity_mode=None, position=None, position_side=None, position_id=None):
        broker = getattr(self, "broker", None)
        if broker is None:
            raise RuntimeError("Connect a broker before closing positions from Sopotek Pilot.")

        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol:
            raise RuntimeError("Close position command needs a symbol.")

        positions = self._market_chat_positions_snapshot()
        selected_position = position if isinstance(position, dict) else None
        selected_side = str(
            position_side
            or (selected_position or {}).get("position_side")
            or (selected_position or {}).get("side")
            or ""
        ).strip().lower()
        selected_id = str(
            position_id
            or (selected_position or {}).get("position_id")
            or (selected_position or {}).get("id")
            or ""
        ).strip().lower()

        matches = [
            item
            for item in positions
            if str(item.get("symbol") or "").strip().upper() == normalized_symbol
        ]
        if selected_id:
            matches = [
                item
                for item in matches
                if str(item.get("position_id") or item.get("id") or "").strip().lower() == selected_id
            ]
        if selected_side:
            matches = [
                item
                for item in matches
                if str(item.get("position_side") or item.get("side") or "").strip().lower() == selected_side
            ]
        if selected_position is not None and not matches:
            matches = [selected_position]
        if len(matches) > 1 and self.hedging_is_active(broker):
            raise RuntimeError(
                f"Multiple hedge legs are open for {normalized_symbol}. Choose the specific LONG or SHORT position from Positions or Position Analysis."
            )
        target = matches[0] if matches else None
        if target is None:
            raise RuntimeError(f"No open position for {normalized_symbol} was found.")

        resolved_amount = None
        if amount is not None:
            try:
                quantity = self.normalize_trade_quantity(normalized_symbol, amount, quantity_mode=quantity_mode)
            except ValueError as exc:
                raise RuntimeError(str(exc)) from exc
            resolved_amount = float(quantity["amount_units"])

        result = None
        if hasattr(broker, "close_position"):
            try:
                result = await broker.close_position(
                    normalized_symbol,
                    amount=resolved_amount,
                    order_type="market",
                    position=target,
                    position_side=selected_side or target.get("position_side") or target.get("side"),
                    position_id=selected_id or target.get("position_id") or target.get("id"),
                )
            except TypeError:
                result = await broker.close_position(normalized_symbol, amount=resolved_amount)

        if result is None:
            side = str(target.get("side") or "").strip().lower()
            close_side = "buy" if side in {"short", "sell"} else "sell"
            fallback_amount = resolved_amount
            if fallback_amount is None:
                fallback_amount = abs(float(target.get("amount", target.get("units", 0.0)) or 0.0))
            if fallback_amount <= 0:
                raise RuntimeError(f"Unable to determine a valid close amount for {normalized_symbol}.")
            result = await broker.create_order(
                symbol=normalized_symbol,
                side=close_side,
                amount=fallback_amount,
                type="market",
                params={"positionFill": "REDUCE_ONLY"} if self.hedging_is_active(broker) else None,
            )

        terminal = getattr(self, "terminal", None)
        if terminal is not None:
            if hasattr(terminal, "_schedule_positions_refresh"):
                terminal._schedule_positions_refresh()
            if hasattr(terminal, "_schedule_open_orders_refresh"):
                terminal._schedule_open_orders_refresh()
        return result

    def market_chat_open_window(self, target):
        terminal = getattr(self, "terminal", None)
        if terminal is None:
            return "Terminal UI is not available."

        target_key = str(target or "").strip().lower()
        if target_key in {"status", "system status"}:
            dock = getattr(terminal, "system_status_dock", None)
            if dock is not None and not dock.isVisible():
                terminal._show_system_status_panel()
            elif dock is not None:
                dock.raise_()
                dock.activateWindow()
            return "System Status panel opened."

        mapping = {
            "settings": ("_open_settings", "Settings window opened."),
            "system health": ("_open_system_health_window", "System Health window opened."),
            "recommendations": ("_open_recommendations_window", "Trade Recommendations window opened."),
            "performance": ("_open_performance", "Performance Analytics window opened."),
            "quant pm": ("_open_quant_pm_window", "Quant PM window opened."),
            "quant dashboard": ("_open_quant_pm_window", "Quant PM window opened."),
            "ml research": ("_open_ml_research_window", "ML Research Lab window opened."),
            "closed journal": ("_open_closed_journal_window", "Closed Trade Journal window opened."),
            "journal review": ("_open_trade_journal_review_window", "Journal Review window opened."),
            "logs": ("_open_logs", "System Logs window opened."),
            "ml monitor": ("_open_ml_monitor", "ML Signal Monitor window opened."),
            "position analysis": ("_open_position_analysis_window", "Position Analysis window opened."),
            "oanda positions": ("_open_position_analysis_window", "Position Analysis window opened."),
            "documentation": ("_open_docs", "Documentation window opened."),
            "api docs": ("_open_api_docs", "API Reference window opened."),
            "license": ("_open_license_manager", "License window opened."),
            "about": ("_show_about", "About window opened."),
            "manual trade": ("_open_manual_trade", "Manual Trade window opened."),
            "market chat": ("_open_market_chat_window", "Sopotek Pilot window opened."),
        }
        method_name, success_message = mapping.get(target_key, (None, None))
        if not method_name or not hasattr(terminal, method_name):
            return None
        getattr(terminal, method_name)()
        return success_message

    def _available_market_chat_symbols(self):
        ordered = []
        def add_symbol(symbol):
            normalized = str(symbol or "").upper().strip()
            if normalized and normalized not in ordered:
                ordered.append(normalized)

        for symbol in list(getattr(self, "symbols", []) or []):
            add_symbol(symbol)

        broker = getattr(self, "broker", None)
        if broker is not None:
            for symbol in list(getattr(broker, "symbols", []) or []):
                add_symbol(symbol)
            markets = getattr(getattr(broker, "exchange", None), "markets", None)
            if isinstance(markets, dict):
                for symbol in markets.keys():
                    add_symbol(symbol)

        terminal = getattr(self, "terminal", None)
        current_symbol = None
        if terminal is not None and hasattr(terminal, "_current_chart_symbol"):
            try:
                current_symbol = terminal._current_chart_symbol()
            except Exception:
                current_symbol = None
        if current_symbol:
            normalized = str(current_symbol).upper().strip()
            if normalized:
                if normalized in ordered:
                    ordered.remove(normalized)
                ordered.insert(0, normalized)
        return ordered

    def _extract_market_chat_timeframe(self, question):
        lowered = str(question or "").strip().lower()
        match = re.search(r"\b(1m|3m|5m|15m|30m|45m|1h|2h|4h|6h|8h|12h|1d|3d|1w)\b", lowered)
        if match:
            return match.group(1)
        return str(getattr(self, "time_frame", "1h") or "1h").strip() or "1h"

    def _resolve_market_chat_symbol(self, question):
        text = str(question or "").strip().upper()
        if not text:
            return ""

        available = self._available_market_chat_symbols()
        available_set = set(available)
        available_compact = {
            re.sub(r"[^A-Z0-9]", "", symbol): symbol
            for symbol in available
            if str(symbol or "").strip()
        }

        for base, quote in re.findall(r"\b([A-Z0-9]{1,20})\s*[/:_-]\s*([A-Z0-9]{1,20})\b", text):
            candidate = f"{base}/{quote}"
            if candidate in available_set:
                return candidate
            compact_candidate = re.sub(r"[^A-Z0-9]", "", candidate)
            if compact_candidate in available_compact:
                return available_compact[compact_candidate]
            return candidate

        for token in re.findall(r"\b[A-Z0-9][A-Z0-9._/-]{0,23}\b", text):
            normalized_token = token.strip().upper()
            if normalized_token in available_set:
                return normalized_token
            compact_token = re.sub(r"[^A-Z0-9]", "", normalized_token)
            if compact_token in available_compact:
                return available_compact[compact_token]

        collapsed_available = {}
        for symbol in available:
            collapsed_available[re.sub(r"[^A-Z0-9]", "", symbol)] = symbol
        for token in re.findall(r"\b[A-Z0-9]{4,24}\b", text):
            if token in collapsed_available:
                return collapsed_available[token]

        base_candidates = []
        for token in re.findall(r"\b[A-Z0-9]{1,20}\b", text):
            matches = [symbol for symbol in available if symbol.startswith(f"{token}/")]
            if matches:
                ranked = self._prioritize_symbols_for_trading(matches, top_n=len(matches))
                if ranked:
                    base_candidates.append(ranked[0])
        return base_candidates[0] if base_candidates else ""

    def _should_answer_market_snapshot(self, question, symbol):
        normalized_symbol = str(symbol or "").upper().strip()
        if not normalized_symbol:
            return False

        lowered = str(question or "").strip().lower()
        if not lowered:
            return False

        blocked_tokens = (
            "trade ",
            "cancel order",
            "cancel orders",
            "close position",
            "open settings",
            "open system health",
            "open recommendations",
            "open performance",
            "open quant pm",
            "open logs",
            "open ml monitor",
            "open position analysis",
            "take screenshot",
            "capture screenshot",
            "telegram",
        )
        if any(token in lowered for token in blocked_tokens):
            return False

        snapshot_tokens = (
            "price",
            "quote",
            "scan",
            "technical",
            "analysis",
            "analyze",
            "analyse",
            "trend",
            "rsi",
            "ema",
            "support",
            "resistance",
            "snapshot",
            "market",
            "what about",
            "how is",
            "what do you think",
        )
        if any(token in lowered for token in snapshot_tokens):
            return True

        compact_question = re.sub(r"[^a-z0-9]", "", lowered)
        compact_symbol = normalized_symbol.lower().replace("/", "")
        if compact_question in {compact_symbol, normalized_symbol.lower().replace("/", ""), normalized_symbol.lower()}:
            return True

        explicit_pair = normalized_symbol.lower() in lowered or compact_symbol in compact_question
        return explicit_pair and len(lowered.split()) <= 4

    @staticmethod
    def _market_chat_rsi(close_series, period=14):
        if close_series is None or len(close_series) < 2:
            return None
        delta = close_series.diff()
        gain = delta.clip(lower=0.0)
        loss = -delta.clip(upper=0.0)
        avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
        avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
        last_gain = float(avg_gain.iloc[-1]) if len(avg_gain) else 0.0
        last_loss = float(avg_loss.iloc[-1]) if len(avg_loss) else 0.0
        if last_loss <= 0:
            if last_gain <= 0:
                return 50.0
            return 100.0
        rs = last_gain / last_loss
        return 100.0 - (100.0 / (1.0 + rs))

    async def market_chat_market_snapshot(self, symbol, timeframe=None):
        normalized_symbol = str(symbol or "").upper().strip()
        if not normalized_symbol:
            return None

        resolved_timeframe = str(timeframe or getattr(self, "time_frame", "1h") or "1h").strip() or "1h"
        tick = await self._safe_fetch_ticker(normalized_symbol)
        candles = await self._safe_fetch_ohlcv(normalized_symbol, timeframe=resolved_timeframe, limit=120)

        if not isinstance(candles, list) or not candles:
            if isinstance(tick, dict):
                last_price = float(tick.get("price") or tick.get("last") or tick.get("bid") or tick.get("ask") or 0.0)
                if last_price > 0:
                    bid = float(tick.get("bid") or 0.0)
                    ask = float(tick.get("ask") or 0.0)
                    spread_pct = ((ask - bid) / last_price * 100.0) if bid > 0 and ask > 0 and last_price > 0 else None
                    lines = [
                        f"{normalized_symbol} snapshot ({resolved_timeframe})",
                        f"Last: {last_price:,.4f}",
                    ]
                    if bid > 0 and ask > 0:
                        spread_line = f"Bid/Ask: {bid:,.4f} / {ask:,.4f}"
                        if spread_pct is not None:
                            spread_line += f" | Spread: {spread_pct:.3f}%"
                        lines.append(spread_line)
                    lines.append("Technical scan is unavailable because candle history could not be loaded yet.")
                    return "\n".join(lines)
            return (
                f"I couldn't pull a live {normalized_symbol} snapshot right now. "
                f"Market data status: {self.get_market_stream_status()}."
            )

        frame = pd.DataFrame(candles)
        if frame.shape[1] < 6:
            return f"{normalized_symbol} data loaded, but the candle format is incomplete for analysis."
        frame = frame.iloc[:, :6].copy()
        frame.columns = ["timestamp", "open", "high", "low", "close", "volume"]
        for column in ("open", "high", "low", "close", "volume"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.dropna(subset=["high", "low", "close"])
        if frame.empty:
            return f"{normalized_symbol} candle history is currently empty after cleanup."

        closes = frame["close"]
        highs = frame["high"]
        lows = frame["low"]
        latest_close = float(closes.iloc[-1])

        bid = ask = 0.0
        if isinstance(tick, dict):
            bid = float(tick.get("bid") or 0.0)
            ask = float(tick.get("ask") or 0.0)
            tick_last = float(tick.get("price") or tick.get("last") or 0.0)
            if tick_last > 0:
                latest_close = tick_last

        ema_fast = float(closes.ewm(span=min(20, max(len(closes), 2)), adjust=False).mean().iloc[-1])
        ema_slow_span = 50 if len(closes) >= 50 else max(21, min(len(closes), 50))
        ema_slow = float(closes.ewm(span=ema_slow_span, adjust=False).mean().iloc[-1])
        rsi = self._market_chat_rsi(closes, period=14)

        window = min(20, len(frame))
        support = float(lows.tail(window).min())
        resistance = float(highs.tail(window).max())

        previous_close = float(closes.iloc[-2]) if len(closes) >= 2 else latest_close
        change_pct = ((latest_close - previous_close) / previous_close * 100.0) if previous_close else 0.0

        if latest_close >= ema_fast >= ema_slow:
            trend = "Bullish"
        elif latest_close <= ema_fast <= ema_slow:
            trend = "Bearish"
        else:
            trend = "Mixed"

        lines = [f"{normalized_symbol} snapshot ({resolved_timeframe})", f"Last: {latest_close:,.4f} | Change: {change_pct:+.2f}%"]
        if bid > 0 and ask > 0:
            spread_pct = ((ask - bid) / latest_close * 100.0) if latest_close > 0 else 0.0
            lines.append(f"Bid/Ask: {bid:,.4f} / {ask:,.4f} | Spread: {spread_pct:.3f}%")
        lines.append(f"Trend: {trend} | EMA20: {ema_fast:,.4f} | EMA50: {ema_slow:,.4f}")
        if rsi is not None:
            lines.append(f"RSI14: {float(rsi):.1f}")
        lines.append(f"Support/Resistance ({window} candles): {support:,.4f} / {resistance:,.4f}")
        return "\n".join(lines)

    async def handle_market_chat_action(self, question):
        lowered = str(question or "").strip().lower()
        if not lowered:
            return None

        if lowered in {"help", "commands", "show commands", "list commands"} or any(
            token in lowered
            for token in (
                "what can you do",
                "what can sopotek pilot do",
                "how do i control",
                "manage the app",
                "control the app",
                "list of command",
                "list of commands",
                "command list",
            )
        ):
            return self.market_chat_command_guide()

        if "telegram" not in lowered and any(
            token in lowered for token in ("show app status", "show status", "app status", "system status", "status summary")
        ):
            return await self.market_chat_app_status_summary(show_panel=True)

        if any(
            token in lowered
            for token in (
                "show bug summary",
                "bug summary",
                "show bugs",
                "recent bugs",
                "any bugs",
                "what bugs",
                "show error log",
                "error log",
                "show crash log",
                "crash log",
                "recent errors",
            )
        ):
            return self.market_chat_error_log_summary(open_window=True)

        window_targets = [
            ("open settings", "settings"),
            ("open preferences", "settings"),
            ("show settings", "settings"),
            ("open system health", "system health"),
            ("show system health", "system health"),
            ("open recommendations", "recommendations"),
            ("show recommendations", "recommendations"),
            ("open performance", "performance"),
            ("show performance", "performance"),
            ("open quant pm", "quant pm"),
            ("show quant pm", "quant pm"),
            ("open quant dashboard", "quant dashboard"),
            ("show quant dashboard", "quant dashboard"),
            ("open ml research", "ml research"),
            ("show ml research", "ml research"),
            ("open closed journal", "closed journal"),
            ("show closed journal", "closed journal"),
            ("open journal review", "journal review"),
            ("show journal review", "journal review"),
            ("open logs", "logs"),
            ("show logs", "logs"),
            ("open ml monitor", "ml monitor"),
            ("show ml monitor", "ml monitor"),
            ("open position analysis", "position analysis"),
            ("show position analysis", "position analysis"),
            ("open oanda positions", "oanda positions"),
            ("show oanda positions", "oanda positions"),
            ("open documentation", "documentation"),
            ("show documentation", "documentation"),
            ("open api docs", "api docs"),
            ("show api docs", "api docs"),
            ("open license", "license"),
            ("show license", "license"),
            ("open about", "about"),
            ("show about", "about"),
            ("open manual trade", "manual trade"),
            ("show manual trade", "manual trade"),
        ]
        for token, target in window_targets:
            if token in lowered:
                message = self.market_chat_open_window(target)
                if message:
                    return message

        terminal = getattr(self, "terminal", None)
        if terminal is not None:
            scope_match = re.search(
                r"(?:set|change|switch)\s+(?:ai\s+)?scope\s+(all|selected|selected symbol|watchlist)",
                lowered,
            )
            if scope_match:
                scope = scope_match.group(1).replace("selected symbol", "selected")
                if hasattr(terminal, "_apply_autotrade_scope"):
                    terminal._apply_autotrade_scope(scope)
                    return f"AI scope set to {getattr(terminal, '_autotrade_scope_label', lambda: scope.title())()}."

            if any(
                token in lowered
                for token in (
                    "start ai trading",
                    "enable ai trading",
                    "turn on ai trading",
                    "start auto trading",
                    "enable auto trading",
                    "turn on auto trading",
                    "start the ai trading",
                )
            ):
                _ok, message = self.market_chat_set_ai_trading(True)
                return message

            if any(
                token in lowered
                for token in (
                    "stop ai trading",
                    "disable ai trading",
                    "turn off ai trading",
                    "stop auto trading",
                    "disable auto trading",
                    "turn off auto trading",
                    "pause ai trading",
                )
            ):
                _ok, message = self.market_chat_set_ai_trading(False)
                return message

            if any(token in lowered for token in ("activate kill switch", "engage kill switch", "emergency stop", "trigger kill switch")):
                if hasattr(terminal, "_activate_emergency_stop_async"):
                    await terminal._activate_emergency_stop_async()
                    return "Emergency kill switch engaged. Auto trading is OFF, open orders are being canceled, and tracked positions are being closed."

            if any(token in lowered for token in ("resume trading", "clear kill switch", "disable kill switch", "resume after kill switch")):
                if bool(getattr(self, "is_emergency_stop_active", lambda: False)()):
                    self.clear_emergency_stop()
                    if hasattr(terminal, "_update_kill_switch_button"):
                        terminal._update_kill_switch_button()
                    if hasattr(terminal, "_refresh_terminal"):
                        terminal._refresh_terminal()
                    return "Emergency lock cleared. Auto trading remains OFF until you enable it again."
                return "Emergency lock is not active."

            if any(token in lowered for token in ("refresh markets", "reload markets", "update markets")):
                if hasattr(terminal, "_refresh_markets"):
                    terminal._refresh_markets()
                    return "Market Watch refresh requested."

            if any(token in lowered for token in ("reload balances", "refresh balances", "reload balance", "refresh balance")):
                if hasattr(terminal, "_reload_balance"):
                    terminal._reload_balance()
                    return "Balance reload requested."

            if any(token in lowered for token in ("refresh chart", "reload chart")):
                if hasattr(terminal, "_refresh_active_chart_data"):
                    terminal._refresh_active_chart_data()
                    return "Active chart refresh requested."

            if any(token in lowered for token in ("refresh orderbook", "reload orderbook")):
                if hasattr(terminal, "_refresh_active_orderbook"):
                    terminal._refresh_active_orderbook()
                    return "Orderbook refresh requested."

        cancel_symbol_match = re.search(
            r"(?:cancel)\s+orders?\s+(?:for|on)\s+([A-Za-z0-9_:/.-]+)",
            lowered,
        )
        cancel_id_match = re.search(
            r"(?:cancel)\s+orders?\s+(?:id\s+)?([A-Za-z0-9_-]{3,})",
            lowered,
        )
        if cancel_symbol_match or ("cancel order" in lowered or "cancel orders" in lowered):
            symbol = cancel_symbol_match.group(1).upper() if cancel_symbol_match else None
            order_id = None
            if not symbol and cancel_id_match:
                token = cancel_id_match.group(1)
                if token.upper() not in {"FOR", "ON", "ALL"}:
                    order_id = token
            if "confirm" not in lowered:
                target_text = f"symbol={symbol}" if symbol else f"order_id={order_id or '?'}"
                return (
                    "Cancel-order command detected but not executed.\n"
                    "Add the word CONFIRM to execute it.\n"
                    f"Parsed target: {target_text}"
                )
            try:
                results = await self.cancel_market_chat_order(
                    order_id=order_id,
                    symbol=symbol,
                    cancel_all_for_symbol=bool(symbol),
                )
            except Exception as exc:
                return f"Cancel-order command failed: {exc}"
            count = len(results or [])
            if symbol:
                return f"Canceled {count} open order(s) for {symbol}."
            return f"Canceled order {order_id}." if count else f"No cancellation was performed for order {order_id}."

        close_match = re.search(
            r"(?:close)\s+(?:(long|short)\s+)?position\s+([A-Za-z0-9_:/.-]+)(?:\s+(?:amount|size|units)\s+([-+]?\d*\.?\d+)(?:\s+(lots?|units?))?)?",
            lowered,
        )
        if close_match:
            position_side, symbol, amount_text, quantity_mode = close_match.groups()
            if "confirm" not in lowered:
                side_text = f" side={position_side.upper()}" if position_side else ""
                mode_text = f" {quantity_mode}" if quantity_mode else ""
                return (
                    "Close-position command detected but not executed.\n"
                    "Add the word CONFIRM to execute it.\n"
                    f"Parsed target: symbol={symbol.upper()}{side_text} amount={amount_text or 'full position'}{mode_text}"
                )
            amount = float(amount_text) if amount_text else None
            try:
                try:
                    result = await self.close_market_chat_position(
                        symbol.upper(),
                        amount=amount,
                        quantity_mode=quantity_mode,
                        position_side=position_side,
                    )
                except TypeError:
                    result = await self.close_market_chat_position(
                        symbol.upper(),
                        amount=amount,
                        quantity_mode=quantity_mode,
                    )
            except Exception as exc:
                return f"Close-position command failed: {exc}"
            status = str(result.get("status") or "submitted").replace("_", " ").upper() if isinstance(result, dict) else "SUBMITTED"
            order_id = str(result.get("order_id") or result.get("id") or "-") if isinstance(result, dict) else "-"
            amount_label = f"{amount} {quantity_mode}" if amount is not None and quantity_mode else amount if amount is not None else "FULL POSITION"
            lines = [
                "Close-position command executed.\n",
                f"Symbol: {symbol.upper()}\n",
            ]
            if position_side:
                lines.append(f"Side: {position_side.upper()}\n")
            lines.extend(
                [
                    f"Amount: {amount_label}\n",
                    f"Status: {status}\n",
                    f"Order ID: {order_id}",
                ]
            )
            return "".join(lines)

        if (
            any(token in lowered for token in ("position analysis", "broker positions", "my positions", "account positions"))
            and any(token in lowered for token in ("position", "positions", "nav", "equity", "margin", "p/l", "pl"))
        ) or (
            "oanda" in lowered and any(token in lowered for token in ("position", "positions", "nav", "margin", "p/l", "pl"))
        ):
            summary = self.market_chat_position_summary(open_window=True)
            if summary:
                return summary

        if any(
            token in lowered
            for token in (
                "quant pm",
                "quant dashboard",
                "portfolio allocator",
                "capital at risk",
                "portfolio risk dashboard",
            )
        ) and any(token in lowered for token in ("show", "open", "summary", "analysis", "analyze", "analyse")):
            summary = await self.market_chat_quant_pm_summary(open_window=True)
            if summary:
                return summary

        if any(
            token in lowered
            for token in (
                "trade history analysis",
                "analyze trade history",
                "analyse trade history",
                "trade journal analysis",
                "review my trades",
                "analyze my trades",
                "analyse my trades",
            )
        ):
            return await self.market_chat_trade_history_summary(limit=400, open_window=True)

        if any(token in lowered for token in ("take screenshot", "take picture", "capture screenshot", "capture screen", "take a picture")):
            path = await self.capture_app_screenshot(prefix="market_chat")
            if path:
                return f"Screenshot captured successfully.\nPath: {path}"
            return "Unable to capture a screenshot right now."

        trade_match = re.search(
            r"(?:^|\b)trade\s+(buy|sell)\s+([A-Za-z0-9_:/.-]+)"
            r"(?:\s+(?:amount|size|units)\s+([-+]?\d*\.?\d+)(?:\s+(lots?|units?))?)?"
            r"(?:\s+(?:type)\s+(market|limit|stop_limit|stop-limit|stop\s+limit))?"
            r"(?:\s+(?:price|at)\s+([-+]?\d*\.?\d+))?"
            r"(?:\s+(?:trigger|stop_price|stoptrigger|stop_trigger|stop\s+trigger)\s+([-+]?\d*\.?\d+))?"
            r"(?:\s+(?:sl|stop|stop_loss)\s+([-+]?\d*\.?\d+))?"
            r"(?:\s+(?:tp|take_profit|takeprofit)\s+([-+]?\d*\.?\d+))?",
            lowered,
        )
        if trade_match:
            side, symbol, amount_text, quantity_mode, order_type, price_text, stop_price_text, sl_text, tp_text = trade_match.groups()
            if "confirm" not in lowered:
                mode_text = f" {quantity_mode}" if quantity_mode else ""
                return (
                    "Trade command detected but not executed.\n"
                    "Add the word CONFIRM to place it.\n"
                    f"Parsed command: side={side.upper()} symbol={symbol.upper()} amount={amount_text or '?'}{mode_text} "
                    f"type={(order_type or 'market').replace('-', '_').replace(' ', '_').upper()} "
                    f"price={price_text or '-'} trigger={stop_price_text or '-'} sl={sl_text or '-'} tp={tp_text or '-'}"
                )

            if not amount_text:
                return "Trade command needs an amount. Example: trade buy EUR/USD amount 0.01 lots confirm"

            amount = float(amount_text)
            if amount <= 0:
                return "Trade amount must be positive."

            resolved_type = (order_type or "market").strip().lower().replace("-", "_").replace(" ", "_")
            price = float(price_text) if price_text else None
            stop_price = float(stop_price_text) if stop_price_text else None
            stop_loss = float(sl_text) if sl_text else None
            take_profit = float(tp_text) if tp_text else None
            if resolved_type == "limit" and (price is None or price <= 0):
                return "Limit trade commands need a positive price."
            if resolved_type == "stop_limit":
                if price is None or price <= 0:
                    return "Stop-limit trade commands need a positive limit price."
                if stop_price is None or stop_price <= 0:
                    return "Stop-limit trade commands need a positive trigger price."

            try:
                trade_kwargs = {
                    "symbol": symbol.upper(),
                    "side": side,
                    "amount": amount,
                    "order_type": resolved_type,
                    "price": price,
                    "stop_price": stop_price,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                }
                if quantity_mode:
                    trade_kwargs["quantity_mode"] = quantity_mode
                order = await self.submit_market_chat_trade(**trade_kwargs)
            except Exception as exc:
                return f"Trade command failed: {exc}"

            status = str(order.get("status") or "submitted").replace("_", " ").upper()
            order_id = str(order.get("order_id") or order.get("id") or "-")
            return (
                f"Trade command executed.\n"
                f"Status: {status}\n"
                f"Symbol: {symbol.upper()}\n"
                f"Side: {side.upper()}\n"
                f"Amount: {amount} {quantity_mode or 'units'}\n"
                f"Type: {resolved_type.upper()}\n"
                f"Order ID: {order_id}"
            )

        market_symbol = self._resolve_market_chat_symbol(question)
        if self._should_answer_market_snapshot(question, market_symbol):
            timeframe = self._extract_market_chat_timeframe(question)
            snapshot = await self.market_chat_market_snapshot(market_symbol, timeframe=timeframe)
            if snapshot:
                return snapshot

        if "telegram" not in lowered:
            return None

        if any(token in lowered for token in ("telegram status", "status telegram", "telegram info", "telegram summary", "manage telegram")):
            return self.telegram_management_text()

        if any(token in lowered for token in ("disable telegram", "turn off telegram", "stop telegram")):
            await self._set_telegram_enabled_state(False)
            return self.telegram_management_text() + "\n\nTelegram has been disabled."

        if any(token in lowered for token in ("enable telegram", "turn on telegram", "start telegram")):
            if not str(getattr(self, "telegram_bot_token", "") or "").strip():
                return "Telegram cannot be enabled because the bot token is not configured in Settings -> Integrations."
            if not str(getattr(self, "telegram_chat_id", "") or "").strip():
                return "Telegram cannot be enabled because the chat ID is not configured in Settings -> Integrations."
            await self._set_telegram_enabled_state(True)
            return self.telegram_management_text() + "\n\nTelegram has been enabled."

        if any(token in lowered for token in ("restart telegram", "reconnect telegram", "refresh telegram")):
            if not str(getattr(self, "telegram_bot_token", "") or "").strip():
                return "Telegram cannot be restarted because the bot token is not configured."
            await self._restart_telegram_service()
            return self.telegram_management_text() + "\n\nTelegram restart requested."

        if any(token in lowered for token in ("test telegram", "send telegram test", "telegram test message")):
            if not str(getattr(self, "telegram_bot_token", "") or "").strip():
                return "Telegram test failed because the bot token is not configured."
            if not str(getattr(self, "telegram_chat_id", "") or "").strip():
                return "Telegram test failed because the chat ID is not configured."
            if not getattr(self, "telegram_enabled", False):
                await self._set_telegram_enabled_state(True)
            sent = await self.send_test_telegram_message()
            if sent:
                return self.telegram_management_text() + "\n\nTest message sent to Telegram."
            return self.telegram_management_text() + "\n\nTelegram test message could not be sent."

        return None

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

        if exchange.startswith("binance"):
            return BinanceUsWebSocket(symbols=symbols, event_bus=self.ws_bus, exchange_name=exchange)

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

    def _normalize_public_trade_rows(self, symbol, rows, limit=40):
        normalized = []
        for raw in rows or []:
            if not isinstance(raw, dict):
                continue

            price = self._normalize_history_float(
                raw.get("price") or raw.get("rate") or raw.get("last")
            )
            amount = self._normalize_history_float(
                raw.get("amount")
                or raw.get("size")
                or raw.get("qty")
                or raw.get("quantity")
                or raw.get("volume")
            )
            cost = self._normalize_history_float(
                raw.get("cost") or raw.get("notional") or raw.get("quoteVolume")
            )

            if cost is None and price is not None and amount is not None:
                cost = price * amount

            if price is None and cost is None:
                continue

            side = str(raw.get("side") or raw.get("taker_side") or raw.get("direction") or "").strip().lower()
            timestamp = (
                raw.get("datetime")
                or raw.get("timestamp")
                or raw.get("time")
                or raw.get("created_at")
            )

            normalized.append(
                {
                    "symbol": str(raw.get("symbol") or symbol or "").strip() or symbol,
                    "side": side if side in {"buy", "sell"} else "unknown",
                    "price": price,
                    "amount": amount,
                    "notional": cost,
                    "timestamp": timestamp,
                    "time": self._history_timestamp_text(timestamp) or str(timestamp or "-"),
                }
            )

        return normalized[: max(1, int(limit or 40))]

    async def _safe_fetch_recent_trades(self, symbol, limit=40):
        if not symbol:
            return []

        broker = getattr(self, "broker", None)
        if broker is not None and hasattr(broker, "fetch_trades"):
            try:
                rows = await broker.fetch_trades(symbol, limit=limit)
                normalized = self._normalize_public_trade_rows(symbol, rows, limit=limit)
                if normalized:
                    return normalized
            except TypeError:
                try:
                    rows = await broker.fetch_trades(symbol)
                    normalized = self._normalize_public_trade_rows(symbol, rows, limit=limit)
                    if normalized:
                        return normalized
                except Exception as exc:
                    self.logger.debug("Recent trade fetch failed for %s: %s", symbol, exc)
            except Exception as exc:
                self.logger.debug("Recent trade fetch failed for %s: %s", symbol, exc)

        tick = await self._safe_fetch_ticker(symbol)
        if not isinstance(tick, dict):
            return []

        price = self._normalize_history_float(tick.get("price") or tick.get("last") or tick.get("bid"))
        if price is None or price <= 0:
            return []

        bid = self._normalize_history_float(tick.get("bid")) or price * 0.9997
        ask = self._normalize_history_float(tick.get("ask")) or price * 1.0003
        now = datetime.now(timezone.utc)
        synthetic = [
            {
                "symbol": symbol,
                "side": "sell",
                "price": bid,
                "amount": 0.35,
                "notional": bid * 0.35,
                "timestamp": now.isoformat(),
                "time": self._history_timestamp_text(now),
            },
            {
                "symbol": symbol,
                "side": "buy",
                "price": ask,
                "amount": 0.42,
                "notional": ask * 0.42,
                "timestamp": now.isoformat(),
                "time": self._history_timestamp_text(now),
            },
        ]
        return synthetic[: max(1, min(int(limit or 2), len(synthetic)))]

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
            normalized.append(self._performance_trade_payload_from_record(trade))

        return normalized

    def _resolve_history_limit(self, limit=None):
        value = limit if limit is not None else getattr(self, "limit", self.MAX_HISTORY_LIMIT)
        try:
            resolved = max(100, int(value))
        except Exception:
            resolved = self.MAX_HISTORY_LIMIT

        return min(resolved, self.MAX_HISTORY_LIMIT)

    def handle_trade_execution(self, trade):
        if not isinstance(trade, dict):
            return

        status = str(trade.get("status") or "").strip().lower().replace("-", "_")
        order_id = str(trade.get("order_id") or "").strip()
        should_record = status in {"filled", "closed"} or trade.get("pnl") not in (None, "")
        if should_record and order_id:
            self._performance_recorded_orders.add(order_id)

        if should_record and getattr(self, "performance_engine", None) is not None:
            self.performance_engine.record_trade(trade)

        self.trade_signal.emit(trade)
        if trade.get("blocked_by_guard") and self.terminal is not None:
            system_console = getattr(self.terminal, "system_console", None)
            if system_console is not None:
                source = str(trade.get("source") or "bot").strip().lower() or "bot"
                reason = str(trade.get("reason") or "Behavior guard blocked the trade.").strip()
                system_console.log(f"{source.title()} trade blocked: {reason}", "WARN")
        telegram_service = getattr(self, "telegram_service", None)
        if telegram_service is not None:
            self._create_task(telegram_service.notify_trade(trade), "telegram_trade_notify")

    def _extract_balance_equity_value(self, balances):
        if not isinstance(balances, dict):
            return None

        direct_equity = self._balance_metric_value(
            balances,
            "nav",
            "equity",
            "net_liquidation",
            "account_value",
            "total_account_value",
            "balance",
            "cash",
        )
        if direct_equity is not None:
            return direct_equity

        total = balances.get("total")
        if isinstance(total, dict):
            for currency in ("USDT", "USD", "USDC", "BUSD"):
                value = total.get(currency)
                if value is None:
                    continue
                try:
                    return float(value)
                except Exception:
                    continue
            if len(total) == 1:
                try:
                    return float(next(iter(total.values())))
                except Exception:
                    return None
        return None

    def _update_performance_equity(self, balances=None):
        perf = getattr(self, "performance_engine", None)
        if perf is None or not hasattr(perf, "update_equity"):
            return None

        equity = self._extract_balance_equity_value(balances if balances is not None else getattr(self, "balances", {}))
        if equity is None:
            return None

        existing = getattr(perf, "equity_curve", None)
        if isinstance(existing, list) and existing:
            try:
                last_value = float(existing[-1])
            except Exception:
                last_value = None
            if last_value is not None and abs(last_value - float(equity)) <= 1e-9:
                return equity

        try:
            perf.update_equity(equity)
            self._persist_performance_history()
            self._persist_equity_snapshot(equity, balances if balances is not None else getattr(self, "balances", {}))
        except Exception:
            self.logger.debug("Performance equity update failed", exc_info=True)
            return None

        return equity

    def _safe_balance_metric(self, value):
        if value is None:
            return None
        if isinstance(value, dict):
            for currency in ("USDT", "USD", "USDC", "BUSD"):
                if currency in value:
                    numeric = self._safe_balance_metric(value.get(currency))
                    if numeric is not None:
                        return numeric
            for nested in value.values():
                numeric = self._safe_balance_metric(nested)
                if numeric is not None:
                    return numeric
            return None
        try:
            return float(value)
        except Exception:
            return None

    def _balance_metric_value(self, balances, *keys):
        if not isinstance(balances, dict):
            return None
        account = dict(balances.get("raw") or {})
        candidates = []
        for key in keys:
            if key is None:
                continue
            key_text = str(key).strip()
            if not key_text:
                continue
            variants = {
                key_text,
                key_text.lower(),
                key_text.upper(),
                key_text.replace("_", ""),
                key_text.replace("_", "").lower(),
                key_text.replace("_", "").upper(),
                key_text.replace("_", "-"),
                key_text.replace("_", " "),
                "".join(part.capitalize() for part in key_text.split("_")),
            }
            parts = [part for part in key_text.split("_") if part]
            if parts:
                variants.add(parts[0].lower() + "".join(part.capitalize() for part in parts[1:]))
            for variant in variants:
                if variant not in candidates:
                    candidates.append(variant)

        for source in (account, balances):
            if not isinstance(source, dict):
                continue
            for candidate in candidates:
                if candidate in source:
                    numeric = self._safe_balance_metric(source.get(candidate))
                    if numeric is not None:
                        return numeric
        return None

    def margin_closeout_snapshot(self, balances=None):
        balances = balances if isinstance(balances, dict) else getattr(self, "balances", {}) or {}
        threshold = max(0.01, min(1.0, float(getattr(self, "max_margin_closeout_pct", 0.50) or 0.50)))
        enabled = bool(getattr(self, "margin_closeout_guard_enabled", True))
        ratio = self._balance_metric_value(
            balances,
            "margin_closeout_percent",
            "margin_closeout_pct",
            "margin_closeout",
            "margin_ratio",
        )
        source = "reported"
        if ratio is not None and ratio > 1.0 and ratio <= 100.0:
            ratio = ratio / 100.0
        if ratio is None:
            margin_used = self._balance_metric_value(balances, "margin_used", "used_margin", "used")
            equity = self._balance_metric_value(
                balances,
                "nav",
                "equity",
                "net_liquidation",
                "account_value",
                "total_account_value",
                "balance",
            )
            if margin_used is not None and equity is not None and equity > 0:
                ratio = max(0.0, float(margin_used) / float(equity))
                source = "derived"

        warning_threshold = max(0.0, min(threshold, threshold * 0.8))
        blocked = bool(enabled and ratio is not None and ratio >= threshold)
        warning = bool(enabled and ratio is not None and ratio >= warning_threshold)
        if ratio is None:
            reason = "Margin closeout risk metric is not available from the current broker balance payload."
        elif blocked:
            reason = (
                f"Margin closeout risk is {ratio:.2%}, above the configured limit of {threshold:.2%}. "
                "New trades are blocked."
            )
        elif warning:
            reason = (
                f"Margin closeout risk is {ratio:.2%}. Guard threshold is {threshold:.2%}."
            )
        else:
            reason = (
                f"Margin closeout risk is {ratio:.2%}. Guard threshold is {threshold:.2%}."
            )
        return {
            "enabled": enabled,
            "available": ratio is not None,
            "ratio": ratio,
            "threshold": threshold,
            "warning_threshold": warning_threshold,
            "warning": warning,
            "blocked": blocked,
            "source": source,
            "reason": reason,
        }

    def _update_behavior_guard_equity(self, balances=None):
        guard = getattr(self, "behavior_guard", None)
        if guard is None:
            return
        equity = self._extract_balance_equity_value(balances if balances is not None else getattr(self, "balances", {}))
        if equity is None:
            return
        try:
            guard.record_equity(equity)
        except Exception:
            self.logger.debug("Behavior guard equity update failed", exc_info=True)

    def current_trading_mode(self):
        broker_config = getattr(getattr(self, "config", None), "broker", None)
        value = getattr(broker_config, "mode", None) or getattr(getattr(self, "broker", None), "mode", None) or "paper"
        return str(value or "paper").strip().lower()

    def is_live_mode(self):
        mode = self.current_trading_mode()
        broker_config = getattr(getattr(self, "config", None), "broker", None)
        exchange = str(getattr(broker_config, "exchange", "") or "").strip().lower()
        return mode == "live" and exchange != "paper"

    def current_account_label(self):
        broker = getattr(self, "broker", None)
        broker_config = getattr(getattr(self, "config", None), "broker", None)
        account_id = getattr(broker, "account_id", None) or getattr(broker_config, "account_id", None) or ""
        text = str(account_id or "").strip()
        if not text:
            return "Not set"
        if len(text) <= 8:
            return text
        return f"{text[:4]}...{text[-4:]}"

    def _resolve_broker_capability(self, method_name):
        broker = getattr(self, "broker", None)
        if broker is None:
            return False
        exchange_has = getattr(broker, "_exchange_has", None)
        if callable(exchange_has):
            try:
                return bool(exchange_has(method_name))
            except Exception:
                pass
        return callable(getattr(broker, method_name, None))

    def get_broker_capabilities(self):
        broker = getattr(self, "broker", None)
        if broker is None:
            return {}

        markets = getattr(getattr(broker, "exchange", None), "markets", None)
        supported_venues = self.supported_market_venues()
        option_market_available = "option" in supported_venues
        derivative_market_available = "derivative" in supported_venues
        otc_market_available = "otc" in supported_venues
        if isinstance(markets, dict) and markets:
            option_market_available = option_market_available or any(
                bool((market or {}).get("option")) for market in markets.values()
            )

        return {
            "connectivity": self._resolve_broker_capability("fetch_status"),
            "trading": self._resolve_broker_capability("create_order"),
            "cancel_orders": self._resolve_broker_capability("cancel_all_orders") or self._resolve_broker_capability("cancel_order"),
            "positions": self._resolve_broker_capability("fetch_positions"),
            "open_orders": self._resolve_broker_capability("fetch_open_orders"),
            "closed_orders": self._resolve_broker_capability("fetch_closed_orders"),
            "order_tracking": self._resolve_broker_capability("fetch_order"),
            "orderbook": self._resolve_broker_capability("fetch_orderbook") or self._resolve_broker_capability("fetch_order_book"),
            "candles": self._resolve_broker_capability("fetch_ohlcv"),
            "ticker": self._resolve_broker_capability("fetch_ticker"),
            "derivatives_market": derivative_market_available,
            "options_market": option_market_available,
            "otc_market": otc_market_available,
            "supported_market_venues": supported_venues,
        }

    def activate_emergency_stop(self, reason="Emergency kill switch active"):
        guard = getattr(self, "behavior_guard", None)
        if guard is None and getattr(getattr(self, "trading_system", None), "behavior_guard", None) is not None:
            guard = self.trading_system.behavior_guard
            self.behavior_guard = guard
        if guard is not None:
            guard.activate_manual_lock(reason)

    def clear_emergency_stop(self):
        guard = getattr(self, "behavior_guard", None)
        if guard is not None:
            guard.clear_manual_lock()

    def is_emergency_stop_active(self):
        guard = getattr(self, "behavior_guard", None)
        if guard is None:
            return False
        return bool(getattr(guard, "is_locked", lambda: False)())

    def get_behavior_guard_status(self):
        guard = getattr(self, "behavior_guard", None)
        if guard is None:
            return {}
        try:
            return dict(guard.status_snapshot() or {})
        except Exception:
            self.logger.debug("Behavior guard status lookup failed", exc_info=True)
            return {}

    async def run_startup_health_check(self):
        broker = getattr(self, "broker", None)
        if broker is None:
            self.health_check_report = []
            self.health_check_summary = "No broker connected"
            return []

        symbol = next(iter(getattr(self, "symbols", []) or []), None)
        capabilities = self.get_broker_capabilities()
        results = []

        async def _run_check(name, coro_factory, optional=False):
            try:
                detail = await coro_factory()
                results.append({"name": name, "status": "pass", "detail": detail or "OK"})
            except NotImplementedError:
                results.append({"name": name, "status": "skip" if optional else "warn", "detail": "Not supported by broker"})
            except Exception as exc:
                results.append({"name": name, "status": "warn" if optional else "fail", "detail": str(exc)})

        if capabilities.get("connectivity"):
            async def _fetch_connectivity():
                status = await broker.fetch_status()
                if isinstance(status, dict):
                    broker_label = str(status.get("broker") or getattr(broker, "exchange_name", "") or "").upper()
                    status_text = str(status.get("status") or "ok").upper()
                    return f"{broker_label + ' ' if broker_label else ''}{status_text}".strip()
                return status

            await _run_check("Connectivity", _fetch_connectivity)
        else:
            results.append(
                {
                    "name": "Connectivity",
                    "status": "pass" if self._broker_is_connected(broker) else "warn",
                    "detail": "Connected" if self._broker_is_connected(broker) else "Connection state unavailable",
                }
            )

        await _run_check("Balance", lambda: self._fetch_balances(broker))
        if symbol and capabilities.get("ticker"):
            await _run_check("Ticker", lambda: self._safe_fetch_ticker(symbol))
        elif symbol:
            results.append({"name": "Ticker", "status": "skip", "detail": "Ticker endpoint not available"})

        if symbol and capabilities.get("candles"):
            await _run_check(
                "Candles",
                lambda: broker.fetch_ohlcv(symbol, timeframe=getattr(self, "time_frame", "1h"), limit=50),
            )
        elif symbol:
            results.append({"name": "Candles", "status": "skip", "detail": "Candle endpoint not available"})

        if symbol and capabilities.get("orderbook"):
            await _run_check("Orderbook", lambda: broker.fetch_orderbook(symbol, limit=10), optional=True)
        else:
            results.append({"name": "Orderbook", "status": "skip", "detail": "Orderbook not supported"})

        if capabilities.get("open_orders"):
            async def _fetch_open_orders():
                snapshot = getattr(broker, "fetch_open_orders_snapshot", None)
                if callable(snapshot):
                    return await snapshot(symbols=getattr(self, "symbols", []), limit=10)
                if symbol:
                    return await broker.fetch_open_orders(symbol=symbol, limit=10)
                return await broker.fetch_open_orders(limit=10)

            await _run_check("Open Orders", _fetch_open_orders, optional=True)
        else:
            results.append({"name": "Open Orders", "status": "skip", "detail": "Open-order endpoint not supported"})

        if capabilities.get("positions"):
            await _run_check("Positions", lambda: broker.fetch_positions(), optional=True)
        else:
            results.append({"name": "Positions", "status": "skip", "detail": "Position endpoint not supported"})

        results.append(
            {
                "name": "Order Submit Route",
                "status": "pass" if capabilities.get("trading") else "warn",
                "detail": "Execution route available" if capabilities.get("trading") else "Order creation not available",
            }
        )
        results.append(
            {
                "name": "Order Tracking",
                "status": "pass" if capabilities.get("order_tracking") else "warn",
                "detail": "Live order status lookup available" if capabilities.get("order_tracking") else "No live fetch_order support",
            }
        )

        passed = sum(1 for item in results if item["status"] == "pass")
        failed = sum(1 for item in results if item["status"] == "fail")
        warned = sum(1 for item in results if item["status"] == "warn")
        self.health_check_report = results
        if failed:
            self.health_check_summary = f"{passed} pass / {warned} warn / {failed} fail"
        else:
            self.health_check_summary = f"{passed} pass / {warned} warn"
        return results

    def get_health_check_report(self):
        return list(self.health_check_report or [])

    def get_health_check_summary(self):
        return str(self.health_check_summary or "Not run")

    def get_pipeline_status_snapshot(self):
        trading_system = getattr(self, "trading_system", None)
        resolver = getattr(trading_system, "pipeline_status_snapshot", None)
        if callable(resolver):
            try:
                return dict(resolver() or {})
            except Exception:
                self.logger.debug("Pipeline status lookup failed", exc_info=True)
        return {}

    def get_pipeline_status_summary(self):
        snapshot = self.get_pipeline_status_snapshot()
        if not snapshot:
            return "Idle"

        counts = {"filled": 0, "submitted": 0, "signal": 0, "approved": 0, "hold": 0, "rejected": 0, "blocked": 0, "skipped": 0}
        for payload in snapshot.values():
            status = str((payload or {}).get("status") or "unknown").strip().lower()
            counts[status] = counts.get(status, 0) + 1

        active = counts.get("filled", 0) + counts.get("submitted", 0) + counts.get("signal", 0) + counts.get("approved", 0)
        guarded = counts.get("rejected", 0) + counts.get("blocked", 0)
        holding = counts.get("hold", 0) + counts.get("skipped", 0)
        return f"{active} active / {guarded} guarded / {holding} idle"

    async def fetch_closed_trade_journal(self, limit=150):
        rows = []
        seen = set()
        repository = getattr(self, "trade_repository", None)
        repo_rows = []
        if repository is not None:
            try:
                repo_rows = await asyncio.to_thread(repository.get_trades, max(int(limit) * 2, 100))
            except Exception as exc:
                self.logger.debug("Trade DB journal load failed: %s", exc)

        source_map = {}
        for trade in repo_rows or []:
            order_id = str(getattr(trade, "order_id", "") or "").strip()
            if order_id and order_id not in source_map:
                source_map[order_id] = {
                    "trade_db_id": getattr(trade, "id", None),
                    "source": getattr(trade, "source", "") or "",
                    "status": getattr(trade, "status", "") or "",
                    "timestamp": getattr(trade, "timestamp", "") or "",
                    "price": getattr(trade, "price", "") or "",
                    "size": getattr(trade, "quantity", "") or "",
                    "pnl": getattr(trade, "pnl", "") or "",
                    "strategy_name": getattr(trade, "strategy_name", "") or "",
                    "reason": getattr(trade, "reason", "") or "",
                    "confidence": getattr(trade, "confidence", "") or "",
                    "expected_price": getattr(trade, "expected_price", "") or "",
                    "spread_bps": getattr(trade, "spread_bps", "") or "",
                    "slippage_bps": getattr(trade, "slippage_bps", "") or "",
                    "fee": getattr(trade, "fee", "") or "",
                    "stop_loss": getattr(trade, "stop_loss", "") or "",
                    "take_profit": getattr(trade, "take_profit", "") or "",
                    "setup": getattr(trade, "setup", "") or "",
                    "outcome": getattr(trade, "outcome", "") or "",
                    "lessons": getattr(trade, "lessons", "") or "",
                }

        broker = getattr(self, "broker", None)
        if broker is not None and self._resolve_broker_capability("fetch_closed_orders"):
            try:
                broker_rows = await broker.fetch_closed_orders(limit=limit)
            except Exception as exc:
                self.logger.debug("Closed-order journal fetch failed: %s", exc)
                broker_rows = []
            for row in broker_rows or []:
                if not isinstance(row, dict):
                    continue
                order_id = str(row.get("id") or row.get("order_id") or "").strip()
                seen.add(order_id)
                repo_meta = source_map.get(order_id, {})
                rows.append(
                    {
                        "trade_db_id": repo_meta.get("trade_db_id"),
                        "timestamp": row.get("timestamp") or repo_meta.get("timestamp") or "",
                        "symbol": row.get("symbol") or "",
                        "source": row.get("source") or repo_meta.get("source") or "broker",
                        "side": row.get("side") or "",
                        "price": row.get("average") or row.get("price") or repo_meta.get("price") or "",
                        "size": row.get("filled") or row.get("amount") or repo_meta.get("size") or "",
                        "order_type": row.get("type") or "",
                        "status": row.get("status") or repo_meta.get("status") or "",
                        "order_id": order_id,
                        "pnl": row.get("pnl") or repo_meta.get("pnl") or "",
                        "strategy_name": repo_meta.get("strategy_name") or "",
                        "reason": repo_meta.get("reason") or "",
                        "confidence": repo_meta.get("confidence") or "",
                        "expected_price": repo_meta.get("expected_price") or "",
                        "spread_bps": repo_meta.get("spread_bps") or "",
                        "slippage_bps": repo_meta.get("slippage_bps") or "",
                        "fee": repo_meta.get("fee") or "",
                        "stop_loss": repo_meta.get("stop_loss") or "",
                        "take_profit": repo_meta.get("take_profit") or "",
                        "setup": repo_meta.get("setup") or "",
                        "outcome": repo_meta.get("outcome") or "",
                        "lessons": repo_meta.get("lessons") or "",
                    }
                )

        for trade in repo_rows or []:
            order_id = str(getattr(trade, "order_id", "") or "").strip()
            if order_id and order_id in seen:
                continue
            status = str(getattr(trade, "status", "") or "").strip().lower()
            if status not in {"filled", "closed", "canceled", "cancelled", "rejected", "expired", "failed"}:
                continue
            rows.append(
                {
                    "trade_db_id": getattr(trade, "id", None),
                    "timestamp": getattr(trade, "timestamp", "") or "",
                    "symbol": getattr(trade, "symbol", "") or "",
                    "source": getattr(trade, "source", "") or "",
                    "side": getattr(trade, "side", "") or "",
                    "price": getattr(trade, "price", "") or "",
                    "size": getattr(trade, "quantity", "") or "",
                    "order_type": getattr(trade, "order_type", "") or "",
                    "status": getattr(trade, "status", "") or "",
                    "order_id": order_id,
                    "pnl": getattr(trade, "pnl", "") or "",
                    "strategy_name": getattr(trade, "strategy_name", "") or "",
                    "reason": getattr(trade, "reason", "") or "",
                    "confidence": getattr(trade, "confidence", "") or "",
                    "expected_price": getattr(trade, "expected_price", "") or "",
                    "spread_bps": getattr(trade, "spread_bps", "") or "",
                    "slippage_bps": getattr(trade, "slippage_bps", "") or "",
                    "fee": getattr(trade, "fee", "") or "",
                    "stop_loss": getattr(trade, "stop_loss", "") or "",
                    "take_profit": getattr(trade, "take_profit", "") or "",
                    "setup": getattr(trade, "setup", "") or "",
                    "outcome": getattr(trade, "outcome", "") or "",
                    "lessons": getattr(trade, "lessons", "") or "",
                }
            )

        rows.sort(key=lambda item: str(item.get("timestamp") or ""), reverse=True)
        return rows[:limit]

    def _normalize_history_float(self, value):
        if value in (None, "", "-"):
            return None
        try:
            return float(value)
        except Exception:
            return None

    def _history_timestamp_text(self, value):
        if value in (None, ""):
            return ""
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc).isoformat()
            return value.astimezone(timezone.utc).isoformat()
        return str(value)

    def _normalize_broker_trade_history_row(self, row, repo_meta=None):
        if not isinstance(row, dict):
            return None
        repo_meta = dict(repo_meta or {})

        order_id = str(
            row.get("order_id")
            or row.get("orderID")
            or row.get("id")
            or row.get("tradeID")
            or row.get("clientOrderId")
            or ""
        ).strip()
        symbol = str(row.get("symbol") or row.get("instrument") or "").strip()

        side = row.get("side")
        if not side:
            units = self._normalize_history_float(
                row.get("currentUnits") or row.get("units") or row.get("amount") or row.get("filled")
            )
            if units is not None:
                side = "buy" if units >= 0 else "sell"
        size = self._normalize_history_float(
            row.get("amount")
            or row.get("filled")
            or row.get("units")
            or row.get("currentUnits")
            or row.get("initialUnits")
            or repo_meta.get("size")
        )
        if size is not None:
            size = abs(size)

        timestamp = (
            row.get("timestamp")
            or row.get("datetime")
            or row.get("time")
            or row.get("closeTime")
            or row.get("openTime")
            or repo_meta.get("timestamp")
            or ""
        )
        status = str(
            row.get("status")
            or row.get("state")
            or row.get("orderState")
            or repo_meta.get("status")
            or "filled"
        ).strip().lower()

        normalized = {
            "trade_db_id": repo_meta.get("trade_db_id"),
            "timestamp": self._history_timestamp_text(timestamp),
            "symbol": symbol,
            "source": repo_meta.get("source") or "broker_trade_history",
            "side": str(side or "").strip().lower(),
            "price": row.get("price") or row.get("average") or row.get("averagePrice") or repo_meta.get("price") or "",
            "size": size if size is not None else "",
            "order_type": row.get("type") or repo_meta.get("order_type") or "",
            "status": status,
            "order_id": order_id,
            "pnl": row.get("pnl") or row.get("realizedPL") or row.get("pl") or repo_meta.get("pnl") or "",
            "strategy_name": repo_meta.get("strategy_name") or "",
            "reason": repo_meta.get("reason") or "",
            "confidence": repo_meta.get("confidence") or "",
            "expected_price": repo_meta.get("expected_price") or "",
            "spread_bps": repo_meta.get("spread_bps") or "",
            "slippage_bps": repo_meta.get("slippage_bps") or "",
            "fee": row.get("fee") or row.get("commission") or row.get("cost") or repo_meta.get("fee") or "",
            "stop_loss": repo_meta.get("stop_loss") or "",
            "take_profit": repo_meta.get("take_profit") or "",
            "setup": repo_meta.get("setup") or "",
            "outcome": repo_meta.get("outcome") or "",
            "lessons": repo_meta.get("lessons") or "",
            "history_kind": "broker_trade",
        }
        if not normalized["symbol"] and not normalized["order_id"]:
            return None
        return normalized

    def _trade_history_dedupe_key(self, row):
        if not isinstance(row, dict):
            return ""
        order_id = str(row.get("order_id") or "").strip()
        if order_id:
            return f"order:{order_id}"
        timestamp = self._history_timestamp_text(row.get("timestamp"))
        symbol = str(row.get("symbol") or "").strip().upper()
        side = str(row.get("side") or "").strip().lower()
        price = self._normalize_history_float(row.get("price"))
        size = self._normalize_history_float(row.get("size"))
        return f"row:{timestamp}|{symbol}|{side}|{price}|{size}"

    async def fetch_trade_history(self, limit=300):
        limit = max(50, int(limit or 300))
        rows = list(await self.fetch_closed_trade_journal(limit=limit) or [])
        seen = {self._trade_history_dedupe_key(row) for row in rows if self._trade_history_dedupe_key(row)}

        source_map = {}
        for row in rows:
            order_id = str(row.get("order_id") or "").strip()
            if order_id and order_id not in source_map:
                source_map[order_id] = dict(row)

        broker = getattr(self, "broker", None)
        broker_rows = []
        if broker is not None:
            try:
                if self._resolve_broker_capability("fetch_my_trades"):
                    broker_rows = await broker.fetch_my_trades(limit=limit)
                elif self._resolve_broker_capability("fetch_trades"):
                    broker_rows = await broker.fetch_trades(None, limit=limit)
            except TypeError:
                try:
                    broker_rows = await broker.fetch_trades(limit=limit)
                except Exception as exc:
                    self.logger.debug("Trade-history fetch failed: %s", exc)
                    broker_rows = []
            except Exception as exc:
                self.logger.debug("Trade-history fetch failed: %s", exc)
                broker_rows = []

        for raw_row in broker_rows or []:
            order_id = str(
                (raw_row or {}).get("order_id")
                or (raw_row or {}).get("orderID")
                or (raw_row or {}).get("id")
                or ""
            ).strip()
            normalized = self._normalize_broker_trade_history_row(raw_row, repo_meta=source_map.get(order_id))
            if normalized is None:
                continue
            key = self._trade_history_dedupe_key(normalized)
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            rows.append(normalized)

        rows.sort(key=lambda item: str(item.get("timestamp") or ""), reverse=True)
        return rows[:limit]

    def _trade_history_stats(self, rows):
        stats = {
            "trade_count": 0,
            "pnl_count": 0,
            "wins": 0,
            "losses": 0,
            "flat": 0,
            "net_pnl": 0.0,
            "avg_pnl": None,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "profit_factor": None,
            "fees": 0.0,
            "avg_fee": None,
            "avg_slippage": None,
            "journal_coverage": None,
            "best_symbol": None,
            "worst_symbol": None,
            "strategy_rows": [],
            "symbol_rows": [],
            "recent_trades": [],
            "source_breakdown": {},
        }
        if not rows:
            return stats

        strategy_map = {}
        symbol_map = {}
        slippage_values = []
        fee_values = []
        journal_complete = 0

        for row in rows:
            stats["trade_count"] += 1
            source = str(row.get("source") or "unknown").strip().lower() or "unknown"
            stats["source_breakdown"][source] = stats["source_breakdown"].get(source, 0) + 1

            symbol = str(row.get("symbol") or "-").strip().upper() or "-"
            strategy = str(row.get("strategy_name") or "Unspecified").strip() or "Unspecified"
            pnl = self._normalize_history_float(row.get("pnl"))
            fee = self._normalize_history_float(row.get("fee"))
            slippage = self._normalize_history_float(row.get("slippage_bps"))

            stats["recent_trades"].append(
                {
                    "timestamp": self._history_timestamp_text(row.get("timestamp")),
                    "symbol": symbol,
                    "side": str(row.get("side") or "").upper(),
                    "status": str(row.get("status") or "").upper(),
                    "pnl": pnl,
                    "source": source,
                }
            )

            if fee is not None:
                stats["fees"] += fee
                fee_values.append(fee)
            if slippage is not None:
                slippage_values.append(slippage)

            if str(row.get("reason") or "").strip() and str(row.get("lessons") or "").strip():
                journal_complete += 1

            if strategy not in strategy_map:
                strategy_map[strategy] = {"strategy": strategy, "trades": 0, "wins": 0, "net_pnl": 0.0}
            if symbol not in symbol_map:
                symbol_map[symbol] = {"symbol": symbol, "trades": 0, "wins": 0, "net_pnl": 0.0}
            strategy_map[strategy]["trades"] += 1
            symbol_map[symbol]["trades"] += 1

            if pnl is None:
                continue
            stats["pnl_count"] += 1
            stats["net_pnl"] += pnl
            strategy_map[strategy]["net_pnl"] += pnl
            symbol_map[symbol]["net_pnl"] += pnl
            if pnl > 0:
                stats["wins"] += 1
                stats["gross_profit"] += pnl
                strategy_map[strategy]["wins"] += 1
                symbol_map[symbol]["wins"] += 1
            elif pnl < 0:
                stats["losses"] += 1
                stats["gross_loss"] += abs(pnl)
            else:
                stats["flat"] += 1

        if stats["pnl_count"] > 0:
            stats["avg_pnl"] = stats["net_pnl"] / float(stats["pnl_count"])
            total_decisions = stats["wins"] + stats["losses"] + stats["flat"]
            if total_decisions > 0:
                stats["win_rate"] = stats["wins"] / float(total_decisions)
            if stats["gross_loss"] > 0:
                stats["profit_factor"] = stats["gross_profit"] / stats["gross_loss"]
            elif stats["gross_profit"] > 0:
                stats["profit_factor"] = float("inf")
        else:
            stats["win_rate"] = None
        stats["journal_coverage"] = journal_complete / float(stats["trade_count"]) if stats["trade_count"] else None
        stats["avg_fee"] = (sum(fee_values) / len(fee_values)) if fee_values else None
        stats["avg_slippage"] = (sum(slippage_values) / len(slippage_values)) if slippage_values else None

        strategy_rows = []
        for item in strategy_map.values():
            trades = int(item["trades"] or 0)
            strategy_rows.append(
                {
                    "strategy": item["strategy"],
                    "trades": trades,
                    "win_rate": (item["wins"] / float(trades)) if trades else None,
                    "net_pnl": item["net_pnl"],
                }
            )
        strategy_rows.sort(key=lambda item: (item.get("net_pnl") or 0.0, item.get("trades") or 0), reverse=True)
        stats["strategy_rows"] = strategy_rows[:5]

        symbol_rows = []
        for item in symbol_map.values():
            trades = int(item["trades"] or 0)
            symbol_rows.append(
                {
                    "symbol": item["symbol"],
                    "trades": trades,
                    "win_rate": (item["wins"] / float(trades)) if trades else None,
                    "net_pnl": item["net_pnl"],
                }
            )
        symbol_rows.sort(key=lambda item: (item.get("net_pnl") or 0.0, item.get("trades") or 0), reverse=True)
        stats["symbol_rows"] = symbol_rows[:5]
        if symbol_rows:
            stats["best_symbol"] = max(symbol_rows, key=lambda item: item.get("net_pnl") or 0.0)
            stats["worst_symbol"] = min(symbol_rows, key=lambda item: item.get("net_pnl") or 0.0)

        stats["recent_trades"] = stats["recent_trades"][:8]
        return stats

    async def get_trade_history_analysis(self, limit=300):
        rows = await self.fetch_trade_history(limit=limit)
        return {
            "rows": rows,
            "stats": self._trade_history_stats(rows),
        }

    async def market_chat_trade_history_summary(self, limit=300, open_window=True):
        analysis = await self.get_trade_history_analysis(limit=limit)
        rows = list(analysis.get("rows") or [])
        stats = dict(analysis.get("stats") or {})

        terminal = getattr(self, "terminal", None)
        if open_window and terminal is not None:
            try:
                terminal._open_closed_journal_window()
                terminal._open_trade_journal_review_window()
            except Exception:
                pass

        if not rows:
            return "Trade history analysis is not available yet because no closed broker or stored trades were found."

        source_parts = [
            f"{name}: {count}"
            for name, count in sorted((stats.get("source_breakdown") or {}).items(), key=lambda item: item[1], reverse=True)
        ]
        lines = [
            "Trade history analysis loaded.",
            (
                f"Trades: {stats.get('trade_count', 0)}"
                f" | With PnL: {stats.get('pnl_count', 0)}"
                f" | Net PnL: {float(stats.get('net_pnl', 0.0) or 0.0):.2f}"
                f" | Win rate: {('-' if stats.get('win_rate') is None else f'{float(stats.get('win_rate')) * 100.0:.1f}%')}"
            ),
        ]
        if stats.get("avg_pnl") is not None or stats.get("profit_factor") is not None:
            profit_factor = stats.get("profit_factor")
            pf_text = "-" if profit_factor is None else ("infinite" if profit_factor == float("inf") else f"{float(profit_factor):.2f}")
            lines.append(
                f"Avg trade: {float(stats.get('avg_pnl') or 0.0):.2f} | Profit factor: {pf_text} | "
                f"Fees: {float(stats.get('fees', 0.0) or 0.0):.2f} | Avg slippage: "
                f"{('-' if stats.get('avg_slippage') is None else f'{float(stats.get('avg_slippage')):.2f} bps')}"
            )
        if source_parts:
            lines.append("Sources: " + " | ".join(source_parts[:4]))
        best_symbol = stats.get("best_symbol")
        worst_symbol = stats.get("worst_symbol")
        if best_symbol is not None:
            lines.append(f"Best symbol: {best_symbol.get('symbol')} ({float(best_symbol.get('net_pnl') or 0.0):.2f})")
        if worst_symbol is not None:
            lines.append(f"Worst symbol: {worst_symbol.get('symbol')} ({float(worst_symbol.get('net_pnl') or 0.0):.2f})")
        top_strategy = next(iter(stats.get("strategy_rows") or []), None)
        if top_strategy is not None:
            lines.append(
                f"Top strategy: {top_strategy.get('strategy')} | Trades: {top_strategy.get('trades')} | "
                f"Net PnL: {float(top_strategy.get('net_pnl') or 0.0):.2f}"
            )
        recent = list(stats.get("recent_trades") or [])[:3]
        if recent:
            recent_bits = []
            for item in recent:
                pnl = item.get("pnl")
                pnl_text = "-" if pnl is None else f"{float(pnl):.2f}"
                recent_bits.append(f"{item.get('symbol')} {item.get('side')} {item.get('status')} pnl {pnl_text}")
            lines.append("Recent trades: " + " ; ".join(recent_bits))
        lines.append("Use Tools -> Closed Journal and Journal Review for the detailed history and review.")
        return "\n".join(lines)

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

    async def telegram_recommendations_text(self):
        terminal = getattr(self, "terminal", None)
        if terminal is None or not hasattr(terminal, "_recommendation_rows"):
            return "<b>Recommendations</b>\nRecommendations are not available right now."
        try:
            rows = list(terminal._recommendation_rows() or [])
        except Exception:
            rows = []
        if not rows:
            return "<b>Recommendations</b>\nNo active recommendations yet."

        lines = ["<b>Recommendations</b>"]
        for row in rows[:8]:
            lines.append(
                f"<code>{row.get('symbol', '-')}</code> | {row.get('action', '-')} | "
                f"conf {row.get('confidence', '-')} | {row.get('why', row.get('reason', '-'))}"
            )
        return "\n".join(lines)

    async def telegram_performance_text(self):
        terminal = getattr(self, "terminal", None)
        if terminal is None or not hasattr(terminal, "_build_performance_snapshot"):
            return "<b>Performance</b>\nPerformance analytics are not available right now."

        try:
            snapshot = terminal._build_performance_snapshot() or {}
        except Exception:
            snapshot = {}
        if not snapshot:
            return "<b>Performance</b>\nNo performance snapshot is available yet."

        return (
            "<b>Performance</b>\n"
            f"Equity: <code>{snapshot.get('equity', '-')}</code>\n"
            f"Net PnL: <code>{snapshot.get('net_pnl', '-')}</code>\n"
            f"Win Rate: <code>{snapshot.get('win_rate_label', '-')}</code>\n"
            f"Max Drawdown: <code>{snapshot.get('max_drawdown_label', '-')}</code>\n"
            f"Profit Factor: <code>{snapshot.get('profit_factor_label', '-')}</code>\n"
            f"Fees: <code>{snapshot.get('fees_label', '-')}</code>\n"
            f"Avg Slippage: <code>{snapshot.get('avg_slippage_label', '-')}</code>"
        )

    async def telegram_position_analysis_text(self, open_window=True):
        summary = self.market_chat_position_summary(open_window=open_window)
        if not summary:
            return "<b>Position Analysis</b>\nPosition analysis is not available right now."
        return f"<b>Position Analysis</b>\n<pre>{self._plain_text(summary)}</pre>"

    async def telegram_open_chart(self, symbol, timeframe=None):
        terminal = getattr(self, "terminal", None)
        if terminal is None:
            return {"ok": False, "message": "Terminal is not available."}

        requested_symbol = str(symbol or "").upper().strip()
        requested_timeframe = str(timeframe or getattr(self, "time_frame", "1h") or "1h").strip() or "1h"
        if not requested_symbol:
            return {"ok": False, "message": "A symbol is required."}

        try:
            terminal._open_symbol_chart(requested_symbol, requested_timeframe)
            chart = terminal._chart_for_symbol(requested_symbol) if hasattr(terminal, "_chart_for_symbol") else None
            if chart is not None and hasattr(terminal, "_schedule_chart_data_refresh"):
                terminal._schedule_chart_data_refresh(chart)
            QApplication.processEvents()
            await asyncio.sleep(0.25)
            QApplication.processEvents()
        except Exception as exc:
            return {"ok": False, "message": f"Unable to open chart {requested_symbol}: {exc}"}

        chart = terminal._chart_for_symbol(requested_symbol) if hasattr(terminal, "_chart_for_symbol") else None
        if chart is None:
            return {"ok": False, "message": f"Chart {requested_symbol} could not be opened."}
        return {
            "ok": True,
            "message": f"Chart opened for {requested_symbol} ({requested_timeframe}).",
            "symbol": requested_symbol,
            "timeframe": requested_timeframe,
        }

    async def capture_chart_screenshot(self, symbol=None, timeframe=None, prefix="chart"):
        terminal = getattr(self, "terminal", None)
        if terminal is None:
            return None

        requested_symbol = str(symbol or "").upper().strip()
        requested_timeframe = str(timeframe or getattr(self, "time_frame", "1h") or "1h").strip() or "1h"
        if requested_symbol:
            open_result = await self.telegram_open_chart(requested_symbol, requested_timeframe)
            if not open_result.get("ok"):
                return None

        chart = None
        if requested_symbol and hasattr(terminal, "_chart_for_symbol"):
            chart = terminal._chart_for_symbol(requested_symbol)
        if chart is None and hasattr(terminal, "_current_chart_widget"):
            chart = terminal._current_chart_widget()
        if chart is None:
            return None

        safe_symbol = sanitize_screenshot_fragment(
            str(getattr(chart, "symbol", requested_symbol or "chart")),
            "chart",
        )
        try:
            QApplication.processEvents()
            await asyncio.sleep(0.15)
            QApplication.processEvents()
            return capture_widget_to_output(chart, prefix=prefix, suffix=safe_symbol)
        except Exception as exc:
            self.logger.debug("Chart screenshot capture failed: %s", exc)
            return None

    async def capture_telegram_screenshot(self):
        return await self.capture_app_screenshot(prefix="telegram")

    async def capture_app_screenshot(self, prefix="market_chat"):
        terminal = getattr(self, "terminal", None)
        if terminal is None:
            return None

        try:
            return capture_widget_to_output(terminal, prefix=prefix)
        except Exception as exc:
            self.logger.debug("App screenshot capture failed: %s", exc)
            return None

    def _plain_text(self, value):
        text = str(value or "")
        if not text:
            return ""
        text = re.sub(r"<[^>]+>", "", text)
        text = (
            text.replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&amp;", "&")
            .replace("&nbsp;", " ")
        )
        return re.sub(r"\s+", " ", text).strip()

    def _openai_news_focus_symbols(self):
        symbols = []
        terminal = getattr(self, "terminal", None)
        active_chart = None
        if terminal is not None and hasattr(terminal, "_current_chart_widget"):
            try:
                active_chart = terminal._current_chart_widget()
            except Exception:
                active_chart = None

        for value in [
            getattr(active_chart, "symbol", None),
            getattr(terminal, "symbol", None) if terminal is not None else None,
        ]:
            symbol = str(value or "").upper().strip()
            if symbol and symbol not in symbols:
                symbols.append(symbol)

        if terminal is not None and hasattr(terminal, "_recommendation_rows"):
            try:
                for item in list(terminal._recommendation_rows() or [])[:3]:
                    symbol = str(item.get("symbol") or "").upper().strip()
                    if symbol and symbol not in symbols:
                        symbols.append(symbol)
            except Exception:
                pass

        for symbol in list(getattr(self, "symbols", []) or [])[:3]:
            normalized = str(symbol or "").upper().strip()
            if normalized and normalized not in symbols:
                symbols.append(normalized)

        return symbols[:3]

    async def _openai_news_context(self, question=""):
        if not getattr(self, "news_enabled", False):
            return []

        lowered = str(question or "").lower()
        wants_news = any(
            token in lowered
            for token in ("news", "headline", "headlines", "event", "events", "rss", "sentiment", "impact")
        )
        symbols = self._openai_news_focus_symbols()
        if not symbols:
            return []

        lines = []
        for symbol in symbols:
            try:
                cached = self._news_cache.get(symbol, {})
                events = list(cached.get("events", []) or [])
                if wants_news or not events:
                    events = await self.request_news(symbol, force=wants_news, max_age_seconds=300)
                if not events:
                    continue
                bias = self.news_service.summarize_news_bias(events)
                direction = self._plain_text(bias.get("direction") or "neutral")
                reason = self._plain_text(bias.get("reason") or "")
                headline = self._plain_text(bias.get("headline") or "")
                score = bias.get("score")
                try:
                    score_text = f"{float(score):.2f}"
                except Exception:
                    score_text = "0.00"
                recent = []
                for event in list(events[:3]):
                    title = self._plain_text(event.get("title") or "")
                    source = self._plain_text(event.get("source") or "News")
                    impact = event.get("impact")
                    try:
                        impact_text = f"{float(impact):.2f}"
                    except Exception:
                        impact_text = "-"
                    if title:
                        recent.append(f"{source}: {title} (impact {impact_text})")
                line = f"News for {symbol}: bias {direction} score {score_text}"
                if reason:
                    line += f" | reason: {reason}"
                if headline:
                    line += f" | headline summary: {headline}"
                if recent:
                    line += " | recent: " + " ; ".join(recent)
                lines.append(line)
            except Exception as exc:
                self.logger.debug("OpenAI news context failed for %s: %s", symbol, exc)
        return lines

    async def _build_openai_runtime_context(self, question=""):
        terminal = getattr(self, "terminal", None)
        broker = getattr(self, "broker", None)
        context_parts = [
            "Sopotek runtime context:",
            f"Connected: {self.connected}",
            f"Mode: {self.current_trading_mode()}",
            f"Exchange: {getattr(broker, 'exchange_name', '-') or '-'}",
            f"Account: {self.current_account_label()}",
            f"Market Data: {self.get_market_stream_status()}",
            f"Telegram: {self.telegram_management_text().replace(chr(10), ' | ')}",
            f"AI Scope: {getattr(self, 'autotrade_scope', 'all')}",
            f"Symbols Loaded: {len(getattr(self, 'symbols', []) or [])}",
            f"Default Timeframe: {getattr(self, 'time_frame', '1h')}",
            f"Health Check: {self.get_health_check_summary()}",
        ]
        bug_summary = self.market_chat_error_log_summary(open_window=False, max_entries=2)
        if bug_summary:
            context_parts.append("Bug Log Summary: " + self._plain_text(bug_summary))

        if terminal is not None:
            active_chart = None
            if hasattr(terminal, "_current_chart_widget"):
                try:
                    active_chart = terminal._current_chart_widget()
                except Exception:
                    active_chart = None
            if active_chart is not None:
                context_parts.append(
                    f"Active Chart: {getattr(active_chart, 'symbol', '-') or '-'} {getattr(active_chart, 'timeframe', '-') or '-'}"
                )
            context_parts.append(
                f"AI Trading Enabled: {bool(getattr(terminal, 'autotrading_enabled', False))}"
            )

        balances_text = self._plain_text(await self.telegram_balances_text(compact=False))
        positions_text = self._plain_text(await self.telegram_positions_text())
        orders_text = self._plain_text(await self.telegram_open_orders_text())
        if balances_text:
            context_parts.append(f"Balances: {balances_text}")
        if positions_text:
            context_parts.append(f"Positions: {positions_text}")
        if orders_text:
            context_parts.append(f"Open Orders: {orders_text}")
        position_summary = self.market_chat_position_summary(open_window=False)
        if position_summary:
            context_parts.append("Position Analysis: " + self._plain_text(position_summary))

        behavior = self.get_behavior_guard_status() or {}
        if behavior:
            summary = self._plain_text(behavior.get("summary") or "Active")
            reason = self._plain_text(behavior.get("reason") or "")
            cooldown = self._plain_text(behavior.get("cooldown_until") or "")
            behavior_line = f"Behavior Guard: {summary}"
            if reason:
                behavior_line += f" | Reason: {reason}"
            if cooldown:
                behavior_line += f" | Cooldown: {cooldown}"
            context_parts.append(behavior_line)

        if terminal is not None and hasattr(terminal, "_performance_snapshot"):
            try:
                snapshot = terminal._performance_snapshot() or {}
            except Exception:
                snapshot = {}
            if snapshot:
                context_parts.append(
                    f"Performance Headline: {self._plain_text(snapshot.get('headline') or 'Unavailable')}"
                )
                metrics = snapshot.get("metrics", {}) or {}
                selected_metrics = []
                for key in (
                    "Equity",
                    "Net PnL",
                    "Return",
                    "Win Rate",
                    "Profit Factor",
                    "Max Drawdown",
                    "Fees",
                    "Avg Slippage",
                    "Execution Drag",
                ):
                    text = self._plain_text((metrics.get(key) or {}).get("text"))
                    if text and text != "-":
                        selected_metrics.append(f"{key}: {text}")
                if selected_metrics:
                    context_parts.append("Performance Metrics: " + " | ".join(selected_metrics))

        if terminal is not None and hasattr(terminal, "_recommendation_rows"):
            try:
                recommendations = list(terminal._recommendation_rows() or [])[:5]
            except Exception:
                recommendations = []
            if recommendations:
                lines = []
                for item in recommendations:
                    symbol = self._plain_text(item.get("symbol") or "-")
                    action = self._plain_text(item.get("action") or item.get("side") or "-")
                    confidence = item.get("confidence")
                    why = self._plain_text(item.get("why") or item.get("reason") or "")
                    confidence_text = ""
                    try:
                        confidence_text = f"{float(confidence):.2f}"
                    except Exception:
                        confidence_text = self._plain_text(confidence)
                    fragment = f"{symbol} {action}"
                    if confidence_text:
                        fragment += f" conf {confidence_text}"
                    if why:
                        fragment += f" because {why}"
                    lines.append(fragment.strip())
                if lines:
                    context_parts.append("Top Recommendations: " + " ; ".join(lines))

        try:
            trade_history_analysis = await self.get_trade_history_analysis(limit=250)
        except Exception:
            trade_history_analysis = {"rows": [], "stats": {}}
        trade_stats = dict(trade_history_analysis.get("stats") or {})
        if trade_stats.get("trade_count"):
            trade_bits = [
                f"trades {int(trade_stats.get('trade_count') or 0)}",
                f"net pnl {float(trade_stats.get('net_pnl') or 0.0):.2f}",
            ]
            if trade_stats.get("win_rate") is not None:
                trade_bits.append(f"win rate {float(trade_stats.get('win_rate')) * 100.0:.1f}%")
            if trade_stats.get("profit_factor") is not None:
                profit_factor = trade_stats.get("profit_factor")
                trade_bits.append(
                    "profit factor "
                    + ("infinite" if profit_factor == float("inf") else f"{float(profit_factor):.2f}")
                )
            if trade_stats.get("journal_coverage") is not None:
                trade_bits.append(f"journal coverage {float(trade_stats.get('journal_coverage')) * 100.0:.1f}%")
            context_parts.append("Trade History Analysis: " + " | ".join(trade_bits))

            best_symbol = trade_stats.get("best_symbol")
            worst_symbol = trade_stats.get("worst_symbol")
            if best_symbol is not None or worst_symbol is not None:
                best_text = (
                    f"best {self._plain_text(best_symbol.get('symbol'))} {float(best_symbol.get('net_pnl') or 0.0):.2f}"
                    if best_symbol is not None else ""
                )
                worst_text = (
                    f"worst {self._plain_text(worst_symbol.get('symbol'))} {float(worst_symbol.get('net_pnl') or 0.0):.2f}"
                    if worst_symbol is not None else ""
                )
                context_parts.append("Trade History Symbols: " + " | ".join(part for part in (best_text, worst_text) if part))

            strategy_rows = list(trade_stats.get("strategy_rows") or [])[:3]
            if strategy_rows:
                strategy_bits = []
                for item in strategy_rows:
                    fragment = (
                        f"{self._plain_text(item.get('strategy'))}: trades {int(item.get('trades') or 0)}, "
                        f"net pnl {float(item.get('net_pnl') or 0.0):.2f}"
                    )
                    if item.get("win_rate") is not None:
                        fragment += f", win rate {float(item.get('win_rate')) * 100.0:.1f}%"
                    strategy_bits.append(fragment)
                context_parts.append("Trade History Strategies: " + " ; ".join(strategy_bits))

            recent_rows = list(trade_stats.get("recent_trades") or [])[:5]
            if recent_rows:
                recent_bits = []
                for item in recent_rows:
                    pnl = item.get("pnl")
                    pnl_text = "-" if pnl is None else f"{float(pnl):.2f}"
                    recent_bits.append(
                        f"{self._plain_text(item.get('symbol'))} {self._plain_text(item.get('side'))} "
                        f"{self._plain_text(item.get('status'))} pnl {pnl_text}"
                    )
                context_parts.append("Recent Trade History: " + " ; ".join(recent_bits))

        if terminal is not None and hasattr(terminal, "symbols_table") and hasattr(terminal, "_market_watch_row_snapshot"):
            market_rows = []
            try:
                for row in range(min(5, terminal.symbols_table.rowCount())):
                    snapshot = terminal._market_watch_row_snapshot(row)
                    symbol = self._plain_text(snapshot.get("symbol"))
                    if not symbol:
                        continue
                    bid = self._plain_text(snapshot.get("bid"))
                    ask = self._plain_text(snapshot.get("ask"))
                    status = self._plain_text(snapshot.get("status"))
                    market_rows.append(f"{symbol} bid {bid} ask {ask} status {status}")
            except Exception:
                market_rows = []
            if market_rows:
                context_parts.append("Market Watch: " + " ; ".join(market_rows))

        news_lines = await self._openai_news_context(question=question)
        if news_lines:
            context_parts.extend(news_lines)

        return "\n".join(part for part in context_parts if part)

    async def ask_openai_about_app(self, question, conversation=None):
        action_result = await self.handle_market_chat_action(question)
        if action_result:
            return action_result

        api_key = str(getattr(self, "openai_api_key", "") or "").strip()
        if not api_key:
            return "OpenAI API key is not configured in Settings -> Integrations."

        context_text = await self._build_openai_runtime_context(question=question)
        history_items = []
        for item in list(conversation or [])[-8:]:
            role = str(item.get("role") or "").strip().lower()
            if role not in {"user", "assistant"}:
                continue
            content = str(item.get("content") or "").strip()
            if not content:
                continue
            history_items.append({"role": role, "content": content})
        payload = {
            "model": self.openai_model or "gpt-5-mini",
            "input": [
                {
                    "role": "system",
                    "content": (
                        "You are an assistant inside Sopotek Trading AI. "
                        "Answer briefly, practically, and honestly using the provided app and market context. "
                        "You can discuss the app, market behavior, balances, equity, performance, profitability, "
                        "recommendations, behavior guard status, and recent news/headline context. "
                        "If data is missing, say so clearly."
                    ),
                },
                {
                    "role": "user",
                    "content": context_text,
                },
                *history_items,
                {
                    "role": "user",
                    "content": f"Question: {question}",
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

    def market_chat_voice_available(self):
        return bool(self.market_chat_voice_input_available() or self.market_chat_voice_output_available())

    def market_chat_voice_input_available(self):
        service = getattr(self, "voice_service", None)
        return bool(service is not None and service.available())

    def _windows_market_chat_voice_available(self):
        service = getattr(self, "voice_service", None)
        return bool(service is not None and service.available())

    def _resolve_market_chat_output_provider(self, output_provider=None):
        requested = str(
            output_provider if output_provider is not None else getattr(self, "voice_output_provider", "windows") or "windows"
        ).strip().lower() or "windows"
        if requested not in {"windows", "openai"}:
            requested = "windows"
        if requested == "openai":
            if str(getattr(self, "openai_api_key", "") or "").strip():
                return "openai"
            if self._windows_market_chat_voice_available():
                return "windows"
        return requested

    def market_chat_voice_output_available(self):
        output_provider = self._resolve_market_chat_output_provider()
        if output_provider == "openai":
            return bool(str(getattr(self, "openai_api_key", "") or "").strip())
        return self._windows_market_chat_voice_available()

    def market_chat_voice_provider_choices(self):
        service = getattr(self, "voice_service", None)
        if service is None:
            return [("windows", "Windows"), ("google", "Google")]
        return list(service.available_recognition_providers())

    def market_chat_voice_output_provider_choices(self):
        return [("windows", "Windows"), ("openai", "OpenAI")]

    def _current_market_chat_voice_name(self):
        output_provider = str(getattr(self, "voice_output_provider", "windows") or "windows").strip().lower() or "windows"
        if output_provider == "openai":
            voice_name = str(getattr(self, "voice_openai_name", "alloy") or "alloy").strip().lower() or "alloy"
            return voice_name if voice_name in self.OPENAI_TTS_VOICES else "alloy"
        return str(getattr(self, "voice_windows_name", "") or "").strip()

    def market_chat_voice_state(self):
        service = getattr(self, "voice_service", None)
        provider = str(getattr(self, "voice_provider", "windows") or "windows").strip().lower() or "windows"
        output_provider = str(getattr(self, "voice_output_provider", "windows") or "windows").strip().lower() or "windows"
        effective_output_provider = self._resolve_market_chat_output_provider(output_provider)
        voice_name = str(self._current_market_chat_voice_name() or "").strip()
        google_ready = bool(service is not None and service.recognition_provider_available("google"))
        windows_ready = bool(service is not None and service.recognition_provider_available("windows"))
        return {
            "provider": provider,
            "recognition_provider": provider,
            "output_provider": output_provider,
            "effective_output_provider": effective_output_provider,
            "output_fallback": output_provider != effective_output_provider,
            "voice_name": voice_name,
            "google_available": google_ready,
            "windows_available": windows_ready,
            "listen_available": self.market_chat_voice_input_available(),
            "speak_available": self.market_chat_voice_output_available(),
            "voice_available": self.market_chat_voice_available(),
            "openai_available": bool(getattr(self, "openai_api_key", "")),
            "openai_model": self.OPENAI_TTS_MODEL,
        }

    async def market_chat_list_voices(self, output_provider=None):
        resolved_provider = self._resolve_market_chat_output_provider(output_provider)
        if resolved_provider == "openai":
            return list(self.OPENAI_TTS_VOICES)
        service = getattr(self, "voice_service", None)
        if service is None:
            return []
        voices = await service.list_voices()
        return [str(item).strip() for item in voices if str(item).strip()]

    def set_market_chat_voice(self, voice_name, output_provider=None):
        normalized = str(voice_name or "").strip()
        resolved_provider = str(
            output_provider if output_provider is not None else getattr(self, "voice_output_provider", "windows") or "windows"
        ).strip().lower() or "windows"
        service = getattr(self, "voice_service", None)
        if resolved_provider == "openai":
            normalized = normalized.lower()
            if normalized and normalized not in self.OPENAI_TTS_VOICES:
                normalized = "alloy"
            self.voice_openai_name = normalized or "alloy"
            self.settings.setValue("integrations/voice_openai_name", self.voice_openai_name)
        else:
            self.voice_windows_name = normalized
            if service is not None:
                service.set_voice(normalized)
            self.settings.setValue("integrations/voice_windows_name", self.voice_windows_name)
        self.voice_name = self._current_market_chat_voice_name()
        self.settings.setValue("integrations/voice_name", self.voice_name)
        return self.voice_name

    def set_market_chat_voice_provider(self, provider):
        normalized = str(provider or "windows").strip().lower() or "windows"
        if normalized not in {"windows", "google"}:
            normalized = "windows"
        self.voice_provider = normalized
        service = getattr(self, "voice_service", None)
        if service is not None:
            service.set_recognition_provider(normalized)
        self.settings.setValue("integrations/voice_provider", normalized)
        return normalized

    def set_market_chat_voice_output_provider(self, provider):
        normalized = str(provider or "windows").strip().lower() or "windows"
        if normalized not in {"windows", "openai"}:
            normalized = "windows"
        self.voice_output_provider = normalized
        self.voice_name = self._current_market_chat_voice_name()
        service = getattr(self, "voice_service", None)
        if service is not None and normalized == "windows":
            service.set_voice(self.voice_name)
        self.settings.setValue("integrations/voice_output_provider", normalized)
        self.settings.setValue("integrations/voice_name", self.voice_name)
        return normalized

    async def market_chat_listen(self, timeout_seconds=8):
        service = getattr(self, "voice_service", None)
        if service is None:
            return {"ok": False, "message": "Voice service is not initialized.", "text": ""}
        return await service.listen(timeout_seconds=timeout_seconds, provider=getattr(self, "voice_provider", "windows"))

    async def market_chat_speak(self, text):
        requested_output_provider = str(getattr(self, "voice_output_provider", "windows") or "windows").strip().lower() or "windows"
        output_provider = self._resolve_market_chat_output_provider(requested_output_provider)
        if output_provider == "openai":
            result = await self._market_chat_speak_openai(text, voice_name=self._current_market_chat_voice_name())
            if result.get("ok") or not self._windows_market_chat_voice_available():
                return result
            service = getattr(self, "voice_service", None)
            if service is None:
                return result
            fallback = await service.speak(text, voice_name=str(getattr(self, "voice_windows_name", "") or "").strip())
            if fallback.get("ok"):
                fallback["message"] = (
                    f"OpenAI speech failed ({result.get('message') or 'unknown error'}). Used Windows speech instead."
                )
            return fallback
        service = getattr(self, "voice_service", None)
        if service is None:
            return {"ok": False, "message": "Voice service is not initialized."}
        return await service.speak(text, voice_name=str(getattr(self, "voice_windows_name", "") or "").strip())

    async def _market_chat_speak_openai(self, text, voice_name="alloy"):
        message = str(text or "").strip()
        if not message:
            return {"ok": False, "message": "No text was provided to speak."}

        api_key = str(getattr(self, "openai_api_key", "") or "").strip()
        if not api_key:
            return {"ok": False, "message": "OpenAI API key is not configured in Settings -> Integrations."}

        normalized_voice = str(voice_name or "alloy").strip().lower() or "alloy"
        if normalized_voice not in self.OPENAI_TTS_VOICES:
            normalized_voice = "alloy"

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload_candidates = [
            {
                "model": self.OPENAI_TTS_MODEL,
                "voice": normalized_voice,
                "input": message,
                "response_format": "wav",
            },
            {
                "model": self.OPENAI_TTS_MODEL,
                "voice": normalized_voice,
                "input": message,
                "format": "wav",
            },
        ]

        audio_bytes = b""
        last_error = ""
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120)) as session:
                for payload in payload_candidates:
                    async with session.post("https://api.openai.com/v1/audio/speech", json=payload, headers=headers) as response:
                        if response.status >= 400:
                            try:
                                data = await response.json(content_type=None)
                                last_error = data.get("error", {}).get("message") or str(data)
                            except Exception:
                                last_error = await response.text()
                            continue
                        audio_bytes = await response.read()
                        if audio_bytes:
                            break
        except Exception as exc:
            return {"ok": False, "message": f"OpenAI voice playback failed: {exc}"}

        if not audio_bytes:
            return {"ok": False, "message": f"OpenAI voice playback failed: {last_error or 'empty audio returned.'}"}
        if winsound is None:
            return {"ok": False, "message": "OpenAI voice playback is currently available only on Windows."}

        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as handle:
                handle.write(audio_bytes)
                temp_path = handle.name
            await asyncio.to_thread(winsound.PlaySound, temp_path, winsound.SND_FILENAME)
        except Exception as exc:
            return {"ok": False, "message": f"OpenAI voice playback failed: {exc}"}
        finally:
            if temp_path:
                try:
                    os.unlink(temp_path)
                except Exception:
                    pass

        self.voice_openai_name = normalized_voice
        self.voice_name = normalized_voice
        self.settings.setValue("integrations/voice_openai_name", normalized_voice)
        self.settings.setValue("integrations/voice_name", normalized_voice)
        return {"ok": True, "message": f"Reply spoken with OpenAI voice {normalized_voice}."}

    async def test_openai_connection(self, api_key=None, model=None):
        resolved_key = str(api_key if api_key is not None else getattr(self, "openai_api_key", "") or "").strip()
        if not resolved_key:
            return {"ok": False, "message": "OpenAI API key is not configured."}

        resolved_model = str(model if model is not None else getattr(self, "openai_model", "gpt-5-mini") or "gpt-5-mini").strip() or "gpt-5-mini"
        payload = {
            "model": resolved_model,
            "input": [
                {"role": "system", "content": "Reply in one short sentence."},
                {"role": "user", "content": "Say OpenAI connection OK and today's UTC date."},
            ],
        }
        headers = {
            "Authorization": f"Bearer {resolved_key}",
            "Content-Type": "application/json",
        }

        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
                async with session.post("https://api.openai.com/v1/responses", json=payload, headers=headers) as response:
                    data = await response.json(content_type=None)
                    if response.status >= 400:
                        message = data.get("error", {}).get("message") or str(data)
                        return {"ok": False, "message": f"OpenAI request failed: {message}"}
        except Exception as exc:
            return {"ok": False, "message": f"OpenAI request failed: {exc}"}

        text = data.get("output_text")
        if isinstance(text, str) and text.strip():
            return {"ok": True, "message": text.strip()}

        parts = []
        for item in data.get("output", []) or []:
            for content in item.get("content", []) or []:
                content_text = content.get("text")
                if isinstance(content_text, str) and content_text.strip():
                    parts.append(content_text.strip())
        if parts:
            return {"ok": True, "message": "\n".join(parts)}
        return {"ok": False, "message": "OpenAI returned no text."}

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

    async def request_recent_trades(self, symbol, limit=40):
        if not symbol:
            return

        now = time.monotonic()
        broker_name = str(getattr(getattr(self, "broker", None), "exchange_name", "") or "").lower()
        min_interval = 5.0 if broker_name == "stellar" else 1.5

        in_flight = self._recent_trades_tasks.get(symbol)
        if in_flight and not in_flight.done():
            return

        cached = list(self._recent_trades_cache.get(symbol) or [])
        last_requested = self._recent_trades_last_request_at.get(symbol, 0.0)
        if cached and (now - last_requested) < min_interval:
            self.recent_trades_signal.emit(symbol, cached[: max(1, int(limit or len(cached)))])
            return

        self._recent_trades_last_request_at[symbol] = now
        current_task = asyncio.current_task()
        if current_task is not None:
            self._recent_trades_tasks[symbol] = current_task

        try:
            trades = await self._safe_fetch_recent_trades(symbol, limit=limit)
            normalized = self._normalize_public_trade_rows(symbol, trades, limit=limit)
            self._recent_trades_cache[symbol] = normalized
            self.recent_trades_signal.emit(symbol, normalized)
        finally:
            active_task = self._recent_trades_tasks.get(symbol)
            if active_task is current_task:
                self._recent_trades_tasks.pop(symbol, None)

    def publish_ai_signal(self, symbol, signal, candles=None):
        if not symbol or not isinstance(signal, dict):
            return
        if getattr(self, "_session_closing", False):
            return
        terminal = getattr(self, "terminal", None)
        if terminal is not None and getattr(terminal, "_ui_shutting_down", False):
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
            "reason": str(signal.get("reason", "") or ""),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        self.ai_signal_monitor.emit(payload)

    def publish_strategy_debug(self, symbol, signal, candles=None, features=None):
        if not symbol or not isinstance(signal, dict):
            return
        if getattr(self, "_session_closing", False):
            return
        terminal = getattr(self, "terminal", None)
        if terminal is not None and getattr(terminal, "_ui_shutting_down", False):
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
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        self.strategy_debug_signal.emit(payload)

    async def request_candle_data(self, symbol, timeframe="1h", limit=None):
        if not symbol:
            return

        limit = self._resolve_history_limit(limit)
        candles = await self._safe_fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        received_count = len(candles) if isinstance(candles, list) else 0
        self._notify_market_data_shortfall(symbol, timeframe, received_count, limit)
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
        return df

    def _notify_market_data_shortfall(self, symbol, timeframe, received_count, requested_count):
        normalized_symbol = str(symbol or "").upper().strip()
        normalized_timeframe = str(timeframe or self.time_frame or "1h").strip() or "1h"
        try:
            requested = max(0, int(requested_count or 0))
        except Exception:
            requested = 0
        try:
            received = max(0, int(received_count or 0))
        except Exception:
            received = 0

        if not normalized_symbol or requested <= 0:
            return

        notice_cache = getattr(self, "_market_data_shortfall_notices", None)
        if not isinstance(notice_cache, dict):
            notice_cache = {}
            self._market_data_shortfall_notices = notice_cache
        cache_key = (normalized_symbol, normalized_timeframe)

        shortfall = max(0, requested - received)
        if received >= requested or shortfall <= 1:
            notice_cache.pop(cache_key, None)
            return

        if notice_cache.get(cache_key) == (received, requested):
            return
        notice_cache[cache_key] = (received, requested)

        if received <= 0:
            message = (
                f"Not enough data for {normalized_symbol} ({normalized_timeframe}): no candles were returned. "
                "Try another timeframe, load more history, or wait for more market data."
            )
        else:
            message = (
                f"Not enough data for {normalized_symbol} ({normalized_timeframe}): received {received} of "
                f"{requested} requested candles. Indicators, AI signals, and backtests may be limited."
            )

        if getattr(self, "logger", None) is not None:
            self.logger.warning(message)

        terminal = getattr(self, "terminal", None)
        system_console = getattr(terminal, "system_console", None) if terminal is not None else None
        if system_console is not None:
            system_console.log(message, "WARN")

    async def _warmup_visible_candles(self):
        # Preload a very small working set so startup stays responsive.
        warm_symbols = list(dict.fromkeys(self.symbols[:2]))
        tasks = [
            self.request_candle_data(symbol=s, timeframe=self.time_frame, limit=180)
            for s in warm_symbols
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _cleanup_session(self, stop_trading=True, close_broker=False):
        self._session_closing = True
        try:
            await self._stop_telegram_service()
            await self.news_service.close()
            self._news_cache.clear()
            self._news_inflight.clear()

            auto_assignment_task = getattr(self, "_strategy_auto_assignment_task", None)
            if auto_assignment_task is not None and not auto_assignment_task.done():
                auto_assignment_task.cancel()
            self._strategy_auto_assignment_task = None
            self.strategy_auto_assignment_in_progress = False
            self.strategy_auto_assignment_ready = not bool(getattr(self, "strategy_auto_assignment_enabled", True))
            self._update_strategy_auto_assignment_progress(
                completed=0,
                total=0,
                current_symbol="",
                timeframe=str(getattr(self, "time_frame", "1h") or "1h"),
                message="Waiting to scan symbols.",
                failed_symbols=[],
            )

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
                self.behavior_guard = None
                self._live_agent_decision_events = {}
                self._live_agent_runtime_feed = []

            if self.terminal:
                try:
                    self.terminal._ui_shutting_down = True
                except Exception:
                    pass
                try:
                    if hasattr(self.terminal, "_disconnect_controller_signals"):
                        self.terminal._disconnect_controller_signals()
                except Exception:
                    pass
                self.stack.removeWidget(self.terminal)
                self.terminal.deleteLater()
                self.terminal = None

            if close_broker and self.broker:
                await self.broker.close()
                self.broker = None

        except Exception as e:
            self.logger.error("Cleanup error: %s", e)
        finally:
            self._session_closing = False

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
