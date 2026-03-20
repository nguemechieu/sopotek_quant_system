import asyncio
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import ccxt.async_support as ccxt

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from frontend.ui.app_controller import AppController, _bounded_window_extent
from event_bus.event_types import EventType
from market_data.ticker_buffer import TickerBuffer
from market_data.ticker_stream import TickerStream


class _SignalRecorder:
    def __init__(self):
        self.calls = []

    def emit(self, *args):
        self.calls.append(args)


class _BufferRecorder:
    def __init__(self):
        self.calls = []

    def update(self, symbol, row):
        self.calls.append((symbol, dict(row)))


class _SettingsRecorder:
    def __init__(self, initial=None):
        self._values = dict(initial or {})

    def value(self, key, default=None):
        return self._values.get(key, default)

    def setValue(self, key, value):
        self._values[key] = value


def _make_controller(candles):
    logs = []
    controller = AppController.__new__(AppController)
    controller.logger = logging.getLogger("test.market_data_messages")
    controller.time_frame = "1h"
    controller.terminal = SimpleNamespace(
        system_console=SimpleNamespace(log=lambda message, level="INFO": logs.append((message, level)))
    )
    controller.candle_buffers = {}
    controller.candle_buffer = _BufferRecorder()
    controller.candle_signal = _SignalRecorder()
    controller._market_data_shortfall_notices = {}
    controller._resolve_history_limit = lambda limit=None: int(limit or 200)

    async def fake_fetch(symbol, timeframe="1h", limit=200):
        return candles

    controller._safe_fetch_ohlcv = fake_fetch
    return controller, logs




def test_bounded_window_extent_clamps_to_small_screen():
    size, minimum = _bounded_window_extent(1600, 900, margin=24, minimum=960)

    assert size == 876
    assert minimum == 876


def test_bounded_window_extent_preserves_requested_size_when_it_fits():
    size, minimum = _bounded_window_extent(1200, 1920, margin=24, minimum=960)

    assert size == 1200
    assert minimum == 960

def test_request_candle_data_warns_when_history_is_short():
    candles = [
        [1, 100.0, 101.0, 99.0, 100.5, 10.0],
        [2, 100.5, 101.5, 100.0, 101.0, 12.0],
        [3, 101.0, 102.0, 100.5, 101.2, 11.0],
    ]
    controller, logs = _make_controller(candles)

    df = asyncio.run(controller.request_candle_data("XLM/USDC", timeframe="1h", limit=120))

    assert df is not None
    assert any("Not enough data for XLM/USDC (1h): received 3 of 120 requested candles." in message for message, _ in logs)
    assert logs[-1][1] == "WARN"
    assert controller.candle_signal.calls


def test_request_candle_data_sanitizes_malformed_ohlcv_rows_before_emitting():
    candles = [
        [1710000000000, 100.0, 95.0, 105.0, 101.0, -4.0],
        [1710000000000, 101.0, 106.0, 99.5, 103.0, 12.0],
        [1710003600000, "bad", 108.0, 100.0, 104.0, 6.0],
        [1710007200000, 104.0, 109.0, 102.0, 108.0, None],
        [None, 105.0, 110.0, 103.0, 109.0, 8.0],
        [1710010800000, 0.0, 111.0, 104.0, 110.0, 9.0],
    ]
    controller, logs = _make_controller(candles)

    df = asyncio.run(controller.request_candle_data("BTC/USDT", timeframe="1h", limit=6))

    assert df is not None
    assert list(df.columns) == ["timestamp", "open", "high", "low", "close", "volume"]
    assert len(df.index) == 2
    assert float(df.iloc[0]["open"]) == 101.0
    assert float(df.iloc[0]["high"]) == 106.0
    assert float(df.iloc[0]["low"]) == 99.5
    assert float(df.iloc[0]["close"]) == 103.0
    assert float(df.iloc[0]["volume"]) == 12.0
    assert float(df.iloc[1]["open"]) == 104.0
    assert float(df.iloc[1]["high"]) == 109.0
    assert float(df.iloc[1]["low"]) == 102.0
    assert float(df.iloc[1]["close"]) == 108.0
    assert float(df.iloc[1]["volume"]) == 0.0
    assert any("Sanitized OHLCV data for BTC/USDT (1h) from runtime" in message for message, _level in logs)
    assert controller.candle_buffer.calls[-1][1]["close"] == 108.0


def test_request_candle_data_warns_when_no_history_is_available():
    controller, logs = _make_controller([])

    df = asyncio.run(controller.request_candle_data("XLM/USDC", timeframe="1h", limit=120))

    assert df is None
    assert logs == [
        (
            "Not enough data for XLM/USDC (1h): no candles were returned. Try another timeframe, load more history, or wait for more market data.",
            "WARN",
        )
    ]
    assert controller.candle_signal.calls == []


def test_safe_fetch_ohlcv_does_not_synthesize_history_from_single_ticker():
    logs = []
    controller = AppController.__new__(AppController)
    controller.logger = logging.getLogger("test.market_data_messages.no_synthetic_history")
    controller.time_frame = "4h"
    controller.terminal = SimpleNamespace(
        system_console=SimpleNamespace(log=lambda message, level="INFO": logs.append((message, level)))
    )
    controller._market_data_warning_timestamps = {}
    controller._resolve_history_limit = lambda limit=None: int(limit or 200)

    async def fake_load(symbol, timeframe="1h", limit=200, start_time=None, end_time=None):
        return []

    persisted = []

    async def fake_persist(symbol, timeframe, rows):
        persisted.append((symbol, timeframe, list(rows)))

    ticker_calls = []

    async def fake_ticker(symbol):
        ticker_calls.append(symbol)
        return {
            "symbol": symbol,
            "last": 71.25,
        }

    async def failing_fetch_ohlcv(symbol, timeframe="1h", limit=200, start_time=None, end_time=None):
        raise RuntimeError("history unavailable")

    controller._load_candles_from_db = fake_load
    controller._persist_candles_to_db = fake_persist
    controller._safe_fetch_ticker = fake_ticker
    controller.broker = SimpleNamespace(fetch_ohlcv=failing_fetch_ohlcv)

    rows = asyncio.run(controller._safe_fetch_ohlcv("ALCX/USDC", timeframe="4h", limit=240))

    assert rows == []
    assert ticker_calls == []
    assert persisted == []
    assert logs == []


def test_safe_fetch_ticker_uses_cached_stellar_snapshot_after_dns_failure():
    logs = []
    controller = AppController.__new__(AppController)
    controller.logger = logging.getLogger("test.market_data_messages.cached_ticker")
    controller.terminal = SimpleNamespace(
        system_console=SimpleNamespace(log=lambda message, level="INFO": logs.append((message, level)))
    )
    controller.ticker_stream = TickerStream()
    controller.ticker_buffer = TickerBuffer(max_length=20)
    controller._market_data_warning_timestamps = {}

    async def failing_fetch(symbol):
        raise OSError("Cannot connect to host horizon.stellar.org:443 ssl:default [getaddrinfo failed]")

    cached = {
        "symbol": "BTC/XLM",
        "last": 0.125,
        "bid": 0.124,
        "ask": 0.126,
        "timestamp": "2026-03-19T12:00:00+00:00",
    }
    controller.broker = SimpleNamespace(exchange_name="stellar", fetch_ticker=failing_fetch)
    controller.ticker_stream.update("BTC/XLM", cached)
    controller.ticker_buffer.update("BTC/XLM", cached)

    result = asyncio.run(controller._safe_fetch_ticker("BTC/XLM"))

    assert result == cached
    assert logs == [
        (
            "Stellar Horizon is temporarily unreachable for BTC/XLM. Using cached ticker data while the connection recovers.",
            "WARN",
        )
    ]


def test_safe_fetch_ticker_rate_limits_hotspot_warning_without_cache():
    logs = []
    controller = AppController.__new__(AppController)
    controller.logger = logging.getLogger("test.market_data_messages.hotspot_warning")
    controller.terminal = SimpleNamespace(
        system_console=SimpleNamespace(log=lambda message, level="INFO": logs.append((message, level)))
    )
    controller.ticker_stream = TickerStream()
    controller.ticker_buffer = TickerBuffer(max_length=20)
    controller._market_data_warning_timestamps = {}

    async def failing_fetch(symbol):
        raise OSError("Cannot connect to host horizon.stellar.org:443 ssl:default [getaddrinfo failed]")

    controller.broker = SimpleNamespace(exchange_name="stellar", fetch_ticker=failing_fetch)

    async def scenario():
        first = await controller._safe_fetch_ticker("BTC/XLM")
        second = await controller._safe_fetch_ticker("BTC/XLM")
        return first, second

    first, second = asyncio.run(scenario())

    assert first is None
    assert second is None
    assert logs == [
        (
            "Stellar Horizon is temporarily unreachable (Cannot connect to host horizon.stellar.org:443 ssl:default [getaddrinfo failed]). The app will keep retrying automatically.",
            "WARN",
        )
    ]


def test_safe_fetch_ticker_skips_unsupported_coinbase_symbol():
    logs = []
    controller = AppController.__new__(AppController)
    controller.logger = logging.getLogger("test.market_data_messages.unsupported_symbol")
    controller.terminal = SimpleNamespace(
        system_console=SimpleNamespace(log=lambda message, level="INFO": logs.append((message, level)))
    )
    controller.ticker_stream = TickerStream()
    controller.ticker_buffer = TickerBuffer(max_length=20)
    controller._market_data_warning_timestamps = {}

    class UnsupportedCoinbaseBroker:
        exchange_name = "coinbase"
        symbols = ["BTC/USD", "ETH/USD"]

        @staticmethod
        def supports_symbol(symbol):
            return symbol in {"BTC/USD", "ETH/USD"}

        async def fetch_ticker(self, symbol):
            raise ccxt.BadSymbol(f"coinbase does not have market symbol {symbol}")

    controller.broker = UnsupportedCoinbaseBroker()

    result = asyncio.run(controller._safe_fetch_ticker("EUR/USD"))

    assert result is None
    assert logs == [
        (
            "COINBASE does not support market symbol EUR/USD on the active broker. The app will skip live market data for this symbol until you switch to a supported market.",
            "WARN",
        )
    ]


def test_extract_balance_equity_value_reads_nested_nav():
    controller = AppController.__new__(AppController)

    equity = controller._extract_balance_equity_value(
        {
            "raw": {
                "NAV": "12500.25",
            }
        }
    )

    assert equity == 12500.25


def test_update_balance_records_equity_and_emits_signal():
    controller = AppController.__new__(AppController)
    controller.logger = logging.getLogger("test.app_controller.performance")
    controller.broker = SimpleNamespace(fetch_balance=lambda: None)
    controller.balance = {}
    controller.balances = {}
    controller.equity_signal = _SignalRecorder()
    behavior_guard_updates = []
    controller._update_behavior_guard_equity = lambda balances: behavior_guard_updates.append(dict(balances))
    recorded_equity = []
    controller.performance_engine = SimpleNamespace(
        equity_curve=[],
        update_equity=lambda value: recorded_equity.append(float(value)),
    )

    async def fake_fetch_balance():
        return {"raw": {"NAV": "10250.50"}}

    controller.broker.fetch_balance = fake_fetch_balance

    asyncio.run(controller.update_balance())

    assert controller.balances == {"raw": {"NAV": "10250.50"}}
    assert controller.balance == {"raw": {"NAV": "10250.50"}}
    assert recorded_equity == [10250.5]
    assert controller.equity_signal.calls == [(10250.5,)]
    assert behavior_guard_updates == [{"raw": {"NAV": "10250.50"}}]


def test_extract_balance_equity_ignores_single_non_cash_asset_quantity():
    controller = AppController.__new__(AppController)

    equity = controller._extract_balance_equity_value(
        {
            "total": {
                "BTC": 0.25,
            }
        }
    )

    assert equity is None

def test_performance_history_persists_timestamp_payload():
    controller = AppController.__new__(AppController)
    controller.settings = _SettingsRecorder()
    controller.performance_engine = SimpleNamespace(
        equity_curve=[1000.0, 1010.5],
        equity_timestamps=[1710000000.0, 1710003600.0],
    )

    controller._persist_performance_history()
    restored = controller._load_persisted_performance_history()

    assert restored == [
        {"equity": 1000.0, "timestamp": 1710000000.0},
        {"equity": 1010.5, "timestamp": 1710003600.0},
    ]


def test_set_forex_candle_price_component_updates_live_oanda_preferences():
    controller = AppController.__new__(AppController)
    controller.settings = _SettingsRecorder()
    controller.forex_candle_price_component = "mid"
    controller.config = SimpleNamespace(broker=SimpleNamespace(options={"market_type": "auto"}))
    observed = []
    controller.broker = SimpleNamespace(
        exchange_name="oanda",
        set_candle_price_component=lambda value: observed.append(value),
    )

    normalized = controller.set_forex_candle_price_component("Bid")

    assert normalized == "bid"
    assert controller.forex_candle_price_component == "bid"
    assert controller.settings._values["market_data/forex_candle_price_component"] == "bid"
    assert controller.config.broker.options["candle_price_component"] == "bid"
    assert observed == ["bid"]


def test_safe_fetch_ohlcv_returns_live_broker_rows_for_non_range_requests():
    controller = AppController.__new__(AppController)
    controller.limit = 50000
    controller.MAX_HISTORY_LIMIT = 50000
    controller.time_frame = "1h"
    controller.logger = logging.getLogger("test.market_data_messages.live_broker_rows")
    controller.terminal = None
    controller._market_data_warning_timestamps = {}
    controller._market_data_shortfall_notices = {}

    class Broker:
        exchange_name = "oanda"
        symbols = ["USD_JPY"]

        async def fetch_ohlcv(self, symbol, timeframe="1h", limit=100, start_time=None, end_time=None):
            return [
                ["2026-03-19T10:00:00Z", 150.0, 151.0, 149.5, 150.5, 100.0],
                ["2026-03-19T11:00:00Z", 150.5, 151.2, 150.1, 150.9, 110.0],
            ]

    async def fake_load(symbol, timeframe="1h", limit=200, start_time=None, end_time=None):
        return []

    async def fake_persist(symbol, timeframe, rows):
        return None

    controller.broker = Broker()
    controller._load_candles_from_db = fake_load
    controller._persist_candles_to_db = fake_persist

    rows = asyncio.run(controller._safe_fetch_ohlcv("USD/JPY", timeframe="1h", limit=180))

    assert len(rows) == 2
    assert rows[0][0].startswith("2026-03-19T10:00:00")



def test_request_candle_data_does_not_warn_when_only_one_bar_is_missing():
    candles = [
        [index, 100.0, 101.0, 99.0, 100.5, 10.0]
        for index in range(1, 180)
    ]
    controller, logs = _make_controller(candles)

    df = asyncio.run(controller.request_candle_data("USD/JPY", timeframe="1h", limit=180))

    assert df is not None
    assert logs == []


def test_latest_agent_decision_chain_prefers_live_runtime_events():
    controller = AppController.__new__(AppController)
    controller.logger = logging.getLogger("test.app_controller.agent_runtime")
    controller.agent_runtime_signal = _SignalRecorder()
    controller._live_agent_decision_events = {}
    controller.agent_decision_repository = SimpleNamespace(
        latest_chain_for_symbol=lambda *args, **kwargs: [
            SimpleNamespace(
                id=7,
                decision_id="repo-1",
                exchange=None,
                account_label=None,
                symbol="EUR/USD",
                agent_name="RiskAgent",
                stage="approved",
                strategy_name="Trend Following",
                timeframe="1h",
                side="buy",
                confidence=None,
                approved=True,
                reason="repo",
                timestamp=datetime(2026, 3, 17, 10, 0, tzinfo=timezone.utc),
                payload_json="{}",
            )
        ]
    )
    controller._active_exchange_code = lambda: None
    controller.current_account_label = lambda: None
    controller.trading_system = None

    controller._handle_live_agent_memory_event(
        {
            "agent": "SignalAgent",
            "stage": "selected",
            "symbol": "EUR/USD",
            "decision_id": "live-1",
            "timestamp": "2026-03-17T10:01:00+00:00",
            "payload": {
                "strategy_name": "EMA Cross",
                "timeframe": "4h",
                "side": "buy",
                "reason": "live breakout",
                "confidence": 0.82,
            },
        }
    )

    chain = controller.latest_agent_decision_chain_for_symbol("eur_usd")

    assert len(chain) == 1
    assert chain[0]["decision_id"] == "live-1"
    assert chain[0]["strategy_name"] == "EMA Cross"
    assert controller.agent_runtime_signal.calls[0][0]["kind"] == "memory"


def test_handle_trading_agent_bus_event_emits_runtime_message():
    controller = AppController.__new__(AppController)
    controller.logger = logging.getLogger("test.app_controller.agent_bus")
    controller.agent_runtime_signal = _SignalRecorder()

    event = SimpleNamespace(
        type=EventType.RISK_ALERT,
        data={
            "symbol": "EUR/USD",
            "decision_id": "dec-1",
            "strategy_name": "EMA Cross",
            "timeframe": "4h",
            "reason": "Risk blocked the trade.",
            "side": "buy",
        },
    )

    asyncio.run(controller._handle_trading_agent_bus_event(event))

    payload = controller.agent_runtime_signal.calls[0][0]
    assert payload["kind"] == "bus"
    assert payload["event_type"] == EventType.RISK_ALERT
    assert payload["symbol"] == "EUR/USD"
    assert "Risk blocked the trade" in payload["message"]


def test_live_agent_runtime_feed_keeps_latest_rows_and_supports_filters():
    controller = AppController.__new__(AppController)
    controller.logger = logging.getLogger("test.app_controller.runtime_feed")
    controller.agent_runtime_signal = _SignalRecorder()
    controller._live_agent_runtime_feed = []

    controller._emit_agent_runtime_signal(
        {
            "kind": "memory",
            "symbol": "EUR/USD",
            "agent_name": "SignalAgent",
            "stage": "selected",
            "strategy_name": "EMA Cross",
            "timeframe": "4h",
            "timestamp": datetime(2026, 3, 17, 10, 5, tzinfo=timezone.utc),
            "message": "Signal selected for EUR/USD.",
        }
    )
    controller._emit_agent_runtime_signal(
        {
            "kind": "bus",
            "event_type": EventType.RISK_APPROVED,
            "symbol": "GBP/USD",
            "timeframe": "1h",
            "message": "Risk approved BUY for GBP/USD.",
        }
    )

    all_rows = controller.live_agent_runtime_feed(limit=10)
    eur_rows = controller.live_agent_runtime_feed(limit=10, symbol="eur_usd")
    bus_rows = controller.live_agent_runtime_feed(limit=10, kinds="bus")

    assert len(all_rows) == 2
    assert all_rows[0]["symbol"] == "GBP/USD"
    assert all_rows[1]["symbol"] == "EUR/USD"
    assert eur_rows == [all_rows[1]]
    assert bus_rows == [all_rows[0]]
    assert all_rows[0]["timestamp_label"]
