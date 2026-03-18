import asyncio
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from frontend.ui.app_controller import AppController, _bounded_window_extent
from event_bus.event_types import EventType


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
