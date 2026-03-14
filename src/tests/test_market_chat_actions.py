import asyncio
import logging
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from frontend.ui.app_controller import AppController


def _make_controller():
    controller = AppController.__new__(AppController)
    controller.logger = logging.getLogger("test.market_chat")
    controller.symbols = ["BTC/USDT", "ETH/USDT"]
    controller.time_frame = "1h"
    controller.terminal = None
    controller.broker = None

    async def fake_ticker(symbol):
        return {
            "symbol": symbol,
            "price": 105.0,
            "last": 105.0,
            "bid": 104.9,
            "ask": 105.1,
        }

    async def fake_ohlcv(symbol, timeframe="1h", limit=120):
        controller._last_requested_symbol = symbol
        controller._last_requested_timeframe = timeframe
        rows = []
        for index in range(60):
            close = 100.0 + index
            rows.append([index, close - 1.0, close + 1.0, close - 2.0, close, 10.0 + index])
        return rows

    controller._safe_fetch_ticker = fake_ticker
    controller._safe_fetch_ohlcv = fake_ohlcv
    controller.get_market_stream_status = lambda: "Running"
    controller._last_requested_symbol = None
    controller._last_requested_timeframe = None
    return controller


def test_handle_market_chat_action_returns_native_snapshot_for_symbol_request():
    controller = _make_controller()

    reply = asyncio.run(controller.handle_market_chat_action("BTC/USDT"))

    assert "BTC/USDT snapshot (1h)" in reply
    assert "Trend:" in reply
    assert "RSI14:" in reply
    assert "What do you want me to do" not in reply


def test_handle_market_chat_action_uses_requested_timeframe_for_market_snapshot():
    controller = _make_controller()

    reply = asyncio.run(controller.handle_market_chat_action("price btc/usdt 4h"))

    assert "BTC/USDT snapshot (4h)" in reply
    assert controller._last_requested_timeframe == "4h"


def test_handle_market_chat_action_supports_broker_market_symbols_not_in_loaded_list():
    controller = _make_controller()
    controller.broker = SimpleNamespace(
        symbols=["AAPL", "EUR/JPY"],
        exchange=SimpleNamespace(markets={"AAPL": {}, "EUR/JPY": {}, "XAU-USD": {}}),
    )

    reply = asyncio.run(controller.handle_market_chat_action("price eur/jpy"))

    assert "EUR/JPY snapshot (1h)" in reply
    assert controller._last_requested_symbol == "EUR/JPY"


def test_handle_market_chat_action_supports_single_ticker_symbols():
    controller = _make_controller()
    controller.broker = SimpleNamespace(
        symbols=["AAPL"],
        exchange=SimpleNamespace(markets={"AAPL": {}}),
    )

    reply = asyncio.run(controller.handle_market_chat_action("analyze aapl"))

    assert "AAPL snapshot (1h)" in reply
    assert controller._last_requested_symbol == "AAPL"


def test_handle_market_chat_action_can_start_ai_trading_from_pilot():
    controller = _make_controller()
    terminal = SimpleNamespace(
        autotrading_enabled=False,
        autotrade_scope_value="selected",
    )

    def set_autotrading_enabled(enabled):
        terminal.autotrading_enabled = bool(enabled)

    terminal._set_autotrading_enabled = set_autotrading_enabled
    terminal._autotrade_scope_label = lambda: "Selected Symbol"

    controller.terminal = terminal
    controller.trading_system = object()
    controller.get_active_autotrade_symbols = lambda: ["BTC/USDT"]
    controller.is_emergency_stop_active = lambda: False

    reply = asyncio.run(controller.handle_market_chat_action("start ai trading"))

    assert reply == "AI trading is ON. Scope: Selected Symbol."
    assert terminal.autotrading_enabled is True


def test_handle_market_chat_action_can_stop_ai_trading_from_pilot():
    controller = _make_controller()
    terminal = SimpleNamespace(
        autotrading_enabled=True,
        autotrade_scope_value="all",
    )

    def set_autotrading_enabled(enabled):
        terminal.autotrading_enabled = bool(enabled)

    terminal._set_autotrading_enabled = set_autotrading_enabled
    terminal._autotrade_scope_label = lambda: "All Symbols"

    controller.terminal = terminal

    reply = asyncio.run(controller.handle_market_chat_action("stop ai trading"))

    assert reply == "AI trading is OFF."
    assert terminal.autotrading_enabled is False


def test_handle_market_chat_action_reports_why_ai_trading_cannot_start():
    controller = _make_controller()
    terminal = SimpleNamespace(
        autotrading_enabled=False,
        autotrade_scope_value="watchlist",
    )

    def set_autotrading_enabled(enabled):
        terminal.autotrading_enabled = bool(enabled)

    terminal._set_autotrading_enabled = set_autotrading_enabled
    terminal._autotrade_scope_label = lambda: "Watchlist"

    controller.terminal = terminal
    controller.trading_system = object()
    controller.get_active_autotrade_symbols = lambda: []
    controller.is_emergency_stop_active = lambda: False

    reply = asyncio.run(controller.handle_market_chat_action("start ai trading"))

    assert reply == "AI trading cannot start because the watchlist scope has no checked symbols."
    assert terminal.autotrading_enabled is False


def test_handle_market_chat_action_can_open_trade_from_pilot():
    controller = _make_controller()
    submitted = {}

    async def fake_submit_market_chat_trade(**kwargs):
        submitted.update(kwargs)
        return {"status": "filled", "order_id": "pilot-001"}

    controller.submit_market_chat_trade = fake_submit_market_chat_trade

    reply = asyncio.run(
        controller.handle_market_chat_action(
            "trade buy eur/usd amount 1000 type limit price 1.25 sl 1.2 tp 1.3 confirm"
        )
    )

    assert submitted == {
        "symbol": "EUR/USD",
        "side": "buy",
        "amount": 1000.0,
        "order_type": "limit",
        "price": 1.25,
        "stop_loss": 1.2,
        "take_profit": 1.3,
    }
    assert "Trade command executed." in reply
    assert "Status: FILLED" in reply
    assert "Order ID: pilot-001" in reply


def test_submit_market_chat_trade_converts_oanda_micro_lots_to_units():
    controller = _make_controller()
    submitted = {}

    async def fake_create_order(**kwargs):
        submitted.update(kwargs)
        return {"status": "submitted", "id": "oanda-001"}

    controller.broker = SimpleNamespace(exchange_name="oanda", create_order=fake_create_order)

    order = asyncio.run(
        controller.submit_market_chat_trade(
            symbol="EUR/USD",
            side="buy",
            amount=0.01,
            quantity_mode="lots",
        )
    )

    assert submitted["amount"] == 1000.0
    assert order["requested_amount"] == 0.01
    assert order["requested_quantity_mode"] == "lots"
    assert order["amount_units"] == 1000.0


def test_handle_market_chat_action_can_open_trade_from_pilot_in_lots():
    controller = _make_controller()
    controller.broker = SimpleNamespace(exchange_name="oanda")
    submitted = {}

    async def fake_submit_market_chat_trade(**kwargs):
        submitted.update(kwargs)
        return {"status": "filled", "order_id": "pilot-lot-001"}

    controller.submit_market_chat_trade = fake_submit_market_chat_trade

    reply = asyncio.run(
        controller.handle_market_chat_action(
            "trade buy eur/usd amount 0.01 lots confirm"
        )
    )

    assert submitted == {
        "symbol": "EUR/USD",
        "side": "buy",
        "amount": 0.01,
        "quantity_mode": "lots",
        "order_type": "market",
        "price": None,
        "stop_loss": None,
        "take_profit": None,
    }
    assert "Amount: 0.01 lots" in reply
    assert "Order ID: pilot-lot-001" in reply


def test_handle_market_chat_action_can_close_position_from_pilot():
    controller = _make_controller()
    closed = {}

    async def fake_close_market_chat_position(symbol, amount=None, quantity_mode=None):
        closed["symbol"] = symbol
        closed["amount"] = amount
        closed["quantity_mode"] = quantity_mode
        return {"status": "submitted", "order_id": "close-123"}

    controller.close_market_chat_position = fake_close_market_chat_position

    reply = asyncio.run(
        controller.handle_market_chat_action(
            "close position btc/usdt amount 0.5 confirm"
        )
    )

    assert closed == {"symbol": "BTC/USDT", "amount": 0.5, "quantity_mode": None}
    assert "Close-position command executed." in reply
    assert "Symbol: BTC/USDT" in reply
    assert "Amount: 0.5" in reply
    assert "Order ID: close-123" in reply


def test_close_market_chat_position_rejects_ambiguous_hedge_symbol_without_side():
    controller = _make_controller()
    controller.hedging_enabled = True
    controller.hedging_is_active = lambda broker=None: True
    controller.broker = SimpleNamespace()
    controller._market_chat_positions_snapshot = lambda: [
        {"symbol": "EUR/USD", "position_id": "EUR/USD:long", "position_side": "long", "amount": 1000.0, "side": "long"},
        {"symbol": "EUR/USD", "position_id": "EUR/USD:short", "position_side": "short", "amount": 1000.0, "side": "short"},
    ]

    try:
        asyncio.run(controller.close_market_chat_position("EUR/USD"))
    except RuntimeError as exc:
        assert "multiple hedge legs" in str(exc).lower()
    else:
        raise AssertionError("Expected ambiguous hedge close to raise RuntimeError")


def test_handle_market_chat_action_can_summarize_recent_bug_logs():
    controller = _make_controller()
    opened = []
    controller.terminal = SimpleNamespace(_open_logs=lambda: opened.append(True))

    with tempfile.TemporaryDirectory() as tmpdir:
        crash_path = Path(tmpdir) / "native_crash.log"
        crash_path.write_text(
            "\n".join(
                [
                    "Current thread 0x00002f8c (most recent call first):",
                    '  File "C:\\\\repo\\\\src\\\\frontend\\\\ui\\\\terminal.py", line 9199 in _update_ai_signal',
                    '  File "C:\\\\repo\\\\src\\\\frontend\\\\ui\\\\app_controller.py", line 4491 in publish_ai_signal',
                    "",
                    "=== Native crash trace session pid=27368 ===",
                ]
            ),
            encoding="utf-8",
        )
        app_log_path = Path(tmpdir) / "app.log"
        app_log_path.write_text(
            "INFO broker ready\n"
            "Error calling Python override of QObject::timerEvent()\n",
            encoding="utf-8",
        )

        controller._market_chat_log_file_paths = lambda: [crash_path, app_log_path]

        reply = asyncio.run(controller.handle_market_chat_action("show bug summary"))

    assert opened == [True]
    assert "Bug summary from local logs:" in reply
    assert "native_crash.log" in reply
    assert "terminal.py:9199 in _update_ai_signal" in reply
    assert "app.log" in reply


def test_margin_closeout_snapshot_uses_reported_or_derived_balance_metrics():
    controller = _make_controller()
    controller.margin_closeout_guard_enabled = True
    controller.max_margin_closeout_pct = 0.50
    controller.balances = {
        "equity": 1000.0,
        "used": {"USD": 400.0},
        "raw": {"marginCloseoutPercent": "0.62", "NAV": "1000", "marginUsed": "620"},
    }

    snapshot = controller.margin_closeout_snapshot()

    assert snapshot["available"] is True
    assert abs(float(snapshot["ratio"]) - 0.62) < 1e-9
    assert snapshot["blocked"] is True
    assert "blocked" in snapshot["reason"].lower()


def test_assign_ranked_strategies_to_symbol_persists_top_ranked_variants():
    stored = {}
    controller = AppController.__new__(AppController)
    controller.settings = SimpleNamespace(setValue=lambda key, value: stored.__setitem__(key, value))
    controller.multi_strategy_enabled = True
    controller.max_symbol_strategies = 3
    controller.symbol_strategy_assignments = {}
    controller.symbol_strategy_rankings = {}
    controller.time_frame = "1h"
    controller.trading_system = None

    assigned = controller.assign_ranked_strategies_to_symbol(
        "eur_usd",
        [
            {"strategy_name": "EMA Cross | London Session Aggressive", "score": 9.0, "total_profit": 120.0},
            {"strategy_name": "Trend Following | Scalp Conservative", "score": 6.0, "total_profit": 90.0},
            {"strategy_name": "MACD Trend", "score": 3.0, "total_profit": 40.0},
        ],
        top_n=2,
        timeframe="4h",
    )

    assert len(assigned) == 2
    assert assigned[0]["strategy_name"] == "EMA Cross | London Session Aggressive"
    assert assigned[1]["strategy_name"] == "Trend Following | Scalp Conservative"
    assert abs(sum(item["weight"] for item in assigned) - 1.0) < 1e-9
    assert "EUR/USD" in controller.symbol_strategy_assignments
    assert "strategy/symbol_assignments" in stored


def test_assign_strategy_to_symbol_persists_manual_symbol_override_and_can_clear_it():
    stored = {}
    controller = AppController.__new__(AppController)
    controller.settings = SimpleNamespace(setValue=lambda key, value: stored.__setitem__(key, value))
    controller.multi_strategy_enabled = False
    controller.max_symbol_strategies = 3
    controller.symbol_strategy_assignments = {}
    controller.symbol_strategy_rankings = {}
    controller.time_frame = "1h"
    controller.strategy_name = "Trend Following"
    controller.trading_system = None

    assigned = controller.assign_strategy_to_symbol(
        "eur_usd",
        "EMA Cross | London Session Aggressive",
        timeframe="4h",
    )

    assert controller.multi_strategy_enabled is True
    assert assigned == controller.assigned_strategies_for_symbol("EUR/USD")
    assert assigned[0]["strategy_name"] == "EMA Cross | London Session Aggressive"
    assert assigned[0]["assignment_mode"] == "single"
    assert assigned[0]["timeframe"] == "4h"

    removed = controller.clear_symbol_strategy_assignment("EUR/USD")

    assert removed[0]["strategy_name"] == "EMA Cross | London Session Aggressive"
    assert controller.assigned_strategies_for_symbol("EUR/USD")[0]["strategy_name"] == "Trend Following"
    assert "strategy/symbol_assignments" in stored
