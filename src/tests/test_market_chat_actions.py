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
        "stop_price": None,
        "stop_loss": 1.2,
        "take_profit": 1.3,
    }
    assert "Trade command executed." in reply
    assert "Status: FILLED" in reply
    assert "Order ID: pilot-001" in reply


def test_handle_market_chat_action_can_open_stop_limit_trade_from_pilot():
    controller = _make_controller()
    submitted = {}

    async def fake_submit_market_chat_trade(**kwargs):
        submitted.update(kwargs)
        return {"status": "filled", "order_id": "pilot-stop-limit-001"}

    controller.submit_market_chat_trade = fake_submit_market_chat_trade

    reply = asyncio.run(
        controller.handle_market_chat_action(
            "trade sell eur/usd amount 1000 type stop_limit price 1.25 trigger 1.255 sl 1.26 tp 1.2 confirm"
        )
    )

    assert submitted == {
        "symbol": "EUR/USD",
        "side": "sell",
        "amount": 1000.0,
        "order_type": "stop_limit",
        "price": 1.25,
        "stop_price": 1.255,
        "stop_loss": 1.26,
        "take_profit": 1.2,
    }
    assert "Trade command executed." in reply
    assert "Type: STOP_LIMIT" in reply
    assert "Order ID: pilot-stop-limit-001" in reply


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


def test_submit_market_chat_trade_passes_stop_limit_trigger_to_broker():
    controller = _make_controller()
    submitted = {}

    async def fake_create_order(**kwargs):
        submitted.update(kwargs)
        return {"status": "submitted", "id": "stop-limit-001"}

    controller.broker = SimpleNamespace(exchange_name="paper", create_order=fake_create_order)

    order = asyncio.run(
        controller.submit_market_chat_trade(
            symbol="BTC/USDT",
            side="buy",
            amount=1.5,
            order_type="stop_limit",
            price=64990.0,
            stop_price=65010.0,
        )
    )

    assert submitted["type"] == "stop_limit"
    assert submitted["price"] == 64990.0
    assert submitted["stop_price"] == 65010.0
    assert order["amount_units"] == 1.5


def test_submit_market_chat_trade_caps_buy_size_to_available_quote_balance():
    controller = _make_controller()
    submitted = {}

    async def fake_fetch_balance():
        return {"free": {"USDT": 100.0}}

    async def fake_create_order(**kwargs):
        submitted.update(kwargs)
        return {"status": "submitted", "id": "spot-cap-001", "amount": kwargs["amount"]}

    controller.broker = SimpleNamespace(
        exchange_name="paper",
        create_order=fake_create_order,
        fetch_balance=fake_fetch_balance,
    )

    order = asyncio.run(
        controller.submit_market_chat_trade(
            symbol="BTC/USDT",
            side="buy",
            amount=2.0,
        )
    )

    expected_amount = 100.0 * controller.ORDER_SIZE_BUFFER / 105.1
    assert abs(submitted["amount"] - expected_amount) < 1e-9
    assert order["size_adjusted"] is True
    assert abs(float(order["amount_units"]) - expected_amount) < 1e-9


def test_submit_market_chat_trade_applies_smaller_openai_size_recommendation():
    controller = _make_controller()
    submitted = {}

    async def fake_create_order(**kwargs):
        submitted.update(kwargs)
        return {"status": "submitted", "id": "ai-size-001", "amount": kwargs["amount"]}

    async def fake_recommend_trade_size_with_openai(**_kwargs):
        return {"recommended_units": 0.4, "reason": "Reduce size for this symbol volatility."}

    controller.broker = SimpleNamespace(exchange_name="paper", create_order=fake_create_order)
    controller._recommend_trade_size_with_openai = fake_recommend_trade_size_with_openai

    order = asyncio.run(
        controller.submit_market_chat_trade(
            symbol="BTC/USDT",
            side="buy",
            amount=1.0,
        )
    )

    assert submitted["amount"] == 0.4
    assert order["ai_adjusted"] is True
    assert order["size_adjusted"] is True
    assert order["applied_requested_mode_amount"] == 0.4


def test_handle_market_chat_action_surfaces_chatgpt_size_note():
    controller = _make_controller()

    async def fake_submit_market_chat_trade(**_kwargs):
        return {
            "status": "submitted",
            "order_id": "ai-size-note-001",
            "requested_quantity_mode": "units",
            "requested_amount": 1.0,
            "applied_requested_mode_amount": 0.4,
            "size_adjusted": True,
            "sizing_summary": "Preflight reduced the order size.",
            "ai_sizing_reason": "Reduce size for this symbol volatility.",
        }

    controller.submit_market_chat_trade = fake_submit_market_chat_trade

    reply = asyncio.run(
        controller.handle_market_chat_action(
            "trade buy btc/usdt amount 1 confirm"
        )
    )

    assert "Amount: 0.4 units" in reply
    assert "Requested Amount: 1.0 units" in reply
    assert "ChatGPT Size Note: Reduce size for this symbol volatility." in reply


def test_submit_market_chat_trade_blocks_when_margin_closeout_guard_trips():
    controller = _make_controller()
    controller.margin_closeout_guard_enabled = True
    controller.max_margin_closeout_pct = 0.50

    async def fake_fetch_balance():
        return {
            "equity": 1000.0,
            "used": {"USD": 620.0},
            "raw": {"marginCloseoutPercent": "0.62", "NAV": "1000", "marginUsed": "620"},
        }

    async def fake_create_order(**_kwargs):
        raise AssertionError("create_order should not run when the closeout guard blocks the trade")

    controller.broker = SimpleNamespace(
        exchange_name="oanda",
        create_order=fake_create_order,
        fetch_balance=fake_fetch_balance,
    )

    try:
        asyncio.run(
            controller.submit_market_chat_trade(
                symbol="EUR/USD",
                side="buy",
                amount=1000.0,
            )
        )
    except RuntimeError as exc:
        assert "blocked" in str(exc).lower()
    else:
        raise AssertionError("Expected margin closeout guard to block the trade")


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
        "stop_price": None,
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


def test_close_market_chat_position_treats_selected_position_amount_as_units():
    controller = _make_controller()
    captured = {}

    async def fake_close_position(symbol, amount=None, order_type="market", position=None, position_side=None, position_id=None):
        captured["symbol"] = symbol
        captured["amount"] = amount
        captured["position"] = position
        captured["position_side"] = position_side
        captured["position_id"] = position_id
        return {"status": "submitted", "id": "close-raw-units"}

    controller.broker = SimpleNamespace(exchange_name="oanda", close_position=fake_close_position)
    position = {
        "symbol": "USD_HUF",
        "position_id": "USD_HUF:long",
        "position_side": "long",
        "amount": 1250.0,
        "side": "long",
    }
    controller._market_chat_positions_snapshot = lambda: [position]

    result = asyncio.run(
        controller.close_market_chat_position(
            "USD_HUF",
            amount=1250.0,
            position=position,
        )
    )

    assert result["status"] == "submitted"
    assert captured["symbol"] == "USD_HUF"
    assert captured["amount"] == 1250.0
    assert captured["position_side"] == "long"
    assert captured["position_id"] == "usd_huf:long"


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
