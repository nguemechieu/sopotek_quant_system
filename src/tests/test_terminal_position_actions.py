import asyncio
import os
import sys
from pathlib import Path
from types import SimpleNamespace

from PySide6.QtWidgets import QApplication, QComboBox, QLabel, QPushButton, QSpinBox, QTableWidget

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from frontend.ui.terminal import Terminal


def _app():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_populate_positions_table_adds_close_action_widgets():
    _app()
    table = QTableWidget()
    close_all_button = QPushButton()
    fake = SimpleNamespace(
        positions_table=table,
        positions_close_all_button=close_all_button,
        controller=SimpleNamespace(broker=object()),
        _normalize_position_entry=lambda raw: Terminal._normalize_position_entry(SimpleNamespace(_lookup_symbol_mid_price=lambda _symbol: None), raw),
        _action_button_style=lambda: "",
    )
    fake._build_position_close_button = lambda position, compact=False: Terminal._build_position_close_button(fake, position, compact=compact)
    fake._confirm_close_position = lambda position: None

    Terminal._populate_positions_table(
        fake,
        [
            {
                "symbol": "EUR/USD",
                "side": "long",
                "amount": 2.0,
                "entry_price": 1.1,
                "mark_price": 1.2,
                "pnl": 10.0,
            }
        ],
    )

    assert table.rowCount() == 1
    assert table.cellWidget(0, 7) is not None
    assert isinstance(table.cellWidget(0, 7), QPushButton)
    assert close_all_button.isEnabled() is True


def test_close_position_async_calls_controller_and_refreshes_views():
    refreshed = {"positions": 0, "analysis": 0, "messages": []}

    async def fake_close_market_chat_position(symbol, amount=None, position=None):
        refreshed["symbol"] = symbol
        refreshed["amount"] = amount
        refreshed["position"] = position
        return {"status": "submitted"}

    fake = SimpleNamespace(
        controller=SimpleNamespace(close_market_chat_position=fake_close_market_chat_position),
        system_console=SimpleNamespace(log=lambda *_args, **_kwargs: None),
        logger=SimpleNamespace(exception=lambda *_args, **_kwargs: None),
        _schedule_positions_refresh=lambda: refreshed.__setitem__("positions", refreshed["positions"] + 1),
        _refresh_position_analysis_window=lambda: refreshed.__setitem__("analysis", refreshed["analysis"] + 1),
        _show_async_message=lambda title, text, icon=None: refreshed["messages"].append((title, text)),
    )

    asyncio.run(
        Terminal._close_position_async(
            fake,
            "EUR/USD",
            amount=1.5,
            position={"symbol": "EUR/USD", "position_side": "short", "position_id": "EUR/USD:short"},
            show_dialog=True,
        )
    )

    assert refreshed["symbol"] == "EUR/USD"
    assert refreshed["amount"] == 1.5
    assert refreshed["position"]["position_side"] == "short"
    assert refreshed["positions"] == 1
    assert refreshed["analysis"] == 1
    assert refreshed["messages"]


def test_validate_manual_trade_amount_converts_micro_lots_to_oanda_units():
    fake = SimpleNamespace()
    fake.controller = SimpleNamespace(
        trade_quantity_context=lambda symbol: {
            "symbol": str(symbol).upper(),
            "supports_lots": True,
            "default_mode": "lots",
            "lot_units": 100000.0,
        }
    )
    fake._manual_trade_quantity_context = lambda symbol: Terminal._manual_trade_quantity_context(fake, symbol)
    fake._normalize_manual_trade_quantity_mode = lambda value: Terminal._normalize_manual_trade_quantity_mode(fake, value)
    fake._manual_trade_format_context = lambda _symbol: {
        "min_amount": 1.0,
        "amount_formatter": lambda value: value,
    }
    fake._normalize_manual_trade_amount = (
        lambda symbol, amount, quantity_mode="units": Terminal._normalize_manual_trade_amount(
            fake, symbol, amount, quantity_mode=quantity_mode
        )
    )

    amount, error = Terminal._validate_manual_trade_amount(fake, "EUR/USD", 0.01, quantity_mode="lots")

    assert error is None
    assert amount == 1000.0


def test_update_risk_heatmap_uses_live_position_snapshot():
    class _RiskMap:
        def __init__(self):
            self.image = None
            self.levels = None

        def setImage(self, image, autoLevels=False, levels=None):
            self.image = image
            self.levels = levels

    state = {}
    fake = SimpleNamespace(
        risk_map=_RiskMap(),
        _latest_positions_snapshot=[
            {
                "symbol": "EUR/USD",
                "side": "long",
                "amount": 1000.0,
                "entry_price": 1.10,
                "mark_price": 1.11,
                "value": 1110.0,
            }
        ],
        _normalize_position_entry=lambda raw: Terminal._normalize_position_entry(
            SimpleNamespace(_lookup_symbol_mid_price=lambda _symbol: None), raw
        ),
        _portfolio_positions_snapshot=lambda: [],
        _set_risk_heatmap_status=lambda message, tone="muted": state.update({"message": message, "tone": tone}),
    )
    fake._risk_heatmap_positions_snapshot = lambda: Terminal._risk_heatmap_positions_snapshot(fake)

    Terminal._update_risk_heatmap(fake)

    assert fake.risk_map.image is not None
    assert fake.risk_map.image.shape == (1, 1)
    assert "Live risk snapshot across 1 position" in state["message"]
    assert state["tone"] == "positive"


def test_populate_strategy_picker_groups_family_variants_together():
    _app()
    picker = QComboBox()
    fake = SimpleNamespace(
        _strategy_family_name=lambda name: Terminal._strategy_family_name(SimpleNamespace(), name),
    )
    fake._grouped_strategy_names = lambda selected_strategy=None: Terminal._grouped_strategy_names(
        fake, selected_strategy=selected_strategy
    )

    Terminal._populate_strategy_picker(fake, picker, selected_strategy="EMA Cross | London Session Aggressive")

    items = [picker.itemText(index) for index in range(picker.count()) if picker.itemText(index)]

    ema_index = items.index("EMA Cross")
    ema_variant_index = items.index("EMA Cross | London Session Aggressive")
    macd_index = items.index("MACD Trend")

    assert ema_index < ema_variant_index < macd_index
    assert picker.currentText() == "EMA Cross | London Session Aggressive"


def test_optimize_strategy_open_failure_is_handled_without_crashing():
    messages = []
    logs = []
    fake = SimpleNamespace(
        _show_optimization_window=lambda: (_ for _ in ()).throw(RuntimeError("boom")),
        logger=SimpleNamespace(exception=lambda *_args, **_kwargs: None),
        system_console=SimpleNamespace(log=lambda message, level="INFO": logs.append((message, level))),
        _show_async_message=lambda title, text, icon=None: messages.append((title, text)),
    )

    Terminal._optimize_strategy(fake)

    assert logs
    assert "failed to open" in logs[0][0].lower()
    assert messages == [("Strategy Optimization Failed", "boom")]


def test_optimize_strategy_only_opens_workspace_until_user_starts_run():
    events = {"opened": 0, "messages": [], "tasks": []}

    def _show_window():
        events["opened"] += 1
        return object()

    fake = SimpleNamespace(
        _show_optimization_window=_show_window,
        _refresh_optimization_window=lambda message=None, window=None: events["messages"].append(message),
        controller=SimpleNamespace(_create_task=lambda coro, name: events["tasks"].append((coro, name))),
        logger=SimpleNamespace(exception=lambda *_args, **_kwargs: None),
        system_console=SimpleNamespace(log=lambda *_args, **_kwargs: None),
        _show_async_message=lambda *_args, **_kwargs: None,
    )

    Terminal._optimize_strategy(fake)

    assert events["opened"] == 1
    assert events["tasks"] == []
    assert events["messages"] == [
        "Optimization is idle. Nothing will run until you click Run Optimization or Rank All Strategies."
    ]


def test_refresh_strategy_assignment_window_shows_default_and_custom_symbol_modes():
    _app()
    window = SimpleNamespace(
        _strategy_assignment_status=QLabel(),
        _strategy_assignment_summary=QLabel(),
        _strategy_assignment_symbol_picker=QComboBox(),
        _strategy_assignment_strategy_picker=QComboBox(),
        _strategy_assignment_timeframe_picker=QComboBox(),
        _strategy_assignment_top_n=QSpinBox(),
        _strategy_assignment_table=QTableWidget(),
    )
    controller = SimpleNamespace(
        symbols=["EUR/USD", "BTC/USDT"],
        strategy_name="Trend Following",
        max_symbol_strategies=3,
        time_frame="1h",
        symbol_strategy_assignments={
            "EUR/USD": [
                {
                    "strategy_name": "EMA Cross | London Session Aggressive",
                    "timeframe": "4h",
                    "assignment_mode": "single",
                    "weight": 1.0,
                }
            ]
        },
        symbol_strategy_rankings={
            "BTC/USDT": [
                {"strategy_name": "MACD Trend"},
                {"strategy_name": "Trend Following"},
            ]
        },
    )

    def strategy_assignment_state_for_symbol(symbol):
        normalized = str(symbol or "").strip().upper().replace("-", "/").replace("_", "/")
        explicit_rows = list(controller.symbol_strategy_assignments.get(normalized, []) or [])
        active_rows = explicit_rows or [
            {
                "strategy_name": controller.strategy_name,
                "timeframe": controller.time_frame,
                "weight": 1.0,
            }
        ]
        return {
            "symbol": normalized,
            "mode": explicit_rows[0].get("assignment_mode") if explicit_rows else "default",
            "explicit_rows": explicit_rows,
            "active_rows": active_rows,
            "ranked_rows": list(controller.symbol_strategy_rankings.get(normalized, []) or []),
        }

    controller.strategy_assignment_state_for_symbol = strategy_assignment_state_for_symbol
    fake = SimpleNamespace(
        controller=controller,
        current_timeframe="1h",
        detached_tool_windows={"strategy_assignments": window},
        _current_chart_symbol=lambda: "",
        _strategy_family_name=lambda name: Terminal._strategy_family_name(SimpleNamespace(), name),
    )
    fake._grouped_strategy_names = lambda selected_strategy=None: Terminal._grouped_strategy_names(
        fake, selected_strategy=selected_strategy
    )
    fake._populate_strategy_picker = lambda picker, selected_strategy=None: Terminal._populate_strategy_picker(
        fake, picker, selected_strategy=selected_strategy
    )

    Terminal._refresh_strategy_assignment_window(fake, window=window, message="ready")

    assert window._strategy_assignment_status.text() == "ready"
    assert window._strategy_assignment_symbol_picker.currentText() == "EUR/USD"
    assert "Assigned Strategy" in window._strategy_assignment_summary.text()
    assert window._strategy_assignment_table.rowCount() == 2
    assert window._strategy_assignment_table.item(0, 0).text() == "EUR/USD"
    assert window._strategy_assignment_table.item(0, 1).text() == "Assigned Strategy"
    assert window._strategy_assignment_table.item(1, 1).text() == "Default Strategy"
