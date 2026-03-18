import asyncio
from datetime import datetime, timezone
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pyqtgraph as pg
from PySide6.QtWidgets import QApplication, QCheckBox, QComboBox, QLabel, QLineEdit, QPushButton, QSpinBox, QTableWidget, QTextBrowser

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from frontend.ui.terminal import Terminal
from event_bus.event_types import EventType


def _app():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_history_request_limit_caps_runtime_chart_requests():
    fake = SimpleNamespace(
        controller=SimpleNamespace(
            limit=50000,
            runtime_history_limit=500,
            broker=SimpleNamespace(MAX_OHLCV_COUNT=5000),
        )
    )

    assert Terminal._history_request_limit(fake) == 500
    assert Terminal._history_request_limit(fake, fallback=240) == 240


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


def test_refresh_stellar_asset_explorer_window_populates_links_from_broker_registry():
    _app()
    window = SimpleNamespace(
        _stellar_asset_status=QLabel(),
        _stellar_asset_picker=QComboBox(),
        _stellar_asset_input=QLineEdit(),
        _stellar_asset_details=QTextBrowser(),
    )
    broker = SimpleNamespace(
        asset_registry={
            "XLM": SimpleNamespace(code="XLM", issuer=None),
            "USDC": SimpleNamespace(code="USDC", issuer="GUSDCISSUER"),
            "AQUA": SimpleNamespace(code="AQUA", issuer="GAQUAISSUER"),
        },
        _account_asset_codes=["XLM", "USDC"],
        _network_asset_codes=["USDC", "AQUA"],
    )
    fake = SimpleNamespace(controller=SimpleNamespace(broker=broker, exchange="stellar"))
    fake._stellar_expert_asset_url = lambda code, issuer=None: Terminal._stellar_expert_asset_url(fake, code, issuer)
    fake._stellar_asset_identifier = lambda code, issuer=None: Terminal._stellar_asset_identifier(fake, code, issuer)
    fake._parse_stellar_asset_entry = lambda raw: Terminal._parse_stellar_asset_entry(fake, raw)
    fake._selected_stellar_asset_row = lambda current_window=None: Terminal._selected_stellar_asset_row(fake, current_window)
    fake._stellar_asset_explorer_rows = lambda: Terminal._stellar_asset_explorer_rows(fake)

    Terminal._refresh_stellar_asset_explorer_window(fake, window=window)

    assert window._stellar_asset_picker.count() == 3
    assert "Loaded 3 Stellar assets" in window._stellar_asset_status.text()
    html = window._stellar_asset_details.toHtml()
    assert "https://stellar.expert/explorer/public/asset/XLM" in html
    assert "https://stellar.expert/explorer/public/asset/USDC-GUSDCISSUER" in html


def test_stellar_asset_explorer_rows_mark_screening_and_trustline_state():
    broker = SimpleNamespace(
        asset_registry={
            "XLM": SimpleNamespace(code="XLM", issuer=None),
            "USDC": SimpleNamespace(code="USDC", issuer="GUSDCISSUER"),
            "AQUA": SimpleNamespace(code="AQUA", issuer="GAQUAISSUER"),
            "SCAM": SimpleNamespace(code="SCAM", issuer="GSCAMISSUER"),
        },
        _account_asset_codes=["XLM", "USDC"],
        _network_asset_codes=["USDC", "AQUA"],
    )
    fake = SimpleNamespace(controller=SimpleNamespace(broker=broker, exchange="stellar"))
    fake._stellar_expert_asset_url = lambda code, issuer=None: Terminal._stellar_expert_asset_url(fake, code, issuer)
    fake._stellar_asset_identifier = lambda code, issuer=None: Terminal._stellar_asset_identifier(fake, code, issuer)

    rows = Terminal._stellar_asset_explorer_rows(fake)
    row_map = {row["id"]: row for row in rows}

    assert row_map["USDC:GUSDCISSUER"]["screened"] is True
    assert row_map["USDC:GUSDCISSUER"]["trusted"] is True
    assert row_map["AQUA:GAQUAISSUER"]["needs_trustline"] is True
    assert row_map["SCAM:GSCAMISSUER"]["screened"] is False


def test_refresh_stellar_asset_explorer_window_filters_screened_assets_for_trustlines():
    _app()
    window = SimpleNamespace(
        _stellar_asset_status=QLabel(),
        _stellar_asset_picker=QComboBox(),
        _stellar_asset_input=QLineEdit(),
        _stellar_asset_details=QTextBrowser(),
        _stellar_asset_filter_safe=QCheckBox(),
        _stellar_asset_filter_untrusted=QCheckBox(),
        _stellar_asset_trustline_btn=QPushButton(),
    )
    window._stellar_asset_filter_safe.setChecked(True)
    window._stellar_asset_filter_untrusted.setChecked(True)
    broker = SimpleNamespace(
        asset_registry={
            "XLM": SimpleNamespace(code="XLM", issuer=None),
            "USDC": SimpleNamespace(code="USDC", issuer="GUSDCISSUER"),
            "AQUA": SimpleNamespace(code="AQUA", issuer="GAQUAISSUER"),
            "SCAM": SimpleNamespace(code="SCAM", issuer="GSCAMISSUER"),
        },
        _account_asset_codes=["XLM", "USDC"],
        _network_asset_codes=["USDC", "AQUA"],
        create_trustline=lambda asset: asset,
    )
    fake = SimpleNamespace(controller=SimpleNamespace(broker=broker, exchange="stellar"))
    fake._stellar_expert_asset_url = lambda code, issuer=None: Terminal._stellar_expert_asset_url(fake, code, issuer)
    fake._stellar_asset_identifier = lambda code, issuer=None: Terminal._stellar_asset_identifier(fake, code, issuer)
    fake._parse_stellar_asset_entry = lambda raw: Terminal._parse_stellar_asset_entry(fake, raw)
    fake._selected_stellar_asset_row = lambda current_window=None: Terminal._selected_stellar_asset_row(fake, current_window)
    fake._stellar_asset_explorer_rows = lambda: Terminal._stellar_asset_explorer_rows(fake)

    Terminal._refresh_stellar_asset_explorer_window(fake, window=window)

    assert window._stellar_asset_picker.count() == 1
    assert window._stellar_asset_picker.currentData() == "AQUA:GAQUAISSUER"
    assert window._stellar_asset_trustline_btn.isEnabled() is True
    assert "Loaded 1 Stellar assets (4 total)." in window._stellar_asset_status.text()


def test_refresh_strategy_assignment_window_disables_manual_controls_until_auto_scan_finishes():
    _app()
    window = SimpleNamespace(
        _strategy_assignment_status=QLabel(),
        _strategy_assignment_summary=QLabel(),
        _strategy_assignment_symbol_picker=QComboBox(),
        _strategy_assignment_strategy_picker=QComboBox(),
        _strategy_assignment_timeframe_picker=QComboBox(),
        _strategy_assignment_top_n=QSpinBox(),
        _strategy_assignment_table=QTableWidget(),
        _strategy_assignment_use_default_btn=QPushButton(),
        _strategy_assignment_assign_single_btn=QPushButton(),
        _strategy_assignment_assign_ranked_btn=QPushButton(),
    )
    controller = SimpleNamespace(
        symbols=["EUR/USD"],
        strategy_name="Trend Following",
        max_symbol_strategies=3,
        time_frame="1h",
        symbol_strategy_assignments={},
        symbol_strategy_rankings={},
        strategy_auto_assignment_status=lambda: {
            "enabled": True,
            "ready": False,
            "running": True,
            "completed": 1,
            "total": 4,
            "current_symbol": "BTC/USDT",
            "message": "Scanning all symbols.",
            "failed_symbols": [],
        },
    )

    def strategy_assignment_state_for_symbol(symbol):
        normalized = str(symbol or "").strip().upper().replace("-", "/").replace("_", "/")
        return {
            "symbol": normalized,
            "mode": "default",
            "explicit_rows": [],
            "active_rows": [
                {
                    "strategy_name": controller.strategy_name,
                    "timeframe": controller.time_frame,
                    "weight": 1.0,
                }
            ],
            "ranked_rows": [],
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

    Terminal._refresh_strategy_assignment_window(fake, window=window)

    assert window._strategy_assignment_status.text() == "Scanning all symbols."
    assert "Auto Scan: 1/4" in window._strategy_assignment_summary.text()
    assert window._strategy_assignment_use_default_btn.isEnabled() is False
    assert window._strategy_assignment_assign_single_btn.isEnabled() is False
    assert window._strategy_assignment_assign_ranked_btn.isEnabled() is False
    assert window._strategy_assignment_strategy_picker.isEnabled() is False
    assert window._strategy_assignment_timeframe_picker.isEnabled() is False
    assert window._strategy_assignment_top_n.isEnabled() is False


def test_live_trading_bar_style_uses_valid_qt_selectors():
    style = Terminal._live_trading_bar_style(SimpleNamespace(), armed=True)

    assert "QFrame {" in style
    assert "QLabel {" in style
    assert "QProgressBar {" in style
    assert "QProgressBar::chunk {" in style
    assert "{{" not in style
    assert "selection-background-color" not in style


def test_refresh_strategy_assignment_window_populates_agent_chain_for_selected_symbol():
    _app()
    window = SimpleNamespace(
        _strategy_assignment_status=QLabel(),
        _strategy_assignment_summary=QLabel(),
        _strategy_assignment_symbol_picker=QComboBox(),
        _strategy_assignment_strategy_picker=QComboBox(),
        _strategy_assignment_timeframe_picker=QComboBox(),
        _strategy_assignment_top_n=QSpinBox(),
        _strategy_assignment_table=QTableWidget(),
        _strategy_assignment_agent_status=QLabel(),
        _strategy_assignment_agent_table=QTableWidget(),
    )
    controller = SimpleNamespace(
        symbols=["EUR/USD"],
        strategy_name="Trend Following",
        max_symbol_strategies=3,
        time_frame="1h",
        symbol_strategy_assignments={},
        symbol_strategy_rankings={
            "EUR/USD": [{"strategy_name": "EMA Cross"}]
        },
        latest_agent_decision_chain_for_symbol=lambda symbol, limit=12: [
            {
                "agent_name": "SignalAgent",
                "stage": "selected",
                "strategy_name": "EMA Cross",
                "timeframe": "4h",
                "reason": "breakout detected",
                "timestamp_label": "2026-03-17 10:00:00 UTC",
                "payload": {},
            },
            {
                "agent_name": "RiskAgent",
                "stage": "approved",
                "strategy_name": "EMA Cross",
                "timeframe": "4h",
                "reason": "within limits",
                "timestamp_label": "2026-03-17 10:00:02 UTC",
                "payload": {},
            },
        ],
        latest_agent_decision_overview_for_symbol=lambda symbol: {
            "strategy_name": "EMA Cross",
            "timeframe": "4h",
            "final_agent": "RiskAgent",
            "final_stage": "approved",
            "timestamp_label": "2026-03-17 10:00:02 UTC",
        },
    )

    def strategy_assignment_state_for_symbol(symbol):
        normalized = str(symbol or "").strip().upper().replace("-", "/").replace("_", "/")
        return {
            "symbol": normalized,
            "mode": "default",
            "explicit_rows": [],
            "active_rows": [
                {
                    "strategy_name": controller.strategy_name,
                    "timeframe": controller.time_frame,
                    "weight": 1.0,
                }
            ],
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

    Terminal._refresh_strategy_assignment_window(fake, window=window)

    assert "Agent Steps: 2" in window._strategy_assignment_summary.text()
    assert "Agent Best: EMA Cross (4h)" in window._strategy_assignment_summary.text()
    assert "Latest Agent Chain: 2 steps" in window._strategy_assignment_agent_status.text()
    assert window._strategy_assignment_agent_table.rowCount() == 2
    assert window._strategy_assignment_agent_table.item(0, 0).text() == "SignalAgent"
    assert window._strategy_assignment_agent_table.item(1, 1).text() == "approved"


def test_refresh_strategy_assignment_window_populates_adaptive_strategy_memory():
    _app()
    window = SimpleNamespace(
        _strategy_assignment_status=QLabel(),
        _strategy_assignment_summary=QLabel(),
        _strategy_assignment_symbol_picker=QComboBox(),
        _strategy_assignment_strategy_picker=QComboBox(),
        _strategy_assignment_timeframe_picker=QComboBox(),
        _strategy_assignment_top_n=QSpinBox(),
        _strategy_assignment_table=QTableWidget(),
        _strategy_assignment_adaptive_status=QLabel(),
        _strategy_assignment_adaptive_table=QTableWidget(),
        _strategy_assignment_adaptive_plot_status=QLabel(),
        _strategy_assignment_adaptive_plot=pg.PlotWidget(axisItems={"bottom": pg.DateAxisItem(orientation="bottom")}),
        _strategy_assignment_adaptive_details=QTextBrowser(),
    )
    controller = SimpleNamespace(
        symbols=["EUR/USD"],
        strategy_name="Trend Following",
        max_symbol_strategies=3,
        time_frame="1h",
        symbol_strategy_assignments={},
        symbol_strategy_rankings={
            "EUR/USD": [{"strategy_name": "EMA Cross", "timeframe": "4h"}]
        },
        adaptive_strategy_profiles_for_symbol=lambda symbol: [
            {
                "strategy_name": "EMA Cross",
                "timeframe": "4h",
                "mode": "active",
                "adaptive_weight": 1.23,
                "sample_size": 4,
                "win_rate": 0.75,
                "average_pnl": 12.5,
                "scope": "timeframe",
            },
            {
                "strategy_name": "Trend Following",
                "timeframe": "1h",
                "mode": "ranked",
                "adaptive_weight": 0.88,
                "sample_size": 3,
                "win_rate": 0.33,
                "average_pnl": -4.0,
                "scope": "strategy",
            },
        ],
        adaptive_strategy_detail_for_symbol=lambda symbol, strategy_name, timeframe=None, limit=8: {
            "scope": "timeframe",
            "profile": {
                "adaptive_weight": 1.23,
                "sample_size": 4,
                "win_rate": 0.75,
                "average_pnl": 12.5,
            },
            "samples": [
                {
                    "timestamp": datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc),
                    "side": "BUY",
                    "pnl": 15.0,
                    "score": 1.0,
                    "reason": "Breakout follow-through",
                },
                {
                    "timestamp": datetime(2026, 3, 17, 14, 0, tzinfo=timezone.utc),
                    "side": "BUY",
                    "pnl": 10.0,
                    "score": 1.0,
                    "reason": "Trend continuation",
                },
            ],
        },
        adaptive_strategy_timeline_for_symbol=lambda symbol, strategy_name, timeframe=None, limit=16: {
            "scope": "timeframe",
            "profile": {
                "adaptive_weight": 1.23,
                "sample_size": 4,
                "win_rate": 0.75,
                "average_pnl": 12.5,
            },
            "timeline": [
                {
                    "timestamp": datetime(2026, 3, 17, 14, 0, tzinfo=timezone.utc),
                    "timestamp_value": datetime(2026, 3, 17, 14, 0, tzinfo=timezone.utc).timestamp(),
                    "adaptive_weight": 1.08,
                    "score": 1.0,
                    "pnl": 10.0,
                    "reason": "Trend continuation",
                    "side": "BUY",
                    "sample_index": 1,
                },
                {
                    "timestamp": datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc),
                    "timestamp_value": datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc).timestamp(),
                    "adaptive_weight": 1.23,
                    "score": 1.0,
                    "pnl": 15.0,
                    "reason": "Breakout follow-through",
                    "side": "BUY",
                    "sample_index": 2,
                },
            ],
        },
    )

    def strategy_assignment_state_for_symbol(symbol):
        normalized = str(symbol or "").strip().upper().replace("-", "/").replace("_", "/")
        return {
            "symbol": normalized,
            "mode": "default",
            "explicit_rows": [],
            "active_rows": [
                {
                    "strategy_name": controller.strategy_name,
                    "timeframe": controller.time_frame,
                    "weight": 1.0,
                }
            ],
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

    Terminal._refresh_strategy_assignment_window(fake, window=window)

    assert "Adaptive Leader: EMA Cross x1.23" in window._strategy_assignment_summary.text()
    assert "Adaptive Memory: 2 strategy profiles" in window._strategy_assignment_adaptive_status.text()
    assert window._strategy_assignment_adaptive_table.rowCount() == 2
    assert window._strategy_assignment_adaptive_table.item(0, 0).text() == "EMA Cross"
    assert window._strategy_assignment_adaptive_table.item(0, 3).text() == "1.23"
    assert window._strategy_assignment_adaptive_table.item(0, 5).text() == "75%"
    assert window._strategy_assignment_adaptive_table.item(1, 7).text() == "strategy"
    assert "Adaptive history: 2 scored trades" in window._strategy_assignment_adaptive_plot_status.text()
    assert len(window._strategy_assignment_adaptive_plot.listDataItems()) >= 1
    Terminal._refresh_strategy_assignment_adaptive_details(
        fake,
        window=window,
        selected_symbol="EUR/USD",
        strategy_name="EMA Cross",
        timeframe="4h",
    )
    detail_html = window._strategy_assignment_adaptive_details.toHtml()
    assert "Adaptive detail" in detail_html
    assert "EMA Cross" in detail_html
    assert "Breakout follow-through" in detail_html
    assert "Current 1.23" in window._strategy_assignment_adaptive_plot_status.text()


def test_handle_agent_runtime_event_refreshes_selected_strategy_assignment_and_pushes_risk_notification():
    _app()
    window = SimpleNamespace(_strategy_assignment_selected_symbol="EUR/USD")
    timeline_window = object()
    refreshed = []
    timeline_refreshes = []
    notifications = []
    logs = []
    fake = SimpleNamespace(
        _ui_shutting_down=False,
        detached_tool_windows={"strategy_assignments": window, "agent_timeline": timeline_window},
        _is_qt_object_alive=lambda obj: obj is not None,
        _refresh_agent_timeline_window=lambda window=None: timeline_refreshes.append(window),
        _refresh_strategy_assignment_window=lambda window=None, message=None: refreshed.append((window, message)),
        _push_notification=lambda *args, **kwargs: notifications.append((args, kwargs)),
        system_console=SimpleNamespace(log=lambda message, level="INFO": logs.append((message, level))),
    )

    Terminal._handle_agent_runtime_event(
        fake,
        {
            "kind": "memory",
            "symbol": "EUR/USD",
            "agent_name": "RiskAgent",
            "stage": "approved",
            "reason": "within limits",
        },
    )
    Terminal._handle_agent_runtime_event(
        fake,
        {
            "kind": "bus",
            "event_type": EventType.RISK_ALERT,
            "symbol": "EUR/USD",
            "reason": "Risk blocked the trade.",
            "message": "Risk blocked the trade.",
        },
    )

    assert refreshed[0][0] is window
    assert "Live agent update" in refreshed[0][1]
    assert timeline_refreshes == [timeline_window, timeline_window]
    assert notifications[0][0][0] == "Agent risk blocked"
    assert logs[0][1] == "WARN"
