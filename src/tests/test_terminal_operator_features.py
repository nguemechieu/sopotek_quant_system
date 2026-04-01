import os
import sys
import time
import asyncio
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QComboBox, QDockWidget, QMainWindow, QTableWidget

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from frontend.ui.chart.chart_widget import ChartWidget
from frontend.ui.terminal import Terminal


class _SettingsRecorder:
    def __init__(self):
        self.values = {}

    def value(self, key, default=None):
        return self.values.get(key, default)

    def setValue(self, key, value):
        self.values[key] = value


class _MenuTerminal(QMainWindow):
    def __init__(self):
        super().__init__()
        self.controller = SimpleNamespace(language_code="en", set_language=lambda _code: None, symbols=[])
        self.show_bid_ask_lines = True
        self.current_connection_status = "connecting"
        self.language_actions = {}
        self.timeframe_buttons = {}
        self.autotrading_enabled = False
        self.connection_indicator = None
        self.symbol_label = None
        self.open_symbol_button = None
        self.screenshot_button = None
        self.system_status_button = None
        self.kill_switch_button = None
        self.session_mode_badge = None
        self.license_badge = None
        self.trading_activity_label = None
        self.favorite_symbols = set()
        self.detached_tool_windows = {}

    def _tr(self, key, **kwargs):
        return key

    def apply_language(self):
        return Terminal.apply_language(self)

    def _sync_chart_timeframe_menu_actions(self):
        return Terminal._sync_chart_timeframe_menu_actions(self)

    def _update_autotrade_button(self):
        return None

    def _set_active_timeframe_button(self, _timeframe):
        return None

    def _current_chart_symbol(self):
        return "BTC/USDT"

    def __getattr__(self, name):
        if name.startswith("_"):
            return lambda *args, **kwargs: None
        raise AttributeError(name)


class _ChartRequestController:
    def __init__(self, frame):
        self.frame = frame
        self.candle_buffers = {}
        self.news_draw_on_chart = False

    async def request_candle_data(self, symbol, timeframe="1h", limit=None):
        return self.frame


class _DerivativeChartRequestController(_ChartRequestController):
    def __init__(self, frame):
        super().__init__(frame)
        self.requested_symbols = []

    @staticmethod
    def _resolve_preferred_market_symbol(symbol, preference=None):
        normalized = str(symbol or "").strip().upper()
        if normalized == "BTC/USD":
            return "BTC/USD:USD"
        return normalized

    async def request_candle_data(self, symbol, timeframe="1h", limit=None):
        self.requested_symbols.append(symbol)
        return self.frame


class _ChartRequestTerminal(QMainWindow):
    def __init__(self, frame):
        super().__init__()
        self.controller = _ChartRequestController(frame)
        self._ui_shutting_down = False
        self.logger = SimpleNamespace(error=lambda *args, **kwargs: None)
        self.system_console = SimpleNamespace(log=lambda *args, **kwargs: None)
        self.heartbeat = SimpleNamespace(setStyleSheet=lambda *args, **kwargs: None)
        self._chart_request_tokens = {}
        self.current_timeframe = "1h"
        self._last_chart_request_key = None
        self._active_chart_widget_ref = None
        self.symbol_picker = None
        self.chart = ChartWidget("AAVE/USD", "1h", self.controller)

    def _history_request_limit(self):
        return 240

    def _is_qt_object_alive(self, obj):
        return obj is not None

    def _iter_chart_widgets(self):
        return [self.chart]


def _app():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _bind(fake, *names):
    for name in names:
        method = getattr(Terminal, name)
        setattr(fake, name, lambda *args, _method=method, **kwargs: _method(fake, *args, **kwargs))


def test_create_menu_bar_adds_workspace_notifications_palette_and_favorite_actions():
    _app()
    terminal = _MenuTerminal()

    Terminal._create_menu_bar(terminal)

    menu_titles = [action.text() for action in terminal.menuBar().actions()]

    workspace_actions = terminal.workspace_menu.actions()
    panels_actions = terminal.panels_menu.actions()
    strategy_actions = terminal.strategy_menu.actions()
    backtest_actions = terminal.backtest_menu.actions()
    assert terminal.action_workspace_trading in workspace_actions
    assert terminal.action_workspace_research in workspace_actions
    assert terminal.action_workspace_risk in workspace_actions
    assert terminal.action_workspace_review in workspace_actions
    assert terminal.action_symbol_universe in workspace_actions
    assert terminal.action_save_workspace_layout in workspace_actions
    assert terminal.action_restore_workspace_layout in workspace_actions
    assert terminal.action_reset_dock_layout in workspace_actions
    assert terminal.panels_menu.menuAction() in workspace_actions
    assert terminal.action_symbol_universe in terminal.tools_menu.actions()
    assert terminal.action_market_watch_panel in panels_actions
    assert terminal.action_system_console_panel in panels_actions
    assert terminal.backtest_menu.menuAction() in strategy_actions
    assert terminal.action_strategy_optimization in backtest_actions
    assert terminal.action_strategy_assigner in strategy_actions
    assert terminal.action_strategy_scorecard in strategy_actions
    assert terminal.action_strategy_debug in strategy_actions
    assert terminal.action_notifications in terminal.review_menu.actions()
    assert terminal.action_notifications in terminal.tools_menu.actions()
    assert terminal.action_agent_timeline in terminal.review_menu.actions()
    assert terminal.action_agent_timeline in terminal.research_menu.actions()
    assert terminal.action_agent_timeline in terminal.tools_menu.actions()
    assert terminal.action_trader_tv in terminal.education_menu.actions()
    assert terminal.action_education_center in terminal.education_menu.actions()
    assert terminal.action_command_palette in terminal.tools_menu.actions()
    assert terminal.action_system_console in terminal.tools_menu.actions()
    assert terminal.action_system_status in terminal.tools_menu.actions()
    assert terminal.action_favorite_symbol in terminal.charts_menu.actions()
    assert terminal.chart_studies_menu.menuAction() in terminal.charts_menu.actions()
    assert terminal.action_remove_indicator in terminal.chart_studies_menu.actions()
    assert terminal.action_chart_settings in terminal.chart_style_menu.actions()
    assert terminal.chart_timeframe_menu.menuAction() in terminal.charts_menu.actions()
    assert menu_titles[-1] == terminal.help_menu.title()
    assert menu_titles[-2] == terminal.workspace_menu.title()


def test_push_notification_dedupes_repeated_messages():
    fake = SimpleNamespace(
        _notification_records=[],
        _notification_dedupe_cache={},
        _runtime_notification_state={},
        detached_tool_windows={},
        action_notifications=None,
        _is_qt_object_alive=lambda _obj: False,
    )
    _bind(fake, "_ensure_notification_state", "_refresh_notification_action_text", "_push_notification")

    Terminal._push_notification(fake, "API disconnected", "Broker API is unavailable.", level="ERROR", source="broker", dedupe_seconds=60.0)
    Terminal._push_notification(fake, "API disconnected", "Broker API is unavailable.", level="ERROR", source="broker", dedupe_seconds=60.0)

    assert len(fake._notification_records) == 1
    assert fake._notification_records[0]["title"] == "API disconnected"


def test_manual_trade_default_payload_uses_saved_template_values():
    fake = SimpleNamespace(
        controller=SimpleNamespace(symbols=["EUR/USD"]),
        symbol="EUR/USD",
        current_timeframe="1h",
        _current_chart_symbol=lambda: "EUR/USD",
        _load_manual_trade_template=lambda: {
            "order_type": "stop_limit",
            "quantity_mode": "lots",
            "amount": 0.5,
            "stop_price": 1.102,
        },
        _safe_float=lambda value, default=None: Terminal._safe_float(SimpleNamespace(), value, default),
        _manual_trade_quantity_context=lambda symbol: {
            "symbol": symbol,
            "supports_lots": True,
            "default_mode": "lots",
            "lot_units": 100000.0,
        },
        _normalize_manual_trade_quantity_mode=lambda value: value,
    )

    payload = Terminal._manual_trade_default_payload(fake, {"symbol": "EUR/USD"})

    assert payload["order_type"] == "stop_limit"
    assert payload["quantity_mode"] == "lots"
    assert payload["amount"] == 0.5
    assert payload["stop_price"] == 1.102


def test_apply_workspace_preset_toggles_docks_and_opens_matching_tools():
    _app()
    fake = QMainWindow()
    fake.settings = _SettingsRecorder()
    fake.favorite_symbols = set()
    fake.detached_tool_windows = {}
    fake.system_console = SimpleNamespace(log=lambda *args, **kwargs: None)
    fake._is_qt_object_alive = lambda obj: obj is not None
    fake._queue_terminal_layout_fit = lambda: None
    fake._save_workspace_layout = lambda slot="last": True
    fake._push_notification = lambda *args, **kwargs: None
    opened = []
    fake._open_tool_window_by_key = lambda key: opened.append(key)
    for attr_name in (
        "market_watch_dock",
        "positions_dock",
        "trade_log_dock",
        "orderbook_dock",
        "risk_heatmap_dock",
        "system_status_dock",
        "system_console_dock",
    ):
        dock = QDockWidget(attr_name, fake)
        dock.show()
        setattr(fake, attr_name, dock)

    Terminal._apply_workspace_preset(fake, "risk")

    assert fake.market_watch_dock.isHidden()
    assert not fake.positions_dock.isHidden()
    assert not fake.orderbook_dock.isHidden()
    assert not fake.risk_heatmap_dock.isHidden()
    assert fake.system_status_dock.isHidden()
    assert fake.system_console_dock.isHidden()
    assert opened == ["portfolio_exposure", "position_analysis"]


def test_command_palette_entries_include_operator_actions():
    fake = SimpleNamespace(
        controller=SimpleNamespace(symbols=[]),
        _open_manual_trade=lambda *args, **kwargs: None,
        _open_notification_center=lambda: None,
        _open_symbol_universe=lambda: None,
        _open_agent_timeline=lambda: None,
        _open_performance=lambda: None,
        _show_portfolio_exposure=lambda: None,
        _open_position_analysis_window=lambda: None,
        _open_trade_checklist_window=lambda: None,
        _open_trade_journal_review_window=lambda: None,
        _open_recommendations_window=lambda: None,
        _open_market_chat_window=lambda: None,
        _open_quant_pm_window=lambda: None,
        _open_strategy_assignment_window=lambda: None,
        _optimize_strategy=lambda: None,
        _show_backtest_window=lambda: None,
        _export_diagnostics_bundle=lambda: None,
        _apply_workspace_preset=lambda _name: None,
        _save_current_workspace_layout=lambda: None,
        _restore_saved_workspace_layout=lambda: None,
        _apply_default_dock_layout=lambda: None,
        _show_workspace_dock=lambda _dock: None,
        market_watch_dock=object(),
        _toggle_current_symbol_favorite=lambda: None,
        _refresh_markets=lambda: None,
        _refresh_active_chart_data=lambda: None,
        _refresh_active_orderbook=lambda: None,
        _reload_balance=lambda: None,
    )

    entries = Terminal._command_palette_entries(fake, "")
    titles = {entry["title"] for entry in entries}

    assert "Trading Workspace" in titles
    assert "Research Workspace" in titles
    assert "Risk Workspace" in titles
    assert "Review Workspace" in titles
    assert "Symbol Universe" in titles
    assert "Export Diagnostics Bundle" in titles
    assert "Reset Dock Layout" in titles
    assert "Show Market Watch" in titles


def test_open_symbol_universe_window_shows_controller_tiers():
    _app()
    terminal = _MenuTerminal()
    terminal.controller = SimpleNamespace(
        language_code="en",
        set_language=lambda _code: None,
        symbols=["BTC/USD", "ETH/USD", "SOL/USD"],
        get_symbol_universe_snapshot=lambda: {
            "active": ["BTC/USD", "ETH/USD"],
            "watchlist": ["BTC/USD", "SOL/USD", "ETH/USD"],
            "catalog": ["BTC/USD", "ETH/USD", "SOL/USD", "XRP/USD"],
            "background_catalog": ["SOL/USD", "XRP/USD"],
            "last_batch": ["BTC/USD", "SOL/USD"],
            "rotation_cursor": 2,
            "policy": {
                "live_symbol_limit": 4,
                "watchlist_limit": 8,
                "discovery_batch_size": 3,
            },
        },
    )
    terminal._is_qt_object_alive = lambda obj: obj is not None
    _bind(
        terminal,
        "_get_or_create_tool_window",
        "_symbol_universe_snapshot",
        "_refresh_symbol_universe_window",
        "_open_symbol_universe",
    )

    window = terminal._open_symbol_universe()

    assert window is not None
    assert "Active 2/4" in window._symbol_universe_summary.text()
    assert "Catalog 4" in window._symbol_universe_summary.text()
    tree = window._symbol_universe_tree
    top_labels = [tree.topLevelItem(index).text(0) for index in range(tree.topLevelItemCount())]
    assert "Active (2)" in top_labels
    assert "Watchlist (3)" in top_labels
    assert "Discovery Batch (2)" in top_labels
    active_item = tree.topLevelItem(0)
    assert active_item.childCount() >= 2
    child_symbols = {active_item.child(i).text(0) for i in range(active_item.childCount())}
    assert {"BTC/USD", "ETH/USD"}.issubset(child_symbols)


def test_market_watch_panel_action_shows_hidden_market_watch_dock():
    _app()
    terminal = _MenuTerminal()
    Terminal._create_menu_bar(terminal)
    _bind(terminal, "_show_workspace_dock")
    terminal._is_qt_object_alive = lambda obj: obj is not None
    terminal.show()
    terminal.market_watch_dock = QDockWidget("Market Watch", terminal)
    terminal.market_watch_dock.setObjectName("market_watch_dock")
    terminal.addDockWidget(Qt.LeftDockWidgetArea, terminal.market_watch_dock)
    terminal.trade_log_dock = QDockWidget("Trade Log", terminal)
    terminal.trade_log_dock.setObjectName("trade_log_dock")
    terminal.addDockWidget(Qt.RightDockWidgetArea, terminal.trade_log_dock)
    terminal.market_watch_dock.setFloating(True)
    terminal.market_watch_dock.hide()

    terminal.action_market_watch_panel.trigger()

    assert not terminal.market_watch_dock.isHidden()
    assert terminal.market_watch_dock.isFloating() is False
    assert terminal.dockWidgetArea(terminal.market_watch_dock) == Qt.LeftDockWidgetArea


def test_open_agent_timeline_builds_runtime_table_from_controller_feed():
    _app()
    terminal = _MenuTerminal()
    now = time.time()
    terminal.controller = SimpleNamespace(
        language_code="en",
        set_language=lambda _code: None,
        symbols=[],
        live_agent_runtime_feed=lambda limit=200, symbol=None, kinds=None: [
            {
                "timestamp_label": "2026-03-17 10:05:00 UTC",
                "timestamp": now - 10,
                "kind": "memory",
                "symbol": "EUR/USD",
                "agent_name": "SignalAgent",
                "stage": "selected",
                "strategy_name": "EMA Cross",
                "timeframe": "4h",
                "message": "Signal selected for EUR/USD.",
                "payload": {"confidence": 0.82},
            },
            {
                "timestamp_label": "2026-03-17 10:06:00 UTC",
                "timestamp": now - 5,
                "kind": "bus",
                "symbol": "EUR/USD",
                "event_type": "risk_approved",
                "stage": "",
                "strategy_name": "EMA Cross",
                "timeframe": "4h",
                "message": "Risk approved BUY for EUR/USD.",
                "payload": {"approved": True},
            },
        ],
        strategy_assignment_state_for_symbol=lambda symbol: {
            "mode": "single",
            "active_rows": [{"strategy_name": "Trend Following", "timeframe": "1h"}],
            "locked": True,
        },
        latest_agent_decision_overview_for_symbol=lambda symbol: {
            "strategy_name": "EMA Cross",
            "timeframe": "4h",
            "side": "buy",
            "approved": True,
            "final_agent": "RiskAgent",
            "final_stage": "approved",
            "reason": "within limits",
        },
    )
    _bind(
        terminal,
        "_get_or_create_tool_window",
        "_is_qt_object_alive",
        "_selected_agent_timeline_symbol",
        "_selected_agent_timeline_row",
        "_agent_timeline_row_status_label",
        "_agent_timeline_assignment_text",
        "_agent_timeline_recommendation_text",
        "_agent_timeline_health_snapshot",
        "_refresh_agent_timeline_health",
        "_agent_timeline_anomaly_snapshot",
        "_agent_timeline_anomaly_fingerprint",
        "_visible_agent_timeline_anomaly_snapshot",
        "_refresh_agent_timeline_anomalies",
        "_refresh_agent_timeline_window",
        "_refresh_agent_timeline_details",
        "_replay_selected_agent_timeline_symbol",
        "_open_selected_agent_timeline_symbol_in_strategy_assigner",
        "_refresh_selected_agent_timeline_symbol",
        "_acknowledge_selected_agent_timeline_anomaly",
        "_open_agent_timeline",
    )

    window = Terminal._open_agent_timeline(terminal)
    tree = window._agent_timeline_tree

    assert tree.topLevelItemCount() == 1
    group = tree.topLevelItem(0)
    assert group.text(2) == "EUR/USD"
    assert group.text(3) == "2 events"
    assert group.childCount() == 2
    assert group.child(0).text(3) == "SignalAgent"
    assert group.child(1).text(3) == "risk_approved"
    assert "Current Assignment" in window._agent_timeline_assignment_label.text()
    assert "Strategy: Trend Following" in window._agent_timeline_assignment_label.text()
    assert "Latest Agent Recommendation" in window._agent_timeline_recommendation_label.text()
    assert "Strategy: EMA Cross" in window._agent_timeline_recommendation_label.text()
    assert "Approved: 1" in window._agent_timeline_health_counts.text()
    assert "Execution: 0" in window._agent_timeline_health_counts.text()
    assert "Count: 1" in window._agent_timeline_health_symbols.text()
    assert "Changes: 2" in window._agent_timeline_health_recent.text()
    group.setExpanded(True)
    tree.setCurrentItem(group.child(0))
    Terminal._refresh_agent_timeline_details(terminal, window)
    assert "Agent/Event: SignalAgent" in window._agent_timeline_detail_browser.toPlainText()
    assert '"confidence": 0.82' in window._agent_timeline_detail_browser.toPlainText()


def test_replay_selected_agent_timeline_symbol_opens_strategy_assigner_for_selected_symbol():
    _app()
    terminal = _MenuTerminal()
    terminal.controller = SimpleNamespace(
        language_code="en",
        set_language=lambda _code: None,
        symbols=[],
        live_agent_runtime_feed=lambda limit=200, symbol=None, kinds=None: [
            {
                "timestamp_label": "2026-03-17 10:05:00 UTC",
                "kind": "memory",
                "symbol": "GBP/USD",
                "agent_name": "SignalAgent",
                "stage": "selected",
                "strategy_name": "Trend Following",
                "timeframe": "1h",
                "message": "Signal selected for GBP/USD.",
            }
        ],
    )
    replay_messages = []
    strategy_window = SimpleNamespace(_strategy_assignment_symbol_picker=QComboBox())
    terminal._open_strategy_assignment_window = lambda: strategy_window
    terminal._refresh_strategy_assignment_window = lambda window=None, message=None: replay_messages.append((window, message))
    _bind(
        terminal,
        "_get_or_create_tool_window",
        "_is_qt_object_alive",
        "_selected_agent_timeline_symbol",
        "_selected_agent_timeline_row",
        "_agent_timeline_row_status_label",
        "_agent_timeline_assignment_text",
        "_agent_timeline_recommendation_text",
        "_agent_timeline_health_snapshot",
        "_refresh_agent_timeline_health",
        "_agent_timeline_anomaly_snapshot",
        "_agent_timeline_anomaly_fingerprint",
        "_visible_agent_timeline_anomaly_snapshot",
        "_refresh_agent_timeline_anomalies",
        "_refresh_agent_timeline_window",
        "_refresh_agent_timeline_details",
        "_replay_selected_agent_timeline_symbol",
        "_open_selected_agent_timeline_symbol_in_strategy_assigner",
        "_refresh_selected_agent_timeline_symbol",
        "_acknowledge_selected_agent_timeline_anomaly",
        "_open_agent_timeline",
    )

    window = Terminal._open_agent_timeline(terminal)
    window._agent_timeline_tree.setCurrentItem(window._agent_timeline_tree.topLevelItem(0).child(0))
    opened = Terminal._replay_selected_agent_timeline_symbol(terminal, window)

    assert opened is strategy_window
    assert strategy_window._strategy_assignment_selected_symbol == "GBP/USD"
    assert strategy_window._strategy_assignment_symbol_picker.currentText() == "GBP/USD"
    assert replay_messages[0][0] is strategy_window
    assert "Replaying the latest agent chain for GBP/USD." == replay_messages[0][1]


def test_agent_timeline_filters_and_pin_symbol_scope_rows():
    _app()
    terminal = _MenuTerminal()
    now = time.time()
    terminal.controller = SimpleNamespace(
        language_code="en",
        set_language=lambda _code: None,
        symbols=[],
        live_agent_runtime_feed=lambda limit=200, symbol=None, kinds=None: [
            {
                "timestamp_label": "2026-03-17 10:06:00 UTC",
                "timestamp": now - 10,
                "kind": "bus",
                "symbol": "EUR/USD",
                "event_type": "risk_approved",
                "approved": True,
                "strategy_name": "EMA Cross",
                "timeframe": "4h",
                "message": "Risk approved BUY for EUR/USD.",
                "payload": {"approved": True},
            },
            {
                "timestamp_label": "2026-03-17 10:05:00 UTC",
                "timestamp": now - 15,
                "kind": "memory",
                "symbol": "EUR/USD",
                "agent_name": "SignalAgent",
                "stage": "selected",
                "strategy_name": "EMA Cross",
                "timeframe": "4h",
                "message": "Signal selected for EUR/USD.",
                "payload": {"confidence": 0.82},
            },
            {
                "timestamp_label": "2026-03-17 10:04:00 UTC",
                "timestamp": now - 120,
                "kind": "bus",
                "symbol": "GBP/USD",
                "event_type": "risk_alert",
                "approved": False,
                "strategy_name": "Trend Following",
                "timeframe": "1h",
                "message": "Risk blocked GBP/USD.",
                "payload": {"approved": False},
            },
        ],
        strategy_assignment_state_for_symbol=lambda symbol: {
            "mode": "single",
            "active_rows": [{"strategy_name": "Trend Following", "timeframe": "1h"}],
            "locked": False,
        },
        latest_agent_decision_overview_for_symbol=lambda symbol: {
            "strategy_name": "EMA Cross" if symbol == "EUR/USD" else "Trend Following",
            "timeframe": "4h" if symbol == "EUR/USD" else "1h",
            "side": "buy",
            "approved": symbol == "EUR/USD",
            "final_agent": "RiskAgent",
            "final_stage": "approved" if symbol == "EUR/USD" else "rejected",
            "reason": "within limits" if symbol == "EUR/USD" else "risk blocked",
        },
    )
    _bind(
        terminal,
        "_get_or_create_tool_window",
        "_is_qt_object_alive",
        "_selected_agent_timeline_symbol",
        "_selected_agent_timeline_row",
        "_agent_timeline_row_status_label",
        "_populate_agent_timeline_filters",
        "_toggle_agent_timeline_pin_symbol",
        "_agent_timeline_health_snapshot",
        "_refresh_agent_timeline_health",
        "_agent_timeline_anomaly_snapshot",
        "_agent_timeline_anomaly_fingerprint",
        "_visible_agent_timeline_anomaly_snapshot",
        "_refresh_agent_timeline_anomalies",
        "_agent_timeline_assignment_text",
        "_agent_timeline_recommendation_text",
        "_refresh_agent_timeline_window",
        "_refresh_agent_timeline_details",
        "_replay_selected_agent_timeline_symbol",
        "_open_selected_agent_timeline_symbol_in_strategy_assigner",
        "_refresh_selected_agent_timeline_symbol",
        "_acknowledge_selected_agent_timeline_anomaly",
        "_open_agent_timeline",
    )

    window = Terminal._open_agent_timeline(terminal)
    tree = window._agent_timeline_tree

    assert tree.topLevelItemCount() == 2
    assert window._agent_timeline_status_filter.findText("Approved") >= 0
    assert window._agent_timeline_status_filter.findText("Rejected") >= 0
    assert window._agent_timeline_timeframe_filter.findText("4h") >= 0
    assert window._agent_timeline_strategy_filter.findText("EMA Cross") >= 0
    assert "Approved: 1" in window._agent_timeline_health_counts.text()
    assert "Rejected: 1" in window._agent_timeline_health_counts.text()
    assert "Changes: 2" in window._agent_timeline_health_recent.text()

    window._agent_timeline_status_filter.setCurrentText("Approved")
    Terminal._refresh_agent_timeline_window(terminal, window)
    assert tree.topLevelItemCount() == 1
    assert tree.topLevelItem(0).text(2) == "EUR/USD"

    window._agent_timeline_status_filter.setCurrentIndex(0)
    window._agent_timeline_timeframe_filter.setCurrentText("1h")
    Terminal._refresh_agent_timeline_window(terminal, window)
    assert tree.topLevelItemCount() == 1
    assert tree.topLevelItem(0).text(2) == "GBP/USD"

    window._agent_timeline_timeframe_filter.setCurrentIndex(0)
    window._agent_timeline_strategy_filter.setCurrentText("EMA Cross")
    Terminal._refresh_agent_timeline_window(terminal, window)
    assert tree.topLevelItemCount() == 1
    assert tree.topLevelItem(0).text(2) == "EUR/USD"

    window._agent_timeline_strategy_filter.setCurrentIndex(0)
    eur_group = tree.topLevelItem(0)
    tree.setCurrentItem(eur_group)
    pinned = Terminal._toggle_agent_timeline_pin_symbol(terminal, window)
    assert pinned == "EUR/USD"
    assert window._agent_timeline_pin_btn.text() == "Unpin EUR/USD"
    assert "Pinned EUR/USD" in window._agent_timeline_summary.text()
    assert "Count: 1" in window._agent_timeline_health_symbols.text()

    unpinned = Terminal._toggle_agent_timeline_pin_symbol(terminal, window)
    assert unpinned == ""
    assert window._agent_timeline_pin_btn.text() == "Pin Selected Symbol"


def test_agent_timeline_anomaly_summary_flags_rejections_stale_and_unfilled_execution():
    _app()
    terminal = _MenuTerminal()
    now = time.time()
    terminal.controller = SimpleNamespace(
        language_code="en",
        set_language=lambda _code: None,
        symbols=[],
        live_agent_runtime_feed=lambda limit=200, symbol=None, kinds=None: [
            {
                "timestamp_label": "2026-03-17 10:06:00 UTC",
                "timestamp": now - 15,
                "kind": "bus",
                "symbol": "EUR/USD",
                "event_type": "risk_alert",
                "approved": False,
                "message": "Risk blocked EUR/USD.",
            },
            {
                "timestamp_label": "2026-03-17 10:05:00 UTC",
                "timestamp": now - 25,
                "kind": "bus",
                "symbol": "EUR/USD",
                "event_type": "risk_alert",
                "approved": False,
                "message": "Risk blocked EUR/USD again.",
            },
            {
                "timestamp_label": "2026-03-17 09:55:00 UTC",
                "timestamp": now - 600,
                "kind": "memory",
                "symbol": "GBP/USD",
                "agent_name": "SignalAgent",
                "stage": "selected",
                "strategy_name": "Trend Following",
                "timeframe": "1h",
                "message": "Signal selected for GBP/USD.",
            },
            {
                "timestamp_label": "2026-03-17 10:04:30 UTC",
                "timestamp": now - 30,
                "kind": "bus",
                "symbol": "USD/JPY",
                "event_type": "execution_plan",
                "decision_id": "dec-42",
                "strategy_name": "EMA Cross",
                "timeframe": "15m",
                "message": "Execution plan ready for USD/JPY.",
            },
        ],
        strategy_assignment_state_for_symbol=lambda symbol: {"mode": "default", "active_rows": [], "locked": False},
        latest_agent_decision_overview_for_symbol=lambda symbol: {},
    )
    _bind(
        terminal,
        "_get_or_create_tool_window",
        "_is_qt_object_alive",
        "_selected_agent_timeline_symbol",
        "_selected_agent_timeline_row",
        "_agent_timeline_row_status_label",
        "_populate_agent_timeline_filters",
        "_toggle_agent_timeline_pin_symbol",
        "_agent_timeline_health_snapshot",
        "_refresh_agent_timeline_health",
        "_agent_timeline_anomaly_snapshot",
        "_agent_timeline_anomaly_fingerprint",
        "_visible_agent_timeline_anomaly_snapshot",
        "_refresh_agent_timeline_anomalies",
        "_agent_timeline_assignment_text",
        "_agent_timeline_recommendation_text",
        "_refresh_agent_timeline_window",
        "_refresh_agent_timeline_details",
        "_replay_selected_agent_timeline_symbol",
        "_open_selected_agent_timeline_symbol_in_strategy_assigner",
        "_refresh_selected_agent_timeline_symbol",
        "_acknowledge_selected_agent_timeline_anomaly",
        "_open_agent_timeline",
    )

    window = Terminal._open_agent_timeline(terminal)

    anomaly_text = window._agent_timeline_anomaly_label.text()
    assert "3 symbols flagged" in anomaly_text
    assert "EUR/USD: Repeated risk rejections (2)" in anomaly_text
    assert "GBP/USD: Stale decision flow" in anomaly_text
    assert "USD/JPY: Execution plan without fill" in anomaly_text

    tree = window._agent_timeline_tree
    group = tree.topLevelItem(0)
    tree.setCurrentItem(group)
    Terminal._refresh_agent_timeline_details(terminal, window)
    if group.text(2) == "EUR/USD":
        assert "Anomalies: Repeated risk rejections (2)" in window._agent_timeline_detail_browser.toPlainText()


def test_acknowledge_selected_agent_timeline_anomaly_hides_current_flagged_symbol():
    _app()
    terminal = _MenuTerminal()
    now = time.time()
    terminal.controller = SimpleNamespace(
        language_code="en",
        set_language=lambda _code: None,
        symbols=[],
        live_agent_runtime_feed=lambda limit=200, symbol=None, kinds=None: [
            {
                "timestamp_label": "2026-03-17 10:06:00 UTC",
                "timestamp": now - 15,
                "kind": "bus",
                "symbol": "EUR/USD",
                "event_type": "risk_alert",
                "approved": False,
                "message": "Risk blocked EUR/USD.",
            },
            {
                "timestamp_label": "2026-03-17 10:05:00 UTC",
                "timestamp": now - 25,
                "kind": "bus",
                "symbol": "EUR/USD",
                "event_type": "risk_alert",
                "approved": False,
                "message": "Risk blocked EUR/USD again.",
            },
        ],
        strategy_assignment_state_for_symbol=lambda symbol: {"mode": "default", "active_rows": [], "locked": False},
        latest_agent_decision_overview_for_symbol=lambda symbol: {},
    )
    _bind(
        terminal,
        "_get_or_create_tool_window",
        "_is_qt_object_alive",
        "_selected_agent_timeline_symbol",
        "_selected_agent_timeline_row",
        "_agent_timeline_row_status_label",
        "_populate_agent_timeline_filters",
        "_toggle_agent_timeline_pin_symbol",
        "_agent_timeline_health_snapshot",
        "_refresh_agent_timeline_health",
        "_agent_timeline_anomaly_snapshot",
        "_agent_timeline_anomaly_fingerprint",
        "_visible_agent_timeline_anomaly_snapshot",
        "_refresh_agent_timeline_anomalies",
        "_agent_timeline_assignment_text",
        "_agent_timeline_recommendation_text",
        "_refresh_agent_timeline_window",
        "_refresh_agent_timeline_details",
        "_replay_selected_agent_timeline_symbol",
        "_open_selected_agent_timeline_symbol_in_strategy_assigner",
        "_refresh_selected_agent_timeline_symbol",
        "_acknowledge_selected_agent_timeline_anomaly",
        "_open_agent_timeline",
    )

    window = Terminal._open_agent_timeline(terminal)
    tree = window._agent_timeline_tree
    tree.setCurrentItem(tree.topLevelItem(0))

    acknowledged = Terminal._acknowledge_selected_agent_timeline_anomaly(terminal, window)

    assert acknowledged == "EUR/USD"
    assert window._agent_timeline_anomaly_snapshot["count"] == 0
    assert window._agent_timeline_anomaly_label.text() == "Agent Anomalies\nAll current anomalies are acknowledged."


def test_refresh_selected_agent_timeline_symbol_opens_chart_and_requests_refresh():
    _app()
    terminal = _MenuTerminal()
    now = time.time()
    calls = []
    terminal.controller = SimpleNamespace(
        language_code="en",
        set_language=lambda _code: None,
        symbols=[],
        live_agent_runtime_feed=lambda limit=200, symbol=None, kinds=None: [
            {
                "timestamp_label": "2026-03-17 10:05:00 UTC",
                "timestamp": now - 10,
                "kind": "memory",
                "symbol": "USD/JPY",
                "agent_name": "SignalAgent",
                "stage": "selected",
                "strategy_name": "EMA Cross",
                "timeframe": "15m",
                "message": "Signal selected for USD/JPY.",
            }
        ],
        strategy_assignment_state_for_symbol=lambda symbol: {"mode": "default", "active_rows": [], "locked": False},
        latest_agent_decision_overview_for_symbol=lambda symbol: {},
    )
    terminal._open_symbol_chart = lambda symbol, timeframe=None: calls.append(("open", symbol, timeframe))
    terminal._refresh_active_chart_data = lambda: calls.append(("chart",))
    terminal._refresh_active_orderbook = lambda: calls.append(("orderbook",))
    _bind(
        terminal,
        "_get_or_create_tool_window",
        "_is_qt_object_alive",
        "_selected_agent_timeline_symbol",
        "_selected_agent_timeline_row",
        "_agent_timeline_row_status_label",
        "_populate_agent_timeline_filters",
        "_toggle_agent_timeline_pin_symbol",
        "_agent_timeline_health_snapshot",
        "_refresh_agent_timeline_health",
        "_agent_timeline_anomaly_snapshot",
        "_agent_timeline_anomaly_fingerprint",
        "_visible_agent_timeline_anomaly_snapshot",
        "_refresh_agent_timeline_anomalies",
        "_agent_timeline_assignment_text",
        "_agent_timeline_recommendation_text",
        "_refresh_agent_timeline_window",
        "_refresh_agent_timeline_details",
        "_replay_selected_agent_timeline_symbol",
        "_open_selected_agent_timeline_symbol_in_strategy_assigner",
        "_refresh_selected_agent_timeline_symbol",
        "_acknowledge_selected_agent_timeline_anomaly",
        "_open_agent_timeline",
    )

    window = Terminal._open_agent_timeline(terminal)
    tree = window._agent_timeline_tree
    tree.setCurrentItem(tree.topLevelItem(0).child(0))

    refreshed = Terminal._refresh_selected_agent_timeline_symbol(terminal, window)

    assert refreshed == "USD/JPY"
    assert calls == [("open", "USD/JPY", "15m"), ("chart",), ("orderbook",)]


def test_chart_context_action_supports_market_ticket_prefill():
    captured = {}
    fake = SimpleNamespace(
        _current_chart_symbol=lambda: "BTC/USDT",
        _open_manual_trade=lambda prefill=None: captured.setdefault("prefill", dict(prefill or {})),
    )

    Terminal._handle_chart_trade_context_action(
        fake,
        {"action": "buy_market_ticket", "symbol": "BTC/USDT", "timeframe": "1h", "price": 100.0},
    )

    assert captured["prefill"]["symbol"] == "BTC/USDT"
    assert captured["prefill"]["side"] == "buy"
    assert captured["prefill"]["order_type"] == "market"


def test_request_chart_data_for_widget_marks_empty_broker_history_on_chart():
    _app()
    terminal = _ChartRequestTerminal(None)
    _bind(
        terminal,
        "_register_chart_request_token",
        "_is_chart_request_current",
        "_request_chart_data_for_widget",
        "_update_chart",
    )

    result = asyncio.run(Terminal._request_chart_data_for_widget(terminal, terminal.chart, limit=240))

    assert result is None
    assert terminal.chart._chart_status_mode == "error"
    assert terminal.chart._chart_status_message == "No data received."


def test_request_chart_data_for_widget_retargets_coinbase_derivative_symbol():
    _app()
    frame = pd.DataFrame(
        {
            "timestamp": [1700000000 + (index * 3600) for index in range(5)],
            "open": [100.0 + index for index in range(5)],
            "high": [101.0 + index for index in range(5)],
            "low": [99.0 + index for index in range(5)],
            "close": [100.4 + index for index in range(5)],
            "volume": [1000.0 + (index * 20.0) for index in range(5)],
        }
    )
    terminal = _ChartRequestTerminal(frame)
    terminal.controller = _DerivativeChartRequestController(frame)
    terminal.chart.controller = terminal.controller
    terminal.chart.symbol = "BTC/USD"
    terminal._active_chart_widget_ref = terminal.chart
    terminal.symbol_picker = QComboBox()
    terminal.symbol_picker.addItem("BTC/USD")
    terminal.symbol_picker.setCurrentText("BTC/USD")
    _bind(
        terminal,
        "_register_chart_request_token",
        "_is_chart_request_current",
        "_request_chart_data_for_widget",
        "_update_chart",
        "_retarget_chart_widget_symbol",
    )

    result = asyncio.run(Terminal._request_chart_data_for_widget(terminal, terminal.chart, limit=240))

    assert result is not None
    assert terminal.controller.requested_symbols == ["BTC/USD:USD"]
    assert terminal.chart.symbol == "BTC/USD:USD"
    assert terminal.symbol_picker.currentText() == "BTC/USD:USD"


def test_request_chart_data_for_widget_marks_limited_history_after_loading():
    _app()
    frame = pd.DataFrame(
        {
            "timestamp": [1700000000 + (index * 3600) for index in range(60)],
            "open": [100.0 + index for index in range(60)],
            "high": [101.0 + index for index in range(60)],
            "low": [99.0 + index for index in range(60)],
            "close": [100.4 + index for index in range(60)],
            "volume": [1000.0 + (index * 20.0) for index in range(60)],
        }
    )
    terminal = _ChartRequestTerminal(frame)
    _bind(
        terminal,
        "_register_chart_request_token",
        "_is_chart_request_current",
        "_request_chart_data_for_widget",
        "_update_chart",
    )

    result = asyncio.run(Terminal._request_chart_data_for_widget(terminal, terminal.chart, limit=240))

    assert result is not None
    assert terminal.chart._chart_status_mode == "notice"
    assert terminal.chart._chart_status_message == "Loaded 60 / 240 candles."
    assert terminal.chart._last_df is not None
    assert len(terminal.chart._last_df.index) == 60


def test_update_symbols_retargets_coinbase_derivative_charts():
    _app()
    chart = ChartWidget("BTC/USD", "1h", SimpleNamespace(broker=None))
    refreshed = []
    symbol_picker = QComboBox()
    symbol_picker.addItem("BTC/USD")
    symbol_picker.setCurrentText("BTC/USD")
    fake = SimpleNamespace(
        controller=SimpleNamespace(
            _resolve_preferred_market_symbol=lambda symbol, preference=None: "BTC/USD:USD" if str(symbol).upper() == "BTC/USD" else str(symbol).upper()
        ),
        symbols_table=QTableWidget(),
        symbol_picker=symbol_picker,
        chart=chart,
        symbol="BTC/USD",
        current_timeframe="1h",
        _active_chart_widget_ref=chart,
        _configure_market_watch_table=lambda: None,
        _set_market_watch_row=lambda row, symbol, bid="-", ask="-", status="", usd_value="-": None,
        _reorder_market_watch_rows=lambda: None,
        _all_chart_widgets=lambda: [chart],
        _schedule_chart_data_refresh=lambda chart_ref: refreshed.append(chart_ref.symbol),
        _is_qt_object_alive=lambda obj: obj is not None,
        _chart_tabs_ready=lambda: False,
        _refresh_symbol_picker_favorites=lambda: None,
        _update_favorite_action_text=lambda: None,
    )
    _bind(fake, "_retarget_chart_widget_symbol")

    Terminal._update_symbols(fake, "coinbase", ["BTC/USD:USD"])

    assert chart.symbol == "BTC/USD:USD"
    assert symbol_picker.currentText() == "BTC/USD:USD"
    assert refreshed == ["BTC/USD:USD"]
