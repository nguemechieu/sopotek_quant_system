import os
import sys
from pathlib import Path
from types import SimpleNamespace

from PySide6.QtWidgets import QApplication, QMainWindow

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from frontend.ui.terminal import Terminal


def _app():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class _FakeTerminal(QMainWindow):
    def __init__(self, exchange_name=""):
        super().__init__()
        broker = SimpleNamespace(exchange_name=exchange_name) if exchange_name else None
        self.controller = SimpleNamespace(language_code="en", set_language=lambda _code: None, broker=broker, config=None)
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
        self.detached_tool_windows = {}

    def _tr(self, key, **kwargs):
        return key

    def apply_language(self):
        return Terminal.apply_language(self)

    def _active_exchange_name(self):
        return Terminal._active_exchange_name(self)

    def _is_qt_object_alive(self, obj):
        return Terminal._is_qt_object_alive(self, obj)

    def _sync_chart_timeframe_menu_actions(self):
        return Terminal._sync_chart_timeframe_menu_actions(self)

    def _update_autotrade_button(self):
        return None

    def _set_active_timeframe_button(self, _timeframe):
        return None

    def __getattr__(self, name):
        if name.startswith("_"):
            return lambda *args, **kwargs: None
        raise AttributeError(name)


def test_create_menu_bar_groups_actions_into_single_clear_menus():
    _app()
    terminal = _FakeTerminal()

    Terminal._create_menu_bar(terminal)

    menu_titles = [action.text() for action in terminal.menuBar().actions()]

    file_actions = terminal.file_menu.actions()
    chart_actions = terminal.charts_menu.actions()
    chart_style_actions = terminal.chart_style_menu.actions()
    chart_study_actions = terminal.chart_studies_menu.actions()
    strategy_actions = terminal.strategy_menu.actions()
    backtest_actions = terminal.backtest_menu.actions()
    risk_actions = terminal.risk_menu.actions()
    analyze_positions_actions = terminal.analyze_positions_menu.actions()
    analyze_trade_actions = terminal.analyze_trade_review_menu.actions()
    analyze_desk_actions = terminal.analyze_desk_menu.actions()
    review_actions = terminal.review_menu.actions()
    research_actions = terminal.research_menu.actions()
    education_actions = terminal.education_menu.actions()
    tools_actions = terminal.tools_menu.actions()
    settings_actions = terminal.settings_menu.actions()

    assert terminal.settings_menu.menuAction() not in file_actions
    assert terminal.action_exit in file_actions
    assert terminal.action_generate_report not in file_actions
    assert terminal.action_export_trades not in file_actions
    assert terminal.settings_menu.title() in menu_titles

    assert terminal.action_app_settings in settings_actions
    assert terminal.language_menu.menuAction() in settings_actions

    assert terminal.backtest_menu.menuAction() in strategy_actions
    assert terminal.action_strategy_assigner in strategy_actions
    assert terminal.action_strategy_scorecard in strategy_actions
    assert terminal.action_strategy_debug in strategy_actions
    assert terminal.action_run_backtest in backtest_actions
    assert terminal.action_strategy_optimization in backtest_actions

    assert terminal.chart_timeframe_menu.menuAction() in chart_actions
    assert terminal.chart_style_menu.menuAction() in chart_actions
    assert terminal.chart_studies_menu.menuAction() in chart_actions
    assert terminal.action_chart_settings in chart_style_actions
    assert terminal.action_candle_colors in chart_style_actions
    assert terminal.action_edit_studies in chart_study_actions
    assert terminal.action_add_indicator in chart_study_actions
    assert terminal.action_remove_indicator in chart_study_actions
    assert terminal.action_remove_all_indicators in chart_study_actions
    assert terminal.chart_timeframe_actions["1h"].isChecked() is True

    assert terminal.analyze_positions_menu.menuAction() in risk_actions
    assert terminal.analyze_trade_review_menu.menuAction() in risk_actions
    assert terminal.analyze_desk_menu.menuAction() in risk_actions
    assert terminal.action_risk_settings in analyze_positions_actions
    assert terminal.action_portfolio_view in analyze_positions_actions
    assert terminal.action_position_analysis in analyze_positions_actions
    assert terminal.action_trade_checklist in analyze_trade_actions
    assert terminal.action_closed_journal in analyze_trade_actions
    assert terminal.action_journal_review in analyze_trade_actions
    assert terminal.action_system_health in analyze_desk_actions
    assert terminal.action_quant_pm in analyze_desk_actions
    assert terminal.action_kill_switch not in risk_actions

    assert terminal.action_performance in review_actions
    assert terminal.action_recommendations in review_actions
    assert terminal.action_closed_journal in review_actions
    assert terminal.action_journal_review in review_actions
    assert terminal.action_generate_report in review_actions
    assert terminal.action_export_trades in review_actions

    assert terminal.action_market_chat in research_actions
    assert terminal.action_quant_pm in research_actions
    assert terminal.action_ml_monitor in research_actions
    assert terminal.action_ml_research in research_actions
    assert terminal.action_stellar_asset_explorer in research_actions
    assert terminal.action_recommendations not in research_actions
    assert terminal.action_strategy_optimization not in research_actions
    assert terminal.action_strategy_assigner not in research_actions
    assert terminal.action_run_backtest not in research_actions

    assert terminal.education_menu.title() in menu_titles
    assert terminal.action_trader_tv in education_actions
    assert terminal.action_education_center in education_actions
    assert terminal.action_documentation in education_actions
    assert terminal.action_api_docs in education_actions
    assert terminal.action_market_chat not in education_actions

    assert terminal.action_logs in tools_actions
    assert terminal.action_export_diagnostics in tools_actions
    assert terminal.action_system_console in tools_actions
    assert terminal.action_system_status in tools_actions
    assert terminal.action_market_chat not in tools_actions
    assert terminal.action_performance not in tools_actions
    assert menu_titles[-1] == terminal.help_menu.title()


def test_create_menu_bar_hides_stellar_explorer_when_exchange_is_not_stellar():
    _app()
    terminal = _FakeTerminal(exchange_name="coinbase")

    Terminal._create_menu_bar(terminal)

    assert terminal.action_stellar_asset_explorer.isVisible() is False
    assert terminal._research_stellar_separator_action.isVisible() is False


def test_create_menu_bar_shows_stellar_explorer_when_exchange_is_stellar():
    _app()
    terminal = _FakeTerminal(exchange_name="stellar")

    Terminal._create_menu_bar(terminal)

    assert terminal.action_stellar_asset_explorer.isVisible() is True
    assert terminal._research_stellar_separator_action.isVisible() is True


def test_learning_windows_use_market_snapshot_in_text_payload():
    captured = []
    fake = SimpleNamespace(
        controller=SimpleNamespace(
            symbols=["ES", "NQ"],
            news_enabled=False,
            broker=SimpleNamespace(exchange_name="schwab"),
        ),
        current_timeframe="15m",
        current_connection_status="connected",
        autotrading_enabled=True,
        symbol="ES",
        _current_chart_symbol=lambda: "ES",
        _active_exchange_name=lambda: "schwab",
    )
    fake._learning_market_snapshot = lambda: Terminal._learning_market_snapshot(fake)
    fake._trader_tv_html = lambda: Terminal._trader_tv_html(fake)
    fake._education_center_html = lambda: Terminal._education_center_html(fake)
    fake._open_text_window = lambda key, title, markup, width=0, height=0: captured.append(
        {"key": key, "title": title, "html": markup, "width": width, "height": height}
    )

    Terminal._open_trader_tv_window(fake)
    Terminal._open_education_center_window(fake)

    assert captured[0]["key"] == "education_trader_tv"
    assert captured[0]["title"] == "Trader TV"
    assert captured[0]["width"] == 920
    assert "Current Desk Snapshot" in captured[0]["html"]
    assert "SCHWAB" in captured[0]["html"]
    assert "Focus symbol:</b> ES" in captured[0]["html"]
    assert "Live feed off" in captured[0]["html"]

    assert captured[1]["key"] == "education_center"
    assert captured[1]["title"] == "Education Center"
    assert captured[1]["width"] == 940
    assert "Practice Loop Inside This App" in captured[1]["html"]
    assert "Ready-For-Live Checklist" in captured[1]["html"]


def test_set_status_value_ignores_deleted_qt_labels():
    terminal = _FakeTerminal()

    class _DeadLabel:
        def setText(self, _value):
            raise AssertionError("deleted label should not be touched")

        def setToolTip(self, _value):
            raise AssertionError("deleted label should not be touched")

    dead_label = _DeadLabel()
    terminal.status_labels = {"Websocket": dead_label}
    terminal._is_qt_object_alive = lambda obj: obj is not dead_label
    terminal._elide_text = lambda value, max_length=42: str(value)

    Terminal._set_status_value(terminal, "Websocket", "Restarting", "Restarting market data")

    assert terminal.status_labels == {}
