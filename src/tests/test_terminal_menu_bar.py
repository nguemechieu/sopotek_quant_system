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
    def __init__(self):
        super().__init__()
        self.controller = SimpleNamespace(language_code="en", set_language=lambda _code: None)
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

    def _tr(self, key, **kwargs):
        return key

    def apply_language(self):
        return Terminal.apply_language(self)

    def _update_autotrade_button(self):
        return None

    def _set_active_timeframe_button(self, _timeframe):
        return None

    def __getattr__(self, name):
        if name.startswith("_"):
            return lambda *args, **kwargs: None
        raise AttributeError(name)


def test_create_menu_bar_adds_trader_focused_risk_review_and_research_menus():
    _app()
    terminal = _FakeTerminal()

    Terminal._create_menu_bar(terminal)

    risk_actions = terminal.risk_menu.actions()
    review_actions = terminal.review_menu.actions()
    research_actions = terminal.research_menu.actions()

    assert terminal.action_risk_settings in risk_actions
    assert terminal.action_portfolio_view in risk_actions
    assert terminal.action_position_analysis in risk_actions
    assert terminal.action_trade_checklist in risk_actions
    assert terminal.action_system_health in risk_actions
    assert terminal.action_kill_switch in risk_actions

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
    assert terminal.action_strategy_optimization in research_actions
    assert terminal.action_strategy_assigner in research_actions
    assert terminal.action_run_backtest in research_actions
