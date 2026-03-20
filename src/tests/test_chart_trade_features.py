import os
import sys
from pathlib import Path
from types import SimpleNamespace

from PySide6.QtWidgets import QApplication

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from frontend.ui.chart.chart_widget import ChartWidget


def _app():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_chart_trade_plan_label_reports_risk_reward_ratio():
    _app()
    widget = ChartWidget("BTC/USDT", "1h", SimpleNamespace(broker=None))

    widget.set_trade_overlay(entry=100.0, stop_loss=95.0, take_profit=110.0, side="buy")

    assert "RR 2.00" in widget.trade_plan_label.text()
    assert "Risk 5.00000" in widget.trade_plan_label.text()
    assert "Reward 10.00000" in widget.trade_plan_label.text()


def test_chart_trade_context_menu_definitions_include_market_ticket_actions():
    _app()
    widget = ChartWidget("ETH/USDT", "15m", SimpleNamespace(broker=None))

    actions = [item for item in widget._trade_context_menu_definitions() if item is not None]
    labels = [label for label, _action in actions]
    action_names = [action for _label, action in actions]

    assert "Buy Market Ticket" in labels
    assert "Sell Market Ticket" in labels
    assert "buy_market_ticket" in action_names
    assert "sell_market_ticket" in action_names
    assert "clear_levels" in action_names
