import os
import sys
from pathlib import Path
from types import SimpleNamespace

from PySide6.QtWidgets import QApplication, QTableWidget, QTableWidgetItem

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from frontend.ui.panels.trading_updates import (
    normalize_open_order_entry,
    normalize_position_entry,
    update_trade_log,
)


def _app():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_normalize_position_entry_derives_mark_value_and_pnl():
    fake = SimpleNamespace(_lookup_symbol_mid_price=lambda _symbol: 1.25)

    result = normalize_position_entry(
        fake,
        {
            "symbol": "EUR/USD",
            "side": "long",
            "amount": 2.0,
            "entry_price": 1.10,
        },
    )

    assert result["mark_price"] == 1.25
    assert result["value"] == 2.5
    assert round(result["pnl"], 2) == 0.30


def test_normalize_open_order_entry_uses_mid_price_and_computes_pnl():
    fake = SimpleNamespace(_lookup_symbol_mid_price=lambda _symbol: 105.0)

    result = normalize_open_order_entry(
        fake,
        {
            "symbol": "BTC/USDT",
            "side": "buy",
            "type": "limit",
            "price": 100.0,
            "amount": 2.0,
            "filled": 0.5,
            "status": "open",
        },
    )

    assert result["mark"] == 105.0
    assert result["remaining"] == 1.5
    assert result["pnl"] == 7.5


def test_update_trade_log_inserts_row_and_sets_tooltip():
    _app()
    refreshed = []
    table = QTableWidget()
    table.setColumnCount(10)
    fake = SimpleNamespace(
        trade_log=table,
        MAX_LOG_ROWS=200,
        _normalize_trade_log_entry=lambda trade: trade,
        _trade_log_row_for_entry=lambda _entry: None,
        _format_trade_log_value=lambda value: "" if value is None else str(value),
        _refresh_performance_views=lambda: refreshed.append(True),
    )

    update_trade_log(
        fake,
        {
            "timestamp": "2026-03-15T12:00:00Z",
            "symbol": "EUR/USD",
            "source": "Bot",
            "side": "buy",
            "price": 1.12,
            "size": 1000,
            "order_type": "market",
            "status": "filled",
            "order_id": "ord-1",
            "pnl": 12.5,
            "stop_loss": 1.1,
            "take_profit": 1.15,
            "reason": "Breakout",
            "strategy_name": "Trend Following",
            "confidence": 0.82,
            "spread_bps": 0.9,
            "slippage_bps": 0.2,
            "fee": 0.1,
            "blocked_by_guard": False,
        },
    )

    assert table.rowCount() == 1
    assert table.item(0, 1).text() == "EUR/USD"
    assert "SL: 1.1" in table.item(0, 0).toolTip()
    assert refreshed == [True]


def test_update_trade_log_updates_existing_row_by_order_id():
    _app()
    table = QTableWidget()
    table.setColumnCount(10)
    table.insertRow(0)
    table.setItem(0, 8, QTableWidgetItem("ord-1"))
    fake = SimpleNamespace(
        trade_log=table,
        MAX_LOG_ROWS=200,
        _normalize_trade_log_entry=lambda trade: trade,
        _trade_log_row_for_entry=lambda _entry: 0,
        _format_trade_log_value=lambda value: "" if value is None else str(value),
        _refresh_performance_views=lambda: None,
    )

    update_trade_log(
        fake,
        {
            "timestamp": "2026-03-15T12:05:00Z",
            "symbol": "EUR/USD",
            "source": "Bot",
            "side": "sell",
            "price": 1.13,
            "size": 1000,
            "order_type": "market",
            "status": "closed",
            "order_id": "ord-1",
            "pnl": 20.0,
            "stop_loss": "",
            "take_profit": "",
            "reason": "",
            "strategy_name": "",
            "confidence": "",
            "spread_bps": "",
            "slippage_bps": "",
            "fee": "",
            "blocked_by_guard": False,
        },
    )

    assert table.rowCount() == 1
    assert table.item(0, 3).text() == "sell"
