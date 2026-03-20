import os
import sys
from pathlib import Path
from types import SimpleNamespace

from PySide6.QtWidgets import QApplication, QDockWidget, QMainWindow, QTableWidget

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from frontend.ui.terminal import Terminal


def _app():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_refresh_ai_monitor_table_populates_rows_from_signal_records():
    _app()
    table = QTableWidget()
    fake = SimpleNamespace(
        MAX_LOG_ROWS=200,
        _ai_signal_records={
            "EUR/USD": {
                "symbol": "EUR/USD",
                "signal": "BUY",
                "confidence": 0.73,
                "regime": "TREND_UP",
                "volatility": 0.0123,
                "timestamp": "2026-03-13T10:00:00+00:00",
            }
        },
        _is_qt_object_alive=lambda obj: obj is not None,
        _monitor_table_is_busy=lambda _table: False,
    )
    fake._ai_monitor_rows = lambda: Terminal._ai_monitor_rows(fake)

    Terminal._refresh_ai_monitor_table(fake, table, force=True)

    assert table.rowCount() == 1
    assert table.columnCount() == 6
    assert table.item(0, 0).text() == "EUR/USD"
    assert table.item(0, 1).text() == "BUY"
    assert table.item(0, 2).text() == "0.73"


def test_update_ai_signal_skips_hidden_dock_table_and_refreshes_visible_monitor_window():
    _app()
    ai_table = QTableWidget()
    ai_dock = QDockWidget()
    ai_dock.setWidget(ai_table)
    ai_dock.hide()

    detached_window = QMainWindow()
    detached_table = QTableWidget()
    detached_window._monitor_table = detached_table
    detached_window.setCentralWidget(detached_table)
    detached_window.show()
    QApplication.processEvents()

    refreshed = []
    fake = SimpleNamespace(
        _ui_shutting_down=False,
        _ai_signal_records={},
        _last_ai_table_refresh_at=0.0,
        AI_TABLE_REFRESH_MIN_SECONDS=0.0,
        ai_table=ai_table,
        ai_signal_dock=ai_dock,
        detached_tool_windows={"ml_monitor": detached_window},
        _record_recommendation=lambda **_kwargs: None,
        _is_qt_object_alive=lambda obj: obj is not None,
    )

    def _refresh(table, force=False):
        refreshed.append((table, force))

    fake._refresh_ai_monitor_table = _refresh

    Terminal._update_ai_signal(
        fake,
        {
            "symbol": "EUR/USD",
            "signal": "BUY",
            "confidence": 0.81,
            "regime": "TREND_UP",
            "volatility": 0.014,
            "reason": "Momentum aligned",
            "timestamp": "2026-03-13T10:00:00+00:00",
        },
    )

    assert "EUR/USD" in fake._ai_signal_records
    refreshed_tables = [table for table, _force in refreshed]
    assert ai_table not in refreshed_tables
    assert detached_table in refreshed_tables
