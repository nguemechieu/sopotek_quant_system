import os
import sys
from pathlib import Path

from PySide6.QtWidgets import QApplication, QMainWindow

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from frontend.ui.panels.system_panels import (
    AI_MONITOR_HEADERS,
    create_ai_signal_panel,
    create_system_console_panel,
    create_system_status_panel,
)


def _app():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class DummyTerminal(QMainWindow):
    def __init__(self):
        super().__init__()
        self.monitor_tables = []
        self.screenshot_requests = 0

    def _configure_monitor_table(self, table):
        self.monitor_tables.append(table)

    def take_screen_shot(self):
        self.screenshot_requests += 1


def test_create_system_console_panel_connects_screenshot_button():
    _app()
    terminal = DummyTerminal()

    create_system_console_panel(terminal)
    terminal.system_console.screenshot_button.click()

    assert terminal.screenshot_requests == 1


def test_create_system_status_panel_populates_labels_and_starts_hidden():
    _app()
    terminal = DummyTerminal()

    dock = create_system_status_panel(terminal)

    assert dock is terminal.system_status_dock
    assert dock.isVisible() is False
    assert "Exchange" in terminal.status_labels
    assert "Timeframe" in terminal.status_labels


def test_create_ai_signal_panel_builds_monitor_table_with_expected_headers():
    _app()
    terminal = DummyTerminal()

    dock = create_ai_signal_panel(terminal)

    assert dock is terminal.ai_signal_dock
    assert terminal.monitor_tables == [terminal.ai_table]
    assert terminal.ai_table.columnCount() == len(AI_MONITOR_HEADERS)
    assert [
        terminal.ai_table.horizontalHeaderItem(index).text()
        for index in range(terminal.ai_table.columnCount())
    ] == AI_MONITOR_HEADERS
