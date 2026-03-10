import asyncio
from datetime import datetime, timezone
import html
import json
from pathlib import Path
import random
import subprocess
import sys
import threading
import tomllib
import traceback

import numpy as np
import pyqtgraph as pg
from PySide6.QtCore import Qt, QSettings, QDateTime, Signal, QTimer
from PySide6.QtGui import QAction, QColor, QTextCursor
from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QDockWidget,
    QTableWidget, QTableWidgetItem,
    QPushButton, QLabel, QComboBox,
    QTabWidget, QToolBar, QFileDialog, QDialog, QGridLayout, QDoubleSpinBox, QMessageBox, QFormLayout, QInputDialog, QColorDialog,
    QFrame,
    QHBoxLayout, QSizePolicy, QTextEdit, QTextBrowser, QApplication, QLineEdit
)
from shiboken6 import isValid

from backtesting.backtest_engine import BacktestEngine
from backtesting.report_generator import ReportGenerator
from backtesting.simulator import Simulator
from frontend.console.system_console import SystemConsole
from frontend.ui.chart.chart_widget import ChartWidget
from frontend.ui.i18n import iter_supported_languages
from frontend.ui.panels.orderbook_panel import OrderBookPanel
from strategy.strategy import Strategy

def global_exception_hook(exctype, value, tb):
    # Ignore expected shutdown interrupts so the terminal closes quietly.
    if exctype in (KeyboardInterrupt, SystemExit):
        return

    print("UNCAUGHT EXCEPTION:")
    traceback.print_exception(exctype, value, tb)




class Terminal(QMainWindow):
    logout_requested = Signal()
    ai_signal = Signal(dict)
    autotrade_toggle = Signal(bool)
    def __init__(self, controller):

        super().__init__(controller)

        sys.excepthook = global_exception_hook

        self.controller = controller
        self.logger = controller.logger

        self.settings = QSettings("Sopotek", "TradingPlatform")

        self.symbols_table = QTableWidget()

        self.risk_map = None
        self.auto_button = QPushButton()

        self.historical_data = controller.historical_data

        self.confidence_data = []

        if controller.symbols:
            index = random.randint(0, len(controller.symbols) - 1)
            self.symbol = controller.symbols[index]
        else:
            self.symbol = "BTC/USDT"

        self.MAX_LOG_ROWS = 200
        self.current_timeframe = getattr(controller,"time_frame")
        self.autotrading_enabled = False
        self.autotrade_scope_value = str(getattr(controller, "autotrade_scope", "all") or "all").lower()
        self.autotrade_watchlist = set(getattr(controller, "autotrade_watchlist", set()) or set())

        self.training_status = {}
        self.show_bid_ask_lines = True
        self._ui_shutting_down = False
        self._positions_refresh_task = None
        self._open_orders_refresh_task = None
        self._latest_positions_snapshot = []
        self._latest_open_orders_snapshot = []
        self._ai_signal_records = {}

        self.candle_up_color = self.settings.value("chart/candle_up_color", "#26a69a")
        self.candle_down_color = self.settings.value("chart/candle_down_color", "#ef5350")

        self.heartbeat = QLabel("●")
        self.heartbeat.setStyleSheet("color: green")

        self._setup_core()
        self._setup_ui()
        self._setup_panels()
        self._connect_signals()
        self._setup_spinner()

        if hasattr(self.controller, "language_changed"):
            self.controller.language_changed.connect(lambda _code: self.apply_language())

        self.controller.symbols_signal.connect(self._update_symbols)

        self.refresh_timer = QTimer()
        self.refresh_timer.timeout.connect(self._refresh_terminal)
        self.refresh_timer.start(1000)

        self.orderbook_timer = QTimer()
        self.orderbook_timer.timeout.connect(self._request_active_orderbook)
        self.orderbook_timer.start(1500)

        self.ai_signal.connect(self._update_ai_signal)


    def _setup_core(self):

        self.order_type = self.controller.order_type
        self.setWindowTitle("Sopotek AI Trading Terminal")
        self.resize(1700, 950)

        self.connection_indicator = QLabel("● CONNECTING")
        self.connection_indicator.setStyleSheet(
            "color: orange; font-weight: bold;"
        )

        self.timeframe_buttons = {}
        self.toolbar = None
        self.toolbar_timeframe_label = None
        self.autotrade_scope_picker = None
        self.system_status_button = None
        self.system_status_dock = None
        self.trading_activity_label = None
        self.symbol_picker = None
        self.detached_tool_windows = {}
        self._last_chart_request_key = None
        self.current_connection_status = "connecting"
        self.language_actions = {}

    def _history_request_limit(self, fallback=None):
        value = fallback if fallback is not None else getattr(self.controller, "limit", 50000)
        try:
            return max(100, int(value))
        except Exception:
            return 50000

    def _tr(self, key, **kwargs):
        if hasattr(self.controller, "tr"):
            return self.controller.tr(key, **kwargs)
        return key

    def _active_exchange_name(self):
        broker = getattr(self.controller, "broker", None)
        if broker is not None:
            name = getattr(broker, "exchange_name", None)
            if name:
                return str(name).lower()

        config = getattr(self.controller, "config", None)
        broker_config = getattr(config, "broker", None)
        if broker_config is not None:
            exchange = getattr(broker_config, "exchange", None)
            if exchange:
                return str(exchange).lower()

        if hasattr(self, "symbols_table") and self.symbols_table is not None:
            name = self.symbols_table.accessibleName()
            if name:
                return str(name).lower()

        return ""

    def _is_stellar_market_watch(self):
        return self._active_exchange_name() == "stellar"

    def _market_watch_headers(self):
        if self._is_stellar_market_watch():
            return ["Watch", "Symbol", "Bid", "Ask", "USD Value", "AI Training"]
        return ["Watch", "Symbol", "Bid", "Ask", "AI Training"]

    def _market_watch_watch_column(self):
        return 0

    def _market_watch_symbol_column(self):
        return 1

    def _market_watch_bid_column(self):
        return 2

    def _market_watch_ask_column(self):
        return 3

    def _market_watch_status_column(self):
        return 5 if self._is_stellar_market_watch() else 4

    def _market_watch_usd_column(self):
        return 4 if self._is_stellar_market_watch() else None

    def _configure_market_watch_table(self):
        if not hasattr(self, "symbols_table") or self.symbols_table is None:
            return
        headers = self._market_watch_headers()
        self.symbols_table.setColumnCount(len(headers))
        self.symbols_table.setHorizontalHeaderLabels(headers)

    def _normalized_symbol(self, symbol):
        return str(symbol or "").upper().strip()

    def _find_market_watch_row(self, symbol):
        target = self._normalized_symbol(symbol)
        symbol_column = self._market_watch_symbol_column()
        for row in range(self.symbols_table.rowCount()):
            item = self.symbols_table.item(row, symbol_column)
            if item is not None and self._normalized_symbol(item.text()) == target:
                return row
        return None

    def _market_watch_check_item(self, symbol, checked=False):
        item = QTableWidgetItem("")
        item.setFlags(
            Qt.ItemFlag.ItemIsEnabled
            | Qt.ItemFlag.ItemIsUserCheckable
            | Qt.ItemFlag.ItemIsSelectable
        )
        item.setCheckState(Qt.CheckState.Checked if checked else Qt.CheckState.Unchecked)
        item.setToolTip(f"Trade {self._normalized_symbol(symbol)} when AI scope is Watchlist")
        return item

    def _set_market_watch_row(self, row, symbol, bid="-", ask="-", status="⏳", usd_value="-"):
        normalized_symbol = self._normalized_symbol(symbol)
        checked = normalized_symbol in self.autotrade_watchlist
        watch_column = self._market_watch_watch_column()
        symbol_column = self._market_watch_symbol_column()
        bid_column = self._market_watch_bid_column()
        ask_column = self._market_watch_ask_column()

        existing_check = self.symbols_table.item(row, watch_column)
        if existing_check is None:
            self.symbols_table.setItem(row, watch_column, self._market_watch_check_item(normalized_symbol, checked))
        else:
            blocked = self.symbols_table.blockSignals(True)
            existing_check.setCheckState(Qt.CheckState.Checked if checked else Qt.CheckState.Unchecked)
            existing_check.setToolTip(f"Trade {normalized_symbol} when AI scope is Watchlist")
            self.symbols_table.blockSignals(blocked)

        self.symbols_table.setItem(row, symbol_column, QTableWidgetItem(normalized_symbol))
        self.symbols_table.setItem(row, bid_column, QTableWidgetItem(str(bid)))
        self.symbols_table.setItem(row, ask_column, QTableWidgetItem(str(ask)))

        usd_column = self._market_watch_usd_column()
        if usd_column is not None:
            self.symbols_table.setItem(row, usd_column, QTableWidgetItem(str(usd_value)))

        self.symbols_table.setItem(row, self._market_watch_status_column(), QTableWidgetItem(str(status)))

    def _sync_watchlist_from_table(self):
        watchlist = set()
        watch_column = self._market_watch_watch_column()
        symbol_column = self._market_watch_symbol_column()
        for row in range(self.symbols_table.rowCount()):
            watch_item = self.symbols_table.item(row, watch_column)
            symbol_item = self.symbols_table.item(row, symbol_column)
            if watch_item is None or symbol_item is None:
                continue
            if watch_item.checkState() == Qt.CheckState.Checked:
                normalized = self._normalized_symbol(symbol_item.text())
                if normalized:
                    watchlist.add(normalized)
        self.autotrade_watchlist = watchlist
        if hasattr(self.controller, "set_autotrade_watchlist"):
            self.controller.set_autotrade_watchlist(sorted(watchlist))

    def _handle_market_watch_item_changed(self, item):
        if item is None or item.column() != self._market_watch_watch_column():
            return
        self._sync_watchlist_from_table()
        self._reorder_market_watch_rows()
        self._refresh_terminal()

    def _market_watch_row_snapshot(self, row):
        watch_item = self.symbols_table.item(row, self._market_watch_watch_column())
        symbol_item = self.symbols_table.item(row, self._market_watch_symbol_column())
        bid_item = self.symbols_table.item(row, self._market_watch_bid_column())
        ask_item = self.symbols_table.item(row, self._market_watch_ask_column())
        status_item = self.symbols_table.item(row, self._market_watch_status_column())
        usd_column = self._market_watch_usd_column()
        usd_item = self.symbols_table.item(row, usd_column) if usd_column is not None else None
        return {
            "checked": watch_item is not None and watch_item.checkState() == Qt.CheckState.Checked,
            "symbol": symbol_item.text() if symbol_item is not None else "",
            "bid": bid_item.text() if bid_item is not None else "-",
            "ask": ask_item.text() if ask_item is not None else "-",
            "status": status_item.text() if status_item is not None else "-",
            "usd_value": usd_item.text() if usd_item is not None else "-",
        }

    def _market_watch_priority_rank(self, symbol, checked=False):
        normalized = self._normalized_symbol(symbol)
        available_symbols = [
            self._normalized_symbol(item)
            for item in (getattr(self.controller, "symbols", []) or [])
            if self._normalized_symbol(item)
        ]
        available_set = set(available_symbols)
        scope = str(self.autotrade_scope_value or "all").lower()

        if scope == "all":
            if normalized in available_set:
                return (0, available_symbols.index(normalized))
            return (1, normalized)
        if scope == "selected":
            selected = self._normalized_symbol(self._current_chart_symbol() or getattr(self, "symbol", ""))
            if normalized == selected and selected:
                return (0, 0)
            if normalized in available_set:
                return (1, available_symbols.index(normalized))
            return (2, normalized)
        if scope == "watchlist":
            if checked or normalized in self.autotrade_watchlist:
                return (0, normalized)
            if normalized in available_set:
                return (1, available_symbols.index(normalized))
            return (2, normalized)
        return (9, normalized)

    def _reorder_market_watch_rows(self):
        if not hasattr(self, "symbols_table") or self.symbols_table is None:
            return
        row_count = self.symbols_table.rowCount()
        if row_count <= 1:
            return

        snapshots = [self._market_watch_row_snapshot(row) for row in range(row_count)]
        checked_symbols = {
            self._normalized_symbol(item.get("symbol", ""))
            for item in snapshots
            if item.get("checked")
        }
        snapshots.sort(
            key=lambda item: (
                self._market_watch_priority_rank(item.get("symbol", ""), item.get("checked", False)),
                self._normalized_symbol(item.get("symbol", "")),
            )
        )

        self.autotrade_watchlist = {symbol for symbol in checked_symbols if symbol}
        blocked = self.symbols_table.blockSignals(True)
        self.symbols_table.setRowCount(0)
        for snapshot in snapshots:
            row = self.symbols_table.rowCount()
            self.symbols_table.insertRow(row)
            self._set_market_watch_row(
                row,
                snapshot.get("symbol", ""),
                bid=snapshot.get("bid", "-"),
                ask=snapshot.get("ask", "-"),
                status=snapshot.get("status", "-"),
                usd_value=snapshot.get("usd_value", "-"),
            )
        self.symbols_table.blockSignals(blocked)

    def _autotrade_scope_label(self):
        labels = {
            "all": "All Symbols",
            "selected": "Selected Symbol",
            "watchlist": "Watchlist",
        }
        return labels.get(str(self.autotrade_scope_value or "all").lower(), "All Symbols")

    def _apply_autotrade_scope(self, scope):
        normalized = str(scope or "all").strip().lower()
        if normalized not in {"all", "selected", "watchlist"}:
            normalized = "all"
        self.autotrade_scope_value = normalized
        if hasattr(self.controller, "set_autotrade_scope"):
            self.controller.set_autotrade_scope(normalized)
        self.settings.setValue("autotrade/scope", normalized)
        if self.autotrade_scope_picker is not None:
            index = self.autotrade_scope_picker.findData(normalized)
            if index >= 0 and self.autotrade_scope_picker.currentIndex() != index:
                blocked = self.autotrade_scope_picker.blockSignals(True)
                self.autotrade_scope_picker.setCurrentIndex(index)
                self.autotrade_scope_picker.blockSignals(blocked)
            self.autotrade_scope_picker.setToolTip(
                "Choose whether AI trading scans all loaded symbols, only the active symbol, or only checked watchlist symbols."
            )
        self._update_autotrade_button()
        if hasattr(self, "status_labels"):
            self._refresh_terminal()
        self._reorder_market_watch_rows()

    def _change_autotrade_scope(self):
        if self.autotrade_scope_picker is None:
            return
        self._apply_autotrade_scope(self.autotrade_scope_picker.currentData())
        if hasattr(self, "system_console"):
            self.system_console.log(f"AI auto trade scope set to {self._autotrade_scope_label()}.", "INFO")

    def _stable_usd_assets(self):
        return {"USD", "USDC", "USDT", "USDP", "FDUSD", "TUSD", "BUSD"}

    def _ticker_mid_price(self, ticker):
        if not isinstance(ticker, dict):
            return None

        try:
            bid = float(ticker.get("bid") or ticker.get("bidPrice") or ticker.get("bp") or 0)
            ask = float(ticker.get("ask") or ticker.get("askPrice") or ticker.get("ap") or 0)
            last = float(ticker.get("last") or ticker.get("price") or 0)
        except Exception:
            return None

        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
        if last > 0:
            return last
        if bid > 0:
            return bid
        if ask > 0:
            return ask
        return None

    def _lookup_symbol_mid_price(self, symbol):
        ticker_stream = getattr(self.controller, "ticker_stream", None)
        if ticker_stream is None:
            return None
        ticker = ticker_stream.get(symbol)
        return self._ticker_mid_price(ticker)

    def _asset_to_usd_rate(self, asset_code):
        code = str(asset_code or "").upper().strip()
        if not code:
            return None
        if code in self._stable_usd_assets():
            return 1.0

        for stable in sorted(self._stable_usd_assets()):
            direct = self._lookup_symbol_mid_price(f"{code}/{stable}")
            if direct and direct > 0:
                return direct

            inverse = self._lookup_symbol_mid_price(f"{stable}/{code}")
            if inverse and inverse > 0:
                return 1.0 / inverse

        return None

    def _stellar_usd_value(self, symbol, bid, ask):
        if not self._is_stellar_market_watch():
            return None
        if not isinstance(symbol, str) or "/" not in symbol:
            return None

        try:
            mid = (float(bid) + float(ask)) / 2.0
        except Exception:
            return None
        if mid <= 0:
            return None

        _base, quote = symbol.upper().split("/", 1)
        quote_to_usd = self._asset_to_usd_rate(quote)
        if quote_to_usd is None:
            return None
        return mid * quote_to_usd

    def _format_market_watch_number(self, value):
        if value is None:
            return "-"
        try:
            numeric = float(value)
        except Exception:
            return "-"
        if numeric >= 1000:
            return f"{numeric:,.2f}"
        if numeric >= 1:
            return f"{numeric:,.4f}"
        return f"{numeric:,.6f}"

    def _format_market_watch_usd(self, value):
        if value is None:
            return "-"
        try:
            numeric = float(value)
        except Exception:
            return "-"
        if numeric >= 1000:
            return f"${numeric:,.2f}"
        if numeric >= 1:
            return f"${numeric:,.4f}"
        return f"${numeric:,.6f}"

    def _is_qt_object_alive(self, obj):
        try:
            return obj is not None and isValid(obj)
        except Exception:
            return False

    def _chart_tabs_ready(self):
        return (not self._ui_shutting_down) and self._is_qt_object_alive(
            getattr(self, "chart_tabs", None)
        )

    def _iter_detached_chart_pages(self):
        windows = getattr(self, "detached_tool_windows", {}) or {}
        pages = []
        stale_keys = []

        for key, window in windows.items():
            if not self._is_qt_object_alive(window):
                stale_keys.append(key)
                continue
            if not getattr(window, "_contains_chart_page", False):
                continue
            try:
                page = window.centralWidget()
            except Exception:
                page = None
            if page is not None:
                pages.append(page)

        for key in stale_keys:
            windows.pop(key, None)

        return pages

    def _chart_widgets_in_page(self, page):
        if page is None:
            return []
        if isinstance(page, ChartWidget):
            return [page]
        try:
            return list(page.findChildren(ChartWidget))
        except Exception:
            return []

    def _single_chart_window_key(self, symbol, timeframe):
        safe_symbol = str(symbol or "").upper().replace("/", "_").replace(":", "_")
        safe_timeframe = str(timeframe or "").lower().replace("/", "_")
        return f"chart_{safe_symbol}_{safe_timeframe}"

    def _detached_chart_windows(self):
        windows = []
        for window in self.detached_tool_windows.values():
            if not self._is_qt_object_alive(window):
                continue
            if getattr(window, "_contains_chart_page", False):
                windows.append(window)
        return windows

    def _find_detached_chart_window(self, symbol=None, timeframe=None):
        target_symbol = str(symbol or "").upper().strip() if symbol else None
        target_timeframe = str(timeframe or "").strip() if timeframe else None
        for window in self._detached_chart_windows():
            charts = self._chart_widgets_in_page(getattr(window, "centralWidget", lambda: None)())
            if not charts:
                continue
            chart = charts[0]
            if target_symbol and str(chart.symbol).upper() != target_symbol:
                continue
            if target_timeframe and str(chart.timeframe) != target_timeframe:
                continue
            return window
        return None

    def _active_detached_chart_window(self):
        active_window = QApplication.activeWindow()
        if active_window is not None and getattr(active_window, "_contains_chart_page", False):
            return active_window
        detached_windows = self._detached_chart_windows()
        if len(detached_windows) == 1:
            return detached_windows[0]
        return None

    def _install_chart_window_actions(self, window):
        if window is None or not self._is_qt_object_alive(window):
            return
        if getattr(window, "_chart_actions_installed", False):
            return

        for action_name in (
            "action_reattach_chart",
            "action_tile_charts",
            "action_cascade_charts",
            "action_refresh_chart",
            "action_refresh_orderbook",
        ):
            action = getattr(self, action_name, None)
            if action is not None:
                window.addAction(action)

        window._chart_actions_installed = True

    def _detached_chart_layouts(self):
        layouts = []
        for window in self._detached_chart_windows():
            charts = self._chart_widgets_in_page(getattr(window, "centralWidget", lambda: None)())
            if not charts:
                continue
            chart = charts[0]
            geometry = window.geometry()
            layouts.append(
                {
                    "symbol": str(chart.symbol),
                    "timeframe": str(chart.timeframe),
                    "x": int(geometry.x()),
                    "y": int(geometry.y()),
                    "width": int(geometry.width()),
                    "height": int(geometry.height()),
                }
            )
        return layouts

    def _save_detached_chart_layouts(self):
        try:
            self.settings.setValue("charts/detached_layouts", json.dumps(self._detached_chart_layouts()))
        except Exception as exc:
            self.logger.debug("Unable to save detached chart layouts: %s", exc)

    def _restore_detached_chart_layouts(self):
        raw_value = self.settings.value("charts/detached_layouts", "[]")
        try:
            layouts = json.loads(raw_value or "[]")
        except Exception:
            layouts = []

        if not isinstance(layouts, list):
            return

        for entry in layouts:
            if not isinstance(entry, dict):
                continue
            symbol = str(entry.get("symbol") or "").strip().upper()
            timeframe = str(entry.get("timeframe") or "").strip() or self.current_timeframe
            if not symbol:
                continue
            try:
                x = int(entry.get("x", 0))
                y = int(entry.get("y", 0))
                width = max(360, int(entry.get("width", 1200)))
                height = max(260, int(entry.get("height", 780)))
            except Exception:
                x, y, width, height = 0, 0, 1200, 780
            self._open_or_focus_detached_chart(symbol, timeframe, geometry=QtCore.QRect(x, y, width, height))

    def _reattach_chart_window(self, window):
        if window is None or not self._is_qt_object_alive(window):
            return
        if not getattr(window, "_contains_chart_page", False):
            return

        page = window.takeCentralWidget()
        if page is None:
            return

        title = self._chart_page_title(page)
        page.setParent(None)
        self.chart_tabs.addTab(page, title)
        self.chart_tabs.setCurrentWidget(page)
        for chart in self._chart_widgets_in_page(page):
            self._schedule_chart_data_refresh(chart)
        self._request_active_orderbook()
        window._contains_chart_page = False
        window.close()
        window.deleteLater()
        self._save_detached_chart_layouts()

    def _iter_chart_widgets(self):
        charts = []
        if self._chart_tabs_ready():
            try:
                count = self.chart_tabs.count()
            except RuntimeError:
                count = 0

            for index in range(count):
                try:
                    page = self.chart_tabs.widget(index)
                except RuntimeError:
                    break
                charts.extend(self._chart_widgets_in_page(page))

        for page in self._iter_detached_chart_pages():
            charts.extend(self._chart_widgets_in_page(page))
        return charts

    def _current_chart_widget(self):
        active_window = QApplication.activeWindow()
        if active_window is not None and active_window is not self:
            charts = self._chart_widgets_in_page(getattr(active_window, "centralWidget", lambda: None)())
            if charts:
                return charts[0]

        if self._chart_tabs_ready():
            try:
                page = self.chart_tabs.currentWidget()
            except RuntimeError:
                page = None
            charts = self._chart_widgets_in_page(page)
            if charts:
                return charts[0]

        detached_pages = self._iter_detached_chart_pages()
        if detached_pages:
            charts = self._chart_widgets_in_page(detached_pages[0])
            if charts:
                return charts[0]

        return None

    def _safe_disconnect(self, signal, slot):
        try:
            signal.disconnect(slot)
        except (RuntimeError, TypeError):
            pass

    def _disconnect_controller_signals(self):
        controller = getattr(self, "controller", None)
        if controller is None:
            return

        for signal_name, slot in (
            ("candle_signal", self._update_chart),
            ("equity_signal", self._update_equity),
            ("trade_signal", self._update_trade_log),
            ("ticker_signal", self._update_ticker),
            ("orderbook_signal", self._update_orderbook),
            ("strategy_debug_signal", self._handle_strategy_debug),
            ("training_status_signal", self._update_training_status),
            ("symbols_signal", self._update_symbols),
        ):
            signal = getattr(controller, signal_name, None)
            if signal is not None:
                self._safe_disconnect(signal, slot)

        ai_monitor = getattr(controller, "ai_signal_monitor", None)
        if ai_monitor is not None:
            self._safe_disconnect(ai_monitor, self._update_ai_signal)

    def _timeframe_button_style(self):
        return """
            QPushButton {
                background-color: #162033;
                color: #c7d2e0;
                border: 1px solid #25314a;
                border-radius: 9px;
                padding: 6px 12px;
                font-weight: 600;
                min-width: 44px;
            }
            QPushButton:hover {
                background-color: #1d2940;
                border-color: #3c537f;
            }
            QPushButton:checked {
                background-color: #2a7fff;
                color: white;
                border-color: #65a3ff;
            }
        """

    def _action_button_style(self):
        return """
            QPushButton {
                background-color: #162033;
                color: #d7dfeb;
                border: 1px solid #2d3a56;
                border-radius: 12px;
                padding: 8px 14px;
                font-weight: 600;
            }
            QPushButton:hover {
                background-color: #1c2940;
                border-color: #4f638d;
            }
        """

    def _set_active_timeframe_button(self, active_tf):
        for tf, button in self.timeframe_buttons.items():
            button.setChecked(tf == active_tf)

        if self.toolbar_timeframe_label is not None:
            self.toolbar_timeframe_label.setText(
                self._tr("terminal.toolbar.timeframe_active", timeframe=active_tf)
            )

    def _update_autotrade_button(self):
        scope_suffix = f" [{self._autotrade_scope_label()}]"
        if self.autotrading_enabled:
            self.auto_button.setText(f"{self._tr('terminal.autotrade.on')}{scope_suffix}")
            self.auto_button.setStyleSheet(
                """
                QPushButton {
                    background-color: #123524;
                    color: #d7ffe9;
                    border: 1px solid #28a86b;
                    border-radius: 14px;
                    padding: 9px 16px;
                    font-weight: 700;
                }
                QPushButton:hover {
                    background-color: #184630;
                }
                """
            )
            self._update_trading_activity_indicator(active=True)
        else:
            self.auto_button.setText(f"{self._tr('terminal.autotrade.off')}{scope_suffix}")
            self.auto_button.setStyleSheet(
                """
                QPushButton {
                    background-color: #34161a;
                    color: #ffd9de;
                    border: 1px solid #b45b68;
                    border-radius: 14px;
                    padding: 9px 16px;
                    font-weight: 700;
                }
                QPushButton:hover {
                    background-color: #442026;
                }
                """
            )
            self._update_trading_activity_indicator(active=False)

    def _update_trading_activity_indicator(self, active=None):
        label = getattr(self, "trading_activity_label", None)
        if label is None:
            return

        is_active = self.autotrading_enabled if active is None else bool(active)
        if not is_active:
            label.setText("AI Idle")
            label.setStyleSheet(
                "color: #8fa7c6; background-color: #132033; border: 1px solid #24324a; "
                "border-radius: 10px; padding: 5px 10px; font-weight: 700;"
            )
            return

        phase = getattr(self, "_spinner_index", 0) % 3
        texts = ["AI Live", "AI Live.", "AI Live.."]
        backgrounds = ["#123524", "#184630", "#0f2e22"]
        borders = ["#28a86b", "#32d296", "#1f8f5c"]
        label.setText(texts[phase])
        label.setStyleSheet(
            f"color: #d7ffe9; background-color: {backgrounds[phase]}; border: 1px solid {borders[phase]}; "
            "border-radius: 10px; padding: 5px 10px; font-weight: 700;"
        )

    def _setup_ui(self):

        self.chart_tabs = QTabWidget()
        self.chart_tabs.setTabsClosable(True)

        self.chart_tabs.tabCloseRequested.connect(self._close_chart_tab)
        self.chart_tabs.currentChanged.connect(self._on_chart_tab_changed)
        self.chart_tabs.tabBarDoubleClicked.connect(self._detach_chart_tab)

        self.setCentralWidget(self.chart_tabs)

        self._create_menu_bar()
        self._create_toolbar()

        self._create_chart_tab(
            self.symbol,
            self.controller.time_frame
        )

        self._restore_settings()
        self.apply_language()

    # ==========================================================
    # MENU
    # ==========================================================

    def _create_menu_bar(self):
        menu_bar = self.menuBar()

        self.file_menu = menu_bar.addMenu("")
        self.action_generate_report = QAction(self)
        self.action_generate_report.triggered.connect(self._generate_report)
        self.file_menu.addAction(self.action_generate_report)
        self.action_export_trades = QAction(self)
        self.action_export_trades.triggered.connect(self._export_trades)
        self.file_menu.addAction(self.action_export_trades)
        self.file_menu.addSeparator()
        self.action_exit = QAction(self)
        self.action_exit.triggered.connect(self.close)
        self.file_menu.addAction(self.action_exit)

        self.trading_menu = menu_bar.addMenu("")
        self.action_start_trading = QAction(self)
        self.action_start_trading.triggered.connect(lambda: self._set_autotrading_enabled(True))
        self.action_start_trading.setShortcut("Ctrl+T")
        self.trading_menu.addAction(self.action_start_trading)
        self.action_stop_trading = QAction(self)
        self.action_stop_trading.triggered.connect(lambda: self._set_autotrading_enabled(False))
        self.trading_menu.addAction(self.action_stop_trading)
        self.action_manual_trade = QAction(self)
        self.action_manual_trade.triggered.connect(self._open_manual_trade)
        self.trading_menu.addAction(self.action_manual_trade)
        self.trading_menu.addSeparator()
        self.action_close_all = QAction(self)
        self.action_close_all.triggered.connect(self._close_all_positions)
        self.trading_menu.addAction(self.action_close_all)
        self.action_cancel_orders = QAction(self)
        self.action_cancel_orders.triggered.connect(self._cancel_all_orders)
        self.trading_menu.addAction(self.action_cancel_orders)

        self.backtest_menu = menu_bar.addMenu("")
        self.action_run_backtest = QAction(self)
        self.action_run_backtest.triggered.connect(
            lambda: asyncio.get_event_loop().create_task(self.run_backtest_clicked())
        )
        self.action_run_backtest.setShortcut("Ctrl+B")
        self.backtest_menu.addAction(self.action_run_backtest)
        self.action_optimize_strategy = QAction(self)
        self.action_optimize_strategy.triggered.connect(self._optimize_strategy)
        self.backtest_menu.addAction(self.action_optimize_strategy)

        self.charts_menu = menu_bar.addMenu("")
        self.action_new_chart = QAction(self)
        self.action_new_chart.setShortcut("Ctrl+N")
        self.action_new_chart.triggered.connect(self._add_new_chart)
        self.charts_menu.addAction(self.action_new_chart)
        self.action_multi_chart = QAction(self)
        self.action_multi_chart.triggered.connect(self._multi_chart_layout)
        self.charts_menu.addAction(self.action_multi_chart)
        self.action_detach_chart = QAction("Detach Current Tab", self)
        self.action_detach_chart.setShortcut("Ctrl+Shift+D")
        self.action_detach_chart.setShortcutContext(Qt.ShortcutContext.ApplicationShortcut)
        self.action_detach_chart.triggered.connect(self._detach_current_chart_tab)
        self.charts_menu.addAction(self.action_detach_chart)
        self.action_reattach_chart = QAction("Reattach Active Chart", self)
        self.action_reattach_chart.setShortcut("Ctrl+Shift+R")
        self.action_reattach_chart.setShortcutContext(Qt.ShortcutContext.ApplicationShortcut)
        self.action_reattach_chart.triggered.connect(self._reattach_active_chart_window)
        self.charts_menu.addAction(self.action_reattach_chart)
        self.action_tile_chart_windows = QAction("Tile Chart Windows", self)
        self.action_tile_chart_windows.setShortcut("Ctrl+Shift+T")
        self.action_tile_chart_windows.setShortcutContext(Qt.ShortcutContext.ApplicationShortcut)
        self.action_tile_chart_windows.triggered.connect(self._tile_chart_windows)
        self.charts_menu.addAction(self.action_tile_chart_windows)
        self.action_cascade_chart_windows = QAction("Cascade Chart Windows", self)
        self.action_cascade_chart_windows.setShortcut("Ctrl+Shift+C")
        self.action_cascade_chart_windows.setShortcutContext(Qt.ShortcutContext.ApplicationShortcut)
        self.action_cascade_chart_windows.triggered.connect(self._cascade_chart_windows)
        self.charts_menu.addAction(self.action_cascade_chart_windows)
        self.action_candle_colors = QAction(self)
        self.action_candle_colors.triggered.connect(self._choose_candle_colors)
        self.charts_menu.addAction(self.action_candle_colors)
        self.action_add_indicator = QAction(self)
        self.action_add_indicator.triggered.connect(self._add_indicator_to_current_chart)
        self.charts_menu.addAction(self.action_add_indicator)
        self.toggle_bid_ask_lines_action = QAction(self)
        self.toggle_bid_ask_lines_action.setCheckable(True)
        self.toggle_bid_ask_lines_action.setChecked(self.show_bid_ask_lines)
        self.toggle_bid_ask_lines_action.triggered.connect(self._toggle_bid_ask_lines)
        self.charts_menu.addAction(self.toggle_bid_ask_lines_action)

        self.data_menu = menu_bar.addMenu("")
        self.action_refresh_markets = QAction(self)
        self.action_refresh_markets.triggered.connect(self._refresh_markets)
        self.data_menu.addAction(self.action_refresh_markets)
        self.action_refresh_chart = QAction(self)
        self.action_refresh_chart.triggered.connect(self._refresh_active_chart_data)
        self.data_menu.addAction(self.action_refresh_chart)
        self.action_refresh_orderbook = QAction(self)
        self.action_refresh_orderbook.triggered.connect(self._refresh_active_orderbook)
        self.data_menu.addAction(self.action_refresh_orderbook)
        self.data_menu.addSeparator()
        self.action_reload_balance = QAction(self)
        self.action_reload_balance.triggered.connect(self._reload_balance)
        self.data_menu.addAction(self.action_reload_balance)

        self.settings_menu = menu_bar.addMenu("")
        self.action_app_settings = QAction(self)
        self.action_app_settings.triggered.connect(self._open_settings)
        self.settings_menu.addAction(self.action_app_settings)
        self.action_portfolio_view = QAction(self)
        self.action_portfolio_view.triggered.connect(self._show_portfolio_exposure)
        self.settings_menu.addAction(self.action_portfolio_view)

        self.language_menu = menu_bar.addMenu("")
        self.language_actions = {}
        for code, label in iter_supported_languages():
            action = QAction(label, self)
            action.setCheckable(True)
            action.triggered.connect(lambda checked=False, lang=code: self.controller.set_language(lang))
            self.language_menu.addAction(action)
            self.language_actions[code] = action

        self.tools_menu = menu_bar.addMenu("")
        self.action_ml_monitor = QAction(self)
        self.action_ml_monitor.triggered.connect(self._open_ml_monitor)
        self.tools_menu.addAction(self.action_ml_monitor)
        self.action_logs = QAction(self)
        self.action_logs.triggered.connect(self._open_logs)
        self.tools_menu.addAction(self.action_logs)
        self.action_performance = QAction(self)
        self.action_performance.triggered.connect(self._open_performance)
        self.tools_menu.addAction(self.action_performance)

        self.help_menu = menu_bar.addMenu("")
        self.action_documentation = QAction(self)
        self.action_documentation.triggered.connect(self._open_docs)
        self.help_menu.addAction(self.action_documentation)
        self.action_api_docs = QAction(self)
        self.action_api_docs.triggered.connect(self._open_api_docs)
        self.help_menu.addAction(self.action_api_docs)
        self.help_menu.addSeparator()
        self.action_about = QAction(self)
        self.action_about.triggered.connect(self._show_about)
        self.help_menu.addAction(self.action_about)

        self.apply_language()

    def update_connection_status(self, status: str):
        self.current_connection_status = status

        if status == "connected":
            self.connection_indicator.setText("● CONNECTED")
            self.connection_indicator.setStyleSheet(
                "color: green; font-weight: bold;"
            )
        elif status == "disconnected":
            self.connection_indicator.setText("● DISCONNECTED")
            self.connection_indicator.setStyleSheet(
                "color: red; font-weight: bold;"
            )
        else:
            self.connection_indicator.setText("● CONNECTING")
            self.connection_indicator.setStyleSheet(
                "color: orange; font-weight: bold;"
            )

    def apply_language(self):
        self.setWindowTitle(self._tr("terminal.window_title"))

        if hasattr(self, "file_menu"):
            self.file_menu.setTitle(self._tr("terminal.menu.file"))
            self.trading_menu.setTitle(self._tr("terminal.menu.trading"))
            self.backtest_menu.setTitle(self._tr("terminal.menu.backtesting"))
            self.charts_menu.setTitle(self._tr("terminal.menu.charts"))
            self.data_menu.setTitle(self._tr("terminal.menu.data"))
            self.settings_menu.setTitle(self._tr("terminal.menu.settings"))
            self.language_menu.setTitle(self._tr("terminal.menu.language"))
            self.tools_menu.setTitle(self._tr("terminal.menu.tools"))
            self.help_menu.setTitle(self._tr("terminal.menu.help"))

            self.action_generate_report.setText(self._tr("terminal.action.generate_report"))
            self.action_export_trades.setText(self._tr("terminal.action.export_trades"))
            self.action_exit.setText(self._tr("terminal.action.exit"))
            self.action_start_trading.setText(self._tr("terminal.action.start_auto"))
            self.action_stop_trading.setText(self._tr("terminal.action.stop_auto"))
            self.action_manual_trade.setText(self._tr("terminal.action.manual_trade"))
            self.action_close_all.setText(self._tr("terminal.action.close_all"))
            self.action_cancel_orders.setText(self._tr("terminal.action.cancel_all"))
            self.action_run_backtest.setText(self._tr("terminal.action.run_backtest"))
            self.action_optimize_strategy.setText(self._tr("terminal.action.optimize"))
            self.action_new_chart.setText(self._tr("terminal.action.new_chart"))
            self.action_multi_chart.setText(self._tr("terminal.action.multi_chart"))
            self.action_detach_chart.setText("Detach Current Tab")
            self.action_reattach_chart.setText("Reattach Active Chart")
            self.action_tile_chart_windows.setText("Tile Chart Windows")
            self.action_cascade_chart_windows.setText("Cascade Chart Windows")
            self.action_candle_colors.setText(self._tr("terminal.action.candle_colors"))
            self.action_add_indicator.setText(self._tr("terminal.action.add_indicator"))
            self.toggle_bid_ask_lines_action.setText(self._tr("terminal.action.toggle_bid_ask"))
            self.action_refresh_markets.setText(self._tr("terminal.action.refresh_markets"))
            self.action_refresh_chart.setText(self._tr("terminal.action.refresh_chart"))
            self.action_refresh_orderbook.setText(self._tr("terminal.action.refresh_orderbook"))
            self.action_reload_balance.setText(self._tr("terminal.action.reload_balance"))
            self.action_app_settings.setText(self._tr("terminal.action.app_settings"))
            self.action_portfolio_view.setText(self._tr("terminal.action.portfolio"))
            self.action_ml_monitor.setText(self._tr("terminal.action.ml_monitor"))
            self.action_logs.setText(self._tr("terminal.action.logs"))
            self.action_performance.setText(self._tr("terminal.action.performance"))
            self.action_documentation.setText(self._tr("terminal.action.documentation"))
            self.action_api_docs.setText(self._tr("terminal.action.api_reference"))
            self.action_about.setText(self._tr("terminal.action.about"))

            active_language = getattr(self.controller, "language_code", "en")
            for code, action in self.language_actions.items():
                action.blockSignals(True)
                action.setChecked(code == active_language)
                action.blockSignals(False)

        if getattr(self, "symbol_label", None) is not None:
            self.symbol_label.setText(self._tr("terminal.toolbar.symbol"))
        if getattr(self, "open_symbol_button", None) is not None:
            self.open_symbol_button.setText(self._tr("terminal.toolbar.open_symbol"))
        if getattr(self, "screenshot_button", None) is not None:
            self.screenshot_button.setText(self._tr("terminal.toolbar.screenshot"))
        if getattr(self, "system_status_button", None) is not None:
            self.system_status_button.setText("Status")
            self.system_status_button.setToolTip("Show or hide the System Status panel")
        if getattr(self, "trading_activity_label", None) is not None:
            self.trading_activity_label.setToolTip("Shows whether AI trading is currently active")

        self._set_active_timeframe_button(getattr(self, "current_timeframe", "1h"))
        self._update_autotrade_button()

        status_key = {
            "connected": "terminal.status.connected",
            "disconnected": "terminal.status.disconnected",
        }.get(self.current_connection_status, "terminal.status.connecting")
        if getattr(self, "connection_indicator", None) is not None:
            self.connection_indicator.setText(f"* {self._tr(status_key)}")

    # ==========================================================
    # TOOLBAR
    # ==========================================================

    def _create_toolbar(self):
        toolbar = QToolBar("Main Toolbar")
        toolbar.setMovable(False)
        toolbar.setFloatable(False)
        toolbar.setStyleSheet("QToolBar { spacing: 8px; padding: 6px; }")
        self.toolbar = toolbar
        self.addToolBar(toolbar)

        symbol_box = QFrame()
        symbol_box.setStyleSheet(
            "QFrame { background-color: #101827; border: 1px solid #24324a; border-radius: 14px; }"
        )
        symbol_layout = QHBoxLayout(symbol_box)
        symbol_layout.setContentsMargins(10, 6, 10, 6)
        symbol_layout.setSpacing(8)

        self.symbol_label = QLabel(self._tr("terminal.toolbar.symbol"))
        self.symbol_label.setStyleSheet("color: #9fb0c7; font-weight: 700;")
        symbol_layout.addWidget(self.symbol_label)

        self.symbol_picker = QComboBox()
        self.symbol_picker.setMinimumWidth(170)
        self.symbol_picker.setStyleSheet(
            """
            QComboBox {
                background-color: #162033;
                color: #d7dfeb;
                border: 1px solid #2d3a56;
                border-radius: 10px;
                padding: 6px 10px;
                font-weight: 600;
            }
            QComboBox::drop-down {
                border: 0;
                width: 24px;
            }
            """
        )
        for sym in self.controller.symbols:
            self.symbol_picker.addItem(sym)
        self.symbol_picker.setCurrentText(self.symbol)
        self.symbol_picker.activated.connect(lambda _=None: self._open_symbol_from_picker())
        symbol_layout.addWidget(self.symbol_picker)

        self.open_symbol_button = QPushButton(self._tr("terminal.toolbar.open_symbol"))
        self.open_symbol_button.setStyleSheet(self._action_button_style())
        self.open_symbol_button.clicked.connect(self._open_symbol_from_picker)
        symbol_layout.addWidget(self.open_symbol_button)

        toolbar.addWidget(symbol_box)

        timeframe_box = QFrame()
        timeframe_box.setStyleSheet(
            "QFrame { background-color: #0f1726; border: 1px solid #24324a; border-radius: 16px; }"
        )
        timeframe_layout = QHBoxLayout(timeframe_box)
        timeframe_layout.setContentsMargins(10, 6, 10, 6)
        timeframe_layout.setSpacing(6)

        self.toolbar_timeframe_label = QLabel(self._tr("terminal.toolbar.timeframe"))
        self.toolbar_timeframe_label.setStyleSheet("color: #9fb0c7; font-weight: 700; padding-right: 6px;")
        timeframe_layout.addWidget(self.toolbar_timeframe_label)

        for tf in ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1mn"]:
            btn = QPushButton(tf)
            btn.setCheckable(True)
            btn.setStyleSheet(self._timeframe_button_style())
            btn.clicked.connect(lambda _, t=tf: self._set_timeframe(t))
            timeframe_layout.addWidget(btn)
            self.timeframe_buttons[tf] = btn

        toolbar.addWidget(timeframe_box)

        utility_box = QFrame()
        utility_box.setStyleSheet(
            "QFrame { background-color: #0f1726; border: 1px solid #24324a; border-radius: 16px; }"
        )
        utility_layout = QHBoxLayout(utility_box)
        utility_layout.setContentsMargins(8, 6, 8, 6)
        utility_layout.setSpacing(8)

        self.system_status_button = QPushButton("Status")
        self.system_status_button.setStyleSheet(self._action_button_style())
        self.system_status_button.setMinimumWidth(84)
        self.system_status_button.setToolTip("Show or hide the System Status panel")
        self.system_status_button.clicked.connect(self._show_system_status_panel)
        utility_layout.addWidget(self.system_status_button)

        self.screenshot_button = QPushButton(self._tr("terminal.toolbar.screenshot"))
        self.screenshot_button.setStyleSheet(self._action_button_style())
        self.screenshot_button.setMinimumWidth(110)
        self.screenshot_button.clicked.connect(self.take_screen_shot)
        utility_layout.addWidget(self.screenshot_button)

        self.trading_activity_label = QLabel("AI Idle")
        self.trading_activity_label.setMinimumWidth(74)
        self.trading_activity_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        utility_layout.addWidget(self.trading_activity_label)

        toolbar.addWidget(utility_box)

        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        toolbar.addWidget(spacer)

        actions_box = QFrame()
        actions_box.setStyleSheet(
            "QFrame { background-color: #0f1726; border: 1px solid #24324a; border-radius: 16px; }"
        )
        actions_layout = QHBoxLayout(actions_box)
        actions_layout.setContentsMargins(8, 6, 8, 6)
        actions_layout.setSpacing(8)

        scope_label = QLabel("Scope")
        scope_label.setStyleSheet("color: #9fb0c7; font-weight: 700;")
        actions_layout.addWidget(scope_label)

        self.autotrade_scope_picker = QComboBox()
        self.autotrade_scope_picker.setMinimumWidth(120)
        self.autotrade_scope_picker.setMaximumWidth(140)
        self.autotrade_scope_picker.setStyleSheet(
            """
            QComboBox {
                background-color: #162033;
                color: #d7dfeb;
                border: 1px solid #2d3a56;
                border-radius: 10px;
                padding: 6px 10px;
                font-weight: 600;
            }
            QComboBox::drop-down {
                border: 0;
                width: 24px;
            }
            """
        )
        self.autotrade_scope_picker.addItem("All Symbols", "all")
        self.autotrade_scope_picker.addItem("Selected Symbol", "selected")
        self.autotrade_scope_picker.addItem("Watchlist", "watchlist")
        self.autotrade_scope_picker.currentIndexChanged.connect(self._change_autotrade_scope)
        actions_layout.addWidget(self.autotrade_scope_picker)

        self.auto_button = QPushButton()
        self.auto_button.clicked.connect(self._toggle_autotrading)
        actions_layout.addWidget(self.auto_button)

        toolbar.addWidget(actions_box)

        self._set_active_timeframe_button(self.current_timeframe)
        self._apply_autotrade_scope(self.autotrade_scope_value)
        self._update_autotrade_button()
        return

        toolbar = QToolBar("Main Toolbar")
        self.addToolBar(toolbar)
        toolbar.addWidget(self.connection_indicator)

        self.heartbeat.setText("●")
        toolbar.addWidget(self.heartbeat)

        for tf in ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1mn"]:
            btn = QPushButton(tf)
            btn.clicked.connect(lambda _, t=tf: self._set_timeframe(t))


            toolbar.addWidget(btn)
            toolbar.addSeparator()


            self.timeframe_buttons[tf] = btn

        toolbar.addSeparator()
        self.auto_button = QPushButton("AutoTrading OFF")
        self.auto_button.clicked.connect(self._toggle_autotrading)

        toolbar.addWidget(self.auto_button)



        screenshot_btn = QPushButton("Screenshot")
        screenshot_btn.clicked.connect(self.take_screen_shot)
        toolbar.addWidget(screenshot_btn)

    # ==========================================================
    # AUTOTRADING
    # ==========================================================

    def _set_autotrading_enabled(self, enabled):
        target = bool(enabled)
        if self.autotrading_enabled == target:
            return
        self._toggle_autotrading()

    def _toggle_autotrading(self):

        self.autotrading_enabled = not self.autotrading_enabled

        if self.autotrading_enabled:

            if not self.controller.trading_system:
                self.logger.error("Trading system is not initialized yet")
                QMessageBox.warning(
                    self,
                    self._tr("terminal.warning.trading_not_ready_title"),
                    self._tr("terminal.warning.trading_not_ready_body"),
                )
                self.autotrading_enabled = False
                self._update_autotrade_button()
                self.autotrade_toggle.emit(False)
                return

            active_symbols = []
            if hasattr(self.controller, "get_active_autotrade_symbols"):
                try:
                    active_symbols = self.controller.get_active_autotrade_symbols()
                except Exception:
                    active_symbols = []
            if not active_symbols:
                message = "No symbols are available for the chosen AI scope."
                if self.autotrade_scope_value == "watchlist":
                    message = "Watchlist scope is selected, but no symbols are checked in Market Watch."
                elif self.autotrade_scope_value == "selected":
                    message = "Selected-symbol scope is selected, but there is no active symbol yet."
                QMessageBox.warning(self, "AI Trading Scope", message)
                self.autotrading_enabled = False
                self._update_autotrade_button()
                self.autotrade_toggle.emit(False)
                return

            self._update_autotrade_button()

            loop = asyncio.get_event_loop()
            loop.create_task(self.controller.trading_system.start())
            self.autotrade_toggle.emit(True)
            if hasattr(self, "system_console"):
                self.system_console.log(
                    f"AI auto trading enabled for {len(active_symbols)} symbol(s) using scope: {self._autotrade_scope_label()}.",
                    "INFO",
                )

        else:

            self._update_autotrade_button()

            if self.controller.trading_system:
                asyncio.create_task(self.controller.trading_system.stop())

            self.autotrade_toggle.emit(False)
            if hasattr(self, "system_console"):
                self.system_console.log("AI auto trading disabled.", "INFO")

    # ==========================================================
    # CHARTS
    # ==========================================================

    def _create_chart_tab(self, symbol, timeframe):
        chart = ChartWidget(
            symbol,
            timeframe,
            self.controller,
            candle_up_color=self.candle_up_color,
            candle_down_color=self.candle_down_color,
        )
        chart.set_bid_ask_lines_visible(self.show_bid_ask_lines)

        row = self._find_market_watch_row(symbol)
        if row is None:
            row = self.symbols_table.rowCount()
            self.symbols_table.insertRow(row)
        self._set_market_watch_row(row, symbol, bid="-", ask="-", status="? Training...", usd_value="-")
        self.chart_tabs.addTab(chart, f"{symbol} ({timeframe})")
        chart.link_all_charts(self.chart_tabs.count())
        self.chart_tabs.setCurrentWidget(chart)
        if self.symbol_picker is not None:
            self.symbol_picker.setCurrentText(symbol)
        self._request_active_orderbook()

    def _show_chart_page_in_window(self, page, title, detach_key, width=1320, height=860, geometry=None):
        window = self._get_or_create_tool_window(detach_key, title, width=width, height=height)
        window._contains_chart_page = True
        window._chart_window_key = detach_key
        window.setWindowTitle(title)
        window.setCentralWidget(page)
        page.setVisible(True)
        self._install_chart_window_actions(window)
        if geometry is not None:
            window.setGeometry(geometry)
        window.show()
        window.raise_()
        window.activateWindow()

        for chart in self._chart_widgets_in_page(page):
            try:
                chart.refresh_context_display()
            except Exception:
                pass
            last_df = getattr(chart, "_last_df", None)
            if last_df is not None and hasattr(last_df, "empty") and not last_df.empty:
                try:
                    chart.update_candles(last_df.copy())
                except Exception:
                    self._schedule_chart_data_refresh(chart)
            else:
                self._schedule_chart_data_refresh(chart)
            try:
                chart.updateGeometry()
                chart.repaint()
            except Exception:
                pass

        try:
            window.centralWidget().updateGeometry()
            window.centralWidget().repaint()
        except Exception:
            pass
        if not getattr(window, "_chart_layout_save_hook_installed", False):
            window.destroyed.connect(lambda *_: self._save_detached_chart_layouts())
            window._chart_layout_save_hook_installed = True
        return window

    def _schedule_chart_data_refresh(self, chart):
        if not isinstance(chart, ChartWidget):
            return

        if hasattr(self.controller, "request_candle_data"):
            asyncio.get_event_loop().create_task(
                self.controller.request_candle_data(
                    symbol=chart.symbol,
                    timeframe=chart.timeframe,
                    limit=self._history_request_limit(),
                )
            )

        asyncio.get_event_loop().create_task(
            self._reload_chart_data(chart.symbol, chart.timeframe)
        )

    def _chart_page_title(self, page, fallback_index=None):
        charts = self._chart_widgets_in_page(page)
        timeframe = self.current_timeframe
        if charts:
            timeframe = getattr(charts[0], "timeframe", timeframe)
        if len(charts) == 1:
            chart = charts[0]
            return f"{chart.symbol} ({chart.timeframe})"
        if len(charts) > 1:
            return f"Chart Group ({timeframe})"
        if fallback_index is not None and self._chart_tabs_ready():
            try:
                return self.chart_tabs.tabText(fallback_index)
            except Exception:
                pass
        return "Detached Chart"

    def _close_chart_tab(self, index):
        if not self._chart_tabs_ready():
            return

        try:
            page = self.chart_tabs.widget(index)
        except RuntimeError:
            return

        self.chart_tabs.removeTab(index)
        if page is not None:
            page.deleteLater()

    def _detach_current_chart_tab(self):
        if not self._chart_tabs_ready():
            return
        self._detach_chart_tab(self.chart_tabs.currentIndex())

    def _reattach_active_chart_window(self):
        window = self._active_detached_chart_window()
        if window is None:
            self.system_console.log("Focus a detached chart window first.", "ERROR")
            return
        self._reattach_chart_window(window)

    def _tile_chart_windows(self):
        windows = self._detached_chart_windows()
        if not windows:
            self.system_console.log("No detached chart windows to tile.", "INFO")
            return

        screen = QApplication.primaryScreen()
        available = screen.availableGeometry() if screen is not None else self.geometry()
        count = len(windows)
        columns = max(1, int(np.ceil(np.sqrt(count))))
        rows = max(1, int(np.ceil(count / columns)))
        width = max(360, available.width() // columns)
        height = max(260, available.height() // rows)

        for index, window in enumerate(windows):
            row = index // columns
            col = index % columns
            window.showNormal()
            window.setGeometry(
                available.x() + (col * width),
                available.y() + (row * height),
                width,
                height,
            )
        self._save_detached_chart_layouts()

    def _cascade_chart_windows(self):
        windows = self._detached_chart_windows()
        if not windows:
            self.system_console.log("No detached chart windows to cascade.", "INFO")
            return

        screen = QApplication.primaryScreen()
        available = screen.availableGeometry() if screen is not None else self.geometry()
        width = max(520, int(available.width() * 0.62))
        height = max(360, int(available.height() * 0.62))
        step = 32

        for index, window in enumerate(windows):
            offset = step * index
            max_x_offset = max(0, available.width() - width)
            max_y_offset = max(0, available.height() - height)
            window.showNormal()
            window.setGeometry(
                available.x() + min(offset, max_x_offset),
                available.y() + min(offset, max_y_offset),
                width,
                height,
            )
            window.raise_()

        windows[-1].activateWindow()
        self._save_detached_chart_layouts()

    def _detach_chart_tab(self, index):
        if not self._chart_tabs_ready():
            return
        if index is None or index < 0:
            return

        try:
            page = self.chart_tabs.widget(index)
        except RuntimeError:
            return
        if page is None:
            return

        title = self._chart_page_title(page, fallback_index=index)
        charts = self._chart_widgets_in_page(page)
        if len(charts) == 1:
            detach_key = self._single_chart_window_key(charts[0].symbol, charts[0].timeframe)
        else:
            object_name = getattr(page, "objectName", lambda: "")() or "chart_page"
            detach_key = getattr(page, "_detach_window_key", None)
            if not detach_key:
                detach_key = f"{object_name}_{abs(hash((title, id(page))))}"
                page._detach_window_key = detach_key

        existing_window = self._find_detached_chart_window(
            symbol=charts[0].symbol if len(charts) == 1 else None,
            timeframe=charts[0].timeframe if len(charts) == 1 else None,
        ) or self.detached_tool_windows.get(detach_key)
        if existing_window is not None:
            existing_window.showNormal()
            existing_window.raise_()
            existing_window.activateWindow()
            return

        self.chart_tabs.removeTab(index)
        page.setParent(None)
        self._show_chart_page_in_window(page, title, detach_key, width=1320, height=860)
        self._save_detached_chart_layouts()

    def _open_or_focus_detached_chart(self, symbol, timeframe=None, geometry=None):
        target_symbol = (symbol or "").strip().upper()
        target_timeframe = timeframe or self.current_timeframe
        if not target_symbol:
            return None

        detach_key = self._single_chart_window_key(target_symbol, target_timeframe)
        existing_window = self._find_detached_chart_window(target_symbol, target_timeframe) or self.detached_tool_windows.get(detach_key)
        if self._is_qt_object_alive(existing_window):
            if geometry is not None:
                existing_window.setGeometry(geometry)
            existing_window.showNormal()
            existing_window.raise_()
            existing_window.activateWindow()
            page = existing_window.centralWidget()
            for chart in self._chart_widgets_in_page(page):
                self._schedule_chart_data_refresh(chart)
            self._save_detached_chart_layouts()
            return existing_window

        existing_index = self._find_chart_tab(target_symbol, target_timeframe)
        if existing_index >= 0:
            try:
                page = self.chart_tabs.widget(existing_index)
            except RuntimeError:
                page = None
            if page is not None:
                self.chart_tabs.removeTab(existing_index)
                page.setParent(None)
                return self._show_chart_page_in_window(
                    page,
                    self._chart_page_title(page),
                    detach_key,
                    width=1320,
                    height=860,
                    geometry=geometry,
                )

        chart = ChartWidget(
            target_symbol,
            target_timeframe,
            self.controller,
            candle_up_color=self.candle_up_color,
            candle_down_color=self.candle_down_color,
        )
        chart.set_bid_ask_lines_visible(self.show_bid_ask_lines)
        window = self._show_chart_page_in_window(
            chart,
            f"{target_symbol} ({target_timeframe})",
            detach_key,
            width=1320,
            height=860,
            geometry=geometry,
        )
        self._schedule_chart_data_refresh(chart)
        self._save_detached_chart_layouts()
        return window

    def _on_chart_tab_changed(self, index):
        if not self._chart_tabs_ready():
            return

        try:
            page = self.chart_tabs.widget(index)
        except RuntimeError:
            return
        charts = self._chart_widgets_in_page(page)
        if not charts:
            return

        chart = charts[0]

        self.current_timeframe = chart.timeframe
        if self.symbol_picker is not None:
            self.symbol_picker.setCurrentText(chart.symbol)

        self._last_chart_request_key = (chart.symbol, chart.timeframe)
        for chart_widget in charts:
            self._schedule_chart_data_refresh(chart_widget)
        self._request_active_orderbook()

    def _add_new_chart(self):
        symbol, ok = QInputDialog.getText(
            self,
            self._tr("terminal.dialog.new_chart_title"),
            self._tr("terminal.dialog.new_chart_prompt"),
        )
        if ok and symbol:
            self._open_symbol_chart(symbol.upper(), self.current_timeframe)

    def _find_chart_tab(self, symbol, timeframe):
        if not self._chart_tabs_ready():
            return -1

        for i in range(self.chart_tabs.count()):
            page = self.chart_tabs.widget(i)
            for chart in self._chart_widgets_in_page(page):
                if chart.symbol == symbol and chart.timeframe == timeframe:
                    return i
        return -1

    def _open_symbol_chart(self, symbol, timeframe=None):
        target_symbol = (symbol or "").strip().upper()
        if not target_symbol:
            return

        target_timeframe = timeframe or self.current_timeframe
        detached_window = self._find_detached_chart_window(target_symbol, target_timeframe) or self.detached_tool_windows.get(
            self._single_chart_window_key(target_symbol, target_timeframe)
        )
        if self._is_qt_object_alive(detached_window):
            detached_window.showNormal()
            detached_window.raise_()
            detached_window.activateWindow()
            page = detached_window.centralWidget()
            for chart in self._chart_widgets_in_page(page):
                self._schedule_chart_data_refresh(chart)
            return

        existing_index = self._find_chart_tab(target_symbol, target_timeframe)
        if existing_index >= 0:
            self.chart_tabs.setCurrentIndex(existing_index)
            return

        self.training_status[target_symbol] = "TRAINING"
        self._create_chart_tab(target_symbol, target_timeframe)

    def _open_symbol_from_picker(self):
        if self.symbol_picker is None:
            return

        self._open_symbol_chart(self.symbol_picker.currentText(), self.current_timeframe)

    def _set_timeframe(self, tf="1h"):

        self.current_timeframe = tf
        self._set_active_timeframe_button(tf)

        if not self._chart_tabs_ready():
            return

        index = self.chart_tabs.currentIndex()
        page = self.chart_tabs.widget(index)
        charts = self._chart_widgets_in_page(page)
        if not charts:
            return

        primary_chart = charts[0]
        for chart in charts:
            chart.timeframe = tf
            if hasattr(chart, "refresh_context_display"):
                chart.refresh_context_display()
            self._schedule_chart_data_refresh(chart)

        if len(charts) == 1:
            tab_text = f"{primary_chart.symbol} ({tf})"
        else:
            tab_text = f"Multi Chart ({tf})"
        self.chart_tabs.setTabText(index, tab_text)

        self._request_active_orderbook()

    def _toggle_bid_ask_lines(self, checked):
        self.show_bid_ask_lines = bool(checked)

        for chart in self._iter_chart_widgets():
            chart.set_bid_ask_lines_visible(self.show_bid_ask_lines)

    # ==========================================================
    # UPDATE METHODS
    # ==========================================================
    def _update_chart(self, symbol, df):
        if self._ui_shutting_down:
            return

        df = candles_to_df(df)

        for chart in self._iter_chart_widgets():
            if chart.symbol == symbol:
                chart.update_candles(df)

        self.heartbeat.setStyleSheet("color: green;")

    def _update_equity(self, equity):
        if getattr(self, "equity_summary_label", None) is not None:
            self.equity_summary_label.setText(f"Equity: {float(equity):,.2f}")
        if getattr(self, "equity_curve", None) is not None:
            self.equity_curve.setData(self.controller.performance_engine.equity_history)
        self._refresh_performance_views()

    def _lookup_symbol_mid_price(self, symbol):
        ticker = None
        ticker_buffer = getattr(self.controller, "ticker_buffer", None)
        if ticker_buffer is not None and hasattr(ticker_buffer, "get"):
            try:
                ticker = ticker_buffer.get(symbol)
            except Exception:
                ticker = None
        if not isinstance(ticker, dict):
            ticker_stream = getattr(self.controller, "ticker_stream", None)
            if ticker_stream is not None and hasattr(ticker_stream, "get"):
                try:
                    ticker = ticker_stream.get(symbol)
                except Exception:
                    ticker = None
        if not isinstance(ticker, dict):
            return None

        candidates = []
        for key in ("price", "last", "close", "bid", "ask"):
            value = ticker.get(key)
            try:
                numeric = float(value)
            except Exception:
                continue
            if numeric > 0:
                candidates.append(numeric)
        if not candidates:
            return None
        if len(candidates) >= 2 and ticker.get("bid") is not None and ticker.get("ask") is not None:
            try:
                bid = float(ticker.get("bid"))
                ask = float(ticker.get("ask"))
                if bid > 0 and ask > 0:
                    return (bid + ask) / 2.0
            except Exception:
                pass
        return candidates[0]

    def _normalize_position_entry(self, raw):
        if raw is None:
            return None

        if isinstance(raw, dict):
            symbol = raw.get("symbol", "")
            side = raw.get("side", "")
            amount = raw.get("amount", raw.get("size", raw.get("quantity", raw.get("qty", 0))))
            entry = raw.get("entry_price", raw.get("avg_entry_price", raw.get("price", raw.get("avg_price", 0))))
            mark = raw.get("mark_price", raw.get("market_price"))
            pnl = raw.get("pnl", raw.get("unrealized_pnl", raw.get("unrealized_pl", raw.get("pl"))))
        else:
            symbol = getattr(raw, "symbol", "")
            side = getattr(raw, "side", "")
            amount = getattr(raw, "amount", getattr(raw, "size", getattr(raw, "quantity", getattr(raw, "qty", 0))))
            entry = getattr(raw, "entry_price", getattr(raw, "avg_entry_price", getattr(raw, "avg_price", getattr(raw, "price", 0))))
            mark = getattr(raw, "mark_price", getattr(raw, "market_price", None))
            pnl = getattr(raw, "pnl", getattr(raw, "unrealized_pnl", getattr(raw, "unrealized_pl", None)))

        try:
            amount = float(amount or 0)
        except Exception:
            amount = 0.0
        try:
            entry = float(entry or 0)
        except Exception:
            entry = 0.0
        try:
            mark = float(mark) if mark not in (None, "") else None
        except Exception:
            mark = None
        try:
            pnl = float(pnl) if pnl not in (None, "") else None
        except Exception:
            pnl = None

        normalized_symbol = str(symbol or "")
        if not normalized_symbol:
            return None

        normalized_side = str(side or "").lower()
        if not normalized_side:
            normalized_side = "long" if amount >= 0 else "short"
        abs_amount = abs(amount)

        if mark is None or mark <= 0:
            mark = self._lookup_symbol_mid_price(normalized_symbol)

        value = abs_amount * float(mark or entry or 0)
        if pnl is None and mark is not None and entry:
            direction = 1.0 if normalized_side != "short" else -1.0
            pnl = (float(mark) - entry) * abs_amount * direction

        return {
            "symbol": normalized_symbol,
            "side": normalized_side,
            "amount": abs_amount,
            "entry_price": entry,
            "mark_price": float(mark or 0),
            "value": value,
            "pnl": float(pnl or 0),
        }

    def _portfolio_positions_snapshot(self):
        portfolio = getattr(self.controller, "portfolio", None)
        positions = []
        if portfolio is None:
            return positions

        raw_positions = getattr(portfolio, "positions", {})
        if isinstance(raw_positions, dict):
            for symbol, pos in raw_positions.items():
                normalized = self._normalize_position_entry(
                    {
                        "symbol": symbol,
                        "amount": getattr(pos, "quantity", 0),
                        "entry_price": getattr(pos, "avg_price", 0),
                        "side": "long" if float(getattr(pos, "quantity", 0) or 0) >= 0 else "short",
                    }
                )
                if normalized is not None and normalized["amount"] > 0:
                    positions.append(normalized)
        return positions

    def _populate_positions_table(self, positions):
        table = getattr(self, "positions_table", None)
        if table is None:
            return

        normalized_positions = []
        for pos in positions or []:
            normalized = self._normalize_position_entry(pos)
            if normalized is not None and normalized["amount"] > 0:
                normalized_positions.append(normalized)

        normalized_positions.sort(key=lambda item: (item["symbol"], item["side"]))
        table.setRowCount(len(normalized_positions))

        for row, pos in enumerate(normalized_positions):
            values = [
                pos["symbol"],
                pos["side"].upper(),
                f"{pos['amount']:.6f}".rstrip("0").rstrip("."),
                f"{pos['entry_price']:.6f}".rstrip("0").rstrip("."),
                f"{pos['mark_price']:.6f}".rstrip("0").rstrip("."),
                f"{pos['value']:.2f}",
                f"{pos['pnl']:.2f}",
            ]
            for col, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col == 6:
                    item.setForeground(QColor("#32d296" if pos["pnl"] >= 0 else "#ef5350"))
                table.setItem(row, col, item)

        table.resizeColumnsToContents()
        table.horizontalHeader().setStretchLastSection(True)

    def _normalize_open_order_entry(self, order):
        if not isinstance(order, dict):
            return None

        symbol = str(order.get("symbol") or "").strip()
        if not symbol:
            return None

        side = str(order.get("side") or "").strip().lower()
        order_type = str(order.get("type") or order.get("order_type") or "").strip().lower()
        status = str(order.get("status") or "").strip().lower()

        amount = abs(float(order.get("amount") or order.get("qty") or order.get("size") or 0) or 0)
        filled = abs(float(order.get("filled") or order.get("filled_qty") or 0) or 0)
        remaining = max(amount - filled, 0.0)

        try:
            price = float(order.get("price")) if order.get("price") not in (None, "") else None
        except Exception:
            price = None
        if price is not None and price <= 0:
            price = None

        mark = self._lookup_symbol_mid_price(symbol)
        if mark is not None and mark <= 0:
            mark = None

        pnl = order.get("pnl")
        if pnl in (None, ""):
            pnl = order.get("unrealized_pnl", order.get("unrealizedPnl"))
        try:
            pnl = float(pnl) if pnl not in (None, "") else None
        except Exception:
            pnl = None

        if pnl is None and price is not None and mark is not None and remaining > 0:
            direction = -1.0 if side == "sell" else 1.0
            pnl = (float(mark) - float(price)) * remaining * direction

        return {
            "symbol": symbol,
            "side": side or "-",
            "type": order_type or "-",
            "price": price,
            "mark": mark,
            "amount": amount,
            "filled": filled,
            "remaining": remaining,
            "status": status or "-",
            "pnl": pnl,
            "order_id": str(order.get("id") or order.get("order_id") or ""),
        }

    def _populate_open_orders_table(self, orders):
        table = getattr(self, "open_orders_table", None)
        if table is None:
            return

        normalized_orders = []
        for order in orders or []:
            normalized = self._normalize_open_order_entry(order)
            if normalized is not None:
                normalized_orders.append(normalized)

        normalized_orders.sort(key=lambda item: (item["symbol"], item["status"], item["order_id"]))
        table.setRowCount(len(normalized_orders))

        for row, order in enumerate(normalized_orders):
            price_text = "-" if order["price"] is None else f"{order['price']:.6f}".rstrip("0").rstrip(".")
            mark_text = "-" if order["mark"] is None else f"{order['mark']:.6f}".rstrip("0").rstrip(".")
            pnl_value = order["pnl"]
            pnl_text = "-" if pnl_value is None else f"{float(pnl_value):.2f}"

            values = [
                order["symbol"],
                order["side"].upper(),
                order["type"].upper(),
                price_text,
                mark_text,
                f"{order['amount']:.6f}".rstrip("0").rstrip("."),
                f"{order['filled']:.6f}".rstrip("0").rstrip("."),
                f"{order['remaining']:.6f}".rstrip("0").rstrip("."),
                order["status"].replace("_", " ").upper(),
                pnl_text,
                order["order_id"],
            ]

            for col, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col == 8:
                    status_value = order["status"]
                    if "partial" in status_value:
                        item.setForeground(QColor("#f0a35e"))
                    elif status_value in {"open", "pending", "submitted", "accepted", "new"}:
                        item.setForeground(QColor("#65a3ff"))
                elif col == 9 and pnl_value is not None:
                    item.setForeground(QColor("#32d296" if float(pnl_value) >= 0 else "#ef5350"))
                table.setItem(row, col, item)

        table.resizeColumnsToContents()
        table.horizontalHeader().setStretchLastSection(True)

    async def _refresh_positions_async(self):
        if self._ui_shutting_down:
            return
        broker = getattr(self.controller, "broker", None)
        positions = []
        if broker is not None and hasattr(broker, "fetch_positions"):
            try:
                positions = await broker.fetch_positions()
            except Exception as exc:
                self.logger.debug("Positions refresh failed: %s", exc)

        if not positions:
            positions = self._portfolio_positions_snapshot()

        self._latest_positions_snapshot = positions or []
        self._populate_positions_table(self._latest_positions_snapshot)

    def _schedule_positions_refresh(self):
        task = getattr(self, "_positions_refresh_task", None)
        if task is not None and not task.done():
            return

        try:
            self._positions_refresh_task = asyncio.get_event_loop().create_task(self._refresh_positions_async())
        except Exception as exc:
            self.logger.debug("Unable to schedule positions refresh: %s", exc)

    async def _refresh_open_orders_async(self):
        if self._ui_shutting_down:
            return
        broker = getattr(self.controller, "broker", None)
        orders = []
        if broker is not None and hasattr(broker, "fetch_open_orders"):
            try:
                orders = await broker.fetch_open_orders(limit=200)
            except TypeError:
                orders = await broker.fetch_open_orders()
            except Exception as exc:
                self.logger.debug("Open orders refresh failed: %s", exc)

        self._latest_open_orders_snapshot = orders or []
        self._populate_open_orders_table(self._latest_open_orders_snapshot)

    def _schedule_open_orders_refresh(self):
        task = getattr(self, "_open_orders_refresh_task", None)
        if task is not None and not task.done():
            return

        try:
            self._open_orders_refresh_task = asyncio.get_event_loop().create_task(self._refresh_open_orders_async())
        except Exception as exc:
            self.logger.debug("Unable to schedule open-orders refresh: %s", exc)

    def _refresh_strategy_comparison_panel(self):
        table = getattr(self, "strategy_table", None)
        if table is None:
            return

        rows = []
        params = dict(getattr(self.controller, "strategy_params", {}) or {})
        active_name = Strategy.normalize_strategy_name(
            getattr(self.controller, "strategy_name", None) or getattr(getattr(self.controller, "config", None), "strategy", "Trend Following")
        )
        rows.append({
            "strategy": active_name,
            "source": "Active",
            "rsi_period": params.get("rsi_period", ""),
            "ema_fast": params.get("ema_fast", ""),
            "ema_slow": params.get("ema_slow", ""),
            "atr_period": params.get("atr_period", ""),
            "min_confidence": params.get("min_confidence", ""),
            "total_profit": "",
            "sharpe_ratio": "",
        })

        results = getattr(self, "optimization_results", None)
        if results is not None and hasattr(results, "empty") and not results.empty:
            for _, result in results.head(5).iterrows():
                rows.append({
                    "strategy": active_name,
                    "source": "Optimized",
                    "rsi_period": result.get("rsi_period", ""),
                    "ema_fast": result.get("ema_fast", ""),
                    "ema_slow": result.get("ema_slow", ""),
                    "atr_period": result.get("atr_period", ""),
                    "min_confidence": params.get("min_confidence", ""),
                    "total_profit": result.get("total_profit", ""),
                    "sharpe_ratio": result.get("sharpe_ratio", ""),
                })

        table.setRowCount(len(rows))
        for row_index, row in enumerate(rows):
            values = [
                row["strategy"],
                row["source"],
                row["rsi_period"],
                row["ema_fast"],
                row["ema_slow"],
                row["atr_period"],
                row["min_confidence"],
                row["total_profit"],
                row["sharpe_ratio"],
            ]
            for col, value in enumerate(values):
                if isinstance(value, float):
                    text = f"{value:.4f}" if col == 6 else f"{value:.2f}"
                else:
                    text = str(value)
                table.setItem(row_index, col, QTableWidgetItem(text))

        table.resizeColumnsToContents()
        table.horizontalHeader().setStretchLastSection(True)

    def _normalize_trade_log_entry(self, trade):
        if not isinstance(trade, dict):
            return None

        normalized = {
            "timestamp": trade.get("timestamp", ""),
            "symbol": trade.get("symbol", ""),
            "side": trade.get("side", ""),
            "price": trade.get("price", ""),
            "size": trade.get("size", trade.get("amount", "")),
            "order_type": trade.get("order_type", trade.get("type", "")),
            "status": trade.get("status", ""),
            "order_id": trade.get("order_id", trade.get("id", "")),
            "pnl": trade.get("pnl", ""),
            "stop_loss": trade.get("stop_loss", trade.get("sl", "")),
            "take_profit": trade.get("take_profit", trade.get("tp", "")),
        }
        return normalized

    def _format_trade_log_value(self, value):
        if value is None:
            return ""
        return str(value)

    def _trade_log_row_for_entry(self, entry):
        order_id = str(entry.get("order_id") or "").strip()
        if not order_id:
            return None

        for row in range(self.trade_log.rowCount()):
            item = self.trade_log.item(row, 7)
            if item is not None and item.text() == order_id:
                return row

        return None

    def _update_trade_log(self, trade):
        entry = self._normalize_trade_log_entry(trade)
        if entry is None:
            return

        row = self._trade_log_row_for_entry(entry)
        if row is None:
            row = self.trade_log.rowCount()

        if row == self.trade_log.rowCount() and row >= self.MAX_LOG_ROWS:
            self.trade_log.removeRow(0)
            row = self.trade_log.rowCount()

        if row == self.trade_log.rowCount():
            self.trade_log.insertRow(row)

        column_values = [
            entry["timestamp"],
            entry["symbol"],
            entry["side"],
            entry["price"],
            entry["size"],
            entry["order_type"],
            entry["status"],
            entry["order_id"],
            entry["pnl"],
        ]
        for column, value in enumerate(column_values):
            self.trade_log.setItem(row, column, QTableWidgetItem(self._format_trade_log_value(value)))

        tooltip_parts = []
        if entry["stop_loss"] not in ("", None):
            tooltip_parts.append(f"SL: {entry['stop_loss']}")
        if entry["take_profit"] not in ("", None):
            tooltip_parts.append(f"TP: {entry['take_profit']}")
        if tooltip_parts:
            tooltip = " | ".join(tooltip_parts)
            for column in range(self.trade_log.columnCount()):
                item = self.trade_log.item(row, column)
                if item is not None:
                    item.setToolTip(tooltip)

        self.trade_log.horizontalHeader().setStretchLastSection(True)
        self._refresh_performance_views()

    async def load_persisted_runtime_data(self):
        loader = getattr(self.controller, "_load_recent_trades", None)
        if loader is None:
            return

        try:
            trades = await loader(limit=min(int(self.MAX_LOG_ROWS or 200), 200))
        except Exception:
            self.logger.exception("Failed to load persisted trade history")
            return

        for trade in trades:
            self._update_trade_log(trade)

    def _update_ticker(self, symbol, bid, ask):
        if self._ui_shutting_down:
            return

        # Ensure symbol appears in the table even if symbols were not pre-populated.
        target_row = self._find_market_watch_row(symbol)

        if target_row is None:
            target_row = self.symbols_table.rowCount()
            self.symbols_table.insertRow(target_row)

        usd_column = self._market_watch_usd_column()
        usd_text = "-"
        if usd_column is not None:
            usd_value = self._stellar_usd_value(symbol, bid, ask)
            usd_text = self._format_market_watch_usd(usd_value)

        self._set_market_watch_row(
            target_row,
            symbol,
            bid=self._format_market_watch_number(bid),
            ask=self._format_market_watch_number(ask),
            status="Live",
            usd_value=usd_text,
        )
        if target_row == self.symbols_table.rowCount() - 1:
            self._reorder_market_watch_rows()

        try:
            mid = (float(bid) + float(ask)) / 2
        except Exception:
            mid = 0.0

        self.tick_prices.append(mid)

        if len(self.tick_prices) > 200:
            self.tick_prices.pop(0)

        self.tick_chart_curve.setData(self.tick_prices)

        # Push live price lines to matching chart tabs.
        for chart in self._iter_chart_widgets():
            if chart.symbol == symbol:
                chart.update_price_lines(bid=bid, ask=ask, last=mid)

    # ==========================================================
    # PANELS
    # ==========================================================

    def _create_market_watch_panel(self):
        dock = QDockWidget("Market Watch", self)
        self.symbols_table = QTableWidget()
        self._configure_market_watch_table()
        self.symbols_table.itemChanged.connect(self._handle_market_watch_item_changed)
        dock.setWidget(self.symbols_table)
        self.addDockWidget(Qt.LeftDockWidgetArea, dock)

        self.tick_chart = pg.PlotWidget()
        self.tick_chart_curve = self.tick_chart.plot(pen="y")
        self.tick_prices = []

        tick_dock = QDockWidget("Tick Chart", self)
        tick_dock.setWidget(self.tick_chart)
        self.addDockWidget(Qt.LeftDockWidgetArea, tick_dock)

    def _create_positions_panel(self):
        dock = QDockWidget("Positions", self)
        self.positions_table = QTableWidget()
        self.positions_table.setColumnCount(7)
        self.positions_table.setHorizontalHeaderLabels(
            ["Symbol", "Side", "Amount", "Entry", "Mark", "Value", "PnL"]
        )
        dock.setWidget(self.positions_table)
        self.addDockWidget(Qt.BottomDockWidgetArea, dock)

    def _create_open_orders_panel(self):
        dock = QDockWidget("Open Orders", self)
        self.open_orders_table = QTableWidget()
        self.open_orders_table.setColumnCount(11)
        self.open_orders_table.setHorizontalHeaderLabels(
            ["Symbol", "Side", "Type", "Price", "Mark", "Amount", "Filled", "Remaining", "Status", "PnL", "Order ID"]
        )
        dock.setWidget(self.open_orders_table)
        self.addDockWidget(Qt.BottomDockWidgetArea, dock)

    def _create_orderbook_panel(self):
        dock = QDockWidget("Orderbook", self)
        self.orderbook_panel = OrderBookPanel()
        dock.setWidget(self.orderbook_panel)
        self.addDockWidget(Qt.RightDockWidgetArea, dock)

    def _create_trade_log_panel(self):
        dock = QDockWidget("Trade Log", self)
        self.trade_log = QTableWidget()
        self.trade_log.setColumnCount(9)
        self.trade_log.setHorizontalHeaderLabels(
            ["Timestamp", "Symbol", "Side", "Price", "Size", "Order Type", "Status", "Order ID", "PnL"]
        )
        dock.setWidget(self.trade_log)
        self.addDockWidget(Qt.RightDockWidgetArea, dock)

    def _create_equity_panel(self):

        dock = QDockWidget("Equity Curve", self)

        container = QWidget()
        layout = QVBoxLayout()

        self.equity_summary_label = QLabel("Equity: 0.00")
        self.equity_summary_label.setStyleSheet("color: #dce7f8; font-size: 15px; font-weight: 700;")
        layout.addWidget(self.equity_summary_label)

        self.equity_chart = pg.PlotWidget()
        self._style_performance_plot(self.equity_chart, left_label="Equity")
        self.equity_curve = self.equity_chart.plot(pen="g")

        layout.addWidget(self.equity_chart)

        container.setLayout(layout)

        dock.setWidget(container)

        self.addDockWidget(Qt.RightDockWidgetArea, dock)

    def _show_system_status_panel(self):
        dock = getattr(self, "system_status_dock", None)
        if dock is None:
            return
        if dock.isVisible():
            dock.hide()
            return
        dock.show()
        dock.raise_()

    def _create_performance_panel(self):
        dock = QDockWidget("Performance", self)
        dock.setMinimumWidth(420)
        container = QWidget()
        container.setStyleSheet("background-color: #0b1220;")
        layout = QVBoxLayout(container)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        summary = QLabel("Performance snapshot will appear here as equity and realized trades accumulate.")
        summary.setWordWrap(True)
        summary.setStyleSheet(
            "color: #d9e6f7; background-color: #101a2d; border: 1px solid #20324d; "
            "border-radius: 12px; padding: 12px; font-size: 13px; font-weight: 600;"
        )
        layout.addWidget(summary)

        metric_names = [
            "Net PnL",
            "Return",
            "Max Drawdown",
            "Sharpe Ratio",
            "Win Rate",
            "Profit Factor",
            "Realized Trades",
            "Pending Orders",
        ]
        metrics_grid, metric_labels = self._build_performance_metric_grid(metric_names, columns=2)
        layout.addLayout(metrics_grid)

        plot = pg.PlotWidget()
        self._style_performance_plot(plot, left_label="Equity")
        plot.setMinimumHeight(180)
        curve = plot.plot(pen=pg.mkPen("#2a7fff", width=2.2))
        layout.addWidget(plot)

        insights = QTextBrowser()
        insights.setOpenExternalLinks(False)
        insights.setMaximumHeight(180)
        insights.setStyleSheet(
            "QTextBrowser { background-color: #101a2d; color: #d8e6ff; border: 1px solid #20324d; "
            "border-radius: 12px; padding: 8px; }"
        )
        layout.addWidget(insights)

        self._performance_panel_widgets = {
            "summary": summary,
            "metric_labels": metric_labels,
            "equity_curve": curve,
            "drawdown_curve": None,
            "insights": insights,
            "symbol_table": None,
        }

        container.setLayout(layout)
        dock.setWidget(container)
        self.addDockWidget(Qt.LeftDockWidgetArea, dock)

    def _build_performance_metric_grid(self, metric_names, columns=2):
        grid = QGridLayout()
        grid.setHorizontalSpacing(10)
        grid.setVerticalSpacing(10)
        labels = {}

        for index, name in enumerate(metric_names):
            frame = QFrame()
            frame.setStyleSheet(
                "QFrame { background-color: #101a2d; border: 1px solid #20324d; border-radius: 12px; }"
            )
            frame_layout = QVBoxLayout(frame)
            frame_layout.setContentsMargins(12, 10, 12, 10)
            frame_layout.setSpacing(4)

            title = QLabel(name)
            title.setStyleSheet("color: #8fa7c6; font-size: 11px; font-weight: 700; text-transform: uppercase;")
            value = QLabel("-")
            value.setStyleSheet("color: #e6edf7; font-size: 18px; font-weight: 700;")
            value.setWordWrap(True)

            frame_layout.addWidget(title)
            frame_layout.addWidget(value)

            row = index // columns
            column = index % columns
            grid.addWidget(frame, row, column)
            labels[name] = value

        return grid, labels

    def _style_performance_plot(self, plot, left_label=None):
        if plot is None:
            return
        plot.setBackground("#0b1220")
        plot.showGrid(x=True, y=True, alpha=0.18)
        plot.setMenuEnabled(False)
        plot.setMouseEnabled(x=False, y=False)
        axis_pen = pg.mkPen("#6f89ac")
        text_pen = pg.mkPen("#8fa7c6")
        for axis_name in ("left", "bottom"):
            axis = plot.getAxis(axis_name)
            axis.setPen(axis_pen)
            axis.setTextPen(text_pen)
        if left_label:
            plot.setLabel("left", left_label, color="#8fa7c6")
        plot.setLabel("bottom", "Samples", color="#8fa7c6")

    def _safe_float(self, value):
        try:
            numeric = float(value)
        except Exception:
            return None
        if not np.isfinite(numeric):
            return None
        return numeric

    def _format_currency(self, value):
        numeric = self._safe_float(value)
        if numeric is None:
            return "-"
        return f"{numeric:,.2f}"

    def _format_percent_text(self, value):
        numeric = self._safe_float(value)
        if numeric is None:
            return "-"
        return f"{numeric * 100.0:.2f}%"

    def _format_ratio_text(self, value):
        numeric = self._safe_float(value)
        if numeric is None:
            return "-"
        return f"{numeric:.2f}"

    def _performance_metric_style(self, tone):
        color_map = {
            "positive": "#32d296",
            "negative": "#ff6b6b",
            "warning": "#ffb84d",
            "muted": "#8fa7c6",
            "neutral": "#e6edf7",
        }
        color = color_map.get(tone, "#e6edf7")
        return f"color: {color}; font-size: 18px; font-weight: 700;"

    def _performance_trade_records(self):
        perf = getattr(self.controller, "performance_engine", None)
        source = []
        if perf is not None:
            trades = getattr(perf, "trades", None)
            if isinstance(trades, list):
                source = [trade for trade in trades if isinstance(trade, dict)]

        deduped = []
        keyed = {}
        anonymous_index = 0
        for trade in source:
            order_id = str(trade.get("order_id") or trade.get("id") or "").strip()
            if order_id:
                keyed[order_id] = dict(trade)
            else:
                anonymous_index += 1
                keyed[f"anon_{anonymous_index}"] = dict(trade)
        deduped = list(keyed.values())

        if deduped:
            return deduped

        fallback = []
        table = getattr(self, "trade_log", None)
        if table is None:
            return fallback
        for row in range(table.rowCount()):
            fallback.append(
                {
                    "timestamp": table.item(row, 0).text() if table.item(row, 0) else "",
                    "symbol": table.item(row, 1).text() if table.item(row, 1) else "",
                    "side": table.item(row, 2).text() if table.item(row, 2) else "",
                    "price": table.item(row, 3).text() if table.item(row, 3) else "",
                    "size": table.item(row, 4).text() if table.item(row, 4) else "",
                    "order_type": table.item(row, 5).text() if table.item(row, 5) else "",
                    "status": table.item(row, 6).text() if table.item(row, 6) else "",
                    "order_id": table.item(row, 7).text() if table.item(row, 7) else "",
                    "pnl": table.item(row, 8).text() if table.item(row, 8) else "",
                }
            )
        return fallback

    def _performance_snapshot(self):
        equity_series = []
        for value in self._performance_series():
            numeric = self._safe_float(value)
            if numeric is not None:
                equity_series.append(numeric)

        perf = getattr(self.controller, "performance_engine", None)
        report = {}
        if perf is not None and hasattr(perf, "report"):
            try:
                report = perf.report() or {}
            except Exception:
                report = {}

        initial_equity = equity_series[0] if equity_series else None
        latest_equity = equity_series[-1] if equity_series else None
        net_pnl = None
        if initial_equity is not None and latest_equity is not None:
            net_pnl = latest_equity - initial_equity

        drawdown_series = []
        current_drawdown = None
        max_drawdown = self._safe_float(report.get("max_drawdown"))
        if equity_series:
            equity_array = np.asarray(equity_series, dtype=float)
            running_peak = np.maximum.accumulate(equity_array)
            safe_peaks = np.where(running_peak == 0, 1.0, running_peak)
            drawdown_series = ((equity_array / safe_peaks) - 1.0).tolist()
            current_drawdown = abs(drawdown_series[-1]) if drawdown_series else 0.0
            if max_drawdown is None:
                max_drawdown = abs(min(drawdown_series))

        trades = self._performance_trade_records()
        trade_count = len(trades)
        pending_statuses = {"submitted", "open", "new", "pending", "partially_filled"}
        rejected_statuses = {"rejected", "failed", "error"}
        canceled_statuses = {"canceled", "cancelled"}
        pending_orders = 0
        rejected_orders = 0
        canceled_orders = 0
        symbol_stats = {}
        realized_pnls = []

        for trade in trades:
            status = str(trade.get("status") or "").strip().lower()
            symbol = str(trade.get("symbol") or "-").strip() or "-"
            pnl = self._safe_float(trade.get("pnl"))

            stats = symbol_stats.setdefault(
                symbol,
                {"symbol": symbol, "orders": 0, "realized": 0, "wins": 0, "pnl": 0.0}
            )
            stats["orders"] += 1

            if status in pending_statuses:
                pending_orders += 1
            elif status in rejected_statuses:
                rejected_orders += 1
            elif status in canceled_statuses:
                canceled_orders += 1

            if pnl is not None:
                realized_pnls.append({"symbol": symbol, "pnl": pnl})
                stats["realized"] += 1
                stats["pnl"] += pnl
                if pnl > 0:
                    stats["wins"] += 1

        realized_trade_count = len(realized_pnls)
        pnl_values = [entry["pnl"] for entry in realized_pnls]
        gross_profit = sum(value for value in pnl_values if value > 0)
        gross_loss = sum(value for value in pnl_values if value < 0)
        win_count = sum(1 for value in pnl_values if value > 0)
        loss_count = sum(1 for value in pnl_values if value < 0)
        win_rate = (win_count / realized_trade_count) if realized_trade_count else None
        avg_trade = (sum(pnl_values) / realized_trade_count) if realized_trade_count else None
        best_trade = max(pnl_values) if pnl_values else None
        worst_trade = min(pnl_values) if pnl_values else None
        if gross_loss < 0:
            profit_factor = gross_profit / abs(gross_loss)
        elif gross_profit > 0:
            profit_factor = float("inf")
        else:
            profit_factor = None

        symbol_rows = []
        for symbol, stats in symbol_stats.items():
            realized = int(stats["realized"])
            symbol_rows.append(
                {
                    "symbol": symbol,
                    "orders": int(stats["orders"]),
                    "realized": realized,
                    "win_rate": (stats["wins"] / realized) if realized else None,
                    "net_pnl": stats["pnl"],
                    "avg_pnl": (stats["pnl"] / realized) if realized else None,
                }
            )
        symbol_rows.sort(key=lambda item: (item["net_pnl"], item["realized"], item["orders"]), reverse=True)

        sharpe_ratio = self._safe_float(report.get("sharpe_ratio"))
        sortino_ratio = self._safe_float(report.get("sortino_ratio"))
        volatility = self._safe_float(report.get("volatility"))
        value_at_risk = self._safe_float(report.get("value_at_risk"))
        conditional_var = self._safe_float(report.get("conditional_var"))
        cumulative_return = self._safe_float(report.get("cumulative_return"))
        if cumulative_return is None and initial_equity not in (None, 0) and latest_equity is not None:
            cumulative_return = (latest_equity / initial_equity) - 1.0

        if realized_trade_count == 0 and len(equity_series) < 2:
            health = "Not enough data"
        elif (sharpe_ratio is not None and sharpe_ratio >= 1.0) and (max_drawdown is not None and max_drawdown <= 0.08):
            health = "Strong risk-adjusted performance"
        elif (net_pnl is not None and net_pnl >= 0) and (max_drawdown is None or max_drawdown <= 0.15):
            health = "Constructive but still developing"
        else:
            health = "Needs closer risk review"

        best_symbol = symbol_rows[0] if symbol_rows else None
        summary_bits = []
        if net_pnl is not None:
            summary_bits.append(f"net PnL {self._format_currency(net_pnl)}")
        if cumulative_return is not None:
            summary_bits.append(f"return {self._format_percent_text(cumulative_return)}")
        if max_drawdown is not None:
            summary_bits.append(f"max drawdown {self._format_percent_text(max_drawdown)}")
        if win_rate is not None:
            summary_bits.append(f"win rate {self._format_percent_text(win_rate)}")
        headline = health
        if summary_bits:
            headline = f"{health}. Current read: " + ", ".join(summary_bits[:4]) + "."

        insights = []
        if latest_equity is not None and initial_equity is not None:
            insights.append(
                f"Equity moved from <b>{self._format_currency(initial_equity)}</b> to <b>{self._format_currency(latest_equity)}</b>, for a net change of <b>{self._format_currency(net_pnl)}</b>."
            )
        if realized_trade_count:
            trade_quality = f"{realized_trade_count} realized trades"
            if win_rate is not None:
                trade_quality += f", <b>{self._format_percent_text(win_rate)}</b> win rate"
            if profit_factor is not None:
                profit_factor_text = "infinite" if profit_factor == float("inf") else self._format_ratio_text(profit_factor)
                trade_quality += f", profit factor <b>{profit_factor_text}</b>"
            insights.append(trade_quality + ".")
        else:
            insights.append("No realized PnL history yet, so trade-quality metrics are still warming up.")

        if best_symbol is not None and best_symbol.get("realized", 0) > 0:
            insights.append(
                f"Best contributing symbol so far is <b>{html.escape(best_symbol['symbol'])}</b> with {best_symbol['realized']} realized trades and <b>{self._format_currency(best_symbol['net_pnl'])}</b> net PnL."
            )

        execution_notes = []
        if pending_orders:
            execution_notes.append(f"{pending_orders} pending/open orders")
        if rejected_orders:
            execution_notes.append(f"{rejected_orders} rejected orders")
        if canceled_orders:
            execution_notes.append(f"{canceled_orders} canceled orders")
        if execution_notes:
            insights.append("Execution state: " + ", ".join(execution_notes) + ".")

        if max_drawdown is not None and max_drawdown >= 0.15:
            insights.append("Drawdown is elevated relative to the current sample; position sizing or strategy selectivity may need tightening.")
        elif max_drawdown is not None:
            insights.append("Drawdown remains contained relative to the current sample, which is a healthier sign than raw PnL alone.")

        metrics = {
            "Equity": {"text": self._format_currency(latest_equity), "tone": "neutral"},
            "Starting Equity": {"text": self._format_currency(initial_equity), "tone": "muted"},
            "Net PnL": {"text": self._format_currency(net_pnl), "tone": "positive" if (net_pnl or 0) > 0 else "negative" if (net_pnl or 0) < 0 else "neutral"},
            "Return": {"text": self._format_percent_text(cumulative_return), "tone": "positive" if (cumulative_return or 0) > 0 else "negative" if (cumulative_return or 0) < 0 else "neutral"},
            "Samples": {"text": str(len(equity_series)), "tone": "muted"},
            "Trades": {"text": str(trade_count), "tone": "muted"},
            "Realized Trades": {"text": str(realized_trade_count), "tone": "muted"},
            "Pending Orders": {"text": str(pending_orders), "tone": "warning" if pending_orders else "muted"},
            "Win Rate": {"text": self._format_percent_text(win_rate), "tone": "positive" if (win_rate or 0) >= 0.5 and win_rate is not None else "neutral"},
            "Profit Factor": {"text": "infinite" if profit_factor == float("inf") else self._format_ratio_text(profit_factor), "tone": "positive" if profit_factor not in (None, float("inf")) and profit_factor >= 1.2 else "neutral"},
            "Volatility": {"text": self._format_percent_text(volatility), "tone": "warning" if (volatility or 0) > 0.4 else "neutral"},
            "Sharpe Ratio": {"text": self._format_ratio_text(sharpe_ratio), "tone": "positive" if (sharpe_ratio or 0) >= 1.0 else "negative" if sharpe_ratio is not None and sharpe_ratio < 0 else "neutral"},
            "Sortino Ratio": {"text": self._format_ratio_text(sortino_ratio), "tone": "positive" if (sortino_ratio or 0) >= 1.0 else "negative" if sortino_ratio is not None and sortino_ratio < 0 else "neutral"},
            "Max Drawdown": {"text": self._format_percent_text(max_drawdown), "tone": "negative" if (max_drawdown or 0) >= 0.1 else "positive" if max_drawdown is not None else "neutral"},
            "Current Drawdown": {"text": self._format_percent_text(current_drawdown), "tone": "negative" if (current_drawdown or 0) >= 0.05 else "neutral"},
            "VaR (95%)": {"text": self._format_percent_text(value_at_risk), "tone": "warning" if value_at_risk is not None and value_at_risk < 0 else "neutral"},
            "CVaR (95%)": {"text": self._format_percent_text(conditional_var), "tone": "warning" if conditional_var is not None and conditional_var < 0 else "neutral"},
            "Best Trade": {"text": self._format_currency(best_trade), "tone": "positive" if best_trade is not None and best_trade > 0 else "neutral"},
            "Worst Trade": {"text": self._format_currency(worst_trade), "tone": "negative" if worst_trade is not None and worst_trade < 0 else "neutral"},
            "Avg Trade": {"text": self._format_currency(avg_trade), "tone": "positive" if avg_trade is not None and avg_trade > 0 else "negative" if avg_trade is not None and avg_trade < 0 else "neutral"},
        }

        return {
            "headline": headline,
            "insights": insights,
            "metrics": metrics,
            "equity_series": equity_series,
            "drawdown_series": [abs(value) for value in drawdown_series],
            "symbol_rows": symbol_rows[:8],
        }

    def _populate_performance_symbol_table(self, table, symbol_rows):
        if table is None:
            return
        table.setRowCount(0)
        for row_data in symbol_rows:
            row = table.rowCount()
            table.insertRow(row)
            values = [
                row_data.get("symbol", "-"),
                str(int(row_data.get("orders", 0) or 0)),
                str(int(row_data.get("realized", 0) or 0)),
                self._format_percent_text(row_data.get("win_rate")),
                self._format_currency(row_data.get("net_pnl")),
                self._format_currency(row_data.get("avg_pnl")),
            ]
            for column, value in enumerate(values):
                table.setItem(row, column, QTableWidgetItem(str(value)))
        table.horizontalHeader().setStretchLastSection(True)

    def _populate_performance_view(self, widgets, snapshot):
        if not widgets:
            return

        summary = widgets.get("summary")
        if summary is not None:
            summary.setText(snapshot.get("headline", "Performance snapshot unavailable."))

        metric_labels = widgets.get("metric_labels", {})
        for name, label in metric_labels.items():
            meta = snapshot.get("metrics", {}).get(name, {"text": "-", "tone": "neutral"})
            label.setText(meta.get("text", "-"))
            label.setStyleSheet(self._performance_metric_style(meta.get("tone", "neutral")))

        curve = widgets.get("equity_curve")
        if curve is not None:
            curve.setData(snapshot.get("equity_series", []))

        drawdown_curve = widgets.get("drawdown_curve")
        if drawdown_curve is not None:
            drawdown_curve.setData(snapshot.get("drawdown_series", []))

        insights = widgets.get("insights")
        if insights is not None:
            lines = "".join(f"<li>{line}</li>" for line in snapshot.get("insights", []))
            insights.setHtml(f"<ul style='margin-top:4px;'>{lines}</ul>")

        self._populate_performance_symbol_table(widgets.get("symbol_table"), snapshot.get("symbol_rows", []))

    def _refresh_performance_views(self):
        snapshot = self._performance_snapshot()
        panel_widgets = getattr(self, "_performance_panel_widgets", None)
        if panel_widgets:
            self._populate_performance_view(panel_widgets, snapshot)

        window = getattr(self, "detached_tool_windows", {}).get("performance_analytics")
        if self._is_qt_object_alive(window):
            self._populate_performance_view(getattr(window, "_performance_widgets", None), snapshot)

    def _create_strategy_comparison(self):
        dock = QDockWidget("Strategy Comparison", self)
        self.strategy_table = QTableWidget()
        self.strategy_table.setColumnCount(9)
        self.strategy_table.setHorizontalHeaderLabels(
            ["Strategy", "Source", "RSI", "EMA Fast", "EMA Slow", "ATR", "Min Conf", "Profit", "Sharpe"]
        )
        dock.setWidget(self.strategy_table)
        self.addDockWidget(Qt.BottomDockWidgetArea, dock)

    # ==========================================================
    # BACKTEST
    # ==========================================================

    async def run_backtest_clicked(self):

     try:
      if self.controller.orchestrator:
        # Initialize backtest engine
        self.backtest_engine = BacktestEngine(
            strategy=self.controller.orchestrator,
            data=self.historical_data,
            initial_capital=self.controller.initial_capital,
            slippage=self.controller.slippage,
            commission=self.controller.commission
        )

        # Buttons
        start_btn = QPushButton("Start Backtest")
        stop_btn = QPushButton("Stop Backtest")

        start_btn.clicked.connect(self.start_backtest)
        stop_btn.clicked.connect(self.stop_backtest)

        # Layout
        layout = QVBoxLayout()
        layout.addWidget(start_btn)
        layout.addWidget(stop_btn)

        # Backtest widget
        backtest_widget = QWidget()
        backtest_widget.setLayout(layout)

        # Dock widget
        self.backtest_dock = QDockWidget("Backtest Results", self)
        self.backtest_dock.setWidget(backtest_widget)

        self.addDockWidget(Qt.RightDockWidgetArea, self.backtest_dock)
      else:

          raise RuntimeError(
              "Please start trading first"
          )

     except Exception as e:

        self.system_console.log(
            f"Backtest initialization error: {e.__str__()}",
            "ERROR"
        )






    # ==========================================================
    # REPORT
    # ==========================================================

    def _generate_report(self):
        generator = ReportGenerator(
            trades=self.controller.performance_engine.trades,
            equity_history=self.controller.performance_engine.equity_history
        )
        generator.export_pdf()
        generator.export_excel()
        self.system_console.log("Report Generated", "INFO")

    # ==========================================================
    # SCREENSHOT
    # ==========================================================

    def take_screen_shot(self):
        pixmap = self.grab()
        timestamp = QDateTime.currentDateTime().toString("yyyyMMdd_hhmmss")
        filename = f"Sopotek_Screenshot_{timestamp}.png"

        path, _ = QFileDialog.getSaveFileName(
            self, "Save Screenshot", filename, "PNG Files (*.png)"
        )

        if path:
            pixmap.save(path, "PNG")
            self.system_console.log("Screenshot saved", "INFO")




    #######################################################
    # Start BackTesting
    #######################################################
    def start_backtest(self):
      self.results= engine.run()









    # ==========================================================
    # SETTINGS
    # ==========================================================

    def closeEvent(self, event):
        self._ui_shutting_down = True

        # Stop periodic timers to prevent callbacks while widgets are tearing down.
        try:
            if hasattr(self, "refresh_timer") and self.refresh_timer is not None:
                self.refresh_timer.stop()
            if hasattr(self, "orderbook_timer") and self.orderbook_timer is not None:
                self.orderbook_timer.stop()
            if hasattr(self, "spinner_timer") and self.spinner_timer is not None:
                self.spinner_timer.stop()
        except Exception:
            pass

        try:
            self._disconnect_controller_signals()
            self._safe_disconnect(self.ai_signal, self._update_ai_signal)
        except Exception:
            pass

        self.settings.setValue("geometry", self.saveGeometry())
        self.settings.setValue("windowState", self.saveState())
        self.settings.setValue("chart/candle_up_color", self.candle_up_color)
        self.settings.setValue("chart/candle_down_color", self.candle_down_color)
        super().closeEvent(event)

    def _restore_settings(self):
        geometry = self.settings.value("geometry")
        if geometry:
            self.restoreGeometry(geometry)

        state = self.settings.value("windowState")
        if state:
            self.restoreState(state)

        self._apply_candle_colors_to_all_charts()

    def _apply_candle_colors_to_all_charts(self):
        for chart in self._iter_chart_widgets():
            chart.set_candle_colors(self.candle_up_color, self.candle_down_color)

    def _choose_candle_colors(self):
        up = QColorDialog.getColor(QColor(self.candle_up_color), self, "Select Bullish Candle Color")
        if not up.isValid():
            return

        down = QColorDialog.getColor(QColor(self.candle_down_color), self, "Select Bearish Candle Color")
        if not down.isValid():
            return

        self.candle_up_color = up.name()
        self.candle_down_color = down.name()

        self.settings.setValue("chart/candle_up_color", self.candle_up_color)
        self.settings.setValue("chart/candle_down_color", self.candle_down_color)

        self._apply_candle_colors_to_all_charts()

    def _add_indicator_to_current_chart(self):
        chart = self._current_chart_widget()
        if not isinstance(chart, ChartWidget):
            QMessageBox.warning(self, "Chart", "Select a chart first.")
            return

        options = [
            "Moving Average",
            "EMA",
            "SMMA",
            "LWMA",
            "VWAP",
            "Fibonacci Retracement",
            "ADX",
            "ATR",
            "Bollinger Bands",
            "Envelopes",
            "Ichimoku",
            "Parabolic SAR",
            "Standard Deviation",
            "Accelerator Oscillator",
            "Awesome Oscillator",
            "CCI",
            "DeMarker",
            "MACD",
            "Momentum",
            "OsMA",
            "RSI",
            "RVI",
            "Stochastic Oscillator",
            "Williams' Percent Range",
            "Accumulation/Distribution",
            "Money Flow Index",
            "On Balance Volume",
            "Volumes",
            "Alligator",
            "Fractal",
            "Gator Oscillator",
            "Market Facilitation Index",
            "Bulls Power",
            "Bears Power",
            "Force Index",
            "Donchian Channel",
            "Keltner Channel",
            "ZigZag",
        ]
        indicator, ok = QInputDialog.getItem(
            self,
            "Add Indicator",
            "Indicator:",
            options,
            0,
            False,
        )
        if not ok or not indicator:
            return

        default_period_map = {
            "Moving Average": 14,
            "EMA": 14,
            "SMMA": 14,
            "LWMA": 14,
            "VWAP": 20,
            "Fibonacci Retracement": 120,
            "ADX": 14,
            "ATR": 14,
            "Bollinger Bands": 20,
            "Envelopes": 14,
            "Standard Deviation": 20,
            "CCI": 14,
            "DeMarker": 14,
            "Momentum": 14,
            "RSI": 14,
            "RVI": 10,
            "Stochastic Oscillator": 14,
            "Williams' Percent Range": 14,
            "Money Flow Index": 14,
            "Force Index": 13,
            "Donchian Channel": 20,
            "Keltner Channel": 20,
            "Fractal": 5,
            "ZigZag": 12,
        }
        fixed_default_indicators = {
            "Ichimoku",
            "Parabolic SAR",
            "Accelerator Oscillator",
            "Awesome Oscillator",
            "MACD",
            "OsMA",
            "Accumulation/Distribution",
            "On Balance Volume",
            "Volumes",
            "Alligator",
            "Gator Oscillator",
            "Market Facilitation Index",
            "Bulls Power",
            "Bears Power",
        }
        period = default_period_map.get(indicator, 20)
        if indicator not in fixed_default_indicators:
            period, ok = QInputDialog.getInt(
                self,
                "Indicator Period",
                "Period:",
                period,
                2,
                500,
                1,
            )
            if not ok:
                return

        key = chart.add_indicator(indicator, period)
        if key is None:
            QMessageBox.warning(self, "Indicator", "Unsupported indicator.")
            return

        # Force redraw using existing candle cache for this symbol/timeframe.
        asyncio.get_event_loop().create_task(self._reload_chart_data(chart.symbol, chart.timeframe))

    def _update_orderbook(self, symbol, bids, asks):
        if self._ui_shutting_down:
            return

        active_symbol = self._current_chart_symbol()

        if hasattr(self, "orderbook_panel") and active_symbol == symbol:
            self.orderbook_panel.update_orderbook(bids, asks)

        for chart in self._iter_chart_widgets():
            if chart.symbol == symbol:
                chart.update_orderbook_heatmap(bids, asks)

    def _create_strategy_debug_panel(self):

        dock = QDockWidget("Strategy Debug", self)

        self.debug_table = QTableWidget()
        self.debug_table.setColumnCount(7)
        self.debug_table.setHorizontalHeaderLabels([
            "Index", "Signal", "RSI",
            "EMA Fast", "EMA Slow",
            "ML Prob", "Reason"
        ])

        dock.setWidget(self.debug_table)
        self.addDockWidget(Qt.RightDockWidgetArea, dock)

    def _handle_strategy_debug(self, debug):
        if self._ui_shutting_down:
            return

        if debug is None:
            print("DEBUG IS NONE")
            return

        row = self.debug_table.rowCount()
        self.debug_table.insertRow(row)

        self.debug_table.setItem(row, 0, QTableWidgetItem(str(debug["index"])))
        self.debug_table.setItem(row, 1, QTableWidgetItem(debug["signal"]))
        self.debug_table.setItem(row, 2, QTableWidgetItem(str(debug["rsi"])))
        self.debug_table.setItem(row, 3, QTableWidgetItem(str(debug["ema_fast"])))
        self.debug_table.setItem(row, 4, QTableWidgetItem(str(debug["ema_slow"])))
        self.debug_table.setItem(row, 5, QTableWidgetItem(str(debug["ml_probability"])))
        self.debug_table.setItem(row, 6, QTableWidgetItem(debug["reason"]))

        # Add to chart
        for chart in self._iter_chart_widgets():
            if chart.symbol == debug["symbol"]:
                chart.add_strategy_signal(
                    debug["index"],
                    debug.get("price", debug["ema_fast"]),
                    debug["signal"]
                )

    def _update_training_status(self, symbol, status):
        status_column = self._market_watch_status_column()

        for row in range(self.symbols_table.rowCount()):
            symbol_item = self.symbols_table.item(row, self._market_watch_symbol_column())
            if symbol_item is not None and symbol_item.text() == symbol:

                if status == "training":
                    item = QTableWidgetItem("⏳ Training...")
                    item.setForeground(QColor("yellow"))
                    icon = self._spinner_frames[self._spinner_index % 2]
                    self._spinner_index += 1

                    item = QTableWidgetItem(f"{icon} Training...")
                    item.setForeground(QColor("yellow"))

                elif status == "ready":
                    item = QTableWidgetItem("🟢 Ready")
                    item.setForeground(QColor("yellow"))

                elif status == "error":
                    item = QTableWidgetItem("🔴 Error")
                    item.setForeground(QColor("red"))
                else:
                    item = QTableWidgetItem(status)

                self.symbols_table.setItem(row, status_column, item)
                break

    def _rotate_spinner(self):

     try:
        self._update_trading_activity_indicator()

        # Lightweight spinner update: only touch existing rows that are in training state.
        if not hasattr(self, "symbols_table") or self.symbols_table is None:
            return

        self._spinner_index += 1
        icon = self._spinner_frames[self._spinner_index % len(self._spinner_frames)]

        rows = self.symbols_table.rowCount()
        status_column = self._market_watch_status_column()

        for row in range(rows):
            status_item = self.symbols_table.item(row, status_column)

            if not status_item:
                continue

            text = status_item.text() or ""

            if "Training" in text or "?" in text or "?" in text:
                status_item.setText(f"{icon} Training...")
                status_item.setForeground(QColor("yellow"))

     except Exception as e:

        self.logger.error(e)

    def _connect_signals(self):

        self.controller.candle_signal.connect(self._update_chart)
        self.controller.equity_signal.connect(self._update_equity)
        self.controller.trade_signal.connect(self._update_trade_log)
        self.controller.ticker_signal.connect(self._update_ticker)

        self.controller.orderbook_signal.connect(
            self._update_orderbook
        )

        if hasattr(self.controller, "ai_signal_monitor"):
            self.controller.ai_signal_monitor.connect(self._update_ai_signal)

        self.controller.strategy_debug_signal.connect(self._handle_strategy_debug)

        self.controller.training_status_signal.connect(
            self._update_training_status
        )

    def _setup_panels(self):

        self.system_console = SystemConsole()

        console_dock = QDockWidget("System Console", self)
        console_dock.setWidget(self.system_console)

        self.addDockWidget(
            Qt.BottomDockWidgetArea,
            console_dock
        )

        self._create_market_watch_panel()
        self._create_orderbook_panel()
        self._create_positions_panel()
        self._create_open_orders_panel()
        self._create_trade_log_panel()
        self._create_performance_panel()
        self._create_strategy_comparison()
        self._create_strategy_debug_panel()
        self._create_system_status_panel()
        self._create_risk_heatmap()
        self._create_ai_signal_panel()

    def _current_chart_symbol(self):
        chart = self._current_chart_widget()
        if chart is not None:
            return chart.symbol
        return getattr(self, "symbol", None)

    def _request_active_orderbook(self):
        if self._ui_shutting_down:
            return

        symbol = self._current_chart_symbol()
        if not symbol or not hasattr(self.controller, "request_orderbook"):
            return

        asyncio.get_event_loop().create_task(
            self.controller.request_orderbook(symbol=symbol, limit=20)
        )

    def _setup_spinner(self):

        self._spinner_frames = ["⏳", "⌛"]
        self._spinner_index = 0

        self.spinner_timer = QTimer()
        self.spinner_timer.timeout.connect(self._rotate_spinner)

        self.spinner_timer.start(500)

    def _update_symbols(self, exchange, symbols):

        blocked = self.symbols_table.blockSignals(True)
        self.symbols_table.setRowCount(0)
        self.symbols_table.setAccessibleName(exchange)
        self._configure_market_watch_table()
        if self.symbol_picker is not None:
            current_symbol = self.symbol_picker.currentText()
            self.symbol_picker.blockSignals(True)
            self.symbol_picker.clear()
            self.symbol_picker.addItems(symbols)
            if current_symbol in symbols:
                self.symbol_picker.setCurrentText(current_symbol)
            elif symbols:
                self.symbol_picker.setCurrentIndex(0)
            self.symbol_picker.blockSignals(False)

        for symbol in symbols:
            row = self.symbols_table.rowCount()
            self.symbols_table.insertRow(row)
            self._set_market_watch_row(row, symbol, bid="-", ask="-", status="⏳", usd_value="-")
        self.symbols_table.blockSignals(blocked)
        self._reorder_market_watch_rows()

    def _open_manual_trade(self):
        if not getattr(self.controller, "broker", None):
            QMessageBox.warning(self, "Manual Order", "Connect a broker before placing an order.")
            return

        symbol_options = list(getattr(self.controller, "symbols", []) or [])
        default_symbol = self._current_chart_symbol() or getattr(self, "symbol", None) or ""
        if default_symbol and default_symbol not in symbol_options:
            symbol_options.insert(0, default_symbol)
        if not symbol_options:
            symbol_options = [default_symbol] if default_symbol else []

        if symbol_options:
            default_index = max(symbol_options.index(default_symbol), 0) if default_symbol in symbol_options else 0
            symbol, ok = QInputDialog.getItem(
                self,
                "Manual Order",
                "Symbol:",
                symbol_options,
                default_index,
                True,
            )
            if not ok:
                return
            symbol = str(symbol).strip()
        else:
            symbol, ok = QInputDialog.getText(self, "Manual Order", "Symbol:")
            if not ok:
                return
            symbol = str(symbol).strip()

        if not symbol:
            QMessageBox.warning(self, "Manual Order", "A symbol is required.")
            return

        side, ok = QInputDialog.getItem(
            self,
            "Manual Order",
            "Side:",
            ["buy", "sell"],
            0,
            False,
        )
        if not ok:
            return

        order_type, ok = QInputDialog.getItem(
            self,
            "Manual Order",
            "Order Type:",
            ["market", "limit"],
            0,
            False,
        )
        if not ok:
            return

        amount, ok = QInputDialog.getDouble(
            self,
            "Manual Order",
            "Amount:",
            1.0,
            0.0,
            1_000_000_000.0,
            8,
        )
        if not ok:
            return

        price = None
        if order_type == "limit":
            price, ok = QInputDialog.getDouble(
                self,
                "Manual Order",
                "Limit Price:",
                0.0,
                0.0,
                1_000_000_000.0,
                8,
            )
            if not ok:
                return
            if price <= 0:
                QMessageBox.warning(self, "Manual Order", "Limit orders require a positive price.")
                return

        stop_loss, ok = QInputDialog.getDouble(
            self,
            "Manual Order",
            "Stop Loss (0 to skip):",
            0.0,
            0.0,
            1_000_000_000.0,
            8,
        )
        if not ok:
            return

        take_profit, ok = QInputDialog.getDouble(
            self,
            "Manual Order",
            "Take Profit (0 to skip):",
            0.0,
            0.0,
            1_000_000_000.0,
            8,
        )
        if not ok:
            return

        asyncio.get_event_loop().create_task(
            self._submit_manual_trade(
                symbol=symbol,
                side=side,
                amount=amount,
                order_type=order_type,
                price=price,
                stop_loss=stop_loss or None,
                take_profit=take_profit or None,
            )
        )

    def _optimize_strategy(self):
        self._open_text_window(
            "strategy_optimization",
            "Strategy Optimization",
            """
            <h2>Strategy Optimization</h2>
            <p>This workspace is reserved for parameter sweeps and strategy comparison.</p>
            <p>Current chart timeframe: <b>{}</b></p>
            <p>Loaded symbols: <b>{}</b></p>
            <p>Optimization controls can be added here next without changing the main terminal layout.</p>
            """.format(self.current_timeframe, len(getattr(self.controller, "symbols", []))),
            width=680,
            height=420,
        )

    def _get_or_create_tool_window(self, key, title, width=900, height=560):
        window = self.detached_tool_windows.get(key)

        if window is not None:
            window.showNormal()
            window.raise_()
            window.activateWindow()
            return window

        window = QMainWindow(self)
        window.setWindowFlag(Qt.WindowType.Window, True)
        window.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
        window.setWindowTitle(title)
        window.resize(width, height)
        window.destroyed.connect(
            lambda *_: self.detached_tool_windows.pop(key, None)
        )

        self.detached_tool_windows[key] = window
        return window

    def _clone_table_widget(self, source, target):
        target.clear()
        target.setColumnCount(source.columnCount())
        target.setRowCount(source.rowCount())

        headers = []
        for col in range(source.columnCount()):
            header_item = source.horizontalHeaderItem(col)
            headers.append(header_item.text() if header_item else f"Column {col + 1}")
        target.setHorizontalHeaderLabels(headers)

        for row in range(source.rowCount()):
            for col in range(source.columnCount()):
                source_item = source.item(row, col)
                if source_item is None:
                    continue
                target.setItem(row, col, source_item.clone())

        target.resizeColumnsToContents()
        target.horizontalHeader().setStretchLastSection(True)

    def _sync_logs_window(self, editor):
        source_text = self.system_console.console.toPlainText()
        if editor.toPlainText() == source_text:
            return

        editor.setPlainText(source_text)
        editor.moveCursor(QTextCursor.MoveOperation.End)

    def _open_logs(self):
        window = self._get_or_create_tool_window(
            "system_logs",
            "System Logs",
            width=980,
            height=620,
        )

        editor = getattr(window, "_logs_editor", None)
        if editor is None:
            editor = QTextEdit()
            editor.setReadOnly(True)
            editor.setStyleSheet(self.system_console.console.styleSheet())
            window.setCentralWidget(editor)
            window._logs_editor = editor

            sync_timer = QTimer(window)
            sync_timer.timeout.connect(lambda: self._sync_logs_window(editor))
            sync_timer.start(700)
            window._sync_timer = sync_timer

        self._sync_logs_window(editor)
        window.show()
        window.raise_()
        window.activateWindow()

    def _open_ml_monitor(self):
        window = self._get_or_create_tool_window(
            "ml_monitor",
            "ML Signal Monitor",
            width=880,
            height=520,
        )

        table = getattr(window, "_monitor_table", None)
        if table is None:
            table = QTableWidget()
            table.setAlternatingRowColors(True)
            window.setCentralWidget(table)
            window._monitor_table = table

            sync_timer = QTimer(window)
            sync_timer.timeout.connect(
                lambda: self._clone_table_widget(self.ai_table, table)
            )
            sync_timer.start(900)
            window._sync_timer = sync_timer

        self._clone_table_widget(self.ai_table, table)
        window.show()
        window.raise_()
        window.activateWindow()

    def _open_text_window(self, key, title, html, width=760, height=520):
        window = self._get_or_create_tool_window(key, title, width=width, height=height)

        browser = getattr(window, "_browser", None)
        if browser is None:
            browser = QTextBrowser()
            browser.setOpenExternalLinks(True)
            browser.setStyleSheet(
                "QTextBrowser { background-color: #0b1220; color: #e6edf7; padding: 16px; }"
            )
            window.setCentralWidget(browser)
            window._browser = browser

        browser.setHtml(html)
        window.show()
        window.raise_()
        window.activateWindow()
        return window

    def _format_backtest_timestamp(self, value):
        if value in (None, ""):
            return "-"

        try:
            numeric = float(value)
            if numeric > 1e12:
                numeric /= 1000.0
            return QDateTime.fromSecsSinceEpoch(int(numeric)).toString("yyyy-MM-dd HH:mm")
        except Exception:
            return str(value)

    def _format_backtest_range(self, dataset):
        if dataset is None or not hasattr(dataset, "__len__") or len(dataset) == 0:
            return "-"

        try:
            start_value = dataset.iloc[0]["timestamp"]
            end_value = dataset.iloc[-1]["timestamp"]
        except Exception:
            try:
                start_value = dataset[0][0]
                end_value = dataset[-1][0]
            except Exception:
                return "-"

        return f"{self._format_backtest_timestamp(start_value)} -> {self._format_backtest_timestamp(end_value)}"

    def _append_backtest_journal(self, message, level="INFO"):
        lines = list(getattr(self, "_backtest_journal_lines", []) or [])
        timestamp = QDateTime.currentDateTime().toString("yyyy-MM-dd HH:mm:ss")
        lines.append(f"[{timestamp}] {level.upper()}: {message}")
        self._backtest_journal_lines = lines[-300:]
        self._refresh_backtest_window()

    def _populate_backtest_results_table(self, table, trades_df):
        headers = ["Time", "Symbol", "Side", "Type", "Price", "Amount", "PnL", "Equity", "Reason"]
        table.clear()
        table.setColumnCount(len(headers))
        table.setHorizontalHeaderLabels(headers)

        if trades_df is None or getattr(trades_df, "empty", True):
            table.setRowCount(0)
            return

        table.setRowCount(len(trades_df))

        for row_index, (_idx, row) in enumerate(trades_df.iterrows()):
            values = [
                self._format_backtest_timestamp(row.get("timestamp")),
                row.get("symbol", "-"),
                row.get("side", "-"),
                row.get("type", "-"),
                f"{float(row.get('price', 0) or 0):.6f}",
                f"{float(row.get('amount', 0) or 0):.6f}",
                f"{float(row.get('pnl', 0) or 0):.2f}",
                f"{float(row.get('equity', 0) or 0):.2f}",
                row.get("reason", ""),
            ]
            for column, value in enumerate(values):
                table.setItem(row_index, column, QTableWidgetItem(str(value)))

        table.resizeColumnsToContents()
        table.horizontalHeader().setStretchLastSection(True)

    def _build_backtest_report_text(self, context, report, trades_df):
        symbol = context.get("symbol", "-")
        timeframe = context.get("timeframe", "-")
        strategy_name = context.get("strategy_name") or getattr(self.controller, "strategy_name", None) or getattr(getattr(self.controller, "config", None), "strategy", "Trend Following")
        candle_count = len(context.get("data")) if hasattr(context.get("data"), "__len__") else 0
        initial_deposit = float(getattr(self.controller, "initial_capital", 10000) or 10000)
        spread_pct = float(getattr(self.controller, "spread_pct", 0.0) or 0.0)
        equity_curve = getattr(getattr(self, "backtest_engine", None), "equity_curve", []) or []

        report = report or {}
        total_profit = float(report.get("total_profit", 0.0) or 0.0)
        total_trades = int(report.get("total_trades", 0) or 0)
        closed_trades = int(report.get("closed_trades", 0) or 0)
        win_rate = float(report.get("win_rate", 0.0) or 0.0) * 100.0
        avg_profit = float(report.get("avg_profit", 0.0) or 0.0)
        max_drawdown = float(report.get("max_drawdown", 0.0) or 0.0)
        final_equity = float(report.get("final_equity", initial_deposit) or initial_deposit)

        gross_profit = 0.0
        gross_loss = 0.0
        if trades_df is not None and not getattr(trades_df, "empty", True) and "pnl" in trades_df:
            pnl_series = trades_df["pnl"].fillna(0).astype(float)
            gross_profit = float(pnl_series[pnl_series > 0].sum())
            gross_loss = float(pnl_series[pnl_series < 0].sum())

        profit_factor = gross_profit / abs(gross_loss) if gross_loss < 0 else (gross_profit if gross_profit > 0 else 0.0)
        bars = len(equity_curve) if equity_curve else candle_count

        lines = [
            "Strategy Tester Report",
            "",
            f"Expert: {strategy_name}",
            f"Symbol: {symbol}",
            f"Period: {timeframe}",
            "Model: Bar-close simulation",
            f"Spread: {spread_pct:.4f}%",
            f"Initial Deposit: {initial_deposit:.2f}",
            f"Bars in Test: {bars}",
            f"Range: {self._format_backtest_range(context.get('data'))}",
            "",
            f"Total Net Profit: {total_profit:.2f}",
            f"Gross Profit: {gross_profit:.2f}",
            f"Gross Loss: {gross_loss:.2f}",
            f"Profit Factor: {profit_factor:.2f}",
            f"Expected Payoff: {avg_profit:.2f}",
            f"Max Drawdown: {max_drawdown:.2f}",
            f"Total Trades: {total_trades}",
            f"Closed Trades: {closed_trades}",
            f"Win Rate: {win_rate:.2f}%",
            f"Final Equity: {final_equity:.2f}",
        ]
        return "\n".join(lines)

    def _show_backtest_window(self):
        window = self._get_or_create_tool_window(
            "backtesting_workspace",
            "Strategy Tester",
            width=1180,
            height=760,
        )

        if getattr(window, "_backtest_container", None) is None:
            container = QWidget()
            layout = QVBoxLayout(container)

            status = QLabel("Strategy tester ready.")
            status.setStyleSheet("color: #e6edf7; font-weight: 700; font-size: 14px;")
            layout.addWidget(status)

            summary = QLabel("-")
            summary.setWordWrap(True)
            summary.setStyleSheet("color: #9fb0c7;")
            layout.addWidget(summary)

            settings_frame = QFrame()
            settings_frame.setStyleSheet(
                "QFrame { background-color: #101b2d; border: 1px solid #24344f; border-radius: 10px; }"
                "QLabel { color: #d7dfeb; }"
            )
            settings_layout = QGridLayout(settings_frame)
            settings_layout.setContentsMargins(14, 12, 14, 12)
            settings_layout.setHorizontalSpacing(16)
            settings_layout.setVerticalSpacing(8)

            setting_names = [
                "Expert",
                "Symbol",
                "Period",
                "Model",
                "Spread",
                "Initial Deposit",
                "Bars",
                "Range",
            ]
            setting_labels = {}
            for index, name in enumerate(setting_names):
                title = QLabel(name)
                title.setStyleSheet("color: #8fa3bf; font-weight: 700;")
                value = QLabel("-")
                value.setStyleSheet("color: #f4f8ff; font-weight: 600;")
                row = index // 4
                col = (index % 4) * 2
                settings_layout.addWidget(title, row, col)
                settings_layout.addWidget(value, row, col + 1)
                setting_labels[name] = value
            layout.addWidget(settings_frame)

            controls = QHBoxLayout()
            toggle_btn = QPushButton("Start Backtest")
            report_btn = QPushButton("Generate Report")
            toggle_btn.clicked.connect(self.start_backtest)
            report_btn.clicked.connect(self._generate_report)
            controls.addWidget(toggle_btn)
            controls.addWidget(report_btn)
            controls.addStretch()
            layout.addLayout(controls)

            metrics_frame = QFrame()
            metrics_frame.setStyleSheet(
                "QFrame { background-color: #0f1727; border: 1px solid #24344f; border-radius: 10px; }"
            )
            metrics_layout = QGridLayout(metrics_frame)
            metrics_layout.setContentsMargins(12, 10, 12, 10)
            metrics_layout.setHorizontalSpacing(18)
            metrics_layout.setVerticalSpacing(6)

            metric_names = [
                "Total Net Profit",
                "Trades",
                "Win Rate",
                "Max Drawdown",
                "Final Equity",
            ]
            metric_labels = {}
            for index, name in enumerate(metric_names):
                title = QLabel(name)
                title.setStyleSheet("color: #8fa3bf; font-weight: 700;")
                value = QLabel("-")
                value.setStyleSheet("color: #f5fbff; font-weight: 700; font-size: 16px;")
                metrics_layout.addWidget(title, 0, index)
                metrics_layout.addWidget(value, 1, index)
                metric_labels[name] = value
            layout.addWidget(metrics_frame)

            tabs = QTabWidget()

            results_table = QTableWidget()
            results_table.setAlternatingRowColors(True)
            tabs.addTab(results_table, "Results")

            graph_tab = QWidget()
            graph_layout = QVBoxLayout(graph_tab)
            graph_layout.setContentsMargins(8, 8, 8, 8)
            graph_plot = pg.PlotWidget()
            graph_plot.setBackground("#0b1220")
            graph_plot.showGrid(x=True, y=True, alpha=0.2)
            graph_plot.setLabel("left", "Equity")
            graph_plot.setLabel("bottom", "Bar")
            graph_curve = graph_plot.plot(pen=pg.mkPen("#2a7fff", width=2))
            graph_layout.addWidget(graph_plot)
            tabs.addTab(graph_tab, "Graph")

            report_text = QTextEdit()
            report_text.setReadOnly(True)
            report_text.setStyleSheet(
                "QTextEdit { background-color: #0b1220; color: #d7dfeb; font-family: Consolas; }"
            )
            tabs.addTab(report_text, "Report")

            journal_text = QTextEdit()
            journal_text.setReadOnly(True)
            journal_text.setStyleSheet(
                "QTextEdit { background-color: #0b1220; color: #d7dfeb; font-family: Consolas; }"
            )
            tabs.addTab(journal_text, "Journal")

            layout.addWidget(tabs)

            window.setCentralWidget(container)
            window._backtest_container = container
            window._backtest_status = status
            window._backtest_summary = summary
            window._backtest_setting_labels = setting_labels
            window._backtest_metric_labels = metric_labels
            window._backtest_tabs = tabs
            window._backtest_results = results_table
            window._backtest_graph_curve = graph_curve
            window._backtest_report = report_text
            window._backtest_journal = journal_text
            window._backtest_toggle_btn = toggle_btn
            window._backtest_report_btn = report_btn

        self._refresh_backtest_window(window)
        window.show()
        window.raise_()
        window.activateWindow()
        return window

    def _refresh_backtest_window(self, window=None, message=None):
        window = window or self.detached_tool_windows.get("backtesting_workspace")
        if window is None:
            return

        status = getattr(window, "_backtest_status", None)
        summary = getattr(window, "_backtest_summary", None)
        results = getattr(window, "_backtest_results", None)
        settings = getattr(window, "_backtest_setting_labels", None)
        metrics = getattr(window, "_backtest_metric_labels", None)
        graph_curve = getattr(window, "_backtest_graph_curve", None)
        report_view = getattr(window, "_backtest_report", None)
        journal_view = getattr(window, "_backtest_journal", None)
        toggle_btn = getattr(window, "_backtest_toggle_btn", None)
        report_btn = getattr(window, "_backtest_report_btn", None)
        if (
            status is None
            or summary is None
            or results is None
            or settings is None
            or metrics is None
            or graph_curve is None
            or report_view is None
            or journal_view is None
        ):
            return

        backtest_context = getattr(self, "_backtest_context", {}) or {}
        dataset = backtest_context.get("data")
        candle_count = len(dataset) if hasattr(dataset, "__len__") else 0
        has_engine = hasattr(self, "backtest_engine")
        symbol = backtest_context.get("symbol", "-")
        timeframe = backtest_context.get("timeframe", "-")
        strategy_name = backtest_context.get("strategy_name") or getattr(self.controller, "strategy_name", None) or getattr(getattr(self.controller, "config", None), "strategy", "Trend Following")
        spread_pct = float(getattr(self.controller, "spread_pct", 0.0) or 0.0)
        initial_deposit = float(getattr(self.controller, "initial_capital", 10000) or 10000)
        range_text = self._format_backtest_range(dataset)
        running = bool(getattr(self, "_backtest_running", False))
        stop_requested = bool(getattr(self, "_backtest_stop_requested", False))

        if toggle_btn is not None:
            toggle_btn.setText("Stop Backtest" if running else "Start Backtest")
        if report_btn is not None:
            report_btn.setEnabled((not running) and getattr(self, "results", None) is not None)

        default_message = "Backtest stop requested..." if stop_requested else ("Backtest running..." if running else ("Strategy tester ready." if has_engine else "Backtest engine not initialized."))
        status.setText(message or default_message)
        summary.setText(
            f"Expert: {strategy_name} | Symbol: {symbol} | Period: {timeframe} | Bars: {candle_count}"
        )

        settings["Expert"].setText(str(strategy_name))
        settings["Symbol"].setText(str(symbol))
        settings["Period"].setText(str(timeframe))
        settings["Model"].setText("Bar-close simulation")
        settings["Spread"].setText(f"{spread_pct:.4f}%")
        settings["Initial Deposit"].setText(f"{initial_deposit:.2f}")
        settings["Bars"].setText(str(candle_count))
        settings["Range"].setText(range_text)

        results_df = getattr(self, "results", None)
        report = getattr(self, "backtest_report", None)
        equity_curve = getattr(getattr(self, "backtest_engine", None), "equity_curve", []) or []

        if results_df is None:
            self._populate_backtest_results_table(results, None)
            graph_curve.setData([])
            metrics["Total Net Profit"].setText("-")
            metrics["Trades"].setText("-")
            metrics["Win Rate"].setText("-")
            metrics["Max Drawdown"].setText("-")
            metrics["Final Equity"].setText("-")
            report_view.setPlainText("No backtest results yet.")
            journal_view.setPlainText("\n".join(getattr(self, "_backtest_journal_lines", []) or []))
            journal_view.moveCursor(QTextCursor.MoveOperation.End)
            return

        try:
            self._populate_backtest_results_table(results, results_df)

            if not isinstance(report, dict):
                report = ReportGenerator(
                    trades=results_df,
                    equity_history=equity_curve,
                ).generate()

            metrics["Total Net Profit"].setText(f"{float(report.get('total_profit', 0.0) or 0.0):.2f}")
            metrics["Trades"].setText(str(int(report.get("total_trades", 0) or 0)))
            metrics["Win Rate"].setText(f"{float(report.get('win_rate', 0.0) or 0.0) * 100.0:.2f}%")
            metrics["Max Drawdown"].setText(f"{float(report.get('max_drawdown', 0.0) or 0.0):.2f}")
            metrics["Final Equity"].setText(f"{float(report.get('final_equity', initial_deposit) or initial_deposit):.2f}")

            graph_curve.setData(equity_curve)
            report_view.setPlainText(self._build_backtest_report_text(backtest_context, report, results_df))
            journal_view.setPlainText("\n".join(getattr(self, "_backtest_journal_lines", []) or []))
            journal_view.moveCursor(QTextCursor.MoveOperation.End)
        except Exception as exc:
            report_view.setPlainText(f"Unable to render backtest results: {exc}")

    def _show_risk_settings_window(self):
        risk_engine = getattr(self.controller, "risk_engine", None)
        if risk_engine is None:
            QMessageBox.warning(self, "Risk Engine Missing", "Trading/risk engine is not initialized yet.")
            return None

        window = self._get_or_create_tool_window(
            "risk_settings",
            "Risk Settings",
            width=460,
            height=340,
        )

        if getattr(window, "_risk_container", None) is None:
            container = QWidget()
            layout = QVBoxLayout(container)
            form = QFormLayout()

            max_portfolio = QDoubleSpinBox()
            max_portfolio.setRange(0, 1)
            max_portfolio.setSingleStep(0.01)

            max_trade = QDoubleSpinBox()
            max_trade.setRange(0, 1)
            max_trade.setSingleStep(0.01)

            max_position = QDoubleSpinBox()
            max_position.setRange(0, 1)
            max_position.setSingleStep(0.01)

            max_gross = QDoubleSpinBox()
            max_gross.setRange(0, 5)
            max_gross.setSingleStep(0.1)

            form.addRow("Max Portfolio Risk:", max_portfolio)
            form.addRow("Max Risk Per Trade:", max_trade)
            form.addRow("Max Position Size:", max_position)
            form.addRow("Max Gross Exposure:", max_gross)
            layout.addLayout(form)

            status = QLabel("-")
            status.setStyleSheet("color: #9fb0c7;")
            layout.addWidget(status)

            save_btn = QPushButton("Save Risk Settings")
            save_btn.clicked.connect(lambda: self._apply_risk_settings(window))
            layout.addWidget(save_btn)

            window.setCentralWidget(container)
            window._risk_container = container
            window._risk_max_portfolio = max_portfolio
            window._risk_max_trade = max_trade
            window._risk_max_position = max_position
            window._risk_max_gross = max_gross
            window._risk_status = status

        window._risk_max_portfolio.setValue(getattr(risk_engine, "max_portfolio_risk", 0.2))
        window._risk_max_trade.setValue(getattr(risk_engine, "max_risk_per_trade", 0.02))
        window._risk_max_position.setValue(getattr(risk_engine, "max_position_size_pct", 0.05))
        window._risk_max_gross.setValue(getattr(risk_engine, "max_gross_exposure_pct", 1.0))
        window._risk_status.setText("Adjust limits and click Save Risk Settings.")

        window.show()
        window.raise_()
        window.activateWindow()
        return window

    def _apply_risk_settings(self, window):
        try:
            risk_engine = getattr(self.controller, "risk_engine", None)
            if risk_engine is None:
                return

            risk_engine.max_portfolio_risk = window._risk_max_portfolio.value()
            risk_engine.max_risk_per_trade = window._risk_max_trade.value()
            risk_engine.max_position_size_pct = window._risk_max_position.value()
            risk_engine.max_gross_exposure_pct = window._risk_max_gross.value()

            window._risk_status.setText("Risk settings saved.")
            self.system_console.log("Risk settings updated successfully.")
        except Exception as exc:
            self.logger.error(f"Risk settings error: {exc}")

    def _populate_portfolio_exposure_table(self, table):
        positions = []
        portfolio = getattr(self.controller, "portfolio", None)
        if portfolio is None:
            return

        if hasattr(portfolio, "get_positions"):
            try:
                positions = portfolio.get_positions() or []
            except Exception:
                positions = []
        elif hasattr(portfolio, "positions"):
            raw_positions = getattr(portfolio, "positions", {})
            if isinstance(raw_positions, dict):
                positions = list(raw_positions.values())

        table.setRowCount(len(positions))
        total_value = 0.0
        for pos in positions:
            try:
                total_value += float(pos.get("value", 0))
            except Exception:
                continue

        for row, pos in enumerate(positions):
            symbol = pos.get("symbol", "-")
            size = pos.get("size", pos.get("amount", "-"))
            value = float(pos.get("value", 0) or 0)
            pct = (value / total_value * 100) if total_value else 0

            table.setItem(row, 0, QTableWidgetItem(str(symbol)))
            table.setItem(row, 1, QTableWidgetItem(str(size)))
            table.setItem(row, 2, QTableWidgetItem(f"{value:.2f}"))
            table.setItem(row, 3, QTableWidgetItem(f"{pct:.2f}%"))

        table.resizeColumnsToContents()
        table.horizontalHeader().setStretchLastSection(True)

    def _show_portfolio_exposure_window(self):
        window = self._get_or_create_tool_window(
            "portfolio_exposure",
            "Portfolio Exposure",
            width=760,
            height=460,
        )

        table = getattr(window, "_exposure_table", None)
        if table is None:
            table = QTableWidget()
            table.setColumnCount(4)
            table.setHorizontalHeaderLabels(
                ["Symbol", "Size", "Value (USD)", "Portfolio %"]
            )
            window.setCentralWidget(table)
            window._exposure_table = table

            sync_timer = QTimer(window)
            sync_timer.timeout.connect(
                lambda: self._populate_portfolio_exposure_table(table)
            )
            sync_timer.start(1200)
            window._sync_timer = sync_timer

        self._populate_portfolio_exposure_table(table)
        window.show()
        window.raise_()
        window.activateWindow()
        return window

    def _show_about(self):
        version_text = html.escape(self._app_version_text())
        self._open_text_window(
            "about_window",
            "About Sopotek Trading",
            f"""
            <h2>Sopotek Trading Platform</h2>
            <p><b>Built by:</b> Sopotek Corporation</p>
            <p><b>Version:</b> {version_text}</p>
            <p><b>Purpose:</b> AI-assisted multi-broker trading workstation for live trading, paper trading, analytics, and historical testing.</p>
            <p><b>Main capabilities:</b> live charts, AI signal monitoring, orderbook analysis, risk controls, backtesting, strategy optimization, and broker abstraction across crypto, stocks, forex, paper, and Stellar.</p>
            <p><b>Best use:</b> start in paper mode, validate charts and signals, confirm balances and risk limits, then move into live trading only after the setup looks stable.</p>
            <p><b>Core stack:</b> PySide6, pyqtgraph, pandas, technical-analysis indicators, broker adapters, and async market-data pipelines.</p>
            <p><b>Designed for:</b> fast iteration without losing visibility into risk, execution status, or model behavior.</p>
            """,
            width=700,
            height=520,
        )

    def _app_version_text(self):
        repo_root = Path(__file__).resolve().parents[3]
        package_version = self._read_package_version(repo_root)
        git_version = self._read_git_version(repo_root)

        if package_version and git_version:
            return f"{package_version} ({git_version})"
        if git_version:
            return git_version
        if package_version:
            return package_version
        return "Version not available"

    def _read_package_version(self, repo_root: Path):
        pyproject_path = repo_root / "pyproject.toml"
        if not pyproject_path.exists():
            return None

        try:
            with pyproject_path.open("rb") as handle:
                data = tomllib.load(handle)
            project = data.get("project", {})
            version = project.get("version")
            if version:
                return str(version)
        except Exception:
            return None
        return None

    def _read_git_version(self, repo_root: Path):
        try:
            describe = subprocess.run(
                ["git", "describe", "--tags", "--always", "--dirty"],
                cwd=repo_root,
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
            if not describe:
                return None

            branch = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                cwd=repo_root,
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
            if branch and branch != "HEAD":
                return f"{describe} on {branch}"
            return describe
        except Exception:
            return None

    def _close_all_positions(self):
        broker = getattr(self.controller, "broker", None)
        if broker is None:
            QMessageBox.warning(self, "Close Positions", "Connect a broker before closing positions.")
            return

        confirm = QMessageBox.question(
            self,
            "Close Positions",
            "Close all tracked positions with market orders?",
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        asyncio.get_event_loop().create_task(self._close_all_positions_async())

    def _export_trades(self):
        try:
            try:
                import pandas as pd
            except Exception:
                pd = None

            trades = getattr(self, "results", None)
            if trades is None or getattr(trades, "empty", True):
                QMessageBox.information(self, "Export Trades", "No trades are available to export yet.")
                return

            path, _ = QFileDialog.getSaveFileName(
                self,
                "Export Trades",
                "trades.csv",
                "CSV Files (*.csv)",
            )
            if not path:
                return

            # Backtest results are already dataframe-like, but we normalize defensively
            # so export still works if the table source changes later.
            if pd is not None and not hasattr(trades, "to_csv"):
                trades = pd.DataFrame(trades)
            trades.to_csv(path, index=False)
            self.system_console.log(f"Trades exported to {path}", "INFO")
            QMessageBox.information(self, "Export Trades", f"Trades exported to:\n{path}")
        except Exception as exc:
            self.logger.exception("Export trades failed")
            self.system_console.log(f"Trade export failed: {exc}", "ERROR")
            QMessageBox.critical(self, "Export Trades Failed", str(exc))

    def _cancel_all_orders(self):
        broker = getattr(self.controller, "broker", None)
        if broker is None:
            QMessageBox.warning(self, "Cancel Orders", "Connect a broker before canceling orders.")
            return

        confirm = QMessageBox.question(
            self,
            "Cancel Orders",
            "Cancel all open orders for the connected broker?",
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        asyncio.get_event_loop().create_task(self._cancel_all_orders_async())

    def _show_async_message(self, title, text, icon=QMessageBox.Icon.Information):
        def _open():
            if self._ui_shutting_down:
                return
            box = QMessageBox(self)
            box.setIcon(icon)
            box.setWindowTitle(title)
            box.setText(str(text))
            box.setStandardButtons(QMessageBox.StandardButton.Ok)
            box.setModal(False)
            box.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
            box.open()

        QTimer.singleShot(0, _open)

    async def _submit_manual_trade(
        self,
        symbol,
        side,
        amount,
        order_type="market",
        price=None,
        stop_loss=None,
        take_profit=None,
    ):
        try:
            trading_system = getattr(self.controller, "trading_system", None)
            execution_manager = getattr(trading_system, "execution_manager", None)
            if execution_manager is not None:
                order = await execution_manager.execute(
                    symbol=symbol,
                    side=side,
                    amount=amount,
                    type=order_type,
                    price=price,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                )
            else:
                order = await self.controller.broker.create_order(
                    symbol=symbol,
                    side=side,
                    amount=amount,
                    type=order_type,
                    price=price,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                )

            if not order:
                self._show_async_message(
                    "Manual Order",
                    f"The order for {symbol} was skipped by broker safety checks.",
                    QMessageBox.Icon.Information,
                )
                return

            status_text = str(order.get("status") or "submitted").replace("_", " ").upper()
            self.system_console.log(
                f"Manual order {status_text}: {side.upper()} {amount} {symbol} ({order_type})",
                "INFO",
            )
            self._show_async_message(
                "Manual Order",
                f"{status_text.title()} {side.upper()} {amount} {symbol}.",
                QMessageBox.Icon.Information,
            )
        except Exception as exc:
            self.logger.exception("Manual order failed")
            self.system_console.log(f"Manual order failed for {symbol}: {exc}", "ERROR")
            self._show_async_message("Manual Order Failed", str(exc), QMessageBox.Icon.Critical)

    def _tracked_app_positions(self):
        trading_system = getattr(self.controller, "trading_system", None)
        portfolio_manager = getattr(trading_system, "portfolio", None)
        portfolio = getattr(portfolio_manager, "portfolio", None)
        positions = getattr(portfolio, "positions", {}) if portfolio is not None else {}
        tracked = []
        if not isinstance(positions, dict):
            return tracked

        for symbol, position in positions.items():
            quantity = float(getattr(position, "quantity", 0) or 0)
            if quantity == 0:
                continue
            tracked.append(
                {
                    "symbol": symbol,
                    "amount": abs(quantity),
                    "side": "long" if quantity > 0 else "short",
                }
            )
        return tracked

    async def _close_all_positions_async(self):
        broker = getattr(self.controller, "broker", None)
        if broker is None:
            return

        try:
            results = []
            if hasattr(broker, "close_all_positions"):
                results = await broker.close_all_positions()

            if not results:
                for position in self._tracked_app_positions():
                    symbol = position.get("symbol")
                    amount = float(position.get("amount", 0) or 0)
                    if not symbol or amount <= 0:
                        continue
                    close_side = "sell" if position.get("side") != "short" else "buy"
                    order = await broker.create_order(
                        symbol=symbol,
                        side=close_side,
                        amount=amount,
                        type="market",
                    )
                    if order:
                        results.append(order)

            count = len(results or [])
            if count == 0:
                QMessageBox.information(
                    self,
                    "Close Positions",
                    "No open positions were found to close.",
                )
                return

            self.system_console.log(f"Closed {count} position(s).", "INFO")
            QMessageBox.information(
                self,
                "Close Positions",
                f"Submitted {count} closing order(s).",
            )
        except Exception as exc:
            self.logger.exception("Close-all positions failed")
            self.system_console.log(f"Close positions failed: {exc}", "ERROR")
            QMessageBox.critical(self, "Close Positions Failed", str(exc))

    async def _cancel_all_orders_async(self):
        broker = getattr(self.controller, "broker", None)
        if broker is None:
            return

        try:
            results = await broker.cancel_all_orders()
            if results is True:
                count = 1
            elif isinstance(results, list):
                count = len(results)
            elif results:
                count = 1
            else:
                count = 0

            self.system_console.log(f"Canceled {count} open order(s).", "INFO")
            self._latest_open_orders_snapshot = []
            self._populate_open_orders_table(self._latest_open_orders_snapshot)
            self._schedule_open_orders_refresh()
            QMessageBox.information(
                self,
                "Cancel Orders",
                "Canceled all open orders." if count else "No open orders were found.",
            )
        except Exception as exc:
            self.logger.exception("Cancel-all orders failed")
            self.system_console.log(f"Cancel orders failed: {exc}", "ERROR")
            QMessageBox.critical(self, "Cancel Orders Failed", str(exc))

    def _open_docs(self):
        self._open_text_window(
            "help_documentation",
            "Documentation",
            """
            <h2>Documentation</h2>
            <h3>1. What This App Does</h3>
            <p>Sopotek is a trading workstation that combines broker access, live charting, AI-driven signal monitoring, orderbook views, execution controls, risk settings, historical backtesting, and strategy optimization.</p>

            <h3>2. Quick Start</h3>
            <p><b>Step 1:</b> Open the dashboard and choose a broker type, exchange, mode, strategy, and risk budget.</p>
            <p><b>Step 2:</b> Use paper mode first whenever you are testing a new broker, strategy, or market.</p>
            <p><b>Step 3:</b> Launch the terminal, open a symbol tab from the toolbar, and confirm candles are loading.</p>
            <p><b>Step 4:</b> Review system status, balances, training states, and application settings before turning on AI trading.</p>
            <p><b>Step 5:</b> Use backtesting and optimization before trusting a strategy in live conditions.</p>

            <h3>3. Main Layout</h3>
            <p><b>Toolbar:</b> symbol picker, timeframe controls, AI trading toggle, and chart actions.</p>
            <p><b>Chart tabs:</b> one tab per symbol and timeframe, with candlesticks, indicators, and bid/ask overlays.</p>
            <p><b>Orderbook:</b> bid/ask ladders plus depth view for the active chart symbol.</p>
            <p><b>AI Signal Monitor:</b> latest model decisions, confidence, regime, and volatility readout.</p>
            <p><b>Strategy Debug:</b> indicator values and strategy reasoning for generated signals.</p>
            <p><b>System Status:</b> connection state, websocket state, balances, and session health summary.</p>
            <p><b>Logs:</b> runtime messages, broker responses, and error diagnostics.</p>

            <h3>4. Charts</h3>
            <p>Use the symbol selector in the toolbar to open a new chart tab. If the symbol already exists, the app focuses the existing tab instead of duplicating it.</p>
            <p>Timeframe buttons reload candles for the active tab. Indicators can be added from the <b>Charts</b> menu. Bid and ask dashed price lines can be toggled from <b>Charts -&gt; Show Bid/Ask Lines</b>.</p>
            <p>The candlestick chart is intentionally the largest area and can be resized where splitters are available.</p>

            <h3>5. AI Trading</h3>
            <p>The AI trading button enables the automated worker loop. It does not guarantee that orders will be sent every cycle; signals still pass through broker checks, balance checks, market-status checks, and exchange minimum filters.</p>
            <p>If AI trading is on but no trades occur, check the logs, AI Signal Monitor, Strategy Debug, and account balances first.</p>

            <h3>6. Orders and Safety</h3>
            <p>The execution path checks available balances before sending orders, trims amounts when necessary, and skips symbols on cooldown after exchange rejections such as closed markets, insufficient balance, or minimum notional failures.</p>
            <p>For live sessions, always confirm that you have enough quote currency for buys and enough base currency for sells.</p>

            <h3>7. Backtesting</h3>
            <p>Open a chart that already has candles loaded, then use <b>Backtesting -&gt; Run Backtest</b>. This initializes the backtest with the active chart symbol, timeframe, and strategy context.</p>
            <p>In the backtesting workspace, click <b>Start Backtest</b> to run the historical simulation and <b>Generate Report</b> to export PDF and spreadsheet results.</p>
            <p>If backtesting says no data is available, reload the chart candles first.</p>

            <h3>8. Strategy Optimization</h3>
            <p>Use <b>Backtesting -&gt; Strategy Optimization</b> to run a parameter sweep over core strategy settings such as RSI, EMA fast, EMA slow, and ATR periods.</p>
            <p>The optimization table ranks results by performance metrics. Use <b>Apply Best Params</b> to push the top result into the active strategy object.</p>
            <p>Optimization depends on historical candle data being available for the active chart.</p>

            <h3>9. Settings and Risk Controls</h3>
            <p>The <b>Settings</b> menu is the main configuration area for trading defaults, chart behavior, refresh intervals, backtesting capital, and all risk limits.</p>
            <p>Portfolio exposure is also available from <b>Settings</b> so you can keep configuration and risk context in one place.</p>

            <h3>10. Tools Windows</h3>
            <p>The <b>Tools</b> menu opens detached utility windows so you can keep charts large while monitoring logs, AI signals, and performance analytics in parallel.</p>

            <h3>11. Supported Broker Concepts</h3>
            <p><b>Crypto:</b> CCXT-compatible exchanges and Stellar.</p>
            <p><b>Forex:</b> Oanda.</p>
            <p><b>Stocks:</b> Alpaca.</p>
            <p><b>Paper:</b> local simulated execution path.</p>

            <h3>12. Stellar Notes</h3>
            <p>For Stellar, use the public key in the dashboard API field and the secret seed in the secret field. Market data currently uses polling via Horizon rather than websocket streaming.</p>
            <p>Non-native assets may require issuer-aware configuration if the code is ambiguous.</p>

            <h3>13. Troubleshooting</h3>
            <p><b>No candles:</b> confirm the symbol exists on the broker and try changing timeframe.</p>
            <p><b>No orderbook:</b> open a chart tab first and wait for the orderbook refresh timer to update the active symbol.</p>
            <p><b>No AI signals:</b> verify that the strategy can compute features from the loaded candles and that AI trading is enabled when required.</p>
            <p><b>Orders rejected:</b> check exchange minimums, market status, insufficient balance, and broker-specific rules in the logs.</p>
            <p><b>Backtest/optimization blank:</b> make sure the active chart already has historical data loaded.</p>

            <h3>14. Recommended Workflow</h3>
            <p>Use this order: dashboard setup -> paper session -> verify charts and signals -> run backtest -> run optimization -> confirm application and risk settings -> move to live trading.</p>

            <h3>15. Where To Look Next</h3>
            <p>For broker-specific and integration-level details, open <b>Help -&gt; API Reference</b>.</p>
            """,
            width=940,
            height=760,
        )

    def _open_api_docs(self):
        self._open_text_window(
            "api_reference",
            "API Reference",
            """
            <h2>API Reference</h2>
            <h3>Broker Layer</h3>
            <p>The app uses a normalized broker interface so the terminal can work across multiple providers with the same core methods.</p>
            <p><b>Common market-data methods:</b> fetch_ticker, fetch_orderbook, fetch_ohlcv, fetch_trades, fetch_symbols, fetch_markets, fetch_status.</p>
            <p><b>Common trading methods:</b> create_order, cancel_order, cancel_all_orders.</p>
            <p><b>Common account methods:</b> fetch_balance, fetch_positions, fetch_orders, fetch_open_orders, fetch_closed_orders, fetch_order.</p>

            <h3>Broker Types in This App</h3>
            <p><b>CCXTBroker:</b> crypto exchanges using the CCXT unified API.</p>
            <p><b>OandaBroker:</b> forex account and market access.</p>
            <p><b>AlpacaBroker:</b> stock and equity trading access.</p>
            <p><b>PaperBroker:</b> local simulation for testing flows safely.</p>
            <p><b>StellarBroker:</b> Horizon-backed Stellar market data, balances, offers, and signed offer submission.</p>

            <h3>Configuration Fields</h3>
            <p><b>type:</b> crypto, forex, stocks, or paper.</p>
            <p><b>exchange:</b> provider name such as binanceus, coinbase, oanda, alpaca, paper, or stellar.</p>
            <p><b>mode:</b> live or paper.</p>
            <p><b>api_key / secret:</b> broker credentials. For Stellar this maps to public key and secret seed.</p>
            <p><b>account_id:</b> required for Oanda.</p>
            <p><b>password / passphrase:</b> required on some exchanges.</p>
            <p><b>sandbox:</b> enables testnet or practice behavior where supported.</p>
            <p><b>options / params:</b> broker-specific advanced settings.</p>

            <h3>Execution Notes</h3>
            <p>Execution passes through a router and execution manager. Before orders are sent, the app checks balances, market state, minimums, and cooldown status.</p>
            <p>Exchange-specific rejections are logged and may place the symbol on cooldown to reduce error spam.</p>

            <h3>Backtesting and Optimization Internals</h3>
            <p><b>BacktestEngine:</b> replays candle windows through the active strategy and simulator.</p>
            <p><b>Simulator:</b> executes simplified buy/sell flows for historical testing.</p>
            <p><b>ReportGenerator:</b> creates summary metrics plus PDF/spreadsheet exports.</p>
            <p><b>StrategyOptimizer:</b> runs parameter sweeps and ranks results by performance.</p>

            <h3>Live Data Notes</h3>
            <p>Some brokers use websocket market data; others fall back to polling. Stellar currently uses polling via Horizon.</p>

            <h3>External References</h3>
            <p><a href="https://docs.ccxt.com">CCXT Documentation</a></p>
            <p><a href="https://github.com/ccxt/ccxt/wiki/manual">CCXT Manual</a></p>
            <p><a href="https://developers.stellar.org/docs/data/apis/horizon/api-reference">Stellar Horizon API Reference</a></p>
            <p><a href="https://stellar-sdk.readthedocs.io/en/latest/index.html">stellar-sdk Python Documentation</a></p>
            <p><a href="https://alpaca.markets/docs/">Alpaca API Docs</a></p>
            <p><a href="https://developer.oanda.com/rest-live-v20/introduction/">Oanda v20 API Docs</a></p>
            """,
            width=900,
            height=720,
        )

    def _multi_chart_layout(self):

        try:
            if self._chart_tabs_ready():
                for index in reversed(range(self.chart_tabs.count())):
                    page = self.chart_tabs.widget(index)
                    if getattr(page, "objectName", lambda: "")() == "multi_chart_page":
                        self._close_chart_tab(index)

            symbols = list(dict.fromkeys((getattr(self.controller, "symbols", []) or [])[:4]))
            if not symbols:
                return

            screen = QApplication.primaryScreen()
            available = screen.availableGeometry() if screen is not None else self.geometry()
            width = max(420, available.width() // 2)
            height = max(320, available.height() // 2)

            positions = [
                (available.x(), available.y()),
                (available.x() + width, available.y()),
                (available.x(), available.y() + height),
                (available.x() + width, available.y() + height),
            ]

            for symbol, (x, y) in zip(symbols, positions):
                rect = type(available)(x, y, width, height)
                self._open_or_focus_detached_chart(symbol, self.current_timeframe, geometry=rect)

        except Exception as e:

            self.logger.error(f"Multi chart layout error: {e}")

    def _open_performance(self):
        window = self._get_or_create_tool_window(
            "performance_analytics",
            "Performance Analytics",
            width=1120,
            height=780,
        )

        if getattr(window, "_performance_container", None) is None:
            container = QWidget()
            container.setStyleSheet("background-color: #0b1220;")
            layout = QVBoxLayout(container)
            layout.setContentsMargins(14, 14, 14, 14)
            layout.setSpacing(12)

            summary = QLabel("Performance analytics will summarize profitability, risk, execution quality, and symbol contribution.")
            summary.setWordWrap(True)
            summary.setStyleSheet(
                "color: #d9e6f7; background-color: #101a2d; border: 1px solid #20324d; "
                "border-radius: 14px; padding: 14px; font-size: 14px; font-weight: 600;"
            )
            layout.addWidget(summary)

            metric_names = [
                "Equity",
                "Starting Equity",
                "Net PnL",
                "Return",
                "Trades",
                "Realized Trades",
                "Win Rate",
                "Profit Factor",
                "Sharpe Ratio",
                "Sortino Ratio",
                "Max Drawdown",
                "VaR (95%)",
            ]
            stats_grid, metric_labels = self._build_performance_metric_grid(metric_names, columns=4)
            layout.addLayout(stats_grid)

            equity_plot = pg.PlotWidget()
            self._style_performance_plot(equity_plot, left_label="Equity")
            equity_plot.setMinimumHeight(230)
            curve = equity_plot.plot(pen=pg.mkPen("#2a7fff", width=2.4))
            layout.addWidget(equity_plot)

            drawdown_plot = pg.PlotWidget()
            self._style_performance_plot(drawdown_plot, left_label="Drawdown")
            drawdown_plot.setMinimumHeight(170)
            drawdown_curve = drawdown_plot.plot(
                pen=pg.mkPen("#ef5350", width=1.8),
                fillLevel=0,
                brush=pg.mkBrush(239, 83, 80, 70),
            )
            layout.addWidget(drawdown_plot)

            insights = QTextBrowser()
            insights.setOpenExternalLinks(False)
            insights.setMinimumHeight(150)
            insights.setStyleSheet(
                "QTextBrowser { background-color: #101a2d; color: #d8e6ff; border: 1px solid #20324d; "
                "border-radius: 12px; padding: 10px; }"
            )
            layout.addWidget(insights)

            symbol_table = QTableWidget()
            symbol_table.setColumnCount(6)
            symbol_table.setHorizontalHeaderLabels(
                ["Symbol", "Orders", "Realized", "Win Rate", "Net PnL", "Avg PnL"]
            )
            symbol_table.setStyleSheet(
                "QTableWidget { background-color: #101a2d; color: #d8e6ff; border: 1px solid #20324d; "
                "border-radius: 12px; gridline-color: #20324d; }"
            )
            layout.addWidget(symbol_table)

            window.setCentralWidget(container)
            window._performance_container = container
            window._performance_widgets = {
                "summary": summary,
                "metric_labels": metric_labels,
                "equity_curve": curve,
                "drawdown_curve": drawdown_curve,
                "insights": insights,
                "symbol_table": symbol_table,
            }

            sync_timer = QTimer(window)
            sync_timer.timeout.connect(lambda: self._refresh_performance_window(window))
            sync_timer.start(1000)
            window._sync_timer = sync_timer

        self._refresh_performance_window(window)
        window.show()
        window.raise_()
        window.activateWindow()

    def _performance_series(self):
        perf = getattr(self.controller, "performance_engine", None)
        if perf is None:
            return []

        for attr in ("equity_history", "equity_curve"):
            series = getattr(perf, attr, None)
            if isinstance(series, list):
                return series

        return []

    def _format_performance_value(self, value, percent=False):
        if value is None:
            return "-"

        try:
            numeric = float(value)
        except Exception:
            return str(value)

        if percent:
            return f"{numeric * 100:.2f}%"
        return f"{numeric:.4f}"

    def _refresh_performance_window(self, window):
        widgets = getattr(window, "_performance_widgets", None)
        if widgets is None:
            return
        self._populate_performance_view(widgets, self._performance_snapshot())

    def _open_risk_settings(self):
        self._show_risk_settings_window()

    def save_settings(self):
        dialog = QDialog(self)
        save_btn = QPushButton("Save")
        layout = QVBoxLayout()
        max_portfolio_risk = QDoubleSpinBox()
        max_portfolio_risk.setRange(0, 1)
        max_portfolio_risk.setSingleStep(0.01)
        max_risk_per_trade = QDoubleSpinBox()
        max_risk_per_trade.setRange(0, 5)
        max_risk_per_trade.setSingleStep(0.01)
        max_position_size = QDoubleSpinBox()
        max_position_size.setRange(0, 5)
        max_position_size.setSingleStep(0.01)
        max_gross_exposure = QDoubleSpinBox()
        max_gross_exposure.setRange(0, 5)
        max_gross_exposure.setSingleStep(0.01)

        try:

            self.controller.risk_engine.max_portfolio_risk = max_portfolio_risk.value()
            self.controller.risk_engine.max_risk_per_trade = max_risk_per_trade.value()
            self.controller.risk_engine.max_position_size_pct = max_position_size.value()
            self.controller.risk_engine.max_gross_exposure_pct = max_gross_exposure.value()

            QMessageBox.information(
                dialog,
                "Risk Settings",
                "Risk settings updated successfully."
            )

            dialog.close()

        except Exception as e:

            self.logger.error(f"Risk settings error: {e}")

        save_btn.clicked.connect(self.save_settings)

        layout.addWidget(save_btn)

        dialog.setLayout(layout)

        dialog.exec()

    def _show_portfolio_exposure(self):
        try:
            self._show_portfolio_exposure_window()
        except Exception as e:
            self.logger.error(f"Portfolio exposure error: {e}")

    async def _reload_chart_data(self, symbol, timeframe):

        try:

            buffers = self.controller.candle_buffers.get(symbol)

            if not buffers:
                return

            df = buffers.get(timeframe)

            if df is None:
                return

            self._update_chart(symbol, df)

        except Exception as e:

            self.logger.error(f"Timeframe reload failed: {e}")



    def _format_balance_text(self, balance):
        """Render balances like: XLM:100, USDT:100."""
        if not isinstance(balance, dict) or not balance:
            return "-"

        # Common CCXT shape: {"free": {...}, "used": {...}, "total": {...}}
        if isinstance(balance.get("total"), dict):
            source = balance.get("total") or {}
        elif isinstance(balance.get("free"), dict):
            source = balance.get("free") or {}
        else:
            # Flat dict fallback; skip known non-asset keys
            skip = {"free", "used", "total", "info", "raw", "equity", "cash", "currency"}
            source = {k: v for k, v in balance.items() if k not in skip}

        parts = []
        for sym, val in source.items():
            try:
                num = float(val)
            except Exception:
                continue
            if num == 0:
                continue
            parts.append(f"{sym}:{num:g}")

        if not parts:
            return "-"

        parts.sort()
        return ", ".join(parts)

    def _compact_balance_text(self, balance, max_items=4):
        full_text = self._format_balance_text(balance)
        if full_text == "-":
            return "-", "-"

        parts = [part.strip() for part in full_text.split(",") if part.strip()]
        compact = ", ".join(parts[:max_items])
        if len(parts) > max_items:
            compact = f"{compact} +{len(parts) - max_items} more"

        return compact, full_text

    def _elide_text(self, value, max_length=42):
        text = str(value)
        if len(text) <= max_length:
            return text
        return f"{text[: max_length - 1]}..."

    def _set_status_value(self, field, value, tooltip=None):
        label = self.status_labels.get(field)
        if label is None:
            return

        display = self._elide_text(value)
        label.setText(display)
        label.setToolTip(tooltip or str(value))

    def _system_status_exchange_display(self):
        broker = getattr(self.controller, "broker", None)
        config = getattr(self.controller, "config", None)
        broker_config = getattr(config, "broker", None)

        exchange = getattr(broker, "exchange_name", None)
        if not exchange and broker_config is not None:
            exchange = getattr(broker_config, "exchange", None)

        normalized = str(exchange or "Unknown").strip()
        normalized_lower = normalized.lower()

        if normalized_lower == "stellar":
            horizon_url = getattr(broker, "horizon_url", "")
            if horizon_url:
                return "Stellar Horizon", horizon_url
            return "Stellar Horizon", "stellar"

        if not normalized:
            return "Unknown", "Unknown"

        return normalized, normalized

    def _refresh_terminal(self):

        try:

            controller = self.controller

            balance = getattr(controller, "balances", {})
            balance_equity = None
            if isinstance(balance, dict):
                balance_equity = balance.get("equity")
                if balance_equity is None:
                    total_balance = balance.get("total")
                    if isinstance(total_balance, dict) and len(total_balance) == 1:
                        try:
                            balance_equity = float(next(iter(total_balance.values())))
                        except Exception:
                            balance_equity = None

            if balance_equity is None:
                equity = getattr(controller.portfolio, "get_equity", lambda: 0)()
            else:
                equity = float(balance_equity)
            spread = getattr(controller, "spread_pct", 0)
            positions = getattr(controller.portfolio, "positions", {})
            symbols = getattr(controller, "symbols", [])
            exchange_display, exchange_tooltip = self._system_status_exchange_display()

            free = balance.get("free", 0) if isinstance(balance, dict) else 0
            used = balance.get("used", 0) if isinstance(balance, dict) else 0

            balance_summary, balance_tooltip = self._compact_balance_text(balance)
            free_summary, free_tooltip = self._compact_balance_text(
                free if isinstance(free, dict) else {"USDT": free}
            )
            used_summary, used_tooltip = self._compact_balance_text(
                used if isinstance(used, dict) else {"USDT": used}
            )

            self._set_status_value("Exchange", exchange_display, exchange_tooltip)

            self._set_status_value("Symbols Loaded", len(symbols))

            self._set_status_value("Equity", f"{equity:.4f}")

            self._set_status_value("Balance", balance_summary, balance_tooltip)

            self._set_status_value("Free Margin", free_summary, free_tooltip)

            self._set_status_value("Used Margin", used_summary, used_tooltip)

            self._set_status_value("Spread %", f"{spread:.4f}")

            self._set_status_value("Open Positions", len(positions))
            self._set_status_value("Open Orders", len(getattr(self, "_latest_open_orders_snapshot", [])))

            market_stream_status = "Stopped"
            if hasattr(controller, "get_market_stream_status"):
                market_stream_status = controller.get_market_stream_status()

            self._set_status_value("Websocket", market_stream_status)

            self._set_status_value("AITrading", "ON" if self.autotrading_enabled else "OFF")
            self._set_status_value("AI Scope", self._autotrade_scope_label())
            self._set_status_value("Watchlist", len(self.autotrade_watchlist))

            self._set_status_value("Timeframe", self.current_timeframe)

            self._update_risk_heatmap()
            self._populate_positions_table(getattr(self, "_latest_positions_snapshot", []))
            self._populate_open_orders_table(getattr(self, "_latest_open_orders_snapshot", []))
            self._schedule_positions_refresh()
            self._schedule_open_orders_refresh()
            self._refresh_strategy_comparison_panel()

        except Exception as e:

            self.logger.error(e)


    def _refresh_markets(self):

        blocked = self.symbols_table.blockSignals(True)
        self.symbols_table.setRowCount(0)
        self._configure_market_watch_table()

        for symbol in self.controller.symbols:

            row = self.symbols_table.rowCount()
            self.symbols_table.insertRow(row)

            self._set_market_watch_row(row, symbol, bid="-", ask="-", status="⏳", usd_value="-")
        self.symbols_table.blockSignals(blocked)
        self._reorder_market_watch_rows()

    def _create_system_status_panel(self):

        dock = QDockWidget("System Status", self)
        dock.setMinimumWidth(250)
        dock.setMaximumWidth(320)
        self.system_status_dock = dock

        container = QWidget()
        layout = QGridLayout()
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setHorizontalSpacing(10)
        layout.setVerticalSpacing(8)

        self.status_labels = {}

        fields = [
            "Exchange",

            "Symbols Loaded",
            "Equity",
            "Balance",
            "Free Margin",
            "Used Margin",
            "Spread %",
            "Open Positions",
            "Open Orders",
            "Websocket",
            "AITrading",
            "AI Scope",
            "Watchlist",
            "Timeframe"
        ]

        for row, field in enumerate(fields):
            title = QLabel(field)
            title.setStyleSheet("color: #8fa3bf; font-weight: 700;")
            value = QLabel("-")
            value.setWordWrap(True)
            value.setStyleSheet("color: #e6edf7; font-weight: 600;")
            value.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)

            layout.addWidget(title, row, 0)
            layout.addWidget(value, row, 1)

            self.status_labels[field] = value

        container.setLayout(layout)

        dock.setWidget(container)

        self.addDockWidget(Qt.LeftDockWidgetArea, dock)
        dock.hide()

    def _create_ai_signal_panel(self):

        dock = QDockWidget("AI Signal Monitor", self)

        self.ai_table = QTableWidget()
        self.ai_table.setColumnCount(6)

        self.ai_table.setHorizontalHeaderLabels([
            "Symbol",
            "Signal",
            "Confidence",
            "Regime",
            "Volatility",
            "Time"
        ])

        dock.setWidget(self.ai_table)

        self.addDockWidget(Qt.RightDockWidgetArea, dock)

    def _update_ai_signal(self, data):
        if not isinstance(data, dict):
            return

        symbol = str(data.get("symbol", "") or "").strip()
        if not symbol:
            return

        record = {
            "symbol": symbol,
            "signal": str(data.get("signal", "") or ""),
            "confidence": float(data.get("confidence", 0.0) or 0.0),
            "regime": str(data.get("regime", "") or ""),
            "volatility": data.get("volatility", ""),
            "timestamp": str(data.get("timestamp", "") or ""),
        }
        self._ai_signal_records[symbol] = record

        def _sort_key(item):
            timestamp_text = item.get("timestamp", "")
            try:
                normalized = timestamp_text.replace("Z", "+00:00")
                return datetime.fromisoformat(normalized)
            except Exception:
                return datetime.min.replace(tzinfo=timezone.utc)

        rows = sorted(self._ai_signal_records.values(), key=_sort_key, reverse=True)[: self.MAX_LOG_ROWS]
        self.ai_table.setRowCount(len(rows))

        for row, item in enumerate(rows):
            values = [
                item["symbol"],
                item["signal"],
                f"{item['confidence']:.2f}",
                item["regime"],
                str(item["volatility"]),
                item["timestamp"],
            ]
            for col, value in enumerate(values):
                self.ai_table.setItem(row, col, QTableWidgetItem(str(value)))

        self.ai_table.resizeColumnsToContents()
        self.ai_table.horizontalHeader().setStretchLastSection(True)

    def _create_regime_panel(self):

        dock = QDockWidget("Market Regime", self)

        container = QWidget()
        layout = QVBoxLayout()

        self.regime_label = QLabel("Regime: UNKNOWN")
        self.regime_label.setStyleSheet("font-size: 18px;")

        layout.addWidget(self.regime_label)

        container.setLayout(layout)

        dock.setWidget(container)

        self.addDockWidget(Qt.LeftDockWidgetArea, dock)

    def _update_regime(self, regime):

        colors = {
            "TREND_UP": "green",
            "TREND_DOWN": "red",
            "RANGE": "yellow",
            "HIGH_VOL": "orange"
        }

        color = colors.get(regime, "white")

        self.regime_label.setText(f"Regime: {regime}")
        self.regime_label.setStyleSheet(
            f"font-size:18px;color:{color}"
        )

    def _create_portfolio_exposure_graph(self):

        dock = QDockWidget("Portfolio Exposure", self)

        self.exposure_chart = pg.PlotWidget()

        self.exposure_bars = pg.BarGraphItem(
            x=[],
            height=[],
            width=0.6
        )

        self.exposure_chart.addItem(self.exposure_bars)

        dock.setWidget(self.exposure_chart)

        self.addDockWidget(Qt.RightDockWidgetArea, dock)

    def _create_model_confidence(self):

        dock = QDockWidget("Model Confidence", self)

        self.confidence_plot = pg.PlotWidget()

        self.confidence_curve = self.confidence_plot.plot(
            pen="cyan"
        )



        dock.setWidget(self.confidence_plot)

        self.addDockWidget(Qt.RightDockWidgetArea, dock)

    def _update_confidence(self, confidence):

        self.confidence_data.append(confidence)

        if len(self.confidence_data) > 200:
            self.confidence_data.pop(0)

        self.confidence_curve.setData(self.confidence_data)

    def _update_portfolio_exposure(self):

        positions = self.controller.portfolio.positions

        if not positions:
            return

        symbols = []
        values = []

        for pos in positions.values():

            symbols.append(pos["symbol"])
            values.append(pos["value"])

        x = list(range(len(symbols)))

        self.exposure_bars.setOpts(
            x=x,
            height=values
        )

    def _set_risk_heatmap_status(self, message, tone="muted"):
        label = getattr(self, "risk_heatmap_status_label", None)
        if label is None:
            return
        color_map = {
            "muted": "#8fa7c6",
            "warning": "#ffb84d",
            "positive": "#32d296",
            "negative": "#ff6b6b",
        }
        color = color_map.get(tone, "#8fa7c6")
        label.setStyleSheet(f"color: {color}; font-weight: 600; padding: 6px 2px 0 2px;")
        label.setText(str(message or ""))

    def _create_risk_heatmap(self):

        dock = QDockWidget("Risk Heatmap", self)

        self.risk_map = pg.ImageItem()

        plot = pg.PlotWidget()
        plot.setBackground("#0b1220")
        plot.showGrid(x=False, y=False, alpha=0.0)
        plot.hideAxis("bottom")
        plot.hideAxis("left")
        plot.addItem(self.risk_map)

        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(4)
        layout.addWidget(plot)

        self.risk_heatmap_status_label = QLabel("Risk heatmap is waiting for portfolio data.")
        self.risk_heatmap_status_label.setWordWrap(True)
        layout.addWidget(self.risk_heatmap_status_label)
        self._set_risk_heatmap_status("Risk heatmap is waiting for portfolio data.", "muted")

        dock.setWidget(container)

        self.addDockWidget(Qt.RightDockWidgetArea, dock)

    def _update_risk_heatmap(self):

        if self.risk_map is None:
            return

        portfolio = getattr(self.controller, "portfolio", None)
        positions = getattr(portfolio, "positions", {}) if portfolio is not None else {}

        if portfolio is None:
            self.risk_map.setImage(np.zeros((1, 1), dtype=float), autoLevels=False, levels=(0.0, 1.0))
            self._set_risk_heatmap_status("Portfolio data is not available yet.", "muted")
            return

        if not positions:
            self.risk_map.setImage(np.zeros((1, 1), dtype=float), autoLevels=False, levels=(0.0, 1.0))
            self._set_risk_heatmap_status("No open positions, so there is no live portfolio risk to map.", "muted")
            return

        risks = []

        for pos in positions.values():
            if not isinstance(pos, dict):
                continue

            risk = pos.get("risk")
            if risk is None:
                size = float(pos.get("size", pos.get("amount", 0)) or 0)
                entry = float(pos.get("entry_price", pos.get("price", 0)) or 0)
                risk = abs(size * entry)

            try:
                risk_value = abs(float(risk))
            except Exception:
                continue

            if risk_value > 0:
                risks.append(risk_value)

        if not risks:
            self.risk_map.setImage(np.zeros((1, 1), dtype=float), autoLevels=False, levels=(0.0, 1.0))
            self._set_risk_heatmap_status("Positions exist, but no usable risk values were found for them.", "warning")
            return

        data = np.array(risks, dtype=float).reshape(1, len(risks))
        max_value = float(np.max(data))

        if max_value <= 0:
            normalized = np.zeros_like(data)
        else:
            normalized = data / max_value

        self.risk_map.setImage(normalized, autoLevels=False, levels=(0.0, 1.0))
        self._set_risk_heatmap_status(
            f"Live risk snapshot across {len(risks)} position(s). Highest relative exposure: {max_value:,.2f}.",
            "positive",
        )




# ==========================================================
# TERMINAL HOTFIX OVERRIDES
# ==========================================================
# These overrides stabilize runtime paths without requiring a full terminal rewrite.


def _empty_candles_frame(pd_module):
    return pd_module.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])


def _normalize_candle_timestamps(pd_module, series):
    if pd_module.api.types.is_numeric_dtype(series):
        numeric = pd_module.to_numeric(series, errors="coerce")
        median = numeric.abs().median()
        unit = "ms" if pd_module.notna(median) and median > 1e11 else "s"
        return pd_module.to_datetime(numeric, unit=unit, errors="coerce", utc=True)

    return pd_module.to_datetime(series, errors="coerce", utc=True)


def candles_to_df(df):
    """Normalize and sanitize OHLCV rows before chart/backtest usage."""
    try:
        import pandas as pd
    except Exception:
        pd = None

    if df is None:
        return _empty_candles_frame(pd) if pd else []

    if pd is not None:
        try:
            frame = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
            if frame.empty:
                return _empty_candles_frame(pd)

            required = ["timestamp", "open", "high", "low", "close", "volume"]

            if not set(required).issubset(frame.columns):
                if frame.shape[1] < 6:
                    return _empty_candles_frame(pd)
                frame = frame.iloc[:, :6].copy()
                frame.columns = required
            else:
                frame = frame.loc[:, required].copy()

            frame["timestamp"] = _normalize_candle_timestamps(pd, frame["timestamp"])

            for column in ["open", "high", "low", "close", "volume"]:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")

            frame.replace([float("inf"), float("-inf")], pd.NA, inplace=True)
            frame.dropna(subset=["timestamp", "open", "high", "low", "close"], inplace=True)
            if frame.empty:
                return _empty_candles_frame(pd)

            # Repair inconsistent broker rows so chart bounds stay sane.
            price_bounds = frame[["open", "high", "low", "close"]]
            frame["high"] = price_bounds.max(axis=1)
            frame["low"] = price_bounds.min(axis=1)
            frame["volume"] = frame["volume"].fillna(0.0).clip(lower=0.0)

            frame.sort_values("timestamp", inplace=True)
            frame.drop_duplicates(subset=["timestamp"], keep="last", inplace=True)
            frame.reset_index(drop=True, inplace=True)
            return frame
        except Exception:
            return _empty_candles_frame(pd)

    return df


async def _hotfix_prepare_backtest_context(self):
    chart = self._current_chart_widget()
    if chart is None and hasattr(self, "_iter_chart_widgets"):
        charts = self._iter_chart_widgets()
        chart = charts[0] if charts else None

    symbol = getattr(chart, "symbol", None) or getattr(self, "symbol", None)
    timeframe = getattr(chart, "timeframe", None) or getattr(self, "current_timeframe", "1h")
    if not symbol:
        raise RuntimeError("Select a chart before starting a backtest")

    strategy_source = None
    trading_system = getattr(self.controller, "trading_system", None)
    if trading_system is not None:
        strategy_source = getattr(trading_system, "strategy", None)
    if strategy_source is None:
        from strategy.strategy_registry import StrategyRegistry
        strategy_source = StrategyRegistry()

    buffers = getattr(self.controller, "candle_buffers", {})
    frame = None
    if hasattr(buffers, "get"):
        frame = (buffers.get(symbol) or {}).get(timeframe)
    if frame is None and hasattr(self.controller, "request_candle_data"):
        await self.controller.request_candle_data(
            symbol=symbol,
            timeframe=timeframe,
            limit=max(500, int(getattr(self.controller, "limit", 1000) or 1000)),
        )
        frame = (getattr(self.controller, "candle_buffers", {}).get(symbol) or {}).get(timeframe)
    if frame is None or getattr(frame, "empty", False):
        raise RuntimeError(f"No candle history available for {symbol} {timeframe}")

    strategy_name = getattr(self.controller, "strategy_name", None) or getattr(getattr(self.controller, "config", None), "strategy", None)
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "data": frame.copy() if hasattr(frame, "copy") else frame,
        "strategy": strategy_source,
        "strategy_name": strategy_name,
    }


async def _hotfix_run_backtest_clicked(self):
    try:
        context = await _hotfix_prepare_backtest_context(self)

        self.backtest_engine = BacktestEngine(
            strategy=context["strategy"],
            simulator=Simulator(
                initial_balance=getattr(self.controller, "initial_capital", 10000)
            ),
        )
        self._backtest_context = context
        self.results = None
        self.backtest_report = None
        self._backtest_journal_lines = []
        self._backtest_running = False
        self._backtest_stop_requested = False
        self._backtest_stop_event = None
        self._backtest_task = None
        self._append_backtest_journal(
            f"Initialized strategy tester for {context['symbol']} {context['timeframe']} using {context.get('strategy_name') or 'Default'}."
        )
        self._show_backtest_window()
        self._refresh_backtest_window(message="Backtest engine initialized.")

    except Exception as e:
        self.system_console.log(f"Backtest initialization error: {e}")
        self._append_backtest_journal(f"Initialization failed: {e}", "ERROR")
        self._show_backtest_window()
        self._refresh_backtest_window(message=f"Backtest initialization error: {e}")


async def _hotfix_run_backtest_async(self, data, symbol, strategy_name, timeframe):
    try:
        self._append_backtest_journal(
            f"Starting backtest for {symbol} on {timeframe}.",
            "INFO",
        )
        self.results = await asyncio.to_thread(
            self.backtest_engine.run,
            data,
            symbol,
            strategy_name,
            self._backtest_stop_event,
        )
        self.backtest_report = ReportGenerator(
            trades=self.results,
            equity_history=getattr(self.backtest_engine, "equity_curve", []),
        ).generate()
        total_trades = len(self.results) if hasattr(self.results, "__len__") else 0
        if getattr(self, "_backtest_stop_requested", False):
            self.system_console.log("Backtest stopped.", "INFO")
            self._append_backtest_journal(
                f"Backtest stopped after {total_trades} trade rows and final equity {float(self.backtest_report.get('final_equity', 0.0) or 0.0):.2f}.",
                "WARN",
            )
            self._refresh_backtest_window(message="Backtest stopped.")
        else:
            self.system_console.log("Backtest completed.", "INFO")
            self._append_backtest_journal(
                f"Backtest completed with {total_trades} trade rows and final equity {float(self.backtest_report.get('final_equity', 0.0) or 0.0):.2f}.",
                "INFO",
            )
            self._refresh_backtest_window(message="Backtest completed.")

    except Exception as e:
        self.system_console.log(f"Backtest failed: {e}", "ERROR")
        self._append_backtest_journal(f"Backtest failed: {e}", "ERROR")
        self._refresh_backtest_window(message=f"Backtest failed: {e}")
    finally:
        self._backtest_running = False
        self._backtest_stop_event = None
        self._backtest_task = None
        self._refresh_backtest_window()


def _hotfix_start_backtest(self):
    if getattr(self, "_backtest_running", False):
        self.stop_backtest()
        return

    try:
        if not hasattr(self, "backtest_engine"):
            self.system_console.log("Backtest engine not initialized.")
            self._append_backtest_journal("Backtest engine not initialized.", "ERROR")
            self._refresh_backtest_window(message="Backtest engine not initialized.")
            return

        backtest_context = getattr(self, "_backtest_context", {}) or {}
        symbol = backtest_context.get("symbol", "BACKTEST")
        strategy_name = backtest_context.get("strategy_name")
        timeframe = backtest_context.get("timeframe", "-")
        data = candles_to_df(backtest_context.get("data"))
        if data is None or not hasattr(data, "__len__") or len(data) == 0:
            self.system_console.log("No historical data available for backtesting.")
            self._append_backtest_journal("No historical data available for backtesting.", "ERROR")
            self._refresh_backtest_window(message="No historical data available for backtesting.")
            return

        self.results = None
        self.backtest_report = None
        self._backtest_running = True
        self._backtest_stop_requested = False
        self._backtest_stop_event = threading.Event()
        self._refresh_backtest_window(message="Backtest running...")
        runner = _hotfix_run_backtest_async(self, data, symbol, strategy_name, timeframe)
        create_task = getattr(self.controller, "_create_task", None)
        if callable(create_task):
            self._backtest_task = create_task(runner, "backtest_run")
        else:
            self._backtest_task = asyncio.create_task(runner)
    except Exception as e:
        self._backtest_running = False
        self._backtest_stop_event = None
        self.system_console.log(f"Backtest failed to start: {e}", "ERROR")
        self._append_backtest_journal(f"Backtest failed to start: {e}", "ERROR")
        self._refresh_backtest_window(message=f"Backtest failed to start: {e}")


def _hotfix_stop_backtest(self):
    if not getattr(self, "_backtest_running", False):
        self._refresh_backtest_window(message="No backtest is currently running.")
        return

    self._backtest_stop_requested = True
    stop_event = getattr(self, "_backtest_stop_event", None)
    if stop_event is not None:
        stop_event.set()
    self.system_console.log("Backtest stop requested.", "INFO")
    self._append_backtest_journal("Backtest stop requested.", "WARN")
    self._refresh_backtest_window(message="Backtest stop requested...")


def _hotfix_generate_report(self):
    try:
        trades = getattr(self, "results", None)
        if trades is None:
            raise RuntimeError("Run a backtest before generating a report")

        generator = ReportGenerator(
            trades=trades,
            equity_history=getattr(self.backtest_engine, "equity_curve", []),
        )
        pdf_path = generator.export_pdf()
        excel_path = generator.export_excel()
        self.backtest_report = generator.generate()
        self.system_console.log(f"Backtest report generated: {pdf_path} | {excel_path}", "INFO")
        self._append_backtest_journal(
            f"Report exported to {pdf_path} and {excel_path}.",
            "INFO",
        )
        self._refresh_backtest_window(message="Backtest report generated.")
    except Exception as e:
        self.system_console.log(f"Report generation failed: {e}")
        self._append_backtest_journal(f"Report generation failed: {e}", "ERROR")


def _hotfix_show_optimization_window(self):
    window = self._get_or_create_tool_window(
        "strategy_optimization",
        "Strategy Optimization",
        width=980,
        height=640,
    )

    if getattr(window, "_optimization_container", None) is None:
        container = QWidget()
        layout = QVBoxLayout(container)

        status = QLabel("Optimization workspace ready.")
        status.setStyleSheet("color: #e6edf7; font-weight: 700;")
        layout.addWidget(status)

        controls = QHBoxLayout()
        run_btn = QPushButton("Run Optimization")
        apply_btn = QPushButton("Apply Best Params")
        run_btn.clicked.connect(lambda: asyncio.get_event_loop().create_task(self._run_strategy_optimization()))
        apply_btn.clicked.connect(self._apply_best_optimization_params)
        controls.addWidget(run_btn)
        controls.addWidget(apply_btn)
        controls.addStretch()
        layout.addLayout(controls)

        summary = QLabel("-")
        summary.setWordWrap(True)
        summary.setStyleSheet("color: #9fb0c7;")
        layout.addWidget(summary)

        table = QTableWidget()
        table.setColumnCount(8)
        table.setHorizontalHeaderLabels(
            [
                "RSI",
                "EMA Fast",
                "EMA Slow",
                "ATR",
                "Profit",
                "Sharpe",
                "Win Rate",
                "Final Equity",
            ]
        )
        layout.addWidget(table)

        window.setCentralWidget(container)
        window._optimization_container = container
        window._optimization_status = status
        window._optimization_summary = summary
        window._optimization_table = table
        window._optimization_run_btn = run_btn
        window._optimization_apply_btn = apply_btn

    self._refresh_optimization_window(window)
    window.show()
    window.raise_()
    window.activateWindow()
    return window


def _hotfix_refresh_optimization_window(self, window=None, message=None):
    window = window or self.detached_tool_windows.get("strategy_optimization")
    if window is None:
        return

    status = getattr(window, "_optimization_status", None)
    summary = getattr(window, "_optimization_summary", None)
    table = getattr(window, "_optimization_table", None)
    run_btn = getattr(window, "_optimization_run_btn", None)
    apply_btn = getattr(window, "_optimization_apply_btn", None)
    if status is None or summary is None or table is None:
        return

    context = getattr(self, "_optimization_context", {}) or {}
    symbol = context.get("symbol", "-")
    timeframe = context.get("timeframe", "-")
    strategy_name = context.get("strategy_name", None) or getattr(self.controller, "strategy_name", None) or getattr(getattr(self.controller, "config", None), "strategy", "Trend Following")
    dataset = context.get("data")
    candle_count = len(dataset) if hasattr(dataset, "__len__") else 0

    if message is not None:
        self._optimization_status_message = message

    running = bool(getattr(self, "_optimization_running", False))
    status_message = getattr(self, "_optimization_status_message", None)
    status.setText(status_message or ("Optimization running..." if running else "Optimization workspace ready."))
    summary.setText(f"Symbol: {symbol} | Timeframe: {timeframe} | Strategy: {strategy_name} | Candles: {candle_count}")
    if run_btn is not None:
        run_btn.setEnabled(not running)
        run_btn.setText("Running..." if running else "Run Optimization")
    if apply_btn is not None:
        apply_btn.setEnabled((not running) and isinstance(getattr(self, "optimization_best", None), dict))

    results = getattr(self, "optimization_results", None)
    if results is None or getattr(results, "empty", True):
        table.setRowCount(0)
        return

    display = results.head(25).reset_index(drop=True)
    table.setRowCount(len(display))

    columns = [
        ("rsi_period", "{:g}"),
        ("ema_fast", "{:g}"),
        ("ema_slow", "{:g}"),
        ("atr_period", "{:g}"),
        ("total_profit", "{:.2f}"),
        ("sharpe_ratio", "{:.3f}"),
        ("win_rate", "{:.2%}"),
        ("final_equity", "{:.2f}"),
    ]

    for row_idx, (_, row) in enumerate(display.iterrows()):
        for col_idx, (column, fmt) in enumerate(columns):
            value = row.get(column, "")
            try:
                text = fmt.format(float(value))
            except Exception:
                text = str(value)
            table.setItem(row_idx, col_idx, QTableWidgetItem(text))

    table.resizeColumnsToContents()


async def _hotfix_run_strategy_optimization(self):
    if getattr(self, "_optimization_running", False):
        self._show_optimization_window()
        self._refresh_optimization_window(message="Optimization is already running.")
        return

    try:
        from backtesting.optimizer import StrategyOptimizer

        context = await _hotfix_prepare_backtest_context(self)
        data = candles_to_df(context.get("data"))
        if data is None or not hasattr(data, "__len__") or len(data) == 0:
            raise RuntimeError("No historical data available for optimization")

        optimizer = StrategyOptimizer(
            strategy=context["strategy"],
            initial_balance=getattr(self.controller, "initial_capital", 10000),
        )
        self._optimization_running = True
        self._optimization_status_message = "Optimization running..."
        self._optimization_context = context
        self._show_optimization_window()
        self._refresh_optimization_window(message="Optimization running...")
        await asyncio.sleep(0)

        self.optimization_results = await asyncio.to_thread(
            optimizer.optimize,
            data,
            context["symbol"],
            context.get("strategy_name"),
        )
        self.optimization_best = None
        if self.optimization_results is not None and not self.optimization_results.empty:
            self.optimization_best = self.optimization_results.iloc[0].to_dict()

        self.system_console.log("Strategy optimization completed.", "INFO")
        self._optimization_status_message = "Strategy optimization completed."
        self._show_optimization_window()
        self._refresh_optimization_window(message="Strategy optimization completed.")

    except Exception as e:
        self.system_console.log(f"Strategy optimization failed: {e}", "ERROR")
        self._optimization_status_message = f"Strategy optimization failed: {e}"
        self._show_optimization_window()
        self._refresh_optimization_window(message=f"Strategy optimization failed: {e}")
    finally:
        self._optimization_running = False
        self._refresh_optimization_window()


def _hotfix_apply_best_optimization_params(self):
    try:
        best = getattr(self, "optimization_best", None)
        if not isinstance(best, dict):
            raise RuntimeError("Run optimization before applying parameters")

        context = getattr(self, "_optimization_context", {}) or {}
        strategy_source = context.get("strategy")
        strategy_name = context.get("strategy_name")

        if strategy_source is None:
            raise RuntimeError("No strategy context available")

        if hasattr(strategy_source, "_resolve_strategy"):
            target = strategy_source._resolve_strategy(strategy_name)
        else:
            target = strategy_source

        applied = []
        for key in ["rsi_period", "ema_fast", "ema_slow", "atr_period"]:
            if key in best and hasattr(target, key):
                setattr(target, key, int(best[key]))
                applied.append(f"{key}={int(best[key])}")

        strategy_params = dict(getattr(self.controller, "strategy_params", {}) or {})
        for key in ["rsi_period", "ema_fast", "ema_slow", "atr_period"]:
            if key in best:
                strategy_params[key] = int(best[key])
        self.controller.strategy_params = strategy_params
        self.settings.setValue("strategy/rsi_period", strategy_params.get("rsi_period", 14))
        self.settings.setValue("strategy/ema_fast", strategy_params.get("ema_fast", 20))
        self.settings.setValue("strategy/ema_slow", strategy_params.get("ema_slow", 50))
        self.settings.setValue("strategy/atr_period", strategy_params.get("atr_period", 14))

        if not applied:
            raise RuntimeError("No compatible strategy parameters were available to apply")

        self.system_console.log(f"Applied optimized params: {', '.join(applied)}", "INFO")
        self._refresh_optimization_window(message="Applied best optimization parameters.")

    except Exception as e:
        self.system_console.log(f"Apply optimization failed: {e}", "ERROR")
        self._refresh_optimization_window(message=f"Apply optimization failed: {e}")


def _hotfix_optimize_strategy(self):
    self._show_optimization_window()
    task_factory = getattr(self.controller, "_create_task", None)
    if callable(task_factory):
        task_factory(self._run_strategy_optimization(), "strategy_optimization")
        return
    asyncio.get_event_loop().create_task(self._run_strategy_optimization())


async def _hotfix_reload_chart_data(self, symbol, timeframe):
    try:
        df = None

        # Preferred cache shape: candle_buffers[symbol][timeframe]
        buffers = getattr(self.controller, "candle_buffers", None)
        if hasattr(buffers, "get"):
            symbol_bucket = buffers.get(symbol)
            if hasattr(symbol_bucket, "get"):
                df = symbol_bucket.get(timeframe)

        # Fallback to legacy candle_buffer store.
        if df is None:
            legacy = getattr(self.controller, "candle_buffer", None)
            if hasattr(legacy, "get"):
                symbol_bucket = legacy.get(symbol)
                if hasattr(symbol_bucket, "get"):
                    df = symbol_bucket.get(timeframe)
                elif symbol_bucket is not None:
                    df = symbol_bucket

                if df is None:
                    df = legacy.get(timeframe)

        if df is None:
            return

        self._update_chart(symbol, df)

    except Exception as e:
        self.logger.error(f"Timeframe reload failed: {e}")


def _hotfix_open_risk_settings(self):
    self._show_settings_window()


def _hotfix_save_settings(self):
    try:
        self._show_settings_window()
    except Exception as e:
        self.logger.error(f"Risk settings error: {e}")


def _hotfix_settings_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _hotfix_settings_float(value, default):
    try:
        return float(value)
    except Exception:
        return default


def _hotfix_settings_int(value, default):
    try:
        return int(float(value))
    except Exception:
        return default


def _hotfix_get_live_risk_engine(self):
    trading_system = getattr(self.controller, "trading_system", None)
    risk_engine = getattr(trading_system, "risk_engine", None)
    if risk_engine is not None:
        return risk_engine
    return getattr(self.controller, "risk_engine", None)


def _hotfix_build_strategy_params(values, controller):
    current = dict(getattr(controller, "strategy_params", {}) or {})
    defaults = {
        "rsi_period": int(current.get("rsi_period", 14)),
        "ema_fast": int(current.get("ema_fast", 20)),
        "ema_slow": int(current.get("ema_slow", 50)),
        "atr_period": int(current.get("atr_period", 14)),
        "oversold_threshold": float(current.get("oversold_threshold", 35.0)),
        "overbought_threshold": float(current.get("overbought_threshold", 65.0)),
        "breakout_lookback": int(current.get("breakout_lookback", 20)),
        "min_confidence": float(current.get("min_confidence", 0.55)),
        "signal_amount": float(current.get("signal_amount", 1.0)),
    }
    params = {
        "rsi_period": max(2, int(values.get("strategy_rsi_period", defaults["rsi_period"]))),
        "ema_fast": max(2, int(values.get("strategy_ema_fast", defaults["ema_fast"]))),
        "ema_slow": max(3, int(values.get("strategy_ema_slow", defaults["ema_slow"]))),
        "atr_period": max(2, int(values.get("strategy_atr_period", defaults["atr_period"]))),
        "oversold_threshold": float(values.get("strategy_oversold_threshold", defaults["oversold_threshold"])),
        "overbought_threshold": float(values.get("strategy_overbought_threshold", defaults["overbought_threshold"])),
        "breakout_lookback": max(2, int(values.get("strategy_breakout_lookback", defaults["breakout_lookback"]))),
        "min_confidence": max(0.0, min(1.0, float(values.get("strategy_min_confidence", defaults["min_confidence"])))),
        "signal_amount": max(0.0001, float(values.get("strategy_signal_amount", defaults["signal_amount"]))),
    }
    if params["ema_fast"] >= params["ema_slow"]:
        params["ema_fast"] = max(2, min(params["ema_fast"], params["ema_slow"] - 1))
    if params["oversold_threshold"] >= params["overbought_threshold"]:
        params["oversold_threshold"] = min(params["oversold_threshold"], params["overbought_threshold"] - 1.0)
    return params


def _hotfix_update_color_button(button, color):
    if button is None:
        return
    button.setText(color)
    button.setStyleSheet(
        """
        QPushButton {
            background-color: %s;
            color: white;
            border: 1px solid #31415f;
            border-radius: 8px;
            padding: 6px 10px;
            font-weight: 700;
        }
        """
        % color
    )


def _hotfix_pick_settings_color(window, attr_name, button, title):
    current = getattr(window, attr_name, "#26a69a")
    picked = QColorDialog.getColor(QColor(current), window, title)
    if not picked.isValid():
        return
    color = picked.name()
    setattr(window, attr_name, color)
    _hotfix_update_color_button(button, color)


def _hotfix_collect_settings_values(self, window=None):
    if window is None:
        window = self.detached_tool_windows.get("application_settings")
    if window is None:
        return None

    return {
        "timeframe": window._settings_timeframe.currentText(),
        "order_type": window._settings_order_type.currentText(),
        "history_limit": int(window._settings_history_limit.value()),
        "initial_capital": float(window._settings_initial_capital.value()),
        "refresh_interval_ms": int(window._settings_refresh_ms.value()),
        "orderbook_interval_ms": int(window._settings_orderbook_ms.value()),
        "show_bid_ask_lines": window._settings_bid_ask_mode.currentData(),
        "candle_up_color": getattr(window, "_settings_up_color", self.candle_up_color),
        "candle_down_color": getattr(window, "_settings_down_color", self.candle_down_color),
        "max_portfolio_risk": float(window._settings_max_portfolio.value()),
        "max_risk_per_trade": float(window._settings_max_trade.value()),
        "max_position_size_pct": float(window._settings_max_position.value()),
        "max_gross_exposure_pct": float(window._settings_max_gross.value()),
        "strategy_name": window._settings_strategy_name.currentText(),
        "strategy_rsi_period": int(window._settings_strategy_rsi_period.value()),
        "strategy_ema_fast": int(window._settings_strategy_ema_fast.value()),
        "strategy_ema_slow": int(window._settings_strategy_ema_slow.value()),
        "strategy_atr_period": int(window._settings_strategy_atr_period.value()),
        "strategy_oversold_threshold": float(window._settings_strategy_oversold.value()),
        "strategy_overbought_threshold": float(window._settings_strategy_overbought.value()),
        "strategy_breakout_lookback": int(window._settings_strategy_breakout.value()),
        "strategy_min_confidence": float(window._settings_strategy_confidence.value()),
        "strategy_signal_amount": float(window._settings_strategy_amount.value()),
        "telegram_enabled": window._settings_telegram_enabled.currentData(),
        "telegram_bot_token": window._settings_telegram_bot_token.text().strip(),
        "telegram_chat_id": window._settings_telegram_chat_id.text().strip(),
        "openai_api_key": window._settings_openai_api_key.text().strip(),
        "openai_model": window._settings_openai_model.text().strip(),
    }


def _hotfix_apply_settings_values(self, values, persist=True, reload_chart=False):
    if not isinstance(values, dict):
        return

    timeframe = values.get("timeframe", getattr(self, "current_timeframe", "1h"))
    order_type = values.get("order_type", getattr(self, "order_type", "limit"))
    history_limit = max(100, int(values.get("history_limit", getattr(self.controller, "limit", 50000))))
    initial_capital = max(0.0, float(values.get("initial_capital", getattr(self.controller, "initial_capital", 10000))))
    refresh_interval_ms = max(250, int(values.get("refresh_interval_ms", 1000)))
    orderbook_interval_ms = max(250, int(values.get("orderbook_interval_ms", 1500)))
    show_bid_ask_lines = bool(values.get("show_bid_ask_lines", getattr(self, "show_bid_ask_lines", True)))
    candle_up_color = values.get("candle_up_color", getattr(self, "candle_up_color", "#26a69a"))
    candle_down_color = values.get("candle_down_color", getattr(self, "candle_down_color", "#ef5350"))
    strategy_name = Strategy.normalize_strategy_name(
        values.get("strategy_name", getattr(self.controller, "strategy_name", "Trend Following"))
    )
    strategy_params = _hotfix_build_strategy_params(values, self.controller)

    self.current_timeframe = timeframe
    self.order_type = order_type
    self.controller.time_frame = timeframe
    self.controller.order_type = order_type
    self.controller.limit = history_limit
    self.controller.initial_capital = initial_capital
    self.controller.max_portfolio_risk = float(values.get("max_portfolio_risk", getattr(self.controller, "max_portfolio_risk", 0.2)))
    self.controller.max_risk_per_trade = float(values.get("max_risk_per_trade", getattr(self.controller, "max_risk_per_trade", 0.02)))
    self.controller.max_position_size_pct = float(values.get("max_position_size_pct", getattr(self.controller, "max_position_size_pct", 0.05)))
    self.controller.max_gross_exposure_pct = float(values.get("max_gross_exposure_pct", getattr(self.controller, "max_gross_exposure_pct", 1.0)))
    self.controller.strategy_name = strategy_name
    self.controller.strategy_params = strategy_params
    if hasattr(self.controller, "update_integration_settings"):
        self.controller.update_integration_settings(
            telegram_enabled=bool(values.get("telegram_enabled", getattr(self.controller, "telegram_enabled", False))),
            telegram_bot_token=values.get("telegram_bot_token", getattr(self.controller, "telegram_bot_token", "")),
            telegram_chat_id=values.get("telegram_chat_id", getattr(self.controller, "telegram_chat_id", "")),
            openai_api_key=values.get("openai_api_key", getattr(self.controller, "openai_api_key", "")),
            openai_model=values.get("openai_model", getattr(self.controller, "openai_model", "gpt-5-mini")),
        )

    self.candle_up_color = candle_up_color
    self.candle_down_color = candle_down_color
    self.show_bid_ask_lines = show_bid_ask_lines

    config = getattr(self.controller, "config", None)
    if config is not None and hasattr(config, "strategy"):
        try:
            config.strategy = strategy_name
        except Exception:
            pass

    if hasattr(self.controller, "candle_buffer") and hasattr(self.controller.candle_buffer, "max_length"):
        self.controller.candle_buffer.max_length = history_limit
    if hasattr(self.controller, "ticker_buffer") and hasattr(self.controller.ticker_buffer, "max_length"):
        self.controller.ticker_buffer.max_length = history_limit

    trading_system = getattr(self.controller, "trading_system", None)
    if trading_system is not None:
        setattr(trading_system, "time_frame", timeframe)
        setattr(trading_system, "limit", history_limit)
        strategy_registry = getattr(trading_system, "strategy", None)
        if strategy_registry is not None and hasattr(strategy_registry, "configure"):
            strategy_registry.configure(strategy_name=strategy_name, params=strategy_params)

    risk_engine = _hotfix_get_live_risk_engine(self)
    if risk_engine is not None:
        risk_engine.account_equity = initial_capital
        risk_engine.max_portfolio_risk = self.controller.max_portfolio_risk
        risk_engine.max_risk_per_trade = self.controller.max_risk_per_trade
        risk_engine.max_position_size_pct = self.controller.max_position_size_pct
        risk_engine.max_gross_exposure_pct = self.controller.max_gross_exposure_pct

    self._set_active_timeframe_button(timeframe)
    self._apply_candle_colors_to_all_charts()

    toggle_action = getattr(self, "toggle_bid_ask_lines_action", None)
    if toggle_action is not None:
        blocked = toggle_action.blockSignals(True)
        toggle_action.setChecked(show_bid_ask_lines)
        toggle_action.blockSignals(blocked)

    for chart in self._iter_chart_widgets():
        chart.set_candle_colors(candle_up_color, candle_down_color)
        chart.set_bid_ask_lines_visible(show_bid_ask_lines)

    if hasattr(self, "refresh_timer") and self.refresh_timer is not None:
        self.refresh_timer.start(refresh_interval_ms)
    if hasattr(self, "orderbook_timer") and self.orderbook_timer is not None:
        self.orderbook_timer.start(orderbook_interval_ms)

    current_chart = self._current_chart_widget()
    if isinstance(current_chart, ChartWidget):
        current_chart.timeframe = timeframe
        if hasattr(current_chart, "refresh_context_display"):
            current_chart.refresh_context_display()
        current_index = self.chart_tabs.currentIndex() if self._chart_tabs_ready() else -1
        if current_index >= 0:
            current_page = self.chart_tabs.widget(current_index)
            current_charts = self._chart_widgets_in_page(current_page)
            if len(current_charts) == 1 and current_charts[0] is current_chart:
                self.chart_tabs.setTabText(current_index, f"{current_chart.symbol} ({timeframe})")
        if reload_chart and hasattr(self.controller, "request_candle_data"):
            asyncio.get_event_loop().create_task(
                self.controller.request_candle_data(
                    symbol=current_chart.symbol,
                    timeframe=timeframe,
                    limit=history_limit,
                )
            )
        asyncio.get_event_loop().create_task(
            self._reload_chart_data(current_chart.symbol, timeframe)
        )
        self._request_active_orderbook()

    if persist:
        self.settings.setValue("geometry", self.saveGeometry())
        self.settings.setValue("windowState", self.saveState())
        self.settings.setValue("chart/candle_up_color", candle_up_color)
        self.settings.setValue("chart/candle_down_color", candle_down_color)
        self.settings.setValue("terminal/current_timeframe", timeframe)
        self.settings.setValue("terminal/order_type", order_type)
        self.settings.setValue("terminal/history_limit", history_limit)
        self.settings.setValue("terminal/initial_capital", initial_capital)
        self.settings.setValue("terminal/refresh_interval_ms", refresh_interval_ms)
        self.settings.setValue("terminal/orderbook_interval_ms", orderbook_interval_ms)
        self.settings.setValue("terminal/show_bid_ask_lines", show_bid_ask_lines)
        self.settings.setValue("risk/max_portfolio_risk", self.controller.max_portfolio_risk)
        self.settings.setValue("risk/max_risk_per_trade", self.controller.max_risk_per_trade)
        self.settings.setValue("risk/max_position_size_pct", self.controller.max_position_size_pct)
        self.settings.setValue("risk/max_gross_exposure_pct", self.controller.max_gross_exposure_pct)
        self.settings.setValue("strategy/name", strategy_name)
        self.settings.setValue("strategy/rsi_period", strategy_params["rsi_period"])
        self.settings.setValue("strategy/ema_fast", strategy_params["ema_fast"])
        self.settings.setValue("strategy/ema_slow", strategy_params["ema_slow"])
        self.settings.setValue("strategy/atr_period", strategy_params["atr_period"])
        self.settings.setValue("strategy/oversold_threshold", strategy_params["oversold_threshold"])
        self.settings.setValue("strategy/overbought_threshold", strategy_params["overbought_threshold"])
        self.settings.setValue("strategy/breakout_lookback", strategy_params["breakout_lookback"])
        self.settings.setValue("strategy/min_confidence", strategy_params["min_confidence"])
        self.settings.setValue("strategy/signal_amount", strategy_params["signal_amount"])
        self.settings.setValue("integrations/telegram_enabled", bool(values.get("telegram_enabled", getattr(self.controller, "telegram_enabled", False))))
        self.settings.setValue("integrations/telegram_bot_token", values.get("telegram_bot_token", getattr(self.controller, "telegram_bot_token", "")))
        self.settings.setValue("integrations/telegram_chat_id", values.get("telegram_chat_id", getattr(self.controller, "telegram_chat_id", "")))
        self.settings.setValue("integrations/openai_api_key", values.get("openai_api_key", getattr(self.controller, "openai_api_key", "")))
        self.settings.setValue("integrations/openai_model", values.get("openai_model", getattr(self.controller, "openai_model", "gpt-5-mini")))
        self._save_detached_chart_layouts()


def _hotfix_show_settings_window(self):
    window = self._get_or_create_tool_window(
        "application_settings",
        "Settings",
        width=680,
        height=700,
    )

    if getattr(window, "_settings_container", None) is None:
        container = QWidget()
        layout = QVBoxLayout(container)

        intro = QLabel("Configure trading defaults, chart behavior, refresh timing, and risk in one place.")
        intro.setWordWrap(True)
        intro.setStyleSheet("color: #c9d5e8; font-weight: 600; padding: 4px 0 10px 0;")
        layout.addWidget(intro)

        tabs = QTabWidget()
        layout.addWidget(tabs)

        general_tab = QWidget()
        general_form = QFormLayout(general_tab)

        timeframe = QComboBox()
        timeframe.addItems(["1m", "5m", "15m", "1h", "4h", "1d"])
        order_type = QComboBox()
        order_type.addItems(["market", "limit"])

        history_limit = QDoubleSpinBox()
        history_limit.setDecimals(0)
        history_limit.setRange(100, 50000)
        history_limit.setSingleStep(500)
        history_limit.setSuffix(" candles")
        history_limit.setToolTip("Maximum number of candles to request and keep for charts and backtesting.")

        initial_capital = QDoubleSpinBox()
        initial_capital.setDecimals(2)
        initial_capital.setRange(0, 1000000000)
        initial_capital.setSingleStep(1000)

        refresh_ms = QDoubleSpinBox()
        refresh_ms.setDecimals(0)
        refresh_ms.setRange(250, 60000)
        refresh_ms.setSingleStep(250)

        orderbook_ms = QDoubleSpinBox()
        orderbook_ms.setDecimals(0)
        orderbook_ms.setRange(250, 60000)
        orderbook_ms.setSingleStep(250)

        general_form.addRow("Default timeframe", timeframe)
        general_form.addRow("Default order type", order_type)
        general_form.addRow("History limit (candles)", history_limit)
        general_form.addRow("Initial capital", initial_capital)
        general_form.addRow("Terminal refresh (ms)", refresh_ms)
        general_form.addRow("Orderbook refresh (ms)", orderbook_ms)
        tabs.addTab(general_tab, "General")

        display_tab = QWidget()
        display_form = QFormLayout(display_tab)

        bid_ask_mode = QComboBox()
        bid_ask_mode.addItem("Show", True)
        bid_ask_mode.addItem("Hide", False)

        up_color_btn = QPushButton()
        down_color_btn = QPushButton()
        up_color_btn.clicked.connect(
            lambda: _hotfix_pick_settings_color(
                window,
                "_settings_up_color",
                up_color_btn,
                "Select Bullish Candle Color",
            )
        )
        down_color_btn.clicked.connect(
            lambda: _hotfix_pick_settings_color(
                window,
                "_settings_down_color",
                down_color_btn,
                "Select Bearish Candle Color",
            )
        )

        display_form.addRow("Bid/ask guide lines", bid_ask_mode)
        display_form.addRow("Bullish candle color", up_color_btn)
        display_form.addRow("Bearish candle color", down_color_btn)
        tabs.addTab(display_tab, "Display")

        risk_tab = QWidget()
        risk_form = QFormLayout(risk_tab)

        max_portfolio = QDoubleSpinBox()
        max_portfolio.setDecimals(4)
        max_portfolio.setRange(0, 100000)
        max_portfolio.setSingleStep(0.01)

        max_trade = QDoubleSpinBox()
        max_trade.setDecimals(4)
        max_trade.setRange(0, 100000)
        max_trade.setSingleStep(0.01)

        max_position = QDoubleSpinBox()
        max_position.setDecimals(4)
        max_position.setRange(0, 100000)
        max_position.setSingleStep(0.01)

        max_gross = QDoubleSpinBox()
        max_gross.setDecimals(4)
        max_gross.setRange(0, 100000)
        max_gross.setSingleStep(0.01)

        risk_form.addRow("Max portfolio risk", max_portfolio)
        risk_form.addRow("Max risk per trade", max_trade)
        risk_form.addRow("Max position size", max_position)
        risk_form.addRow("Max gross exposure", max_gross)
        tabs.addTab(risk_tab, "Risk")

        strategy_tab = QWidget()
        strategy_form = QFormLayout(strategy_tab)

        strategy_name = QComboBox()
        strategy_name.addItems(["Trend Following", "Mean Reversion", "Breakout", "AI Hybrid"])

        strategy_rsi_period = QDoubleSpinBox()
        strategy_rsi_period.setDecimals(0)
        strategy_rsi_period.setRange(2, 500)
        strategy_rsi_period.setSingleStep(1)

        strategy_ema_fast = QDoubleSpinBox()
        strategy_ema_fast.setDecimals(0)
        strategy_ema_fast.setRange(2, 500)
        strategy_ema_fast.setSingleStep(1)

        strategy_ema_slow = QDoubleSpinBox()
        strategy_ema_slow.setDecimals(0)
        strategy_ema_slow.setRange(3, 1000)
        strategy_ema_slow.setSingleStep(1)

        strategy_atr_period = QDoubleSpinBox()
        strategy_atr_period.setDecimals(0)
        strategy_atr_period.setRange(2, 500)
        strategy_atr_period.setSingleStep(1)

        strategy_oversold = QDoubleSpinBox()
        strategy_oversold.setDecimals(1)
        strategy_oversold.setRange(0, 100)
        strategy_oversold.setSingleStep(1)

        strategy_overbought = QDoubleSpinBox()
        strategy_overbought.setDecimals(1)
        strategy_overbought.setRange(0, 100)
        strategy_overbought.setSingleStep(1)

        strategy_breakout = QDoubleSpinBox()
        strategy_breakout.setDecimals(0)
        strategy_breakout.setRange(2, 500)
        strategy_breakout.setSingleStep(1)

        strategy_confidence = QDoubleSpinBox()
        strategy_confidence.setDecimals(2)
        strategy_confidence.setRange(0, 1)
        strategy_confidence.setSingleStep(0.01)

        strategy_amount = QDoubleSpinBox()
        strategy_amount.setDecimals(4)
        strategy_amount.setRange(0.0001, 1000000)
        strategy_amount.setSingleStep(0.01)

        strategy_form.addRow("Active strategy", strategy_name)
        strategy_form.addRow("RSI period", strategy_rsi_period)
        strategy_form.addRow("EMA fast", strategy_ema_fast)
        strategy_form.addRow("EMA slow", strategy_ema_slow)
        strategy_form.addRow("ATR period", strategy_atr_period)
        strategy_form.addRow("Oversold threshold", strategy_oversold)
        strategy_form.addRow("Overbought threshold", strategy_overbought)
        strategy_form.addRow("Breakout lookback", strategy_breakout)
        strategy_form.addRow("AI min confidence", strategy_confidence)
        strategy_form.addRow("Signal amount", strategy_amount)
        tabs.addTab(strategy_tab, "Strategy")

        integrations_tab = QWidget()
        integrations_form = QFormLayout(integrations_tab)

        telegram_enabled = QComboBox()
        telegram_enabled.addItem("Disabled", False)
        telegram_enabled.addItem("Enabled", True)

        telegram_bot_token = QLineEdit()
        telegram_bot_token.setPlaceholderText("Telegram bot token")

        telegram_chat_id = QLineEdit()
        telegram_chat_id.setPlaceholderText("Telegram chat ID")

        openai_api_key = QLineEdit()
        openai_api_key.setEchoMode(QLineEdit.EchoMode.Password)
        openai_api_key.setPlaceholderText("OpenAI API key")

        openai_model = QLineEdit()
        openai_model.setPlaceholderText("gpt-5-mini")

        integrations_form.addRow("Telegram notifications", telegram_enabled)
        integrations_form.addRow("Telegram bot token", telegram_bot_token)
        integrations_form.addRow("Telegram chat ID", telegram_chat_id)
        integrations_form.addRow("OpenAI API key", openai_api_key)
        integrations_form.addRow("OpenAI model", openai_model)
        tabs.addTab(integrations_tab, "Integrations")

        summary = QLabel("-")
        summary.setWordWrap(True)
        summary.setStyleSheet("color: #9fb0c7; padding-top: 8px;")
        layout.addWidget(summary)

        actions = QHBoxLayout()
        exposure_btn = QPushButton("Open Portfolio Exposure")
        exposure_btn.clicked.connect(self._show_portfolio_exposure)
        apply_btn = QPushButton("Save Settings")
        apply_btn.setStyleSheet(self._action_button_style())
        apply_btn.clicked.connect(lambda: self._apply_settings_window(window))
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(window.close)
        actions.addWidget(exposure_btn)
        actions.addStretch()
        actions.addWidget(apply_btn)
        actions.addWidget(close_btn)
        layout.addLayout(actions)

        window.setCentralWidget(container)
        window._settings_container = container
        window._settings_tabs = tabs
        window._settings_timeframe = timeframe
        window._settings_order_type = order_type
        window._settings_history_limit = history_limit
        window._settings_initial_capital = initial_capital
        window._settings_refresh_ms = refresh_ms
        window._settings_orderbook_ms = orderbook_ms
        window._settings_bid_ask_mode = bid_ask_mode
        window._settings_up_button = up_color_btn
        window._settings_down_button = down_color_btn
        window._settings_max_portfolio = max_portfolio
        window._settings_max_trade = max_trade
        window._settings_max_position = max_position
        window._settings_max_gross = max_gross
        window._settings_strategy_name = strategy_name
        window._settings_strategy_rsi_period = strategy_rsi_period
        window._settings_strategy_ema_fast = strategy_ema_fast
        window._settings_strategy_ema_slow = strategy_ema_slow
        window._settings_strategy_atr_period = strategy_atr_period
        window._settings_strategy_oversold = strategy_oversold
        window._settings_strategy_overbought = strategy_overbought
        window._settings_strategy_breakout = strategy_breakout
        window._settings_strategy_confidence = strategy_confidence
        window._settings_strategy_amount = strategy_amount
        window._settings_telegram_enabled = telegram_enabled
        window._settings_telegram_bot_token = telegram_bot_token
        window._settings_telegram_chat_id = telegram_chat_id
        window._settings_openai_api_key = openai_api_key
        window._settings_openai_model = openai_model
        window._settings_summary = summary

    risk_engine = _hotfix_get_live_risk_engine(self)
    refresh_interval = 1000
    if hasattr(self, "refresh_timer") and self.refresh_timer is not None:
        refresh_interval = max(250, self.refresh_timer.interval())
    orderbook_interval = 1500
    if hasattr(self, "orderbook_timer") and self.orderbook_timer is not None:
        orderbook_interval = max(250, self.orderbook_timer.interval())

    window._settings_timeframe.setCurrentText(getattr(self, "current_timeframe", getattr(self.controller, "time_frame", "1h")))
    window._settings_order_type.setCurrentText(getattr(self, "order_type", getattr(self.controller, "order_type", "limit")))
    window._settings_history_limit.setValue(float(getattr(self.controller, "limit", 50000)))
    window._settings_initial_capital.setValue(float(getattr(self.controller, "initial_capital", 10000)))
    window._settings_refresh_ms.setValue(float(refresh_interval))
    window._settings_orderbook_ms.setValue(float(orderbook_interval))
    window._settings_bid_ask_mode.setCurrentIndex(0 if getattr(self, "show_bid_ask_lines", True) else 1)

    window._settings_up_color = getattr(self, "candle_up_color", "#26a69a")
    window._settings_down_color = getattr(self, "candle_down_color", "#ef5350")
    _hotfix_update_color_button(window._settings_up_button, window._settings_up_color)
    _hotfix_update_color_button(window._settings_down_button, window._settings_down_color)

    window._settings_max_portfolio.setValue(float(getattr(risk_engine, "max_portfolio_risk", getattr(self.controller, "max_portfolio_risk", 0.2))))
    window._settings_max_trade.setValue(float(getattr(risk_engine, "max_risk_per_trade", getattr(self.controller, "max_risk_per_trade", 0.02))))
    window._settings_max_position.setValue(float(getattr(risk_engine, "max_position_size_pct", getattr(self.controller, "max_position_size_pct", 0.05))))
    window._settings_max_gross.setValue(float(getattr(risk_engine, "max_gross_exposure_pct", getattr(self.controller, "max_gross_exposure_pct", 1.0))))
    strategy_params = dict(getattr(self.controller, "strategy_params", {}) or {})
    current_strategy_name = Strategy.normalize_strategy_name(
        getattr(self.controller, "strategy_name", None) or getattr(getattr(self.controller, "config", None), "strategy", "Trend Following")
    )
    window._settings_strategy_name.setCurrentText(str(current_strategy_name))
    window._settings_strategy_rsi_period.setValue(float(strategy_params.get("rsi_period", 14)))
    window._settings_strategy_ema_fast.setValue(float(strategy_params.get("ema_fast", 20)))
    window._settings_strategy_ema_slow.setValue(float(strategy_params.get("ema_slow", 50)))
    window._settings_strategy_atr_period.setValue(float(strategy_params.get("atr_period", 14)))
    window._settings_strategy_oversold.setValue(float(strategy_params.get("oversold_threshold", 35.0)))
    window._settings_strategy_overbought.setValue(float(strategy_params.get("overbought_threshold", 65.0)))
    window._settings_strategy_breakout.setValue(float(strategy_params.get("breakout_lookback", 20)))
    window._settings_strategy_confidence.setValue(float(strategy_params.get("min_confidence", 0.55)))
    window._settings_strategy_amount.setValue(float(strategy_params.get("signal_amount", 1.0)))
    window._settings_telegram_enabled.setCurrentIndex(1 if getattr(self.controller, "telegram_enabled", False) else 0)
    window._settings_telegram_bot_token.setText(str(getattr(self.controller, "telegram_bot_token", "") or ""))
    window._settings_telegram_chat_id.setText(str(getattr(self.controller, "telegram_chat_id", "") or ""))
    window._settings_openai_api_key.setText(str(getattr(self.controller, "openai_api_key", "") or ""))
    window._settings_openai_model.setText(str(getattr(self.controller, "openai_model", "gpt-5-mini") or "gpt-5-mini"))

    window._settings_summary.setText(
        "Current defaults: "
        f"{window._settings_timeframe.currentText()} | "
        f"{window._settings_order_type.currentText()} orders | "
        f"{window._settings_strategy_name.currentText()} | "
        f"history {int(window._settings_history_limit.value())} candles | "
        f"capital {window._settings_initial_capital.value():.2f} | "
        f"Telegram {'on' if window._settings_telegram_enabled.currentData() else 'off'} | "
        f"OpenAI {'set' if window._settings_openai_api_key.text().strip() else 'not set'}"
    )

    window.show()
    window.raise_()
    window.activateWindow()
    return window


def _hotfix_apply_settings_window(self, window=None):
    try:
        values = _hotfix_collect_settings_values(self, window)
        if not values:
            return

        _hotfix_apply_settings_values(self, values, persist=True, reload_chart=True)

        active_window = window or self.detached_tool_windows.get("application_settings")
        summary = getattr(active_window, "_settings_summary", None)
        if summary is not None:
            summary.setText(
                "Saved settings. "
                f"Strategy: {values['strategy_name']} | "
                f"Timeframe: {values['timeframe']} | "
                f"Order type: {values['order_type']} | "
                f"History: {values['history_limit']} | "
                f"Bid/ask lines: {'shown' if values['show_bid_ask_lines'] else 'hidden'} | "
                f"Telegram: {'enabled' if values['telegram_enabled'] else 'disabled'} | "
                f"OpenAI model: {values.get('openai_model') or 'gpt-5-mini'}"
            )

        self.system_console.log("Application settings updated successfully.", "INFO")

    except Exception as e:
        self.logger.error(f"Settings error: {e}")


def _hotfix_open_settings(self):
    self._show_settings_window()


def _hotfix_restore_settings(self):
    geometry = self.settings.value("geometry")
    if geometry:
        self.restoreGeometry(geometry)

    state = self.settings.value("windowState")
    if state:
        self.restoreState(state)

    strategy_params = dict(getattr(self.controller, "strategy_params", {}) or {})

    values = {
        "timeframe": self.settings.value("terminal/current_timeframe", getattr(self.controller, "time_frame", getattr(self, "current_timeframe", "1h"))),
        "order_type": self.settings.value("terminal/order_type", getattr(self.controller, "order_type", getattr(self, "order_type", "limit"))),
        "history_limit": _hotfix_settings_int(self.settings.value("terminal/history_limit", getattr(self.controller, "limit", 50000)), getattr(self.controller, "limit", 50000)),
        "initial_capital": _hotfix_settings_float(self.settings.value("terminal/initial_capital", getattr(self.controller, "initial_capital", 10000)), getattr(self.controller, "initial_capital", 10000)),
        "refresh_interval_ms": _hotfix_settings_int(self.settings.value("terminal/refresh_interval_ms", 1000), 1000),
        "orderbook_interval_ms": _hotfix_settings_int(self.settings.value("terminal/orderbook_interval_ms", 1500), 1500),
        "show_bid_ask_lines": _hotfix_settings_bool(self.settings.value("terminal/show_bid_ask_lines", getattr(self, "show_bid_ask_lines", True)), getattr(self, "show_bid_ask_lines", True)),
        "candle_up_color": self.settings.value("chart/candle_up_color", getattr(self, "candle_up_color", "#26a69a")),
        "candle_down_color": self.settings.value("chart/candle_down_color", getattr(self, "candle_down_color", "#ef5350")),
        "max_portfolio_risk": _hotfix_settings_float(self.settings.value("risk/max_portfolio_risk", getattr(self.controller, "max_portfolio_risk", 0.2)), getattr(self.controller, "max_portfolio_risk", 0.2)),
        "max_risk_per_trade": _hotfix_settings_float(self.settings.value("risk/max_risk_per_trade", getattr(self.controller, "max_risk_per_trade", 0.02)), getattr(self.controller, "max_risk_per_trade", 0.02)),
        "max_position_size_pct": _hotfix_settings_float(self.settings.value("risk/max_position_size_pct", getattr(self.controller, "max_position_size_pct", 0.05)), getattr(self.controller, "max_position_size_pct", 0.05)),
        "max_gross_exposure_pct": _hotfix_settings_float(self.settings.value("risk/max_gross_exposure_pct", getattr(self.controller, "max_gross_exposure_pct", 1.0)), getattr(self.controller, "max_gross_exposure_pct", 1.0)),
        "strategy_name": self.settings.value("strategy/name", getattr(self.controller, "strategy_name", "Trend Following")),
        "strategy_rsi_period": _hotfix_settings_int(self.settings.value("strategy/rsi_period", strategy_params.get("rsi_period", 14)), 14),
        "strategy_ema_fast": _hotfix_settings_int(self.settings.value("strategy/ema_fast", strategy_params.get("ema_fast", 20)), 20),
        "strategy_ema_slow": _hotfix_settings_int(self.settings.value("strategy/ema_slow", strategy_params.get("ema_slow", 50)), 50),
        "strategy_atr_period": _hotfix_settings_int(self.settings.value("strategy/atr_period", strategy_params.get("atr_period", 14)), 14),
        "strategy_oversold_threshold": _hotfix_settings_float(self.settings.value("strategy/oversold_threshold", strategy_params.get("oversold_threshold", 35.0)), 35.0),
        "strategy_overbought_threshold": _hotfix_settings_float(self.settings.value("strategy/overbought_threshold", strategy_params.get("overbought_threshold", 65.0)), 65.0),
        "strategy_breakout_lookback": _hotfix_settings_int(self.settings.value("strategy/breakout_lookback", strategy_params.get("breakout_lookback", 20)), 20),
        "strategy_min_confidence": _hotfix_settings_float(self.settings.value("strategy/min_confidence", strategy_params.get("min_confidence", 0.55)), 0.55),
        "strategy_signal_amount": _hotfix_settings_float(self.settings.value("strategy/signal_amount", strategy_params.get("signal_amount", 1.0)), 1.0),
    }

    _hotfix_apply_settings_values(self, values, persist=False, reload_chart=False)
    self._restore_detached_chart_layouts()


def _hotfix_close_event(self, event):
    self._save_detached_chart_layouts()
    self._ui_shutting_down = True
    try:
        if hasattr(self, "refresh_timer") and self.refresh_timer is not None:
            self.refresh_timer.stop()
        if hasattr(self, "orderbook_timer") and self.orderbook_timer is not None:
            self.orderbook_timer.stop()
        if hasattr(self, "spinner_timer") and self.spinner_timer is not None:
            self.spinner_timer.stop()
        for task_name in ("_positions_refresh_task", "_open_orders_refresh_task"):
            task = getattr(self, task_name, None)
            if task is not None and not task.done():
                task.cancel()
    except Exception:
        pass

    values = {
        "timeframe": getattr(self, "current_timeframe", getattr(self.controller, "time_frame", "1h")),
        "order_type": getattr(self, "order_type", getattr(self.controller, "order_type", "limit")),
        "history_limit": getattr(self.controller, "limit", 50000),
        "initial_capital": getattr(self.controller, "initial_capital", 10000),
        "refresh_interval_ms": self.refresh_timer.interval() if hasattr(self, "refresh_timer") and self.refresh_timer is not None else 1000,
        "orderbook_interval_ms": self.orderbook_timer.interval() if hasattr(self, "orderbook_timer") and self.orderbook_timer is not None else 1500,
        "show_bid_ask_lines": getattr(self, "show_bid_ask_lines", True),
        "candle_up_color": getattr(self, "candle_up_color", "#26a69a"),
        "candle_down_color": getattr(self, "candle_down_color", "#ef5350"),
        "max_portfolio_risk": getattr(self.controller, "max_portfolio_risk", 0.2),
        "max_risk_per_trade": getattr(self.controller, "max_risk_per_trade", 0.02),
        "max_position_size_pct": getattr(self.controller, "max_position_size_pct", 0.05),
        "max_gross_exposure_pct": getattr(self.controller, "max_gross_exposure_pct", 1.0),
        "strategy_name": getattr(self.controller, "strategy_name", "Trend Following"),
        "strategy_rsi_period": getattr(getattr(self.controller, "strategy_params", {}), "get", lambda *_: 14)("rsi_period", 14),
        "strategy_ema_fast": getattr(getattr(self.controller, "strategy_params", {}), "get", lambda *_: 20)("ema_fast", 20),
        "strategy_ema_slow": getattr(getattr(self.controller, "strategy_params", {}), "get", lambda *_: 50)("ema_slow", 50),
        "strategy_atr_period": getattr(getattr(self.controller, "strategy_params", {}), "get", lambda *_: 14)("atr_period", 14),
        "strategy_oversold_threshold": getattr(getattr(self.controller, "strategy_params", {}), "get", lambda *_: 35.0)("oversold_threshold", 35.0),
        "strategy_overbought_threshold": getattr(getattr(self.controller, "strategy_params", {}), "get", lambda *_: 65.0)("overbought_threshold", 65.0),
        "strategy_breakout_lookback": getattr(getattr(self.controller, "strategy_params", {}), "get", lambda *_: 20)("breakout_lookback", 20),
        "strategy_min_confidence": getattr(getattr(self.controller, "strategy_params", {}), "get", lambda *_: 0.55)("min_confidence", 0.55),
        "strategy_signal_amount": getattr(getattr(self.controller, "strategy_params", {}), "get", lambda *_: 1.0)("signal_amount", 1.0),
    }
    _hotfix_apply_settings_values(self, values, persist=True, reload_chart=False)
    super(Terminal, self).closeEvent(event)


async def _hotfix_refresh_markets_async(self):
    broker = getattr(self.controller, "broker", None)
    if broker is None:
        raise RuntimeError("Broker is not connected")

    if hasattr(broker, "connect"):
        try:
            maybe = broker.connect()
            if asyncio.iscoroutine(maybe):
                await maybe
        except Exception:
            pass

    symbols = None
    if hasattr(self.controller, "_fetch_symbols"):
        symbols = await self.controller._fetch_symbols(broker)
    else:
        if hasattr(broker, "fetch_symbol"):
            symbols = await broker.fetch_symbol()
        elif hasattr(broker, "fetch_symbols"):
            symbols = await broker.fetch_symbols()

    if not symbols:
        raise RuntimeError("No symbols were returned by the broker")

    broker_cfg = getattr(getattr(self.controller, "config", None), "broker", None)
    broker_type = getattr(broker_cfg, "type", None)
    exchange = getattr(broker_cfg, "exchange", None) or getattr(broker, "exchange_name", "Broker")

    if hasattr(self.controller, "_filter_symbols_for_trading"):
        symbols = self.controller._filter_symbols_for_trading(symbols, broker_type, exchange=exchange)

    if hasattr(self.controller, "_select_trade_symbols"):
        selected = await self.controller._select_trade_symbols(symbols, broker_type)
        if selected:
            symbols = selected

    self.controller.symbols = list(symbols)
    self.controller.symbols_signal.emit(str(exchange), list(self.controller.symbols))

    active_symbol = self._current_chart_symbol()
    if active_symbol and hasattr(self.controller, "request_candle_data"):
        await self.controller.request_candle_data(
            symbol=active_symbol,
            timeframe=getattr(self, "current_timeframe", "1h"),
            limit=self._history_request_limit(),
        )
        await self._reload_chart_data(active_symbol, getattr(self, "current_timeframe", "1h"))

    if active_symbol and hasattr(self.controller, "request_orderbook"):
        await self.controller.request_orderbook(symbol=active_symbol, limit=20)

    self._refresh_terminal()
    self.system_console.log(f"Markets refreshed: {len(self.controller.symbols)} symbols loaded.", "INFO")


def _hotfix_refresh_markets(self):
    async def runner():
        try:
            await _hotfix_refresh_markets_async(self)
        except Exception as e:
            self.system_console.log(f"Market refresh failed: {e}", "ERROR")

    asyncio.get_event_loop().create_task(runner())


async def _hotfix_reload_balance_async(self):
    if not hasattr(self.controller, "update_balance"):
        raise RuntimeError("Balance reload is not supported by this controller")

    await self.controller.update_balance()
    self._refresh_terminal()

    balance = getattr(self.controller, "balances", {})
    summary, _tooltip = self._compact_balance_text(balance)
    self.system_console.log(f"Balances reloaded: {summary}", "INFO")


def _hotfix_reload_balance(self):
    async def runner():
        try:
            await _hotfix_reload_balance_async(self)
        except Exception as e:
            self.system_console.log(f"Balance reload failed: {e}", "ERROR")

    asyncio.get_event_loop().create_task(runner())


def _hotfix_refresh_active_orderbook(self):
    symbol = self._current_chart_symbol()
    if not symbol:
        self.system_console.log("Open a chart before refreshing orderbook.", "ERROR")
        return

    async def runner():
        try:
            if not hasattr(self.controller, "request_orderbook"):
                raise RuntimeError("Orderbook refresh is not supported by this controller")
            await self.controller.request_orderbook(symbol=symbol, limit=20)
            self.system_console.log(f"Orderbook refreshed for {symbol}.", "INFO")
        except Exception as e:
            self.system_console.log(f"Orderbook refresh failed: {e}", "ERROR")

    asyncio.get_event_loop().create_task(runner())


def _hotfix_refresh_active_chart_data(self):
    chart = self._current_chart_widget()
    if not isinstance(chart, ChartWidget):
        self.system_console.log("Open a chart before refreshing candles.", "ERROR")
        return

    symbol = chart.symbol
    timeframe = getattr(chart, "timeframe", getattr(self, "current_timeframe", "1h"))

    async def runner():
        try:
            if not hasattr(self.controller, "request_candle_data"):
                raise RuntimeError("Chart refresh is not supported by this controller")

            await self.controller.request_candle_data(
                symbol=symbol,
                timeframe=timeframe,
                limit=self._history_request_limit(),
            )
            await self._reload_chart_data(symbol, timeframe)
            self.system_console.log(f"Chart data refreshed for {symbol} ({timeframe}).", "INFO")
        except Exception as e:
            self.system_console.log(f"Chart refresh failed: {e}", "ERROR")

    asyncio.get_event_loop().create_task(runner())


# Bind overrides
Terminal.run_backtest_clicked = _hotfix_run_backtest_clicked
Terminal.start_backtest = _hotfix_start_backtest
Terminal.stop_backtest = _hotfix_stop_backtest
Terminal._generate_report = _hotfix_generate_report
Terminal._show_optimization_window = _hotfix_show_optimization_window
Terminal._refresh_optimization_window = _hotfix_refresh_optimization_window
Terminal._run_strategy_optimization = _hotfix_run_strategy_optimization
Terminal._apply_best_optimization_params = _hotfix_apply_best_optimization_params
Terminal._optimize_strategy = _hotfix_optimize_strategy
Terminal._reload_chart_data = _hotfix_reload_chart_data
Terminal._refresh_markets = _hotfix_refresh_markets
Terminal._reload_balance = _hotfix_reload_balance
Terminal._refresh_active_chart_data = _hotfix_refresh_active_chart_data
Terminal._refresh_active_orderbook = _hotfix_refresh_active_orderbook
Terminal._show_settings_window = _hotfix_show_settings_window
Terminal._apply_settings_window = _hotfix_apply_settings_window
Terminal._show_risk_settings_window = _hotfix_show_settings_window
Terminal._apply_risk_settings = _hotfix_apply_settings_window
Terminal._open_settings = _hotfix_open_settings
Terminal._open_risk_settings = _hotfix_open_risk_settings
Terminal._restore_settings = _hotfix_restore_settings
Terminal.closeEvent = _hotfix_close_event
Terminal.save_settings = _hotfix_save_settings

















