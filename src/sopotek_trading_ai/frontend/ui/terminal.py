import asyncio
import random
import sys
import traceback
import webbrowser

import numpy as np
import pyqtgraph as pg
from PySide6.QtCore import Qt, QSettings, QDateTime, Signal, QTimer
from PySide6.QtGui import QAction, QColor
from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QDockWidget,
    QTableWidget, QTableWidgetItem,
    QPushButton, QLabel,
    QTabWidget, QToolBar, QFileDialog, QDialog, QGridLayout, QDoubleSpinBox, QMessageBox, QFormLayout, QInputDialog
)

from sopotek_trading.backend.strategy.backtest_engine import BacktestEngine
from sopotek_trading.backend.utils.utils import candles_to_df
from sopotek_trading.frontend.ui.chart.ChartWidget import ChartWidget
from sopotek_trading.frontend.ui.report_generator import ReportGenerator
from sopotek_trading.frontend.ui.system_console import SystemConsole
def global_exception_hook(exctype, value, tb):
    print("🔥 UNCAUGHT EXCEPTION:")
    traceback.print_exception(exctype, value, tb)




class Terminal(QMainWindow):
    logout_requested = Signal()
    ai_signal = Signal(str,list)



    def __init__(self, controller):

        super().__init__(controller)
        self.ai_signal = None
        sys.excepthook = global_exception_hook

        self.autotrade_toggle = None
        self.symbols_table=QTableWidget()


        self.risk_map = None
        self.auto_button = QPushButton()

        self.controller = controller
        self.confidence_data = [self.controller.confidence]
        self.logger = controller.logger
        self.settings = QSettings("Sopotek", "TradingPlatform")

        self.historical_data = controller.historical_data


        if controller.symbols:
            index = random.randint(0, len(controller.symbols) - 1)
            self.symbol = controller.symbols[index]
        else:
            self.symbol = "BTC/USDT"


        self.MAX_LOG_ROWS = 500
        self.current_timeframe = "1m"
        self.autotrading_enabled = False

        self.training_status = {}
        self.heartbeat= QLabel()
        self.heartbeat.setText("●")
        self.heartbeat.setStyleSheet("color: green")

        self._setup_core()
        self._setup_ui()
        self._setup_panels()
        self._connect_signals()
        self._setup_spinner()
        self.controller.symbols_signal.connect(self._update_symbols)
        self.refresh_timer = QTimer()
        self.refresh_timer.timeout.connect(self._refresh_terminal)
        self.refresh_timer.start(1000)  # every second
        self.ai_signal.connect(self._update_ai_signal)
        self.autotrade_toggle.emit(self.autotrading_enabled)


    def _setup_core(self):

        self.order_type = self.controller.order_type
        self.setWindowTitle("Sopotek AI Trading Terminal")
        self.resize(1700, 950)

        self.connection_indicator = QLabel("● CONNECTING")
        self.connection_indicator.setStyleSheet(
            "color: orange; font-weight: bold;"
        )

        self.timeframe_buttons = {}

    def _setup_ui(self):

        self.chart_tabs = QTabWidget()
        self.chart_tabs.setTabsClosable(True)

        self.chart_tabs.tabCloseRequested.connect(
            lambda i: self.chart_tabs.removeTab(i)
        )

        self.setCentralWidget(self.chart_tabs)

        self._create_menu_bar()
        self._create_toolbar()

        self._create_chart_tab(
            self.symbol,
            self.controller.time_frame
        )

        self._restore_settings()

    # ==========================================================
    # MENU
    # ==========================================================

    def _create_menu_bar(self):

        menu_bar = self.menuBar()

        # ======================================
        # FILE MENU
        # ======================================

        file_menu = menu_bar.addMenu("File")

        generate_report = QAction("Generate Trading Report", self)
        generate_report.triggered.connect(self._generate_report)
        file_menu.addAction(generate_report)

        export_trades = QAction("Export Trades CSV", self)
        export_trades.triggered.connect(self._export_trades)
        file_menu.addAction(export_trades)

        file_menu.addSeparator()

        exit_action = QAction("Exit", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # ======================================
        # TRADING MENU
        # ======================================

        trading_menu = menu_bar.addMenu("Trading")

        start_trading = QAction("Start Auto Trading", self)
        start_trading.triggered.connect(self._toggle_autotrading)

        trading_menu.addAction(start_trading)

        stop_trading = QAction("Stop Auto Trading", self)

        trading_menu.addAction(stop_trading)

        manual_trade = QAction("Manual Trade", self)
        manual_trade.triggered.connect(self._open_manual_trade)
        trading_menu.addAction(manual_trade)

        trading_menu.addSeparator()

        close_all = QAction("Close All Positions", self)

        trading_menu.addAction(close_all)

        cancel_orders = QAction("Cancel All Orders", self)

        trading_menu.addAction(cancel_orders)

        # ======================================
        # BACKTEST MENU
        # ======================================

        backtest_menu = menu_bar.addMenu("Backtesting")

        run_backtest = QAction("Run Backtest", self)
        run_backtest.triggered.connect(
            lambda: asyncio.get_event_loop().create_task(self.run_backtest_clicked())
        )
        backtest_menu.addAction(run_backtest)
        start_trading.setShortcut("Ctrl+T")
        run_backtest.setShortcut("Ctrl+B")

        optimize_strategy = QAction("Strategy Optimization", self)
        optimize_strategy.triggered.connect(self._optimize_strategy)
        backtest_menu.addAction(optimize_strategy)

        # ======================================
        # CHART MENU
        # ======================================

        charts_menu = menu_bar.addMenu("Charts")

        new_chart = QAction("New Chart", self)
        new_chart.setShortcut("Ctrl+N")
        new_chart.triggered.connect(self._add_new_chart)
        charts_menu.addAction(new_chart)

        multi_chart = QAction("Multi Chart Layout", self)
        multi_chart.triggered.connect(self._multi_chart_layout)
        charts_menu.addAction(multi_chart)

        # ======================================
        # DATA MENU
        # ======================================

        data_menu = menu_bar.addMenu("Data")



        refresh_markets = QAction("Refresh Markets", self)

        refresh_markets.triggered.connect(self._refresh_markets)


        data_menu.addAction(refresh_markets)

        reload_balance = QAction("Reload Balance", self)
        reload_balance.triggered.connect(
            lambda: asyncio.get_event_loop().create_task(self.controller.update_balance())
        )
        data_menu.addAction(reload_balance)

        # ======================================
        # RISK MENU
        # ======================================

        risk_menu = menu_bar.addMenu("Risk")

        risk_settings = QAction("Risk Settings", self)
        risk_settings.triggered.connect(self._open_risk_settings)
        risk_menu.addAction(risk_settings)

        portfolio_view = QAction("Portfolio Exposure", self)
        portfolio_view.triggered.connect(self._show_portfolio_exposure)
        risk_menu.addAction(portfolio_view)

        # ======================================
        # TOOLS MENU
        # ======================================

        tools_menu = menu_bar.addMenu("Tools")

        ai_signal = QAction("ML Signal Monitor", self)
        ai_signal.triggered.connect(self._open_ml_monitor)
        tools_menu.addAction(ai_signal)

        logs = QAction("System Logs", self)
        logs.triggered.connect(self._open_logs)
        tools_menu.addAction(logs)

        performance = QAction("Performance Analytics", self)
        performance.triggered.connect(self._open_performance)
        tools_menu.addAction(performance)

        # ======================================
        # HELP MENU
        # ======================================

        help_menu = menu_bar.addMenu("Help")

        documentation = QAction("Documentation", self)
        documentation.triggered.connect(self._open_docs)
        help_menu.addAction(documentation)

        api_docs = QAction("API Reference", self)
        api_docs.triggered.connect(self._open_api_docs)
        help_menu.addAction(api_docs)

        help_menu.addSeparator()

        about_action = QAction("About Sopotek Trading", self)
        about_action.triggered.connect(self._show_about)
        help_menu.addAction(about_action)

    def update_connection_status(self, status: str):

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

    # ==========================================================
    # TOOLBAR
    # ==========================================================

    def _create_toolbar(self):
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

    def _toggle_autotrading(self):

        self.autotrading_enabled = not self.autotrading_enabled

        if self.autotrading_enabled:

            self.auto_button.setText("AutoTrading ON")
            self.auto_button.setStyleSheet(
                "background-color: green; color: white;"
            )
            loop = asyncio.get_event_loop()

            loop.create_task(self.controller.trading_app.start())



        else:

            self.auto_button.setText("AutoTrading OFF")
            self.auto_button.setStyleSheet("")
        self.autotrade_toggle.emit(self.autotrading_enabled)

    # ==========================================================
    # CHARTS
    # ==========================================================

    def _create_chart_tab(self, symbol, timeframe):
        chart = ChartWidget(symbol, timeframe, self.controller)

        # Add symbol to market watch table

        row = self.symbols_table.rowCount()
        self.symbols_table.insertRow(row)

        self.symbols_table.setItem(row, 0, QTableWidgetItem(symbol))
        self.symbols_table.setItem(row, 1, QTableWidgetItem("-"))
        self.symbols_table.setItem(row, 2, QTableWidgetItem("-"))
        self.symbols_table.setItem(row, 3, QTableWidgetItem("⏳ Training..."))
        self.chart_tabs.addTab(chart, f"{symbol} ({timeframe})")

        chart.link_all_charts(self.chart_tabs.count())

    def _add_new_chart(self):
        symbol, ok = QInputDialog.getText(
            self, "New Chart", "Enter Symbol:"
        )
        self.training_status[symbol] = "TRAINING"
        if ok and symbol:
            self._create_chart_tab(symbol.upper(), "1h")

    def _set_timeframe(self, tf="1h"):

        self.current_timeframe = tf

        index = self.chart_tabs.currentIndex()
        chart = self.chart_tabs.widget(index)

        if not isinstance(chart, ChartWidget):
            return

        # Update timeframe
        chart.timeframe = tf

        # Update tab title
        self.chart_tabs.setTabText(
            index,
            f"{chart.symbol} ({tf})"
        )

        # Request new historical data
        asyncio.get_event_loop().create_task(
            self._reload_chart_data(chart.symbol, tf)
        )

    # ==========================================================
    # UPDATE METHODS
    # ==========================================================
    def _update_chart(self, symbol, df):

        df = candles_to_df(df)

        for i in range(self.chart_tabs.count()):
            chart = self.chart_tabs.widget(i)

            if isinstance(chart, ChartWidget) and chart.symbol == symbol:
                chart.update_candles(df)

        self.heartbeat.setStyleSheet("color: green;")

    def _update_equity(self, equity):
        self.equity_label.setText(f"Equity: {equity:.2f}")
        self.equity_curve.setData(self.controller.performance_engine.equity_history)

    def _update_trade_log(self, trade):

        row = self.trade_log.rowCount()

        if row >= self.MAX_LOG_ROWS:
            self.trade_log.removeRow(0)
            row -= 1

        self.trade_log.insertRow(row)
        self.trade_log.setItem(row, 0, QTableWidgetItem(str(trade.get("symbol", ""))))
        self.trade_log.setItem(row, 1, QTableWidgetItem(str(trade.get("side", ""))))
        self.trade_log.setItem(row, 2, QTableWidgetItem(str(trade.get("price", ""))))
        self.trade_log.setItem(row, 3, QTableWidgetItem(str(trade.get("size", ""))))
        self.trade_log.setItem(row, 4, QTableWidgetItem(str(trade.get("sl", ""))))
        self.trade_log.setItem(row, 5, QTableWidgetItem(str(trade.get("tp", ""))))
        self.trade_log.setItem(row, 6, QTableWidgetItem(str(trade.get("rt", ""))))
        self.symbols_table.horizontalHeader().setStretchLastSection(True)
        self.trade_log.horizontalHeader().setStretchLastSection(True)

    def _update_ticker(self, symbol, bid, ask):

        for row in range(self.symbols_table.rowCount()):
            item = self.symbols_table.item(row, 0)

            if item and item.text() == symbol:
                self.symbols_table.setItem(row, 1, QTableWidgetItem(str(bid)))
                self.symbols_table.setItem(row, 2, QTableWidgetItem(str(ask)))
                break

        mid = (bid + ask) / 2
        self.tick_prices.append(mid)

        if len(self.tick_prices) > 200:
            self.tick_prices.pop(0)

        self.tick_chart_curve.setData(self.tick_prices)

    # ==========================================================
    # PANELS
    # ==========================================================

    def _create_market_watch_panel(self):
        dock = QDockWidget("Market Watch", self)
        self.symbols_table = QTableWidget()
        self.symbols_table.setColumnCount(4)
        self.symbols_table.setHorizontalHeaderLabels(
            ["Symbol", "Bid", "Ask", "AI Training"]
        )
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
        dock.setWidget(self.positions_table)
        self.addDockWidget(Qt.BottomDockWidgetArea, dock)

    def _create_trade_log_panel(self):
        dock = QDockWidget("Trade Log", self)
        self.trade_log = QTableWidget()
        self.trade_log.setColumnCount(9)
        self.trade_log.setHorizontalHeaderLabels(
            ["Symbol", "Price", "Size", "OrderType", "Side", "SL", "TP", "TimeStamp", "Pnl"])
        dock.setWidget(self.trade_log)
        self.addDockWidget(Qt.RightDockWidgetArea, dock)

    def _create_equity_panel(self):

        dock = QDockWidget("Equity Curve", self)

        container = QWidget()
        layout = QVBoxLayout()

        self.equity_label = QLabel("Equity: 0")
        layout.addWidget(self.equity_label)

        self.equity_chart = pg.PlotWidget()
        self.equity_curve = self.equity_chart.plot(pen="g")

        layout.addWidget(self.equity_chart)

        container.setLayout(layout)

        dock.setWidget(container)

        self.addDockWidget(Qt.RightDockWidgetArea, dock)

    def _create_performance_panel(self):
        dock = QDockWidget("Performance", self)
        container = QWidget()
        layout = QVBoxLayout()

        self.equity_label = QLabel("Equity: 0")
        layout.addWidget(self.equity_label)

        container.setLayout(layout)
        dock.setWidget(container)
        self.addDockWidget(Qt.LeftDockWidgetArea, dock)

    def _create_strategy_comparison(self):
        dock = QDockWidget("Strategy Comparison", self)
        self.strategy_table = QTableWidget()
        dock.setWidget(self.strategy_table)
        self.addDockWidget(Qt.BottomDockWidgetArea, dock)

    # ==========================================================
    # BACKTEST
    # ==========================================================

    async def run_backtest_clicked(self):

        try:

            engine = BacktestEngine(
                strategy=self.controller.orchestrator,
                data=self.historical_data,
                initial_capital=self.controller.initial_capital,
                slippage=self.controller.slippage,
                commission=self.controller.commission

            )

            results = engine.run()

            self.system_console.log(
                f"Backtest completed: {results}",
                "INFO"
            )

        except Exception as e:

            self.system_console.log(
                f"Backtest failed: {e}",
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

    # ==========================================================
    # SETTINGS
    # ==========================================================

    def closeEvent(self, event):
        self.settings.setValue("geometry", self.saveGeometry())
        self.settings.setValue("windowState", self.saveState())
        super().closeEvent(event)

    def _restore_settings(self):
        geometry = self.settings.value("geometry")
        if geometry:
            self.restoreGeometry(geometry)

        state = self.settings.value("windowState")
        if state:
            self.restoreState(state)

    def _update_orderbook(self, symbol, bids, asks):

        for i in range(self.chart_tabs.count()):
            chart = self.chart_tabs.widget(i)

            if isinstance(chart, ChartWidget) and chart.symbol == symbol:
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
        for i in range(self.chart_tabs.count()):
            chart = self.chart_tabs.widget(i)
            if chart.symbol == debug["symbol"]:
                chart.add_strategy_signal(
                    debug["index"],
                    debug["ema_fast"],
                    debug["ema_slow"],
                    debug["ml_probability"],
                    debug["reason"],
                    debug["signal"]
                )

    def _update_training_status(self, symbol, status):

        for row in range(self.symbols_table.rowCount()):
            if self.symbols_table.item(row, 0).text() == symbol:

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

                self.symbols_table.setItem(row, 3, item)
                break

    # def _rotate_spinner(self):
    #      try:
    #         self._spinner_index += 1
    #         self.symbols_table.setRowCount(0)
    #         self.symbols_table.setRowCount(self._spinner_index)
    #
    #         for row in range(self.symbols_table.rowCount()):
    #
    #             item = self.symbols_table.item(row, 3)
    #             if not item:
    #                 continue
    #
    #             if "Training" in item.text():
    #                 icon = self._spinner_frames[self._spinner_index % len(self._spinner_frames)]
    #                 item.setText(f"{icon} Training...")
    #                 item.setForeground(QColor("yellow"))
    #      except Exception as e:
    #         self._spinner_index += 1
    #         self.logger.error(e)

    def _connect_signals(self):

        self.controller.candle_signal.connect(self._update_chart)
        self.controller.equity_signal.connect(self._update_equity)
        self.controller.trade_signal.connect(self._update_trade_log)
        self.controller.ticker_signal.connect(self._update_ticker)

        self.controller.orderbook_signal.connect(
            self._update_orderbook
        )

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
        self._create_positions_panel()
        self._create_trade_log_panel()
        self._create_equity_panel()
        self._create_performance_panel()
        self._create_strategy_comparison()
        self._create_strategy_debug_panel()
        self._create_system_status_panel()
        self._create_ai_signal_panel()

    def _setup_spinner(self):

        self._spinner_frames = ["⏳", "⌛"]
        self._spinner_index = 0

        self.spinner_timer = QTimer()
        #self.spinner_timer.timeout.connect(self._rotate_spinner)

        self.spinner_timer.start(500)

    def _update_symbols(self, exchange, symbols):

        self.symbols_table.setRowCount(0)
        self.symbols_table.setAccessibleName(exchange)

        for symbol in symbols:
            row = self.symbols_table.rowCount()
            self.symbols_table.insertRow(row)

            self.symbols_table.setItem(row, 0, QTableWidgetItem(symbol))
            self.symbols_table.setItem(row, 1, QTableWidgetItem("-"))
            self.symbols_table.setItem(row, 2, QTableWidgetItem("-"))
            self.symbols_table.setItem(row, 3, QTableWidgetItem("⏳"))

    def _open_manual_trade(self):
        pass

    def _optimize_strategy(self):
        pass

    def _open_logs(self):
        pass

    def _open_ml_monitor(self):
        pass

    def _show_about(self):

        dialog = QDialog(self)
        dialog.setWindowTitle("About Sopotek Trading")
        dialog.resize(400, 250)

        layout = QVBoxLayout()

        title = QLabel("<h2>Sopotek Trading Platform</h2>")
        layout.addWidget(title)

        version = QLabel("Version: 1.0")
        layout.addWidget(version)

        description = QLabel(
            "Advanced algorithmic trading platform\n"
            "with AI signals and institutional risk management."
        )
        layout.addWidget(description)

        author = QLabel("Developed by Sopotek")
        layout.addWidget(author)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(dialog.close)

        layout.addWidget(close_btn)

        dialog.setLayout(layout)
        dialog.exec()

    def _close_all_positions(self):
        pass

    def _export_trades(self):
        pass

    def _cancel_all_orders(self):
        pass

    def _open_docs(self):
        pass

    def _open_api_docs(self):

        url = "https://docs.ccxt.com"

        webbrowser.open(url)

    def _multi_chart_layout(self):

        try:

            # Create container widget
            container = QWidget(self)

            # Grid layout
            layout = QGridLayout(container)

            # Example symbols (or take from controller)
            symbols = self.controller.symbols[:4]

            # Create 2x2 grid of charts
            positions = [(0, 0), (0, 1), (1, 0), (1, 1)]

            for symbol, position in zip(symbols, positions):
                chart = ChartWidget(symbol, self.current_timeframe,self.controller)

                layout.addWidget(chart, *position)

            container.setLayout(layout)

            # Set as central widget
            self.setCentralWidget(container)

        except Exception as e:

            self.logger.error(f"Multi chart layout error: {e}")

    def _open_performance(self):
        pass

    def _open_risk_settings(self):

        dialog = QDialog(self)
        dialog.setWindowTitle("Risk Settings")
        dialog.resize(400, 300)

        layout = QVBoxLayout()
        form = QFormLayout()

        risk_engine = self.controller.risk_engine

        # Max Portfolio Risk
        max_portfolio_risk = QDoubleSpinBox()
        max_portfolio_risk.setRange(0, 1)
        max_portfolio_risk.setSingleStep(0.01)
        max_portfolio_risk.setValue(risk_engine.max_portfolio_risk)

        # Max Risk Per Trade
        max_risk_per_trade = QDoubleSpinBox()
        max_risk_per_trade.setRange(0, 1)
        max_risk_per_trade.setSingleStep(0.01)
        max_risk_per_trade.setValue(risk_engine.max_risk_per_trade)

        # Max Position Size
        max_position_size = QDoubleSpinBox()
        max_position_size.setRange(0, 1)
        max_position_size.setSingleStep(0.01)
        max_position_size.setValue(risk_engine.max_position_size_pct)

        # Max Gross Exposure
        max_gross_exposure = QDoubleSpinBox()
        max_gross_exposure.setRange(0, 5)
        max_gross_exposure.setSingleStep(0.1)
        max_gross_exposure.setValue(risk_engine.max_gross_exposure_pct)

        form.addRow("Max Portfolio Risk:", max_portfolio_risk)
        form.addRow("Max Risk Per Trade:", max_risk_per_trade)
        form.addRow("Max Position Size:", max_position_size)
        form.addRow("Max Gross Exposure:", max_gross_exposure)

        layout.addLayout(form)

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

            positions = self.controller.portfolio.get_positions()

            dialog = QDialog(self)
            dialog.setWindowTitle("Portfolio Exposure")
            dialog.resize(600, 400)

            layout = QVBoxLayout()

            table = QTableWidget()
            table.setColumnCount(4)
            table.setHorizontalHeaderLabels(
                ["Symbol", "Size", "Value (USD)", "Portfolio %"]
            )

            total_value = sum(p["value"] for p in positions)

            table.setRowCount(len(positions))

            for row, pos in enumerate(positions):
                symbol = pos["symbol"]
                size = pos["size"]
                value = pos["value"]

                pct = (value / total_value * 100) if total_value else 0

                table.setItem(row, 0, QTableWidgetItem(str(symbol)))
                table.setItem(row, 1, QTableWidgetItem(str(size)))
                table.setItem(row, 2, QTableWidgetItem(f"{value:.2f}"))
                table.setItem(row, 3, QTableWidgetItem(f"{pct:.2f}%"))

            layout.addWidget(table)

            dialog.setLayout(layout)

            dialog.exec()

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

    def _refresh_terminal(self):

        try:

            controller = self.controller

            equity = getattr(controller.portfolio, "get_equity", lambda: 0)()
            balance = getattr(controller, "balance", {})
            spread = getattr(controller, "spread_pct", 0)
            positions = getattr(controller.portfolio, "positions", {})
            symbols = getattr(controller, "symbols", [])
            exchange = getattr(controller.broker, "exchange_name", "Unknown")

            free = balance.get("free", 0) if isinstance(balance, dict) else 0
            used = balance.get("used", 0) if isinstance(balance, dict) else 0

            self.status_labels["Exchange"].setText(f"Exchange: {exchange}")

            self.status_labels["Symbols Loaded"].setText(
                f"Symbols Loaded: {len(symbols)}"
            )

            self.status_labels["Equity"].setText(
                f"Equity: {equity:.4f}"
            )

            self.status_labels["Balance"].setText(
                f"Balance: {balance}"
            )

            self.status_labels["Free Margin"].setText(
                f"Free Margin: {free}"
            )

            self.status_labels["Used Margin"].setText(
                f"Used Margin: {used}"
            )

            self.status_labels["Spread %"].setText(
                f"Spread %: {spread:.4f}"
            )

            self.status_labels["Open Positions"].setText(
                f"Open Positions: {len(positions)}"
            )

            ws = getattr(controller, "ws_manager", None)

            self.status_labels["Websocket"].setText(
                f"Websocket: {'Running' if ws and ws.running else 'Stopped'}")

            self.status_labels["AutoTrading"].setText(
                f"AutoTrading: {'ON' if self.autotrading_enabled else 'OFF'}"
            )

            self.status_labels["Timeframe"].setText(
                f"Timeframe: {self.current_timeframe}"
            )

        except Exception as e:

            self.logger.error(e)


    def _refresh_markets(self):

        self.symbols_table.setRowCount(0)

        for symbol in self.controller.symbols:

            row = self.symbols_table.rowCount()
            self.symbols_table.insertRow(row)

            self.symbols_table.setItem(row, 0, QTableWidgetItem(symbol))
            self.symbols_table.setItem(row, 1, QTableWidgetItem("-"))
            self.symbols_table.setItem(row, 2, QTableWidgetItem("-"))
            self.symbols_table.setItem(row, 3, QTableWidgetItem("⏳"))

    def _create_system_status_panel(self):

        dock = QDockWidget("System Status", self)

        container = QWidget()
        layout = QVBoxLayout()

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
            "Websocket",
            "AutoTrading",
            "Timeframe"
        ]

        for field in fields:
            label = QLabel(f"{field}: -")
            label.setStyleSheet("font-weight: bold;")

            layout.addWidget(label)

            self.status_labels[field] = label

        container.setLayout(layout)

        dock.setWidget(container)

        self.addDockWidget(Qt.LeftDockWidgetArea, dock)

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

        row = self.ai_table.rowCount()
        self.ai_table.insertRow(row)

        self.ai_table.setItem(row, 0, QTableWidgetItem(data["symbol"]))
        self.ai_table.setItem(row, 1, QTableWidgetItem(data["signal"]))
        self.ai_table.setItem(row, 2, QTableWidgetItem(f'{data["confidence"]:.2f}'))
        self.ai_table.setItem(row, 3, QTableWidgetItem(data["regime"]))
        self.ai_table.setItem(row, 4, QTableWidgetItem(str(data["volatility"])))
        self.ai_table.setItem(row, 5, QTableWidgetItem(str(data["timestamp"])))

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

    def _create_risk_heatmap(self):

        dock = QDockWidget("Risk Heatmap", self)

        self.risk_map = pg.ImageItem()

        plot = pg.PlotWidget()
        plot.addItem(self.risk_map)

        dock.setWidget(plot)

        self.addDockWidget(Qt.RightDockWidgetArea, dock)

    def _update_risk_heatmap(self):

        positions = self.controller.portfolio.positions

        if not positions:
            return

        risks = []

        for pos in positions.values():

            risks.append(pos["risk"])

        data = np.array(risks).reshape(1, len(risks))

        self.risk_map.setImage(data)