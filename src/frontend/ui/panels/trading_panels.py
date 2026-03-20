from PySide6.QtCore import Qt
from PySide6.QtWidgets import QDockWidget, QHBoxLayout, QPushButton, QTableWidget, QTabWidget, QVBoxLayout, QWidget


POSITION_HEADERS = ["Symbol", "Side", "Amount", "Entry", "Mark", "Value", "PnL", "Action"]
OPEN_ORDER_HEADERS = [
    "Symbol",
    "Side",
    "Type",
    "Price",
    "Mark",
    "Amount",
    "Filled",
    "Remaining",
    "Status",
    "PnL",
    "Order ID",
]
TRADE_LOG_HEADERS = [
    "Timestamp",
    "Symbol",
    "Source",
    "Side",
    "Price",
    "Size",
    "Order Type",
    "Status",
    "Order ID",
    "PnL",
]


def _build_positions_tab(terminal):
    container = QWidget()
    layout = QVBoxLayout(container)
    layout.setContentsMargins(8, 8, 8, 8)
    layout.setSpacing(8)

    actions = QHBoxLayout()
    actions.setContentsMargins(0, 0, 0, 0)
    actions.addStretch()
    close_all_btn = QPushButton("Close All Positions")
    close_all_btn.setStyleSheet(terminal._action_button_style())
    close_all_btn.clicked.connect(terminal._close_all_positions)
    actions.addWidget(close_all_btn)
    layout.addLayout(actions)

    terminal.positions_table = QTableWidget()
    terminal.positions_table.setColumnCount(len(POSITION_HEADERS))
    terminal.positions_table.setHorizontalHeaderLabels(POSITION_HEADERS)
    layout.addWidget(terminal.positions_table)
    terminal.positions_close_all_button = close_all_btn
    return container


def _build_open_orders_tab(terminal):
    terminal.open_orders_table = QTableWidget()
    terminal.open_orders_table.setColumnCount(len(OPEN_ORDER_HEADERS))
    terminal.open_orders_table.setHorizontalHeaderLabels(OPEN_ORDER_HEADERS)
    return terminal.open_orders_table


def create_positions_panel(terminal):
    dock = QDockWidget("Positions & Orders", terminal)
    dock.setObjectName("positions_dock")
    terminal.positions_dock = dock
    terminal.open_orders_dock = dock

    tabs = QTabWidget()
    tabs.setObjectName("positions_orders_tabs")
    tabs.setDocumentMode(True)
    tabs.setUsesScrollButtons(True)
    tabs.addTab(_build_positions_tab(terminal), "Positions")
    tabs.addTab(_build_open_orders_tab(terminal), "Open Orders")

    terminal.positions_orders_tabs = tabs
    dock.setWidget(tabs)
    terminal.addDockWidget(Qt.BottomDockWidgetArea, dock)
    return dock


def create_open_orders_panel(terminal):
    dock = getattr(terminal, "positions_dock", None)
    if dock is None:
        dock = create_positions_panel(terminal)
    terminal.open_orders_dock = dock
    return dock


def create_trade_log_panel(terminal):
    dock = QDockWidget("Trade Log", terminal)
    dock.setObjectName("trade_log_dock")
    terminal.trade_log_dock = dock
    terminal.trade_log = QTableWidget()
    terminal.trade_log.setColumnCount(len(TRADE_LOG_HEADERS))
    terminal.trade_log.setHorizontalHeaderLabels(TRADE_LOG_HEADERS)
    dock.setWidget(terminal.trade_log)
    terminal.addDockWidget(Qt.RightDockWidgetArea, dock)
    return dock
