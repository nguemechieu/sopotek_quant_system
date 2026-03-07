import asyncio

from PIL.QoiImagePlugin import QoiImageFile
from PySide6.QtCore import Signal, Qt
from PySide6.QtGui import QFont, QMovie, QPixmap
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLineEdit,
    QPushButton, QComboBox, QMessageBox,
    QFormLayout, QCheckBox, QLabel,
    QFrame, QSpinBox, QListWidget,
    QHBoxLayout
)

from config.credential_manager import CredentialManager


# ------------------------------------------------
# STYLES
# ------------------------------------------------

def _get_styles():
    return """
    QWidget {
        background-color: #0f1115;
        color: white;
        font-size: 14px;
    }

    QFrame#loginCard {
        background-color: #1c1f26;
        border-radius: 12px;
        padding: 25px;
    }

    QLineEdit, QComboBox, QSpinBox {
        background-color: #2a2e38;
        border: 1px solid #3a3f4b;
        border-radius: 6px;
        padding: 6px;
    }

    QPushButton {
        background-color: #0078d7;
        border-radius: 6px;
        font-weight: bold;
        padding: 8px;
    }

    QPushButton:hover {
        background-color: #0095ff;
    }
    """


# ------------------------------------------------
# EXCHANGE MAP
# ------------------------------------------------

EXCHANGE_MAP = {

    "crypto": [
        "binance", "binanceus", "coinbase",
        "kraken", "kucoin", "bybit",
        "okx", "gateio", "bitget"
    ],

    "forex": [
        "oanda"
    ],

    "stocks": [
        "alpaca"
    ],

    "paper": [
        "paper"
    ]
}


# ======================================================
# DASHBOARD
# ======================================================

class Dashboard(QWidget):

    login_requested = Signal(dict)

    def __init__(self, controller):

        super().__init__()

        self.controller = controller

        self.setWindowTitle("Sopotek AI Trading Platform")
        self.resize(650, 760)


        self.setStyleSheet(_get_styles())

        main_layout = QVBoxLayout(self)
        main_layout.setAlignment(Qt.AlignCenter)

        self.background = QLabel(self)
        pixmap = QPixmap("assets/logo")

        self.background.setPixmap(pixmap)
        self.background.setScaledContents(True)
        self.background.lower()

        # ------------------------------------------------
        # LOADING SPINNER
        # ------------------------------------------------

        self.spinner = QLabel()
        self.spinner.setAlignment(Qt.AlignCenter)
        self.spinner.setVisible(False)

        self.spinner_movie = QMovie("assets/logo.png")
        self.spinner.setMovie(self.spinner_movie)

        main_layout.addWidget(self.spinner)

        # ------------------------------------------------
        # HEADER
        # ------------------------------------------------

        logo = QLabel("🚀")
        logo.setAlignment(Qt.AlignCenter)
        logo.setFont(QFont("Arial", 50))

        main_layout.addWidget(logo)

        title = QLabel("SOPOTEK AI TRADING PLATFORM")
        title.setAlignment(Qt.AlignCenter)
        title.setFont(QFont("Arial", 20, QFont.Bold))

        main_layout.addWidget(title)

        subtitle = QLabel("Institutional-Grade Algorithmic Infrastructure")
        subtitle.setAlignment(Qt.AlignCenter)
        subtitle.setStyleSheet("color: gray")

        main_layout.addWidget(subtitle)

        # ------------------------------------------------
        # LOGIN CARD
        # ------------------------------------------------

        card = QFrame()
        card.setObjectName("loginCard")
        card.setFixedWidth(520)

        layout = QVBoxLayout(card)

        # ------------------------------------------------
        # ACCOUNT MANAGER
        # ------------------------------------------------

        account_layout = QHBoxLayout()

        self.account_list = QListWidget()
        self.account_list.setMaximumHeight(80)

        account_buttons = QVBoxLayout()

        self.load_account_btn = QPushButton("Load")
        self.delete_account_btn = QPushButton("Delete")

        account_buttons.addWidget(self.load_account_btn)
        account_buttons.addWidget(self.delete_account_btn)

        account_layout.addWidget(self.account_list)
        account_layout.addLayout(account_buttons)

        layout.addLayout(account_layout)

        # ------------------------------------------------
        # FORM
        # ------------------------------------------------

        self.form_layout = QFormLayout()

        self.exchange_type_box = QComboBox()
        self.exchange_type_box.addItems(["crypto", "forex", "stocks", "paper"])

        self.exchange_box = QComboBox()

        self.api_input = QLineEdit()
        self.secret_input = QLineEdit()
        self.secret_input.setEchoMode(QLineEdit.Password)

        self.mode_box = QComboBox()
        self.mode_box.addItems(["paper", "live"])

        self.strategy_box = QComboBox()
        self.strategy_box.addItems([
            "LSTM",
            "EMA_CROSS",
            "RSI_MEAN_REVERSION",
            "MACD_TREND"
        ])

        self.risk_input = QSpinBox()
        self.risk_input.setRange(1, 10)
        self.risk_input.setValue(2)

        self.remember_checkbox = QCheckBox("Save Account")

        self.form_layout.addRow("Exchange Type", self.exchange_type_box)
        self.form_layout.addRow("Exchange", self.exchange_box)
        self.form_layout.addRow("API Key", self.api_input)
        self.form_layout.addRow("Secret", self.secret_input)
        self.form_layout.addRow("Mode", self.mode_box)
        self.form_layout.addRow("Strategy", self.strategy_box)
        self.form_layout.addRow("Risk %", self.risk_input)
        self.form_layout.addRow("", self.remember_checkbox)

        layout.addLayout(self.form_layout)

        # ------------------------------------------------
        # CONNECT BUTTON
        # ------------------------------------------------

        self.connect_button = QPushButton("CONNECT")
        self.connect_button.setFixedHeight(45)

        layout.addWidget(self.connect_button)

        main_layout.addSpacing(20)
        main_layout.addWidget(card, alignment=Qt.AlignCenter)

        # ------------------------------------------------
        # SIGNALS
        # ------------------------------------------------

        self.exchange_type_box.currentTextChanged.connect(self._update_exchange_list)
        self.connect_button.clicked.connect(self._on_connect)

        self.load_account_btn.clicked.connect(self._load_selected_account)
        self.delete_account_btn.clicked.connect(self._delete_account)

        self.login_requested.connect(self._handle_login_async)

        # ------------------------------------------------
        # INIT
        # ------------------------------------------------

        self._update_exchange_list(self.exchange_type_box.currentText())
        self._load_accounts()

    # ======================================================
    # ACCOUNT MANAGEMENT
    # ======================================================

    def _load_accounts(self):

        self.account_list.clear()

        accounts = CredentialManager.list_accounts()

        for acc in accounts:
            self.account_list.addItem(acc)

    def resizeEvent(self, event):
        self.background.resize(self.size())

    def _load_selected_account(self):

        name = self.account_list.currentItem()

        if not name:
            return

        name = name.text()

        creds = CredentialManager.load_account(name)

        if not creds:
            return

        self.exchange_box.setCurrentText(creds["exchange"])
        self.api_input.setText(creds["api_key"])
        self.secret_input.setText(creds["secret"])

    def _delete_account(self):

        name = self.account_list.currentItem()

        if not name:
            return

        name = name.text()

        CredentialManager.delete_account(name)

        self._load_accounts()

    # ======================================================
    # EXCHANGE LIST
    # ======================================================

    def _update_exchange_list(self, exchange_type):

        self.exchange_box.clear()

        exchanges = EXCHANGE_MAP.get(exchange_type, [])

        self.exchange_box.addItems(exchanges)

    # ======================================================
    # CONNECT
    # ======================================================

    def _on_connect(self):

        exchange = self.exchange_box.currentText()

        api_key = self.api_input.text().strip()
        secret = self.secret_input.text().strip()

        if not api_key and exchange != "paper":

            QMessageBox.warning(
                self,
                "Missing Credentials",
                "API credentials required."
            )
            return

        config = {

            "type": self.exchange_type_box.currentText(),

            "broker": {
                "exchange": exchange,
                "mode": self.mode_box.currentText(),
                "api_key": api_key,
                "secret": secret
            },

            "strategy": self.strategy_box.currentText(),

            "risk": {
                "risk_percent": self.risk_input.value()
            }
        }

        if self.remember_checkbox.isChecked():

            name = f"{exchange}_{api_key[:6]}"

            CredentialManager.save_account(name, config)

            self._load_accounts()

        self.show_loading()

        self.login_requested.emit(config)

    # ======================================================
    # ASYNC LOGIN
    # ======================================================

    def _handle_login_async(self, config):

        asyncio.create_task(self.handle_login(config))

    async def handle_login(self, config):

        try:

            await self.controller.handle_login(config)

            self.hide_loading()

        except Exception as e:

            self.hide_loading()

            QMessageBox.critical(
                self,
                "Connection Error",
                str(e)
            )

    # ======================================================
    # LOADING UI
    # ======================================================

    def show_loading(self):

        self.connect_button.setEnabled(False)
        self.connect_button.setText("CONNECTING...")

        self.spinner.setVisible(True)
        self.spinner_movie.start()

    def hide_loading(self):

        self.spinner.setVisible(False)
        self.spinner_movie.stop()

        self.connect_button.setEnabled(True)
        self.connect_button.setText("CONNECT")