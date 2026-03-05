from PySide6.QtCore import Signal, Qt
from PySide6.QtGui import QFont, QMovie
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLineEdit,
    QPushButton, QComboBox, QMessageBox,
    QFormLayout, QCheckBox, QLabel,
    QFrame, QSpinBox
)

from sopotek_trading.backend.services.credential_manager import CredentialManager


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

    QLineEdit:focus, QComboBox:focus {
        border: 1px solid #0078d7;
    }

    QPushButton {
        background-color: #0078d7;
        border-radius: 6px;
        font-weight: bold;
    }

    QPushButton:hover {
        background-color: #0095ff;
    }
    """


class Dashboard(QWidget):

    login_success = Signal(dict)

    def __init__(self,controller):
        super().__init__()
        self.logger=controller.logger

        self.setWindowTitle("Sopotek AI Trading Platform")
        self.resize(650, 760)
        self.setStyleSheet(_get_styles())
        self.symbols = [
            "BTC/USDT",
            "ETH/USDT",
            "SOL/USDT",
            "XRP/USDT",
            "ADA/USDT",
            "XLM/USDT"
        ]

        main_layout = QVBoxLayout(self)
        main_layout.setAlignment(Qt.AlignCenter)
        self.controller=controller

        # =======================
        # SPINNER
        # =======================
        self.spinner = QLabel()
        self.spinner.setAlignment(Qt.AlignCenter)
        self.spinner.setVisible(False)
        self.spinner_movie = QMovie("assets/spinner.gif")
        self.spinner.setMovie(self.spinner_movie)
        main_layout.addWidget(self.spinner)

        # =======================
        # HEADER
        # =======================
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
        subtitle.setStyleSheet("color: gray;")
        main_layout.addWidget(subtitle)

        # =======================
        # LOGIN CARD
        # =======================
        card = QFrame()
        card.setFixedWidth(520)
        card.setObjectName("loginCard")

        layout = QVBoxLayout(card)



        self.form_layout = QFormLayout()

# Exchange selector
        self.exchange_box = QComboBox()
        self.exchange_box.addItems([
    "binance", "binanceus", "coinbase",
    "kraken", "kucoin", "bybit",
    "okx", "gateio", "bitget", "oanda"
])
        self.form_layout.addRow("Exchange:", self.exchange_box)

# ===== Credential container =====
        self.credential_widget = QWidget()
        self.credential_layout = QFormLayout(self.credential_widget)

        self.form_layout.addRow(self.credential_widget)
        self.form_layout.addRow("Exchange:", self.exchange_box)

        # Credential fields (dynamic)
        self.api_input = QLineEdit()
        self.secret_input = QLineEdit()

        # Mode
        self.mode_box = QComboBox()
        self.mode_box.addItems(["paper", "live"])

        # Strategy
        self.strategy_box = QComboBox()
        self.strategy_box.addItems([
            "LSTM",
            "EMA_CROSS",
            "RSI_MEAN_REVERSION",
            "MACD_TREND"
        ])

        # Risk
        self.risk_input = QSpinBox()
        self.risk_input.setRange(1, 10)
        self.risk_input.setValue(2)

        # Advanced
        self.advanced_checkbox = QCheckBox("Advanced Settings")

        self.limit_input = QSpinBox()
        self.limit_input.setRange(100, 5000)
        self.limit_input.setValue(1000)

        self.refresh_input = QSpinBox()
        self.refresh_input.setRange(10, 300)
        self.refresh_input.setValue(60)

        self.rate_limit_input = QSpinBox()
        self.rate_limit_input.setRange(1, 10)
        self.rate_limit_input.setValue(3)

        # Remember
        self.remember_checkbox = QCheckBox("Remember Credentials")

        layout.addLayout(self.form_layout)

        # Add static rows
        self.form_layout.addRow("Mode:", self.mode_box)
        self.form_layout.addRow("Strategy:", self.strategy_box)
        self.form_layout.addRow("Risk % per Trade:", self.risk_input)
        self.form_layout.addRow("", self.advanced_checkbox)

        # Advanced rows
        self.form_layout.addRow("Data Limit:", self.limit_input)
        self.form_layout.addRow("Equity Refresh (sec):", self.refresh_input)
        self.form_layout.addRow("Rate Limit:", self.rate_limit_input)
        self.exchange_type_box = QComboBox()
        self.exchange_type_box.addItems([
    "crypto",
    "forex",
    "stocks",
    "paper"
])

        self.form_layout.addRow("Exchange Type:", self.exchange_type_box)

        self.form_layout.addRow("", self.remember_checkbox)

        # Connect button
        self.connect_button = QPushButton("CONNECT")
        self.connect_button.setFixedHeight(45)
        layout.addWidget(self.connect_button)

        main_layout.addSpacing(20)
        main_layout.addWidget(card, alignment=Qt.AlignCenter)

        # Signals
        self.connect_button.clicked.connect(self._on_connect)
        self.exchange_box.currentTextChanged.connect(self._on_exchange_change)
        self.advanced_checkbox.toggled.connect(self._toggle_advanced)

        # Init
        self._on_exchange_change(self.exchange_box.currentText())
        self._toggle_advanced(False)
        self.exchange_type_box.currentTextChanged.connect(self._update_exchange_list)
        self._update_exchange_list(self.exchange_type_box.currentText())

    # ======================================================
    # DYNAMIC CREDENTIAL FIELDS
    # ======================================================

    def _on_exchange_change(self, exchange):
        self._build_credentials_fields(exchange)
        self._load_saved_credentials(exchange)

    def _build_credentials_fields(self, exchange):

     while self.credential_layout.count():
        item = self.credential_layout.takeAt(0)
        widget = item.widget()
        if widget:
            widget.deleteLater()

     self.api_input = QLineEdit()
     self.secret_input = QLineEdit()
     self.secret_input.setEchoMode(QLineEdit.Password)

     if exchange == "oanda":

        self.api_input.setPlaceholderText("Account ID")
        self.secret_input.setPlaceholderText("API Token")

        self.credential_layout.addRow("Account ID:", self.api_input)
        self.credential_layout.addRow("API Token:", self.secret_input)

     elif exchange == "alpaca":

        self.api_input.setPlaceholderText("API Key")
        self.secret_input.setPlaceholderText("Secret Key")

        self.credential_layout.addRow("API Key:", self.api_input)
        self.credential_layout.addRow("Secret Key:", self.secret_input)

     else:

        self.api_input.setPlaceholderText("API Key")
        self.secret_input.setPlaceholderText("Secret Key")

        self.credential_layout.addRow("API Key:", self.api_input)
        self.credential_layout.addRow("Secret:", self.secret_input)
    # ======================================================
    # ADVANCED SETTINGS TOGGLE
    # ======================================================

    def _toggle_advanced(self, enabled):
        self.limit_input.setVisible(enabled)
        self.refresh_input.setVisible(enabled)
        self.rate_limit_input.setVisible(enabled)

    # ======================================================
    # LOAD CREDENTIALS
    # ======================================================

    def _load_saved_credentials(self, exchange):
        api_key, secret = CredentialManager.load_credentials(exchange)
        self.api_input.setText(api_key or "")
        self.secret_input.setText(secret or "")
        self.remember_checkbox.setChecked(bool(api_key and secret))

    # ======================================================
    # CONNECT
    # ======================================================

    def _on_connect(self):

        exchange = self.exchange_box.currentText()
        mode = self.mode_box.currentText()
        strategy = self.strategy_box.currentText()
        risk_percent = self.risk_input.value()

        if exchange == "oanda":
            api_key = self.api_input.text().strip()
            self.controller.account_id =api_key
            secret = self.secret_input.text().strip()
            self.controller.secret = secret
            secret = ""
        else:

            api_key = self.api_input.text().strip()
            self.controller.api_key = api_key
            secret = self.secret_input.text().strip()
            self.controller.secret = secret

        if not api_key:
            QMessageBox.warning(self, "Missing Credentials", "API credentials required.")
            return

        if mode == "live":
            confirm = QMessageBox.question(
                self,
                "Live Trading Warning",
                "You are about to enable LIVE trading.\nContinue?",
                QMessageBox.Yes | QMessageBox.No
            )
            if confirm != QMessageBox.Yes:
                return

        if self.remember_checkbox.isChecked():
            CredentialManager.save_credentials(exchange, api_key, secret)
        else:
            CredentialManager.delete_credentials(exchange)


        self.config = {

            "type": self.exchange_type_box.currentText(),
            "exchange_name": exchange,
            "mode": mode,
            "strategy": strategy,

            "limit": self.limit_input.value(),
            "equity_refresh": self.refresh_input.value(),
            "risk_percent": risk_percent,

            "api_key": api_key,
            "secret": secret,
            "account_id": api_key if exchange == "oanda" else None,

            "options": {
                "exchange": exchange,
                "rate_limit": self.rate_limit_input.value()
            }
        }

        self.show_loading()
        self.login_success.emit(self.config)

    # ======================================================
    # LOADING CONTROL
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


    def _update_exchange_list(self, exchange_type):

     self.exchange_box.clear()

     if exchange_type == "crypto":

        self.exchange_box.addItems([
            "binance",
            "binanceus",
            "coinbase",
            "kraken",
            "kucoin",
            "bybit",
            "okx",
            "gateio",
            "bitget"
        ])

     elif exchange_type == "forex":

        self.exchange_box.addItems([
            "oanda",
            "fxcm",
            "forex.com",
            "ic markets",
            "fxpro"

        ])

     elif exchange_type == "stocks":

        self.exchange_box.addItems([
            "alpaca",
        "ib_insync (Interactive Brokers)",
        "yfinance",
        "polygon"
        ])
    # ======================================================
    # STYLE
    # ======================================================

