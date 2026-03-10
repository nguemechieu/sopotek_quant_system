from pathlib import Path

from PySide6.QtCore import QSettings, Qt, Signal
from PySide6.QtGui import QMovie, QPixmap
from PySide6.QtWidgets import (
    QBoxLayout,
    QCheckBox,
    QComboBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from config.config import AppConfig, BrokerConfig, RiskConfig, SystemConfig
from config.credential_manager import CredentialManager
from frontend.ui.i18n import iter_supported_languages


ROOT_DIR = Path(__file__).resolve().parents[2]
ASSETS_DIR = ROOT_DIR / "assets"

LOGO_PATH = ASSETS_DIR / "logo.png"
SPINNER_PATH = ASSETS_DIR / "spinner.gif"

EXCHANGE_MAP = {
    "crypto": [
        "binanceus",
        "coinbase",
        "stellar",
        "binance",
        "kraken",
        "kucoin",
        "bybit",
        "okx",
        "gateio",
        "bitget",
    ],
    "forex": ["oanda"],
    "stocks": ["alpaca"],
    "paper": ["paper"],
}

BROKER_COPY = {
    "crypto": "Multi-venue crypto routing with stronger session clarity before execution starts.",
    "forex": "Account-aware FX setup with the fields needed for a cleaner Oanda handoff.",
    "stocks": "Equity sessions tuned for Alpaca with a simpler launch path and saved profiles.",
    "paper": "A zero-risk rehearsal mode that still feels like the real desk experience.",
}

STRATEGY_COPY = {
    "LSTM": "AI-first momentum scanning for traders who want a model-led market read.",
    "EMA_CROSS": "Trend following with a familiar, lower-friction signal structure.",
    "RSI_MEAN_REVERSION": "Best for calmer reversions and oversold bounce setups.",
    "MACD_TREND": "Balanced directional bias for swing-style continuation entries.",

}


class Dashboard(QWidget):
    login_requested = Signal(object)
    LAST_PROFILE_SETTING = "dashboard/last_profile"

    def __init__(self, controller):
        super().__init__()

        self.controller = controller
        self._field_blocks = {}
        self._current_layout_mode = None
        self.settings = getattr(controller, "settings", None) or QSettings("Sopotek", "TradingPlatform")

        self.setWindowTitle("Sopotek AI Trading Platform")
        self.resize(1320, 880)

        self._apply_styles()
        self._build_ui()
        self._connect_signals()
        if hasattr(self.controller, "language_changed"):
            self.controller.language_changed.connect(lambda _code: self.apply_language())

        self._update_exchange_list(self.exchange_type_box.currentText())
        self._load_accounts_index()
        self._load_last_account()
        self._update_optional_fields()
        self._update_broker_hint()
        self._update_session_preview()
        self.apply_language()
        self._sync_shell_layout()

    def _apply_styles(self):
        self.setStyleSheet(
            """
            QWidget {
                background: #0b1220;
                color: #d7dfeb;
                font-family: "Segoe UI", "Aptos", sans-serif;
            }
            QScrollArea {
                border: 0;
                background: transparent;
            }
            QScrollBar:vertical {
                background: #0f1726;
                width: 10px;
                margin: 2px;
                border-radius: 5px;
            }
            QScrollBar::handle:vertical {
                background: #24324a;
                min-height: 30px;
                border-radius: 5px;
            }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                height: 0;
            }
            QFrame#heroPanel {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 #101827, stop:0.6 #0f1726, stop:1 #0b1220);
                border: 1px solid #24324a;
                border-radius: 28px;
            }
            QFrame#connectPanel {
                background: #101827;
                color: #d7dfeb;
                border: 1px solid #24324a;
                border-radius: 28px;
            }
            QFrame#glassCard {
                background-color: #101b2d;
                border: 1px solid #24344f;
                border-radius: 20px;
            }
            QFrame#marketStrip {
                background: #0f1727;
                border: 1px solid #24344f;
                border-radius: 18px;
            }
            QFrame#summaryCard {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                    stop:0 #101b2d, stop:1 #0f1727);
                border: 1px solid #24344f;
                border-radius: 22px;
            }
            QFrame#statPill {
                background: #0f1726;
                border: 1px solid #24324a;
                border-radius: 16px;
            }
            QLabel#eyebrow {
                color: #8fa3bf;
                font-size: 12px;
                font-weight: 800;
                letter-spacing: 0.18em;
                text-transform: uppercase;
            }
            QLabel#heroTitle {
                font-size: 34px;
                font-weight: 800;
                color: #f4f8ff;
            }
            QLabel#heroLead {
                font-size: 14px;
                line-height: 1.5;
                color: #9fb0c7;
            }
            QLabel#heroSectionTitle {
                color: #e6edf7;
                font-size: 18px;
                font-weight: 800;
            }
            QLabel#heroSectionBody {
                color: #9fb0c7;
                font-size: 13px;
            }
            QLabel#panelTitle {
                color: #f4f8ff;
                font-size: 28px;
                font-weight: 800;
            }
            QLabel#panelBody {
                color: #9fb0c7;
                font-size: 14px;
            }
            QLabel#sectionLabel {
                color: #8fa3bf;
                font-size: 12px;
                font-weight: 800;
                letter-spacing: 0.12em;
                text-transform: uppercase;
            }
            QLabel#fieldLabel {
                color: #9fb0c7;
                font-size: 12px;
                font-weight: 700;
                letter-spacing: 0.05em;
                text-transform: uppercase;
            }
            QLabel#hintLabel {
                color: #8fa3bf;
                font-size: 12px;
            }
            QLabel#pillLabel {
                color: #8fa3bf;
                font-size: 12px;
                font-weight: 700;
            }
            QLabel#pillValue {
                color: #f4f8ff;
                font-size: 20px;
                font-weight: 800;
            }
            QLabel#summaryTitle {
                color: #f4f8ff;
                font-size: 18px;
                font-weight: 800;
            }
            QLabel#summaryBody {
                color: #9fb0c7;
                font-size: 13px;
            }
            QLabel#summaryMeta {
                color: #65a3ff;
                font-size: 12px;
                font-weight: 700;
            }
            QLabel#marketTitle {
                color: #dfe8f5;
                font-size: 15px;
                font-weight: 800;
            }
            QLabel#marketBody {
                color: #9fb0c7;
                font-size: 12px;
            }
            QLabel#checkTitle {
                color: #d7dfeb;
                font-size: 13px;
            }
            QLabel#checkStateGood {
                color: #34c27a;
                font-size: 12px;
                font-weight: 800;
            }
            QLabel#checkStateWarn {
                color: #f0a35e;
                font-size: 12px;
                font-weight: 800;
            }
            QLineEdit, QComboBox, QSpinBox {
                background-color: #162033;
                color: #d7dfeb;
                border: 1px solid #2d3a56;
                border-radius: 12px;
                padding: 11px 12px;
                min-height: 22px;
                font-size: 14px;
                selection-background-color: #2a7fff;
            }
            QLineEdit:hover, QComboBox:hover, QSpinBox:hover {
                border-color: #4f638d;
            }
            QLineEdit:focus, QComboBox:focus, QSpinBox:focus {
                border: 1px solid #65a3ff;
            }
            QComboBox::drop-down {
                border: 0;
                width: 26px;
            }
            QComboBox QAbstractItemView {
                background-color: #162033;
                color: #d7dfeb;
                border: 1px solid #2d3a56;
                selection-background-color: #2a7fff;
            }
            QCheckBox {
                color: #c7d2e0;
                font-size: 13px;
                spacing: 8px;
            }
            QCheckBox::indicator {
                width: 18px;
                height: 18px;
                border-radius: 5px;
                border: 1px solid #2d3a56;
                background: #162033;
            }
            QCheckBox::indicator:checked {
                background: #2a7fff;
                border: 1px solid #65a3ff;
            }
            QPushButton#presetButton,
            QPushButton#secondaryButton {
                background-color: #162033;
                color: #d7dfeb;
                border: 1px solid #2d3a56;
                border-radius: 12px;
                padding: 10px 14px;
                font-size: 12px;
                font-weight: 800;
            }
            QPushButton#presetButton:hover,
            QPushButton#secondaryButton:hover {
                background: #1b2940;
                border-color: #4f638d;
            }
            QPushButton#connectButton {
                background: #2a7fff;
                color: white;
                border: 0;
                border-radius: 16px;
                padding: 15px 20px;
                font-size: 15px;
                font-weight: 800;
            }
            QPushButton#connectButton:hover {
                background: #3d8dff;
            }
            QPushButton#connectButton:pressed {
                background: #1f68d6;
            }
            """
        )

    def _tr(self, key, **kwargs):
        if hasattr(self.controller, "tr"):
            return self.controller.tr(key, **kwargs)
        return key

    def _language_box_current_code(self):
        if not hasattr(self, "language_box") or self.language_box is None:
            return None
        return self.language_box.currentData()

    def _credential_field_schema(self):
        broker_type = self.exchange_type_box.currentText() if hasattr(self, "exchange_type_box") else ""
        exchange = self.exchange_box.currentText() if hasattr(self, "exchange_box") else ""

        schema = {
            "api_label": self._tr("dashboard.api_key"),
            "api_placeholder": "Public key or broker token",
            "secret_label": self._tr("dashboard.secret"),
            "secret_placeholder": "Secret key",
            "secret_echo": QLineEdit.Password,
            "account_label": self._tr("dashboard.account_id"),
            "account_placeholder": "Required for Oanda",
        }

        if exchange == "stellar":
            schema.update(
                {
                    "api_label": "Public Key",
                    "api_placeholder": "Stellar public key",
                    "secret_label": "Private Key",
                    "secret_placeholder": "Stellar private key",
                    "secret_echo": QLineEdit.Password,
                }
            )
        elif exchange == "oanda" or broker_type == "forex":
            schema.update(
                {
                    "api_label": "Account ID",
                    "api_placeholder": "Oanda account ID",
                    "secret_label": "API Key",
                    "secret_placeholder": "Oanda API key",
                    "secret_echo": QLineEdit.Normal,
                }
            )

        return schema

    def _apply_credential_field_schema(self):
        schema = self._credential_field_schema()

        api_block = self._field_blocks.get("api")
        if api_block is not None:
            api_block.label_widget.setText(schema["api_label"])
        self.api_input.setPlaceholderText(schema["api_placeholder"])

        secret_block = self._field_blocks.get("secret")
        if secret_block is not None:
            secret_block.label_widget.setText(schema["secret_label"])
        self.secret_input.setPlaceholderText(schema["secret_placeholder"])
        self.secret_input.setEchoMode(schema["secret_echo"])

        account_block = self._field_blocks.get("account_id")
        if account_block is not None:
            account_block.label_widget.setText(schema["account_label"])
        self.account_id_input.setPlaceholderText(schema["account_placeholder"])

    def _resolved_broker_inputs(self):
        broker_type = self.exchange_type_box.currentText()
        exchange = self.exchange_box.currentText()
        api_value = self.api_input.text().strip()
        secret_value = self.secret_input.text().strip()
        password_value = self.password_input.text().strip()
        account_value = self.account_id_input.text().strip()

        if exchange == "oanda" or broker_type == "forex":
            return {
                "api_key": secret_value,
                "secret": None,
                "password": password_value or None,
                "account_id": api_value or None,
            }

        return {
            "api_key": api_value or None,
            "secret": secret_value or None,
            "password": password_value or None,
            "account_id": account_value or None,
        }

    def _build_ui(self):
        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

        container = QWidget()
        outer_layout = QVBoxLayout(container)
        outer_layout.setContentsMargins(28, 28, 28, 28)

        self.shell = QWidget()
        self.shell_layout = QBoxLayout(QBoxLayout.LeftToRight, self.shell)
        self.shell_layout.setContentsMargins(0, 0, 0, 0)
        self.shell_layout.setSpacing(22)

        self.hero_panel = self._build_hero_panel()
        self.connect_panel = self._build_connect_panel()
        self.shell_layout.addWidget(self.hero_panel, 7)
        self.shell_layout.addWidget(self.connect_panel, 5)

        outer_layout.addWidget(self.shell)
        scroll.setWidget(container)
        root_layout.addWidget(scroll)

    def _build_hero_panel(self):
        panel = QFrame()
        panel.setObjectName("heroPanel")
        panel_layout = QVBoxLayout(panel)
        panel_layout.setContentsMargins(34, 34, 34, 34)
        panel_layout.setSpacing(22)

        top_row = QHBoxLayout()
        top_row.setSpacing(18)

        logo = QLabel()
        pixmap = QPixmap(str(LOGO_PATH)) if LOGO_PATH.exists() else QPixmap()
        if not pixmap.isNull():
            logo.setPixmap(
                pixmap.scaled(110, 110, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            )
        logo.setFixedSize(116, 116)
        logo.setAlignment(Qt.AlignCenter)
        top_row.addWidget(logo, 0, Qt.AlignTop)

        headline_col = QVBoxLayout()
        headline_col.setSpacing(8)

        self.eyebrow_label = QLabel("AI Trading Command Deck")
        self.eyebrow_label.setObjectName("eyebrow")
        headline_col.addWidget(self.eyebrow_label)

        self.hero_title_label = QLabel("Sopotek AI Trading Platform")
        self.hero_title_label.setObjectName("heroTitle")
        self.hero_title_label.setWordWrap(True)
        headline_col.addWidget(self.hero_title_label)

        self.hero_lead_label = QLabel(
            "Configure broker, strategy, and risk profile before launching the trading terminal. "
            "This screen is designed to make the next step feel obvious, calm, and safe."
        )
        self.hero_lead_label.setObjectName("heroLead")
        self.hero_lead_label.setWordWrap(True)
        headline_col.addWidget(self.hero_lead_label)

        top_row.addLayout(headline_col, 1)
        panel_layout.addLayout(top_row)

        pills_row = QHBoxLayout()
        pills_row.setSpacing(12)
        pills_row.addWidget(self._create_stat_pill("Session", "Paper", value_attr="session_pill_value"))
        pills_row.addWidget(self._create_stat_pill("Readiness", "58%", value_attr="readiness_pill_value"))
        pills_row.addWidget(self._create_stat_pill("Market Reach", "Multi-Asset", value_attr="market_pill_value"))
        panel_layout.addLayout(pills_row)

        market_card = QFrame()
        market_card.setObjectName("glassCard")
        market_layout = QVBoxLayout(market_card)
        market_layout.setContentsMargins(20, 20, 20, 20)
        market_layout.setSpacing(12)

        self.market_title_label = QLabel("Desk Snapshot")
        self.market_title_label.setObjectName("heroSectionTitle")
        market_layout.addWidget(self.market_title_label)

        self.market_body_label = QLabel(
            "Use the dashboard like a pre-flight panel: confirm broker type, strategy posture, and credentials before the terminal takes over."
        )
        self.market_body_label.setObjectName("heroSectionBody")
        self.market_body_label.setWordWrap(True)
        market_layout.addWidget(self.market_body_label)

        self.market_primary = self._create_market_strip("Primary Venue", "Binance US selected with paper rehearsal guardrails.")
        self.market_secondary = self._create_market_strip("Strategy Lens", "LSTM with moderate risk budget and cleaner onboarding copy.")
        self.market_tertiary = self._create_market_strip("Operator Signal", "Saved profile support keeps repeat sessions faster and safer.")
        market_layout.addWidget(self.market_primary)
        market_layout.addWidget(self.market_secondary)
        market_layout.addWidget(self.market_tertiary)
        panel_layout.addWidget(market_card)

        lower_grid = QGridLayout()
        lower_grid.setHorizontalSpacing(14)
        lower_grid.setVerticalSpacing(14)

        checklist_card = QFrame()
        checklist_card.setObjectName("glassCard")
        checklist_layout = QVBoxLayout(checklist_card)
        checklist_layout.setContentsMargins(20, 20, 20, 20)
        checklist_layout.setSpacing(12)

        self.checklist_title_label = QLabel("Launch Checklist")
        self.checklist_title_label.setObjectName("heroSectionTitle")
        checklist_layout.addWidget(self.checklist_title_label)

        self.check_credentials = self._create_checklist_row("Credentials", "Needs input")
        self.check_broker = self._create_checklist_row("Broker setup", "Ready")
        self.check_strategy = self._create_checklist_row("Strategy plan", "Ready")
        self.check_risk = self._create_checklist_row("Risk profile", "Conservative")
        checklist_layout.addWidget(self.check_credentials)
        checklist_layout.addWidget(self.check_broker)
        checklist_layout.addWidget(self.check_strategy)
        checklist_layout.addWidget(self.check_risk)
        checklist_layout.addStretch(1)

        notes_card = QFrame()
        notes_card.setObjectName("glassCard")
        notes_layout = QVBoxLayout(notes_card)
        notes_layout.setContentsMargins(20, 20, 20, 20)
        notes_layout.setSpacing(10)

        self.notes_title_label = QLabel("Session Notes")
        self.notes_title_label.setObjectName("heroSectionTitle")
        notes_layout.addWidget(self.notes_title_label)

        self.notes_bullet_labels = []
        for line in [
            "Paper mode is the safest way to verify broker setup and chart loading.",
            "Broker-specific fields appear only when the selected venue requires them.",
            "Saved profiles help repeat sessions start faster.",
            "Live sessions should be reviewed carefully before launch.",
        ]:
            item = QLabel(line)
            item.setObjectName("heroSectionBody")
            item.setWordWrap(True)
            notes_layout.addWidget(item)
            self.notes_bullet_labels.append(item)

        lower_grid.addWidget(checklist_card, 0, 0)
        lower_grid.addWidget(notes_card, 0, 1)
        panel_layout.addLayout(lower_grid)
        panel_layout.addStretch(1)
        return panel

    def _build_connect_panel(self):
        panel = QFrame()
        panel.setObjectName("connectPanel")
        panel_layout = QVBoxLayout(panel)
        panel_layout.setContentsMargins(28, 28, 28, 28)
        panel_layout.setSpacing(16)

        self.connect_title_label = QLabel("Launch Session")
        self.connect_title_label.setObjectName("panelTitle")
        panel_layout.addWidget(self.connect_title_label)

        self.connect_body_label = QLabel(
            "Choose broker access, session mode, credentials, and risk settings before opening the trading workspace."
        )
        self.connect_body_label.setObjectName("panelBody")
        self.connect_body_label.setWordWrap(True)
        panel_layout.addWidget(self.connect_body_label)

        self.presets_label = QLabel("Quick Presets")
        self.presets_label.setObjectName("sectionLabel")
        panel_layout.addWidget(self.presets_label)

        presets_row = QHBoxLayout()
        presets_row.setSpacing(8)
        self.paper_preset_button = self._create_preset_button("Paper Warmup")
        self.crypto_preset_button = self._create_preset_button("Crypto Live")
        self.fx_preset_button = self._create_preset_button("FX Live")
        presets_row.addWidget(self.paper_preset_button)
        presets_row.addWidget(self.crypto_preset_button)
        presets_row.addWidget(self.fx_preset_button)
        panel_layout.addLayout(presets_row)

        self.profile_label = QLabel("Saved Profiles")
        self.profile_label.setObjectName("sectionLabel")
        panel_layout.addWidget(self.profile_label)

        profile_row = QHBoxLayout()
        profile_row.setSpacing(10)
        self.saved_account_box = QComboBox()
        self.saved_account_box.addItem("Recent profiles")
        profile_row.addWidget(self._wrap_field("Choose Profile", self.saved_account_box), 1)

        self.refresh_accounts_button = QPushButton("Refresh")
        self.refresh_accounts_button.setObjectName("secondaryButton")
        profile_row.addWidget(self.refresh_accounts_button, 0, Qt.AlignBottom)
        panel_layout.addLayout(profile_row)

        language_row = QHBoxLayout()
        language_row.setSpacing(10)
        self.language_box = QComboBox()
        for code, label in iter_supported_languages():
            self.language_box.addItem(label, code)
        language_row.addWidget(self._wrap_field("Language", self.language_box, block_name="language"), 1)
        panel_layout.addLayout(language_row)

        self.market_label = QLabel("Market Access")
        self.market_label.setObjectName("sectionLabel")
        panel_layout.addWidget(self.market_label)

        market_row = QHBoxLayout()
        market_row.setSpacing(10)
        self.exchange_type_box = QComboBox()
        self.exchange_type_box.addItems(["crypto", "forex", "stocks", "paper"])
        self.exchange_box = QComboBox()
        market_row.addWidget(self._wrap_field("Broker Type", self.exchange_type_box), 1)
        market_row.addWidget(self._wrap_field("Exchange", self.exchange_box), 1)
        panel_layout.addLayout(market_row)

        strategy_row = QHBoxLayout()
        strategy_row.setSpacing(10)
        self.mode_box = QComboBox()
        self.mode_box.addItems(["live", "paper"])
        self.strategy_box = QComboBox()
        self.strategy_box.addItems(["LSTM", "EMA_CROSS", "RSI_MEAN_REVERSION", "MACD_TREND"])
        strategy_row.addWidget(self._wrap_field("Mode", self.mode_box), 1)
        strategy_row.addWidget(self._wrap_field("Strategy", self.strategy_box), 1)
        panel_layout.addLayout(strategy_row)

        self.credentials_label = QLabel("Credentials")
        self.credentials_label.setObjectName("sectionLabel")
        panel_layout.addWidget(self.credentials_label)

        self.api_input = QLineEdit()
        self.api_input.setPlaceholderText("Public key or broker token")
        panel_layout.addWidget(self._wrap_field("API Key", self.api_input, block_name="api"))

        self.secret_input = QLineEdit()
        self.secret_input.setEchoMode(QLineEdit.Password)
        self.secret_input.setPlaceholderText("Secret key")
        panel_layout.addWidget(self._wrap_field("Secret", self.secret_input, block_name="secret"))

        credential_row = QHBoxLayout()
        credential_row.setSpacing(10)

        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.Password)
        self.password_input.setPlaceholderText("Exchange passphrase when required")
        credential_row.addWidget(self._wrap_field("Passphrase", self.password_input, block_name="password"), 1)

        self.account_id_input = QLineEdit()
        self.account_id_input.setPlaceholderText("Required for Oanda")
        credential_row.addWidget(self._wrap_field("Account ID", self.account_id_input, block_name="account_id"), 1)
        panel_layout.addLayout(credential_row)

        self.risk_label = QLabel("Risk and Persistence")
        self.risk_label.setObjectName("sectionLabel")
        panel_layout.addWidget(self.risk_label)

        risk_row = QHBoxLayout()
        risk_row.setSpacing(10)
        self.risk_input = QSpinBox()
        self.risk_input.setRange(1, 100)
        self.risk_input.setValue(2)
        self.risk_input.setSuffix(" %")
        risk_row.addWidget(self._wrap_field("Risk Budget", self.risk_input), 1)

        self.remember_checkbox = QCheckBox("Save this broker profile")
        self.remember_checkbox.setChecked(True)
        remember_wrap = QWidget()
        remember_layout = QVBoxLayout(remember_wrap)
        remember_layout.setContentsMargins(0, 18, 0, 0)
        remember_layout.addWidget(self.remember_checkbox)
        risk_row.addWidget(remember_wrap, 1)
        panel_layout.addLayout(risk_row)

        summary_card = QFrame()
        summary_card.setObjectName("summaryCard")
        summary_layout = QVBoxLayout(summary_card)
        summary_layout.setContentsMargins(18, 18, 18, 18)
        summary_layout.setSpacing(8)

        self.summary_title = QLabel("Paper desk ready")
        self.summary_title.setObjectName("summaryTitle")
        summary_layout.addWidget(self.summary_title)

        self.summary_body = QLabel("Start with a paper rehearsal, verify the broker shape, then move into the full terminal.")
        self.summary_body.setObjectName("summaryBody")
        self.summary_body.setWordWrap(True)
        summary_layout.addWidget(self.summary_body)

        self.summary_meta = QLabel("Risk 2%  |  Strategy LSTM  |  Profile not saved yet")
        self.summary_meta.setObjectName("summaryMeta")
        self.summary_meta.setWordWrap(True)
        summary_layout.addWidget(self.summary_meta)

        panel_layout.addWidget(summary_card)

        self.broker_hint = QLabel()
        self.broker_hint.setObjectName("hintLabel")
        self.broker_hint.setWordWrap(True)
        panel_layout.addWidget(self.broker_hint)

        self.spinner = QLabel()
        self.spinner.setAlignment(Qt.AlignCenter)
        self.spinner.setVisible(False)
        self.spinner_movie = QMovie(str(SPINNER_PATH)) if SPINNER_PATH.exists() else None
        if self.spinner_movie is not None:
            self.spinner.setMovie(self.spinner_movie)
        panel_layout.addWidget(self.spinner)

        self.connect_button = QPushButton("Open Paper Terminal")
        self.connect_button.setObjectName("connectButton")
        self.connect_button.setMinimumHeight(56)
        panel_layout.addWidget(self.connect_button)

        self.footer_label = QLabel(
            "Start in paper mode when testing a new broker or strategy combination. "
            "The dashboard is designed to make that transition obvious and fast."
        )
        self.footer_label.setObjectName("hintLabel")
        self.footer_label.setWordWrap(True)
        panel_layout.addWidget(self.footer_label)

        panel_layout.addStretch(1)
        return panel

    def _create_stat_pill(self, label, value, value_attr=None):
        pill = QFrame()
        pill.setObjectName("statPill")
        pill.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        layout = QVBoxLayout(pill)
        layout.setContentsMargins(16, 14, 16, 14)
        layout.setSpacing(4)

        label_widget = QLabel(label)
        label_widget.setObjectName("pillLabel")
        value_widget = QLabel(value)
        value_widget.setObjectName("pillValue")
        layout.addWidget(label_widget)
        layout.addWidget(value_widget)

        if value_attr:
            setattr(self, value_attr, value_widget)
        return pill

    def _create_market_strip(self, title, body):
        card = QFrame()
        card.setObjectName("marketStrip")
        layout = QVBoxLayout(card)
        layout.setContentsMargins(16, 14, 16, 14)
        layout.setSpacing(4)

        title_label = QLabel(title)
        title_label.setObjectName("marketTitle")
        body_label = QLabel(body)
        body_label.setObjectName("marketBody")
        body_label.setWordWrap(True)

        layout.addWidget(title_label)
        layout.addWidget(body_label)

        card.title_label = title_label
        card.body_label = body_label
        return card

    def _create_checklist_row(self, title, state):
        row = QFrame()
        layout = QHBoxLayout(row)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        title_label = QLabel(title)
        title_label.setObjectName("checkTitle")
        state_label = QLabel(state)
        state_label.setObjectName("checkStateWarn")

        layout.addWidget(title_label)
        layout.addStretch(1)
        layout.addWidget(state_label)

        row.title_label = title_label
        row.state_label = state_label
        return row

    def _create_preset_button(self, text):
        button = QPushButton(text)
        button.setObjectName("presetButton")
        button.setCursor(Qt.PointingHandCursor)
        return button

    def _wrap_field(self, label_text, widget, block_name=None):
        block = QFrame()
        block_layout = QVBoxLayout(block)
        block_layout.setContentsMargins(0, 0, 0, 0)
        block_layout.setSpacing(7)

        label = QLabel(label_text)
        label.setObjectName("fieldLabel")
        block_layout.addWidget(label)
        block_layout.addWidget(widget)
        block.label_widget = label
        block.field_widget = widget

        if block_name:
            self._field_blocks[block_name] = block

        return block

    def _connect_signals(self):
        self.exchange_type_box.currentTextChanged.connect(self._update_exchange_list)
        self.exchange_type_box.currentTextChanged.connect(self._update_optional_fields)
        self.exchange_type_box.currentTextChanged.connect(self._update_broker_hint)
        self.exchange_type_box.currentTextChanged.connect(self._update_session_preview)
        self.exchange_box.currentTextChanged.connect(self._update_optional_fields)
        self.exchange_box.currentTextChanged.connect(self._update_broker_hint)
        self.exchange_box.currentTextChanged.connect(self._update_session_preview)
        self.mode_box.currentTextChanged.connect(self._update_broker_hint)
        self.mode_box.currentTextChanged.connect(self._update_session_preview)
        self.strategy_box.currentTextChanged.connect(self._update_session_preview)
        self.risk_input.valueChanged.connect(self._update_session_preview)
        self.api_input.textChanged.connect(self._update_session_preview)
        self.secret_input.textChanged.connect(self._update_session_preview)
        self.password_input.textChanged.connect(self._update_session_preview)
        self.account_id_input.textChanged.connect(self._update_session_preview)
        self.remember_checkbox.toggled.connect(self._update_session_preview)
        self.saved_account_box.currentTextChanged.connect(self._load_selected_account)
        self.refresh_accounts_button.clicked.connect(self._load_accounts_index)
        self.language_box.currentIndexChanged.connect(self._on_language_changed)
        self.paper_preset_button.clicked.connect(lambda: self._apply_preset("paper"))
        self.crypto_preset_button.clicked.connect(lambda: self._apply_preset("crypto"))
        self.fx_preset_button.clicked.connect(lambda: self._apply_preset("forex"))
        self.connect_button.clicked.connect(self._on_connect)

    def _on_language_changed(self, _index):
        language_code = self._language_box_current_code()
        if not language_code or not hasattr(self.controller, "set_language"):
            return
        self.controller.set_language(language_code)

    def apply_language(self):
        self.setWindowTitle(self._tr("dashboard.window_title"))

        self.eyebrow_label.setText(self._tr("dashboard.hero_eyebrow"))
        self.hero_title_label.setText(self._tr("dashboard.hero_title"))
        self.hero_lead_label.setText(self._tr("dashboard.hero_lead"))
        self.market_title_label.setText(self._tr("dashboard.desk_snapshot_title"))
        self.market_body_label.setText(self._tr("dashboard.desk_snapshot_body"))
        self.market_primary.title_label.setText(self._tr("dashboard.market_primary_title"))
        self.market_secondary.title_label.setText(self._tr("dashboard.market_secondary_title"))
        self.market_tertiary.title_label.setText(self._tr("dashboard.market_tertiary_title"))

        self.checklist_title_label.setText(self._tr("dashboard.launch_checklist_title"))
        self.check_credentials.title_label.setText(self._tr("dashboard.check_credentials_title"))
        self.check_broker.title_label.setText(self._tr("dashboard.check_broker_title"))
        self.check_strategy.title_label.setText(self._tr("dashboard.check_strategy_title"))
        self.check_risk.title_label.setText(self._tr("dashboard.check_risk_title"))

        self.notes_title_label.setText(self._tr("dashboard.notes_title"))
        for label, key in zip(
            self.notes_bullet_labels,
            (
                "dashboard.notes_bullet_1",
                "dashboard.notes_bullet_2",
                "dashboard.notes_bullet_3",
                "dashboard.notes_bullet_4",
            ),
        ):
            label.setText(self._tr(key))

        self.connect_title_label.setText(self._tr("dashboard.connect_title"))
        self.connect_body_label.setText(self._tr("dashboard.connect_body"))
        self.presets_label.setText(self._tr("dashboard.quick_presets"))
        self.paper_preset_button.setText(self._tr("dashboard.paper_preset"))
        self.crypto_preset_button.setText(self._tr("dashboard.crypto_preset"))
        self.fx_preset_button.setText(self._tr("dashboard.fx_preset"))
        self.profile_label.setText(self._tr("dashboard.saved_profiles"))
        self.refresh_accounts_button.setText(self._tr("dashboard.refresh"))
        self.market_label.setText(self._tr("dashboard.market_access"))
        self.credentials_label.setText(self._tr("dashboard.credentials"))
        self.risk_label.setText(self._tr("dashboard.risk_persistence"))
        self.remember_checkbox.setText(self._tr("dashboard.save_profile"))

        field_labels = {
            "language": "dashboard.language",
            "api": "dashboard.api_key",
            "secret": "dashboard.secret",
            "password": "dashboard.passphrase",
            "account_id": "dashboard.account_id",
        }
        for block_name, key in field_labels.items():
            block = self._field_blocks.get(block_name)
            if block is not None:
                block.label_widget.setText(self._tr(key))

        self.password_input.setPlaceholderText("Exchange passphrase when required")
        self._apply_credential_field_schema()

        if self.language_box is not None:
            current_code = getattr(self.controller, "language_code", self._language_box_current_code())
            self.language_box.blockSignals(True)
            for index, (code, label) in enumerate(iter_supported_languages()):
                self.language_box.setItemText(index, label)
                if code == current_code:
                    self.language_box.setCurrentIndex(index)
            self.language_box.blockSignals(False)

        recent_text = self._tr("dashboard.recent_profiles")
        if self.saved_account_box.count() > 0:
            self.saved_account_box.blockSignals(True)
            self.saved_account_box.setItemText(0, recent_text)
            self.saved_account_box.setItemData(0, "__recent__")
            self.saved_account_box.blockSignals(False)

        profile_block = self.saved_account_box.parentWidget()
        if profile_block is not None and hasattr(profile_block, "label_widget"):
            profile_block.label_widget.setText(self._tr("dashboard.choose_profile"))

        exchange_type_block = self.exchange_type_box.parentWidget()
        if exchange_type_block is not None and hasattr(exchange_type_block, "label_widget"):
            exchange_type_block.label_widget.setText(self._tr("dashboard.broker_type"))

        exchange_block = self.exchange_box.parentWidget()
        if exchange_block is not None and hasattr(exchange_block, "label_widget"):
            exchange_block.label_widget.setText(self._tr("dashboard.exchange"))

        mode_block = self.mode_box.parentWidget()
        if mode_block is not None and hasattr(mode_block, "label_widget"):
            mode_block.label_widget.setText(self._tr("dashboard.mode"))

        strategy_block = self.strategy_box.parentWidget()
        if strategy_block is not None and hasattr(strategy_block, "label_widget"):
            strategy_block.label_widget.setText(self._tr("dashboard.strategy"))

        risk_block = self.risk_input.parentWidget()
        if risk_block is not None and hasattr(risk_block, "label_widget"):
            risk_block.label_widget.setText(self._tr("dashboard.risk_budget"))

        self._update_session_preview()

    def _load_accounts_index(self):
        current = self.saved_account_box.currentText()
        accounts = CredentialManager.list_accounts()
        self.saved_account_box.blockSignals(True)
        self.saved_account_box.clear()
        self.saved_account_box.addItem(self._tr("dashboard.recent_profiles"), "__recent__")
        self.saved_account_box.addItems(accounts)
        if current in accounts:
            self.saved_account_box.setCurrentText(current)
        self.saved_account_box.blockSignals(False)
        self._update_session_preview()

    def _load_selected_account(self, account_name):
        if not account_name or self.saved_account_box.currentData() == "__recent__":
            return

        creds = CredentialManager.load_account(account_name)
        if not creds:
            return

        CredentialManager.touch_account(account_name)
        self.settings.setValue(self.LAST_PROFILE_SETTING, account_name)

        broker = creds.get("broker", {})
        self.exchange_type_box.setCurrentText(broker.get("type", "crypto"))
        self._update_exchange_list(broker.get("type", "crypto"))
        self.exchange_box.setCurrentText(broker.get("exchange", ""))
        if broker.get("exchange") == "oanda" or broker.get("type") == "forex":
            self.api_input.setText(broker.get("account_id", ""))
            self.secret_input.setText(broker.get("api_key", ""))
            self.account_id_input.clear()
        else:
            self.api_input.setText(broker.get("api_key", ""))
            self.secret_input.setText(broker.get("secret", ""))
            self.account_id_input.setText(broker.get("account_id", ""))
        self.password_input.setText(broker.get("password") or broker.get("passphrase", ""))
        self.mode_box.setCurrentText(broker.get("mode", "paper"))
        self.risk_input.setValue(int(creds.get("risk", {}).get("risk_percent", 2) or 2))
        self.strategy_box.setCurrentText(creds.get("strategy", "EMA_CROSS"))

        self._update_optional_fields()
        self._update_broker_hint()
        self._update_session_preview()

    def _load_last_account(self):
        accounts = CredentialManager.list_accounts()
        if not accounts:
            return
        saved_last = str(self.settings.value(self.LAST_PROFILE_SETTING, "") or "").strip()
        target = saved_last if saved_last in accounts else accounts[0]
        self.saved_account_box.setCurrentText(target)
        self._load_selected_account(target)

    def _apply_preset(self, preset_name):
        if preset_name == "paper":
            self.exchange_type_box.setCurrentText("paper")
            self._update_exchange_list("paper")
            self.exchange_box.setCurrentText("paper")
            self.mode_box.setCurrentText("paper")
            self.strategy_box.setCurrentText("LSTM")
            self.risk_input.setValue(2)
        elif preset_name == "crypto":
            self.exchange_type_box.setCurrentText("crypto")
            self._update_exchange_list("crypto")
            if self.exchange_box.findText("binanceus") >= 0:
                self.exchange_box.setCurrentText("binanceus")
            self.mode_box.setCurrentText("live")
            self.strategy_box.setCurrentText("EMA_CROSS")
            self.risk_input.setValue(2)
        elif preset_name == "forex":
            self.exchange_type_box.setCurrentText("forex")
            self._update_exchange_list("forex")
            self.exchange_box.setCurrentText("oanda")
            self.mode_box.setCurrentText("live")
            self.strategy_box.setCurrentText("MACD_TREND")
            self.risk_input.setValue(1)

        self._update_optional_fields()
        self._update_broker_hint()
        self._update_session_preview()

    def _update_exchange_list(self, exchange_type):
        current = self.exchange_box.currentText()
        exchanges = EXCHANGE_MAP.get(exchange_type, [])

        self.exchange_box.blockSignals(True)
        self.exchange_box.clear()
        self.exchange_box.addItems(exchanges)
        if current in exchanges:
            self.exchange_box.setCurrentText(current)
        self.exchange_box.blockSignals(False)

    def _update_optional_fields(self):
        broker_type = self.exchange_type_box.currentText()
        exchange = self.exchange_box.currentText()
        is_paper = broker_type == "paper" or exchange == "paper"
        uses_mapped_account_field = broker_type == "forex" or exchange == "oanda"
        needs_account_id = uses_mapped_account_field
        needs_password = exchange in {"coinbase", "okx", "kucoin"}

        self._field_blocks["api"].setVisible(not is_paper)
        self._field_blocks["secret"].setVisible(not is_paper)
        self._field_blocks["account_id"].setVisible(needs_account_id and not uses_mapped_account_field)
        self._field_blocks["password"].setVisible((not is_paper) and needs_password)

        if uses_mapped_account_field:
            self.account_id_input.clear()

        if is_paper:
            self.mode_box.blockSignals(True)
            self.mode_box.setCurrentText("paper")
            self.mode_box.blockSignals(False)
            self.mode_box.setEnabled(False)
        else:
            self.mode_box.setEnabled(True)

        self._apply_credential_field_schema()

    def _update_broker_hint(self):
        broker_type = self.exchange_type_box.currentText()
        exchange = self.exchange_box.currentText()
        mode = self.mode_box.currentText()

        copy = BROKER_COPY.get(broker_type, "Configure a broker session and launch the terminal.")
        if exchange:
            copy = f"{exchange.upper()} in {mode.upper()} mode. {copy}"
        if exchange == "stellar":
            copy += (
                " Use your Stellar public key in the first field. "
                "The private key is optional for read-only market data, but required for order execution."
            )
        elif exchange == "oanda" or broker_type == "forex":
            copy += " Enter Oanda account ID in the first field and API key in the second field."
        self.broker_hint.setText(copy)

    def _set_check_state(self, row, text, is_ready):
        row.state_label.setText(text)
        row.state_label.setObjectName("checkStateGood" if is_ready else "checkStateWarn")
        row.state_label.style().unpolish(row.state_label)
        row.state_label.style().polish(row.state_label)

    def _update_session_preview(self):
        broker_type = self.exchange_type_box.currentText()
        exchange = self.exchange_box.currentText() or "paper"
        mode = self.mode_box.currentText()
        strategy = self.strategy_box.currentText()
        risk_value = self.risk_input.value()

        is_paper = broker_type == "paper" or exchange == "paper" or mode == "paper"
        needs_credentials = exchange != "paper" and broker_type != "paper"
        needs_account_id = broker_type == "forex" or exchange == "oanda"
        needs_password = exchange in {"coinbase", "okx", "kucoin"} and not is_paper

        resolved = self._resolved_broker_inputs()
        has_api = bool(resolved.get("api_key"))
        has_secret = bool(resolved.get("secret"))
        has_account_id = bool(resolved.get("account_id"))
        has_password = bool(resolved.get("password"))
        if exchange == "oanda" or broker_type == "forex":
            credentials_ready = has_api and has_account_id
        elif exchange == "stellar":
            credentials_ready = has_api
        else:
            credentials_ready = not needs_credentials or (has_api and has_secret and (not needs_password or has_password))

        readiness = 20
        readiness += 20 if exchange else 0
        readiness += 15 if strategy else 0
        readiness += 15 if risk_value <= 3 else 8
        readiness += 20 if credentials_ready else 0
        readiness += 10 if (not needs_account_id or has_account_id) else 0
        readiness = max(0, min(100, readiness))

        session_label = "Paper" if is_paper else "Live"
        market_reach = {
            "crypto": "Crypto Desk",
            "forex": "FX Desk",
            "stocks": "Equity Desk",
            "paper": "Paper Desk",
        }.get(broker_type, "Multi-Asset")

        self.session_pill_value.setText(session_label)
        self.readiness_pill_value.setText(f"{readiness}%")
        self.market_pill_value.setText(market_reach)

        venue_label = exchange.upper() if exchange else "PAPER"
        strategy_copy = STRATEGY_COPY.get(strategy, "The selected strategy is ready for a cleaner launch flow.")
        mode_copy = "paper rehearsal" if is_paper else "live execution"

        self.market_primary.body_label.setText(
            f"{venue_label} selected with {mode_copy} framing and a more readable broker handoff."
        )
        self.market_secondary.body_label.setText(f"{strategy} selected. {strategy_copy}")
        self.market_tertiary.body_label.setText(
            f"Risk budget is {risk_value}%. Profiles are {'saved' if self.remember_checkbox.isChecked() else 'temporary'} for this session."
        )

        broker_ready = bool(exchange)
        strategy_ready = bool(strategy)
        risk_ready = risk_value <= 3

        self._set_check_state(self.check_credentials, "Ready" if credentials_ready else "Needs input", credentials_ready)
        self._set_check_state(self.check_broker, "Ready" if broker_ready else "Choose venue", broker_ready)
        self._set_check_state(self.check_strategy, strategy if strategy_ready else "Choose strategy", strategy_ready)
        self._set_check_state(self.check_risk, "Conservative" if risk_ready else "Aggressive", risk_ready)

        if is_paper:
            self.summary_title.setText("Paper desk ready")
            self.summary_body.setText(
                "This setup is optimized for a safer rehearsal. You can validate the broker shape, charts, and strategy flow before taking live risk."
            )
            self.connect_button.setText("Open Paper Terminal")
        else:
            self.summary_title.setText(f"{venue_label} live launch")
            self.summary_body.setText(
                "This session is configured for live execution. Review credentials, account details, and the selected strategy before entering the terminal."
            )
            self.connect_button.setText("Launch Live Trading Terminal")

        profile_state = self.saved_account_box.currentText()
        profile_copy = profile_state if profile_state and profile_state != "Recent profiles" else "Profile not saved yet"
        self.summary_meta.setText(f"Risk {risk_value}%  |  Strategy {strategy}  |  {profile_copy}")

    def _sync_shell_layout(self):
        is_compact = self.width() < 1220
        desired_mode = QBoxLayout.TopToBottom if is_compact else QBoxLayout.LeftToRight
        if desired_mode == self._current_layout_mode:
            return

        self._current_layout_mode = desired_mode
        self.shell_layout.setDirection(desired_mode)

        if is_compact:
            self.hero_panel.setMinimumWidth(0)
            self.connect_panel.setMaximumWidth(16777215)
        else:
            self.hero_panel.setMinimumWidth(540)
            self.connect_panel.setMaximumWidth(520)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._sync_shell_layout()

    def _on_connect(self):
        exchange = self.exchange_box.currentText()
        broker_type = self.exchange_type_box.currentText()
        resolved = self._resolved_broker_inputs()
        api_key = resolved.get("api_key")
        secret = resolved.get("secret")
        password = resolved.get("password")
        account_id = resolved.get("account_id")

        if exchange != "paper" and broker_type != "paper" and not api_key:
            QMessageBox.warning(self, "Missing Credentials", "API credentials are required for this broker.")
            return
        if broker_type == "forex" and not account_id:
            QMessageBox.warning(self, "Missing Account ID",
                                "Account ID is required for Oanda sessions.")
            return

        broker_config = BrokerConfig(
            type=broker_type,
            exchange=exchange,
            mode=self.mode_box.currentText(),
            api_key=api_key,
            secret=secret,
            password=password or None,
            account_id=account_id or None,
        )

        config = AppConfig(
            broker=broker_config,
            risk=RiskConfig(risk_percent=self.risk_input.value()),
            system=SystemConfig(),
            strategy=self.strategy_box.currentText(),
        )

        if self.remember_checkbox.isChecked():
            profile_name = f"{exchange}_{api_key[:6] if api_key else 'paper'}"
            payload = config.model_dump() if hasattr(config, "model_dump") else config.dict()
            CredentialManager.save_account(profile_name, payload)
            self.settings.setValue(self.LAST_PROFILE_SETTING, profile_name)
            self._load_accounts_index()
            self.saved_account_box.setCurrentText(profile_name)

        self.show_loading()
        self.login_requested.emit(config)

    def show_loading(self):
        self.connect_button.setEnabled(False)
        self.connect_button.setText("Connecting Session...")
        self.spinner.setVisible(True)
        if self.spinner_movie is not None:
            self.spinner_movie.start()

    def hide_loading(self):
        self.spinner.setVisible(False)
        if self.spinner_movie is not None:
            self.spinner_movie.stop()
        self.connect_button.setEnabled(True)
        self._update_session_preview()
