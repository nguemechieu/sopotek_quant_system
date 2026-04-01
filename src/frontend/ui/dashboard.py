import re
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
    QInputDialog,
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

from broker.coinbase_credentials import coinbase_validation_error, normalize_coinbase_credentials
from config.config import AppConfig, BrokerConfig, RiskConfig, SystemConfig
from config.credential_manager import CredentialManager
from broker.market_venues import MARKET_VENUE_CHOICES, supported_market_venues_for_profile
from frontend.ui.i18n import apply_runtime_translations, iter_supported_languages


ROOT_DIR = Path(__file__).resolve().parents[2]
ASSETS_DIR = ROOT_DIR / "assets"

LOGO_PATH = ASSETS_DIR / "logo.png"
SPINNER_PATH = ASSETS_DIR / "spinner.gif"

CRYPTO_EXCHANGE_MAP = {
    "us": [
        "binanceus",
        "coinbase",
        "stellar",
        "kraken",
        "kucoin",
        "bybit",
        "okx",
        "gateio",
        "bitget",
    ],
    "global": [
        "binance",
        "coinbase",
        "stellar",
        "kraken",
        "kucoin",
        "bybit",
        "okx",
        "gateio",
        "bitget",
    ],
}

DERIVATIVE_EXCHANGE_MAP = {
    "options": ["schwab"],
    "futures": ["ibkr", "amp", "tradovate"],
    "derivatives": ["ibkr", "schwab", "amp", "tradovate"],
}

BROKER_TYPE_OPTIONS = [
    "crypto",
    "forex",
    "stocks",
    "options",
    "futures",
    "derivatives",
    "paper",
]

EXCHANGE_MAP = {
    "crypto": [],
    "forex": ["oanda"],
    "stocks": ["alpaca"],
    "options": DERIVATIVE_EXCHANGE_MAP["options"],
    "futures": DERIVATIVE_EXCHANGE_MAP["futures"],
    "derivatives": DERIVATIVE_EXCHANGE_MAP["derivatives"],
    "paper": ["paper"],
}

CUSTOMER_REGION_OPTIONS = [
    ("US", "us"),
    ("Outside US", "global"),
]

BROKER_COPY = {
    "crypto": "Multi-venue crypto routing with stronger session clarity before execution starts.",
    "forex": "Account-aware FX setup with the fields needed for a cleaner Oanda handoff.",
    "stocks": "Equity sessions tuned for Alpaca with a simpler launch path and saved profiles.",
    "options": "Contract-aware options sessions with a faster path into Greeks-aware execution workflows.",
    "futures": "Futures routing built for margin-aware workflows, rollover context, and contract metadata.",
    "derivatives": "A broader derivatives setup that keeps option and futures-capable broker paths visible in one place.",
    "paper": "A zero-risk rehearsal mode that still feels like the real desk experience.",
}

EXCHANGE_CREDENTIAL_SCHEMAS = {
    "default": {
        "api_label": "API Key",
        "api_placeholder": "Public key or broker token",
        "secret_label": "Secret",
        "secret_placeholder": "Secret key",
        "secret_echo": QLineEdit.Password,
        "password_label": "Passphrase",
        "password_placeholder": "Exchange passphrase when required",
        "password_echo": QLineEdit.Password,
        "account_label": "Account ID",
        "account_placeholder": "Optional account identifier",
        "show_password": False,
        "show_account": False,
        "required_fields": ("api", "secret"),
        "field_targets": {
            "api": "api_key",
            "secret": "secret",
            "password": "password",
            "account": "account_id",
        },
        "field_fallbacks": {
            "password": ("passphrase",),
        },
    },
    "stellar": {
        "api_label": "Public Key",
        "api_placeholder": "Stellar public key",
        "secret_label": "Private Key",
        "secret_placeholder": "Stellar private key",
        "required_fields": ("api",),
    },
    "coinbase": {
        "api_label": "Key Name or ID",
        "api_placeholder": "organizations/.../apiKeys/... or key id",
        "secret_label": "Private Key",
        "secret_placeholder": "Private key PEM or full Coinbase key JSON",
    },
    "oanda": {
        "api_label": "Account ID",
        "api_placeholder": "Oanda account ID",
        "secret_label": "API Key",
        "secret_placeholder": "Oanda API key",
        "secret_echo": QLineEdit.Normal,
        "required_fields": ("api", "secret"),
        "field_targets": {
            "api": "account_id",
            "secret": "api_key",
        },
    },
    "schwab": {
        "api_label": "Client ID",
        "api_placeholder": "Schwab client id",
        "secret_label": "Client Secret",
        "secret_placeholder": "Schwab client secret",
        "password_label": "Refresh Token",
        "password_placeholder": "Schwab refresh token",
        "password_echo": QLineEdit.Password,
        "account_label": "Account Hash",
        "account_placeholder": "Optional Schwab account hash or account number",
        "show_password": True,
        "show_account": True,
        "required_fields": ("api", "secret", "password"),
        "field_targets": {
            "api": "api_key",
            "secret": "secret",
            "password": "options.refresh_token",
            "account": "options.account_hash",
        },
        "field_fallbacks": {
            "password": ("password",),
            "account": ("account_id",),
        },
    },
    "amp": {
        "api_label": "Username",
        "api_placeholder": "AMP username",
        "secret_label": "Password",
        "secret_placeholder": "AMP password",
        "password_label": "API Key",
        "password_placeholder": "Optional AMP API key",
        "password_echo": QLineEdit.Normal,
        "account_label": "API Secret",
        "account_placeholder": "Optional AMP API secret",
        "show_password": True,
        "show_account": True,
        "required_fields": ("api", "secret"),
        "field_targets": {
            "api": "options.username",
            "secret": "password",
            "password": "api_key",
            "account": "secret",
        },
        "field_fallbacks": {
            "api": ("api_key",),
            "secret": ("password", "secret"),
            "password": ("api_key",),
            "account": ("secret", "account_id"),
        },
    },
    "tradovate": {
        "api_label": "Username",
        "api_placeholder": "Tradovate username",
        "secret_label": "Password",
        "secret_placeholder": "Tradovate password",
        "password_label": "Company ID",
        "password_placeholder": "Optional Tradovate company id",
        "password_echo": QLineEdit.Normal,
        "account_label": "Security Code",
        "account_placeholder": "Optional Tradovate security code",
        "show_password": True,
        "show_account": True,
        "required_fields": ("api", "secret"),
        "field_targets": {
            "api": "options.username",
            "secret": "password",
            "password": "api_key",
            "account": "secret",
        },
        "field_fallbacks": {
            "api": ("api_key",),
            "secret": ("password", "secret"),
            "password": ("api_key",),
            "account": ("secret", "account_id"),
        },
    },
    "ibkr": {
        "api_label": "Session Token",
        "api_placeholder": "Optional IBKR session token or client id",
        "secret_label": "Session Secret",
        "secret_placeholder": "Optional gateway secret",
        "account_label": "Account ID",
        "account_placeholder": "Optional IBKR account id override",
        "show_password": False,
        "show_account": True,
        "required_fields": (),
        "field_targets": {
            "api": "api_key",
            "secret": "secret",
            "password": "password",
            "account": "account_id",
        },
    },
}

"""The dashboard is the launchpad for trading sessions, where customers configure broker access and review session readiness before"""
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
        """Translate a key using controller translation support.

        Falls back to returning the key directly when no translation method exists.
        """
        if hasattr(self.controller, "tr"):
            return self.controller.tr(key, **kwargs)
        return key

    def _language_box_current_code(self):
        """Return currently selected language code from language dropdown."""
        if not hasattr(self, "language_box") or self.language_box is None:
            return None
        return self.language_box.currentData()

    def _selected_customer_region(self):
        """Return selected customer region for crypto routing."""
        if not hasattr(self, "customer_region_box") or self.customer_region_box is None:
            return "us"
        return str(self.customer_region_box.currentData() or "us").strip().lower()

    def _crypto_exchange_options_for_region(self):
        """Return available crypto exchange options based on selected region."""
        return list(CRYPTO_EXCHANGE_MAP.get(self._selected_customer_region(), CRYPTO_EXCHANGE_MAP["us"]))

    def _credential_field_schema(self):
        """Generate labels/placeholders and input modes for broker credential fields."""
        broker_type = self.exchange_type_box.currentText() if hasattr(self, "exchange_type_box") else ""
        exchange = self.exchange_box.currentText() if hasattr(self, "exchange_box") else ""

        schema = dict(EXCHANGE_CREDENTIAL_SCHEMAS["default"])
        schema.update(
            {
                "api_label": self._tr("dashboard.api_key"),
                "secret_label": self._tr("dashboard.secret"),
                "password_label": self._tr("dashboard.passphrase"),
                "account_label": self._tr("dashboard.account_id"),
            }
        )
        schema["field_targets"] = dict(schema.get("field_targets") or {})
        schema["field_fallbacks"] = dict(schema.get("field_fallbacks") or {})

        schema_key = None
        if exchange in EXCHANGE_CREDENTIAL_SCHEMAS:
            schema_key = exchange
        elif broker_type == "forex":
            schema_key = "oanda"

        if schema_key and schema_key != "default":
            override = EXCHANGE_CREDENTIAL_SCHEMAS[schema_key]
            merged_targets = dict(schema["field_targets"])
            merged_targets.update(dict(override.get("field_targets") or {}))
            merged_fallbacks = dict(schema["field_fallbacks"])
            merged_fallbacks.update(dict(override.get("field_fallbacks") or {}))
            schema.update(
                {
                    key: value
                    for key, value in override.items()
                    if key not in {"field_targets", "field_fallbacks"}
                }
            )
            schema["field_targets"] = merged_targets
            schema["field_fallbacks"] = merged_fallbacks

        if exchange in {"okx", "kucoin"}:
            schema["show_password"] = True

        return schema

    @staticmethod
    def _schema_target_paths(target):
        """Normalize a schema target entry into a tuple of target paths."""
        if not target:
            return ()
        if isinstance(target, (tuple, list)):
            return tuple(path for path in target if path)
        return (target,)

    @staticmethod
    def _mapping_value(mapping, path):
        """Read a dotted path from a nested dictionary structure."""
        current = mapping
        for part in str(path or "").split("."):
            if not isinstance(current, dict):
                return None
            current = current.get(part)
            if current is None:
                return None
        return current

    @staticmethod
    def _set_mapping_value(mapping, path, value):
        """Write a dotted path into a nested dictionary structure."""
        if value is None or value == "":
            return
        parts = str(path or "").split(".")
        current = mapping
        for part in parts[:-1]:
            next_value = current.get(part)
            if not isinstance(next_value, dict):
                next_value = {}
                current[part] = next_value
            current = next_value
        current[parts[-1]] = value

    def _dashboard_field_values(self, schema=None):
        """Return the current credential values keyed by raw dashboard field names."""
        schema = schema or self._credential_field_schema()
        is_paper = self.exchange_type_box.currentText() == "paper" or self.exchange_box.currentText() == "paper"
        return {
            "api": "" if is_paper else self.api_input.text().strip(),
            "secret": "" if is_paper else self.secret_input.text().strip(),
            "password": (
                ""
                if is_paper or not schema.get("show_password")
                else self.password_input.text().strip()
            ),
            "account": (
                ""
                if is_paper or not schema.get("show_account")
                else self.account_id_input.text().strip()
            ),
        }

    @staticmethod
    def _schema_label_key(field_name):
        """Return the schema key that stores a raw field's visible label."""
        return {
            "api": "api_label",
            "secret": "secret_label",
            "password": "password_label",
            "account": "account_label",
        }[field_name]

    def _schema_field_has_value(self, schema, resolved, field_name):
        """Return whether a required dashboard field resolved into broker config data."""
        for path in self._schema_target_paths((schema.get("field_targets") or {}).get(field_name)):
            value = self._mapping_value(resolved, path)
            if str(value or "").strip():
                return True
        return False

    def _schema_field_value_from_broker(self, schema, broker, field_name):
        """Read a saved broker value back into the matching dashboard field."""
        candidate_paths = []
        candidate_paths.extend(
            self._schema_target_paths((schema.get("field_targets") or {}).get(field_name))
        )
        candidate_paths.extend(
            self._schema_target_paths((schema.get("field_fallbacks") or {}).get(field_name))
        )
        for path in candidate_paths:
            value = self._mapping_value(broker, path)
            text = str(value or "").strip()
            if text:
                return text
        return ""

    def _populate_credential_fields(self, broker, schema=None):
        """Populate the dashboard credential inputs from a saved broker payload."""
        schema = schema or self._credential_field_schema()
        self.api_input.setText(self._schema_field_value_from_broker(schema, broker, "api"))
        self.secret_input.setText(self._schema_field_value_from_broker(schema, broker, "secret"))
        self.password_input.setText(self._schema_field_value_from_broker(schema, broker, "password"))
        self.account_id_input.setText(self._schema_field_value_from_broker(schema, broker, "account"))

    def _apply_credential_field_schema(self):
        """Apply credential field schema to the current UI elements."""
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

        password_block = self._field_blocks.get("password")
        if password_block is not None:
            password_block.label_widget.setText(schema["password_label"])
        self.password_input.setPlaceholderText(schema["password_placeholder"])
        self.password_input.setEchoMode(schema["password_echo"])

        account_block = self._field_blocks.get("account_id")
        if account_block is not None:
            account_block.label_widget.setText(schema["account_label"])
        self.account_id_input.setPlaceholderText(schema["account_placeholder"])

    def _resolved_broker_inputs(self):
        """Return normalized broker credential values from UI inputs."""
        exchange = self.exchange_box.currentText()
        schema = self._credential_field_schema()
        field_values = self._dashboard_field_values(schema)

        if exchange == "coinbase":
            field_values["api"], field_values["secret"], field_values["password"] = normalize_coinbase_credentials(
                field_values["api"],
                field_values["secret"],
                field_values["password"],
            )

        resolved = {
            "api_key": None,
            "secret": None,
            "password": None,
            "account_id": None,
            "options": {},
        }
        for field_name, value in field_values.items():
            if not value:
                continue
            for path in self._schema_target_paths((schema.get("field_targets") or {}).get(field_name)):
                self._set_mapping_value(resolved, path, value)

        return resolved

    @staticmethod
    def _strip_wrapped_quotes(value):
        """Remove surrounding single or double quotes from a string value."""
        text = str(value or "").strip()
        if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
            return text[1:-1].strip()
        return text

    @classmethod
    def _coinbase_validation_error(cls, api_key, secret, password=None):
        """Validate Coinbase credentials and return an error message if invalid."""
        return coinbase_validation_error(api_key, secret, password=password)

    def _build_ui(self):
        """Build the dashboard UI structure and layout."""
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
        """Build and return the dashboard hero status overview panel."""
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
            "Configure broker access and risk profile before launching the trading terminal. "
            "Strategy assignment happens automatically per symbol after launch, with terminal overrides available when needed."
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
        pills_row.addWidget(self._create_stat_pill("License", "Trial", value_attr="license_pill_value"))
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
            "Use the dashboard like a pre-flight panel: confirm broker type, credentials, and risk posture before the terminal takes over."
        )
        self.market_body_label.setObjectName("heroSectionBody")
        self.market_body_label.setWordWrap(True)
        market_layout.addWidget(self.market_body_label)

        self.market_primary = self._create_market_strip("Primary Venue", "Venue routing stays aligned with customer region and launch mode.")
        self.market_secondary = self._create_market_strip("Strategy Routing", "Per-symbol strategy ranking starts after launch, and terminal overrides stay available.")
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
        self.check_strategy = self._create_checklist_row("Strategy routing", "Automatic")
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
        """Build and return the broker connection configuration panel."""
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
        self.exchange_type_box.addItems(BROKER_TYPE_OPTIONS)
        self.exchange_box = QComboBox()
        market_row.addWidget(self._wrap_field("Broker Type", self.exchange_type_box), 1)
        market_row.addWidget(self._wrap_field("Exchange", self.exchange_box), 1)
        panel_layout.addLayout(market_row)

        jurisdiction_row = QHBoxLayout()
        jurisdiction_row.setSpacing(10)
        self.customer_region_box = QComboBox()
        for label, value in CUSTOMER_REGION_OPTIONS:
            self.customer_region_box.addItem(label, value)
        saved_region = str(self.settings.value("dashboard/customer_region", "us") or "us").strip().lower()
        region_index = self.customer_region_box.findData(saved_region)
        self.customer_region_box.setCurrentIndex(region_index if region_index >= 0 else 0)
        jurisdiction_row.addWidget(self._wrap_field("Customer Region", self.customer_region_box, block_name="customer_region"), 1)
        jurisdiction_row.addStretch(1)
        panel_layout.addLayout(jurisdiction_row)

        strategy_row = QHBoxLayout()
        strategy_row.setSpacing(10)
        self.mode_box = QComboBox()
        self.mode_box.addItems(["live", "paper"])
        self.market_type_box = QComboBox()
        for label, value in MARKET_VENUE_CHOICES:
            self.market_type_box.addItem(label, value)
        strategy_row.addWidget(self._wrap_field("Mode", self.mode_box), 1)
        strategy_row.addWidget(self._wrap_field("Venue", self.market_type_box), 1)
        strategy_row.addStretch(1)
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

        self.summary_meta = QLabel("Risk 2%  |  Strategy Auto per symbol  |  Profile not saved yet")
        self.summary_meta.setObjectName("summaryMeta")
        self.summary_meta.setWordWrap(True)
        summary_layout.addWidget(self.summary_meta)

        self.live_guard_checkbox = QCheckBox("I understand this session can place live orders.")
        self.live_guard_checkbox.setVisible(False)
        summary_layout.addWidget(self.live_guard_checkbox)

        license_row = QHBoxLayout()
        license_row.setSpacing(8)
        self.license_status_label = QLabel("License: Trial active")
        self.license_status_label.setObjectName("hintLabel")
        self.license_status_label.setWordWrap(True)
        license_row.addWidget(self.license_status_label, 1)
        self.manage_license_button = QPushButton("Manage License")
        license_row.addWidget(self.manage_license_button)
        summary_layout.addLayout(license_row)

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
            "Start in paper mode when testing a new broker path. "
            "Per-symbol strategy assignment happens after launch, and the terminal can still override it when needed."
        )
        self.footer_label.setObjectName("hintLabel")
        self.footer_label.setWordWrap(True)
        panel_layout.addWidget(self.footer_label)

        panel_layout.addStretch(1)
        return panel

    def _create_stat_pill(self, label, value, value_attr=None):
        """Create a reusable stat pill widget for status display."""
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
        """Create a reusable market status strip with title and description."""
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
        """Create a checklist row widget for launch status indicators."""
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
        """Create a styled preset button."""
        button = QPushButton(text)
        button.setObjectName("presetButton")
        button.setCursor(Qt.PointingHandCursor)
        return button

    def _wrap_field(self, label_text, widget, block_name=None):
        """Wrap a field input widget with a label and optional field block registration."""
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
        """Wire UI events to their corresponding handlers."""
        self.exchange_type_box.currentTextChanged.connect(self._update_exchange_list)
        self.exchange_type_box.currentTextChanged.connect(self._update_optional_fields)
        self.exchange_type_box.currentTextChanged.connect(self._update_broker_hint)
        self.exchange_type_box.currentTextChanged.connect(self._update_session_preview)
        self.customer_region_box.currentIndexChanged.connect(self._handle_customer_region_changed)
        self.exchange_box.currentTextChanged.connect(self._update_optional_fields)
        self.exchange_box.currentTextChanged.connect(self._update_broker_hint)
        self.exchange_box.currentTextChanged.connect(self._update_session_preview)
        self.mode_box.currentTextChanged.connect(self._update_broker_hint)
        self.mode_box.currentTextChanged.connect(self._update_session_preview)
        self.market_type_box.currentIndexChanged.connect(self._update_session_preview)
        self.risk_input.valueChanged.connect(self._update_session_preview)
        self.api_input.textChanged.connect(self._update_session_preview)
        self.secret_input.textChanged.connect(self._update_session_preview)
        self.password_input.textChanged.connect(self._update_session_preview)
        self.account_id_input.textChanged.connect(self._update_session_preview)
        self.remember_checkbox.toggled.connect(self._update_session_preview)
        self.live_guard_checkbox.toggled.connect(self._update_session_preview)
        self.saved_account_box.currentTextChanged.connect(self._load_selected_account)
        self.refresh_accounts_button.clicked.connect(self._load_accounts_index)
        self.language_box.currentIndexChanged.connect(self._on_language_changed)
        self.paper_preset_button.clicked.connect(lambda: self._apply_preset("paper"))
        self.crypto_preset_button.clicked.connect(lambda: self._apply_preset("crypto"))
        self.fx_preset_button.clicked.connect(lambda: self._apply_preset("forex"))
        self.connect_button.clicked.connect(self._on_connect)
        self.manage_license_button.clicked.connect(
            lambda: getattr(self.controller, "show_license_dialog", lambda *_: None)(self)
        )
        if hasattr(self.controller, "license_changed"):
            self.controller.license_changed.connect(lambda _status: self._update_session_preview())

    def _on_language_changed(self, _index):
        """Handle language selection changes from the UI."""
        language_code = self._language_box_current_code()
        if not language_code or not hasattr(self.controller, "set_language"):
            return
        self.controller.set_language(language_code)

    def apply_language(self):
        """Apply translations to all UI text elements based on selected language."""
        previous_language = getattr(self, "_applied_language_code", None)
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
            "customer_region": None,
            "api": "dashboard.api_key",
            "secret": "dashboard.secret",
            "password": "dashboard.passphrase",
            "account_id": "dashboard.account_id",
        }
        for block_name, key in field_labels.items():
            block = self._field_blocks.get(block_name)
            if block is not None:
                block.label_widget.setText(self._tr(key) if key else "Customer Region")

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

        market_type_block = self.market_type_box.parentWidget()
        if market_type_block is not None and hasattr(market_type_block, "label_widget"):
            market_type_block.label_widget.setText("Venue")

        risk_block = self.risk_input.parentWidget()
        if risk_block is not None and hasattr(risk_block, "label_widget"):
            risk_block.label_widget.setText(self._tr("dashboard.risk_budget"))

        self._update_session_preview()
        apply_runtime_translations(
            self,
            getattr(self.controller, "language_code", "en"),
            previous_language=previous_language,
        )
        self._applied_language_code = getattr(self.controller, "language_code", "en")

    def _load_accounts_index(self):
        """Load available saved broker profiles into the profile combo box."""
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
        """Load a saved account settings profile and populate form fields."""
        if not account_name or self.saved_account_box.currentData() == "__recent__":
            return

        creds = CredentialManager.load_account(account_name)
        if not creds:
            return

        CredentialManager.touch_account(account_name)
        self.settings.setValue(self.LAST_PROFILE_SETTING, account_name)

        broker = creds.get("broker", {})
        self.exchange_type_box.setCurrentText(broker.get("type", "crypto"))
        exchange_value = str(broker.get("exchange", "") or "").strip().lower()
        default_region = "global" if exchange_value == "binance" else "us"
        region_value = str(
            broker.get("customer_region")
            or (broker.get("options", {}) or {}).get("customer_region")
            or default_region
        ).strip().lower()
        region_index = self.customer_region_box.findData(region_value)
        self.customer_region_box.setCurrentIndex(region_index if region_index >= 0 else 0)
        self._update_exchange_list(broker.get("type", "crypto"))
        self.exchange_box.setCurrentText(broker.get("exchange", ""))
        self._update_optional_fields()
        self._populate_credential_fields(broker)
        self._refresh_market_type_options()
        self.mode_box.setCurrentText(broker.get("mode", "paper"))
        market_type_index = self.market_type_box.findData((broker.get("options", {}) or {}).get("market_type", "auto"))
        self.market_type_box.setCurrentIndex(market_type_index if market_type_index >= 0 else 0)
        self.risk_input.setValue(int(creds.get("risk", {}).get("risk_percent", 2) or 2))

        self._update_optional_fields()
        self._update_broker_hint()
        self._update_session_preview()

    def _load_last_account(self):
        """Load last used profile at startup if available."""
        accounts = CredentialManager.list_accounts()
        if not accounts:
            return
        saved_last = str(self.settings.value(self.LAST_PROFILE_SETTING, "") or "").strip()
        target = saved_last if saved_last in accounts else accounts[0]
        self.saved_account_box.setCurrentText(target)
        self._load_selected_account(target)

    def _apply_preset(self, preset_name):
        """Apply a preset configuration for quick session setup."""
        if preset_name == "paper":
            self.exchange_type_box.setCurrentText("paper")
            self._update_exchange_list("paper")
            self.exchange_box.setCurrentText("paper")
            self.mode_box.setCurrentText("paper")
            self._refresh_market_type_options()
            auto_index = self.market_type_box.findData("auto")
            self.market_type_box.setCurrentIndex(auto_index if auto_index >= 0 else 0)
            self.risk_input.setValue(2)
        elif preset_name == "crypto":
            self.exchange_type_box.setCurrentText("crypto")
            region_index = self.customer_region_box.findData("us")
            if region_index >= 0:
                self.customer_region_box.setCurrentIndex(region_index)
            self._update_exchange_list("crypto")
            if self.exchange_box.findText("binanceus") >= 0:
                self.exchange_box.setCurrentText("binanceus")
            self.mode_box.setCurrentText("live")
            self._refresh_market_type_options()
            spot_index = self.market_type_box.findData("spot")
            self.market_type_box.setCurrentIndex(spot_index if spot_index >= 0 else 0)
            self.risk_input.setValue(2)
        elif preset_name == "forex":
            self.exchange_type_box.setCurrentText("forex")
            self._update_exchange_list("forex")
            self.exchange_box.setCurrentText("oanda")
            self.mode_box.setCurrentText("live")
            self._refresh_market_type_options()
            otc_index = self.market_type_box.findData("otc")
            self.market_type_box.setCurrentIndex(otc_index if otc_index >= 0 else 0)
            self.risk_input.setValue(1)

        self._update_optional_fields()
        self._update_broker_hint()
        self._update_session_preview()

    def _handle_customer_region_changed(self):
        """Handle changes to customer region and update dependent fields."""
        region = self._selected_customer_region()
        self.settings.setValue("dashboard/customer_region", region)
        if self.exchange_type_box.currentText() == "crypto":
            self._update_exchange_list("crypto")
        self._update_optional_fields()
        self._update_broker_hint()
        self._update_session_preview()

    def _update_exchange_list(self, exchange_type):
        """Update the exchange dropdown list when broker type changes."""
        current = self.exchange_box.currentText()
        if exchange_type == "crypto":
            exchanges = self._crypto_exchange_options_for_region()
        else:
            exchanges = EXCHANGE_MAP.get(exchange_type, [])

        self.exchange_box.blockSignals(True)
        self.exchange_box.clear()
        self.exchange_box.addItems(exchanges)
        if current in exchanges:
            self.exchange_box.setCurrentText(current)
        elif exchanges:
            self.exchange_box.setCurrentIndex(0)
        self.exchange_box.blockSignals(False)

    def _refresh_market_type_options(self):
        """Refresh the market-type options to only include supported venues for the selected profile."""
        current = str(self.market_type_box.currentData() or "auto").strip().lower() or "auto"
        supported = supported_market_venues_for_profile(
            self.exchange_type_box.currentText(),
            self.exchange_box.currentText(),
        )

        self.market_type_box.blockSignals(True)
        self.market_type_box.clear()
        for label, value in MARKET_VENUE_CHOICES:
            if value in supported:
                self.market_type_box.addItem(label, value)

        target = current if current in supported else ("auto" if "auto" in supported else supported[0])
        index = self.market_type_box.findData(target)
        self.market_type_box.setCurrentIndex(index if index >= 0 else 0)
        self.market_type_box.blockSignals(False)

    def _update_optional_fields(self):
        """Show/hide broker-specific form fields based on selected exchange type."""
        broker_type = self.exchange_type_box.currentText()
        exchange = self.exchange_box.currentText()
        schema = self._credential_field_schema()
        is_paper = broker_type == "paper" or exchange == "paper"
        self._refresh_market_type_options()

        self._field_blocks["api"].setVisible(not is_paper)
        self._field_blocks["secret"].setVisible(not is_paper)
        self._field_blocks["account_id"].setVisible((not is_paper) and bool(schema.get("show_account")))
        self._field_blocks["password"].setVisible((not is_paper) and bool(schema.get("show_password")))
        self._field_blocks["customer_region"].setVisible(broker_type == "crypto" and not is_paper)

        if is_paper:
            self.mode_box.blockSignals(True)
            self.mode_box.setCurrentText("paper")
            self.mode_box.blockSignals(False)
            self.mode_box.setEnabled(False)
        else:
            self.mode_box.setEnabled(True)

        venue_enabled = self.market_type_box.count() > 1
        self.market_type_box.setEnabled(venue_enabled)
        if not venue_enabled:
            self.market_type_box.blockSignals(True)
            self.market_type_box.setCurrentIndex(0)
            self.market_type_box.blockSignals(False)

        self._apply_credential_field_schema()

    def _update_broker_hint(self):
        """Update broker hint text describing selected broker/mode constraints."""
        broker_type = self.exchange_type_box.currentText()
        exchange = self.exchange_box.currentText()
        mode = self.mode_box.currentText()
        market_type = self.market_type_box.currentData()
        customer_region = self._selected_customer_region()

        copy = BROKER_COPY.get(broker_type, "Configure a broker session and launch the terminal.")
        if exchange:
            copy = f"{exchange.upper()} in {mode.upper()} mode. {copy}"
        if exchange and exchange != "paper":
            copy += f" Trading venue preference: {str(market_type or 'auto').upper()}."
        if broker_type == "crypto" and exchange and exchange != "paper":
            if exchange == "binanceus":
                copy += " Binance US is reserved for US customers."
            elif exchange == "binance":
                copy += " Binance.com is for customers outside the US."
            elif exchange == "coinbase":
                copy += (
                    " For Coinbase Advanced Trade, paste the API key name in the first field "
                    "and the privateKey value in the second field."
                )
            copy += f" Customer region: {customer_region.upper()}."
        if exchange == "stellar":
            copy += (
                " Use your Stellar public key in the first field. "
                "The private key is optional for read-only market data, but required for order execution."
            )
        elif exchange == "oanda" or broker_type == "forex":
            copy += " Enter Oanda account ID in the first field and API key in the second field."
        elif exchange == "schwab":
            copy += (
                " Enter the Schwab client ID, client secret, and refresh token. "
                "The last field can pin a specific account hash when you do not want auto-resolution."
            )
        elif exchange == "amp":
            copy += (
                " Enter the AMP username and password first. "
                "The extra fields can carry optional AMP API credentials when your endpoint expects them."
            )
        elif exchange == "tradovate":
            copy += (
                " Enter the Tradovate username and password first. "
                "Company ID and security code are only needed on environments that require them."
            )
        elif exchange == "ibkr":
            copy += (
                " IBKR can auto-resolve the account through Client Portal Gateway or TWS. "
                "Use the account field only when you want to force a specific account id."
            )
        self.broker_hint.setText(copy)

    def _set_check_state(self, row, text, is_ready):
        """Set the state label and style of a checklist row."""
        row.state_label.setText(text)
        row.state_label.setObjectName("checkStateGood" if is_ready else "checkStateWarn")
        row.state_label.style().unpolish(row.state_label)
        row.state_label.style().polish(row.state_label)

    def _update_session_preview(self):
        """Update the dashboard session preview and readiness indicators."""
        broker_type = self.exchange_type_box.currentText()
        exchange = self.exchange_box.currentText() or "paper"
        customer_region = self._selected_customer_region()
        mode = self.mode_box.currentText()
        schema = self._credential_field_schema()
        market_type = str(self.market_type_box.currentData() or "auto").upper()
        strategy = "Auto per-symbol assignment"
        risk_value = self.risk_input.value()

        is_paper = broker_type == "paper" or exchange == "paper" or mode == "paper"
        needs_credentials = exchange != "paper" and broker_type != "paper"

        resolved = self._resolved_broker_inputs()
        required_fields = tuple(schema.get("required_fields") or ())
        credentials_ready = not needs_credentials or all(
            self._schema_field_has_value(schema, resolved, field_name)
            for field_name in required_fields
        )
        has_optional_account = bool(self._dashboard_field_values(schema).get("account"))

        readiness = 20
        readiness += 20 if exchange else 0
        readiness += 15 if mode else 0
        readiness += 15 if risk_value <= 3 else 8
        readiness += 20 if credentials_ready else 0
        readiness += 10 if (not schema.get("show_account") or has_optional_account) else 0
        readiness = max(0, min(100, readiness))

        session_label = "Paper" if is_paper else "Live"
        market_reach = {
            "crypto": "Crypto Desk",
            "forex": "FX Desk",
            "stocks": "Equity Desk",
            "options": "Options Desk",
            "futures": "Futures Desk",
            "derivatives": "Derivatives Desk",
            "paper": "Paper Desk",
        }.get(broker_type, "Multi-Asset")

        self.session_pill_value.setText(session_label)
        self.readiness_pill_value.setText(f"{readiness}%")
        self.market_pill_value.setText(market_reach)
        license_status = {}
        if hasattr(self.controller, "get_license_status"):
            try:
                license_status = self.controller.get_license_status()
            except Exception:
                license_status = {}
        self.license_pill_value.setText(str(license_status.get("badge", "FREE") or "FREE"))
        license_plan = str(license_status.get("plan_name", "License") or "License")
        license_summary = str(license_status.get("summary", "Status unavailable") or "Status unavailable")
        self.license_status_label.setText(f"{license_plan}: {license_summary}")

        venue_label = exchange.upper() if exchange else "PAPER"
        strategy_copy = "The system ranks strategies per symbol after launch, and terminal users can still override assignments when needed."
        mode_copy = "paper rehearsal" if is_paper else "live execution"

        self.market_primary.body_label.setText(
            f"{venue_label} selected with {mode_copy} framing and {customer_region.upper()} customer routing."
        )
        self.market_secondary.body_label.setText(f"{strategy}. {strategy_copy}")
        self.market_tertiary.body_label.setText(
            f"Risk budget is {risk_value}%. Venue {market_type}. Profiles are {'saved' if self.remember_checkbox.isChecked() else 'temporary'} for this session."
        )

        broker_ready = bool(exchange)
        strategy_ready = True
        risk_ready = risk_value <= 3

        self._set_check_state(self.check_credentials, "Ready" if credentials_ready else "Needs input", credentials_ready)
        self._set_check_state(self.check_broker, "Ready" if broker_ready else "Choose venue", broker_ready)
        self._set_check_state(self.check_strategy, "Auto or terminal-managed", strategy_ready)
        self._set_check_state(self.check_risk, "Conservative" if risk_ready else "Aggressive", risk_ready)

        if is_paper:
            self.summary_title.setText("Paper desk ready")
            self.summary_body.setText(
                "This setup is optimized for a safer rehearsal. You can validate the broker shape, charts, and automatic strategy routing before taking live risk."
            )
            self.connect_button.setText("Open Paper Terminal")
            self.live_guard_checkbox.setVisible(False)
            self.live_guard_checkbox.setChecked(False)
        else:
            self.summary_title.setText(f"{venue_label} live launch")
            self.summary_body.setText(
                "This session is configured for live execution. Review credentials, account details, and risk posture before entering the terminal. Strategy assignment will happen automatically per symbol after launch."
            )
            if hasattr(self.controller, "license_allows") and not self.controller.license_allows("live_trading"):
                self.connect_button.setText("Activate License For Live Trading")
            else:
                self.connect_button.setText("Launch Live Trading Terminal")
            self.live_guard_checkbox.setVisible(True)
            self.live_guard_checkbox.setText(
                f"I understand {venue_label} can place live orders on this account."
            )

        profile_state = self.saved_account_box.currentText()
        profile_copy = profile_state if profile_state and profile_state != "Recent profiles" else "Profile not saved yet"
        self.summary_meta.setText(f"Risk {risk_value}%  |  Strategy Auto per symbol  |  {profile_copy}")

    def _confirm_live_launch(self, exchange, account_id):
        """Request explicit user confirmation before allowing live trading launch."""
        confirmation = QMessageBox.question(
            self,
            "Confirm Live Trading",
            (
                f"You are about to open a LIVE trading session for {str(exchange or '').upper() or 'the selected broker'}.\n\n"
                f"Account: {account_id or 'Not set'}\n"
                "Mode: LIVE\n\n"
                "Use paper mode whenever you are testing a new strategy or broker path.\n"
                "Continue only if you intend to allow real orders."
            ),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if confirmation != QMessageBox.StandardButton.Yes:
            return False

        typed, ok = QInputDialog.getText(
            self,
            "Type LIVE To Continue",
            "Type LIVE to confirm this real-money session:",
        )
        return bool(ok and str(typed).strip().upper() == "LIVE")

    def _sync_shell_layout(self):
        """Switch layout orientation based on window width for responsive behavior."""
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
        """Handle resize events to reflow layout responsively."""
        super().resizeEvent(event)
        self._sync_shell_layout()

    def _on_connect(self):
        """Validate input and emit login request when connecting the session."""
        exchange = self.exchange_box.currentText()
        broker_type = self.exchange_type_box.currentText()
        customer_region = self._selected_customer_region()
        schema = self._credential_field_schema()
        resolved = self._resolved_broker_inputs()
        api_key = resolved.get("api_key")
        secret = resolved.get("secret")
        password = resolved.get("password")
        account_id = resolved.get("account_id")

        missing_fields = []
        if exchange != "paper" and broker_type != "paper":
            for field_name in tuple(schema.get("required_fields") or ()):
                if not self._schema_field_has_value(schema, resolved, field_name):
                    missing_fields.append(field_name)
        if missing_fields:
            missing_label = schema.get(self._schema_label_key(missing_fields[0]), "Credential")
            QMessageBox.warning(
                self,
                "Missing Credentials",
                f"{missing_label} is required for this broker session.",
            )
            return
        if exchange in {"binance", "binanceus"} and api_key and any(ch.isspace() for ch in api_key):
            QMessageBox.warning(
                self,
                "Invalid API Key",
                "The Binance API key contains spaces or line breaks. Paste the key exactly as issued by the exchange.",
            )
            return
        if exchange in {"binance", "binanceus"} and secret and any(ch.isspace() for ch in secret):
            QMessageBox.warning(
                self,
                "Invalid Secret",
                "The Binance secret contains spaces or line breaks. Paste the secret exactly as issued by the exchange.",
            )
            return
        if exchange == "coinbase":
            coinbase_error = self._coinbase_validation_error(api_key, secret, password=password)
            if coinbase_error:
                QMessageBox.warning(
                    self,
                    "Invalid Coinbase Credentials",
                    coinbase_error,
                )
                return
        if broker_type == "crypto" and exchange == "binance" and customer_region == "us":
            QMessageBox.warning(
                self,
                "Binance Jurisdiction",
                "Binance.com is not available for US customers. Switch the customer region to Outside US or use Binance US.",
            )
            return
        if broker_type == "crypto" and exchange == "binanceus" and customer_region != "us":
            QMessageBox.warning(
                self,
                "Binance US Jurisdiction",
                "Binance US is only available for US customers. Switch the customer region to US or choose Binance for non-US customers.",
            )
            return
        if self.mode_box.currentText() == "live" and exchange != "paper" and broker_type != "paper":
            if hasattr(self.controller, "license_allows") and not self.controller.license_allows("live_trading"):
                QMessageBox.information(
                    self,
                    "License Required",
                    "Live trading requires an active Trial, Subscription, or Full License. The license window will open now.",
                )
                if hasattr(self.controller, "show_license_dialog"):
                    self.controller.show_license_dialog(self)
                return
        if self.mode_box.currentText() == "live" and exchange != "paper" and broker_type != "paper":
            if not self.live_guard_checkbox.isChecked():
                QMessageBox.warning(
                    self,
                    "Live Safety Check",
                    "Tick the live-order acknowledgement before launching a live session.",
                )
                return
            if not self._confirm_live_launch(exchange, account_id):
                QMessageBox.information(
                    self,
                    "Live Session Canceled",
                    "Live launch canceled. The terminal was not opened.",
                )
                return

        broker_options = dict(resolved.get("options") or {})
        broker_options.update(
            {
                "market_type": str(self.market_type_box.currentData() or "auto"),
                "customer_region": customer_region,
                "candle_price_component": str(
                    getattr(self.controller, "forex_candle_price_component", "bid") or "bid"
                ).strip().lower(),
            }
        )
        broker_config = BrokerConfig(
            type=broker_type,
            exchange=exchange,
            customer_region=customer_region,
            mode=self.mode_box.currentText(),
            api_key=api_key,
            secret=secret,
            password=password or None,
            account_id=account_id or None,
            options=broker_options,
        )

        config = AppConfig(
            broker=broker_config,
            risk=RiskConfig(risk_percent=self.risk_input.value()),
            system=SystemConfig(),
            strategy=str(getattr(self.controller, "strategy_name", "Trend Following") or "Trend Following"),
        )

        if self.remember_checkbox.isChecked():
            profile_seed = "paper"
            for candidate in (
                broker_options.get("username"),
                api_key,
                account_id,
                broker_options.get("account_hash"),
            ):
                candidate_text = str(candidate or "").strip()
                if candidate_text:
                    profile_seed = candidate_text
                    break
            profile_name = f"{exchange}_{profile_seed[:6]}"
            payload = config.model_dump() if hasattr(config, "model_dump") else config.dict()
            CredentialManager.save_account(profile_name, payload)
            self.settings.setValue(self.LAST_PROFILE_SETTING, profile_name)
            self._load_accounts_index()
            self.saved_account_box.setCurrentText(profile_name)

        self.show_loading()
        self.login_requested.emit(config)

    def show_loading(self):
        """Show loading status while connecting to the trading terminal."""
        self.connect_button.setEnabled(False)
        self.connect_button.setText("Connecting Session...")
        self.spinner.setVisible(True)
        if self.spinner_movie is not None:
            self.spinner_movie.start()

    def hide_loading(self):
        """Hide loading status and restore the connect button state."""
        self.spinner.setVisible(False)
        if self.spinner_movie is not None:
            self.spinner_movie.stop()
        self.connect_button.setEnabled(True)
        self._update_session_preview()
