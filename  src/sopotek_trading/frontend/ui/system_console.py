from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QTextEdit,
    QPushButton, QHBoxLayout, QLabel, QComboBox
)
from PySide6.QtCore import  Signal
from PySide6.QtGui import QTextCursor, QColor
import datetime


class SystemConsole(QWidget):

    log_signal = Signal(str, str)
    # (message, level)

    def __init__(self):
        super().__init__()

        self.setWindowTitle("System Console")

        self.layout = QVBoxLayout(self)

        # --- Controls ---
        control_layout = QHBoxLayout()

        self.clear_button = QPushButton("Clear")
        self.level_filter = QComboBox()
        self.level_filter.addItems(
            ["ALL", "INFO", "WARNING", "ERROR"]
        )

        control_layout.addWidget(QLabel("Filter:"))
        control_layout.addWidget(self.level_filter)
        control_layout.addStretch()
        control_layout.addWidget(self.clear_button)

        # --- Log View ---
        self.console = QTextEdit()
        self.console.setReadOnly(True)
        self.console.setStyleSheet(
            "background-color: black; color: white;"
        )

        self.layout.addLayout(control_layout)
        self.layout.addWidget(self.console)

        # --- Signals ---
        self.clear_button.clicked.connect(self.console.clear)
        self.log_signal.connect(self._append_log)

    # --------------------------------------------------
    # PUBLIC METHOD TO ADD LOG
    # --------------------------------------------------

    def log(self, message: str, level: str = "INFO"):
        self.log_signal.emit(message, level)

    # --------------------------------------------------
    # INTERNAL APPEND
    # --------------------------------------------------

    def _append_log(self, message: str, level: str):

        selected_filter = self.level_filter.currentText()

        if selected_filter != "ALL" and selected_filter != level:
            return

        timestamp = datetime.datetime.now().strftime("%H:%M:%S")

        color = self._get_color(level)

        formatted = f"[{timestamp}] [{level}] {message}"

        self.console.setTextColor(color)
        self.console.append(formatted)

        # Auto-scroll
        self.console.moveCursor(QTextCursor.End)

    # --------------------------------------------------
    # COLOR MAPPING
    # --------------------------------------------------

    def _get_color(self, level):

        if level == "INFO":
            return QColor("#00FF00")   # Green
        elif level == "WARNING":
            return QColor("#FFA500")   # Orange
        elif level == "ERROR":
            return QColor("#FF0000")   # Red
        else:
            return QColor("#FFFFFF")   # White