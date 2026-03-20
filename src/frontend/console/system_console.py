import logging
from datetime import datetime

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QTextEdit,
    QPushButton,
    QHBoxLayout
)

from PySide6.QtCore import Signal, Qt


class SystemConsole(QWidget):

    log_signal = Signal(str)
    screenshot_requested = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle("System Console")

        self.layout = QVBoxLayout()

        # Console output
        self.console = QTextEdit()
        self.console.setReadOnly(True)
        self.console.setStyleSheet("""
            background-color: black;
            color: #00ff90;
            font-family: Consolas;
            font-size: 11pt;
        """)

        self.layout.addWidget(self.console)

        # Buttons
        btn_layout = QHBoxLayout()

        self.clear_button = QPushButton("Clear")
        self.save_button = QPushButton("Save Logs")
        self.screenshot_button = QPushButton("Screenshot")

        btn_layout.addWidget(self.clear_button)
        btn_layout.addWidget(self.save_button)
        btn_layout.addWidget(self.screenshot_button)

        self.layout.addLayout(btn_layout)

        self.setLayout(self.layout)

        # Signals
        self.log_signal.connect(self.write_log)

        # Button actions
        self.clear_button.clicked.connect(self.clear_console)
        self.save_button.clicked.connect(self.save_logs)
        self.screenshot_button.clicked.connect(self.screenshot_requested.emit)

    # ------------------------------------------------
    # Write log to console
    # ------------------------------------------------

    def write_log(self, message):

        timestamp = datetime.now().strftime("%H:%M:%S")

        log_line = f"[{timestamp}] {message}"

        self.console.append(log_line)

    # ------------------------------------------------
    # Clear console
    # ------------------------------------------------

    def clear_console(self):

        self.console.clear()

    # ------------------------------------------------
    # Save logs
    # ------------------------------------------------

    def save_logs(self):

        with open("logs/system_console.log", "a") as f:

            f.write(self.console.toPlainText())

        self.log_signal.emit("Logs saved")

    # ------------------------------------------------
    # External logging
    # ------------------------------------------------

    def log(self, message, level=None):

        if level:
            self.log_signal.emit(f"[{level}] {message}")
            return

        self.log_signal.emit(message)
