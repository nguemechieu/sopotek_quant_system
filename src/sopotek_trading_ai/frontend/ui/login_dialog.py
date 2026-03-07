from PySide6.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton
)


class LoginDialog(QDialog):

    def __init__(self):

        super().__init__()

        self.setWindowTitle("Broker Login")

        layout = QVBoxLayout()

        self.api_key = QLineEdit()
        self.api_key.setPlaceholderText("API Key")

        self.secret = QLineEdit()
        self.secret.setPlaceholderText("Secret Key")

        self.login_button = QPushButton("Connect")

        layout.addWidget(QLabel("API Key"))
        layout.addWidget(self.api_key)

        layout.addWidget(QLabel("Secret"))
        layout.addWidget(self.secret)

        layout.addWidget(self.login_button)

        self.setLayout(layout)