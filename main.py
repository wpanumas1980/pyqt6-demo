import sys
from PyQt6.QtWidgets import (
    QApplication,
    QWidget,
    QPushButton,
    QMessageBox,
    QVBoxLayout
)

class MyApp(QWidget):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("PyQt6 Demo App")
        self.resize(300, 150)

        layout = QVBoxLayout()

        btn = QPushButton("Click Me")
        btn.clicked.connect(self.show_message)

        layout.addWidget(btn)
        self.setLayout(layout)

    def show_message(self):
        QMessageBox.information(
            self,
            "Hello",
            "Hello from PyQt6 🚀"
        )

app = QApplication(sys.argv)
window = MyApp()
window.show()
sys.exit(app.exec())
