import sys
import os
import subprocess
import re
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QTextEdit, QGroupBox, QCheckBox
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QFont
from sqlalchemy import create_engine, text


def get_db_host():
    """Auto-detect database host (WSL2-aware)"""
    if os.path.exists('/proc/version'):
        with open('/proc/version', 'r') as f:
            if 'microsoft' in f.read().lower():
                try:
                    result = subprocess.run(
                        ['cat', '/etc/resolv.conf'], 
                        capture_output=True, 
                        text=True
                    )
                    match = re.search(r'nameserver\s+(\d+\.\d+\.\d+\.\d+)', result.stdout)
                    if match:
                        return match.group(1)
                except:
                    pass
    return 'localhost'


class ConnectionTestThread(QThread):
    """Thread สำหรับทดสอบ connection แบบ async"""
    finished = pyqtSignal(bool, str)
    
    def __init__(self, host, user, password, database):
        super().__init__()
        self.host = host
        self.user = user
        self.password = password
        self.database = database
    
    def run(self):
        try:
            # Build connection string
            if self.database:
                conn_str = f"mssql+pymssql://{self.user}:{self.password}@{self.host}:1433/{self.database}"
            else:
                conn_str = f"mssql+pymssql://{self.user}:{self.password}@{self.host}:1433"
            
            # Test connection
            engine = create_engine(conn_str, pool_pre_ping=True)
            with engine.connect() as conn:
                result = conn.execute(text("SELECT @@VERSION"))
                version = result.fetchone()[0]
                
                # Get database name
                result = conn.execute(text("SELECT DB_NAME()"))
                db_name = result.fetchone()[0]
                
                success_msg = f"✅ Connection Successful!\n\n"
                success_msg += f"Host: {self.host}:1433\n"
                success_msg += f"User: {self.user}\n"
                success_msg += f"Database: {db_name or '(default)'}\n\n"
                success_msg += f"Server Version:\n{version[:200]}..."
                
                self.finished.emit(True, success_msg)
        
        except Exception as e:
            error_msg = f"❌ Connection Failed!\n\n"
            error_msg += f"Host: {self.host}:1433\n"
            error_msg += f"User: {self.user}\n"
            error_msg += f"Database: {self.database or '(default)'}\n\n"
            error_msg += f"Error:\n{str(e)}"
            
            self.finished.emit(False, error_msg)


class MSSQLConnectionTester(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("MSSQL Connection Tester")
        self.setGeometry(100, 100, 600, 500)
        
        # Auto-detect host
        auto_host = get_db_host()
        
        # Main widget
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout(main_widget)
        
        # Title
        title = QLabel("🗄️ MSSQL Connection Tester")
        title_font = QFont()
        title_font.setPointSize(16)
        title_font.setBold(True)
        title.setFont(title_font)
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)
        
        # Connection Settings Group
        settings_group = QGroupBox("Connection Settings")
        settings_layout = QVBoxLayout()
        
        # Host
        host_layout = QHBoxLayout()
        host_layout.addWidget(QLabel("Host:"))
        self.host_input = QLineEdit(auto_host)
        self.host_input.setPlaceholderText("e.g., localhost or 10.255.255.254")
        host_layout.addWidget(self.host_input)
        settings_layout.addLayout(host_layout)
        
        # Auto-detect checkbox
        self.auto_detect = QCheckBox(f"Auto-detect WSL2 host (Current: {auto_host})")
        self.auto_detect.setChecked(True)
        self.auto_detect.stateChanged.connect(self.toggle_auto_detect)
        settings_layout.addWidget(self.auto_detect)
        
        # User
        user_layout = QHBoxLayout()
        user_layout.addWidget(QLabel("User:"))
        self.user_input = QLineEdit("sa")
        user_layout.addWidget(self.user_input)
        settings_layout.addLayout(user_layout)
        
        # Password
        password_layout = QHBoxLayout()
        password_layout.addWidget(QLabel("Password:"))
        self.password_input = QLineEdit("admin@supersecret123")
        self.password_input.setEchoMode(QLineEdit.EchoMode.Password)
        password_layout.addWidget(self.password_input)
        settings_layout.addLayout(password_layout)
        
        # Database (optional)
        database_layout = QHBoxLayout()
        database_layout.addWidget(QLabel("Database:"))
        self.database_input = QLineEdit()
        self.database_input.setPlaceholderText("(optional)")
        database_layout.addWidget(self.database_input)
        settings_layout.addLayout(database_layout)
        
        settings_group.setLayout(settings_layout)
        layout.addWidget(settings_group)
        
        # Test Button
        self.test_button = QPushButton("🔌 Test Connection")
        self.test_button.setMinimumHeight(40)
        self.test_button.clicked.connect(self.test_connection)
        layout.addWidget(self.test_button)
        
        # Result Display
        result_group = QGroupBox("Result")
        result_layout = QVBoxLayout()
        
        self.result_text = QTextEdit()
        self.result_text.setReadOnly(True)
        self.result_text.setPlaceholderText("Click 'Test Connection' to start...")
        result_layout.addWidget(self.result_text)
        
        result_group.setLayout(result_layout)
        layout.addWidget(result_group)
        
        # Thread
        self.test_thread = None
    
    def toggle_auto_detect(self, state):
        """Toggle auto-detect host"""
        if state == Qt.CheckState.Checked.value:
            auto_host = get_db_host()
            self.host_input.setText(auto_host)
            self.host_input.setEnabled(False)
        else:
            self.host_input.setEnabled(True)
    
    def test_connection(self):
        """Start connection test"""
        # Disable button during test
        self.test_button.setEnabled(False)
        self.test_button.setText("⏳ Testing...")
        self.result_text.clear()
        self.result_text.append("Connecting to database...\n")
        
        # Get values
        host = self.host_input.text().strip() or 'localhost'
        user = self.user_input.text().strip() or 'sa'
        password = self.password_input.text()
        database = self.database_input.text().strip()
        
        # Start test thread
        self.test_thread = ConnectionTestThread(host, user, password, database)
        self.test_thread.finished.connect(self.on_test_finished)
        self.test_thread.start()
    
    def on_test_finished(self, success, message):
        """Handle test completion"""
        self.result_text.clear()
        self.result_text.append(message)
        
        # Re-enable button
        self.test_button.setEnabled(True)
        self.test_button.setText("🔌 Test Connection")
        
        # Change text color based on result
        if success:
            self.result_text.setStyleSheet("color: green;")
        else:
            self.result_text.setStyleSheet("color: red;")


def main():
    app = QApplication(sys.argv)
    
    # Set application style
    app.setStyle('Fusion')
    
    window = MSSQLConnectionTester()
    window.show()
    
    sys.exit(app.exec())


if __name__ == "__main__":
    main()