import sys
import os
import io
import json
import urllib.parse
import pandas as pd
import msoffcrypto
from sqlalchemy import create_engine, text, NVARCHAR
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QPushButton, QLineEdit, QLabel, 
                             QFileDialog, QTextEdit, QMessageBox, QGroupBox, 
                             QFormLayout, QTableWidget, QTableWidgetItem, 
                             QHeaderView, QComboBox, QProgressBar)
from PyQt6.QtCore import QThread, pyqtSignal, Qt
from PyQt6.QtGui import QFont


class TestConnectionWorker(QThread):
    """Thread for testing Database connection"""
    finished = pyqtSignal(bool, str)

    def __init__(self, db_config):
        super().__init__()
        self.db_config = db_config

    def run(self):
        try:
            safe_password = urllib.parse.quote_plus(self.db_config['password'])
            conn_str = (
                f"mssql+pymssql://{self.db_config['user']}:{safe_password}"
                f"@{self.db_config['host']}:1433/{self.db_config['db_name']}?charset=utf8"
            )
            # Set a short timeout for testing
            engine = create_engine(conn_str, connect_args={'timeout': 10})
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.finished.emit(True, "Database connected successfully!")
        except Exception as e:
            self.finished.emit(False, str(e))


class ImportWorker(QThread):
    finished = pyqtSignal(str)
    log_signal = pyqtSignal(str)

    def __init__(self, db_config, file_info, mod_cfg, global_prefix, revision, dest_table_name):
        super().__init__()
        self.db_config = db_config
        self.file_info = file_info  # {path, password}
        self.mod_cfg = mod_cfg      # from config.json module_config
        self.global_prefix = global_prefix
        self.revision = revision
        self.dest_table_name = dest_table_name

    def clean_special_characters(self, text_val):
        if not isinstance(text_val, str):
            return text_val
        return "".join(c for c in text_val if c.isprintable())

    def run(self):
        try:
            module_name = self.mod_cfg.get('module_name')

            # 1. Process Start
            self.log_signal.emit(f"🚀 Starting process for Module: {module_name}")

            # 2. Excel Handling & Decryption
            excel_source = self.file_info['path']
            if self.file_info['password']:
                self.log_signal.emit("🔐 Decrypting Excel file...")
                decrypted_data = io.BytesIO()
                with open(self.file_info['path'], "rb") as f:
                    office_file = msoffcrypto.OfficeFile(f)
                    office_file.load_key(password=self.file_info['password'])
                    office_file.decrypt(decrypted_data)
                excel_source = decrypted_data

            # 3. Read Data from Excel
            self.log_signal.emit("📊 Loading data from Excel...")
            df = pd.read_excel(
                excel_source,
                skiprows=self.mod_cfg.get('skiprows', 0),
                usecols=self.mod_cfg.get('usecols', None),
                dtype=str,
                keep_default_na=False
            )

            row_count = len(df)
            self.log_signal.emit(f"📈 Found {row_count} rows in Excel")

            # 4. Clean Special Characters
            self.log_signal.emit("🔍 Cleaning special characters...")
            for col in df.columns:
                df[col] = df[col].apply(self.clean_special_characters)

            # 5. Connect to Database
            self.log_signal.emit("💾 Connecting to MS SQL Database...")
            safe_password = urllib.parse.quote_plus(self.db_config['password'])
            conn_str = (
                f"mssql+pymssql://{self.db_config['user']}:{safe_password}"
                f"@{self.db_config['host']}:1433/{self.db_config['db_name']}?charset=utf8"
            )
            engine = create_engine(conn_str, connect_args={'timeout': 30})

            # 6. Check/Create Schema
            schema_name = self.global_prefix
            with engine.connect() as conn:
                self.log_signal.emit(f"🛠 Checking Schema: {schema_name}")
                conn.execute(text(
                    f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema_name}') "
                    f"EXEC('CREATE SCHEMA {schema_name}')"
                ))
                conn.commit()

            # 7. Save Data
            dest_table = f"Raw{module_name}{self.revision}{self.dest_table_name}"
            self.log_signal.emit(f"📝 Writing data to table {schema_name}.{dest_table}...")

            dtype_map = {col: NVARCHAR(500) for col in df.columns}
            df.to_sql(
                dest_table, con=engine, schema=schema_name,
                if_exists='replace', index=False, dtype=dtype_map
            )

            self.finished.emit(f"✅ Success! Imported {row_count} rows → {schema_name}.{dest_table}")

        except Exception as e:
            self.finished.emit(f"❌ Error: {str(e)}")


class FetchTablesWorker(QThread):
    """Thread for fetching Table list from Database"""
    finished = pyqtSignal(list)
    error = pyqtSignal(str)

    def __init__(self, db_config):
        super().__init__()
        self.db_config = db_config

    def run(self):
        try:
            safe_password = urllib.parse.quote_plus(self.db_config['password'])
            conn_str = (
                f"mssql+pymssql://{self.db_config['user']}:{safe_password}"
                f"@{self.db_config['host']}:1433/{self.db_config['db_name']}?charset=utf8"
            )
            engine = create_engine(conn_str, connect_args={'timeout': 15})
            with engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME"
                ))
                tables = [f"{row[0]}.{row[1]}" for row in result.fetchall()]
            self.finished.emit(tables)
        except Exception as e:
            self.error.emit(str(e))


class App(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Excel to MS SQL Importer")
        self.setMinimumSize(800, 900)
        self.config_data = {}
        self.initUI()
        self.load_json_config()

    def initUI(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # ── Section 1: Database Connection ──
        db_group = QGroupBox("1. Database Connection (MS SQL Server)")
        db_form = QFormLayout()
        self.db_host = QLineEdit()
        self.db_name = QLineEdit()
        self.db_user = QLineEdit()
        self.db_pass = QLineEdit()
        self.db_pass.setEchoMode(QLineEdit.EchoMode.Password)
        
        # Test Connection Button
        self.btn_test_db = QPushButton("⚡ Test Connection")
        self.btn_test_db.setFixedWidth(150)
        self.btn_test_db.clicked.connect(self.test_db_connection)
        
        db_form.addRow("Server Address:", self.db_host)
        db_form.addRow("Database Name:", self.db_name)
        db_form.addRow("Username:", self.db_user)
        db_form.addRow("Password:", self.db_pass)
        db_form.addRow("", self.btn_test_db)
        
        db_group.setLayout(db_form)
        main_layout.addWidget(db_group)

        # ── Section 2: Excel & Module Configuration ──
        ex_group = QGroupBox("2. Excel & Module Configuration")
        ex_form = QFormLayout()

        # Module dropdown
        self.combo_module = QComboBox()
        ex_form.addRow("Select Module:", self.combo_module)

        # Table dropdown + Refresh button
        table_box = QHBoxLayout()
        self.combo_table = QComboBox()
        self.combo_table.setEditable(True)
        self.combo_table.setPlaceholderText("-- Select Table or Type Manually --")
        self.btn_refresh_tables = QPushButton("🔄 Refresh List")
        self.btn_refresh_tables.setFixedWidth(120)
        self.btn_refresh_tables.clicked.connect(self.fetch_tables_from_db)
        table_box.addWidget(self.combo_table)
        table_box.addWidget(self.btn_refresh_tables)
        ex_form.addRow("Destination Table:", table_box)

        # Excel file browse
        file_box = QHBoxLayout()
        self.txt_file = QLineEdit()
        self.txt_file.setReadOnly(True)
        btn_browse = QPushButton("Browse")
        btn_browse.clicked.connect(self.browse_file)
        file_box.addWidget(self.txt_file)
        file_box.addWidget(btn_browse)
        ex_form.addRow("Excel File:", file_box)

        # Excel password
        self.txt_excel_pass = QLineEdit()
        self.txt_excel_pass.setEchoMode(QLineEdit.EchoMode.Password)
        self.txt_excel_pass.setPlaceholderText("Password for encrypted Excel (Optional)")
        ex_form.addRow("Excel Password:", self.txt_excel_pass)

        ex_group.setLayout(ex_form)
        main_layout.addWidget(ex_group)

        # ── Action Buttons ──
        btn_layout = QHBoxLayout()
        self.btn_run = QPushButton("💾 SAVE TO DATABASE")
        self.btn_run.setFixedHeight(50)
        self.btn_run.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        self.btn_run.setStyleSheet(
            "background-color: #033dfc; color: white; border-radius: 5px;"
        )
        self.btn_run.clicked.connect(self.start_process)

        self.btn_export = QPushButton("🚀 EXPORT LOG")
        self.btn_export.setFixedHeight(50)
        self.btn_export.clicked.connect(self.export_log)

        btn_layout.addWidget(self.btn_run, 3)
        btn_layout.addWidget(self.btn_export, 1)
        main_layout.addLayout(btn_layout)

        # ── Log Display ──
        main_layout.addWidget(QLabel("Process Logs:"))
        self.log_display = QTextEdit()
        self.log_display.setReadOnly(True)
        self.log_display.setStyleSheet("""
            background-color: #1E1E1E; 
            color: #00FF00; 
            font-family: 'Consolas', monospace; 
            font-size: 14px; 
            padding: 10px;
        """)
        main_layout.addWidget(self.log_display)

    # ────────────────────────────────────────────
    # Test Connection Logic
    # ────────────────────────────────────────────
    def test_db_connection(self):
        db_config = {
            'host': self.db_host.text().strip(),
            'db_name': self.db_name.text().strip(),
            'user': self.db_user.text().strip(),
            'password': self.db_pass.text().strip(),
        }

        if not all([db_config['host'], db_config['db_name'], db_config['user']]):
            QMessageBox.warning(self, "Missing Info", "Please provide Server, Database, and User info.")
            return

        self.btn_test_db.setEnabled(False)
        self.btn_test_db.setText("⏳ Testing...")
        self.log_display.append("📡 Testing connection...")

        self.conn_worker = TestConnectionWorker(db_config)
        self.conn_worker.finished.connect(self.on_test_connection_finished)
        self.conn_worker.start()

    def on_test_connection_finished(self, success, message):
        self.btn_test_db.setEnabled(True)
        self.btn_test_db.setText("⚡ Test Connection")
        
        if success:
            self.log_display.append(f"✅ {message}")
            QMessageBox.information(self, "Success", message)
        else:
            self.log_display.append(f"❌ Connection Failed: {message}")
            QMessageBox.critical(self, "Connection Error", f"Cannot connect to Database:\n{message}")

    # ────────────────────────────────────────────
    # Config Loading
    # ────────────────────────────────────────────
    def load_json_config(self):
        try:
            config_path = 'config.json'
            if not os.path.exists(config_path):
                self.log_display.append("⚠️ config.json not found")
                return
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config_data = json.load(f)

            db = self.config_data.get('database', {})
            self.db_host.setText(db.get('host', 'localhost'))
            self.db_name.setText(db.get('database', 'master'))
            self.db_user.setText(db.get('user', 'sa'))
            self.db_pass.setText(db.get('password', ''))

            modules = self.config_data.get('module_config', [])
            self.combo_module.clear()
            for m in modules:
                if m.get('enabled', True):
                    self.combo_module.addItem(m.get('module_name'), m)

            if modules:
                default_table = modules[0].get('table_name', '')
                self.combo_table.setCurrentText(default_table)

            self.log_display.append("✅ Config loaded successfully")
        except Exception as e:
            self.log_display.append(f"❌ Config loading error: {str(e)}")

    # ────────────────────────────────────────────
    # Fetch Tables from Database
    # ────────────────────────────────────────────
    def fetch_tables_from_db(self):
        db_config = {
            'host': self.db_host.text().strip(),
            'db_name': self.db_name.text().strip(),
            'user': self.db_user.text().strip(),
            'password': self.db_pass.text().strip(),
        }

        if not db_config['host'] or not db_config['db_name']:
            QMessageBox.warning(self, "Incomplete Data", "Please specify Server Address and Database Name")
            return

        self.btn_refresh_tables.setEnabled(False)
        self.btn_refresh_tables.setText("⏳ Loading...")
        self.log_display.append("🔄 Fetching table list from Database...")

        self.table_worker = FetchTablesWorker(db_config)
        self.table_worker.finished.connect(self.on_tables_fetched)
        self.table_worker.error.connect(self.on_tables_fetch_error)
        self.table_worker.start()

    def on_tables_fetched(self, tables):
        self.btn_refresh_tables.setEnabled(True)
        self.btn_refresh_tables.setText("🔄 Refresh List")

        current_text = self.combo_table.currentText()
        self.combo_table.clear()
        self.combo_table.addItems(tables)
        self.log_display.append(f"✅ Found {len(tables)} tables")

        if current_text:
            idx = self.combo_table.findText(current_text)
            if idx >= 0:
                self.combo_table.setCurrentIndex(idx)
            else:
                self.combo_table.setCurrentText(current_text)

    def on_tables_fetch_error(self, error_msg):
        self.btn_refresh_tables.setEnabled(True)
        self.btn_refresh_tables.setText("🔄 Refresh List")
        self.log_display.append(f"❌ Error fetching tables: {error_msg}")
        QMessageBox.critical(self, "Connection Error", f"Cannot connect to Database:\n{error_msg}")

    # ────────────────────────────────────────────
    # File Browse
    # ────────────────────────────────────────────
    def browse_file(self):
        file, _ = QFileDialog.getOpenFileName(
            self, "Select Excel File", "", "Excel Files (*.xlsx *.xls)"
        )
        if file:
            self.txt_file.setText(file)

    # ────────────────────────────────────────────
    # Start Import Process
    # ────────────────────────────────────────────
    def start_process(self):
        mod_cfg = self.combo_module.currentData()
        dest_table_name = self.combo_table.currentText().strip()

        if not self.txt_file.text() or not mod_cfg:
            QMessageBox.warning(self, "Incomplete Data", "Please select an Excel file and a Module.")
            return

        if not dest_table_name:
            QMessageBox.warning(self, "Incomplete Data", "Please select or type a Destination Table.")
            return

        if '.' in dest_table_name:
            dest_table_name = dest_table_name.split('.')[-1]

        db_config = {
            'host': self.db_host.text().strip(),
            'db_name': self.db_name.text().strip(),
            'user': self.db_user.text().strip(),
            'password': self.db_pass.text().strip(),
        }
        file_info = {
            'path': self.txt_file.text(),
            'password': self.txt_excel_pass.text(),
        }
        prefix = self.config_data.get('Prefix', 'ERP_ERPCONV')
        revision = str(self.config_data.get('revision', ''))

        self.btn_run.setEnabled(False)
        self.log_display.clear()

        self.worker = ImportWorker(
            db_config, file_info, mod_cfg, prefix, revision, dest_table_name
        )
        self.worker.log_signal.connect(self.log_display.append)
        self.worker.finished.connect(self.on_finished)
        self.worker.start()

    def on_finished(self, message):
        self.btn_run.setEnabled(True)
        self.log_display.append("-" * 40)
        self.log_display.append(message)
        QMessageBox.information(self, "Process Status", message)

    # ────────────────────────────────────────────
    # Export Log
    # ────────────────────────────────────────────
    def export_log(self):
        log_content = self.log_display.toPlainText()
        if not log_content.strip():
            QMessageBox.warning(self, "No Data", "No log data available to export.")
            return

        file_path, _ = QFileDialog.getSaveFileName(
            self, "Save Log File", "import_log.txt", "Text Files (*.txt)"
        )
        if file_path:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(log_content)
            QMessageBox.information(self, "Success", f"Log saved successfully to:\n{file_path}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = App()
    window.show()
    sys.exit(app.exec())