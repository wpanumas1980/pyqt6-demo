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
                             QFormLayout, QComboBox)
from PyQt6.QtCore import QThread, pyqtSignal, Qt
from PyQt6.QtGui import QFont


class ImportWorker(QThread):
    """Thread สำหรับประมวลผลการ Import ข้อมูล พร้อมระบบบันทึกการ Cleaning"""
    finished = pyqtSignal(str, list) # message, cleaning_report
    log_signal = pyqtSignal(str)

    def __init__(self, db_config, file_info, module_name, table_cfg, global_prefix, revision):
        super().__init__()
        self.db_config = db_config
        self.file_info = file_info
        self.module_name = module_name
        self.table_cfg = table_cfg 
        self.global_prefix = global_prefix
        self.revision = revision

    def run(self):
        cleaning_report = []
        try:
            self.log_signal.emit(f"🚀 เริ่มต้นทำงานสำหรับ Module: {self.module_name}")
            
            # 1. จัดการไฟล์ Excel (รองรับ Password)
            excel_source = self.file_info['path']
            if self.file_info['password']:
                self.log_signal.emit("🔐 กำลังถอดรหัสไฟล์ Excel...")
                decrypted_data = io.BytesIO()
                with open(self.file_info['path'], "rb") as f:
                    office_file = msoffcrypto.OfficeFile(f)
                    office_file.load_key(password=self.file_info['password'])
                    office_file.decrypt(decrypted_data)
                excel_source = decrypted_data

            # 2. อ่านข้อมูลจาก Excel
            sheet_to_read = self.table_cfg.get('sheet_name', 0)
            self.log_signal.emit(f"📊 กำลังอ่าน Sheet: {sheet_to_read} (Table: {self.table_cfg.get('table_name')})...")
            
            df = pd.read_excel(
                excel_source,
                sheet_name=sheet_to_read,
                skiprows=self.table_cfg.get('skiprows', 0),
                usecols=self.table_cfg.get('usecols', None),
                dtype=str,
                keep_default_na=False
            )

            row_count = len(df)
            self.log_signal.emit(f"📈 พบข้อมูลทั้งหมด {row_count} แถว")

            # 3. ล้างอักขระพิเศษและเก็บประวัติ (Cleaning)
            self.log_signal.emit("🔍 กำลังตรวจสอบและล้างอักขระพิเศษ...")
            for col in df.columns:
                for idx, val in df[col].items():
                    if isinstance(val, str):
                        cleaned_val = "".join(c for c in val if c.isprintable())
                        if val != cleaned_val:
                            removed = "".join(set(c for c in val if not c.isprintable()))
                            cleaning_report.append({
                                'Row': idx + 2 + self.table_cfg.get('skiprows', 0), 
                                'Column': col,
                                'Original_Value': val,
                                'Cleaned_Value': cleaned_val,
                                'Removed_Chars_Hex': [hex(ord(c)) for c in removed]
                            })
                            df.at[idx, col] = cleaned_val

            # 4. เชื่อมต่อ Database
            self.log_signal.emit("💾 กำลังเชื่อมต่อกับ MS SQL Database...")
            safe_password = urllib.parse.quote_plus(self.db_config['password'])
            port = self.db_config.get('port', 1433)
            conn_str = (
                f"mssql+pymssql://{self.db_config['user']}:{safe_password}"
                f"@{self.db_config['host']}:{port}/{self.db_config['db_name']}?charset=utf8"
            )
            engine = create_engine(conn_str, connect_args={'timeout': 30})

            # 5. ตรวจสอบ/สร้าง Schema
            schema_name = self.global_prefix
            with engine.connect() as conn:
                conn.execute(text(
                    f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema_name}') "
                    f"EXEC('CREATE SCHEMA {schema_name}')"
                ))
                conn.commit()

            # 6. บันทึกข้อมูลลง Table
            table_name_raw = self.table_cfg.get('table_name')
            dest_table = f"Raw{self.module_name}{self.revision}{table_name_raw}"
            self.log_signal.emit(f"📝 กำลังเขียนข้อมูลลงตาราง {schema_name}.{dest_table}...")

            dtype_map = {col: NVARCHAR(500) for col in df.columns}
            df.to_sql(
                dest_table, con=engine, schema=schema_name,
                if_exists='replace', index=False, dtype=dtype_map
            )

            self.finished.emit(f"✅ สำเร็จ! นำเข้า {row_count} แถว -> {schema_name}.{dest_table}", cleaning_report)

        except Exception as e:
            self.finished.emit(f"❌ เกิดข้อผิดพลาด: {str(e)}", cleaning_report)


class TestConnectionWorker(QThread):
    finished = pyqtSignal(bool, str)
    def __init__(self, db_config):
        super().__init__(); self.db_config = db_config
    def run(self):
        try:
            safe_password = urllib.parse.quote_plus(self.db_config['password'])
            port = self.db_config.get('port', 1433)
            conn_str = f"mssql+pymssql://{self.db_config['user']}:{safe_password}@{self.db_config['host']}:{port}/{self.db_config['db_name']}?charset=utf8"
            engine = create_engine(conn_str, connect_args={'timeout': 10})
            with engine.connect() as conn: conn.execute(text("SELECT 1"))
            self.finished.emit(True, "Database connected successfully!")
        except Exception as e: self.finished.emit(False, str(e))


class App(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Excel to MS SQL Importer (BSA Enhanced)")
        self.setMinimumSize(950, 900)
        self.config_data = {}
        self.last_cleaning_report = []
        self.initUI()
        self.load_json_config()

    def initUI(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # ── Part 1: Database Connection ──
        db_group = QGroupBox("1. Database Connection")
        db_form = QFormLayout()
        
        self.db_host = QComboBox(); self.db_host.setEditable(True)
        self.db_user = QLineEdit()
        self.db_pass = QLineEdit(); self.db_pass.setEchoMode(QLineEdit.EchoMode.Password)
        self.db_name = QComboBox(); self.db_name.setEditable(True)
        
        self.btn_test_db = QPushButton("⚡ Test Connection")
        self.btn_test_db.clicked.connect(self.test_db_connection)
        
        db_form.addRow("Server Address:", self.db_host)
        db_form.addRow("Username:", self.db_user)
        db_form.addRow("Password:", self.db_pass)
        db_form.addRow("Database Name:", self.db_name)
        db_form.addRow("", self.btn_test_db)
        db_group.setLayout(db_form)
        main_layout.addWidget(db_group)

        # ── Part 2: Configuration ──
        ex_group = QGroupBox("2. Configuration & Excel File")
        ex_form = QFormLayout()

        self.combo_module = QComboBox()
        self.combo_module.currentIndexChanged.connect(self.on_module_changed)
        ex_form.addRow("Select Module:", self.combo_module)

        self.combo_table = QComboBox()
        self.combo_table.setEditable(True)
        self.combo_table.currentIndexChanged.connect(self.on_table_changed)
        ex_form.addRow("Destination Table:", self.combo_table)

        self.combo_sheet = QComboBox(); self.combo_sheet.setEditable(True)
        ex_form.addRow("Excel Sheet Name:", self.combo_sheet)

        file_box = QHBoxLayout()
        self.txt_file = QLineEdit(); self.txt_file.setReadOnly(True)
        btn_browse = QPushButton("Browse")
        btn_browse.clicked.connect(self.browse_file)
        file_box.addWidget(self.txt_file); file_box.addWidget(btn_browse)
        ex_form.addRow("Excel File:", file_box)

        self.txt_excel_pass = QLineEdit(); self.txt_excel_pass.setEchoMode(QLineEdit.EchoMode.Password)
        ex_form.addRow("Excel Password:", self.txt_excel_pass)

        ex_group.setLayout(ex_form)
        main_layout.addWidget(ex_group)

        # ── Actions ──
        btn_layout = QHBoxLayout()
        self.btn_run = QPushButton("💾 SAVE TO DATABASE")
        self.btn_run.setFixedHeight(50)
        self.btn_run.setStyleSheet("background-color: #0078D7; color: white; font-weight: bold;")
        self.btn_run.clicked.connect(self.start_process)

        self.btn_export_clean = QPushButton("🧹 EXPORT CLEANING LOG")
        self.btn_export_clean.setFixedHeight(50); self.btn_export_clean.setEnabled(False)
        self.btn_export_clean.clicked.connect(self.export_cleaning_report)

        btn_layout.addWidget(self.btn_run, 2); btn_layout.addWidget(self.btn_export_clean, 1)
        main_layout.addLayout(btn_layout)

        self.log_display = QTextEdit(); self.log_display.setReadOnly(True)
        self.log_display.setStyleSheet("background-color: #1e1e1e; color: #d4d4d4; font-family: Consolas;")
        main_layout.addWidget(QLabel("Process Logs:"))
        main_layout.addWidget(self.log_display)

    def load_json_config(self):
        try:
            config_path = 'config.json'
            if not os.path.exists(config_path): return
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config_data = json.load(f)

            db_cfg = self.config_data.get('database', {})
            self.db_host.clear()
            hosts = db_cfg.get('host', [])
            self.db_host.addItems(hosts) if isinstance(hosts, list) else self.db_host.addItem(str(hosts))
            
            self.db_user.setText(db_cfg.get('user', ''))
            self.db_pass.setText(db_cfg.get('password', ''))

            self.db_name.clear()
            dbs = db_cfg.get('database', [])
            self.db_name.addItems(dbs) if isinstance(dbs, list) else self.db_name.addItem(str(dbs))
            
            self.combo_module.clear()
            for m in self.config_data.get('module_config', []):
                if m.get('enabled', True): self.combo_module.addItem(m.get('module_name'), m)

            self.log_display.append("✅ Config JSON loaded successfully.")
        except Exception as e: self.log_display.append(f"❌ Config Error: {str(e)}")

    def on_module_changed(self):
        mod_cfg = self.combo_module.currentData()
        if not mod_cfg: return
        self.combo_table.clear()
        for t in mod_cfg.get('tables', []): self.combo_table.addItem(t.get('table_name'), t)

    def on_table_changed(self):
        table_cfg = self.combo_table.currentData()
        self.combo_sheet.clear()
        if table_cfg and 'sheet_name' in table_cfg:
            sheets = table_cfg['sheet_name']
            self.combo_sheet.addItems(sheets) if isinstance(sheets, list) else self.combo_sheet.addItem(str(sheets))

    def browse_file(self):
        file, _ = QFileDialog.getOpenFileName(self, "Select Excel", "", "Excel Files (*.xlsx *.xls)")
        if file: self.txt_file.setText(file)

    def get_db_params(self):
        db_cfg = self.config_data.get('database', {})
        return {
            'host': self.db_host.currentText().strip(),
            'db_name': self.db_name.currentText().strip(),
            'user': self.db_user.text().strip(),
            'password': self.db_pass.text().strip(),
            'port': db_cfg.get('port', 1433)
        }

    def test_db_connection(self):
        self.btn_test_db.setEnabled(False)
        self.conn_worker = TestConnectionWorker(self.get_db_params())
        self.conn_worker.finished.connect(self.on_test_finished); self.conn_worker.start()

    def on_test_finished(self, success, message):
        self.btn_test_db.setEnabled(True)
        QMessageBox.information(self, "Result", message) if success else QMessageBox.critical(self, "Error", message)

    def start_process(self):
        mod_cfg = self.combo_module.currentData()
        table_selection = self.combo_table.currentData()
        if not self.txt_file.text() or not mod_cfg:
            QMessageBox.warning(self, "Warning", "Please select file and module."); return

        final_table_cfg = table_selection.copy() if isinstance(table_selection, dict) else {
            "table_name": self.combo_table.currentText().split('.')[-1],
            "usecols": None, "skiprows": 0
        }
        final_table_cfg['sheet_name'] = self.combo_sheet.currentText() or 0
        
        self.btn_run.setEnabled(False); self.btn_export_clean.setEnabled(False); self.log_display.clear()
        self.worker = ImportWorker(
            self.get_db_params(), 
            {'path': self.txt_file.text(), 'password': self.txt_excel_pass.text()},
            mod_cfg.get('module_name'), final_table_cfg, 
            self.config_data.get('Prefix', 'ERP_ERPCONV'), str(self.config_data.get('revision', ''))
        )
        self.worker.log_signal.connect(self.log_display.append)
        self.worker.finished.connect(self.on_import_finished); self.worker.start()

    def on_import_finished(self, message, report):
        self.btn_run.setEnabled(True); self.last_cleaning_report = report
        self.log_display.append(message)
        if report:
            self.btn_export_clean.setEnabled(True)
            self.log_display.append(f"💡 Found {len(report)} special characters cleaned. Click Export Cleaning Log to see details.")
        QMessageBox.information(self, "Result", message)

    def export_cleaning_report(self):
        if not self.last_cleaning_report: return
        path, _ = QFileDialog.getSaveFileName(self, "Save Report", "cleaning_report.csv", "CSV Files (*.csv)")
        if path:
            pd.DataFrame(self.last_cleaning_report).to_csv(path, index=False, encoding='utf-8-sig')
            QMessageBox.information(self, "Success", "Cleaning report exported.")


if __name__ == "__main__":
    app = QApplication(sys.argv); window = App(); window.show(); sys.exit(app.exec())