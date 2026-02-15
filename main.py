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


class TestConnectionWorker(QThread):
    """Thread สำหรับทดสอบการเชื่อมต่อ Database"""
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
            engine = create_engine(conn_str, connect_args={'timeout': 10})
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.finished.emit(True, "Database connected successfully!")
        except Exception as e:
            self.finished.emit(False, str(e))


class ImportWorker(QThread):
    """Thread สำหรับประมวลผลการ Import ข้อมูล"""
    finished = pyqtSignal(str)
    log_signal = pyqtSignal(str)

    def __init__(self, db_config, file_info, module_name, table_cfg, global_prefix, revision):
        super().__init__()
        self.db_config = db_config
        self.file_info = file_info
        self.module_name = module_name
        self.table_cfg = table_cfg 
        self.global_prefix = global_prefix
        self.revision = revision

    def clean_special_characters(self, text_val):
        if not isinstance(text_val, str):
            return text_val
        return "".join(c for c in text_val if c.isprintable())

    def run(self):
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

            # 2. อ่านข้อมูลจาก Excel ตาม Config ของ Table
            self.log_signal.emit(f"📊 กำลังอ่านข้อมูลจาก Excel (Table: {self.table_cfg.get('table_name')})...")
            df = pd.read_excel(
                excel_source,
                skiprows=self.table_cfg.get('skiprows', 0),
                usecols=self.table_cfg.get('usecols', None),
                dtype=str,
                keep_default_na=False
            )

            row_count = len(df)
            self.log_signal.emit(f"📈 พบข้อมูลทั้งหมด {row_count} แถว")

            # 3. ล้างอักขระพิเศษ
            self.log_signal.emit("🔍 กำลังล้างอักขระพิเศษ...")
            for col in df.columns:
                df[col] = df[col].apply(self.clean_special_characters)

            # 4. เชื่อมต่อ Database
            self.log_signal.emit("💾 กำลังเชื่อมต่อกับ MS SQL Database...")
            safe_password = urllib.parse.quote_plus(self.db_config['password'])
            conn_str = (
                f"mssql+pymssql://{self.db_config['user']}:{safe_password}"
                f"@{self.db_config['host']}:1433/{self.db_config['db_name']}?charset=utf8"
            )
            engine = create_engine(conn_str, connect_args={'timeout': 30})

            # 5. ตรวจสอบ/สร้าง Schema
            schema_name = self.global_prefix
            with engine.connect() as conn:
                self.log_signal.emit(f"🛠 กำลังตรวจสอบ Schema: {schema_name}")
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

            self.finished.emit(f"✅ สำเร็จ! นำเข้าข้อมูล {row_count} แถว -> {schema_name}.{dest_table}")

        except Exception as e:
            self.finished.emit(f"❌ เกิดข้อผิดพลาด: {str(e)}")


class FetchTablesWorker(QThread):
    """Thread สำหรับดึงรายชื่อตารางที่มีอยู่จริงใน DB"""
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
        self.setWindowTitle("Excel to MS SQL Importer (BSA Version)")
        self.setMinimumSize(850, 900)
        self.config_data = {}
        self.initUI()
        self.load_json_config()

    def initUI(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # ── ส่วนที่ 1: Database Connection ──
        db_group = QGroupBox("1. Database Connection (MS SQL Server)")
        db_form = QFormLayout()
        
        self.db_host = QLineEdit()
        self.db_user = QLineEdit()
        self.db_pass = QLineEdit()
        self.db_pass.setEchoMode(QLineEdit.EchoMode.Password)
        
        # เปลี่ยนเป็น QComboBox (Dropdown)
        self.db_name = QComboBox()
        self.db_name.setEditable(True) # พิมพ์เองได้หากไม่มีใน List
        
        self.btn_test_db = QPushButton("⚡ Test Connection")
        self.btn_test_db.setFixedWidth(160)
        self.btn_test_db.clicked.connect(self.test_db_connection)
        
        db_form.addRow("Server Address:", self.db_host)
        db_form.addRow("Username:", self.db_user)
        db_form.addRow("Password:", self.db_pass)
        db_form.addRow("Database Name:", self.db_name) # อยู่ด้านล่าง Password ตามที่ต้องการ
        db_form.addRow("", self.btn_test_db)
        
        db_group.setLayout(db_form)
        main_layout.addWidget(db_group)

        # ── ส่วนที่ 2: Module & Table Configuration ──
        ex_group = QGroupBox("2. Configuration & Excel File")
        ex_form = QFormLayout()

        self.combo_module = QComboBox()
        self.combo_module.currentIndexChanged.connect(self.on_module_changed)
        ex_form.addRow("Select Module:", self.combo_module)

        table_box = QHBoxLayout()
        self.combo_table = QComboBox()
        self.combo_table.setEditable(True)
        self.combo_table.setPlaceholderText("-- เลือกตารางหรือพิมพ์ชื่อตารางเอง --")
        
        self.btn_refresh_tables = QPushButton("🔄 DB Refresh")
        self.btn_refresh_tables.setFixedWidth(120)
        self.btn_refresh_tables.clicked.connect(self.fetch_tables_from_db)
        
        table_box.addWidget(self.combo_table)
        table_box.addWidget(self.btn_refresh_tables)
        ex_form.addRow("Destination Table:", table_box)

        file_box = QHBoxLayout()
        self.txt_file = QLineEdit()
        self.txt_file.setReadOnly(True)
        btn_browse = QPushButton("Browse")
        btn_browse.clicked.connect(self.browse_file)
        file_box.addWidget(self.txt_file)
        file_box.addWidget(btn_browse)
        ex_form.addRow("Excel File:", file_box)

        self.txt_excel_pass = QLineEdit()
        self.txt_excel_pass.setEchoMode(QLineEdit.EchoMode.Password)
        self.txt_excel_pass.setPlaceholderText("ใส่รหัสผ่านหากไฟล์ถูกล็อก (ถ้ามี)")
        ex_form.addRow("Excel Password:", self.txt_excel_pass)

        ex_group.setLayout(ex_form)
        main_layout.addWidget(ex_group)

        btn_layout = QHBoxLayout()
        self.btn_run = QPushButton("💾 SAVE TO DATABASE")
        self.btn_run.setFixedHeight(55)
        self.btn_run.setFont(QFont("Segoe UI", 12, QFont.Weight.Bold))
        self.btn_run.setStyleSheet("background-color: #0078D7; color: white; border-radius: 6px;")
        self.btn_run.clicked.connect(self.start_process)

        self.btn_export = QPushButton("📄 EXPORT LOG")
        self.btn_export.setFixedHeight(55)
        self.btn_export.clicked.connect(self.export_log)

        btn_layout.addWidget(self.btn_run, 3)
        btn_layout.addWidget(self.btn_export, 1)
        main_layout.addLayout(btn_layout)

        main_layout.addWidget(QLabel("Process Logs:"))
        self.log_display = QTextEdit()
        self.log_display.setReadOnly(True)
        self.log_display.setStyleSheet("""
            background-color: #121212; 
            color: #00FF41; 
            font-family: 'Consolas', monospace; 
            font-size: 13px; 
            padding: 10px;
        """)
        main_layout.addWidget(self.log_display)

    def test_db_connection(self):
        db_config = self.get_db_config()
        if not all([db_config['host'], db_config['db_name'], db_config['user']]):
            QMessageBox.warning(self, "ข้อมูลไม่ครบ", "กรุณาระบุ Server, Database และ User ให้ครบถ้วน")
            return

        self.btn_test_db.setEnabled(False)
        self.btn_test_db.setText("⏳ Testing...")
        self.log_display.append("📡 กำลังทดสอบการเชื่อมต่อ...")

        self.conn_worker = TestConnectionWorker(db_config)
        self.conn_worker.finished.connect(self.on_test_connection_finished)
        self.conn_worker.start()

    def on_test_connection_finished(self, success, message):
        self.btn_test_db.setEnabled(True)
        self.btn_test_db.setText("⚡ Test Connection")
        if success:
            self.log_display.append(f"✅ {message}")
            QMessageBox.information(self, "เชื่อมต่อสำเร็จ", message)
        else:
            self.log_display.append(f"❌ การเชื่อมต่อล้มเหลว: {message}")
            QMessageBox.critical(self, "ข้อผิดพลาด", f"ไม่สามารถเชื่อมต่อได้:\n{message}")

    def load_json_config(self):
        try:
            config_path = 'config.json'
            if not os.path.exists(config_path):
                self.log_display.append("⚠️ ไม่พบไฟล์ config.json")
                return
                
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config_data = json.load(f)

            db_cfg = self.config_data.get('database', {})
            self.db_host.setText(db_cfg.get('host', 'localhost'))
            self.db_user.setText(db_cfg.get('user', 'sa'))
            self.db_pass.setText(db_cfg.get('password', ''))

            # ปรับปรุงการโหลด Database Name (รองรับทั้ง List และ String)
            self.db_name.clear()
            db_val = db_cfg.get('database', [])
            if isinstance(db_val, list):
                self.db_name.addItems(db_val)
            elif isinstance(db_val, str):
                self.db_name.addItem(db_val)
            
            # โหลดรายการ Modules
            modules = self.config_data.get('module_config', [])
            self.combo_module.clear()
            for m in modules:
                if m.get('enabled', True):
                    self.combo_module.addItem(m.get('module_name'), m)

            self.log_display.append("✅ โหลดการตั้งค่าจาก JSON สำเร็จ")
        except Exception as e:
            self.log_display.append(f"❌ ผิดพลาดในการโหลด Config: {str(e)}")

    def on_module_changed(self):
        mod_cfg = self.combo_module.currentData()
        if not mod_cfg:
            return
            
        self.combo_table.clear()
        tables = mod_cfg.get('tables', [])
        for t in tables:
            self.combo_table.addItem(t.get('table_name'), t)
        
        self.log_display.append(f"📁 เปลี่ยนเป็น Module: {mod_cfg.get('module_name')} (พบ {len(tables)} ตาราง)")

    def fetch_tables_from_db(self):
        db_config = self.get_db_config()
        if not db_config['host'] or not db_config['db_name']:
            QMessageBox.warning(self, "ข้อมูลไม่ครบ", "ระบุ Server และ Database Name ก่อน")
            return

        self.btn_refresh_tables.setEnabled(False)
        self.btn_refresh_tables.setText("⏳ Loading...")
        self.log_display.append("🔄 กำลังดึงรายชื่อตารางจากฐานข้อมูล...")

        self.table_worker = FetchTablesWorker(db_config)
        self.table_worker.finished.connect(self.on_tables_fetched)
        self.table_worker.error.connect(self.on_tables_fetch_error)
        self.table_worker.start()

    def on_tables_fetched(self, tables):
        self.btn_refresh_tables.setEnabled(True)
        self.btn_refresh_tables.setText("🔄 DB Refresh")
        current_text = self.combo_table.currentText()
        for t in tables:
            self.combo_table.addItem(t, None)
        self.log_display.append(f"✅ ดึงรายชื่อ {len(tables)} ตารางจากฐานข้อมูลสำเร็จ")
        self.combo_table.setCurrentText(current_text)

    def on_tables_fetch_error(self, error_msg):
        self.btn_refresh_tables.setEnabled(True)
        self.btn_refresh_tables.setText("🔄 DB Refresh")
        self.log_display.append(f"❌ ไม่สามารถดึงรายชื่อตารางได้: {error_msg}")

    def browse_file(self):
        file, _ = QFileDialog.getOpenFileName(
            self, "เลือกไฟล์ Excel", "", "Excel Files (*.xlsx *.xls)"
        )
        if file:
            self.txt_file.setText(file)

    def get_db_config(self):
        return {
            'host': self.db_host.text().strip(),
            'db_name': self.db_name.currentText().strip(), # ดึงค่าจาก currentText ของ ComboBox
            'user': self.db_user.text().strip(),
            'password': self.db_pass.text().strip(),
        }

    def start_process(self):
        mod_cfg = self.combo_module.currentData()
        table_selection_data = self.combo_table.currentData()
        dest_table_raw_name = self.combo_table.currentText().strip()

        if not self.txt_file.text() or not mod_cfg:
            QMessageBox.warning(self, "ข้อมูลไม่ครบ", "กรุณาเลือกไฟล์ Excel และ Module")
            return

        if not dest_table_raw_name:
            QMessageBox.warning(self, "ข้อมูลไม่ครบ", "กรุณาระบุชื่อตารางปลายทาง")
            return

        if isinstance(table_selection_data, dict):
            final_table_cfg = table_selection_data
        else:
            final_table_cfg = {
                "table_name": dest_table_raw_name.split('.')[-1],
                "usecols": None,
                "skiprows": 0
            }

        db_config = self.get_db_config()
        file_info = {
            'path': self.txt_file.text(),
            'password': self.txt_excel_pass.text(),
        }
        prefix = self.config_data.get('Prefix', 'ERP_ERPCONV')
        revision = str(self.config_data.get('revision', ''))

        self.btn_run.setEnabled(False)
        self.log_display.clear()

        self.worker = ImportWorker(
            db_config, file_info, mod_cfg.get('module_name'), final_table_cfg, prefix, revision
        )
        self.worker.log_signal.connect(self.log_display.append)
        self.worker.finished.connect(self.on_finished)
        self.worker.start()

    def on_finished(self, message):
        self.btn_run.setEnabled(True)
        self.log_display.append("-" * 40)
        self.log_display.append(message)
        QMessageBox.information(self, "ผลการทำงาน", message)

    def export_log(self):
        log_content = self.log_display.toPlainText()
        if not log_content.strip():
            QMessageBox.warning(self, "ไม่มีข้อมูล", "ไม่มี Log ให้ส่งออก")
            return

        file_path, _ = QFileDialog.getSaveFileName(
            self, "บันทึกไฟล์ Log", "import_log.txt", "Text Files (*.txt)"
        )
        if file_path:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(log_content)
            QMessageBox.information(self, "สำเร็จ", f"บันทึก Log เรียบร้อยแล้ว")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = App()
    window.show()
    sys.exit(app.exec())